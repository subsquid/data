use arrow::datatypes::FieldRef;
use sqd_array::builder::{AnyBuilder, ArrayBuilder};
use sqd_array::chunking::ChunkRange;
use sqd_array::io::file::{ArrayFile, ArrayFileWriter, FileReader};
use sqd_array::reader::{AnyChunkedReader, ChunkedArrayReader};
use sqd_array::slice::{AnyTableSlice, AsSlice, Slice};
use sqd_array::sort::sort_table_to_indexes;
use sqd_array::util::{build_offsets, get_offset_position};
use sqd_array::writer::ArrayWriter;


pub struct TableSorter {
    data_table: Vec<ArrayFileWriter>,
    data_key: Vec<usize>,
    sort_table: Vec<AnyBuilder>,
    sort_key: Vec<usize>,
    batch_offsets: Vec<usize>,
}


impl TableSorter {
    pub fn new(fields: &[FieldRef], sort_key: Vec<usize>) -> anyhow::Result<Self> {
        assert!(sort_key.len() > 0);

        let sort_table = sort_key.iter().map(|i| {
            AnyBuilder::new(fields[*i].data_type())
        }).collect();

        let data_key: Vec<usize> = (0..fields.len())
            .filter(|i| !sort_key.contains(i))
            .collect();

        let data_table = data_key.iter().map(|i| {
            let file = ArrayFile::new_temporary(fields[*i].data_type().clone())?;
            file.write()
        }).collect::<anyhow::Result<_>>()?;

        Ok(Self {
            data_table,
            data_key,
            sort_table,
            sort_key,
            batch_offsets: vec![0],
        })
    }

    pub fn push_batch(&mut self, records: &AnyTableSlice<'_>) -> anyhow::Result<()> {
        if records.len() == 0 {
            return Ok(());
        }

        let order = sort_table_to_indexes(records, &self.sort_key);

        for (i, col) in self.data_key.iter().copied().zip(self.data_table.iter_mut()) {
            records.column(i).write_indexes(col, order.iter().copied())?
        }

        for (i, col) in self.sort_key.iter().copied().zip(self.sort_table.iter_mut()) {
            records.column(i).write_indexes(col, order.iter().copied())?;
        }

        self.batch_offsets.push(self.num_rows() + records.len());
        Ok(())
    }

    pub fn num_rows(&self) -> usize {
        self.batch_offsets.last().copied().unwrap()
    }

    pub fn finish(self) -> anyhow::Result<SortedTable> {
        let data_table = self.data_table.into_iter()
            .map(|c| c.finish())
            .collect::<anyhow::Result<Vec<_>>>()?;

        let num_batches = self.batch_offsets.len() - 1;

        let data_readers = data_table.iter().map(|c| {
            let mut chunked = AnyChunkedReader::with_capacity(num_batches, c.data_type());
            for _ in 0..num_batches {
                chunked.push(c.read()?);
            }
            Ok(chunked)
        }).collect::<anyhow::Result<Vec<_>>>()?;

        let sort_table = AnyTableSlice::new(
            self.sort_table.iter().map(|c| c.as_slice()).collect()
        );

        let order = sort_table_to_indexes(
            &sort_table,
            &(0..sort_table.num_columns()).collect::<Vec<_>>(),
        );

        let chunk_tracker = ChunkTracker::new(&self.batch_offsets, &order);

        Ok(SortedTable {
            data_table,
            data_key: self.data_key,
            sort_table: self.sort_table,
            sort_key: self.sort_key,
            batch_offsets: self.batch_offsets,
            order,
            chunk_tracker,
            data_readers,
        })
    }
}


pub struct SortedTable {
    data_table: Vec<ArrayFile>,
    data_key: Vec<usize>,
    sort_table: Vec<AnyBuilder>,
    sort_key: Vec<usize>,
    batch_offsets: Vec<usize>,
    order: Vec<usize>,
    chunk_tracker: ChunkTracker,
    data_readers: Vec<AnyChunkedReader<FileReader>>,
}


impl SortedTable {
    pub fn into_sorter(mut self) -> anyhow::Result<TableSorter> {
        drop(self.data_readers);

        let data_table = self.data_table.into_iter()
            .map(|c| c.write())
            .collect::<anyhow::Result<_>>()?;

        self.sort_table.iter_mut().for_each(|c| c.clear());
        self.batch_offsets.clear();
        self.batch_offsets.push(0);

        Ok(TableSorter {
            data_table,
            data_key: self.data_key,
            sort_table: self.sort_table,
            sort_key: self.sort_key,
            batch_offsets: self.batch_offsets,
        })
    }

    pub fn num_rows(&self) -> usize {
        self.order.len()
    }

    pub fn read_column(
        &mut self,
        dst: &mut impl ArrayWriter,
        i: usize,
        offset: usize,
        len: usize
    ) -> anyhow::Result<()>
    {
        assert!(offset + len <= self.num_rows());
        assert!(
            i < self.sort_key.len() + self.data_key.len(),
            "column {} does not exist",
            i
        );

        if len == 0 {
            return Ok(());
        }

        if let Some(pos) = self.sort_key.iter().position(|c| *c == i) {
            self.sort_table[pos].as_slice().write_indexes(
                dst,
                self.order[offset..offset + len].iter().copied(),
            )
        } else {
            let pos = self.data_key.iter().position(|c| *c == i).unwrap();
            let reader = &mut self.data_readers[pos];
            let (first, middle, last) = self.chunk_tracker.find(offset, len);
            reader.read_chunked_ranges(dst, first.into_iter())?;
            reader.read_chunked_ranges(dst, middle.iter().cloned())?;
            reader.read_chunked_ranges(dst, last.into_iter())?;
            Ok(())
        }
    }
}


struct ChunkTracker {
    chunks: Vec<ChunkRange>,
    offsets: Vec<u32>,
    last_start_pos: usize,
    last_end_pos: usize,
}


impl ChunkTracker {
    fn new(batch_offsets: &[usize], order: &[usize]) -> Self {
        let chunks = ChunkRange::build_tag_list(batch_offsets, order);
        let offsets = build_offsets(0, chunks.iter().map(|c| c.len));
        Self {
            chunks,
            offsets,
            last_start_pos: 0,
            last_end_pos: 0,
        }
    }

    fn find(&mut self, offset: usize, len: usize) -> (Option<ChunkRange>, &[ChunkRange], Option<ChunkRange>) {
        let start = offset as u32;
        let len = len as u32;
        let end = start + len;

        let mut sp = self.find_start_position(start);

        let mut first_chunk = None;

        if self.offsets[sp] < start {
            let ch_offset = start - self.offsets[sp];
            let ch = &self.chunks[sp];
            first_chunk = Some(ChunkRange {
                chunk: ch.chunk,
                offset: ch.offset + ch_offset,
                len: std::cmp::min(ch.len - ch_offset, len),
            });
        }

        if self.offsets[sp + 1] >= end {
            return (
                first_chunk.or_else(|| {
                    let ch = &self.chunks[sp];
                    Some(ChunkRange {
                        chunk: ch.chunk,
                        offset: ch.offset,
                        len,
                    })
                }),
                &[],
                None
            );
        }

        if first_chunk.is_some() {
            sp += 1;
        }

        let ep = self.find_end_position(end - 1);

        if self.offsets[ep + 1] > end {
            let ch = &self.chunks[ep];
            (
                first_chunk,
                &self.chunks[sp..ep],
                Some(ChunkRange {
                    chunk: ch.chunk,
                    offset: ch.offset,
                    len: ch.len + end - self.offsets[ep + 1],
                })
            )
        } else {
            (first_chunk, &self.chunks[sp..ep + 1], None)
        }
    }

    fn find_start_position(&mut self, index: u32) -> usize {
        self.last_start_pos = get_offset_position(&self.offsets, index, self.last_start_pos);
        self.last_start_pos
    }

    fn find_end_position(&mut self, index: u32) -> usize {
        self.last_end_pos = get_offset_position(&self.offsets, index, self.last_end_pos);
        self.last_end_pos
    }
}