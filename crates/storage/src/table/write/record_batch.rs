use anyhow::ensure;
use arrow::array::{Array, ArrayRef, RecordBatch};
use arrow::datatypes::SchemaRef;
use sqd_dataset::TableOptions;

use crate::array::ext::{bisect_at_byte_size, get_byte_size};
use crate::kv::KvWrite;
use crate::util::next_chunk;
use crate::table::write::writer::TableWriter;


const TOL: usize = 10;


struct ChunkedArray {
    chunks: Vec<ArrayRef>
}


impl ChunkedArray {
    fn new() -> Self {
        Self {
            chunks: Vec::with_capacity(32)
        }
    }

    fn push(&mut self, array: ArrayRef) {
        self.chunks.push(array)
    }

    fn byte_size(&self) -> usize {
        self.chunks.iter().map(|a| get_byte_size(a.as_ref())).sum()
    }

    fn take(&mut self, byte_size: usize) -> ArrayRef {
        assert!(self.chunks.len() > 0);
        let (idx, len) = self.find_byte_size_pos(byte_size);
        if idx == 0 {
            if self.chunks[0].len() == len {
                self.chunks.remove(0)
            } else {
                let array = self.chunks[0].clone();
                self.chunks[0] = array.slice(len, array.len() - len);
                array.slice(0, len)
            }
        } else {
            let mut seq = Vec::with_capacity(idx + 1);
            let last_array = self.chunks[idx].clone();
            if last_array.len() == len {
                seq.extend(self.chunks.drain(0..idx + 1));
            } else {
                self.chunks[idx] = last_array.slice(len, last_array.len() - len);
                seq.extend(self.chunks.drain(0..idx));
                seq.push(last_array.slice(0, len));
            }
            let ref_seq: Vec<_> = seq.iter().map(|a| a.as_ref()).collect();
            arrow::compute::concat(ref_seq.as_slice()).unwrap()
        }
    }

    fn find_byte_size_pos(&self, byte_size: usize) -> (usize, usize) {
        assert!(self.chunks.len() > 0);
        let mut total = 0;
        for (idx, array) in self.chunks.iter().enumerate() {
            let new_total = total + get_byte_size(&array);
            if new_total > byte_size + byte_size / TOL {
                return (idx, bisect_at_byte_size(&array, byte_size - total, TOL))
            }
            if byte_size - byte_size / 10 <= new_total {
                return (idx, array.len())
            }
            total = new_total;
        }
        (self.chunks.len() - 1, self.chunks.last().unwrap().len())
    }

    fn take_all(&mut self, byte_size: usize) -> impl Iterator<Item = ArrayRef> + '_ {
        assert!(byte_size > 0);
        std::iter::from_fn(move || {
            if self.chunks.len() == 0 {
                return None
            }
            let total = self.byte_size();
            let next_size = next_chunk(byte_size, total);
            Some(self.take(next_size))
        })
    }
}


pub struct RecordBatchWriter<S> {
    writer: TableWriter<S>,
    columns: Vec<ChunkedArray>,
    num_rows: usize,
    current_row_group_size: usize,
    page_size: usize,
    expected_num_rows: usize,
    target_row_group_size: usize,
    had_error: bool
}


impl <S: KvWrite> RecordBatchWriter<S> {
    pub fn new(
        storage: S,
        schema: SchemaRef,
        table_name: &[u8],
        table_options: &TableOptions
    ) -> Self
    {
        Self {
            writer: TableWriter::new(storage, schema.clone(), table_name, table_options),
            columns: std::iter::repeat_with(ChunkedArray::new)
                .take(schema.fields().len())
                .collect(),
            num_rows: 0,
            current_row_group_size: 0,
            page_size: table_options.default_page_size,
            expected_num_rows: 0,
            target_row_group_size: 0,
            had_error: false
        }
    }

    pub fn with_expected_row_count(&mut self, row_count: usize) {
        self.expected_num_rows = row_count;
    }

    pub fn with_row_group_size(&mut self, row_count: usize) {
        self.target_row_group_size = row_count;
    }

    pub fn storage(&self) -> &S {
        self.writer.storage()
    }

    pub fn storage_mut(&mut self) -> &mut S {
        self.writer.storage_mut()
    }

    pub fn finish(mut self) -> anyhow::Result<S> {
        self.flush_pages()?;
        self.writer.finish()
    }

    pub fn write_record_batch(&mut self, record_batch: &RecordBatch) -> anyhow::Result<()> {
        ensure!(!self.had_error, "writer is not usable due to prev error");

        let result = if self.target_row_group_size > 0 {
            self.write_with_target_row_group_size(record_batch)
        } else {
            self.write(record_batch)
        };

        self.had_error = result.is_err();
        result
    }

    fn write_with_target_row_group_size(
        &mut self,
        record_batch: &RecordBatch
    ) -> anyhow::Result<()>
    {
        let mut next_batch: Option<RecordBatch> = None;
        loop {
            let record_batch = next_batch.as_ref().unwrap_or(record_batch);
            let target_size = self.next_row_group_size();
            let current_size = self.current_row_group_size + record_batch.num_rows();
            if current_size <= target_size {
                self.write(record_batch)?;
                return Ok(())
            }
            if self.current_row_group_size >= target_size - target_size * TOL / 100 {
                self.new_row_group()?;
            } else {
                let split_len = target_size - self.current_row_group_size;
                self.write(&record_batch.slice(0, split_len))?;
                self.new_row_group()?;
                next_batch = Some(record_batch.slice(split_len, record_batch.num_rows() - split_len));
            }
        }
    }

    fn next_row_group_size(&self) -> usize {
        assert!(self.target_row_group_size > 0);
        let left = self.expected_num_rows.saturating_sub(self.num_rows - self.current_row_group_size);
        if left == 0 {
            self.target_row_group_size
        } else {
            next_chunk(self.target_row_group_size, left)
        }
    }

    fn write(&mut self, record_batch: &RecordBatch) -> anyhow::Result<()> {
        assert_eq!(record_batch.num_columns(), self.columns.len());
        for (idx, (arr, col)) in record_batch.columns().iter().cloned().zip(self.columns.iter_mut()).enumerate() {
            col.push(arr);
            while col.byte_size() > 2 * self.page_size  {
                self.writer.push_page(idx, col.take(self.page_size).as_ref())?;
            }
        }
        self.num_rows += record_batch.num_rows();
        self.current_row_group_size += record_batch.num_rows();
        Ok(())
    }

    fn new_row_group(&mut self) -> anyhow::Result<()> {
        self.flush_pages()?;
        self.writer.new_row_group()?;
        self.current_row_group_size = 0;
        Ok(())
    }

    fn flush_pages(&mut self) -> anyhow::Result<()> {
        for (idx, col) in self.columns.iter_mut().enumerate() {
            for page in col.take_all(self.page_size) {
                self.writer.push_page(idx, page.as_ref())?;
            }
        }
        Ok(())
    }
}