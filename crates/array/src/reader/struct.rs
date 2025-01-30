use crate::chunking::ChunkRange;
use crate::reader::any::AnyReader;
use crate::reader::{AnyChunkedReader, ArrayReader, BitmaskReader, ChunkedArrayReader, Reader};
use crate::writer::ArrayWriter;
use anyhow::ensure;


pub struct StructReader<R: Reader> {
    nulls: R::Nullmask,
    columns: Vec<AnyReader<R>>
}


impl <R: Reader> StructReader<R> {
    pub fn try_new(nulls: R::Nullmask, columns: Vec<AnyReader<R>>) -> anyhow::Result<Self> {
        let len = nulls.len();
        for (i, c) in columns.iter().enumerate() {
            ensure!(
                len == c.len(),
                "null mask and column {} have incompatible lengths",
                i
            );
        }
        Ok(Self {
            nulls,
            columns
        })
    }
}


impl <R: Reader> ArrayReader for StructReader<R> {
    fn num_buffers(&self) -> usize {
        1 + self.columns.iter().map(|c| c.num_buffers()).sum::<usize>()
    }

    fn len(&self) -> usize {
        self.nulls.len()
    }

    fn read_slice(&mut self, dst: &mut impl ArrayWriter, offset: usize, len: usize) -> anyhow::Result<()> {
        self.nulls.read_slice(dst.nullmask(0), offset, len)?;

        let mut shift = 1;
        for col in self.columns.iter_mut() {
            col.read_slice(&mut dst.shift(shift), offset, len)?;
            shift += col.num_buffers()
        }

        Ok(())
    }
}


pub struct ChunkedStructReader<R: Reader> {
    nulls: Vec<R::Nullmask>,
    columns: Vec<AnyChunkedReader<R>>
}


impl<R: Reader> ChunkedStructReader<R> {
    pub fn new(cap: usize, columns: Vec<AnyChunkedReader<R>>) -> Self {
        Self {
            nulls: Vec::with_capacity(cap),
            columns
        }
    }
}


impl<R: Reader> ChunkedArrayReader for ChunkedStructReader<R> {
    type Chunk = StructReader<R>;

    fn num_buffers(&self) -> usize {
        1 + self.columns.iter().map(|c| c.num_buffers()).sum::<usize>()
    }

    fn push(&mut self, chunk: Self::Chunk) {
        assert_eq!(self.columns.len(), chunk.columns.len(), "type mismatch");
        self.nulls.push(chunk.nulls);
        for (c, r) in self.columns.iter_mut().zip(chunk.columns.into_iter()) {
            c.push(r)
        }
    }

    fn read_chunked_ranges(
        &mut self,
        dst: &mut impl ArrayWriter,
        ranges: impl Iterator<Item=ChunkRange> + Clone
    ) -> anyhow::Result<()>
    {
        let nullmask_dst = dst.nullmask(0);
        for r in ranges.clone() {
            self.nulls[r.chunk_index()].read_slice(
                nullmask_dst,
                r.offset_index(),
                r.len_index()
            )?;
        }

        let mut shift = 1;
        for c in self.columns.iter_mut() {
            let mut dst = dst.shift(shift);
            c.read_chunked_ranges(&mut dst, ranges.clone())?;
            shift += c.num_buffers();
        }

        Ok(())
    }
}


pub struct TableReader<R: Reader> {
    columns: Vec<AnyReader<R>>
}


impl<R: Reader> TableReader<R> {
    pub fn try_new(columns: Vec<AnyReader<R>>) -> anyhow::Result<Self> {
        let len = columns.first().map_or(0, |c| c.len());
        for (i, c) in columns.iter().enumerate() {
            ensure!(
                len == c.len(),
                "columns 0 and {} have different lengths",
                i
            );
        }
        Ok(Self {
            columns
        })
    }
    
    pub fn column_reader(&mut self, i: usize) -> &mut AnyReader<R> {
        &mut self.columns[i]
    }
}


impl <R: Reader> ArrayReader for TableReader<R> {
    fn num_buffers(&self) -> usize {
        self.columns.iter().map(|c| c.num_buffers()).sum()
    }

    fn len(&self) -> usize {
        self.columns.first().map_or(0, |c| c.len())
    }

    fn read_slice(&mut self, dst: &mut impl ArrayWriter, offset: usize, len: usize) -> anyhow::Result<()> {
        let mut shift = 0;
        for col in self.columns.iter_mut() {
            col.read_slice(&mut dst.shift(shift), offset, len)?;
            shift += col.num_buffers()
        }
        Ok(())
    }
}


pub struct ChunkedTableReader<R: Reader> {
    columns: Vec<AnyChunkedReader<R>>
}


impl <R: Reader> ChunkedTableReader<R> {
    pub fn new(columns: Vec<AnyChunkedReader<R>>) -> Self {
        Self {
            columns
        }
    }
}


impl <R: Reader> ChunkedArrayReader for ChunkedTableReader<R> {
    type Chunk = TableReader<R>;

    fn num_buffers(&self) -> usize {
        self.columns.iter().map(|c| c.num_buffers()).sum()
    }

    fn push(&mut self, chunk: Self::Chunk) {
        assert_eq!(self.columns.len(), chunk.columns.len(), "chunk type ,mismatch");
        for (c, r) in self.columns.iter_mut().zip(chunk.columns.into_iter()) {
            c.push(r)
        }
    }

    fn read_chunked_ranges(&mut self, dst: &mut impl ArrayWriter, ranges: impl Iterator<Item=ChunkRange> + Clone) -> anyhow::Result<()> {
        let mut shift = 0;
        for col in self.columns.iter_mut() {
            col.read_chunked_ranges(&mut dst.shift(shift), ranges.clone())?;
            shift += col.num_buffers();
        }
        Ok(())
    }
}