use crate::chunking::ChunkRange;
use crate::reader::{ArrayReader, BitmaskReader, ChunkedArrayReader, Reader};
use crate::writer::ArrayWriter;
use anyhow::ensure;


pub struct FixedSizeListReader<R: Reader, T> {
    size: usize,
    nulls: R::Nullmask,
    values: T
}


impl <R: Reader, T: ArrayReader> FixedSizeListReader<R, T> {
    pub fn try_new(size: usize, nulls: R::Nullmask, values: T) -> anyhow::Result<Self> {
        ensure!(
            nulls.len() * size == values.len(),
            "null and value buffers have incompatible lengths: {} * {} != {}",
            nulls.len(), size, values.len()
        );
        Ok(Self {
            size,
            nulls,
            values
        })
    }
}


impl <R: Reader, T: ArrayReader> ArrayReader for FixedSizeListReader<R, T> {
    fn num_buffers(&self) -> usize {
        1 + self.values.num_buffers()
    }

    fn len(&self) -> usize {
        self.nulls.len()
    }

    fn read_slice(&mut self, dst: &mut impl ArrayWriter, offset: usize, len: usize) -> anyhow::Result<()> {
        self.nulls.read_slice(dst.nullmask(0), offset, len)?;

        self.values.read_slice(&mut dst.shift(1), offset * self.size, len * self.size)
    }
}


pub struct ChunkedFixedSizeListReader<R: Reader, T> {
    size: usize,
    nulls: Vec<R::Nullmask>,
    values: T
}


impl<R: Reader, T> ChunkedFixedSizeListReader<R, T> {
    pub fn new(size: usize, capacity: usize, values: T) -> Self {
        Self {
            size,
            nulls: Vec::with_capacity(capacity),
            values
        }
    }
}


impl <R: Reader, T: ChunkedArrayReader> ChunkedArrayReader for ChunkedFixedSizeListReader<R, T> {
    type Chunk = FixedSizeListReader<R, T::Chunk>;

    fn num_buffers(&self) -> usize {
        1 + self.values.num_buffers()
    }

    fn push(&mut self, chunk: Self::Chunk) {
        self.nulls.push(chunk.nulls);
        self.values.push(chunk.values)
    }

    fn read_chunked_ranges(
        &mut self,
        dst: &mut impl ArrayWriter,
        ranges: impl Iterator<Item=ChunkRange> + Clone
    ) -> anyhow::Result<()>
    {
        let nullmask_dst = dst.nullmask(0);
        let mut ranges_len = 0;
        for r in ranges.clone() {
            self.nulls[r.chunk_index()].read_slice(
                nullmask_dst,
                r.offset_index(),
                r.len_index()
            )?;
            ranges_len += 1;
        }

        let mut value_ranges = Vec::with_capacity(ranges_len);
        for r in ranges {
            value_ranges.push(ChunkRange {
                chunk: r.chunk,
                offset: r.offset * self.size as u32,
                len: r.len * self.size as u32
            })
        }

        self.values.read_chunked_ranges(
            &mut dst.shift(1), 
            value_ranges.into_iter()
        )
    }
}
