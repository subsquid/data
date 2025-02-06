use crate::chunking::ChunkRange;
use crate::reader::{ArrayReader, BitmaskReader, ChunkedArrayReader, NativeReader, Reader};
use crate::writer::ArrayWriter;
use anyhow::ensure;


pub struct PrimitiveReader<R: Reader> {
    nulls: R::Nullmask,
    values: R::Native
}


impl <R: Reader> PrimitiveReader<R> {
    pub fn try_new(nulls: R::Nullmask, values: R::Native) -> anyhow::Result<Self> {
        ensure!(
            nulls.len() == values.len(), 
            "null and value buffers have incompatible lengths"
        );
        Ok(Self {
            nulls,
            values
        })
    }
}


impl <R: Reader> ArrayReader for PrimitiveReader<R> {
    fn num_buffers(&self) -> usize {
        2
    }

    fn len(&self) -> usize {
        self.nulls.len()
    }

    fn read_slice(&mut self, dst: &mut impl ArrayWriter, offset: usize, len: usize) -> anyhow::Result<()> {
        self.nulls.read_slice(dst.nullmask(0), offset, len)?;
        self.values.read_slice(dst.native(1), offset, len)
    }
}


pub struct ChunkedPrimitiveReader<R: Reader> {
    nulls: Vec<R::Nullmask>,
    values: Vec<R::Native>
}


impl<R: Reader> ChunkedPrimitiveReader<R> {
    pub fn with_capacity(cap: usize) -> Self {
        Self {
            nulls: Vec::with_capacity(cap),
            values: Vec::with_capacity(cap)
        }
    }
}


impl<R: Reader> ChunkedArrayReader for ChunkedPrimitiveReader<R> {
    type Chunk = PrimitiveReader<R>;

    fn num_buffers(&self) -> usize {
        2
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
        for r in ranges.clone() {
            self.nulls[r.chunk_index()].read_slice(
                nullmask_dst,
                r.offset_index(),
                r.len_index()
            )?;
        }

        let values_dst = dst.native(1);
        for r in ranges {
            self.values[r.chunk_index()].read_slice(
                values_dst,
                r.offset_index(),
                r.len_index()
            )?;
        }

        Ok(())
    }
}