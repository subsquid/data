use crate::chunking::ChunkRange;
use crate::reader::chunked::ChunkedArrayReader;
use crate::reader::{ArrayReader, BitmaskReader, NativeReader, Reader};
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

    fn read_chunk_ranges(
        chunks: &mut impl ChunkedArrayReader<ArrayReader=Self>, 
        dst: &mut impl ArrayWriter, 
        ranges: impl Iterator<Item=ChunkRange> + Clone
    ) -> anyhow::Result<()> 
    {
        let nullmask_dst = dst.nullmask(0);
        for r in ranges.clone() {
            chunks.chunk(r.chunk).nulls.read_slice(nullmask_dst, r.offset, r.len)?
        }
        
        let native_dst = dst.native(1);
        for r in ranges {
            chunks.chunk(r.chunk).values.read_slice(native_dst, r.offset, r.len)?
        }
        Ok(())
    }
}