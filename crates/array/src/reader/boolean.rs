use crate::chunking::ChunkRange;
use crate::reader::chunked::ChunkedArrayReader;
use crate::reader::{ArrayReader, BitmaskReader, Reader};
use crate::writer::ArrayWriter;
use anyhow::ensure;


pub struct BooleanReader<R: Reader> {
    nulls: R::Nullmask,
    values: R::Bitmask
}


impl <R: Reader> BooleanReader<R> {
    pub fn new_unchecked(nulls: R::Nullmask, values: R::Bitmask) -> Self {
        Self {
            nulls,
            values
        }
    }
    
    pub fn try_new(nulls: R::Nullmask, values: R::Bitmask) -> anyhow::Result<Self> {
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


impl <R: Reader> ArrayReader for BooleanReader<R> {
    fn num_buffers(&self) -> usize {
        2
    }

    fn len(&self) -> usize {
        self.nulls.len()
    }

    fn read_slice(&mut self, dst: &mut impl ArrayWriter, offset: usize, len: usize) -> anyhow::Result<()> {
        self.nulls.read_slice(dst.nullmask(0), offset, len)?;
        self.values.read_slice(dst.bitmask(1), offset, len)
    }

    fn read_chunk_ranges(
        chunks: &mut (impl ChunkedArrayReader<ArrayReader=Self> + ?Sized),
        dst: &mut impl ArrayWriter,
        ranges: impl Iterator<Item=ChunkRange> + Clone
    ) -> anyhow::Result<()>
    {
        let nullmask_dst = dst.nullmask(0);
        for r in ranges.clone() {
            chunks
                .chunk(r.chunk_index())
                .nulls
                .read_slice(nullmask_dst, r.offset_index(), r.len_index())?
        }

        let bitmask_dst = dst.bitmask(1);
        for r in ranges {
            chunks
                .chunk(r.chunk_index())
                .values
                .read_slice(bitmask_dst, r.offset_index(), r.len_index())?
        }
        
        Ok(())
    }
}