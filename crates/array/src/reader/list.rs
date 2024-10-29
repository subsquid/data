use crate::chunking::ChunkRange;
use crate::reader::chunked::ChunkedArrayReader;
use crate::reader::{ArrayReader, BitmaskReader, OffsetsReader, Reader};
use crate::writer::ArrayWriter;
use anyhow::ensure;


pub struct ListReader<R: Reader, T> {
    nulls: R::Nullmask,
    offsets: R::Offset,
    values: T
}


impl <R: Reader, T> ListReader<R, T> {
    pub fn try_new(nulls: R::Nullmask, offsets: R::Offset, values: T) -> anyhow::Result<Self> {
        ensure!(
            nulls.len() == offsets.len(), 
            "null and offset buffers have incompatible lengths"
        );
        Ok(Self {
            nulls,
            offsets,
            values
        })
    }
}


impl <R: Reader, T: ArrayReader> ArrayReader for ListReader<R, T> {
    fn num_buffers(&self) -> usize {
        2 + self.values.num_buffers()
    }

    fn len(&self) -> usize {
        self.nulls.len()
    }

    fn read_slice(&mut self, dst: &mut impl ArrayWriter, offset: usize, len: usize) -> anyhow::Result<()> {
        self.nulls.read_slice(dst.nullmask(0), offset, len)?;
        
        let value_range = self.offsets.read_slice(dst.offset(1), offset, len)?;
        
        self.values.read_slice(&mut dst.shift(2), value_range.start, value_range.len())
    }

    fn read_chunk_ranges(
        chunks: &mut (impl ChunkedArrayReader<ArrayReader=Self> + ?Sized), 
        dst: &mut impl ArrayWriter, 
        ranges: impl Iterator<Item=ChunkRange> + Clone
    ) -> anyhow::Result<()> 
    {
        let nullmask_dst = dst.nullmask(0);
        let mut span = 0;
        for r in ranges.clone() {
            chunks
                .chunk(r.chunk_index())
                .nulls
                .read_slice(nullmask_dst, r.offset_index(), r.len_index())?;
            span += 1;
        }

        let offsets_dst = dst.offset(1);
        let mut value_ranges = Vec::with_capacity(span);
        for r in ranges {
            let value_range = chunks
                .chunk(r.chunk_index())
                .offsets
                .read_slice(offsets_dst, r.offset_index(), r.len_index())?;
            
            if !value_range.is_empty() {
                value_ranges.push(ChunkRange {
                    chunk: r.chunk,
                    offset: value_range.start as u32,
                    len: value_range.len() as u32
                })
            }
        }

        T::read_chunk_ranges(
            &mut chunks.map(|r| &mut r.values),
            &mut dst.shift(2),
            value_ranges.iter().cloned()
        )?;

        Ok(())
    }
}