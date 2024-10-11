use crate::chunking::ChunkRange;
use crate::reader::chunked::ChunkedArrayReader;
use crate::reader::{ArrayReader, NativeReader};
use crate::writer::ArrayWriter;


pub struct NativeArrayReader<R> {
    native_reader: R
}


impl<R> NativeArrayReader<R> {
    pub fn new(native_reader: R) -> Self {
        Self {
            native_reader
        }
    }
}


impl <R: NativeReader> ArrayReader for NativeArrayReader<R> {
    #[inline]
    fn num_buffers(&self) -> usize {
        1
    }

    #[inline]
    fn len(&self) -> usize {
        self.native_reader.len()
    }

    #[inline]
    fn read_slice(&mut self, dst: &mut impl ArrayWriter, offset: usize, len: usize) -> anyhow::Result<()> {
        self.native_reader.read_slice(dst.native(0), offset, len)
    }

    fn read_chunk_ranges(
        chunks: &mut impl ChunkedArrayReader<ArrayReader=Self>, 
        dst: &mut impl ArrayWriter, 
        ranges: impl Iterator<Item=ChunkRange> + Clone
    ) -> anyhow::Result<()> 
    {
        let dst = dst.native(0);
        for r in ranges {
            chunks.chunk(r.chunk).native_reader.read_slice(dst, r.offset, r.len)?
        }
        Ok(())
    }
}