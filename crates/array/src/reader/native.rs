use crate::chunking::ChunkRange;
use crate::reader::{ArrayReader, ChunkedArrayReader, NativeReader};
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
}


pub struct ChunkedNativeArrayReader<R> {
    chunks: Vec<R>
}


impl<R> ChunkedNativeArrayReader<R> {
    pub fn with_capacity(cap: usize) -> Self {
        Self {
            chunks: Vec::with_capacity(cap)
        }
    }
}  


impl <R: NativeReader> ChunkedArrayReader for ChunkedNativeArrayReader<R> {
    type Chunk = NativeArrayReader<R>;

    fn num_buffers(&self) -> usize {
        1
    }

    fn push(&mut self, chunk: Self::Chunk) {
        self.chunks.push(chunk.native_reader)
    }

    fn read_chunked_ranges(
        &mut self, 
        dst: &mut impl ArrayWriter, 
        ranges: impl Iterator<Item=ChunkRange> + Clone
    ) -> anyhow::Result<()> 
    {
        let dst = dst.native(0);
        for r in ranges {
            self.chunks[r.chunk_index()].read_slice(
                dst,
                r.offset_index(),
                r.len_index()
            )?;
        }
        Ok(())
    }
}