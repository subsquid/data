use crate::chunking::ChunkRange;
use crate::writer::{ArrayWriter, BitmaskWriter, NativeWriter, OffsetsWriter};
use arrow_buffer::ArrowNativeType;
use std::ops::Range;


mod any;
mod binary;
mod boolean;
mod list;
mod native;
mod primitive;
mod r#struct;


pub use any::*;
pub use binary::*;
pub use boolean::*;
pub use list::*;
pub use primitive::*;
pub use r#struct::*;


pub trait BitmaskReader {
    fn len(&self) -> usize;

    fn read_slice(
        &mut self,
        dst: &mut impl BitmaskWriter,
        offset: usize,
        len: usize
    ) -> anyhow::Result<()>;

    fn read(&mut self, dst: &mut impl BitmaskWriter) -> anyhow::Result<()> {
        self.read_slice(dst, 0, self.len())
    }
}


pub trait NativeReader {
    fn len(&self) -> usize;

    fn read_slice(
        &mut self,
        dst: &mut impl NativeWriter,
        offset: usize,
        len: usize
    ) -> anyhow::Result<()>;
    
    fn read(&mut self, dst: &mut impl NativeWriter) -> anyhow::Result<()> {
        self.read_slice(dst, 0, self.len())
    }
}


pub trait OffsetsReader {
    fn len(&self) -> usize;

    fn read_slice(
        &mut self,
        dst: &mut impl OffsetsWriter,
        offset: usize,
        len: usize
    ) -> anyhow::Result<Range<usize>>;
    
    #[inline]
    fn read(&mut self, dst: &mut impl OffsetsWriter) -> anyhow::Result<Range<usize>> {
        self.read_slice(dst, 0, self.len())
    }
}


pub trait Reader {
    type Nullmask: BitmaskReader;
    type Bitmask: BitmaskReader;
    type Native: NativeReader;
    type Offset: OffsetsReader;
}


pub trait ArrayReader {
    fn num_buffers(&self) -> usize;
    
    fn len(&self) -> usize;

    fn read(&mut self, dst: &mut impl ArrayWriter) -> anyhow::Result<()> {
        self.read_slice(dst, 0, self.len())
    }

    fn read_slice(
        &mut self,
        dst: &mut impl ArrayWriter,
        offset: usize,
        len: usize
    ) -> anyhow::Result<()>;
}


pub trait ChunkedArrayReader {
    type Chunk;

    fn num_buffers(&self) -> usize;

    fn push(&mut self, chunk: Self::Chunk);

    fn read_chunked_ranges(
        &mut self,
        dst: &mut impl ArrayWriter,
        ranges: impl Iterator<Item=ChunkRange> + Clone
    ) -> anyhow::Result<()>;
}


pub trait ReaderFactory {
    type Reader: Reader;

    fn nullmask(&mut self) -> anyhow::Result<<Self::Reader as Reader>::Nullmask>;

    fn bitmask(&mut self) -> anyhow::Result<<Self::Reader as Reader>::Bitmask>;

    fn native<T: ArrowNativeType>(&mut self) -> anyhow::Result<<Self::Reader as Reader>::Native>;

    fn offset(&mut self) -> anyhow::Result<<Self::Reader as Reader>::Offset>;
}