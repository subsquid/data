use crate::chunking::ChunkRange;
use crate::writer::{ArrayWriter, BitmaskWriter, NativeWriter, OffsetsWriter};
use arrow_buffer::ArrowNativeType;
use std::ops::Range;


mod any;
mod binary;
mod boolean;
mod chunked;
mod list;
mod native;
mod primitive;
mod r#struct;


pub use any::*;
pub use binary::*;
pub use boolean::*;
pub use chunked::*;
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
}


pub trait NativeReader {
    fn len(&self) -> usize;

    fn read_slice(
        &mut self,
        dst: &mut impl NativeWriter,
        offset: usize,
        len: usize
    ) -> anyhow::Result<()>;
}


pub trait OffsetsReader {
    fn len(&self) -> usize;

    fn read_slice(
        &mut self,
        dst: &mut impl OffsetsWriter,
        offset: usize,
        len: usize
    ) -> anyhow::Result<Range<usize>>;
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

    fn read_chunk_ranges(
        chunks: &mut (impl ChunkedArrayReader<ArrayReader=Self> + ?Sized),
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