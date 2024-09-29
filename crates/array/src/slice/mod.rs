mod bitmask;
mod primitive;
mod nullmask;
mod list;
mod native;
mod any;
mod boolean;
mod r#struct;


use crate::writer::{ArrayWriter, RangeList};
pub use bitmask::*;
use std::ops::Range;
use std::sync::Arc;


pub trait Slice: Clone {
    fn num_buffers(&self) -> usize;
    
    fn len(&self) -> usize;
    
    fn slice(&self, offset: usize, len: usize) -> Self;
    
    fn write(&self, dst: &mut impl ArrayWriter) -> anyhow::Result<()>;
    
    #[inline]
    fn write_range(&self, dst: &mut impl ArrayWriter, range: Range<usize>) -> anyhow::Result<()> {
        self.slice(range.start, range.len()).write(dst)
    }

    fn write_ranges(&self, dst: &mut impl ArrayWriter, ranges: &mut impl RangeList) -> anyhow::Result<()>;

    fn write_indexes(
        &self, 
        dst: &mut impl ArrayWriter, 
        indexes: impl IntoIterator<Item = usize, IntoIter: Clone>
    ) -> anyhow::Result<()>;
}


impl <T: Slice> Slice for Arc<T> {
    fn num_buffers(&self) -> usize {
        self.as_ref().num_buffers()
    }

    fn len(&self) -> usize {
        self.as_ref().len()
    }

    fn slice(&self, offset: usize, len: usize) -> Self {
        Arc::new(self.as_ref().slice(offset, len))
    }

    #[inline]
    fn write(&self, dst: &mut impl ArrayWriter) -> anyhow::Result<()> {
        self.as_ref().write(dst)
    }

    #[inline]
    fn write_range(&self, dst: &mut impl ArrayWriter, range: Range<usize>) -> anyhow::Result<()> {
        self.as_ref().write_range(dst, range)
    }

    #[inline]
    fn write_ranges(&self, dst: &mut impl ArrayWriter, ranges: &mut impl RangeList) -> anyhow::Result<()> {
        self.as_ref().write_ranges(dst, ranges)
    }

    #[inline]
    fn write_indexes(&self, dst: &mut impl ArrayWriter, indexes: impl IntoIterator<Item=usize, IntoIter: Clone>) -> anyhow::Result<()> {
        self.as_ref().write_indexes(dst, indexes)
    }
}