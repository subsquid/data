use crate::index::{IndexList, RangeList};
use crate::slice::Slice;
use crate::writer::{ArrayWriter, NativeWriter};
use arrow_buffer::ArrowNativeType;
use std::ops::Range;


impl <'a, T: ArrowNativeType> Slice for &'a [T] {
    #[inline]
    fn num_buffers(&self) -> usize {
        1
    }

    #[inline]
    fn len(&self) -> usize {
        self.as_ref().len()
    }

    #[inline]
    fn slice(&self, offset: usize, len: usize) -> Self {
        &self[offset..offset + len]
    }

    #[inline]
    fn write(&self, dst: &mut impl ArrayWriter) -> anyhow::Result<()> {
        dst.native(0).write_slice(self)
    }

    #[inline]
    fn write_range(&self, dst: &mut impl ArrayWriter, range: Range<usize>) -> anyhow::Result<()> {
        dst.native(0).write_slice(&self[range])
    }

    #[inline]
    fn write_ranges(&self, dst: &mut impl ArrayWriter, ranges: &mut impl RangeList) -> anyhow::Result<()> {
        dst.native(0).write_slice_ranges(self, ranges)
    }

    #[inline]
    fn write_indexes(
        &self, 
        dst: &mut impl ArrayWriter, 
        indexes: &(impl IndexList + ?Sized)
    ) -> anyhow::Result<()> 
    {
        dst.native(0).write_slice_indexes(self, indexes.index_iter())
    }
}