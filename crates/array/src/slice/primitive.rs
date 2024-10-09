use crate::slice::nullmask::NullmaskSlice;
use crate::slice::Slice;
use crate::writer::{ArrayWriter, NativeWriter, RangeList};
use arrow_buffer::ArrowNativeType;
use std::ops::Range;
use crate::slice::bitmask::BitmaskSlice;


#[derive(Clone)]
pub struct PrimitiveSlice<'a, T> {
    nulls: NullmaskSlice<'a>,
    values: &'a [T]
}


impl <'a, T> PrimitiveSlice<'a, T> {
    pub fn new(values: &'a [T], nulls: Option<BitmaskSlice<'a>>) -> Self {
        Self {
            nulls: NullmaskSlice::new(values.len(), nulls),
            values
        }
    }
}


impl <'a, T: ArrowNativeType> Slice for PrimitiveSlice<'a, T> {
    #[inline]
    fn num_buffers(&self) -> usize {
        2
    }

    #[inline]
    fn len(&self) -> usize {
        self.values.len()
    }

    #[inline]
    fn slice(&self, offset: usize, len: usize) -> Self {
        Self {
            nulls: self.nulls.slice(offset, len),
            values: &self.values[offset..offset + len]
        }
    }

    fn write(&self, dst: &mut impl ArrayWriter) -> anyhow::Result<()> {
        self.nulls.write(dst.nullmask(0))?;
        dst.native(1).write_slice(self.values)
    }

    fn write_range(&self, dst: &mut impl ArrayWriter, range: Range<usize>) -> anyhow::Result<()> {
        self.nulls.write_range(dst.nullmask(0), range.clone())?;
        dst.native(1).write_slice(&self.values[range])
    }

    fn write_ranges(&self, dst: &mut impl ArrayWriter, ranges: &mut impl RangeList) -> anyhow::Result<()> {
        self.nulls.write_ranges(dst.nullmask(0), ranges)?;
        dst.native(1).write_slice_ranges(self.values, ranges)
    }

    fn write_indexes(
        &self, 
        dst: &mut impl ArrayWriter,
        indexes: impl IntoIterator<Item=usize, IntoIter: Clone>
    ) -> anyhow::Result<()> 
    {
        let indexes = indexes.into_iter();
        self.nulls.write_indexes(dst.nullmask(0), indexes.clone())?;
        dst.native(1).write_slice_indexes(self.values, indexes)
    }
}