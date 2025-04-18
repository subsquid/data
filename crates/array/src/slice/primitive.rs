use crate::access::Access;
use crate::index::RangeList;
use crate::slice::bitmask::BitmaskSlice;
use crate::slice::nullmask::NullmaskSlice;
use crate::slice::Slice;
use crate::writer::{ArrayWriter, NativeWriter};
use arrow::array::{ArrowPrimitiveType, PrimitiveArray};
use arrow_buffer::ArrowNativeType;
use std::ops::Range;


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
    
    pub fn nulls(&self) -> NullmaskSlice<'a> {
        self.nulls.clone()
    } 
    
    pub fn values(&self) -> &'a [T] {
        self.values
    }
}


impl <'a, T: ArrowNativeType> Slice for PrimitiveSlice<'a, T> {
    #[inline]
    fn num_buffers(&self) -> usize {
        2
    }

    #[inline]
    fn byte_size(&self) -> usize {
        self.nulls.byte_size() + self.values.byte_size()
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
        indexes: impl Iterator<Item=usize> + Clone
    ) -> anyhow::Result<()> 
    {
        self.nulls.write_indexes(dst.nullmask(0), indexes.clone())?;
        dst.native(1).write_slice_indexes(self.values, indexes)
    }
}


impl <'a, T: ArrowPrimitiveType> From<&'a PrimitiveArray<T>> for PrimitiveSlice<'a, T::Native> {
    fn from(value: &'a PrimitiveArray<T>) -> Self {
        Self {
            nulls: NullmaskSlice::from_array(value),
            values: value.values()
        }
    }
}


impl <'a, T: ArrowNativeType> Access for PrimitiveSlice<'a, T> {
    type Value = T;

    #[inline]
    fn get(&self, i: usize) -> Self::Value {
        self.values[i]
    }

    #[inline]
    fn is_valid(&self, i: usize) -> bool {
        self.nulls.is_valid(i)
    }

    #[inline]
    fn has_nulls(&self) -> bool {
        self.nulls.has_nulls()
    }
}


impl<'a, T: ArrowNativeType + Ord> PrimitiveSlice<'a, T> {
    pub fn max(&self) -> Option<T> {
        if self.nulls.has_nulls() {
            self.values.iter().enumerate().filter_map(|(i, v)| {
                self.nulls.is_valid(i).then_some(*v)
            }).max()
        } else {
            self.values.iter().max().copied()
        }
    }

    pub fn min(&self) -> Option<T> {
        if self.nulls.has_nulls() {
            self.values.iter().enumerate().filter_map(|(i, v)| {
                self.nulls.is_valid(i).then_some(*v)
            }).min()
        } else {
            self.values.iter().min().copied()
        }
    }
}