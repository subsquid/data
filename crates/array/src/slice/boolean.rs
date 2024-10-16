use crate::slice::bitmask::BitmaskSlice;
use crate::slice::nullmask::NullmaskSlice;
use crate::slice::Slice;
use crate::writer::{ArrayWriter, RangeList};
use arrow::array::BooleanArray;
use std::ops::Range;
use crate::access::Access;


#[derive(Clone)]
pub struct BooleanSlice<'a> {
    nulls: NullmaskSlice<'a>,
    values: BitmaskSlice<'a>
}


impl <'a> BooleanSlice<'a> {
    pub fn new(values: BitmaskSlice<'a>, nulls: Option<BitmaskSlice<'a>>) -> Self {
        Self {
            nulls: NullmaskSlice::new(values.len(), nulls),
            values
        }
    }
    
    pub fn with_nullmask(values: BitmaskSlice<'a>, nulls: NullmaskSlice<'a>) -> Self {
        assert_eq!(values.len(), nulls.len());
        Self {
            nulls,
            values
        }
    }
}


impl <'a> Slice for BooleanSlice<'a> {
    fn num_buffers(&self) -> usize {
        2
    }

    fn len(&self) -> usize {
        self.values.len()
    }

    fn slice(&self, offset: usize, len: usize) -> Self {
        Self {
            nulls: self.nulls.slice(offset, len),
            values: self.values.slice(offset, len)
        }
    }

    fn write(&self, dst: &mut impl ArrayWriter) -> anyhow::Result<()> {
        self.nulls.write(dst.nullmask(0))?;
        self.values.write(dst.bitmask(1))
    }

    fn write_range(&self, dst: &mut impl ArrayWriter, range: Range<usize>) -> anyhow::Result<()> {
        self.nulls.write_range(dst.nullmask(0), range.clone())?;
        self.values.write_range(dst.bitmask(1), range)
    }

    fn write_ranges(&self, dst: &mut impl ArrayWriter, ranges: &mut impl RangeList) -> anyhow::Result<()> {
        self.nulls.write_ranges(dst.nullmask(0), ranges)?;
        self.values.write_ranges(dst.bitmask(1), ranges)
    }

    fn write_indexes(&self, dst: &mut impl ArrayWriter, indexes: impl Iterator<Item = usize> + Clone) -> anyhow::Result<()> {
        self.nulls.write_indexes(dst.nullmask(0), indexes.clone())?;
        self.values.write_indexes(dst.bitmask(1), indexes)
    }
}


impl <'a> From<&'a BooleanArray> for BooleanSlice<'a> {
    fn from(value: &'a BooleanArray) -> Self {
        Self {
            nulls: NullmaskSlice::from_array(value),
            values: value.values().into()
        }
    }
}


impl <'a> Access for BooleanSlice<'a> {
    type Value = bool;

    #[inline]
    fn get(&self, i: usize) -> Self::Value {
        self.values.value(i)
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