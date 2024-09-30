use std::ops::Range;
use crate::slice::{BitmaskSlice, Slice};
use crate::slice::nullmask::NullmaskSlice;
use crate::writer::{ArrayWriter, RangeList};


#[derive(Clone)]
pub struct BooleanSlice<'a> {
    nulls: NullmaskSlice<'a>,
    values: BitmaskSlice<'a>
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

    fn write_indexes(&self, dst: &mut impl ArrayWriter, indexes: impl IntoIterator<Item=usize, IntoIter: Clone>) -> anyhow::Result<()> {
        let indexes = indexes.into_iter();
        self.nulls.write_indexes(dst.nullmask(0), indexes.clone())?;
        self.values.write_indexes(dst.bitmask(1), indexes)
    }
}