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
        todo!()
    }

    fn write(&self, dst: &mut impl ArrayWriter) -> anyhow::Result<()> {
        todo!()
    }

    fn write_range(&self, dst: &mut impl ArrayWriter, range: Range<usize>) -> anyhow::Result<()> {
        todo!()
    }

    fn write_ranges(&self, dst: &mut impl ArrayWriter, ranges: &mut impl RangeList) -> anyhow::Result<()> {
        todo!()
    }

    fn write_indexes(&self, dst: &mut impl ArrayWriter, indexes: impl IntoIterator<Item=usize, IntoIter: Clone>) -> anyhow::Result<()> {
        todo!()
    }
}