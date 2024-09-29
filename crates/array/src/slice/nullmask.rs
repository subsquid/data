use crate::slice::bitmask::BitmaskSlice;
use crate::writer::{BitmaskWriter, RangeList};
use std::ops::Range;


#[derive(Clone)]
pub struct NullmaskSlice<'a> {
    nulls: Option<BitmaskSlice<'a>>,
    len: usize
}


impl<'a> NullmaskSlice<'a> {
    pub fn len(&self) -> usize {
        self.len
    }
    
    pub fn slice(&self, offset: usize, len: usize) -> Self {
        assert!(offset + len <= self.len);
        Self {
            nulls: self.nulls.as_ref().map(|nulls| nulls.slice(offset, len)),
            len
        }
    }
    
    #[inline]
    pub fn is_valid(&self, i: usize) -> bool {
        assert!(i < self.len);
        self.nulls.as_ref().map(|nulls| nulls.value(i)).unwrap_or(true)
    }
    
    pub fn write(&self, dst: &mut impl BitmaskWriter) -> anyhow::Result<()> {
        if let Some(nulls) = self.nulls.as_ref() {
            nulls.write(dst)
        } else {
            dst.write_many(true, self.len)
        }
    }

    pub fn write_range(&self, dst: &mut impl BitmaskWriter, range: Range<usize>) -> anyhow::Result<()> {
        if range.is_empty() {
            return Ok(())
        }
        if let Some(nulls) = self.nulls.as_ref() {
            nulls.write_range(dst, range)
        } else {
            dst.write_many(true, range.len())
        }
    }
    
    pub fn write_ranges(&self, dst: &mut impl BitmaskWriter, ranges: &mut impl RangeList) -> anyhow::Result<()> {
        if let Some(nulls) = self.nulls.as_ref() {
            nulls.write_ranges(dst, ranges)
        } else {
            dst.write_many(true, ranges.size())
        }
    }

    pub fn write_indexes(
        &self,
        dst: &mut impl BitmaskWriter,
        indexes: impl Iterator<Item = usize>
    ) -> anyhow::Result<()>
    {
        if let Some(nulls) = self.nulls.as_ref() {
            nulls.write_indexes(dst, indexes)
        } else {
            dst.write_many(true, indexes.count())
        }
    }
}