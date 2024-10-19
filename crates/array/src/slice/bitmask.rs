use crate::index::{IndexList, RangeList};
use crate::writer::BitmaskWriter;
use arrow_buffer::{bit_util, BooleanBuffer};
use std::ops::Range;


#[derive(Clone)]
pub struct BitmaskSlice<'a> {
    data: &'a [u8],
    offset: usize,
    len: usize
}


impl<'a> BitmaskSlice<'a> {
    pub fn new(data: &'a [u8], offset: usize, len: usize) -> Self {
        assert!(offset + len <= data.len() * 8);
        Self {
            data,
            offset,
            len
        }
    }
    
    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }
    
    pub fn slice(&self, offset: usize, len: usize) -> Self {
        assert!(offset + len <= self.len);
        Self {
            data: self.data,
            offset: self.offset + offset,
            len
        }
    }
    
    #[inline]
    pub fn value(&self, i: usize) -> bool {
        assert!(self.offset + i < self.len);
        // SAFETY: bounds should be guaranteed by construction and the above assertion
        unsafe {
            bit_util::get_bit_raw(self.data.as_ptr(), self.offset + i)
        }
    }

    pub fn write(&self, dst: &mut impl BitmaskWriter) -> anyhow::Result<()> {
        dst.write_slice(self.data, self.offset, self.len)
    }

    pub fn write_range(&self, dst: &mut impl BitmaskWriter, range: Range<usize>) -> anyhow::Result<()> {
        if range.is_empty() {
            return Ok(())
        }
        dst.write_slice(self.data, self.offset + range.start, range.len())
    }
    
    pub fn write_ranges(
        &self, 
        dst: &mut impl BitmaskWriter, 
        ranges: &mut impl RangeList
    ) -> anyhow::Result<()> 
    {
        dst.write_slice_ranges(self.data, &mut ranges.shift(self.offset, self.len))
    }

    pub fn write_indexes(
        &self,
        dst: &mut impl BitmaskWriter,
        indexes: &(impl IndexList + ?Sized)
    ) -> anyhow::Result<()>
    {
        dst.write_slice_indexes(
            &self.data, 
            indexes.shift(self.offset, self.len).index_iter()
        )
    }
}


impl<'a> From<&'a BooleanBuffer> for BitmaskSlice<'a> {
    fn from(value: &'a BooleanBuffer) -> Self {
        Self {
            data: value.values(),
            offset: value.offset(),
            len: value.len()
        }
    }
}