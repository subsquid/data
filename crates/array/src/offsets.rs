use crate::util::validate_offsets;
use std::ops::Range;
use arrow_buffer::OffsetBuffer;


#[derive(Copy, Clone)]
pub struct Offsets<'a> {
    offsets: &'a [i32]
}


impl <'a> Offsets<'a> {
    pub fn new(offsets: &'a [i32]) -> Self {
        Self::try_new(offsets).unwrap()
    }
    
    pub fn try_new(offsets: &'a [i32]) -> Result<Self, &'static str> {
        validate_offsets(offsets, 0)?;
        Ok(Self { 
            offsets 
        })
    }

    #[inline]
    pub unsafe fn new_unchecked(offsets: &'a [i32]) -> Self {
        Self { offsets }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.offsets.len() - 1
    }

    #[inline]
    pub fn first_offset(&self) -> i32 {
        self.offsets[0]
    }

    #[inline]
    pub fn last_offset(&self) -> i32 {
        self.offsets[self.len()]
    }

    #[inline]
    pub fn last_index(&self) -> usize {
        self.last_offset() as usize
    }

    #[inline]
    pub fn slice(&self, offset: usize, len: usize) -> Self {
        Self {
            offsets: &self.offsets[offset..offset + len + 1]
        }
    }

    #[inline]
    pub fn slice_by_range(&self, range: Range<usize>) -> Self {
        Self {
            offsets: &self.offsets[range.start..range.end + 1]
        }
    }

    #[inline]
    pub fn index(&self, i: usize) -> usize {
        self.offsets[i] as usize
    }

    #[inline]
    pub fn range(&self) -> Range<usize> {
        self.offsets[0] as usize .. self.last_index()
    }
    
    #[inline]
    pub fn at(&self, i: usize) -> i32 {
        self.offsets[i]
    }

    #[inline]
    pub fn values(&self) -> &[i32] {
        self.offsets
    }
}


impl <'a> From<&'a OffsetBuffer<i32>> for Offsets<'a> {
    fn from(value: &'a OffsetBuffer<i32>) -> Self {
        Self {
            offsets: value.inner()
        }
    }
}