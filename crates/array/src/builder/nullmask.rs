use crate::builder::bitmask::BitmaskBuilder;
use crate::index::RangeList;
use crate::slice::nullmask::NullmaskSlice;
use crate::util::bit_tools;
use crate::writer::BitmaskWriter;
use arrow_buffer::NullBuffer;


pub struct NullmaskBuilder {
    nulls: BitmaskBuilder,
    len: usize,
    capacity: usize,
    has_nulls: bool
}


impl NullmaskBuilder {
    pub fn new(capacity: usize) -> Self {
        Self {
            nulls: BitmaskBuilder::new(0),
            len: 0,
            capacity,
            has_nulls: false
        }
    }

    pub fn byte_size(&self) -> usize {
        self.nulls.bytes_size()
    }
    
    pub fn len(&self) -> usize {
        if self.has_nulls {
            self.nulls.len()
        } else {
            self.len
        }
    }

    pub fn clear(&mut self) {
        self.nulls.clear();
        self.len = 0;
        self.has_nulls = false
    }
    
    pub fn append_slice(&mut self, data: &[u8], offset: usize, len: usize) {
        if self.has_nulls {
            self.nulls.append_slice(data, offset, len);
        } else {
            if bit_tools::all_valid(data, offset, len) {
                self.len += len
            } else {
                self.init_nulls(len);
                self.nulls.append_slice(data, offset, len);
            }
        }
    }

    pub fn append_slice_indexes(&mut self, data: &[u8], indexes: impl Iterator<Item=usize> + Clone) {
        if self.has_nulls {
            self.nulls.append_slice_indexes(data, indexes);
        } else {
            if let Some(len) = bit_tools::all_indexes_valid(data, indexes.clone()) {
                self.len += len
            } else {
                self.init_nulls(indexes.size_hint().0);
                self.nulls.append_slice_indexes(data, indexes);
            }
        }
    }

    pub fn append_slice_ranges(&mut self, data: &[u8], ranges: &mut impl RangeList) {
        if self.has_nulls {
            self.nulls.append_slice_ranges(data, ranges);
        } else {
            if let Some(len) = bit_tools::all_ranges_valid(data, ranges.iter()) {
                self.len += len;
            } else {
                self.init_nulls(ranges.span());
                self.nulls.append_slice_ranges(data, ranges);
            }
        }
    }
    
    pub fn append_many(&mut self, val: bool, count: usize) {
        if count == 0 {
            return;
        }
        match (self.has_nulls, val) {
            (true, val) => self.nulls.append_many(val, count),
            (false, true) => {
                self.len += count
            }, 
            (false, false) => {
                self.init_nulls(count);
                self.nulls.append_many(false, count)
            }
        }
    }
    
    #[inline]
    pub fn append(&mut self, val: bool) {
        match (self.has_nulls, val) { 
            (true, val) => self.nulls.append(val),
            (false, true) => {
                self.len += 1;
            },
            (false, false) => {
                self.init_nulls(1);
                self.nulls.append(false)
            }
        }
    }
    
    fn init_nulls(&mut self, additional: usize) {
        let cap = std::cmp::max(self.capacity, self.len + additional);
        self.nulls.reserve(cap);
        self.nulls.append_many(true, self.len);
        self.has_nulls = true
    }
    
    pub fn finish(self) -> Option<NullBuffer> {
        self.has_nulls.then(|| NullBuffer::new(self.nulls.finish()))
    }
    
    pub fn as_slice(&self) -> NullmaskSlice<'_> {
        if self.has_nulls {
            NullmaskSlice::new(self.nulls.len(), Some(self.nulls.as_slice()))
        } else {
            NullmaskSlice::new(self.len, None)
        }
    }
}


impl BitmaskWriter for NullmaskBuilder {
    #[inline]
    fn write_slice(&mut self, data: &[u8], offset: usize, len: usize) -> anyhow::Result<()> {
        self.append_slice(data, offset, len);
        Ok(())
    }

    #[inline]
    fn write_slice_indexes(
        &mut self,
        data: &[u8],
        indexes: impl Iterator<Item=usize> + Clone
    ) -> anyhow::Result<()>
    {
        self.append_slice_indexes(data, indexes);
        Ok(())
    }

    #[inline]
    fn write_slice_ranges(&mut self, data: &[u8], ranges: &mut impl RangeList) -> anyhow::Result<()> {
        self.append_slice_ranges(data, ranges);
        Ok(())
    }

    #[inline]
    fn write_many(&mut self, val: bool, count: usize) -> anyhow::Result<()> {
        self.append_many(val, count);
        Ok(())
    }
}