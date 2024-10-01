use crate::array_builder::bitmask::BitmaskBuilder;
use crate::util::bit_tools;
use crate::writer::{BitmaskWriter, RangeList};


pub struct NullmaskBuilder {
    nulls: Option<BitmaskBuilder>,
    len: usize,
    capacity: usize
}


impl NullmaskBuilder {
    pub fn new(capacity: usize) -> Self {
        Self {
            nulls: None,
            len: 0,
            capacity
        }
    }
    
    pub fn append_slice(&mut self, data: &[u8], offset: usize, len: usize) {
        if let Some(nulls) = self.nulls.as_mut() {
            nulls.append_slice(data, offset, len);
            self.len = nulls.len()
        } else {
            if bit_tools::all_valid(data, offset, len) {
                self.len += len
            } else {
                let mut nulls = self.make_nulls(len);
                nulls.append_slice(data, offset, len);
                self.len = nulls.len();
                self.nulls = Some(nulls);
            }
        }
    }

    pub fn append_slice_indexes(&mut self, data: &[u8], indexes: impl Iterator<Item=usize> + Clone) {
        if let Some(nulls) = self.nulls.as_mut() {
            nulls.append_slice_indexes(data, indexes);
            self.len = nulls.len()
        } else {
            if let Some(len) = bit_tools::all_indexes_valid(data, indexes.clone()) {
                self.len += len
            } else {
                let mut nulls = self.make_nulls(indexes.size_hint().0);
                nulls.append_slice_indexes(data, indexes);
                self.len = nulls.len();
                self.nulls = Some(nulls)
            }
        }
    }

    pub fn append_slice_ranges(&mut self, data: &[u8], ranges: &mut impl RangeList) {
        if let Some(nulls) = self.nulls.as_mut() {
            nulls.append_slice_ranges(data, ranges);
            self.len = nulls.len()
        } else {
            if let Some(len) = bit_tools::all_ranges_valid(data, ranges.iter()) {
                self.len += len;
            } else {
                let mut nulls = self.make_nulls(ranges.size());
                nulls.append_slice_ranges(data, ranges);
                self.len = nulls.len();
                self.nulls = Some(nulls)
            }
        }
    }
    
    pub fn append_many(&mut self, val: bool, count: usize) {
        if count == 0 {
            return;
        }
        match (self.nulls.as_mut(), val) {
            (Some(nulls), val) => nulls.append_many(val, count),
            (None, true) => {
                self.len += count
            }, 
            (None, false) => {
                let mut nulls = self.make_nulls(count);
                nulls.append_many(false, count);
                self.len = nulls.len();
                self.nulls = Some(nulls);
            }, 
        }
    }
    
    #[inline]
    pub fn append(&mut self, val: bool) {
        match (self.nulls.as_mut(), val) { 
            (Some(nulls), val) => nulls.append(val),
            (None, true) => {
                self.len += 1;
            },
            (None, false) => {
                let mut nulls = self.make_nulls(1);
                nulls.append(false);
                self.len += 1;
                self.nulls = Some(nulls);
            },
        }
    }
    
    fn make_nulls(&self, additional: usize) -> BitmaskBuilder {
        let cap = std::cmp::max(self.capacity, self.len + additional);
        let mut nulls = BitmaskBuilder::new(cap);
        nulls.append_many(true, self.len);
        nulls
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