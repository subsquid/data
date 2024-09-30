use crate::array_builder::bitmask::BitmaskBuilder;
use crate::writer::{BitmaskWriter, RangeList};
use arrow_buffer::bit_chunk_iterator::UnalignedBitChunk;
use arrow_buffer::bit_util;


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
            if is_all_valid(data, offset, len) {
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
            let mut all_valid = true;
            let mut len = 0;
            for i in indexes.clone() {
                if !bit_util::get_bit(data, i) {
                    all_valid = false;
                    break
                }
                len += 1
            }
            if all_valid {
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
            let mut all_valid = true;
            let mut len = 0;
            for r in ranges.iter() {
                if !is_all_valid(data, r.start, r.len()) {
                    all_valid = false;
                    break
                }
                len += r.len()
            }
            if all_valid {
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


fn is_all_valid(data: &[u8], offset: usize, len: usize) -> bool {
    // TODO: optimize
    UnalignedBitChunk::new(data, offset, len).count_ones() == len
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