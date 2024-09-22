use arrow_buffer::bit_chunk_iterator::UnalignedBitChunk;
use crate::array_builder::bitmask::BitmaskBuilder;
use crate::data_builder::BitmaskWriter;


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
    
    pub fn append_packed_nulls(&mut self, data: &[u8], offset: usize, len: usize) {
        if let Some(nulls) = self.nulls.as_mut() {
            nulls.append_packed_bits(data, offset, len);
            self.len = nulls.len()
        } else {
            // TODO: optimize all_valid check
            let all_valid = UnalignedBitChunk::new(data, offset, len).count_ones() == len;
            if all_valid {
                self.len += len
            } else {
                let mut nulls = self.make_nulls(len);
                nulls.append_packed_bits(data, offset, len);
                self.len = nulls.len();
                self.nulls = Some(nulls);
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
    fn write_packed_bits(&mut self, data: &[u8], offset: usize, len: usize) {
        self.append_packed_nulls(data, offset, len)
    }

    fn write_many(&mut self, val: bool, count: usize) {
        self.append_many(val, count)
    }
}