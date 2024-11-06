use crate::index::RangeList;
use crate::slice::bitmask::BitmaskSlice;
use crate::writer::BitmaskWriter;
use arrow_buffer::{bit_mask, bit_util, BooleanBuffer, MutableBuffer};


pub struct BitmaskBuilder {
    buffer: MutableBuffer,
    len: usize,
}


impl BitmaskBuilder {
    pub fn new(capacity: usize) -> Self {
        Self {
            buffer: MutableBuffer::new(bit_util::ceil(capacity, 8)),
            len: 0
        }
    }

    pub fn bytes_size(&self) -> usize {
        bit_util::ceil(self.len, 8)
    }

    pub fn clear(&mut self) {
        self.buffer.clear();
        self.len = 0
    }
    
    pub fn reserve(&mut self, additional: usize) {
        let new_byte_len = bit_util::ceil(self.len + additional, 8);
        self.buffer.reserve(new_byte_len - self.buffer.len())
    }
    
    #[inline]
    fn resize(&mut self, additional: usize) {
        let new_byte_len = bit_util::ceil(self.len + additional, 8);
        self.buffer.resize(new_byte_len, 0)
    }

    pub fn append_slice(&mut self, data: &[u8], offset: usize, len: usize) {
        self.resize(len);

        bit_mask::set_bits(
            self.buffer.as_slice_mut(),
            data,
            self.len,
            offset,
            len
        );

        self.len += len
    }

    pub fn append_slice_indexes(&mut self, data: &[u8], mut indexes: impl Iterator<Item=usize>) {
        let (min_bit_len, _) = indexes.size_hint();
        self.resize(min_bit_len);

        while self.len < self.buffer.len() * 8 {
            if let Some(i) = indexes.next() {
                if bit_util::get_bit(data, i) {
                    unsafe { bit_util::set_bit_raw(self.buffer.as_mut_ptr(), self.len) };
                }
                self.len += 1;
            } else {
                return;
            }
        }

        for i in indexes {
            self.append(bit_util::get_bit(data, i))
        }
    }
    
    pub fn append_slice_ranges(&mut self, data: &[u8], ranges: &mut impl RangeList) {
        self.resize(ranges.span());
        
        for r in ranges.iter() {
            bit_mask::set_bits(
                self.buffer.as_slice_mut(),
                data,
                self.len,
                r.start,
                r.len()
            );
            self.len += r.len();
        }
    }

    pub fn append_many(&mut self, val: bool, count: usize) {
        let new_len = self.len + count;
        let new_len_bytes = bit_util::ceil(new_len, 8);
        if val {
            let cur_remainder = self.len % 8;
            let new_remainder = new_len % 8;

            if cur_remainder != 0 {
                // Pad last byte with 1s
                *self.buffer.as_slice_mut().last_mut().unwrap() |= !((1 << cur_remainder) - 1)
            }
            
            self.buffer.truncate(bit_util::ceil(self.len, 8));
            self.buffer.resize(new_len_bytes, 0xFF);
            
            if new_remainder != 0 {
                // Clear remaining bits
                *self.buffer.as_slice_mut().last_mut().unwrap() &= (1 << new_remainder) - 1
            }
        } else if new_len_bytes > self.buffer.len() {
            self.buffer.resize(new_len_bytes, 0);
        }
        self.len = new_len;
    }

    pub fn append(&mut self, val: bool) {
        self.resize(1);
        if val {
            unsafe { bit_util::set_bit_raw(self.buffer.as_mut_ptr(), self.len) };
        }
        self.len += 1
    }

    pub fn data(&self) -> &[u8] {
        &self.buffer
    }
    
    pub fn as_slice(&self) -> BitmaskSlice<'_> {
        BitmaskSlice::new(self.data(), 0, self.len)
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn shift(&mut self, len: usize) {
        if len == 0 {
            return;
        }
        assert!(len <= self.len);
        assert_eq!(len % 8, 0, "only byte aligned shifts are allowed");
        let byte_len = len / 8;
        let new_byte_len = self.buffer.len() - byte_len;
        if new_byte_len > 0 {
            self.buffer.as_slice_mut().copy_within(byte_len.., 0);
        }
        self.buffer.truncate(new_byte_len);
        self.len -= len;
    }

    pub fn finish(self) -> BooleanBuffer {
        BooleanBuffer::new(self.buffer.into(), 0, self.len)
    }
}


impl BitmaskWriter for BitmaskBuilder {
    #[inline]
    fn write_slice(&mut self, data: &[u8], offset: usize, len: usize) -> anyhow::Result<()> {
        self.append_slice(data, offset, len);
        Ok(())
    }

    #[inline]
    fn write_slice_indexes(&mut self, data: &[u8], indexes: impl Iterator<Item=usize>) -> anyhow::Result<()> {
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