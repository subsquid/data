use arrow_buffer::{bit_mask, bit_util, MutableBuffer};
use crate::data_builder::BitmaskWriter;


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

    pub fn append_packed_bits(&mut self, data: &[u8], offset: usize, len: usize) {
        let new_byte_len = bit_util::ceil(self.len + len, 8);
        if new_byte_len > self.buffer.len() {
            self.buffer.resize(new_byte_len, 0)
        }

        bit_mask::set_bits(
            self.buffer.as_slice_mut(),
            data,
            self.len,
            offset,
            len
        );

        self.len += len
    }

    pub fn append_many(&mut self, val: bool, count: usize) {
        let new_len = self.len + count;
        let new_len_bytes = bit_util::ceil(new_len, 8);
        match val {
            true => {
                let cur_remainder = self.len % 8;
                let new_remainder = new_len % 8;

                if cur_remainder != 0 {
                    // Pad last byte with 1s
                    *self.buffer.as_slice_mut().last_mut().unwrap() |= !((1 << cur_remainder) - 1)
                }
                self.buffer.resize(new_len_bytes, 0xFF);
                if new_remainder != 0 {
                    // Clear remaining bits
                    *self.buffer.as_slice_mut().last_mut().unwrap() &= (1 << new_remainder) - 1
                }
            },
            false => {
                if new_len_bytes > self.buffer.len() {
                    self.buffer.resize(new_len_bytes, 0);
                }
            }
        }
        self.len = new_len;
    }

    pub fn append(&mut self, val: bool) {
        let new_len = self.len + 1;
        let new_len_bytes = bit_util::ceil(new_len, 8);
        if new_len_bytes > self.buffer.len() {
            self.buffer.resize(new_len_bytes, 0);
        }
        if val {
            unsafe { bit_util::set_bit_raw(self.buffer.as_mut_ptr(), self.len) };
        }
        self.len = new_len
    }

    pub fn data(&self) -> &[u8] {
        &self.buffer
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn shift(&mut self, byte_offset: usize) {
        assert!(byte_offset * 8 <= self.len);
        let new_byte_len = self.buffer.len() - byte_offset;
        if new_byte_len > 0 {
            let bytes = self.buffer.as_slice_mut();
            bytes.copy_within(byte_offset.., 0);
        }
        self.buffer.truncate(new_byte_len);
        self.len = self.len - byte_offset * 8;
    }
}


impl BitmaskWriter for BitmaskBuilder {
    fn write_packed_bits(&mut self, data: &[u8], offset: usize, len: usize) {
        self.append_packed_bits(data, offset, len)
    }
    
    fn write_many(&mut self, val: bool, count: usize) {
        self.append_many(val, count)
    }
}