use std::fmt::{Debug};


pub const LEN_BYTES: usize = 8;


pub fn encode_len<I>(i: I) -> [u8; LEN_BYTES]
    where I: TryInto<u64>,
          I::Error: Debug
{
    i.try_into().unwrap().to_le_bytes()
}


static PADDING: [u8; 64] = [0; 64];


pub struct PageWriter<'a> {
    pub buf: &'a mut Vec<u8>,
    offset: usize,
    buffer_mark: usize
}


impl <'a> PageWriter<'a> {
    pub fn new(buf: &'a mut Vec<u8>) -> Self {
        let offset = buf.len();
        Self {
            buf,
            offset,
            buffer_mark: offset
        }
    }

    pub fn pad(&mut self) {
        let len = (self.buf.len() - self.buffer_mark) % PADDING.len();
        if len > 0 {
            self.buf.extend_from_slice(&PADDING[len..])
        }
        self.buffer_mark = self.buf.len()
    }

    pub fn append_index(&mut self, index: usize) {
        self.buf.extend_from_slice(&encode_len(index))
    }

    pub fn set_index(&mut self, i: usize, index: usize) {
        let offset = self.offset + i * LEN_BYTES;
        self.buf[offset..offset + LEN_BYTES].copy_from_slice(&encode_len(index))
    }

    pub fn reserve_index_section(&mut self, size: usize) {
        self.buf.extend(std::iter::repeat(0).take(LEN_BYTES * size))
    }

    pub fn set_buffer_index(&mut self, i: usize) {
        self.set_index(i, self.buf.len() - self.buffer_mark)
    }
}