use std::fmt::{Debug};
use anyhow::{Context, ensure};
use crate::bitmask::{BitSlice, read_bit_page};
use crate::Slice;


pub const LEN_BYTES: usize = 8;


pub fn encode_index<I>(i: I) -> [u8; LEN_BYTES]
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
        self.buf.extend_from_slice(&encode_index(index))
    }

    pub fn set_index(&mut self, i: usize, index: usize) {
        let offset = self.offset + i * LEN_BYTES;
        self.buf[offset..offset + LEN_BYTES].copy_from_slice(&encode_index(index))
    }

    pub fn reserve_index_section(&mut self, size: usize) {
        self.buf.extend(std::iter::repeat(0).take(LEN_BYTES * size))
    }

    pub fn set_buffer_index(&mut self, i: usize) {
        self.set_index(i, self.buf.len() - self.buffer_mark)
    }
}


pub fn read_index(bytes: &[u8], i: usize) -> anyhow::Result<usize> {
    let beg = i * LEN_BYTES;
    let end = beg + LEN_BYTES;
    ensure!(end < bytes.len());
    let index = u64::from_le_bytes(bytes[beg..end].try_into().unwrap());
    Ok(index as usize)
}


pub struct PageReader<'a> {
    bytes: &'a [u8],
    index_section_len: usize,
    index: usize,
    bytes_pos: usize,
    num_buffers: usize
}


impl <'a> PageReader<'a> {
    pub fn new(bytes: &'a [u8], num_buffers: Option<usize>) -> anyhow::Result<Self> {
        let (index_section_len, num_buffers, index) = if let Some(num_buffers) = num_buffers {
            (num_buffers.saturating_sub(1), num_buffers, 0)
        } else {
            let num_buffers = read_index(bytes, 0)?;
            (1 + num_buffers.saturating_sub(1), num_buffers, 1)
        };
        ensure!(bytes.len() >= index_section_len);
        Ok(Self {
            bytes,
            index_section_len,
            index,
            bytes_pos: index_section_len * LEN_BYTES,
            num_buffers
        })
    }

    pub fn read_null_mask(&mut self) -> anyhow::Result<Option<BitSlice<'a>>> {
        let (mask, byte_len) = read_bit_page(&self.bytes[self.bytes_pos..], false)
            .context("failed to read null mask")?;

        self.bytes_pos += byte_len;
        self.pad();

        Ok(if mask.len() == 0 {
            None
        } else {
            Some(mask)
        })
    }
    
    pub fn buffers_left(&self) -> usize {
        self.num_buffers
    }

    pub fn read_next_buffer(&mut self) -> anyhow::Result<&'a [u8]> {
        assert!(self.num_buffers > 0);
        Ok(if self.index < self.index_section_len {
            let len = read_index(self.bytes, self.index)?;
            let beg = self.bytes_pos;
            let end = self.bytes_pos + len;
            ensure!(self.bytes.len() >= end);
            self.index += 1;
            self.bytes_pos = end;
            self.pad();
            self.num_buffers -= 1;
            &self.bytes[beg..end]
        } else {
            self.num_buffers -= 1;
            &self.bytes[self.bytes_pos..]
        })
    }

    fn pad(&mut self) {
        let extra = self.bytes_pos % PADDING.len();
        if extra > 0 {
            self.bytes_pos += PADDING.len() - extra;
        }
    }
}


macro_rules! assert_data_type {
    ($dt:expr, $pat:pat) => {
        if let Some(data_type) = $dt.as_ref() {
            match data_type {
                $pat => {},
                ty => panic!("got unexpected data type - {}", ty)
            }
        };
    };
}
pub(crate) use assert_data_type;