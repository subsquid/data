use crate::writer::{BitmaskWriter, RangeList};
use arrow_buffer::bit_chunk_iterator::BitChunks;
use arrow_buffer::{bit_util, ToByteSlice};
use std::io::Write;


pub struct BitmaskIOWriter<W> {
    writer: W,
    buf: u64,
    len: usize
}


impl<W> BitmaskIOWriter<W> {
    pub fn new(writer: W) -> Self {
        Self {
            writer,
            buf: 0,
            len: 0
        }
    }
    
    #[inline]
    fn buf_mut_prt(&mut self) -> *mut u8 {
        std::ptr::from_mut(&mut self.buf).cast()
    }
}


impl <W: Write> BitmaskWriter for BitmaskIOWriter<W> {
    fn write_slice(&mut self, data: &[u8], mut offset: usize, mut len: usize) -> anyhow::Result<()> {
        if self.len > 0 {
            let to_set = std::cmp::min(64 - self.len, len);
            assert!(data.len() >= bit_util::ceil(offset + len, 8));
            unsafe {
                set_bits_slow(self.buf_mut_prt(), self.len, data.as_ptr(), offset, to_set);
            }
            self.len += to_set;
            if self.len == 64 {
                self.writer.write_all(self.buf.to_byte_slice())?;
                self.buf = 0;
                self.len = 0;
            } else {
                return Ok(())
            }
            offset += to_set;
            len -= to_set;
        }

        if len == 0 {
            return Ok(())
        }

        let bit_chunks = BitChunks::new(data, offset, len);
        for chunk in bit_chunks.iter() {
            self.writer.write_all(chunk.to_byte_slice())?;
        }
        
        self.buf = bit_chunks.remainder_bits();
        self.len = bit_chunks.remainder_len();
        
        Ok(())
    }

    fn write_slice_indexes(&mut self, data: &[u8], mut indexes: impl Iterator<Item = usize>) -> anyhow::Result<()> {
        loop {
            while self.len < 64 {
                if let Some(i) = indexes.next() {
                    if bit_util::get_bit(data, i) {
                        unsafe {
                            bit_util::set_bit_raw(self.buf_mut_prt(), self.len);
                        }
                    }
                    self.len += 1;
                } else {
                    return Ok(())
                }
            }
            self.writer.write_all(self.buf.to_byte_slice())?;
            self.buf = 0;
            self.len = 0;
        }
    }

    fn write_slice_ranges(&mut self, data: &[u8], ranges: &mut impl RangeList) -> anyhow::Result<()> {
        for r in ranges.iter() {
            self.write_slice(data, r.start, r.len())?;
        }
        Ok(())
    }

    fn write_many(&mut self, val: bool, mut count: usize) -> anyhow::Result<()> {
        let ones: u64 = 0xffffffffffffffff;
        
        if self.len > 0 {
            let to_set = std::cmp::min(64 - self.len, count);
            let new_len = self.len + to_set;
            
            if val {
                self.buf |= ones >> self.len;
                self.buf &= ones << (64 - new_len);
            }
            
            if new_len == 64 {
                self.writer.write_all(self.buf.to_byte_slice())?;
                self.buf = 0;
                self.len = 0;
                count -= to_set;
            } else {
                self.len = new_len;
                return Ok(())
            }
        }

        if val {
            while count >= 64 {
                self.writer.write_all(ones.to_byte_slice())?;
                count -= 64;
            }
            if count > 0 {
                self.len = count;
                self.buf = ones << (64 - count);
            }
        } else {
            while count >= 64 {
                self.writer.write_all(0u64.to_byte_slice())?;
                count -= 64;
            }
            self.len = count;
        }
        
        Ok(())
    }
}


unsafe fn set_bits_slow(dst: *mut u8, dst_offset: usize, data: *const u8, offset: usize, len: usize) {
    for i in 0..len {
        if bit_util::get_bit_raw(data, offset + 1) {
            bit_util::set_bit_raw(dst, dst_offset + i)
        }
    }
}