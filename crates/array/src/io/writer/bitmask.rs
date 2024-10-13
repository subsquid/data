use crate::writer::{BitmaskWriter, RangeList};
use arrow_buffer::bit_chunk_iterator::BitChunks;
use arrow_buffer::{bit_util, ToByteSlice};
use std::io::Write;


pub struct BitmaskIOWriter<W> {
    write: W,
    buf: u64,
    buf_len: usize,
    len: usize
}


impl <W> BitmaskIOWriter<W> {
    pub fn new(writer: W) -> Self {
        Self {
            write: writer,
            buf: 0,
            buf_len: 0,
            len: 0
        }
    }
    
    pub fn into_write(self) -> W {
        self.write
    }
    
    #[inline]
    fn buf_mut_prt(&mut self) -> *mut u8 {
        std::ptr::from_mut(&mut self.buf).cast()
    }
}


impl <W: Write> BitmaskWriter for BitmaskIOWriter<W> {
    fn write_slice(&mut self, data: &[u8], mut offset: usize, mut len: usize) -> anyhow::Result<()> {
        assert!(data.len() >= bit_util::ceil(offset + len, 8));
        
        self.len += len;
        
        if self.buf_len > 0 {
            let to_set = std::cmp::min(64 - self.buf_len, len);
            unsafe {
                set_bits_slow(self.buf_mut_prt(), self.buf_len, data.as_ptr(), offset, to_set);
            }
            self.buf_len += to_set;
            if self.buf_len == 64 {
                self.write.write_all(self.buf.to_byte_slice())?;
                self.buf = 0;
                self.buf_len = 0;
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
            self.write.write_all(chunk.to_byte_slice())?;
        }
        
        self.buf = bit_chunks.remainder_bits();
        self.buf_len = bit_chunks.remainder_len();
        
        Ok(())
    }

    fn write_slice_indexes(&mut self, data: &[u8], mut indexes: impl Iterator<Item = usize>) -> anyhow::Result<()> {
        loop {
            while self.buf_len < 64 {
                if let Some(i) = indexes.next() {
                    if bit_util::get_bit(data, i) {
                        unsafe {
                            bit_util::set_bit_raw(self.buf_mut_prt(), self.buf_len);
                        }
                    }
                    self.buf_len += 1;
                    self.len += 1;
                } else {
                    return Ok(())
                }
            }
            self.write.write_all(self.buf.to_byte_slice())?;
            self.buf = 0;
            self.buf_len = 0;
        }
    }

    fn write_slice_ranges(&mut self, data: &[u8], ranges: &mut impl RangeList) -> anyhow::Result<()> {
        for r in ranges.iter() {
            self.write_slice(data, r.start, r.len())?;
        }
        Ok(())
    }

    fn write_many(&mut self, val: bool, mut count: usize) -> anyhow::Result<()> {
        self.len += count;
        
        let ones: u64 = 0xffffffffffffffff;
        
        if self.buf_len > 0 {
            let to_set = std::cmp::min(64 - self.buf_len, count);
            let new_len = self.buf_len + to_set;
            
            if val {
                self.buf |= ones >> self.buf_len;
                self.buf &= ones << (64 - new_len);
            }
            
            if new_len == 64 {
                self.write.write_all(self.buf.to_byte_slice())?;
                self.buf = 0;
                self.buf_len = 0;
                count -= to_set;
            } else {
                self.buf_len = new_len;
                return Ok(())
            }
        }

        if val {
            while count >= 64 {
                self.write.write_all(ones.to_byte_slice())?;
                count -= 64;
            }
            if count > 0 {
                self.buf_len = count;
                self.buf = ones << (64 - count);
            }
        } else {
            while count >= 64 {
                self.write.write_all(0u64.to_byte_slice())?;
                count -= 64;
            }
            self.buf_len = count;
        }
        
        Ok(())
    }
}


impl <W: Write> BitmaskIOWriter<W> {
    pub fn finish(mut self) -> anyhow::Result<W> {
        if self.buf_len > 0 {
            let byte_len = bit_util::ceil(self.buf_len, 8);
            self.write.write_all(&self.buf.to_byte_slice()[0..byte_len])?;
        }

        self.write.write_all(
            (self.len as u32).to_byte_slice()
        )?;

        Ok(self.write)
    }
}


unsafe fn set_bits_slow(dst: *mut u8, dst_offset: usize, data: *const u8, offset: usize, len: usize) {
    for i in 0..len {
        if bit_util::get_bit_raw(data, offset + 1) {
            bit_util::set_bit_raw(dst, dst_offset + i)
        }
    }
}