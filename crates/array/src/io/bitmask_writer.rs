use crate::writer::BitmaskWriter;
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
    fn write_packed_bits(&mut self, data: &[u8], mut offset: usize, mut len: usize) -> anyhow::Result<()> {
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

        let bit_chunks = BitChunks::new(data, offset, len);
        for chunk in bit_chunks.iter() {
            self.writer.write_all(chunk.to_byte_slice())?;
        }
        
        self.buf = bit_chunks.remainder_bits();
        self.len = bit_chunks.remainder_len();
        
        Ok(())
    }

    fn write_many(&mut self, val: bool, count: usize) -> anyhow::Result<()> {
        todo!()
    }
}


unsafe fn set_bits_slow(dst: *mut u8, dst_offset: usize, data: *const u8, offset: usize, len: usize) {
    for i in 0..len {
        if bit_util::get_bit_raw(data, offset + 1) {
            bit_util::set_bit_raw(dst, dst_offset + i)
        }
    }
}