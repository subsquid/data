use anyhow::ensure;
use crate::io::reader::byte_reader::ByteReader;
use crate::writer::BitmaskWriter;
use arrow_buffer::bit_util;
use crate::reader::BitmaskReader;


pub struct BitmaskIOReader<R> {
    byte_reader: R,
    len: usize
}


impl <R> BitmaskIOReader<R> {
    pub fn new(byte_reader: R, len: usize) -> Self {
        Self {
            byte_reader,
            len
        }
    }
}


impl <R: ByteReader> BitmaskReader for BitmaskIOReader<R> {
    fn len(&self) -> usize {
        self.len
    }
    
    fn read_slice(&mut self, dst: &mut impl BitmaskWriter, offset: usize, mut len: usize) -> anyhow::Result<()> {
        ensure!(offset + len <= self.len);
        
        if len == 0 {
            return Ok(())
        }
        
        let byte_offset = offset / 8;
        let byte_len = bit_util::ceil(len, 8);
        let mut bit_offset = offset - byte_offset * 8;
        
        self.byte_reader.read_exact(byte_offset, byte_len, |data| {
            let bits_to_write = std::cmp::min(data.len() * 8 - bit_offset, len);
            dst.write_slice(data, bit_offset, bits_to_write)?;
            len -= bits_to_write;
            bit_offset = 0;
            Ok(())
        })?;
        
        Ok(())
    }
}