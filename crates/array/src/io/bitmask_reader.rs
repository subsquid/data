use crate::io::byte_reader::ByteReader;
use crate::writer::BitmaskWriter;
use arrow_buffer::bit_util;


pub struct BitmaskReader<R> {
    data: R,
    len: usize
}


impl <R: ByteReader> BitmaskReader<R> {
    pub fn len(&self) -> usize {
        self.len
    }
    
    pub fn read_slice(&mut self, dst: &mut impl BitmaskWriter, offset: usize, mut len: usize) -> anyhow::Result<()> {
        assert!(offset + len <= self.len);
        
        if len == 0 {
            return Ok(())
        }
        
        let byte_offset = offset / 8;
        let byte_len = bit_util::ceil(len, 8);
        let mut bit_offset = offset - byte_offset * 8;
        
        self.data.read_exact(byte_offset, byte_len, |data| {
            let bits_to_write = std::cmp::min(data.len() * 8 - bit_offset, len);
            dst.write_slice(data, bit_offset, bits_to_write)?;
            len -= bits_to_write;
            bit_offset = 0;
            Ok(())
        })?;
        
        Ok(())
    }
}