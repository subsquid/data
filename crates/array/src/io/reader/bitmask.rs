use crate::io::reader::byte_reader::ByteReader;
use crate::reader::BitmaskReader;
use crate::writer::BitmaskWriter;
use anyhow::{ensure, Context};
use arrow_buffer::bit_util;


pub struct BitmaskIOReader<R> {
    byte_reader: R,
    len: usize
}


impl <R: ByteReader> BitmaskIOReader<R> {
    pub fn new_unchecked(byte_reader: R, len: usize) -> Self {
        Self {
            byte_reader,
            len
        }
    }

    pub fn new(mut byte_reader: R) -> anyhow::Result<Self> {
        let (byte_len, bit_len) = Self::read_length(&mut byte_reader).context(
            "failed to read bitmask length"
        )?;

        ensure!(
            byte_len == bit_util::ceil(bit_len, 8),
            "bitmask buffer has unexpected length"
        );

        Ok(
            Self::new_unchecked(byte_reader, bit_len)
        )
    }

    pub(super) fn read_length(byte_reader: &mut R) -> anyhow::Result<(usize, usize)> {
        const WORD: usize = size_of::<u32>();
        
        ensure!(byte_reader.len() >= WORD);
        let byte_len = byte_reader.len() - WORD;

        let mut bit_len_slice = [0u8; WORD];
        let mut pos = 0;
        byte_reader.read_exact(byte_reader.len() - WORD, WORD, |bytes| {
            bit_len_slice[pos..pos + bytes.len()].copy_from_slice(bytes);
            pos += bytes.len();
            Ok(())
        })?;
        let bit_len = u32::from_le_bytes(bit_len_slice);
        
        Ok(
            (byte_len, bit_len as usize)
        )
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
        let mut bit_offset = offset - byte_offset * 8;
        let byte_len = bit_util::ceil(len + bit_offset, 8);
        
        self.byte_reader.read_exact(byte_offset, byte_len, |data| {
            let bits_to_write = std::cmp::min(data.len() * 8 - bit_offset, len);
            dst.write_slice(data, bit_offset, bits_to_write)?;
            len -= bits_to_write;
            bit_offset = 0;
            Ok(())
        })?;
        
        debug_assert_eq!(len, 0, "got unexpected number of bytes from .read_exact()");
        
        Ok(())
    }
}