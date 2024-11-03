use crate::io::reader::bitmask::BitmaskIOReader;
use crate::io::reader::byte_reader::ByteReader;
use crate::reader::BitmaskReader;
use crate::writer::BitmaskWriter;
use anyhow::ensure;
use arrow_buffer::bit_util;


pub struct NullmaskIOReader<R> {
    bitmask: Option<BitmaskIOReader<R>>,
    len: usize
}


impl <R: ByteReader> NullmaskIOReader<R> {
    pub fn new(mut byte_reader: R) -> anyhow::Result<Self> {
        let (byte_len, bit_len) = BitmaskIOReader::<R>::read_length(&mut byte_reader)?;
        if byte_len == 0 {
            Ok(Self {
                bitmask: None,
                len: bit_len
            })
        } else {
            ensure!(
                byte_len == bit_util::ceil(bit_len, 8),
                "bitmask buffer has unexpected length"
            );
            Ok(Self {
                bitmask: Some(BitmaskIOReader::new_unchecked(byte_reader, bit_len)),
                len: bit_len
            })
        }
    }
}


impl <R: ByteReader> BitmaskReader for NullmaskIOReader<R> {
    fn len(&self) -> usize {
        self.len
    }

    fn read_slice(
        &mut self, 
        dst: &mut impl BitmaskWriter, 
        offset: usize, 
        len: usize
    ) -> anyhow::Result<()> 
    {
        if let Some(bitmask) = self.bitmask.as_mut() {
            bitmask.read_slice(dst, offset, len)
        } else {
            ensure!(offset + len <= self.len);
            dst.write_many(true, len)
        }   
    }
}