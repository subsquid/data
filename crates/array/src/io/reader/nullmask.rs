use crate::io::reader::bitmask::BitmaskIOReader;
use crate::io::reader::byte_reader::ByteReader;
use crate::reader::BitmaskReader;
use crate::writer::BitmaskWriter;
use anyhow::ensure;


pub struct NullmaskIOReader<R> {
    bitmask: Option<BitmaskIOReader<R>>,
    len: usize
}


impl <R: ByteReader> NullmaskIOReader<R> {
    pub fn new(len: usize, bitmask: Option<BitmaskIOReader<R>>) -> Self {
        assert_eq!(len, bitmask.as_ref().map(|m| m.len()).unwrap_or(len));
        Self {
            bitmask,
            len
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