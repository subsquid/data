use crate::reader::bitmask::BitmaskReader;
use crate::reader::ByteReader;
use crate::writer::BitmaskWriter;


pub struct NullmaskReader<R> {
    bitmask: Option<BitmaskReader<R>>,
    len: usize
}


impl <R: ByteReader> NullmaskReader<R> {
    pub fn read_slice(
        &mut self, 
        dst: &mut impl BitmaskWriter, 
        offset: usize, 
        len: usize
    ) -> anyhow::Result<()> 
    {
        if let Some(bitmask) = self.bitmask.as_mut() {
            bitmask.read_slice(dst, offset, len)
        } else {
            assert!(offset + len <= self.len);
            dst.write_many(true, len)
        }   
    }
    
    pub fn len(&self) -> usize {
        self.len
    }
}