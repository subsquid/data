use anyhow::ensure;
use crate::io::reader::byte_reader::ByteReader;
use crate::reader::NativeReader;
use crate::writer::NativeWriter;


pub struct NativeIOReader<R> {
    byte_reader: R,
    value_size: usize
}


impl <R: ByteReader> NativeIOReader<R> {
    pub fn new_unchecked(byte_reader: R, value_size: usize) -> Self {
        Self {
            byte_reader,
            value_size
        }
    }
    
    pub fn new(byte_reader: R, value_size: usize) -> anyhow::Result<Self> {
        ensure!(
            byte_reader.len() % value_size == 0,
            "invalid buffer length: {} is not a multiple of {}",
            byte_reader.len(),
            value_size
        );
        Ok(Self {
            byte_reader,
            value_size
        })
    }
}


impl <R: ByteReader> NativeReader for NativeIOReader<R> {
    fn len(&self) -> usize {
        self.byte_reader.len() / self.value_size
    }

    fn read_slice(
        &mut self, 
        dst: &mut impl NativeWriter, 
        offset: usize, 
        len: usize
    ) -> anyhow::Result<()> 
    {
        self.byte_reader.read_exact(offset * self.value_size, len * self.value_size, |bytes| {
            dst.write_slice(bytes)
        })    
    }
}