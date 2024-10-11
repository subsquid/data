use crate::io::byte_reader::ByteReader;
use crate::writer::NativeWriter;


pub struct NativeReader<R> {
    reader: R,
    value_size: usize
}


impl <R: ByteReader> NativeReader<R> {
    pub fn read_slice(
        &mut self, 
        dst: &mut impl NativeWriter, 
        offset: usize, 
        len: usize
    ) -> anyhow::Result<()> 
    {
        self.reader.read_exact(offset * self.value_size, len * self.value_size, |bytes| {
            dst.write_slice(bytes)
        })    
    }
    
    pub fn len(&self) -> usize {
        self.reader.len() / self.value_size
    }
}