use crate::reader::{ArrowReader, ByteReader};
use crate::writer::{ArrayWriter, NativeWriter};


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


impl<R: ByteReader> ArrowReader for NativeReader<R> {
    fn num_buffers(&self) -> usize {
        1
    }

    #[inline]
    fn len(&self) -> usize {
        self.len()
    }

    #[inline]
    fn read_slice(&mut self, dst: &mut impl ArrayWriter, offset: usize, len: usize) -> anyhow::Result<()> {
        self.read_slice(dst.native(0), offset, len)
    }
}