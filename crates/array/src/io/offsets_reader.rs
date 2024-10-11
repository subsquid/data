use crate::io::byte_reader::ByteReader;
use crate::writer::OffsetsWriter;
use std::ops::Range;


pub struct OffsetsReader<R> {
    reader: R,
    buf: Vec<i32>
}


impl <R: ByteReader> OffsetsReader<R> {
    pub fn len(&self) -> usize {
        self.reader.len() / size_of::<i32>()
    }
    
    pub fn read_slice(
        &mut self, 
        dst: &mut impl OffsetsWriter, 
        offset: usize, 
        len: usize
    ) -> anyhow::Result<Range<usize>> 
    {
        todo!()
    }
}