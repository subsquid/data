use crate::io::reader::byte_reader::ByteReader;
use crate::reader::OffsetsReader;
use crate::writer::OffsetsWriter;
use arrow_buffer::MutableBuffer;
use std::ops::Range;


pub struct OffsetsIOReader<R> {
    byte_reader: R,
    buf: MutableBuffer
}


impl<R> OffsetsIOReader<R> {
    pub fn new(byte_reader: R) -> Self {
        Self {
            byte_reader,
            buf: MutableBuffer::new(0)
        }
    }
}


impl <R: ByteReader> OffsetsReader for OffsetsIOReader<R> {
    fn len(&self) -> usize {
        self.byte_reader.len() / size_of::<i32>()
    }
    
    fn read_slice(
        &mut self, 
        dst: &mut impl OffsetsWriter, 
        offset: usize, 
        len: usize
    ) -> anyhow::Result<Range<usize>> 
    {
        todo!()
    }
}