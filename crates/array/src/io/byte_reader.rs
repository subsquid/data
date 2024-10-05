use crate::reader::ByteReader;
use std::io::{BufRead, Seek};


pub struct IOByteReader<R> {
    read: R,
    len: usize,
    pos: Option<usize>
}


impl <R> IOByteReader<R> {
    pub fn new(read: R, len: usize) -> Self {
        Self {
            read,
            len,
            pos: None
        }
    }
}


impl <R: BufRead + Seek> ByteReader for IOByteReader<R> {
    fn len(&self) -> usize {
        self.len
    }

    fn read(&mut self, offset: usize, len: usize) -> anyhow::Result<&[u8]> {
        todo!()
    }
}