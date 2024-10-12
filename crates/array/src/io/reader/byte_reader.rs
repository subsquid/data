use std::io::{BufRead, Seek};
use anyhow::ensure;


pub trait ByteReader {
    fn len(&self) -> usize;

    fn read(&mut self, offset: usize, len: usize) -> anyhow::Result<&[u8]>;

    fn read_exact(
        &mut self,
        mut offset: usize,
        len: usize,
        mut cb: impl FnMut(&[u8]) -> anyhow::Result<()>
    ) -> anyhow::Result<()>
    {
        let end = offset + len;
        while offset < end {
            let bytes = self.read(offset, end - offset)?;
            cb(bytes)?;
            offset += bytes.len()
        }
        Ok(())
    }
}


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
        ensure!(offset + len <= self.len, "out of bounds read");
        todo!()
    }
}
