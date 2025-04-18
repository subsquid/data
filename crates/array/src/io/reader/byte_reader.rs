use anyhow::ensure;
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom};


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
            assert!(bytes.len() > 0);
            cb(bytes)?;
            offset += bytes.len()
        }
        Ok(())
    }
}


impl<'a> ByteReader for &'a [u8] {
    fn len(&self) -> usize {
        (*self).len()
    }

    fn read(&mut self, offset: usize, len: usize) -> anyhow::Result<&[u8]> {
        let bytes = &self[offset..offset + len];
        Ok(bytes)
    }

    fn read_exact(
        &mut self,
        offset: usize,
        len: usize,
        mut cb: impl FnMut(&[u8]) -> anyhow::Result<()>
    ) -> anyhow::Result<()>
    {
        let bytes = &self[offset..offset + len]; 
        cb(bytes)
    }
}


pub struct IOByteReader<R> {
    read: BufReader<R>,
    len: usize,
    pos: Option<usize>
}


impl <R: Read + Seek> IOByteReader<R> {
    pub fn new(len: usize, read: R) -> Self {
        Self {
            read: BufReader::new(read),
            len,
            pos: None
        }
    }
}


impl <R: Read + Seek> ByteReader for IOByteReader<R> {
    fn len(&self) -> usize {
        self.len
    }

    fn read(&mut self, offset: usize, len: usize) -> anyhow::Result<&[u8]> {
        ensure!(offset + len <= self.len, "out of bounds read");

        if len == 0 {
            return Ok(&[])
        }

        if let Some(pos) = self.pos {
            self.pos = None;
            let rel = offset as i64 - pos as i64;
            self.read.seek_relative(rel)?;
        } else {
            self.read.seek(SeekFrom::Start(offset as u64))?;
        }

        let bytes = self.read.fill_buf()?;
        ensure!(bytes.len() > 0, "reached EOF");
        
        let take = std::cmp::min(len, bytes.len());
        self.pos = Some(offset);

        Ok(&bytes[0..take])
    }
}
