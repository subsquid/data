use crate::io::file::shared_file::SharedFileRef;
use crate::io::reader::ByteReader;
use anyhow::ensure;


pub struct FileByteReader {
    file: SharedFileRef,
    file_len: usize,
    buf: Box<[u8; 4096]>,
    buffered_offset: usize,
    buffered_len: usize
}


impl FileByteReader {
    pub fn new(file: SharedFileRef) -> anyhow::Result<Self> {
        let len = file.len()?;
        Ok(Self {
            file,
            file_len: len,
            buf: Box::new([0; 4096]),
            buffered_offset: 0,
            buffered_len: 0
        })
    }
    
    #[inline(never)]
    fn fill_buffer(&mut self, offset: usize) -> std::io::Result<()> {
        let len = self.file.read(offset, self.buf.as_mut_slice())?;
        self.buffered_offset = offset;
        self.buffered_len = len;
        Ok(())
    }
}


impl ByteReader for FileByteReader {
    fn len(&self) -> usize {
        self.file_len
    }

    fn read(&mut self, offset: usize, len: usize) -> anyhow::Result<&[u8]> {
        ensure!(offset + len <= self.file_len, "out of bounds read");
        
        if self.buffered_offset <= offset && offset < self.buffered_offset + self.buffered_len {
            let beg = offset - self.buffered_offset;
            let end = beg + std::cmp::min(len, self.buffered_len - beg);
            return Ok(&self.buf[beg..end])
        }

        if len == 0 {
            return Ok(&[])
        }
        
        self.fill_buffer(offset)?;
        
        Ok(&self.buf[0..std::cmp::min(len, self.buffered_len)])
    }
}