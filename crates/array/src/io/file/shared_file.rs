use parking_lot::Mutex;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::sync::Arc;


#[derive(Clone)]
pub struct SharedFileRef {
    inner: Arc<Mutex<SharedFile>>
}


impl SharedFileRef {
    pub fn new(file: File) -> Self {
        Self {
            inner: Arc::new(Mutex::new(SharedFile::new(file)))
        }
    }
    
    pub fn read(&self, offset: usize, buf: &mut [u8]) -> std::io::Result<usize> {
        self.inner.lock().read(offset, buf)
    }
    
    pub fn len(&self) -> std::io::Result<usize> {
        self.inner.lock().len()
    }
    
    pub fn into_file(self) -> File {
        Arc::into_inner(self.inner)
            .expect("this shared file is still in use by other readers")
            .into_inner()
            .into_file()
    }
}


pub struct SharedFile {
    inner: File,
    pos: Option<usize>
}


impl SharedFile {
    pub fn new(file: File) -> Self {
        Self {
            inner: file,
            pos: None
        }
    }

    pub fn read(&mut self, offset: usize, buf: &mut [u8]) -> std::io::Result<usize> {
        let pos = std::mem::take(&mut self.pos);
        if pos != Some(offset) {
            let new_pos = self.inner.seek(SeekFrom::Start(offset as u64))?;
            assert_eq!(new_pos as usize, offset);
        }
        let len = self.inner.read(buf)?;
        self.pos = Some(offset + len);
        Ok(len)
    }

    pub fn len(&self) -> std::io::Result<usize> {
        self.inner.metadata().map(|m| m.len() as usize)
    }

    pub fn into_file(self) -> File {
        self.inner
    }
}