use std::io::Cursor;
use std::sync::Arc;

use bytes::Bytes;
use memmap2::{Mmap, MmapOptions};
use parquet::file::reader::{ChunkReader, Length};


#[derive(Clone)]
pub struct MmapIO {
    mmap: Arc<Mmap>
}


impl MmapIO {
    pub fn open(filename: &str) -> std::io::Result<Self> {
        let file = std::fs::File::open(filename)?;
        let mmap = unsafe {
            MmapOptions::new().map(&file)
        }?;
        Ok(MmapIO {
            mmap: Arc::new(mmap)
        })
    }
}


impl AsRef<[u8]> for MmapIO {
    fn as_ref(&self) -> &[u8] {
        self.mmap.as_ref()
    }
}


impl ChunkReader for MmapIO {
    type T = Cursor<Self>;

    fn get_read(&self, start: u64) -> parquet::errors::Result<Self::T> {
        let mut cursor = Cursor::new(self.clone());
        cursor.set_position(start);
        Ok(cursor)
    }

    fn get_bytes(&self, start: u64, length: usize) -> parquet::errors::Result<Bytes> {
        let beg = start as usize;
        let end = beg + length;
        Ok(Bytes::copy_from_slice(&self.as_ref()[beg..end]))
    }
}


impl Length for MmapIO {
    fn len(&self) -> u64 {
        self.mmap.len() as u64
    }
}