use crate::io::reader::{BitmaskIOReader, IOByteReader, NativeIOReader, NullmaskIOReader, OffsetsIOReader};
use crate::reader::{AnyReader, Reader, ReaderFactory};
use arrow_buffer::ArrowNativeType;
use std::fs::File;
use std::io::BufReader;
use std::marker::PhantomData;
use std::path::Path;


pub type FileByteReader = IOByteReader<BufReader<File>>;


pub struct FileReader<'a> {
    phantom_data: PhantomData<&'a ()>
}


impl <'a> Reader for FileReader<'a> {
    type Nullmask = NullmaskIOReader<FileByteReader>;
    type Bitmask = BitmaskIOReader<FileByteReader>;
    type Native = NativeIOReader<FileByteReader>;
    type Offset = OffsetsIOReader<FileByteReader>;
}


pub type ArrayFileReader<'a> = AnyReader<FileReader<'a>>;


pub(super) struct FileReaderFactory<'a, F> {
    buffers: &'a [F],
    pos: usize
}


impl <'a, F: AsRef<Path>> FileReaderFactory<'a, F> {
    pub fn new(buffers: &'a [F]) -> Self {
        Self {
            buffers,
            pos: 0
        }
    }

    fn next_file(&mut self) -> anyhow::Result<FileByteReader> {
        let file = File::open(&self.buffers[self.pos])?;
        let len = file.metadata()?.len() as usize;
        let reader = IOByteReader::new(BufReader::new(file), len);
        self.pos += 1;
        Ok(reader)
    }
}


impl <'a, F: AsRef<Path>> ReaderFactory for FileReaderFactory<'a, F> {
    type Reader = FileReader<'a>;

    fn nullmask(&mut self) -> anyhow::Result<<Self::Reader as Reader>::Nullmask> {
        todo!()
    }

    fn bitmask(&mut self) -> anyhow::Result<<Self::Reader as Reader>::Bitmask> {
        todo!()
    }

    fn native<T: ArrowNativeType>(&mut self) -> anyhow::Result<<Self::Reader as Reader>::Native> {
        todo!()
    }

    fn offset(&mut self) -> anyhow::Result<<Self::Reader as Reader>::Offset> {
        todo!()
    }
}