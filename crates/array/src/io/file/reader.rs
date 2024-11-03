use crate::io::file::byte_reader::FileByteReader;
use crate::io::file::shared_file::SharedFileRef;
use crate::io::reader::{BitmaskIOReader, NativeIOReader, NullmaskIOReader, OffsetsIOReader};
use crate::reader::{AnyReader, Reader, ReaderFactory};
use arrow_buffer::ArrowNativeType;


pub type ArrayFileReader = AnyReader<FileReader>;


pub struct FileReader;


impl Reader for FileReader {
    type Nullmask = NullmaskIOReader<FileByteReader>;
    type Bitmask = BitmaskIOReader<FileByteReader>;
    type Native = NativeIOReader<FileByteReader>;
    type Offset = OffsetsIOReader<FileByteReader>;
}


pub(super) struct FileReaderFactory<'a> {
    buffers: &'a [SharedFileRef],
    pos: usize
}


impl<'a> FileReaderFactory<'a> {
    pub fn new(buffers: &'a [SharedFileRef]) -> Self {
        Self {
            buffers,
            pos: 0
        }
    }

    fn next_file(&mut self) -> anyhow::Result<FileByteReader> {
        let byte_reader = FileByteReader::new(self.buffers[self.pos].clone())?;
        self.pos += 1;
        Ok(byte_reader)
    }
}


impl <'a> ReaderFactory for FileReaderFactory<'a> {
    type Reader = FileReader;

    fn nullmask(&mut self) -> anyhow::Result<<Self::Reader as Reader>::Nullmask> {
        let byte_reader = self.next_file()?;
        NullmaskIOReader::new(byte_reader)
    }

    fn bitmask(&mut self) -> anyhow::Result<<Self::Reader as Reader>::Bitmask> {
        let byte_reader = self.next_file()?;
        BitmaskIOReader::new(byte_reader)
    }

    fn native<T: ArrowNativeType>(&mut self) -> anyhow::Result<<Self::Reader as Reader>::Native> {
        let byte_reader = self.next_file()?;
        NativeIOReader::new(byte_reader, T::get_byte_width())
    }

    fn offset(&mut self) -> anyhow::Result<<Self::Reader as Reader>::Offset> {
        let byte_reader = self.next_file()?;
        OffsetsIOReader::new(byte_reader)
    }
}
