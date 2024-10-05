use crate::io::byte_reader::IOByteReader;
use crate::io::{BitmaskIOWriter, NativeIOWriter, NullmaskIOWriter, OffsetsIOWriter};
use crate::reader::AnyReader;
use crate::writer::{AnyArrayWriter, Writer};
use arrow::datatypes::DataType;
use std::fs::File;
use std::io::{BufReader, BufWriter};


pub type FileByteReader = IOByteReader<BufReader<File>>;
pub type FileArrayReader = AnyReader<FileByteReader>;


pub struct FileWriter;


impl Writer for FileWriter {
    type Bitmask = BitmaskIOWriter<BufWriter<File>>;
    type Nullmask = NullmaskIOWriter<BufWriter<File>>;
    type Native = NativeIOWriter<BufWriter<File>>;
    type Offset = OffsetsIOWriter<BufWriter<File>>;
}


pub type FileArrayWriter = AnyArrayWriter<FileWriter>;


pub struct ArrowFile {
    data_type: DataType,
    buffers: Vec<File>
}


impl ArrowFile {
    pub fn read(
        &mut self,
        mut cb: impl FnMut(&mut FileArrayReader) -> anyhow::Result<()>
    ) -> anyhow::Result<()>
    {
        let mut buf = 0;

        let mut reader = AnyReader::from_byte_readers(&self.data_type, || {
            let file = self.buffers[buf].try_clone()?;
            let len = file.metadata()?.len() as usize;
            let reader = IOByteReader::new(BufReader::new(file), len);
            buf += 1;
            Ok(reader)
        })?;

        cb(&mut reader)
    }

    pub fn write(
        &mut self,
        mut cb: impl FnMut(&mut FileArrayWriter) -> anyhow::Result<()>
    ) -> anyhow::Result<()>
    {
        todo!()
    }
}
