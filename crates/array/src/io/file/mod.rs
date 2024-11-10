mod byte_reader;
mod shared_file;
mod writer;


use crate::io::reader::IOReader;
use crate::reader::AnyReader;
use crate::util::get_num_buffers;
use arrow::datatypes::DataType;
use byte_reader::FileByteReader;
use shared_file::SharedFileRef;


pub use writer::*;


pub type FileReader = IOReader<FileByteReader>;
pub type ArrayFileReader = AnyReader<FileReader>;


pub struct ArrayFile {
    data_type: DataType,
    buffers: Vec<SharedFileRef>
}


impl ArrayFile {
    pub(self) fn new(data_type: DataType, buffers: Vec<SharedFileRef>) -> Self {
        Self {
            data_type,
            buffers
        }
    }

    pub fn data_type(&self) -> &DataType {
        &self.data_type
    }

    pub fn read(&self) -> anyhow::Result<ArrayFileReader> {
        let mut pos = 0;
        let mut factory = || {
            let byte_reader = FileByteReader::new(self.buffers[pos].clone())?;
            pos += 1;
            Ok(byte_reader)
        };
        AnyReader::from_factory(&mut factory, &self.data_type)
    }
    
    pub fn write(self) -> anyhow::Result<ArrayFileWriter> {
        ArrayFileWriter::new(self.data_type, self.buffers)
    }
    
    pub fn new_temporary(data_type: DataType) -> anyhow::Result<Self> {
        let buffers =
            std::iter::repeat_with(|| {
                tempfile::tempfile().map(SharedFileRef::new)
            })
                .take(get_num_buffers(&data_type))
                .collect::<Result<Vec<_>, _>>()?;

        Ok(Self {
            data_type,
            buffers
        })
    }
}