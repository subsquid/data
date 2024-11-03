mod byte_reader;
mod shared_file;
mod reader;
mod writer;


use crate::io::file::shared_file::SharedFileRef;
use crate::reader::AnyReader;
use crate::util::get_num_buffers;
use arrow::datatypes::DataType;
pub use reader::*;
pub use writer::*;


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
        let mut factory = FileReaderFactory::new(&self.buffers);
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