use arrow::datatypes::DataType;
use std::path::Path;


mod writer;
mod reader;


use crate::reader::AnyReader;
pub use reader::*;
pub use writer::*;


pub struct ArrayFile<F> {
    pub(self) data_type: DataType,
    pub(self) buffers: Vec<F>
}


impl <F: AsRef<Path>> ArrayFile<F> {
    pub fn data_type(&self) -> &DataType {
        &self.data_type
    }
    
    pub fn read(&self) -> anyhow::Result<ArrayFileReader> {
        let mut factory = FileReaderFactory::new(&self.buffers);
        AnyReader::from_factory(&mut factory, &self.data_type)
    }
    
    pub fn write(self) -> anyhow::Result<ArrayFileWriter<F>> {
        ArrayFileWriter::new(self)
    }
}


impl ArrayFile<tempfile::NamedTempFile> {
    pub fn new_temporary(data_type: DataType) -> anyhow::Result<Self> {
        use crate::util::get_num_buffers;
        
        let buffers = std::iter::repeat_with(tempfile::NamedTempFile::new)
            .take(get_num_buffers(&data_type))
            .collect::<Result<Vec<_>, _>>()?;
        
        Ok(Self {
            data_type,
            buffers
        })
    }
}