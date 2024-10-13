use crate::reader::AnyReader;
use crate::writer::AnyArrayWriter;
use arrow::datatypes::DataType;
use std::path::Path;
use writer::FileWriterFactory;


mod writer;
mod reader;


pub use reader::*;
pub use writer::*;


pub struct ArrayFile<F> {
    data_type: DataType,
    buffers: Vec<F>
}


impl <F: AsRef<Path>> ArrayFile<F> {
    pub fn data_type(&self) -> &DataType {
        &self.data_type
    }
    
    pub fn read(&self) -> anyhow::Result<ArrayFileReader<'_>> {
        let mut factory = FileReaderFactory::new(&self.buffers);
        AnyReader::from_factory(&mut factory, &self.data_type)
    }
    
    pub fn write(&mut self) -> anyhow::Result<ArrayFileWriter<'_>> {
        let mut factory = FileWriterFactory::new(&self.buffers);
        AnyArrayWriter::from_factory(&mut factory, &self.data_type)
    }
}


#[cfg(feature = "tempfile")]
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