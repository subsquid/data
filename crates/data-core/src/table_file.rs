use arrow::datatypes::Fields;
use sqd_array::io::file::{ArrayFile, ArrayFileReader, ArrayFileWriter};
use sqd_array::reader::ArrayReader;
use sqd_array::slice::{AnyTableSlice, Slice};
use sqd_array::writer::ArrayWriter;


pub struct TableFileWriter {
    columns: Vec<ArrayFileWriter>
}


impl TableFileWriter {
    pub fn new(fields: &Fields) -> anyhow::Result<Self> {
        let columns = fields.iter().map(|f| {
            let file = ArrayFile::new_temporary(f.data_type().clone())?;
            file.write()
        }).collect::<anyhow::Result<Vec<_>>>()?;

        Ok(Self {
            columns
        })
    }

    pub fn push_batch(&mut self, records: &AnyTableSlice<'_>) -> anyhow::Result<()> {
        for (i, c) in self.columns.iter_mut().enumerate() {
            records.column(i).write(c)?
        }
        Ok(())
    }

    pub fn finish(self) -> anyhow::Result<TableFile> {
        let columns = self.columns.into_iter()
            .map(|col| col.finish())
            .collect::<Result<Vec<_>, _>>()?;
        
        let readers = columns.iter()
            .map(|c| c.read())
            .collect::<Result<Vec<_>, _>>()?;
        
        Ok(TableFile {
            columns,
            readers
        })
    }
}


pub struct TableFile {
    columns: Vec<ArrayFile>,
    readers: Vec<ArrayFileReader>
}


impl TableFile {
    pub fn read_column(
        &mut self, 
        dst: &mut impl ArrayWriter, 
        i: usize, 
        offset: usize,
        len: usize
    ) -> anyhow::Result<()> 
    {
        self.readers[i].read_slice(dst, offset, len)
    }
    
    pub fn into_writer(self) -> anyhow::Result<TableFileWriter> {
        drop(self.readers);
        
        let columns = self.columns.into_iter()
            .map(|file| file.write())
            .collect::<Result<Vec<_>, _>>()?;
        
        Ok(TableFileWriter {
            columns
        })
    }
}