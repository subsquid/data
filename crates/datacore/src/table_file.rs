use arrow::datatypes::Fields;
use tempfile::NamedTempFile;
use sqd_array::io::file::{ArrayFile, ArrayFileWriter};
use sqd_array::slice::{AnyTableSlice, Slice};


type ColumnFile = ArrayFile<NamedTempFile>;
type ColumnWriter = ArrayFileWriter<NamedTempFile>;


pub struct TableFileWriter {
    columns: Vec<ColumnWriter>,
    num_rows: usize
}


impl TableFileWriter {
    pub fn new(fields: &Fields) -> anyhow::Result<Self> {
        let columns = fields.iter().map(|f| {
            let file = ArrayFile::new_temporary(f.data_type().clone())?;
            file.write()
        }).collect::<anyhow::Result<Vec<_>>>()?;

        Ok(Self {
            columns,
            num_rows: 0
        })
    }

    pub fn push_batch(&mut self, records: &AnyTableSlice<'_>) -> anyhow::Result<()> {
        for (i, c) in self.columns.iter_mut().enumerate() {
            records.column(i).write(c)?
        }
        self.num_rows += records.len();
        Ok(())
    }
    
    pub fn num_rows(&self) -> usize {
        self.num_rows
    }
    
    pub fn finish(self) -> anyhow::Result<TableFile> {
        todo!()
    }
}


pub struct TableFile {
    columns: Vec<ColumnWriter>,
}