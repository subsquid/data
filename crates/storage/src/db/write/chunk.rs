use crate::db::db::RocksDB;
use crate::db::table_id::TableId;
use crate::db::write::storage::TableStorage;
use crate::table::write::StorageCell;
use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use parking_lot::Mutex;
use sqd_array::slice::{AsSlice, Slice};
use sqd_array::writer::{ArrayWriter, Writer};
use sqd_dataset::DatasetDescriptionRef;
use std::collections::HashMap;


#[derive(Default)]
pub struct ChunkTables {
    pub(super) tables: HashMap<String, TableId>,
    pub(super) max_num_rows: u32
}


pub struct ChunkBuilder<'a> {
    db: &'a RocksDB,
    _dataset_description: Option<DatasetDescriptionRef>,
    tables: Mutex<ChunkTables>
}


impl <'a> ChunkBuilder<'a> {
    pub fn new(db: &'a RocksDB, dataset_description: Option<DatasetDescriptionRef>) -> Self {
        Self {
            db,
            _dataset_description: dataset_description,
            tables: Mutex::new(ChunkTables::default())
        }
    }

    pub fn add_table(&self, name: &str, schema: SchemaRef) -> ChunkTableWriter<'_> {
        let table_id = TableId::new();

        let mut storage = TableStorage::new(self.db);
        storage.mark_table_dirty(table_id);

        let writer = TableWriter::new(
            StorageCell::new(storage),
            table_id.as_ref(),
            schema
        );

        ChunkTableWriter {
            chunk: self,
            table_id,
            table_name: name.to_string(),
            writer
        }
    }

    pub fn finish(self) -> ChunkTables {
        self.tables.into_inner()
    }
}


type TableWriter<'a> = crate::table::write::TableWriter<StorageCell<TableStorage<'a>>>;


pub struct ChunkTableWriter<'a> {
    chunk: &'a ChunkBuilder<'a>,
    table_id: TableId,
    table_name: String,
    writer: TableWriter<'a>,
}


impl <'a> ChunkTableWriter<'a> {
    pub fn write_record_batch(&mut self, record_batch: &RecordBatch) -> anyhow::Result<()> {
        record_batch.as_slice().write(&mut self.writer)
    }

    pub fn finish(self) -> anyhow::Result<()> {
        self.writer.finish()?.into_inner().finish()?;
        let mut tables = self.chunk.tables.lock();
        tables.tables.insert(self.table_name, self.table_id);
        Ok(())
    }
}


impl<'a> ArrayWriter for ChunkTableWriter<'a> {
    type Writer = <TableWriter<'a> as ArrayWriter>::Writer;

    #[inline]
    fn bitmask(&mut self, buf: usize) -> &mut <Self::Writer as Writer>::Bitmask {
        self.writer.bitmask(buf)
    }

    #[inline]
    fn nullmask(&mut self, buf: usize) -> &mut <Self::Writer as Writer>::Nullmask {
        self.writer.nullmask(buf)
    }

    #[inline]
    fn native(&mut self, buf: usize) -> &mut <Self::Writer as Writer>::Native {
        self.writer.native(buf)
    }

    #[inline]
    fn offset(&mut self, buf: usize) -> &mut <Self::Writer as Writer>::Offset {
        self.writer.offset(buf)
    }
}