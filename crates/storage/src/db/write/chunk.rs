use std::cmp::max;
use std::collections::HashMap;

use anyhow::{anyhow, ensure};
use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use parking_lot::Mutex;

use crate::db::data::DatasetDescriptionRef;
use crate::db::db::RocksDB;
use crate::db::table_id::TableId;
use crate::db::write::storage::TableStorage;
use crate::table::write::RecordBatchWriter;


#[derive(Default)]
pub struct ChunkTables {
    pub(super) tables: HashMap<String, TableId>,
    pub(super) max_num_rows: u32
}


pub struct ChunkBuilder<'a> {
    db: &'a RocksDB,
    dataset_description: DatasetDescriptionRef,
    tables: Mutex<ChunkTables>
}


impl <'a> ChunkBuilder<'a> {
    pub fn new(db: &'a RocksDB, dataset_description: DatasetDescriptionRef) -> Self {
        Self {
            db,
            dataset_description,
            tables: Mutex::new(ChunkTables::default())
        }
    }

    pub fn add_table(&self, name: &str, schema: SchemaRef) -> anyhow::Result<ChunkTableWriter<'_>> {
        let desc = self.dataset_description.tables.iter().find(|t| t.name == name).ok_or_else(|| {
            anyhow!("table `{}` is not defined in the dataset", name)
        })?;

        for col in desc.sort_key.iter() {
            ensure!(
                schema.column_with_name(col).is_some(),
                "sort key {} is not present in the schema",
                col
            );
        }

        let table_id = TableId::new();

        let mut storage = TableStorage::new(self.db);
        storage.mark_table_dirty(table_id);

        let writer = RecordBatchWriter::new(storage, schema, table_id.as_ref(), &desc.options);

        Ok(ChunkTableWriter {
            chunk: self,
            table_id,
            table_name: name.to_string(),
            writer,
            num_rows: 0
        })
    }

    pub fn finish(self) -> ChunkTables {
        self.tables.into_inner()
    }
}


pub struct ChunkTableWriter<'a> {
    chunk: &'a ChunkBuilder<'a>,
    table_id: TableId,
    table_name: String,
    writer: RecordBatchWriter<TableStorage<'a>>,
    num_rows: u32
}


impl <'a> ChunkTableWriter<'a> {
    pub fn write_record_batch(&mut self, record_batch: &RecordBatch) -> anyhow::Result<()> {
        self.writer.write_record_batch(record_batch)?;
        self.num_rows += record_batch.num_rows() as u32;
        if self.writer.storage().byte_size() > 5 * 1024 * 1024 {
            self.writer.storage_mut().flush()?;
        }
        Ok(())
    }

    pub fn finish(self) -> anyhow::Result<()> {
        self.writer.finish()?.flush()?;
        let mut tables = self.chunk.tables.lock();
        tables.tables.insert(self.table_name, self.table_id);
        tables.max_num_rows = max(tables.max_num_rows, self.num_rows);
        Ok(())
    }
}