use crate::db::db::{RocksDB, CF_TABLES};
use crate::db::table_id::TableId;
use crate::db::write::storage::TableStorage;
use crate::db::ReadSnapshot;
use crate::table::key::TableKeyFactory;
use crate::table::stats::serialize_stats;
use crate::table::write::StorageCell;
use anyhow::Context;
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
    dataset_description: Option<DatasetDescriptionRef>,
    tables: Mutex<ChunkTables>
}


impl <'a> ChunkBuilder<'a> {
    pub fn new(db: &'a RocksDB, dataset_description: Option<DatasetDescriptionRef>) -> Self {
        Self {
            db,
            dataset_description,
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
    
    fn build_table_stats(&self, table_name: &str, table_id: TableId) -> anyhow::Result<()> {
        let column_options = self.dataset_description.as_ref().and_then(|d| {
            d.tables.get(table_name).map(|t| &t.options.column_options)
        });
        
        let column_options = if let Some(column_options) = column_options {
            column_options 
        } else {
            return Ok(())
        } ;

        if column_options.values().all(|opts| !opts.stats_enable) {
            return Ok(())
        }

        let snapshot = ReadSnapshot::new(self.db);
        let table_cf = self.db.cf_handle(CF_TABLES).unwrap();
        let table_reader = snapshot.create_table_reader(table_id)?;
        let mut bytes = Vec::new();
        let mut key = TableKeyFactory::new(table_id);
        
        for (name, opts) in column_options.iter().filter(|e| e.1.stats_enable) {
            if let Some(column_index) = table_reader.schema().index_of(name).ok() {
                let stats = table_reader
                    .build_column_stats(opts.stats_partition, column_index)
                    .with_context(|| {
                        format!("failed to build stats for column '{}'", name)
                    })?;
                
                bytes.clear();
                serialize_stats(&mut bytes, &stats).with_context(|| {
                    format!("failed to serialize stats of column {}", name)
                })?;
                
                self.db.put_cf(
                    table_cf,
                    key.statistic(column_index),
                    &bytes
                )?
            }
        }
        
        Ok(())
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
        self.chunk.build_table_stats(&self.table_name, self.table_id)?;
        
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