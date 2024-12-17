use crate::db::db::{RocksDB, CF_TABLES};
use crate::db::table_id::TableId;
use crate::db::write::storage::TableStorage;
use crate::db::ReadSnapshot;
use crate::table::key::TableKeyFactory;
use crate::table::stats::{can_have_stats, serialize_stats};
use crate::table::write::StorageCell;
use anyhow::{ensure, Context};
use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use parking_lot::Mutex;
use sqd_array::slice::{AsSlice, Slice};
use sqd_array::writer::{ArrayWriter, Writer};
use std::collections::{BTreeMap, BTreeSet};


pub type ChunkTables = BTreeMap<String, TableId>;


pub struct ChunkBuilder<'a> {
    db: &'a RocksDB,
    tables: Mutex<ChunkTables>
}


impl <'a> ChunkBuilder<'a> {
    pub fn new(db: &'a RocksDB) -> Self {
        Self {
            db,
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
            schema.clone()
        );

        ChunkTableWriter {
            chunk: self,
            table_id,
            table_name: name.to_string(),
            schema,
            columns_with_stats: BTreeSet::new(),
            writer
        }
    }

    pub fn finish(self) -> ChunkTables {
        self.tables.into_inner()
    }
    
    fn build_table_stats(&self,
        table_id: TableId,
        columns_with_stats: &BTreeSet<usize>
    ) -> anyhow::Result<()>
    {
        if columns_with_stats.is_empty() {
            return Ok(())
        }

        let snapshot = ReadSnapshot::new(self.db);
        let table_cf = self.db.cf_handle(CF_TABLES).unwrap();
        let table_reader = snapshot.create_table_reader(table_id)?;
        let mut bytes = Vec::new();
        let mut key = TableKeyFactory::new(table_id);

        for column_index in columns_with_stats.iter().copied() {
            let stats = table_reader
                .build_column_stats(4096, column_index)
                .with_context(|| {
                    format!(
                        "failed to build stats for column '{}'",
                        table_reader.schema().field(column_index).name()
                    )
                })?;

            bytes.clear();
            serialize_stats(&mut bytes, &stats)
                .with_context(|| {
                    format!(
                        "failed to serialize stats of column {}",
                        table_reader.schema().field(column_index).name()
                    )
                })?;

            self.db.put_cf(
                table_cf,
                key.statistic(column_index),
                &bytes
            )?
        }

        Ok(())
    }
}


type TableWriter<'a> = crate::table::write::TableWriter<StorageCell<TableStorage<'a>>>;


pub struct ChunkTableWriter<'a> {
    chunk: &'a ChunkBuilder<'a>,
    table_id: TableId,
    table_name: String,
    schema: SchemaRef,
    columns_with_stats: BTreeSet<usize>,
    writer: TableWriter<'a>,
}


impl <'a> ChunkTableWriter<'a> {
    pub fn write_record_batch(&mut self, record_batch: &RecordBatch) -> anyhow::Result<()> {
        record_batch.as_slice().write(&mut self.writer)
    }

    pub fn add_stat_by_name(&mut self, name: &str) -> anyhow::Result<()> {
        let index = self.schema.index_of(name)?;
        let data_type = self.schema.field(index).data_type();
        ensure!(
            can_have_stats(data_type),
            "can't stat column `{}`: columns of type {} can't have stats", 
            name,
            data_type
        );
        self.columns_with_stats.insert(index);
        Ok(())
    }
    
    pub fn set_stats(&mut self, columns: impl IntoIterator<Item=usize>) -> anyhow::Result<()> {
        let num_columns = self.schema.fields().len();
        self.columns_with_stats = columns.into_iter().map(|index| {
            ensure!(index < num_columns, "column {} does not exist", index);
            let field = self.schema.field(index);
            ensure!(
                can_have_stats(field.data_type()),
                "can't stat column {} ({}): columns of type {} can't have stats", 
                index,
                field.name(),
                field.data_type()
            );
            Ok(index)
        }).collect::<anyhow::Result<BTreeSet<_>>>()?;
        Ok(())
    }

    pub fn finish(self) -> anyhow::Result<()> {
        self.writer.finish()?.into_inner().finish()?;
        self.chunk.build_table_stats(self.table_id, &self.columns_with_stats)?;
        let mut tables = self.chunk.tables.lock();
        tables.insert(self.table_name, self.table_id);
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