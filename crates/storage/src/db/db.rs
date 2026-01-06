use super::data::{Dataset, DatasetId, DatasetKind, DatasetLabel};
use super::read::snapshot::ReadSnapshot;
use crate::db::ops::{perform_dataset_compaction, CompactionStatus};
use crate::db::read::datasets::list_all_datasets;
use crate::db::write::ops::deleted_deleted_tables;
use crate::db::write::table_builder::TableBuilder;
use crate::db::write::tx::Tx;
use crate::db::{Chunk, DatasetUpdate};
use anyhow::ensure;
use arrow::datatypes::SchemaRef;
use rocksdb::{ColumnFamilyDescriptor, Options as RocksOptions};
use sqd_primitives::Name;
use std::path::Path;


pub(super) const CF_DATASETS: Name = "DATASETS";
pub(super) const CF_CHUNKS: Name = "CHUNKS";
pub(super) const CF_TABLES: Name = "TABLES";
pub(super) const CF_DIRTY_TABLES: Name = "DIRTY_TABLES";
pub(super) const CF_DELETED_TABLES: Name = "DELETED_TABLES";


pub(super) type RocksDB = rocksdb::OptimisticTransactionDB;
pub(super) type RocksTransaction<'a> = rocksdb::Transaction<'a, RocksDB>;
pub(super) type RocksTransactionOptions = rocksdb::OptimisticTransactionOptions;
pub(super) type RocksWriteBatch = rocksdb::WriteBatchWithTransaction<true>;
pub(super) type RocksIterator<'a, DB> = rocksdb::DBRawIteratorWithThreadMode<'a, DB>;
pub(super) type RocksSnapshot<'a, DB> = rocksdb::SnapshotWithThreadMode<'a, DB>;


pub struct DatabaseSettings {
    chunk_cache_size: usize,
    data_cache_size: usize,
    with_rocksdb_stats: bool,
    direct_io: bool,
    cache_index_and_filter_blocks: bool,
}


impl Default for DatabaseSettings {
    fn default() -> Self {
        Self {
            chunk_cache_size: 64,
            data_cache_size: 256,
            with_rocksdb_stats: false,
            direct_io: false,
            cache_index_and_filter_blocks: false,
        }
    }
}


impl DatabaseSettings {
    pub fn with_chunk_cache_size(mut self, mb: usize) -> Self {
        self.chunk_cache_size = mb;
        self
    }
    
    pub fn with_data_cache_size(mut self, mb: usize) -> Self {
        self.data_cache_size = mb;
        self
    }

    pub fn with_rocksdb_stats(mut self, on: bool) -> Self {
        self.with_rocksdb_stats = on;
        self
    }
    
    pub fn with_direct_io(mut self, yes: bool) -> Self {
        self.direct_io = yes;
        self
    }

    pub fn with_cache_index_and_filter_blocks(mut self, yes: bool) -> Self {
        self.cache_index_and_filter_blocks = yes;
        self
    }

    fn db_options(&self) -> RocksOptions {
        let mut options = RocksOptions::default();
        options.create_if_missing(true);
        options.create_missing_column_families(true);
        options.set_wal_compression_type(rocksdb::DBCompressionType::Zstd);
        if self.with_rocksdb_stats {
            options.enable_statistics();
        }
        if self.direct_io {
            options.set_use_direct_reads(true);
            options.set_use_direct_io_for_flush_and_compaction(true);
        }
        options
    }

    fn chunks_cf_options(&self) -> RocksOptions {
        let mut block_based_table_factory = rocksdb::BlockBasedOptions::default();

        if self.chunk_cache_size > 0 {
            let cache = rocksdb::Cache::new_lru_cache(self.chunk_cache_size * 1024 * 1024);
            block_based_table_factory.set_block_cache(&cache);
        } else {
            block_based_table_factory.disable_cache();
        }

        block_based_table_factory.set_cache_index_and_filter_blocks(self.cache_index_and_filter_blocks);

        let mut options = RocksOptions::default();
        options.set_block_based_table_factory(&block_based_table_factory);
        options
    }

    fn tables_cf_options(&self) -> RocksOptions {
        let mut block_based_table_factory = rocksdb::BlockBasedOptions::default();

        if self.data_cache_size > 0 {
            let cache = rocksdb::Cache::new_lru_cache(self.data_cache_size * 1024 * 1024);
            block_based_table_factory.set_block_cache(&cache);
        } else {
            block_based_table_factory.disable_cache();
        }

        block_based_table_factory.set_cache_index_and_filter_blocks(self.cache_index_and_filter_blocks);

        let mut options = RocksOptions::default();
        options.set_block_based_table_factory(&block_based_table_factory);
        options.set_compression_type(rocksdb::DBCompressionType::Lz4);
        options
    }

    fn cf_default_options(&self) -> RocksOptions {
        let mut block_based_table_factory = rocksdb::BlockBasedOptions::default();
        let mut options = RocksOptions::default();
        block_based_table_factory.set_cache_index_and_filter_blocks(self.cache_index_and_filter_blocks);
        options.set_block_based_table_factory(&block_based_table_factory);
        options
    }
    
    pub fn open(&self, path: impl AsRef<Path>) -> anyhow::Result<Database> {
        let options = self.db_options();
        
        let db = RocksDB::open_cf_descriptors(&options, path, [
            ColumnFamilyDescriptor::new(CF_DATASETS, self.cf_default_options()),
            ColumnFamilyDescriptor::new(CF_CHUNKS, self.chunks_cf_options()),
            ColumnFamilyDescriptor::new(CF_TABLES, self.tables_cf_options()),
            ColumnFamilyDescriptor::new(CF_DIRTY_TABLES, self.cf_default_options()),
            ColumnFamilyDescriptor::new(CF_DELETED_TABLES, self.cf_default_options())
        ])?;
        
        Ok(Database {
            db,
            options
        })
    }
}


pub struct Database {
    db: RocksDB,
    options: RocksOptions
}


impl Database {
    pub fn create_dataset(&self, id: DatasetId, kind: DatasetKind) -> anyhow::Result<()> {
        Tx::new(&self.db).run(|tx| {
            let label = tx.find_label_for_update(id)?;
            ensure!(label.is_none(), "dataset {} already exists", id);
            tx.write_label(id, &DatasetLabel::V0 {
                kind,
                version: 0,
                finalized_head: None
            })
        })
    }

    pub fn create_dataset_if_not_exists(&self, id: DatasetId, kind: DatasetKind) -> anyhow::Result<()> {
        Tx::new(&self.db).run(|tx| {
            if let Some(label) = tx.find_label_for_update(id)? {
                ensure!(
                    label.kind() == kind,
                    "wanted to create dataset {} of kind {}, but it already exists with kind {}",
                    id,
                    label.kind(),
                    kind
                );
                Ok(())
            } else {
                tx.write_label(id, &DatasetLabel::V0 {
                    kind,
                    version: 0,
                    finalized_head: None
                })
            }
        })
    }

    pub fn new_table_builder(&self, schema: SchemaRef) -> TableBuilder<'_> {
        TableBuilder::new(&self.db, schema)
    }

    pub fn insert_chunk(
        &self,
        dataset_id: DatasetId,
        chunk: &Chunk
    ) -> anyhow::Result<()>
    {
        self.update_dataset(dataset_id, |tx| {
            tx.insert_chunk(chunk)
        })
    }
    
    pub fn insert_fork(
        &self,
        dataset_id: DatasetId,
        chunk: &Chunk
    ) -> anyhow::Result<()>
    {
        self.update_dataset(dataset_id, |tx| {
            tx.insert_fork(chunk)
        })
    }
    
    pub fn update_dataset<F, R>(
        &self, 
        dataset_id: DatasetId, 
        mut cb: F
    ) -> anyhow::Result<R> 
    where 
        F: FnMut(&mut DatasetUpdate<'_>) -> anyhow::Result<R>
    {
        Tx::new(&self.db).run(|tx| {
            let mut upd = DatasetUpdate::new(tx, dataset_id)?;
            let result = cb(&mut upd)?;
            upd.finish()?;
            Ok(result)
        })
    }

    pub fn snapshot(&self) -> ReadSnapshot<'_> {
        ReadSnapshot::new(&self.db)
    }

    pub fn get_all_datasets(&self) -> anyhow::Result<Vec<Dataset>> {
        let cursor = self.db.raw_iterator_cf(
            self.db.cf_handle(CF_DATASETS).unwrap()
        );
        list_all_datasets(cursor).collect()
    }

    pub fn perform_dataset_compaction(
        &self,
        dataset_id: DatasetId,
        max_chunk_size: Option<usize>,
        write_amplification_limit: Option<f64>,
        compaction_len_limit: Option<usize>,
    ) -> anyhow::Result<CompactionStatus>
    {
        perform_dataset_compaction(&self.db, dataset_id, max_chunk_size, write_amplification_limit, compaction_len_limit)
    }

    pub fn delete_dataset(&self, dataset_id: DatasetId) -> anyhow::Result<()> {
        Tx::new(&self.db).run(|tx| {
            let label = tx.find_label_for_update(dataset_id)?;
            if label.is_none() {
                return Ok(());
            }

            let chunks = tx.list_chunks(dataset_id, 0, None);
            for chunk_result in chunks {
                let chunk = chunk_result?;
                tx.delete_chunk(dataset_id, &chunk)?;
            }

            tx.delete_label(dataset_id)?;

            Ok(())
        })?;

        self.cleanup()?;
        Ok(())
    }

    pub fn cleanup(&self) -> anyhow::Result<usize> {
        deleted_deleted_tables(&self.db)
    }

    pub fn get_statistics(&self) -> Option<String> {
        self.options.get_statistics()
    }

    pub fn get_property(&self, cf: &str, name: &str) -> anyhow::Result<Option<String>> {
        let Some(cf_handle) = self.db.cf_handle(cf) else {
            return Ok(None)
        };
        let val = self.db.property_value_cf(cf_handle, name)?;
        Ok(val)
    }
}


impl std::fmt::Debug for Database {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Database")
            .field("path", &self.db.path())
            .finish()
    }
}