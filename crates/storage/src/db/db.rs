use super::data::{Dataset, DatasetId, DatasetKind, DatasetLabel};
use super::read::snapshot::ReadSnapshot;
use super::write::chunk::ChunkBuilder;
use crate::db::ops::{perform_dataset_compaction, CompactionStatus};
use crate::db::read::datasets::list_all_datasets;
use crate::db::write::tx::Tx;
use crate::db::Chunk;
use anyhow::ensure;
use rocksdb::{ColumnFamilyDescriptor, Options as RocksOptions};
use sqd_primitives::Name;
use std::path::Path;


pub(super) const CF_DATASETS: Name = "DATASETS";
pub(super) const CF_CHUNKS: Name = "CHUNKS";
pub(super) const CF_TABLES: Name = "TABLES";
pub(super) const CF_DIRTY_TABLES: Name = "DIRTY_TABLES";


pub(super) type RocksDB = rocksdb::OptimisticTransactionDB;
pub(super) type RocksTransaction<'a> = rocksdb::Transaction<'a, RocksDB>;
pub(super) type RocksTransactionIterator<'a> = rocksdb::DBRawIteratorWithThreadMode<'a, RocksTransaction<'a>>;
pub(super) type RocksTransactionOptions = rocksdb::OptimisticTransactionOptions;
pub(super) type RocksSnapshot<'a> = rocksdb::SnapshotWithThreadMode<'a, RocksDB>;
pub(super) type RocksSnapshotIterator<'a> = rocksdb::DBRawIteratorWithThreadMode<'a, RocksDB>;
pub(super) type RocksWriteBatch = rocksdb::WriteBatchWithTransaction<true>;


pub struct DatabaseSettings {
    data_cache_size: usize,
    with_rocksdb_stats: bool
}


impl Default for DatabaseSettings {
    fn default() -> Self {
        Self {
            data_cache_size: 32,
            with_rocksdb_stats: false
        }
    }
}


impl DatabaseSettings {
    pub fn set_data_cache_size(self, mb: usize) -> Self {
        Self {
            data_cache_size: mb,
            ..self
        }
    }
    
    pub fn with_rocksdb_stats(self, on: bool) -> Self {
        Self {
            with_rocksdb_stats: on,
            ..self
        }
    }
    
    fn db_options(&self) -> RocksOptions {
        let mut options = RocksOptions::default();
        options.create_if_missing(true);
        options.create_missing_column_families(true);
        options.set_wal_compression_type(rocksdb::DBCompressionType::Zstd);
        if self.with_rocksdb_stats {
            options.enable_statistics();
        }
        options
    }
    
    fn tables_cf_options(&self) -> RocksOptions {
        let mut options = RocksOptions::default();
        options.set_compression_type(rocksdb::DBCompressionType::Lz4);

        let mut block_based_table_factory = rocksdb::BlockBasedOptions::default();
        if self.data_cache_size > 0 {
            let cache = rocksdb::Cache::new_lru_cache(self.data_cache_size * 1024 * 1024);
            block_based_table_factory.set_block_cache(&cache);
        } else {
            block_based_table_factory.disable_cache();
        }
        options.set_block_based_table_factory(&block_based_table_factory);
        
        options
    }
    
    pub fn open(&self, path: impl AsRef<Path>) -> anyhow::Result<Database> {
        let options = self.db_options();
        
        let db = RocksDB::open_cf_descriptors(&options, path, [
            ColumnFamilyDescriptor::new(CF_DATASETS, RocksOptions::default()),
            ColumnFamilyDescriptor::new(CF_CHUNKS, RocksOptions::default()),
            ColumnFamilyDescriptor::new(CF_TABLES, self.tables_cf_options()),
            ColumnFamilyDescriptor::new(CF_DIRTY_TABLES, RocksOptions::default())
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
            tx.write_label(id, &DatasetLabel {
                kind,
                version: 0
            })
        })
    }

    pub fn create_dataset_if_not_exists(&self, id: DatasetId, kind: DatasetKind) -> anyhow::Result<()> {
        Tx::new(&self.db).run(|tx| {
            if let Some(label) = tx.find_label_for_update(id)? {
                ensure!(
                    label.kind == kind,
                    "wanted to create dataset {} of kind {}, but it already exists with kind {}",
                    id,
                    label.kind,
                    kind
                );
                Ok(())
            } else {
                tx.write_label(id, &DatasetLabel {
                    kind,
                    version: 0
                })
            }
        })
    }

    pub fn new_chunk_builder(&self) -> ChunkBuilder<'_> {
        ChunkBuilder::new(&self.db)
    }

    pub fn insert_chunk(
        &self,
        dataset_id: DatasetId,
        chunk: &Chunk,
        prev_block_hash: Option<&str>
    ) -> anyhow::Result<()>
    {
        Tx::new_with_snapshot(&self.db).run(|tx| {
            let mut label = tx.get_label_for_update(dataset_id)?;
            label.version += 1;
            tx.write_label(dataset_id, &label)?;
            tx.validate_chunk_insertion(dataset_id, chunk, prev_block_hash)?;
            tx.write_chunk(dataset_id, &chunk)
        })
    }

    pub fn snapshot(&self) -> ReadSnapshot<'_> {
        ReadSnapshot::new(&self.db)
    }

    pub fn perform_dataset_compaction(&self, dataset_id: DatasetId) -> anyhow::Result<CompactionStatus> {
        perform_dataset_compaction(&self.db, dataset_id)
    }

    pub fn get_all_datasets(&self) -> anyhow::Result<Vec<Dataset>> {
        let cursor = self.db.raw_iterator_cf(
            self.db.cf_handle(CF_DATASETS).unwrap()
        );
        list_all_datasets(cursor).collect()
    }

    pub fn get_statistics(&self) -> Option<String> {
        self.options.get_statistics()
    }
 }