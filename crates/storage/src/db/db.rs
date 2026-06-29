use std::path::Path;

use anyhow::ensure;
use arrow::datatypes::SchemaRef;
use rocksdb::{ColumnFamilyDescriptor, Options as RocksOptions};
use sqd_primitives::Name;

use super::{
    data::{Dataset, DatasetId, DatasetKind, DatasetLabel},
    read::snapshot::ReadSnapshot
};
use crate::db::{
    ops::{perform_dataset_compaction, CompactionStatus},
    read::datasets::list_all_datasets,
    write::{ops as cleanup_ops, table_builder::TableBuilder, tx::Tx},
    Chunk, DatasetUpdate
};

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
    max_log_file_size: usize,
    keep_log_file_num: usize,
    auto_compactions: bool
}

impl Default for DatabaseSettings {
    fn default() -> Self {
        Self {
            chunk_cache_size: 64,
            data_cache_size: 256,
            with_rocksdb_stats: false,
            direct_io: false,
            cache_index_and_filter_blocks: false,
            max_log_file_size: 10,
            keep_log_file_num: 10,
            auto_compactions: true
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

    /// Max size of a single info log file in MB (0 means unlimited)
    pub fn with_max_log_file_size(mut self, mb: usize) -> Self {
        self.max_log_file_size = mb;
        self
    }

    /// Max number of info log files to keep
    pub fn with_keep_log_file_num(mut self, count: usize) -> Self {
        self.keep_log_file_num = count;
        self
    }

    /// Enable/disable RocksDB background auto-compaction of the table data.
    /// Defaults to `true`; off mainly for tests needing deterministic compaction.
    pub fn with_auto_compactions(mut self, yes: bool) -> Self {
        self.auto_compactions = yes;
        self
    }

    fn db_options(&self) -> RocksOptions {
        let mut options = RocksOptions::default();
        options.create_if_missing(true);
        options.create_missing_column_families(true);
        options.set_wal_compression_type(rocksdb::DBCompressionType::Zstd);
        // Bound info log (LOG, LOG.old.*) growth
        options.set_max_log_file_size(self.max_log_file_size * 1024 * 1024);
        options.set_keep_log_file_num(self.keep_log_file_num);
        // Keep compaction ahead of ingest + deletion tombstones: the default 2 jobs
        // could not keep up during the NET-819 incident, and routine reclaim now relies
        // entirely on compaction (the file unlink is startup-only). Load-bearing.
        options.set_max_background_jobs(8);
        options.set_max_subcompactions(4);
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
        // Find tombstone-heavy SSTs and bound staleness so no dead file lingers. Deleting
        // a table point-deletes all of its keys, so its SSTs turn dense with tombstones and
        // the deletion collector compacts them out; the 24h periodic compaction backstops
        // any file that never crosses the density threshold. Thresholds are provisional.
        options.add_compact_on_deletion_collector_factory(128 * 1024, 64 * 1024, 0.5);
        options.set_periodic_compaction_seconds(24 * 60 * 60);
        // Bound space amplification under leveled compaction (default since RocksDB 8.4;
        // pinned because it is load-bearing here).
        options.set_level_compaction_dynamic_level_bytes(true);
        if !self.auto_compactions {
            options.set_disable_auto_compactions(true);
        }
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

        let db = RocksDB::open_cf_descriptors(
            &options,
            path,
            [
                ColumnFamilyDescriptor::new(CF_DATASETS, self.cf_default_options()),
                ColumnFamilyDescriptor::new(CF_CHUNKS, self.chunks_cf_options()),
                ColumnFamilyDescriptor::new(CF_TABLES, self.tables_cf_options()),
                ColumnFamilyDescriptor::new(CF_DIRTY_TABLES, self.cf_default_options()),
                ColumnFamilyDescriptor::new(CF_DELETED_TABLES, self.cf_default_options())
            ]
        )?;

        Ok(Database { db, options })
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
            tx.write_label(
                id,
                &DatasetLabel::V0 {
                    kind,
                    version: 0,
                    finalized_head: None
                }
            )
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
                tx.write_label(
                    id,
                    &DatasetLabel::V0 {
                        kind,
                        version: 0,
                        finalized_head: None
                    }
                )
            }
        })
    }

    pub fn new_table_builder(&self, schema: SchemaRef) -> TableBuilder<'_> {
        TableBuilder::new(&self.db, schema)
    }

    pub fn insert_chunk(&self, dataset_id: DatasetId, chunk: &Chunk) -> anyhow::Result<()> {
        self.update_dataset(dataset_id, |tx| tx.insert_chunk(chunk))
    }

    pub fn insert_fork(&self, dataset_id: DatasetId, chunk: &Chunk) -> anyhow::Result<()> {
        self.update_dataset(dataset_id, |tx| tx.insert_fork(chunk))
    }

    pub fn update_dataset<F, R>(&self, dataset_id: DatasetId, mut cb: F) -> anyhow::Result<R>
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
        let cursor = self.db.raw_iterator_cf(self.db.cf_handle(CF_DATASETS).unwrap());
        list_all_datasets(cursor).collect()
    }

    pub fn perform_dataset_compaction(
        &self,
        dataset_id: DatasetId,
        max_chunk_size: Option<usize>,
        write_amplification_limit: Option<f64>,
        compaction_len_limit: Option<usize>
    ) -> anyhow::Result<CompactionStatus> {
        perform_dataset_compaction(
            &self.db,
            dataset_id,
            max_chunk_size,
            write_amplification_limit,
            compaction_len_limit
        )
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

    /// Phase 1 -- logically purge deleted tables (snapshot-safe point deletes).
    /// Returns the number of tables logically deleted by this call.
    pub fn cleanup(&self) -> anyhow::Result<usize> {
        cleanup_ops::logical_cleanup(&self.db)
    }

    /// Physically reclaim disk by unlinking whole SST files below the live watermark
    /// (min live `TableId`). No writes, so it works even at a full disk, unlike
    /// compaction. IGNORES snapshots, so it is safe only where no live reader exists --
    /// today only at STARTUP. See [`cleanup_ops::reclaim_disk_space`] for the trade-off.
    pub fn reclaim_disk_space(&self) -> anyhow::Result<()> {
        cleanup_ops::reclaim_disk_space(&self.db)
    }

    /// Crash recovery: purge `DIRTY_TABLES` markers left by builds that died before
    /// commit -- an orphan's id otherwise pins the reclaim watermark forever (see
    /// [`Database::reclaim_disk_space`]). MUST run before any ingest starts (e.g. at
    /// startup): it treats every dirty marker as an orphan. Returns orphans purged.
    pub fn purge_orphan_dirty_tables(&self) -> anyhow::Result<usize> {
        cleanup_ops::purge_orphan_dirty_tables(&self.db)
    }

    /// Flush the table-data column family's memtable to SST files (e.g. before a
    /// reclaim, so freshly written data is unlinkable).
    pub fn flush(&self) -> anyhow::Result<()> {
        self.db.flush_cf(self.db.cf_handle(CF_TABLES).unwrap())?;
        Ok(())
    }

    /// Force a full compaction of the table-data column family. Unlike
    /// [`Database::reclaim_disk_space`] this *writes* (unsafe at a full disk); it pushes
    /// data to the bottom level and rewrites tombstone-heavy files the unlink can't reach.
    pub fn compact_tables(&self) {
        let cf = self.db.cf_handle(CF_TABLES).unwrap();
        self.db.compact_range_cf(cf, None::<&[u8]>, None::<&[u8]>);
    }

    pub fn get_statistics(&self) -> Option<String> {
        self.options.get_statistics()
    }

    pub fn get_property(&self, cf: &str, name: &str) -> anyhow::Result<Option<String>> {
        let Some(cf_handle) = self.db.cf_handle(cf) else {
            return Ok(None);
        };
        let val = self.db.property_value_cf(cf_handle, name)?;
        Ok(val)
    }
}

impl std::fmt::Debug for Database {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Database").field("path", &self.db.path()).finish()
    }
}
