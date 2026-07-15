use std::path::Path;

use anyhow::ensure;
use arrow::datatypes::SchemaRef;
use parking_lot::Mutex;
use rocksdb::{ColumnFamilyDescriptor, Options as RocksOptions};
use sqd_primitives::Name;

use super::{
    data::{Dataset, DatasetId, DatasetKind, DatasetLabel, HashIndexKey},
    read::snapshot::ReadSnapshot
};
use crate::db::{
    ops::{perform_dataset_compaction, CompactionStatus},
    read::datasets::list_all_datasets,
    write::{
        ops as cleanup_ops,
        table_builder::TableBuilder,
        tx::{HashIndexWriteMetrics, Tx}
    },
    Chunk, DatasetUpdate
};

// Public so out-of-process readers (`reclaim-measure`) don't copy the strings.
pub const CF_DATASETS: Name = "DATASETS";
pub const CF_CHUNKS: Name = "CHUNKS";
pub const CF_TABLES: Name = "TABLES";
pub const CF_DIRTY_TABLES: Name = "DIRTY_TABLES";
pub const CF_DELETED_TABLES: Name = "DELETED_TABLES";
pub const CF_BLOCK_HASHES: Name = "BLOCK_HASHES";
pub const CF_TRANSACTION_HASHES: Name = "TRANSACTION_HASHES";

/// Whole-file rewrite cadence for `CF_TABLES`. RocksDB leaves `periodic_compaction_seconds`
/// disabled for leveled compaction without a compaction filter, so the effective baseline
/// is the separate 30-day `ttl` default. 7 days buys ~4x that rewrite rate.
pub const DEFAULT_PERIODIC_COMPACTION_SECS: u64 = 7 * 24 * 60 * 60;

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
    auto_compactions: bool,
    max_background_jobs: usize,
    periodic_compaction_secs: u64,
    block_hash_index: bool,
    transaction_hash_index: bool
}

/// RocksDB's default of 2 could not keep up with ingest during the NET-819 incident, but a
/// fixed 8 would just thrash a 2-core node.
fn default_max_background_jobs() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(2)
        .clamp(2, 8)
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
            auto_compactions: true,
            max_background_jobs: default_max_background_jobs(),
            periodic_compaction_secs: DEFAULT_PERIODIC_COMPACTION_SECS,
            block_hash_index: false,
            transaction_hash_index: false
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

    /// Concurrent RocksDB background flush + compaction jobs. Defaults to the core count,
    /// clamped to `2..=8`.
    pub fn with_max_background_jobs(mut self, jobs: usize) -> Self {
        self.max_background_jobs = jobs.max(1);
        self
    }

    /// Rewrite every `CF_TABLES` SST older than this, collecting dead data the deletion
    /// collector never triggers on. `0` disables it, leaving the 30-day `ttl` as the only
    /// backstop. See [`DEFAULT_PERIODIC_COMPACTION_SECS`].
    pub fn with_periodic_compaction_secs(mut self, secs: u64) -> Self {
        self.periodic_compaction_secs = secs;
        self
    }

    /// Whether newly ingested chunks get their block hashes indexed in
    /// `CF_BLOCK_HASHES`. Write-side only: entries are always removed when
    /// their chunk is pruned, so the index drains after the flag goes off.
    pub fn with_block_hash_index(mut self, yes: bool) -> Self {
        self.block_hash_index = yes;
        self
    }

    /// Whether newly ingested chunks get their transaction hashes indexed in
    /// `CF_TRANSACTION_HASHES`. Independent of block-hash indexing because the
    /// transaction index is commonly orders of magnitude larger.
    pub fn with_transaction_hash_index(mut self, yes: bool) -> Self {
        self.transaction_hash_index = yes;
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
        // Keep compaction ahead of ingest + deletion tombstones: the default 2 jobs could
        // not keep up during the NET-819 incident, and routine reclaim now relies entirely
        // on compaction (the file unlink is startup-only).
        options.set_max_background_jobs(self.max_background_jobs as i32);
        options.set_max_subcompactions((self.max_background_jobs as u32 / 2).max(1));
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
        // A table purge point-deletes all of its keys, so its SSTs turn tombstone-dense and
        // the deletion collector compacts them out; periodic compaction backstops files
        // that never cross the density threshold, rewriting live data along with them.
        // Thresholds are provisional.
        options.add_compact_on_deletion_collector_factory(128 * 1024, 64 * 1024, 0.5);
        if self.periodic_compaction_secs > 0 {
            options.set_periodic_compaction_seconds(self.periodic_compaction_secs);
        }
        // Bound space amplification (default since RocksDB 8.4; pinned -- load-bearing here).
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

    /// Point-lookup indexes use full Bloom filters to make misses cheap.
    /// Retention produces dense runs of tombstones, so the same bounded
    /// deletion/periodic-compaction policy used for table data keeps index
    /// space converging in the background.
    fn hash_index_cf_options(&self, compression: rocksdb::DBCompressionType) -> RocksOptions {
        let mut block_based_table_factory = rocksdb::BlockBasedOptions::default();
        block_based_table_factory.set_cache_index_and_filter_blocks(self.cache_index_and_filter_blocks);
        block_based_table_factory.set_bloom_filter(10.0, false);
        block_based_table_factory.set_optimize_filters_for_memory(true);

        let mut options = RocksOptions::default();
        options.set_block_based_table_factory(&block_based_table_factory);
        options.set_compression_type(compression);
        options.add_compact_on_deletion_collector_factory(128 * 1024, 64 * 1024, 0.5);
        if self.periodic_compaction_secs > 0 {
            options.set_periodic_compaction_seconds(self.periodic_compaction_secs);
        }
        options.set_level_compaction_dynamic_level_bytes(true);
        if !self.auto_compactions {
            options.set_disable_auto_compactions(true);
        }
        options
    }

    pub fn open(&self, path: impl AsRef<Path>) -> anyhow::Result<Database> {
        // Keep the established block index on Snappy: the A/B sizing probe
        // found less than 0.5% difference, which does not justify rewriting
        // existing SSTs. The new transaction index uses LZ4 like table data.
        self.open_with_hash_index_compressions(
            path,
            rocksdb::DBCompressionType::Snappy,
            rocksdb::DBCompressionType::Lz4
        )
    }

    fn open_with_hash_index_compressions(
        &self,
        path: impl AsRef<Path>,
        block_hash_compression: rocksdb::DBCompressionType,
        transaction_hash_compression: rocksdb::DBCompressionType
    ) -> anyhow::Result<Database> {
        let options = self.db_options();

        let db = RocksDB::open_cf_descriptors(
            &options,
            path,
            [
                ColumnFamilyDescriptor::new(CF_DATASETS, self.cf_default_options()),
                ColumnFamilyDescriptor::new(CF_CHUNKS, self.chunks_cf_options()),
                ColumnFamilyDescriptor::new(CF_TABLES, self.tables_cf_options()),
                ColumnFamilyDescriptor::new(CF_DIRTY_TABLES, self.cf_default_options()),
                ColumnFamilyDescriptor::new(CF_DELETED_TABLES, self.cf_default_options()),
                ColumnFamilyDescriptor::new(CF_BLOCK_HASHES, self.hash_index_cf_options(block_hash_compression)),
                ColumnFamilyDescriptor::new(
                    CF_TRANSACTION_HASHES,
                    self.hash_index_cf_options(transaction_hash_compression)
                )
            ]
        )?;

        Ok(Database {
            db,
            options,
            block_hash_index: self.block_hash_index,
            transaction_hash_index: self.transaction_hash_index,
            lifecycle_lock: Mutex::new(())
        })
    }
}

pub struct Database {
    db: RocksDB,
    options: RocksOptions,
    block_hash_index: bool,
    transaction_hash_index: bool,
    /// Serializes only CREATE/DROP so a dataset ID cannot be reused before a
    /// prior incarnation's derived-index prefixes have been physically purged.
    lifecycle_lock: Mutex<()>
}

impl Database {
    pub fn create_dataset(&self, id: DatasetId, kind: DatasetKind) -> anyhow::Result<()> {
        let _lifecycle_guard = self.lifecycle_lock.lock();
        self.purge_stale_hash_indexes_if_dataset_absent(id)?;

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
        let _lifecycle_guard = self.lifecycle_lock.lock();
        self.purge_stale_hash_indexes_if_dataset_absent(id)?;

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

    pub fn update_dataset<F, R>(&self, dataset_id: DatasetId, cb: F) -> anyhow::Result<R>
    where
        F: FnMut(&mut DatasetUpdate<'_>) -> anyhow::Result<R>
    {
        let mut metrics = HashIndexWriteMetrics::default();
        self.update_dataset_with_hash_index_metrics(dataset_id, &mut metrics, cb)
    }

    /// Runs a dataset update and returns aggregate block/transaction hash-index
    /// staging time through `metrics`. Work repeated by optimistic transaction
    /// retries is included.
    pub fn update_dataset_with_hash_index_metrics<F, R>(
        &self,
        dataset_id: DatasetId,
        metrics: &mut HashIndexWriteMetrics,
        mut cb: F
    ) -> anyhow::Result<R>
    where
        F: FnMut(&mut DatasetUpdate<'_>) -> anyhow::Result<R>
    {
        Tx::new(&self.db)
            .with_block_hash_index(self.block_hash_index)
            .with_transaction_hash_index(self.transaction_hash_index)
            .run_with_hash_index_metrics(metrics, |tx| {
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
        let _lifecycle_guard = self.lifecycle_lock.lock();

        // Metadata is removed atomically first. Hash lookups check the label in
        // their snapshot, so the logical indexes disappear in this same commit;
        // bounded physical cleanup below cannot expose stale hits.
        Tx::new(&self.db).run(|tx| {
            if tx.find_label_for_update(dataset_id)?.is_none() {
                return Ok(());
            }
            for chunk_result in tx.list_chunks(dataset_id, 0, None) {
                let chunk = chunk_result?;
                tx.delete_chunk(dataset_id, &chunk)?;
            }
            tx.delete_label(dataset_id)
        })?;

        // The logical DROP has committed. Errors below mean physical cleanup
        // is incomplete, not that the dataset is still visible; retry is safe.
        self.purge_hash_indexes(dataset_id)?;
        self.cleanup()?;
        Ok(())
    }

    /// A crash after DROP's logical commit may leave physical index keys. An
    /// absent dataset must purge those keys before publishing a new label for
    /// the same ID, or a later incarnation could inherit stale entries.
    fn purge_stale_hash_indexes_if_dataset_absent(&self, dataset_id: DatasetId) -> anyhow::Result<()> {
        if !self.snapshot().has_dataset(dataset_id)? {
            self.purge_hash_indexes(dataset_id)?;
        }
        Ok(())
    }

    fn purge_hash_indexes(&self, dataset_id: DatasetId) -> anyhow::Result<()> {
        self.purge_hash_index(CF_BLOCK_HASHES, dataset_id)?;
        self.purge_hash_index(CF_TRANSACTION_HASHES, dataset_id)
    }

    /// Point-deletes every hash-index entry of `dataset_id` in bounded batches
    /// (`delete_range` is not supported on an optimistic transaction).
    /// Runs after logical DROP; the absent dataset label makes these physical
    /// entries invisible to hash lookups while cleanup proceeds.
    fn purge_hash_index(&self, cf_name: Name, dataset_id: DatasetId) -> anyhow::Result<()> {
        const BATCH_SIZE: usize = 10_000;

        let cf = self.db.cf_handle(cf_name).unwrap();
        let (start, end) = HashIndexKey::dataset_range(dataset_id);

        let mut read_opts = rocksdb::ReadOptions::default();
        read_opts.set_iterate_upper_bound(end);

        let mut cursor = self.db.raw_iterator_cf_opt(cf, read_opts);
        cursor.seek(&start);

        let mut batch = RocksWriteBatch::default();
        while cursor.valid() {
            batch.delete_cf(cf, cursor.key().unwrap());
            if batch.len() >= BATCH_SIZE {
                self.db.write(std::mem::take(&mut batch))?;
            }
            cursor.next();
        }
        cursor.status()?;

        if !batch.is_empty() {
            self.db.write(batch)?;
        }
        Ok(())
    }

    /// Phase 1 -- logically purge deleted tables (snapshot-safe point deletes).
    /// Returns the number of tables logically deleted by this call.
    pub fn cleanup(&self) -> anyhow::Result<usize> {
        cleanup_ops::logical_cleanup(&self.db)
    }

    /// Physically reclaim disk by unlinking whole SST files below the live watermark
    /// (min live `TableId`). Needs no scratch space, unlike compaction, which must write
    /// its merged output before dropping the inputs. IGNORES snapshots, so it is safe only
    /// where no live reader exists -- today only at STARTUP.
    /// See [`cleanup_ops::reclaim_disk_space`].
    pub fn reclaim_disk_space(&self) -> anyhow::Result<()> {
        cleanup_ops::reclaim_disk_space(&self.db)
    }

    /// Crash recovery: purge `DIRTY_TABLES` markers left by builds that died before
    /// commit -- an orphan pins the reclaim watermark forever. MUST run before any ingest
    /// starts: it treats every dirty marker as an orphan. Returns orphans purged.
    pub fn purge_orphan_dirty_tables(&self) -> anyhow::Result<usize> {
        cleanup_ops::purge_orphan_dirty_tables(&self.db)
    }

    /// Flush `CF_TABLES`'s memtable to SST files (e.g. before a reclaim, so freshly
    /// written data is unlinkable). Bookkeeping column families are not flushed.
    pub fn flush_tables(&self) -> anyhow::Result<()> {
        self.db.flush_cf(self.db.cf_handle(CF_TABLES).unwrap())?;
        Ok(())
    }

    /// Force a full compaction of the table-data column family. Test support: production
    /// relies on background compaction. Rewrites files, so it needs scratch space -- the
    /// very thing that deadlocks on the full disk [`Database::reclaim_disk_space`] exists for.
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

    /// Read an integer RocksDB property for a column family.
    ///
    /// Returns `None` when either the column family or property does not exist. Intrinsic
    /// properties are available even when RocksDB statistics collection is disabled.
    pub fn get_int_property(&self, cf: &str, name: &str) -> anyhow::Result<Option<u64>> {
        let Some(cf_handle) = self.db.cf_handle(cf) else {
            return Ok(None);
        };
        let val = self.db.property_int_value_cf(cf_handle, name)?;
        Ok(val)
    }
}

impl std::fmt::Debug for Database {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Database").field("path", &self.db.path()).finish()
    }
}

#[cfg(test)]
mod tests {
    use std::fmt::Write as _;

    use super::*;

    fn realistic_hash(sequence: u64) -> String {
        let mut state = sequence;
        let mut hash = String::with_capacity(66);
        hash.push_str("0x");
        for _ in 0..4 {
            state = state.wrapping_add(0x9e3779b97f4a7c15);
            let mut word = state;
            word = (word ^ (word >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
            word = (word ^ (word >> 27)).wrapping_mul(0x94d049bb133111eb);
            word ^= word >> 31;
            write!(hash, "{word:016x}").unwrap();
        }
        hash
    }

    fn transaction_position(sequence: u64) -> [u8; 12] {
        const TRANSACTIONS_PER_BLOCK: u64 = 100;

        let block_number = sequence / TRANSACTIONS_PER_BLOCK;
        let transaction_index = u32::try_from(sequence % TRANSACTIONS_PER_BLOCK).unwrap();
        let mut bytes = [0; 12];
        bytes[..8].copy_from_slice(&block_number.to_be_bytes());
        bytes[8..].copy_from_slice(&transaction_index.to_be_bytes());
        bytes
    }

    fn write_hash_index_entries(db: &Database, dataset_id: DatasetId, first: u64, end: u64) {
        const WRITE_BATCH_SIZE: u64 = 10_000;

        let block_cf = db.db.cf_handle(CF_BLOCK_HASHES).unwrap();
        let transaction_cf = db.db.cf_handle(CF_TRANSACTION_HASHES).unwrap();
        let mut batch_first = first;
        while batch_first < end {
            let batch_end = batch_first.saturating_add(WRITE_BATCH_SIZE).min(end);
            let mut batch = RocksWriteBatch::default();
            let mut key = HashIndexKey::new(dataset_id, "");
            for sequence in batch_first..batch_end {
                key.set_hash(&realistic_hash(sequence));
                batch.put_cf(block_cf, &key, sequence.to_be_bytes());
                batch.put_cf(transaction_cf, &key, transaction_position(sequence));
            }
            db.db.write(batch).unwrap();
            batch_first = batch_end;
        }
    }

    fn flush_and_compact_hash_indexes(db: &Database) {
        for cf_name in [CF_BLOCK_HASHES, CF_TRANSACTION_HASHES] {
            let cf = db.db.cf_handle(cf_name).unwrap();
            db.db.flush_cf(cf).unwrap();
            db.db.compact_range_cf(cf, None::<&[u8]>, None::<&[u8]>);
        }
    }

    fn print_hash_index_size(db: &Database, codec: &str, cf_name: &str, index: &str, entries: u64) {
        let live_sst_bytes = db
            .get_int_property(cf_name, "rocksdb.live-sst-files-size")
            .unwrap()
            .unwrap();
        let bytes_per_entry =
            f64::from(u32::try_from(live_sst_bytes).unwrap()) / f64::from(u32::try_from(entries).unwrap());
        let gib_per_billion = bytes_per_entry * 1_000_000_000.0 / 1024.0_f64.powi(3);

        eprintln!(
            "hash_index_compression_size codec={codec} index={index} entries={entries} live_sst_bytes={live_sst_bytes} bytes_per_entry={bytes_per_entry:.2} gib_per_billion={gib_per_billion:.2}"
        );
    }

    fn measure_hash_index_compression(compression: rocksdb::DBCompressionType, codec: &str) {
        const TARGETS: [u64; 2] = [100_000, 1_000_000];

        let dir = tempfile::tempdir().unwrap();
        let db = DatabaseSettings::default()
            .with_auto_compactions(false)
            .with_periodic_compaction_secs(0)
            .open_with_hash_index_compressions(dir.path(), compression, compression)
            .unwrap();
        let dataset_id = DatasetId::from_str("compression-measurement");
        let mut indexed_entries = 0;

        for target in TARGETS {
            write_hash_index_entries(&db, dataset_id, indexed_entries, target);
            indexed_entries = target;
            flush_and_compact_hash_indexes(&db);
            print_hash_index_size(&db, codec, CF_BLOCK_HASHES, "block", indexed_entries);
            print_hash_index_size(&db, codec, CF_TRANSACTION_HASHES, "transaction", indexed_entries);
        }
    }

    /// Compares compressed SST footprint with identical random-looking hashes,
    /// production key/value encodings, Bloom filters and full compaction. WAL,
    /// memtables and table data are deliberately outside the measurement.
    ///
    /// Run with:
    /// `cargo test -p sqd-storage --release --lib measure_hash_index_compression_disk_size -- --ignored --nocapture`
    #[test]
    #[ignore = "manual Snappy/LZ4 hash-index disk footprint measurement"]
    fn measure_hash_index_compression_disk_size() {
        measure_hash_index_compression(rocksdb::DBCompressionType::Snappy, "snappy");
        measure_hash_index_compression(rocksdb::DBCompressionType::Lz4, "lz4");
    }

    #[test]
    fn create_purges_crash_residue_before_publishing_the_dataset_label() {
        let dir = tempfile::tempdir().unwrap();
        let db = DatabaseSettings::default().open(dir.path()).unwrap();
        let dataset_id = DatasetId::from_str("reused-dataset");
        let hash = "0xstale";
        let key = HashIndexKey::new(dataset_id, hash);

        db.db
            .put_cf(db.db.cf_handle(CF_BLOCK_HASHES).unwrap(), &key, [0; 8])
            .unwrap();
        db.db
            .put_cf(db.db.cf_handle(CF_TRANSACTION_HASHES).unwrap(), &key, [0; 12])
            .unwrap();

        // A crash after DROP may leave these physical keys, but an absent label
        // keeps them out of the logical indexes.
        assert_eq!(db.snapshot().find_block_by_hash(dataset_id, hash).unwrap(), None);
        assert_eq!(db.snapshot().find_transaction_by_hash(dataset_id, hash).unwrap(), None);

        db.create_dataset(dataset_id, DatasetKind::from_str("evm")).unwrap();

        // CREATE must purge the old incarnation before making the label visible.
        assert_eq!(db.snapshot().find_block_by_hash(dataset_id, hash).unwrap(), None);
        assert_eq!(db.snapshot().find_transaction_by_hash(dataset_id, hash).unwrap(), None);
    }

    #[test]
    fn hash_lookup_presence_gate_does_not_deserialize_the_dataset_label() {
        let dir = tempfile::tempdir().unwrap();
        let db = DatabaseSettings::default().open(dir.path()).unwrap();
        let dataset_id = DatasetId::from_str("raw-label-gate");
        let hash = "0xindexed";
        let key = HashIndexKey::new(dataset_id, hash);
        let block_number = 42_u64;
        let transaction_index = 7_u32;
        let mut transaction_position = [0; 12];
        transaction_position[..8].copy_from_slice(&block_number.to_be_bytes());
        transaction_position[8..].copy_from_slice(&transaction_index.to_be_bytes());

        db.db
            .put_cf(db.db.cf_handle(CF_DATASETS).unwrap(), dataset_id, [u8::MAX])
            .unwrap();
        db.db
            .put_cf(
                db.db.cf_handle(CF_BLOCK_HASHES).unwrap(),
                &key,
                block_number.to_be_bytes()
            )
            .unwrap();
        db.db
            .put_cf(
                db.db.cf_handle(CF_TRANSACTION_HASHES).unwrap(),
                &key,
                transaction_position
            )
            .unwrap();

        let snapshot = db.snapshot();
        assert!(snapshot.get_label(dataset_id).is_err());

        let block = snapshot.find_block_by_hash(dataset_id, hash).unwrap().unwrap();
        assert_eq!(block.number, block_number);
        assert_eq!(block.hash, hash);

        let transaction = snapshot.find_transaction_by_hash(dataset_id, hash).unwrap().unwrap();
        assert_eq!(transaction.block_number, block_number);
        assert_eq!(transaction.transaction_index, transaction_index);
        assert_eq!(transaction.hash, hash);
    }
}
