use std::{
    cell::RefCell,
    cmp::{max, min},
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, Instant}
};

use anyhow::{anyhow, bail, ensure, Context};
use rocksdb::ColumnFamily;
use sqd_primitives::{BlockNumber, ItemIndex};

use crate::db::{
    data::{ChunkId, HashIndexKey},
    db::{
        RocksDB, RocksIterator, RocksTransaction, RocksTransactionOptions, CF_BLOCK_HASHES, CF_CHUNKS, CF_DATASETS,
        CF_DELETED_TABLES, CF_DIRTY_TABLES, CF_TRANSACTION_HASHES
    },
    read::{
        blocks_table::{for_each_block_hash, get_parent_block_hash},
        chunk::ChunkIterator,
        transactions_table::for_each_transaction_hash
    },
    table_id::TableId,
    Chunk, DatasetId, DatasetKind, DatasetLabel, ReadSnapshot
};

static GLOBAL_RESTARTS: AtomicU64 = AtomicU64::new(0);

thread_local! {
    static LOCAL_RESTARTS: RefCell<u64> = RefCell::new(0);
}

pub fn get_global_tx_restarts() -> u64 {
    GLOBAL_RESTARTS.load(Ordering::Relaxed)
}

pub fn get_local_tx_restarts() -> u64 {
    LOCAL_RESTARTS.with_borrow(|val| *val)
}

fn record_restart() {
    GLOBAL_RESTARTS.fetch_add(1, Ordering::SeqCst);
    LOCAL_RESTARTS.with_borrow_mut(|val| *val = val.wrapping_add(1))
}

/// Dataset kinds covered by the derived hash indexes. EVM-only for now;
/// hyperliquid must stay out because its `hash` is not a crypto hash and can
/// collide.
fn is_indexed_kind(kind: DatasetKind) -> bool {
    kind == DatasetKind::from_str("evm")
}

/// Aggregate time spent staging derived hash-index changes in an optimistic
/// storage transaction, including work repeated after transaction conflicts.
/// Timings are collected once per index scan, never once per entry.
#[derive(Clone, Copy, Debug, Default)]
pub struct HashIndexWriteMetrics {
    block_hash_index_duration: Duration,
    block_hash_index_operations: u64,
    transaction_hash_index_duration: Duration,
    transaction_hash_index_operations: u64
}

impl HashIndexWriteMetrics {
    /// Total time spent staging block-hash index changes, if the update
    /// performed any block-hash index scans.
    pub fn block_hash_index_duration(&self) -> Option<Duration> {
        (self.block_hash_index_operations > 0).then_some(self.block_hash_index_duration)
    }

    /// Number of block-hash index scans, including retried transaction attempts.
    pub fn block_hash_index_operations(&self) -> u64 {
        self.block_hash_index_operations
    }

    /// Total time spent staging transaction-hash index changes, if the update
    /// performed any transaction-hash index scans.
    pub fn transaction_hash_index_duration(&self) -> Option<Duration> {
        (self.transaction_hash_index_operations > 0).then_some(self.transaction_hash_index_duration)
    }

    /// Number of transaction-hash index scans, including retried attempts.
    pub fn transaction_hash_index_operations(&self) -> u64 {
        self.transaction_hash_index_operations
    }

    fn record(&mut self, index: HashIndex, duration: Duration) {
        match index {
            HashIndex::Block => {
                self.block_hash_index_duration = self.block_hash_index_duration.saturating_add(duration);
                self.block_hash_index_operations = self.block_hash_index_operations.saturating_add(1);
            }
            HashIndex::Transaction => {
                self.transaction_hash_index_duration = self.transaction_hash_index_duration.saturating_add(duration);
                self.transaction_hash_index_operations = self.transaction_hash_index_operations.saturating_add(1);
            }
        }
    }

    fn merge(&mut self, other: Self) {
        self.block_hash_index_duration = self
            .block_hash_index_duration
            .saturating_add(other.block_hash_index_duration);
        self.block_hash_index_operations = self
            .block_hash_index_operations
            .saturating_add(other.block_hash_index_operations);
        self.transaction_hash_index_duration = self
            .transaction_hash_index_duration
            .saturating_add(other.transaction_hash_index_duration);
        self.transaction_hash_index_operations = self
            .transaction_hash_index_operations
            .saturating_add(other.transaction_hash_index_operations);
    }
}

#[derive(Clone, Copy)]
enum HashIndex {
    Block,
    Transaction
}

pub struct Tx<'a> {
    db: &'a RocksDB,
    transaction: RocksTransaction<'a>,
    block_hash_index: bool,
    transaction_hash_index: bool,
    hash_index_write_metrics: RefCell<HashIndexWriteMetrics>
}

impl<'a> Tx<'a> {
    pub fn new(db: &'a RocksDB) -> Self {
        let mut tx_options = RocksTransactionOptions::default();
        tx_options.set_snapshot(true);

        let transaction = db.transaction_opt(&rocksdb::WriteOptions::default(), &tx_options);

        Self {
            db,
            transaction,
            block_hash_index: false,
            transaction_hash_index: false,
            hash_index_write_metrics: RefCell::new(HashIndexWriteMetrics::default())
        }
    }

    /// Enables block hash indexing for chunks written through this transaction.
    /// Set by [`Database::update_dataset`] from the database-level setting.
    pub fn with_block_hash_index(mut self, yes: bool) -> Self {
        self.block_hash_index = yes;
        self
    }

    /// Enables transaction hash indexing for chunks written through this
    /// transaction. The switch is intentionally independent of block hashes.
    pub fn with_transaction_hash_index(mut self, yes: bool) -> Self {
        self.transaction_hash_index = yes;
        self
    }

    pub fn run<R, F>(self, cb: F) -> anyhow::Result<R>
    where
        F: FnMut(&Self) -> anyhow::Result<R>
    {
        let mut metrics = HashIndexWriteMetrics::default();
        self.run_with_hash_index_metrics(&mut metrics, cb)
    }

    pub(crate) fn run_with_hash_index_metrics<R, F>(
        self,
        metrics: &mut HashIndexWriteMetrics,
        mut cb: F
    ) -> anyhow::Result<R>
    where
        F: FnMut(&Self) -> anyhow::Result<R>
    {
        let db = self.db;
        let block_hash_index = self.block_hash_index;
        let transaction_hash_index = self.transaction_hash_index;
        let mut tx = self;
        loop {
            let result = match cb(&tx) {
                Ok(result) => result,
                Err(err) => {
                    metrics.merge(tx.hash_index_write_metrics.into_inner());
                    return Err(err);
                }
            };
            let (commit_result, attempt_metrics) = tx.commit();
            metrics.merge(attempt_metrics);
            match commit_result {
                Ok(_) => return Ok(result),
                Err(err) if err.kind() == rocksdb::ErrorKind::TryAgain || err.kind() == rocksdb::ErrorKind::Busy => {
                    record_restart();
                    tx = Self::new(db)
                        .with_block_hash_index(block_hash_index)
                        .with_transaction_hash_index(transaction_hash_index)
                }
                Err(err) => return Err(err.into())
            }
        }
    }

    fn commit(self) -> (Result<(), rocksdb::Error>, HashIndexWriteMetrics) {
        let Self {
            transaction,
            hash_index_write_metrics,
            ..
        } = self;
        (transaction.commit(), hash_index_write_metrics.into_inner())
    }

    fn measure_hash_index<R>(&self, index: HashIndex, cb: impl FnOnce() -> anyhow::Result<R>) -> anyhow::Result<R> {
        let started = Instant::now();
        let result = cb();
        self.hash_index_write_metrics
            .borrow_mut()
            .record(index, started.elapsed());
        result
    }

    pub fn find_label_for_update(&self, dataset_id: DatasetId) -> anyhow::Result<Option<DatasetLabel>> {
        let maybe_bytes = self
            .transaction
            .get_pinned_for_update_cf(self.cf_handle(CF_DATASETS), dataset_id, true)?;
        Ok(if let Some(bytes) = maybe_bytes {
            let label = borsh::from_slice(bytes.as_ref())?;
            Some(label)
        } else {
            None
        })
    }

    pub fn get_label_for_update(&self, dataset_id: DatasetId) -> anyhow::Result<DatasetLabel> {
        self.find_label_for_update(dataset_id)
            .and_then(|maybe_chunk| maybe_chunk.ok_or_else(|| anyhow!("dataset {} not found", dataset_id)))
    }

    pub fn write_label(&self, dataset_id: DatasetId, label: &DatasetLabel) -> anyhow::Result<()> {
        self.transaction
            .put_cf(self.cf_handle(CF_DATASETS), dataset_id, &borsh::to_vec(label).unwrap())?;
        Ok(())
    }

    pub fn delete_label(&self, dataset_id: DatasetId) -> anyhow::Result<()> {
        self.transaction.delete_cf(self.cf_handle(CF_DATASETS), dataset_id)?;
        Ok(())
    }

    pub fn write_chunk(&self, dataset_id: DatasetId, chunk: &Chunk) -> anyhow::Result<()> {
        self.transaction.put_cf(
            self.cf_handle(CF_CHUNKS),
            ChunkId::new_for_chunk(dataset_id, chunk),
            &borsh::to_vec(chunk).unwrap()
        )?;
        for table in chunk.tables().values() {
            self.transaction.delete_cf(self.cf_handle(CF_DIRTY_TABLES), table)?;
        }
        Ok(())
    }

    pub fn delete_chunk(&self, dataset_id: DatasetId, chunk: &Chunk) -> anyhow::Result<()> {
        self.transaction
            .delete_cf(self.cf_handle(CF_CHUNKS), ChunkId::new_for_chunk(dataset_id, chunk))?;
        for table_id in chunk.tables().values() {
            self.delete_table(table_id)?
        }
        Ok(())
    }

    pub fn delete_table(&self, table_id: &TableId) -> anyhow::Result<()> {
        // Value unused; the key's presence is the signal. `ops::logical_cleanup`
        // point-deletes the table's data and drops this entry.
        self.transaction
            .put_cf(self.cf_handle(CF_DELETED_TABLES), table_id, [])?;
        Ok(())
    }

    /// Adds the enabled derived-index entries for `chunk`. Both indexes are
    /// staged in the same optimistic transaction as chunk metadata.
    pub(crate) fn index_hashes(&self, dataset_id: DatasetId, chunk: &Chunk) -> anyhow::Result<()> {
        if !self.block_hash_index && !self.transaction_hash_index {
            return Ok(());
        }

        let Some(label) = self.find_label_for_update(dataset_id)? else {
            return Ok(()); // dataset does not exist - nothing to index
        };
        if !is_indexed_kind(label.kind()) {
            return Ok(());
        }

        if self.block_hash_index {
            self.measure_hash_index(HashIndex::Block, || self.index_block_hashes(dataset_id, chunk))?;
        }
        if self.transaction_hash_index {
            self.measure_hash_index(HashIndex::Transaction, || {
                self.index_transaction_hashes(dataset_id, chunk)
            })?;
        }
        Ok(())
    }

    fn index_block_hashes(&self, dataset_id: DatasetId, chunk: &Chunk) -> anyhow::Result<()> {
        let Some(blocks_table_id) = chunk.tables().get("blocks").copied() else {
            return Ok(()); // defensively skip chunks without a blocks table
        };

        let snapshot = ReadSnapshot::new(self.db);
        let reader = snapshot.create_table_reader(blocks_table_id)?;
        let cf = self.cf_handle(CF_BLOCK_HASHES);
        let mut key = HashIndexKey::new(dataset_id, "");
        for_each_block_hash(&reader, |number, hash| {
            key.set_hash(hash);
            self.transaction.put_cf(cf, &key, number.to_be_bytes())?;
            Ok(())
        })
    }

    fn index_transaction_hashes(&self, dataset_id: DatasetId, chunk: &Chunk) -> anyhow::Result<()> {
        let Some(transactions_table_id) = chunk.tables().get("transactions").copied() else {
            return Ok(());
        };

        let snapshot = ReadSnapshot::new(self.db);
        let reader = snapshot.create_table_reader(transactions_table_id)?;
        let cf = self.cf_handle(CF_TRANSACTION_HASHES);
        let mut key = HashIndexKey::new(dataset_id, "");
        for_each_transaction_hash(&reader, |block_number, transaction_index, hash| {
            key.set_hash(hash);
            self.transaction
                .put_cf(cf, &key, encode_transaction_position(block_number, transaction_index))?;
            Ok(())
        })
    }

    /// Removes every hash-index entry contributed by `chunk`.
    ///
    /// Removal is gated on neither flag nor dataset kind: entries written while
    /// a flag was on must still be removed when their chunk is pruned, or they
    /// would be stranded forever. Each column family is first probed with one
    /// prefix seek, making never-indexed chunks cheap and idempotent.
    pub(crate) fn unindex_hashes(&self, dataset_id: DatasetId, chunk: &Chunk) -> anyhow::Result<()> {
        self.measure_hash_index(HashIndex::Block, || self.unindex_block_hashes(dataset_id, chunk))?;
        self.measure_hash_index(HashIndex::Transaction, || {
            self.unindex_transaction_hashes(dataset_id, chunk)
        })
    }

    fn unindex_block_hashes(&self, dataset_id: DatasetId, chunk: &Chunk) -> anyhow::Result<()> {
        let cf = self.cf_handle(CF_BLOCK_HASHES);
        if !self.has_hash_entries(cf, dataset_id)? {
            return Ok(());
        }

        let Some(blocks_table_id) = chunk.tables().get("blocks").copied() else {
            return Ok(());
        };

        let snapshot = ReadSnapshot::new(self.db);
        let reader = snapshot.create_table_reader(blocks_table_id)?;
        let mut key = HashIndexKey::new(dataset_id, "");
        for_each_block_hash(&reader, |_number, hash| {
            key.set_hash(hash);
            self.transaction.delete_cf(cf, &key)?;
            Ok(())
        })
    }

    fn unindex_transaction_hashes(&self, dataset_id: DatasetId, chunk: &Chunk) -> anyhow::Result<()> {
        let cf = self.cf_handle(CF_TRANSACTION_HASHES);
        if !self.has_hash_entries(cf, dataset_id)? {
            return Ok(());
        }

        let Some(transactions_table_id) = chunk.tables().get("transactions").copied() else {
            return Ok(());
        };

        let snapshot = ReadSnapshot::new(self.db);
        let reader = snapshot.create_table_reader(transactions_table_id)?;
        let mut key = HashIndexKey::new(dataset_id, "");
        for_each_transaction_hash(&reader, |_block_number, _transaction_index, hash| {
            key.set_hash(hash);
            self.transaction.delete_cf(cf, &key)?;
            Ok(())
        })
    }

    /// Whether `dataset_id` holds at least one entry in `cf`: a single seek.
    /// Iterating the transaction (not the bare DB) keeps the answer accurate
    /// part-way through a multi-chunk `insert_fork`.
    fn has_hash_entries(&self, cf: &ColumnFamily, dataset_id: DatasetId) -> anyhow::Result<bool> {
        let (start, end) = HashIndexKey::dataset_range(dataset_id);

        let mut read_opts = rocksdb::ReadOptions::default();
        read_opts.set_snapshot(&self.transaction.snapshot());
        read_opts.set_iterate_upper_bound(end);

        let mut cursor = self.transaction.raw_iterator_cf_opt(cf, read_opts);

        cursor.seek(&start);
        cursor.status()?;

        Ok(cursor.valid())
    }

    pub fn insert_fork(&self, dataset_id: DatasetId, chunk: &Chunk) -> anyhow::Result<()> {
        let existing = self.list_chunks(dataset_id, 0, None).into_reversed();

        for head_result in existing {
            let head = head_result?;
            if chunk.first_block() <= head.first_block() {
                self.unindex_hashes(dataset_id, &head)?;
                self.delete_chunk(dataset_id, &head)?;
            } else if head.last_block() + 1 == chunk.first_block() {
                ensure!(
                    head.last_block_hash() == chunk.parent_block_hash(),
                    "chain continuity is violated between new chunk {} and its existing parent {}, expected parent hash was {}",
                    chunk,
                    head,
                    chunk.parent_block_hash()
                );
                break;
            } else if head.last_block() < chunk.first_block() {
                bail!(
                    "there is a gap between new chunk {} and existing {}, that is just below",
                    chunk,
                    head
                )
            } else {
                bail!("new chunk {} overlaps with existing {}", chunk, head)
            }
        }

        self.write_chunk(dataset_id, chunk)?;
        self.index_hashes(dataset_id, chunk)?;

        Ok(())
    }

    pub fn validate_chunk_insertion(&self, dataset_id: DatasetId, chunk: &Chunk) -> anyhow::Result<()> {
        ensure!(chunk.first_block() <= chunk.last_block());

        let existing = self
            .list_chunks(dataset_id, 0, Some(chunk.last_block() + 1))
            .into_reversed()
            .take(2);

        for chunk_result in existing {
            let n = chunk_result.context("failed to get neighbors")?;

            let is_disjoint = min(n.last_block(), chunk.last_block()) < max(n.first_block(), chunk.first_block());
            ensure!(is_disjoint, "new chunk {} overlaps with existing {}", chunk, n);

            if chunk.last_block() + 1 == n.first_block() {
                ensure!(
                    chunk.last_block_hash() == n.parent_block_hash(),
                    "chain continuity was violated between new {} and existing {}",
                    chunk,
                    n
                );
            }

            if n.last_block() + 1 == chunk.first_block() {
                ensure!(
                    n.last_block_hash() == chunk.parent_block_hash(),
                    "chain continuity was violated between new {} and existing {}",
                    chunk,
                    n
                );
            }
        }

        Ok(())
    }

    pub fn validate_parent_block_hash(
        &self,
        chunk: &Chunk,
        block_number: BlockNumber,
        expected_parent_hash: &str
    ) -> anyhow::Result<Result<(), String>> {
        if chunk.first_block() == block_number {
            return if chunk.parent_block_hash() == expected_parent_hash {
                Ok(Ok(()))
            } else {
                Ok(Err(chunk.parent_block_hash().to_string()))
            };
        }

        if chunk.last_block() + 1 == block_number {
            return if chunk.last_block_hash() == expected_parent_hash {
                Ok(Ok(()))
            } else {
                Ok(Err(chunk.last_block_hash().to_string()))
            };
        }

        ensure!(
            chunk.first_block() < block_number && block_number <= chunk.last_block(),
            "chunk {} does not have information about parent hash of block {}",
            chunk,
            block_number
        );

        let blocks_table_id = chunk
            .tables()
            .get("blocks")
            .copied()
            .ok_or_else(|| anyhow!("'blocks' table does not exist in chunk {}", chunk))?;

        let parent_hash = get_parent_block_hash(
            &ReadSnapshot::new(self.db).create_table_reader(blocks_table_id)?,
            block_number
        )?;

        if parent_hash == expected_parent_hash {
            Ok(Ok(()))
        } else {
            Ok(Err(parent_hash))
        }
    }

    pub fn list_chunks(
        &self,
        dataset_id: DatasetId,
        from_block: BlockNumber,
        to_block: Option<BlockNumber>
    ) -> ChunkIterator<RocksIterator<'_, RocksTransaction<'_>>> {
        let mut read_opts = rocksdb::ReadOptions::default();
        read_opts.set_snapshot(&self.transaction.snapshot());

        let cursor = self
            .transaction
            .raw_iterator_cf_opt(self.cf_handle(CF_CHUNKS), read_opts);

        ChunkIterator::new(cursor, dataset_id, from_block, to_block)
    }

    fn cf_handle(&self, name: &str) -> &ColumnFamily {
        self.db.cf_handle(name).unwrap()
    }
}

fn encode_transaction_position(block_number: BlockNumber, transaction_index: ItemIndex) -> [u8; 12] {
    let mut bytes = [0; 12];
    bytes[..8].copy_from_slice(&block_number.to_be_bytes());
    bytes[8..].copy_from_slice(&transaction_index.to_be_bytes());
    bytes
}
