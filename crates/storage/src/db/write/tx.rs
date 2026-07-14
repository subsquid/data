use std::{
    cell::RefCell,
    cmp::{max, min},
    sync::atomic::{AtomicU64, Ordering}
};

use anyhow::{anyhow, bail, ensure, Context};
use rocksdb::ColumnFamily;
use sqd_primitives::BlockNumber;

use crate::db::{
    data::{BlockHashIndexKey, ChunkId},
    db::{
        RocksDB, RocksIterator, RocksTransaction, RocksTransactionOptions, CF_BLOCK_HASHES, CF_CHUNKS, CF_DATASETS,
        CF_DELETED_TABLES, CF_DIRTY_TABLES
    },
    read::{
        blocks_table::{for_each_block_hash, get_parent_block_hash},
        chunk::ChunkIterator
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

/// Datasets whose block hashes get indexed in `CF_BLOCK_HASHES`. EVM-only for
/// now; hyperliquid must stay out - its `hash` is not a crypto hash and can
/// collide.
fn is_indexed_kind(kind: DatasetKind) -> bool {
    kind == DatasetKind::from_str("evm")
}

pub struct Tx<'a> {
    db: &'a RocksDB,
    transaction: RocksTransaction<'a>,
    block_hash_index: bool
}

impl<'a> Tx<'a> {
    pub fn new(db: &'a RocksDB) -> Self {
        let mut tx_options = RocksTransactionOptions::default();
        tx_options.set_snapshot(true);

        let transaction = db.transaction_opt(&rocksdb::WriteOptions::default(), &tx_options);

        Self {
            db,
            transaction,
            block_hash_index: false
        }
    }

    /// Enables block hash indexing for chunks written through this transaction.
    /// Set by [`Database::update_dataset`] from the database-level setting.
    pub fn with_block_hash_index(mut self, yes: bool) -> Self {
        self.block_hash_index = yes;
        self
    }

    pub fn run<R, F>(self, mut cb: F) -> anyhow::Result<R>
    where
        F: FnMut(&Self) -> anyhow::Result<R>
    {
        let db = self.db;
        let block_hash_index = self.block_hash_index;
        let mut tx = self;
        loop {
            let result = cb(&tx)?;
            match tx.commit() {
                Ok(_) => return Ok(result),
                Err(err) if err.kind() == rocksdb::ErrorKind::TryAgain || err.kind() == rocksdb::ErrorKind::Busy => {
                    record_restart();
                    tx = Self::new(db).with_block_hash_index(block_hash_index)
                }
                Err(err) => return Err(err.into())
            }
        }
    }

    pub fn commit(self) -> Result<(), rocksdb::Error> {
        self.transaction.commit()
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

    /// Adds every `(hash, block number)` pair of `chunk`'s `blocks` table to
    /// `CF_BLOCK_HASHES`. No-op unless indexing is enabled on this transaction
    /// and the dataset kind is whitelisted in [`is_indexed_kind`].
    pub fn index_block_hashes(&self, dataset_id: DatasetId, chunk: &Chunk) -> anyhow::Result<()> {
        if !self.block_hash_index {
            return Ok(());
        }

        let Some(label) = self.find_label_for_update(dataset_id)? else {
            return Ok(()); // dataset does not exist - nothing to index
        };
        if !is_indexed_kind(label.kind()) {
            return Ok(());
        }

        let Some(blocks_table_id) = chunk.tables().get("blocks").copied() else {
            return Ok(()); // defensively skip chunks without a blocks table
        };

        let snapshot = ReadSnapshot::new(self.db);
        let reader = snapshot.create_table_reader(blocks_table_id)?;
        let cf = self.cf_handle(CF_BLOCK_HASHES);
        for_each_block_hash(&reader, |number, hash| {
            self.transaction
                .put_cf(cf, BlockHashIndexKey::new(dataset_id, hash), number.to_be_bytes())?;
            Ok(())
        })
    }

    /// Removes every hash of `chunk`'s `blocks` table from `CF_BLOCK_HASHES`.
    ///
    /// Unlike [`Tx::index_block_hashes`], gated on neither the flag nor the
    /// dataset kind, but on whether the dataset has any entries at all -
    /// entries written while the flag was on must still be removed when their
    /// chunk is pruned, or they would be stranded forever. Idempotent over
    /// never-indexed chunks: `delete_cf` on a missing key is a no-op.
    pub fn unindex_block_hashes(&self, dataset_id: DatasetId, chunk: &Chunk) -> anyhow::Result<()> {
        if !self.has_block_hash_entries(dataset_id)? {
            return Ok(());
        }

        let Some(blocks_table_id) = chunk.tables().get("blocks").copied() else {
            return Ok(());
        };

        let snapshot = ReadSnapshot::new(self.db);
        let reader = snapshot.create_table_reader(blocks_table_id)?;
        let cf = self.cf_handle(CF_BLOCK_HASHES);
        for_each_block_hash(&reader, |_number, hash| {
            self.transaction
                .delete_cf(cf, BlockHashIndexKey::new(dataset_id, hash))?;
            Ok(())
        })
    }

    /// Whether `dataset_id` holds at least one `CF_BLOCK_HASHES` entry: a
    /// single seek. Iterating the transaction (not the bare DB) keeps the
    /// answer accurate part-way through a multi-chunk `insert_fork`.
    fn has_block_hash_entries(&self, dataset_id: DatasetId) -> anyhow::Result<bool> {
        let (start, end) = BlockHashIndexKey::dataset_range(dataset_id);

        let mut read_opts = rocksdb::ReadOptions::default();
        read_opts.set_snapshot(&self.transaction.snapshot());
        read_opts.set_iterate_upper_bound(end);

        let mut cursor = self
            .transaction
            .raw_iterator_cf_opt(self.cf_handle(CF_BLOCK_HASHES), read_opts);

        cursor.seek(&start);
        cursor.status()?;

        Ok(cursor.valid())
    }

    pub fn insert_fork(&self, dataset_id: DatasetId, chunk: &Chunk) -> anyhow::Result<()> {
        let existing = self.list_chunks(dataset_id, 0, None).into_reversed();

        for head_result in existing {
            let head = head_result?;
            if chunk.first_block() <= head.first_block() {
                self.unindex_block_hashes(dataset_id, &head)?;
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
        self.index_block_hashes(dataset_id, chunk)?;

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
