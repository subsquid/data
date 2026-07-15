use std::{collections::BTreeMap, ops::Deref, sync::Arc};

use anyhow::{anyhow, Context};
use parking_lot::Mutex;
use rocksdb::{ColumnFamily, ReadOptions};
use sqd_primitives::{BlockNumber, BlockRef, Name, TransactionRef};

use crate::{
    db::{
        data::{Chunk, DatasetId, HashIndexKey},
        db::{
            RocksDB, RocksIterator, RocksSnapshot, CF_BLOCK_HASHES, CF_CHUNKS, CF_DATASETS, CF_TABLES,
            CF_TRANSACTION_HASHES
        },
        read::chunk::ChunkIterator,
        table_id::TableId,
        DatasetLabel
    },
    kv::KvRead,
    table::read::TableReader
};

pub struct ReadSnapshot<'a> {
    db: &'a RocksDB,
    snapshot: RocksSnapshot<'a, RocksDB>
}

impl<'a> ReadSnapshot<'a> {
    pub fn new(db: &'a RocksDB) -> Self {
        Self {
            db,
            snapshot: db.snapshot()
        }
    }

    pub fn get_label(&self, dataset_id: DatasetId) -> anyhow::Result<Option<DatasetLabel>> {
        let maybe_bytes = self
            .db
            .get_pinned_cf_opt(self.cf_handle(CF_DATASETS), dataset_id, &self.new_options())?;
        Ok(if let Some(bytes) = maybe_bytes {
            let label = borsh::from_slice(bytes.as_ref())?;
            Some(label)
        } else {
            None
        })
    }

    pub(crate) fn has_dataset(&self, dataset_id: DatasetId) -> anyhow::Result<bool> {
        Ok(self
            .db
            .get_pinned_cf_opt(self.cf_handle(CF_DATASETS), dataset_id, &self.new_options())?
            .is_some())
    }

    pub fn create_table_reader(&self, table_id: TableId) -> anyhow::Result<SnapshotTableReader<'_>> {
        let storage = CFSnapshot {
            snapshot: self,
            cf: CF_TABLES
        };
        let reader = TableReader::new(storage, table_id.as_ref())?;
        Ok(reader)
    }

    pub fn create_chunk_reader(&self, chunk: Chunk) -> ChunkReader<'_> {
        ChunkReader::new(self, chunk)
    }

    pub fn list_chunks(
        &self,
        dataset_id: DatasetId,
        from_block: BlockNumber,
        to_block: Option<BlockNumber>
    ) -> ReadSnapshotChunkIterator<'a> {
        let cursor = self
            .db
            .raw_iterator_cf_opt(self.cf_handle(CF_CHUNKS), self.new_options());
        ChunkIterator::new(cursor, dataset_id, from_block, to_block)
    }

    pub fn get_first_chunk(&self, dataset_id: DatasetId) -> anyhow::Result<Option<Chunk>> {
        self.list_chunks(dataset_id, 0, None).next().transpose()
    }

    pub fn get_last_chunk(&self, dataset_id: DatasetId) -> anyhow::Result<Option<Chunk>> {
        self.list_chunks(dataset_id, 0, None).into_reversed().next().transpose()
    }

    /// Resolves a block hash via the `CF_BLOCK_HASHES` index. `Ok(None)` means
    /// the hash is not indexed (unknown, pre-index chunk, or non-indexed kind).
    pub fn find_block_by_hash(&self, dataset_id: DatasetId, hash: &str) -> anyhow::Result<Option<BlockRef>> {
        // DROP removes the label before bounded physical index cleanup. Checking
        // it in this same snapshot makes the logical index transition atomic.
        if !self.has_dataset(dataset_id)? {
            return Ok(None);
        }

        let key = HashIndexKey::new(dataset_id, hash);
        let Some(bytes) = self
            .db
            .get_pinned_cf_opt(self.cf_handle(CF_BLOCK_HASHES), &key, &self.new_options())?
        else {
            return Ok(None);
        };
        // A wrong length means corruption; error rather than panic.
        let arr: [u8; 8] = bytes
            .as_ref()
            .try_into()
            .context("CF_BLOCK_HASHES value has unexpected length, expected 8 bytes")?;
        Ok(Some(BlockRef {
            number: BlockNumber::from_be_bytes(arr),
            hash: hash.to_string()
        }))
    }

    /// Resolves a transaction hash via `CF_TRANSACTION_HASHES`. `Ok(None)`
    /// means the hash is not indexed, which is deliberately not proof that the
    /// transaction is absent from the retained window.
    pub fn find_transaction_by_hash(
        &self,
        dataset_id: DatasetId,
        hash: &str
    ) -> anyhow::Result<Option<TransactionRef>> {
        const VALUE_LEN: usize = 12;

        // See `find_block_by_hash`: stale physical keys after DROP are never
        // observable, and CREATE purges them before reusing a dataset ID.
        if !self.has_dataset(dataset_id)? {
            return Ok(None);
        }

        let key = HashIndexKey::new(dataset_id, hash);
        let Some(bytes) =
            self.db
                .get_pinned_cf_opt(self.cf_handle(CF_TRANSACTION_HASHES), &key, &self.new_options())?
        else {
            return Ok(None);
        };

        let bytes: &[u8; VALUE_LEN] = bytes
            .as_ref()
            .try_into()
            .context("CF_TRANSACTION_HASHES value has unexpected length, expected 12 bytes")?;
        let mut block_number_bytes = [0; 8];
        block_number_bytes.copy_from_slice(&bytes[..8]);
        let mut transaction_index_bytes = [0; 4];
        transaction_index_bytes.copy_from_slice(&bytes[8..]);

        Ok(Some(TransactionRef {
            block_number: BlockNumber::from_be_bytes(block_number_bytes),
            transaction_index: u32::from_be_bytes(transaction_index_bytes),
            hash: hash.to_string()
        }))
    }

    fn new_options(&self) -> ReadOptions {
        let mut options = ReadOptions::default();
        options.set_snapshot(&self.snapshot);
        options
    }

    fn cf_handle(&self, name: &str) -> &ColumnFamily {
        self.db.cf_handle(name).unwrap()
    }
}

pub type ReadSnapshotChunkIterator<'a> = ChunkIterator<RocksIterator<'a, RocksDB>>;

pub struct ChunkReader<'a> {
    snapshot: &'a ReadSnapshot<'a>,
    chunk: Chunk,
    cache: BTreeMap<String, Mutex<Option<Arc<SnapshotTableReader<'a>>>>>
}

impl<'a> ChunkReader<'a> {
    fn new(snapshot: &'a ReadSnapshot<'a>, chunk: Chunk) -> Self {
        let cache = chunk
            .tables()
            .keys()
            .map(|name| (name.to_string(), Mutex::new(None)))
            .collect();

        Self { snapshot, chunk, cache }
    }

    pub fn first_block(&self) -> BlockNumber {
        self.chunk.first_block()
    }

    pub fn last_block(&self) -> BlockNumber {
        self.chunk.last_block()
    }

    pub fn last_block_hash(&self) -> &str {
        &self.chunk.last_block_hash()
    }

    pub fn base_block_hash(&self) -> &str {
        &self.chunk.parent_block_hash()
    }

    pub fn has_table(&self, name: &str) -> bool {
        self.chunk.tables().contains_key(name)
    }

    pub fn chunk(&self) -> &Chunk {
        &self.chunk
    }

    pub fn tables(&self) -> &BTreeMap<String, TableId> {
        self.chunk.tables()
    }

    pub fn get_table_reader(&self, name: &str) -> anyhow::Result<Arc<SnapshotTableReader<'a>>> {
        let mut reader_lock = self
            .cache
            .get(name)
            .ok_or_else(|| anyhow!("table `{}` does not exist in this chunk", name))?
            .lock();

        if let Some(reader) = reader_lock.as_ref() {
            return Ok(reader.clone());
        }

        let table_id = self.chunk.tables().get(name).unwrap();
        let reader = self.snapshot.create_table_reader(*table_id)?;
        let reader = Arc::new(reader);

        *reader_lock = Some(reader.clone());
        Ok(reader)
    }

    pub fn into_chunk(self) -> Chunk {
        self.chunk
    }
}

pub type SnapshotTableReader<'a> = TableReader<CFSnapshot<'a>>;

pub struct CFSnapshot<'a> {
    snapshot: &'a ReadSnapshot<'a>,
    cf: Name
}

impl<'a> KvRead for CFSnapshot<'a> {
    type Cursor = RocksIterator<'a, RocksDB>;

    fn get(&self, key: &[u8]) -> anyhow::Result<Option<impl Deref<Target = [u8]>>> {
        Ok(self
            .snapshot
            .db
            .get_pinned_cf_opt(self.snapshot.cf_handle(self.cf), key, &self.snapshot.new_options())?)
    }

    fn new_cursor(&self) -> Self::Cursor {
        self.snapshot
            .db
            .raw_iterator_cf_opt(self.snapshot.cf_handle(self.cf), self.snapshot.new_options())
    }
}
