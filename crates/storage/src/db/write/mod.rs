use std::cmp::{max, min};

use anyhow::{anyhow, ensure};
use rocksdb::ColumnFamily;

pub use chunk::*;
use sqd_primitives::{BlockNumber, ShortHash};

use crate::db::data::{Chunk, ChunkId, DatasetId, DatasetLabel};
use crate::db::db::{CF_CHUNKS, CF_DATASETS, CF_DIRTY_TABLES, RocksDB, RocksTransaction, RocksTransactionOptions};
use crate::db::read::chunk::list_chunks;
use crate::util::borsh_serialize;


mod chunk;
mod storage;


pub struct NewChunk {
    pub prev_block_hash: Option<ShortHash>,
    pub first_block: BlockNumber,
    pub last_block: BlockNumber,
    pub last_block_hash: ShortHash,
    pub tables: ChunkTables
}


pub struct Tx<'a> {
    db: &'a RocksDB,
    transaction: RocksTransaction<'a>
}


impl <'a> Tx<'a> {
    pub fn new(db: &'a RocksDB) -> Self {
        Self {
            db,
            transaction: db.transaction()
        }
    }

    pub fn new_with_snapshot(db: &'a RocksDB) -> Self {
        let mut tx_options = RocksTransactionOptions::default();
        tx_options.set_snapshot(true);

        let transaction = db.transaction_opt(
            &rocksdb::WriteOptions::default(),
            &tx_options
        );

        Self {
            db,
            transaction
        }
    }

    pub fn commit(self) -> anyhow::Result<()> {
        self.transaction.commit()?;
        Ok(())
    }

    pub fn find_label_for_update(&self, dataset_id: DatasetId) -> anyhow::Result<Option<DatasetLabel>> {
        let maybe_bytes = self.transaction.get_pinned_for_update_cf(
            self.cf_handle(CF_DATASETS),
            dataset_id,
            true
        )?;
        Ok(if let Some(bytes) = maybe_bytes {
            let label = borsh::from_slice(bytes.as_ref())?;
            Some(label)
        } else {
            None
        })
    }

    pub fn get_label_for_update(&self, dataset_id: DatasetId) -> anyhow::Result<DatasetLabel> {
        self.find_label_for_update(dataset_id).and_then(|maybe_chunk| {
            maybe_chunk.ok_or_else(|| anyhow!("dataset {} not found", dataset_id))
        })
    }

    pub fn write_label(&self, dataset_id: DatasetId, label: &DatasetLabel) -> anyhow::Result<()> {
        self.transaction.put_cf(
            self.cf_handle(CF_DATASETS),
            dataset_id,
            &borsh_serialize(label)
        )?;
        Ok(())
    }

    pub fn write_new_chunk(&self, dataset_id: DatasetId, new_chunk: NewChunk) -> anyhow::Result<()> {
        let chunk = Chunk {
            first_block: new_chunk.first_block,
            last_block: new_chunk.last_block,
            last_block_hash: new_chunk.last_block_hash,
            max_num_rows: new_chunk.tables.max_num_rows,
            tables: new_chunk.tables.tables
        };

        self.transaction.put_cf(
            self.cf_handle(CF_CHUNKS),
            ChunkId::new_for_chunk(dataset_id, &chunk),
            &borsh_serialize(&chunk)
        )?;

        for table in chunk.tables.values() {
            self.transaction.delete_cf(self.cf_handle(CF_DIRTY_TABLES), table)?;
        }

        Ok(())
    }

    pub fn validate_new_chunk_insertion(&self, dataset_id: DatasetId, new_chunk: &NewChunk) -> anyhow::Result<()> {
        ensure!(new_chunk.first_block <= new_chunk.last_block);

        let existing = list_chunks(
            self.transaction.raw_iterator_cf(self.cf_handle(CF_CHUNKS)),
            dataset_id,
            new_chunk.first_block.saturating_sub(1),
            None
        ).take(2).collect::<anyhow::Result<Vec<_>>>()?;

        if let Some(prev_block_hash) = new_chunk.prev_block_hash {
            let ok = existing.first().map(|prev| {
                prev.last_block + 1 == new_chunk.first_block && prev.last_block_hash == prev_block_hash
            }).unwrap_or(true);
            ensure!(ok, "chain continuity violated");
        }

        let first_is_disjoint = existing.first().map(|c| {
            let beg = max(c.first_block, new_chunk.first_block);
            let end = min(c.last_block, new_chunk.last_block);
            end < beg
        }).unwrap_or(true);

        let last_is_disjoint = existing.last().map(|c| {
            let beg = max(c.first_block, new_chunk.first_block);
            let end = min(c.last_block, new_chunk.last_block);
            end < beg
        }).unwrap_or(true);

        ensure!(first_is_disjoint && last_is_disjoint, "found overlapping chunks");

        Ok(())
    }

    fn cf_handle(&self, name: &str) -> &ColumnFamily {
        self.db.cf_handle(name).unwrap()
    }
}