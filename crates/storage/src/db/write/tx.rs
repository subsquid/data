use crate::db::data::ChunkId;
use crate::db::db::{RocksDB, RocksTransaction, RocksTransactionOptions, CF_CHUNKS, CF_DATASETS, CF_DIRTY_TABLES};
use crate::db::read::chunk::list_chunks;
use crate::db::{Chunk, DatasetId, DatasetLabel};
use anyhow::{anyhow, ensure};
use rocksdb::ColumnFamily;
use sqd_primitives::BlockNumber;
use std::cmp::{max, min};


pub struct Tx<'a> {
    db: &'a RocksDB,
    transaction: RocksTransaction<'a>,
    with_snapshot: bool
}


impl <'a> Tx<'a> {
    pub fn new(db: &'a RocksDB) -> Self {
        Self {
            db,
            transaction: db.transaction(),
            with_snapshot: false
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
            transaction,
            with_snapshot: true
        }
    }

    pub fn run<R, F>(self, mut cb: F) -> anyhow::Result<R>
    where
        F: FnMut(&Self) -> anyhow::Result<R>
    {
        let db = self.db;
        let with_snapshot = self.with_snapshot;
        let mut tx = self;
        loop {
            let result = cb(&tx)?;
            match tx.commit() {
                Ok(_) => return Ok(result),
                Err(err) if err.kind() == rocksdb::ErrorKind::TryAgain => {
                    tx = if with_snapshot {
                        Self::new_with_snapshot(db)
                    } else {
                        Self::new(db)
                    }
                },
                Err(err) => return Err(err.into())
            }
        }
    }

    pub fn commit(self) -> Result<(), rocksdb::Error> {
        self.transaction.commit()
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
            &borsh::to_vec(label).unwrap()
        )?;
        Ok(())
    }

    pub fn write_chunk(&self, dataset_id: DatasetId, chunk: &Chunk) -> anyhow::Result<()> {
        self.transaction.put_cf(
            self.cf_handle(CF_CHUNKS),
            ChunkId::new_for_chunk(dataset_id, chunk),
            &borsh::to_vec(chunk).unwrap()
        )?;
        for table in chunk.tables.values() {
            self.transaction.delete_cf(self.cf_handle(CF_DIRTY_TABLES), table)?;
        }
        Ok(())
    }

    pub fn delete_chunk(&self, dataset_id: DatasetId, chunk: &Chunk) -> anyhow::Result<()> {
        for table_id in chunk.tables.values() {
            self.transaction.put_cf(
                self.cf_handle(CF_DIRTY_TABLES),
                table_id,
                []
            )?
        }
        self.transaction.delete_cf(
            self.cf_handle(CF_CHUNKS),
            ChunkId::new_for_chunk(dataset_id, chunk)
        )?;
        Ok(())
    }

    pub fn validate_chunk_insertion(
        &self,
        dataset_id: DatasetId,
        chunk: &Chunk,
        prev_block_hash: Option<&str>
    ) -> anyhow::Result<()>
    {
        ensure!(chunk.first_block <= chunk.last_block);

        let existing = self.list_chunks(
            dataset_id,
            chunk.first_block.saturating_sub(1),
            None
        ).take(2).collect::<anyhow::Result<Vec<_>>>()?;

        if let Some(prev_block_hash) = prev_block_hash {
            let ok = existing.first().map(|prev| {
                prev.last_block + 1 == chunk.first_block && prev.last_block_hash == prev_block_hash
            }).unwrap_or(true);
            ensure!(ok, "chain continuity violated");
        }

        let first_is_disjoint = existing.first().map(|c| {
            let beg = max(c.first_block, chunk.first_block);
            let end = min(c.last_block, chunk.last_block);
            end < beg
        }).unwrap_or(true);

        let last_is_disjoint = existing.last().map(|c| {
            let beg = max(c.first_block, chunk.first_block);
            let end = min(c.last_block, chunk.last_block);
            end < beg
        }).unwrap_or(true);

        ensure!(first_is_disjoint && last_is_disjoint, "found overlapping chunks");

        Ok(())
    }

    pub fn list_chunks(
        &self, 
        dataset_id: DatasetId,
        first_block: BlockNumber, 
        last_block: Option<BlockNumber>
    ) -> impl Iterator<Item=anyhow::Result<Chunk>> + '_
    {
        list_chunks(
            self.transaction.raw_iterator_cf(self.cf_handle(CF_CHUNKS)),
            dataset_id,
            first_block,
            last_block
        )
    }

    fn cf_handle(&self, name: &str) -> &ColumnFamily {
        self.db.cf_handle(name).unwrap()
    }
}