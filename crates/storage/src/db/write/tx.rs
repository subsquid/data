use crate::db::data::ChunkId;
use crate::db::db::{RocksDB, RocksTransaction, RocksTransactionIterator, RocksTransactionOptions, CF_CHUNKS, CF_DATASETS, CF_DIRTY_TABLES};
use crate::db::read::chunk::ChunkIterator;
use crate::db::table_id::TableId;
use crate::db::{Chunk, DatasetId, DatasetLabel};
use anyhow::{anyhow, bail, ensure, Context};
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

    pub fn bump_label(&self, dataset_id: DatasetId) -> anyhow::Result<()> {
        let mut label = self.get_label_for_update(dataset_id)?;
        label.bump_version();
        self.write_label(dataset_id, &label)
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

    pub fn insert_fork(
        &self,
        dataset_id: DatasetId,
        chunk: &Chunk
    ) -> anyhow::Result<Vec<TableId>>
    {
        let mut deleted_tables = Vec::new();
        
        let existing = self.list_chunks(
            dataset_id,
            0,
            None
        ).into_reversed();
        
        for chunk_result in existing {
            let head = chunk_result?;
            if chunk.first_block <= head.first_block {
                self.delete_chunk(dataset_id, &head)?;
                deleted_tables.extend(head.tables.values());
            } else if head.last_block + 1 == chunk.first_block {
                ensure!(head.last_block_hash == chunk.parent_block_hash);
                break
            } else if head.last_block < chunk.first_block {
                bail!(
                    "chain continuity was violated between new chunk {} and existing {}",
                    chunk,
                    head
                )
            } else {
                bail!("new chunk {} overlaps with existing {}", chunk, head)
            }    
        }
        
        self.write_chunk(dataset_id, chunk)?;
        
        Ok(deleted_tables)
    }

    pub fn validate_chunk_insertion(
        &self,
        dataset_id: DatasetId,
        chunk: &Chunk
    ) -> anyhow::Result<()>
    {
        ensure!(chunk.first_block <= chunk.last_block);

        let existing = self.list_chunks(dataset_id, 0, Some(chunk.last_block + 1))
            .into_reversed()
            .take(2);
        
        for chunk_result in existing {
            let n = chunk_result.context("failed to get neighbors")?;
            
            let is_disjoint = min(n.last_block, chunk.last_block) < max(n.first_block, chunk.first_block);
            ensure!(
                is_disjoint,
                "new chunk {} overlaps with existing {}",
                chunk,
                n
            );
            
            if chunk.last_block + 1 == n.first_block {
                ensure!(
                    chunk.last_block_hash == n.parent_block_hash,
                    "chain continuity was violated between new {} and existing {}",
                    chunk,
                    n
                );
            }
            
            if n.last_block + 1 == chunk.first_block {
                ensure!(
                    n.last_block_hash == chunk.parent_block_hash,
                    "chain continuity was violated between new {} and existing {}",
                    chunk,
                    n
                );
            }
        }

        Ok(())
    }

    pub fn retain_head(&self, dataset_id: DatasetId, from_block: BlockNumber) -> anyhow::Result<Vec<TableId>> {
        let mut deleted_tables = Vec::new();
        for chunk_result in self.list_chunks(dataset_id, 0, Some(from_block.saturating_sub(1))) {
            let chunk = chunk_result?;
            if chunk.last_block < from_block {
                self.delete_chunk(dataset_id, &chunk)?;
                deleted_tables.extend(chunk.tables.values())
            }
        }
        Ok(deleted_tables)
    }

    pub fn list_chunks(
        &self,
        dataset_id: DatasetId,
        from_block: BlockNumber,
        to_block: Option<BlockNumber>
    ) -> ChunkIterator<RocksTransactionIterator<'_>>
    {
        ChunkIterator::new(
            self.transaction.raw_iterator_cf(
                self.cf_handle(CF_CHUNKS)
            ),
            dataset_id,
            from_block,
            to_block
        )
    }

    fn cf_handle(&self, name: &str) -> &ColumnFamily {
        self.db.cf_handle(name).unwrap()
    }
}