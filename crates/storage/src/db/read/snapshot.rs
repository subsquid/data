use std::ops::Deref;

use anyhow::anyhow;
use rocksdb::{ColumnFamily, ReadOptions};
use sqd_primitives::{BlockNumber, Name};

use crate::db::data::{Chunk, ChunkId, DatasetId};
use crate::db::db::{CF_CHUNKS, CF_TABLES, RocksDB, RocksSnapshot};
use crate::db::read::chunk::list_chunks;
use crate::kv::{KvRead, KvReadCursor};
use crate::table::read::TableReader;


pub struct ReadSnapshot<'a> {
    db: &'a RocksDB,
    snapshot: RocksSnapshot<'a>
}


impl <'a> ReadSnapshot<'a> {
    pub fn new(db: &'a RocksDB) -> Self {
        Self {
            db,
            snapshot: db.snapshot()
        }
    }

    pub fn list_chunks(
        &self,
        dataset_id: DatasetId,
        from_block: BlockNumber
    ) -> impl Iterator<Item=anyhow::Result<ChunkReader<'_>>>
    {
        let cursor = self.db.raw_iterator_cf_opt(self.cf_handle(CF_CHUNKS), self.new_options());
        list_chunks(cursor, dataset_id, from_block).map(move |chunk_result| {
            chunk_result.map(|chunk| {
                ChunkReader {
                    snapshot: self,
                    chunk,
                    dataset_id
                }
            })
        })
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


pub struct ChunkReader<'a> {
    snapshot: &'a ReadSnapshot<'a>,
    chunk: Chunk,
    dataset_id: DatasetId
}


impl <'a> ChunkReader<'a> {
    pub fn first_block(&self) -> BlockNumber {
        self.chunk.first_block
    }

    pub fn last_block(&self) -> BlockNumber {
        self.chunk.last_block
    }

    pub fn has_table(&self, name: &str) -> bool {
        self.chunk.tables.contains_key(name)
    }

    pub fn get_table(&self, name: &str) -> anyhow::Result<ChunkTableReader<'a>> {
        let table_id = self.chunk.tables.get(name).ok_or_else(|| {
            anyhow!(
                "table `{}` does not exist in chunk {}",
                name,
                ChunkId::new_for_chunk(self.dataset_id, &self.chunk)
            )
        })?;

        let storage = CFSnapshot {
            snapshot: self.snapshot,
            cf: CF_TABLES
        };

        let reader = TableReader::new(storage, table_id.as_ref())?;

        Ok(reader)
    }
}


pub type ChunkTableReader<'a> = TableReader<CFSnapshot<'a>>;


pub struct CFSnapshot<'a> {
    snapshot: &'a ReadSnapshot<'a>,
    cf: Name
}


impl <'a> KvRead for CFSnapshot<'a> {
    fn get<'b, 'c>(&'b self, key: &'c [u8]) -> anyhow::Result<Option<impl Deref<Target=[u8]> + 'b>> {
        Ok(self.snapshot.db.get_pinned_cf_opt(
            self.snapshot.cf_handle(self.cf),
            key,
            &self.snapshot.new_options()
        )?)
    }

    fn new_cursor(&self) -> impl KvReadCursor {
        self.snapshot.db.raw_iterator_cf_opt(
            self.snapshot.cf_handle(self.cf),
            self.snapshot.new_options()
        )
    }
}