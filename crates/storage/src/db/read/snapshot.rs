use std::ops::Deref;

use anyhow::anyhow;
use rocksdb::{ColumnFamily, ReadOptions};
use sqd_primitives::{BlockNumber, Name, ShortHash};

use crate::db::data::{Chunk, ChunkId, DatasetId};
use crate::db::db::{RocksDB, RocksSnapshot, CF_CHUNKS, CF_DATASETS, CF_TABLES};
use crate::db::read::chunk::{list_chunks, read_current_chunk};
use crate::db::DatasetLabel;
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

    pub fn get_label(&self, dataset_id: DatasetId) -> anyhow::Result<Option<DatasetLabel>> {
        let maybe_bytes = self.db.get_pinned_cf_opt(
            self.cf_handle(CF_DATASETS),
            dataset_id,
            &self.new_options()
        )?;
        Ok(if let Some(bytes) = maybe_bytes {
            let label = borsh::from_slice(bytes.as_ref())?;
            Some(label)
        } else {
            None
        })
    }

    pub fn create_chunk_reader(&self, chunk: Chunk) -> ChunkReader<'_> {
        ChunkReader {
            snapshot: self,
            chunk
        }
    }

    pub fn list_raw_chunks(
        &self,
        dataset_id: DatasetId,
        from_block: BlockNumber,
        to_block: Option<BlockNumber>
    ) -> impl Iterator<Item=anyhow::Result<Chunk>> + '_
    {
        let cursor = self.db.raw_iterator_cf_opt(
            self.cf_handle(CF_CHUNKS),
            self.new_options()
        );
        list_chunks(cursor, dataset_id, from_block, to_block)
    }

    pub fn list_chunks(
        &self,
        dataset_id: DatasetId,
        from_block: BlockNumber,
        to_block: Option<BlockNumber>
    ) -> impl Iterator<Item=anyhow::Result<ChunkReader<'_>>>
    {
        self.list_raw_chunks(dataset_id, from_block, to_block).map(|result| {
            result.map(|chunk| self.create_chunk_reader(chunk))
        })
    }
    
    pub fn get_first_chunk(&self, dataset_id: DatasetId) -> anyhow::Result<Option<ChunkReader<'_>>> {
        self.list_chunks(dataset_id, 0, None).next().transpose()
    }

    pub fn get_last_chunk(&self, dataset_id: DatasetId) -> anyhow::Result<Option<ChunkReader<'_>>> {
        let mut cursor = self.db.raw_iterator_cf_opt(
            self.cf_handle(CF_CHUNKS),
            self.new_options()
        );

        let max_chunk_id = ChunkId::new(dataset_id, BlockNumber::MAX);
        cursor.seek_for_prev(max_chunk_id);
        cursor.status()?;

        let reader = read_current_chunk(&cursor, dataset_id)?.map(|chunk| {
            self.create_chunk_reader(chunk)
        });

        Ok(reader)
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
    chunk: Chunk
}


impl <'a> ChunkReader<'a> {
    pub fn first_block(&self) -> BlockNumber {
        self.chunk.first_block
    }

    pub fn last_block(&self) -> BlockNumber {
        self.chunk.last_block
    }
    
    pub fn last_block_hash(&self) -> ShortHash {
        self.chunk.last_block_hash
    }

    pub fn has_table(&self, name: &str) -> bool {
        self.chunk.tables.contains_key(name)
    }

    pub fn get_table(&self, name: &str) -> anyhow::Result<ChunkTableReader<'a>> {
        let table_id = self.chunk.tables.get(name).ok_or_else(|| {
            anyhow!("table `{}` does not exist in this chunk", name)
        })?;

        let storage = CFSnapshot {
            snapshot: self.snapshot,
            cf: CF_TABLES
        };

        let reader = TableReader::new(storage, table_id.as_ref())?;

        Ok(reader)
    }
    
    pub fn into_data(self) -> Chunk {
        self.chunk
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