use std::ops::Deref;
use crate::types::DBRef;
use ouroboros::self_referencing;
use sqd_primitives::BlockNumber;
use sqd_storage::db::{Chunk, ChunkReader, Database, DatasetId, DatasetLabel, ReadSnapshot, ReadSnapshotChunkIterator};
use std::sync::Arc;


#[self_referencing]
struct StaticSnapshotInner {
    db: DBRef,
    #[borrows(db)]
    #[covariant]
    snapshot: ReadSnapshot<'this>,
}


#[derive(Clone)]
pub struct StaticSnapshot {
    inner: Arc<StaticSnapshotInner>
}


impl StaticSnapshot {
    pub fn new(db: DBRef) -> Self {
        let inner = StaticSnapshotInnerBuilder {
            db,
            snapshot_builder: |db: &DBRef| db.snapshot()
        }.build();
        Self {
            inner: Arc::new(inner)
        }
    }

    pub fn database(&self) -> &Database {
        self.inner.borrow_db().as_ref()
    }

    pub fn snapshot(&self) -> &ReadSnapshot<'_> {
        self.inner.borrow_snapshot()
    }

    pub fn get_label(&self, dataset_id: DatasetId) -> anyhow::Result<Option<DatasetLabel>> {
        self.snapshot().get_label(dataset_id)
    }

    pub fn list_chunks(
        &self,
        dataset_id: DatasetId,
        from_block: BlockNumber,
        to_block: Option<BlockNumber>
    ) -> StaticChunkIterator
    {
        StaticChunkIterator::new(self.clone(), dataset_id, from_block, to_block)
    }

    pub fn create_chunk_reader(&self, chunk: Chunk) -> StaticChunkReader {
        StaticChunkReader::new(self.clone(), chunk)
    }
}


#[self_referencing]
struct StaticChunkIteratorInner {
    snapshot: StaticSnapshot,
    #[borrows(snapshot)]
    #[covariant]
    iter: ReadSnapshotChunkIterator<'this>
}


pub struct StaticChunkIterator {
    inner: StaticChunkIteratorInner
}


impl StaticChunkIterator {
    pub fn new(
        snapshot: StaticSnapshot,
        dataset_id: DatasetId,
        from_block: BlockNumber,
        to_block: Option<BlockNumber>
    ) -> Self
    {
        let inner = StaticChunkIteratorInnerBuilder {
            snapshot,
            iter_builder: |snapshot| snapshot.snapshot().list_chunks(dataset_id, from_block, to_block)
        }.build();
        Self {
            inner
        }
    }

    pub fn snapshot(&self) -> &StaticSnapshot {
        self.inner.borrow_snapshot()
    }
}


impl Iterator for StaticChunkIterator {
    type Item = anyhow::Result<Chunk>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.with_iter_mut(|it| it.next())
    }
}


#[self_referencing]
struct StaticChunkReaderInner {
    snapshot: StaticSnapshot,
    #[borrows(snapshot)]
    #[not_covariant]
    reader: ChunkReader<'this>
}


#[derive(Clone)]
pub struct StaticChunkReader {
    inner: Arc<StaticChunkReaderInner>
}


impl StaticChunkReader {
    pub fn new(snapshot: StaticSnapshot, chunk: Chunk) -> Self {
        let inner = StaticChunkReaderInnerBuilder {
            snapshot,
            reader_builder: |snapshot| snapshot.snapshot().create_chunk_reader(chunk)
        }.build();
        Self {
            inner: Arc::new(inner)
        }
    }

    pub fn with_reader<R, F>(&self, cb: F) -> R
    where
        F: FnOnce(&ChunkReader<'_>) -> R
    {
        self.inner.with_reader(cb)
    }

    pub fn first_block(&self) -> BlockNumber {
        self.inner.with_reader(|r| r.first_block())
    }

    pub fn last_block(&self) -> BlockNumber {
        self.inner.with_reader(|r| r.last_block())
    }
}