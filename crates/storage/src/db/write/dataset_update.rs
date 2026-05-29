use sqd_primitives::{BlockNumber, BlockRef};

use crate::db::{
    db::{RocksIterator, RocksTransaction},
    read::chunk::ChunkIterator,
    write::tx::Tx,
    Chunk, DatasetId, DatasetLabel
};

pub struct DatasetUpdate<'a> {
    tx: &'a Tx<'a>,
    dataset_id: DatasetId,
    label: DatasetLabel
}

impl<'a> DatasetUpdate<'a> {
    pub(crate) fn new(tx: &'a Tx<'a>, dataset_id: DatasetId) -> anyhow::Result<Self> {
        let label = tx.get_label_for_update(dataset_id)?;
        Ok(Self { tx, dataset_id, label })
    }

    pub fn dataset_id(&self) -> DatasetId {
        self.dataset_id
    }

    pub fn label(&self) -> &DatasetLabel {
        &self.label
    }

    pub fn insert_chunk(&self, chunk: &Chunk) -> anyhow::Result<()> {
        self.tx.validate_chunk_insertion(self.dataset_id, chunk)?;
        self.tx.write_chunk(self.dataset_id, chunk)?;
        self.tx.index_block_hashes(self.dataset_id, chunk)
    }

    pub fn insert_fork(&self, chunk: &Chunk) -> anyhow::Result<()> {
        self.tx.insert_fork(self.dataset_id, chunk)
    }

    pub fn validate_parent_block_hash(
        &self,
        chunk: &Chunk,
        block_number: BlockNumber,
        expected_parent_hash: &str
    ) -> anyhow::Result<Result<(), String>> {
        self.tx
            .validate_parent_block_hash(chunk, block_number, expected_parent_hash)
    }

    pub fn delete_chunk(&self, chunk: &Chunk) -> anyhow::Result<()> {
        self.tx.unindex_block_hashes(self.dataset_id, chunk)?;
        self.tx.delete_chunk(self.dataset_id, chunk)
    }

    pub fn set_finalized_head(&mut self, block_ref: impl Into<Option<BlockRef>>) {
        self.label.set_finalized_head(block_ref.into())
    }

    pub fn list_chunks(
        &self,
        from_block: BlockNumber,
        to_block: Option<BlockNumber>
    ) -> ChunkIterator<RocksIterator<'a, RocksTransaction<'a>>> {
        self.tx.list_chunks(self.dataset_id, from_block, to_block)
    }

    pub fn finish(mut self) -> anyhow::Result<()> {
        self.label.bump_version();
        self.tx.write_label(self.dataset_id, &self.label)
    }
}
