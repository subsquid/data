use crate::types::DBRef;
use sqd_data_client::BlockRef;
use sqd_storage::db::{Chunk as StorageChunk, DatasetId};


pub struct WriteController {
    db: DBRef,
    dataset_id: DatasetId,
    base: BlockRef,
    head: BlockRef,
    finalized_head: BlockRef,
}


impl WriteController {
    pub fn head(&self) -> &BlockRef {
        &self.head
    }

    pub fn rollback(&mut self, prev: &[BlockRef]) -> anyhow::Result<BlockRef> {
        if prev.is_empty() {
            return Ok(self.head.clone())
        }

        let snapshot = self.db.snapshot();

        let existing_chunks = snapshot.list_chunks(
            self.dataset_id,
            0,
            Some(prev.last().unwrap().number())
        ).into_reversed();

        let mut prev_blocks = prev.iter().rev().peekable();

        for chunk_result in existing_chunks {
            let head = chunk_result?;
            
            if prev_blocks.peek().map_or(false, |b| head.last_block > b.number()) {
                continue
            }
            
            while prev_blocks.peek().map_or(false, |b| b.number() > head.last_block) {
                prev_blocks.next();
            }
            
            if let Some(&block) = prev_blocks.peek() {
                if block.number() == head.last_block && block.hash() == &head.last_block_hash {
                    return Ok(block.clone())
                }
            } else {
                return Ok(
                    BlockRef::new(head.last_block, &head.last_block_hash)
                )
            }
        }

        Ok(self.base.clone())
    }

    pub fn insert_new_chunk(&mut self, chunk: &StorageChunk) -> anyhow::Result<()> {
        if self.head.number() < chunk.first_block {
            self.db.insert_chunk(self.dataset_id, chunk)?;
        } else {
            self.db.insert_fork(self.dataset_id, chunk)?;
        }
        self.head = BlockRef::new(chunk.last_block, &chunk.last_block_hash);
        Ok(())
    }

    pub fn finalize(&mut self, head: &BlockRef) -> anyhow::Result<()> {
        todo!()
    }
}