use crate::types::{DBRef, DatasetKind};
use anyhow::ensure;
use either::Either;
use sqd_data_client::BlockRef;
use sqd_data_core::BlockNumber;
use sqd_storage::db::{Chunk as StorageChunk, DatasetId};


pub struct WriteController {
    db: DBRef,
    dataset_id: DatasetId,
    dataset_kind: DatasetKind,
    first_block: BlockNumber,
    head: Option<BlockRef>,
    finalized_head: Option<BlockRef>,
}


impl WriteController {
    pub fn head_hash(&self) -> Option<&str> {
        self.head.as_ref().map(|h| h.hash())
    }
    
    pub fn next_block(&self) -> BlockNumber {
        self.head.as_ref().map_or(self.first_block, |h| h.number() + 1)
    }

    pub fn compute_rollback(&self, prev: &[BlockRef]) -> anyhow::Result<Either<BlockRef, BlockNumber>> {
        ensure!(!prev.is_empty(), "no previous blocks where provided");
        ensure!(
            prev.windows(2).all(|s| s[0].number() < s[1].number()),
            "list of previous blocks is not ordered"
        );

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
                    return Ok(Either::Left(block.clone()))
                }
            } else {
                return Ok(
                    Either::Left(BlockRef::new(head.last_block, &head.last_block_hash))
                )
            }
        }

        Ok(Either::Right(self.first_block))
    }

    pub fn insert_new_chunk(&mut self, chunk: &StorageChunk) -> anyhow::Result<()> {
        if self.next_block() <= chunk.first_block {
            ensure!(self.next_block() == chunk.first_block);
            self.db.insert_chunk(self.dataset_id, chunk)?;
        } else {
            self.db.insert_fork(self.dataset_id, chunk)?;
        }
        self.head = Some(BlockRef::new(chunk.last_block, &chunk.last_block_hash));
        Ok(())
    }

    pub fn finalize(&mut self, head: &BlockRef) -> anyhow::Result<()> {
        todo!()
    }
}