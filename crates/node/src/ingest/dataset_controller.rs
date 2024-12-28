use crate::types::{DBRef, DatasetKind};
use anyhow::{anyhow, bail, ensure};
use either::Either;
use sqd_primitives::{BlockNumber, BlockRef};
use sqd_storage::db::{Chunk as StorageChunk, Chunk, DatasetId};
use std::fmt::{Display, Formatter};
use tracing::warn;


#[derive(Debug)]
pub struct DatasetController {
    db: DBRef,
    dataset_id: DatasetId,
    dataset_kind: DatasetKind,
    first_block: BlockNumber,
    head: Option<BlockRef>,
}


impl DatasetController {
    pub fn dataset_id(&self) -> DatasetId {
        self.dataset_id
    }

    pub fn head_hash(&self) -> Option<&str> {
        self.head.as_ref().map(|h| h.hash.as_ref())
    }
    
    pub fn next_block(&self) -> BlockNumber {
        self.head.as_ref().map_or(self.first_block, |h| h.number + 1)
    }

    pub fn compute_rollback(&self, mut prev: &[BlockRef]) -> anyhow::Result<Either<BlockRef, BlockNumber>> {
        ensure!(!prev.is_empty(), "no previous blocks where provided");
        ensure!(
            prev.windows(2).all(|s| s[0].number < s[1].number),
            "list of previous blocks is not ordered"
        );

        let snapshot = self.db.snapshot();
        
        let label = snapshot.get_label(self.dataset_id)?.ok_or_else(|| {
            anyhow!("dataset {} no longer exists", self.dataset_id)
        })?;
        
        if let Some(finalized_head) = label.finalized_head() {
            let pos = match prev.iter().position(|b| b.number >= finalized_head.number) {
                Some(pos) => pos,
                None => bail!("all passed prev blocks lie below finalized head")
            };
            if prev[pos].number == finalized_head.number {
                ensure!(prev[pos].hash == finalized_head.hash);
            }
            prev = &prev[pos..]
        }

        let existing_chunks = snapshot.list_chunks(
            self.dataset_id,
            0,
            Some(prev.last().unwrap().number)
        ).into_reversed();

        let mut prev_blocks = prev.iter().rev().peekable();

        for chunk_result in existing_chunks {
            let head = chunk_result?;
            
            if prev_blocks.peek().map_or(false, |b| head.last_block() > b.number) {
                continue
            }
            
            while prev_blocks.peek().map_or(false, |b| b.number > head.last_block()) {
                prev_blocks.next();
            }
            
            if let Some(&block) = prev_blocks.peek() {
                if block.number == head.last_block() && block.hash == head.last_block_hash() {
                    return Ok(Either::Left(block.clone()))
                }
            } else {
                return Ok(
                    Either::Left(BlockRef {
                        number: head.last_block(),
                        hash: head.last_block_hash().to_string()
                    })
                )
            }
        }

        Ok(Either::Right(self.first_block))
    }

    pub fn retain_head(&mut self, from_block: BlockNumber) -> anyhow::Result<()> {
        let bottom_chunk = self.db.update_dataset(self.dataset_id, |tx| {
            for chunk_result in tx.list_chunks(0, None) {
                let chunk = chunk_result?;
                if chunk.last_block() < from_block {
                    tx.delete_chunk(&chunk)?;
                } else {
                    return Ok(Some(chunk))
                }
            }
            Ok(None)
        })?;

        self.first_block = from_block;

        if bottom_chunk.as_ref().is_none() {
            self.head = None
        }

        if let Some(chunk) = bottom_chunk {
            if chunk.first_block() > from_block {
                warn!(
                    "there is a gap between requested trim horizon {} \
                    and bottom chunk {} that will not be filled",
                    from_block,
                    chunk
                );
            }
        }

        Ok(())
    }

    pub fn finalize(
        &mut self,
        head_hash: &str,
        new_finalized_head: &BlockRef
    ) -> anyhow::Result<Option<BlockRef>>
    {
        self.db.update_dataset(self.dataset_id, |tx| {
            if let Some(current) = tx.label().finalized_head() {
                if current.number > new_finalized_head.number {
                    return Ok(None)
                }
                if current.number == new_finalized_head.number {
                    ensure!(current.hash == new_finalized_head.hash);
                    return Ok(None)
                }
            }

            let maybe_head_chunk = tx.list_chunks(0, None)
                .into_reversed()
                .next()
                .transpose()?;

            let head_chunk = match maybe_head_chunk {
                Some(c) => c,
                None => return Ok(None)
            };

            if head_chunk.last_block_hash() != head_hash {
                return Ok(None)
            }

            let new_finalized_head = if new_finalized_head.number > head_chunk.last_block() {
                BlockRef {
                    number: head_chunk.last_block(),
                    hash: head_chunk.last_block_hash().to_string()
                }
            } else if new_finalized_head.number == head_chunk.last_block() {
                ensure!(new_finalized_head.hash == head_chunk.last_block_hash());
                new_finalized_head.clone()
            } else {
                new_finalized_head.clone()
            };
            
            tx.set_finalized_head(new_finalized_head.clone());
            
            Ok(Some(new_finalized_head))
        })
    }

    pub fn new_chunk(
        &mut self,
        finalized_head: Option<&BlockRef>,
        chunk: &StorageChunk
    ) -> anyhow::Result<()>
    {
        self.db.update_dataset(self.dataset_id, |tx| {
            let new_finalized_head = match (finalized_head, tx.label().finalized_head()) {
                (Some(new), None) => {
                    Some(new)
                },
                (Some(new), Some(current)) if new.number >= current.number => {
                    Some(new)
                },
                (_, Some(current)) if current.number < chunk.first_block() => {
                    Some(current)
                },
                (_, Some(current)) => bail!(LowFinalizedHead),
                (None, None) => None
            };

            let new_finalized_head = new_finalized_head.map(|head| {
                if head.number < chunk.last_block() {
                    head.clone()
                } else {
                    BlockRef {
                        number: chunk.last_block(),
                        hash: chunk.last_block_hash().to_string()
                    }
                }
            });

            tx.set_finalized_head(new_finalized_head);
            tx.insert_fork(chunk)
        })?;

        self.head = Some(BlockRef {
            number: chunk.last_block(),
            hash: chunk.last_block_hash().to_string()
        });

        Ok(())
    }
}


#[derive(Debug)]
pub struct LowFinalizedHead;


impl Display for LowFinalizedHead {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "can't fork safely, because fork base is below the current finalized head \
            and finalized head of the data pack is below the current"
        )
    }
}


impl std::error::Error for LowFinalizedHead {}