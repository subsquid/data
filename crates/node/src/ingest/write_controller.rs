use crate::types::{DBRef, DatasetKind};
use anyhow::{anyhow, bail, ensure};
use either::Either;
use sqd_primitives::{BlockNumber, BlockRef, DisplayBlockRefOption};
use sqd_storage::db::{Chunk as StorageChunk, Chunk, DatasetId, DatasetUpdate};
use std::fmt::{Display, Formatter};
use tracing::{info, instrument, warn, Level};


#[derive(Debug)]
pub struct Rollback {
    pub first_block: BlockNumber,
    pub parent_block_hash: Option<String>,
    pub finalized_head: Option<BlockRef>
}


#[derive(Debug)]
pub struct WriteController {
    db: DBRef,
    dataset_id: DatasetId,
    dataset_kind: DatasetKind,
    first_block: BlockNumber,
    parent_block_hash: Option<String>,
    head: Option<BlockRef>,
    finalized_head: Option<BlockRef>
}


impl WriteController {
    pub fn new(
        db: DBRef,
        dataset_id: DatasetId,
        dataset_kind: DatasetKind
    ) -> anyhow::Result<Self>
    {
        db.create_dataset_if_not_exists(dataset_id, dataset_kind.storage_kind())?;

        let snapshot = db.snapshot();
        let label = snapshot.get_label(dataset_id)?;
        let first_chunk = snapshot.get_first_chunk(dataset_id)?;
        let last_chunk = snapshot.get_last_chunk(dataset_id)?;
        
        let mut controller = Self {
            db: db.clone(),
            dataset_id,
            dataset_kind,
            first_block: first_chunk.as_ref().map_or(0, |c| c.first_block()),
            parent_block_hash: first_chunk.map(|c| c.last_block_hash().to_string()),
            head: last_chunk.map(|c| BlockRef {
                number: c.last_block(),
                hash: c.last_block_hash().to_string()
            }),
            finalized_head: label.and_then(|l| l.finalized_head().cloned())
        };
        
        Ok(controller)
    }
    
    pub fn dataset_id(&self) -> DatasetId {
        self.dataset_id
    }
    
    pub fn dataset_kind(&self) -> DatasetKind {
        self.dataset_kind
    }
    
    pub fn start_block(&self) -> BlockNumber {
        self.first_block
    }

    pub fn start_block_parent_hash(&self) -> Option<&str> {
        self.parent_block_hash.as_ref().map(String::as_str)
    }

    pub fn next_block(&self) -> BlockNumber {
        self.head.as_ref().map_or(self.first_block, |h| h.number + 1)
    }

    pub fn head_hash(&self) -> Option<&str> {
        self.head.as_ref()
            .map(|h| h.hash.as_str())
            .or_else(|| self.start_block_parent_hash())
    }

    pub fn head(&self) -> Option<&BlockRef> {
        self.head.as_ref()
    }
    
    pub fn finalized_head(&self) -> Option<&BlockRef> {
        self.finalized_head.as_ref()
    }
    
    pub fn compute_rollback(&self, mut prev: &[BlockRef]) -> anyhow::Result<Rollback> {
        // FIXME: self.first_block rollback limit
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
            
            if prev_blocks.peek().map_or(false, |b| b.number < head.last_block()) {
                continue
            }
            
            while prev_blocks.peek().map_or(false, |b| b.number > head.last_block()) {
                prev_blocks.next();
            }
            
            if let Some(&b) = prev_blocks.peek() {
                if b.number == head.last_block() && b.hash == head.last_block_hash() {
                    return Ok(Rollback {
                        first_block: b.number + 1,
                        parent_block_hash: Some(b.hash.clone()),
                        finalized_head: label.finalized_head().cloned()
                    })
                }
            } else {
                return Ok(Rollback {
                    first_block: head.last_block() + 1,
                    parent_block_hash: Some(head.last_block_hash().to_string()),
                    finalized_head: label.finalized_head().cloned()
                })
            }
        }

        Ok(Rollback {
            first_block: self.first_block,
            parent_block_hash: None,
            finalized_head: None
        })
    }

    #[instrument(
        name = "retain",
        fields(dataset_id =? self.dataset_id()),
        skip(self, delete_mismatch)
    )]
    fn _retain(
        &mut self,
        from_block: BlockNumber,
        parent_block_hash: Option<String>,
        delete_mismatch: bool
    ) -> anyhow::Result<()>
    {
        #[derive(Eq, PartialEq)]
        enum Status {
            Range {
                bottom: Chunk,
                head: Chunk,
                finalized_head: Option<BlockRef>
            },
            HashMismatch,
            Gap(BlockNumber),
            Clear
        }

        let status = self.db.update_dataset(self.dataset_id, |tx| {
            let mut status = Status::Clear;
            for chunk_result in tx.list_chunks(0, None) {
                let chunk = chunk_result?;
                if chunk.last_block() < from_block {
                    tx.delete_chunk(&chunk)?;
                } else if from_block < chunk.first_block() {
                    if delete_mismatch {
                        tx.delete_chunk(&chunk)?;
                    } else {
                        bail!(
                            "there is a gap between first requested block {} and already existing chunk {}, \
                            that could not be filled",
                            from_block,
                            chunk
                        );
                    }
                    if status == Status::Clear {
                        status = Status::Gap(chunk.first_block());
                    }
                } else {
                    let hash_mismatch = parent_block_hash.as_ref().map_or(false, |parent_hash| {
                        false  // FIXME
                    });
                    if hash_mismatch {
                        if delete_mismatch {
                            tx.delete_chunk(&chunk)?;
                            status = Status::HashMismatch;
                        } else {
                            bail!("hash mismatch")
                        }
                    } else {
                        let head = tx.list_chunks(0, None)
                            .into_reversed()
                            .next()
                            .expect("bottom chunk can't exist without head chunk")?;

                        let finalized_head = tx.label()
                            .finalized_head()
                            .filter(|h| chunk.first_block() <= h.number)
                            .cloned();

                        if finalized_head.is_none() {
                            tx.set_finalized_head(None)
                        }

                        return Ok(Status::Range {
                            bottom: chunk,
                            head,
                            finalized_head
                        })
                    }
                }
            }
            tx.set_finalized_head(None);
            Ok(status)
        })?;

        match status {
            Status::Range {
                bottom,
                head,
                finalized_head
            } => {
                self.head = Some(BlockRef {
                    number: head.last_block(),
                    hash: head.last_block_hash().to_string()
                });

                self.finalized_head = finalized_head;

                info!(
                    "retained blocks from {} to {}",
                    bottom.first_block(),
                    head.last_block()
                );
            },
            Status::HashMismatch => {
                warn!("cleared dataset due to parent block hash mismatch")
            }
            Status::Gap(existed) => {
                warn!(
                    "cleared dataset, because there was a gap between first requested block {} and already existed {}",
                    from_block,
                    existed
                )
            }
            Status::Clear => {
                info!("dataset was cleared")
            }
        }
        
        self.first_block = from_block;
        self.parent_block_hash = parent_block_hash;
        Ok(())
    }

    pub fn retain(&mut self, from_block: BlockNumber, parent_block_hash: Option<String>) -> anyhow::Result<()> {
        self._retain(from_block, parent_block_hash, true)
    }

    pub fn init_retention(&mut self, from_block: BlockNumber, parent_block_hash: Option<String>) -> anyhow::Result<()> {
        self._retain(from_block, parent_block_hash, false)
    }

    #[instrument(skip_all, fields(
        dataset_id = %self.dataset_id(),
        new_finalized_head = %new_finalized_head
    ))]
    pub fn finalize(&mut self, new_finalized_head: &BlockRef) -> anyhow::Result<()> {
        let Some(head) = self.head.as_ref() else {
            return Ok(())
        };
        
        let update = self.db.update_dataset(self.dataset_id, |tx| {
            ensure!(
                tx.label().finalized_head() == self.finalized_head.as_ref(),
                "seems like the dataset is controlled by multiple processes"
            );

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
                Some(c) if c.last_block_hash() == head.hash => c,
                _ => bail!("seems like the dataset is controlled by multiple processes")
            };

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
        })?;

        if let Some(new_head) = update {
            info!(committed_finalized_head = %new_head, "committed");
            self.finalized_head = Some(new_head);
        } else {
            info!("ignored")
        }
        
        Ok(())
    }

    #[instrument(skip_all, fields(
        dataset_id = %self.dataset_id(),
        chunk = %chunk,
        finalized_head = %DisplayBlockRefOption(finalized_head)
    ))]
    pub fn new_chunk(
        &mut self,
        finalized_head: Option<&BlockRef>,
        chunk: &StorageChunk
    ) -> anyhow::Result<()>
    {
        // FIXME: accept self.first_block rollback limit
        let finalized_head = self.db.update_dataset(self.dataset_id, |tx| {
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
                (_, Some(current)) => bail!(            
                    "can't fork safely, because fork base is below the current finalized head \
                    and finalized head of the data pack is below the current"
                ),
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

            tx.set_finalized_head(new_finalized_head.clone());
            tx.insert_fork(chunk)?;
            Ok(new_finalized_head)
        })?;

        info!(
            committed_finalized_head = %DisplayBlockRefOption(finalized_head.as_ref()),
            "commited"
        );

        self.finalized_head = finalized_head;

        self.head = Some(BlockRef {
            number: chunk.last_block(),
            hash: chunk.last_block_hash().to_string()
        });
        
        Ok(())
    }
}