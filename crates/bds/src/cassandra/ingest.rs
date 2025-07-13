use super::store::CassandraStorage;
use crate::block::BlockArc;
use crate::ingest::Store;
use crate::util::compute_fork_base;
use anyhow::bail;
use futures::TryStreamExt;
use sqd_primitives::{Block, BlockNumber, BlockPtr, BlockRef};
use std::pin::pin;


impl Store for CassandraStorage {
    type Block = BlockArc;

    fn max_pending_writes(&self) -> usize {
        5
    }

    async fn get_chain_head(&self, first_block: BlockNumber, parent_hash: Option<&str>) -> anyhow::Result<Option<BlockRef>> {
        let states = self.fetch_write_states().await?;

        if !states.iter().any(|s| s.head.number >= first_block) {
            return Ok(None)
        }

        let Some(parent_hash) = parent_hash else {
            let head = states.into_iter().max_by_key(|s| s.head.number).map(|s| s.head);
            return Ok(head)
        };

        let top = states.iter().map(|s| s.head.number).max().unwrap();

        let finalized_top = states.iter()
            .map(|s| s.finalized_head.as_ref().map_or(0, |h| h.number))
            .max()
            .unwrap();

        let mut head = BlockRef {
            number: 0,
            hash: String::new()
        };

        enum Stage {
            ParentHashValidation,
            ParentHashMismatch,
            Building,
            FinalizedHeadJump
        }

        let mut stage = Stage::ParentHashValidation;
        let mut stream = self.list_blocks(first_block, top);
        let mut stream_pin = pin!(stream);

        while let Some(batch) = stream_pin.try_next().await? {
            for b in batch.blocks() {
                match stage {
                    Stage::ParentHashValidation | Stage::ParentHashMismatch => {
                        if b.parent_hash == parent_hash {
                            if finalized_top > b.number {
                                stream = self.list_blocks(finalized_top, top);
                                stage = Stage::FinalizedHeadJump;
                                break
                            } else {
                                head.number = b.number;
                                head.set_hash(b.hash());
                                stage = Stage::Building;
                            }
                        } else if b.is_final || b.number > first_block {
                            bail!("parent hash mismatch");
                        } else {
                            stage = Stage::ParentHashMismatch;
                        }
                    },
                    Stage::Building => {
                        if b.parent_number() == head.number && b.parent_hash() == head.hash {
                            head.number = b.number;
                            head.set_hash(b.hash());
                        }
                    },
                    Stage::FinalizedHeadJump => {
                        if b.is_final {
                            head.number = b.number;
                            head.set_hash(b.hash());
                            stage = Stage::Building;
                        }
                    }
                }
            }
        }

        match stage {
            Stage::Building => Ok(Some(head)),
            Stage::ParentHashMismatch => bail!("parent hash mismatch"),
            Stage::ParentHashValidation => bail!(
                "block {} was marked as present in a writer state, but was not found in the database", 
                first_block
            ),
            Stage::FinalizedHeadJump => bail!(
                "block {} was marked in a writer state as final, but was not found in the database", 
                finalized_top
            )
        }
    }

    async fn compute_fork_base(
        &self,
        chain: BlockPtr<'_>,
        mut prev: &[BlockRef]
    ) -> anyhow::Result<Option<BlockRef>>
    {
        let mut stream = pin! {
            self.list_blocks_in_reversed_order(0, chain.number)
        };

        let prev = &mut prev;
        let mut expected = chain.to_ref();

        while let Some(batch) = stream.try_next().await? {
            if batch.partition_end() < expected.number {
                return Ok(None)
            }

            let block_chain = batch.blocks().iter().filter(|b| {
                if b.number == expected.number && b.hash == expected.hash {
                    expected.number = b.parent_number;
                    expected.set_hash(b.parent_hash.as_ref());
                    true
                } else {
                    false
                }
            });

            if let Some(block) = compute_fork_base(block_chain, prev) {
                return Ok(Some(block))
            }
        }

        Ok(None)
    }

    async fn save(&self, block: Self::Block) -> anyhow::Result<Self::Block> {
        self.save_block(block.as_ref()).await?;
        Ok(block)
    }
}