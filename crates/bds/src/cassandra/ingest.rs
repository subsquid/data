use super::store::CassandraStorage;
use crate::block::BlockArc;
use crate::ingest::Store;
use anyhow::bail;
use futures::TryStreamExt;
use sqd_primitives::{Block, BlockNumber, BlockPtr, BlockRef};
use std::pin::pin;


impl Store for CassandraStorage {
    type Block = BlockArc;

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

    async fn save(&self, block: &Self::Block) -> anyhow::Result<()> {
        self.save_block(block).await
    }

    async fn set_head(&self, head: BlockPtr<'_>) -> anyhow::Result<()> {
        todo!()
    }

    async fn finalize(&self, from: BlockNumber, to: BlockPtr<'_>) -> anyhow::Result<()> {
        todo!()
    }
}