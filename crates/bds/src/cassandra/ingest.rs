use super::store::CassandraStorage;
use crate::block::BlockArc;
use crate::chain::HeadChain;
use crate::ingest::Store;
use anyhow::{bail, ensure};
use futures::TryStreamExt;
use sqd_primitives::{Block, BlockNumber, BlockPtr, BlockRef};
use std::pin::pin;


impl Store for CassandraStorage {
    type Block = BlockArc;

    async fn get_chain_head(&self, first_block: BlockNumber, parent_hash: Option<&str>) -> anyhow::Result<HeadChain> {
        let states = self.fetch_write_states().await?;

        if !states.iter().any(|s| s.head.number >= first_block) {
            return Ok(HeadChain::empty())
        }

        let last_block = states.into_iter()
            .max_by_key(|s| s.head.number)
            .unwrap()
            .head
            .number;

        if let Some(parent_hash) = parent_hash {
            validate_chain_base(self, parent_hash, first_block, last_block).await?;
        }

        build_chain(self, first_block, last_block, parent_hash).await
    }

    async fn save(&self, block: &Self::Block) -> anyhow::Result<()> {
        self.save_block(block).await
    }

    async fn set_head(&self, head: BlockPtr<'_>) -> anyhow::Result<()> {
        self.set_head(head.number, head.hash).await
    }

    async fn finalize(&self, from: BlockNumber, to: BlockPtr<'_>) -> anyhow::Result<()> {
        todo!()
    }
}


async fn validate_chain_base(
    store: &CassandraStorage,
    parent_hash: &str,
    first_block: BlockNumber,
    last_block: BlockNumber
) -> anyhow::Result<()>
{
    let mut stream = store.list_blocks(first_block, last_block);
    let mut stream_pin = pin!(stream);
    'L: while let Some(batch) = stream_pin.try_next().await? {
        for b in batch.blocks() {
            if b.parent_number >= first_block {
                break 'L
            }
            if b.parent_hash == parent_hash {
                return Ok(())
            }
        }
    }
    bail!(
        "blocks above {} are already present in the database, but none of those is directly based on {}",
        first_block,
        parent_hash
    );
}


async fn build_chain(
    store: &CassandraStorage,
    first_block: BlockNumber,
    last_block: BlockNumber,
    parent_hash: Option<&str>
) -> anyhow::Result<HeadChain>
{
    let mut stream = pin! {
        store.list_blocks_in_reversed_order(first_block, last_block)
    };

    let mut chain = HeadChain::empty();
    let mut expected = BlockRef::default();

    'L: while let Some(batch) = stream.try_next().await? {
        for b in batch.blocks() {
            if b.ptr() == expected.ptr() || chain.blocks.is_empty() {
                expected.set_ptr(b.parent_ptr());
                chain.blocks.push(b.ptr().to_ref());
                if b.is_final {
                    chain.first_finalized = true;
                    break 'L
                }
            }
        }
        if batch.partition_end() < expected.number {
            bail!(
                "block {} is missing in the database, while the above block is present", 
                expected
            );
        }
    }

    if chain.blocks.is_empty() {
        return Ok(chain)
    }

    chain.blocks.reverse();

    ensure!(
        chain.first_finalized || expected.number < first_block || first_block == 0,
        "block {} is missing in the database, while the above block is present",
        expected
    );
    
    if let Some(parent_hash) = parent_hash {
        if expected.number < first_block {
            ensure!(
                expected.hash == parent_hash,
                "the highest available chain {} is not based on block with hash {}", 
                chain.blocks.last().unwrap(),
                parent_hash
            );
        }
    }

    Ok(chain)
}