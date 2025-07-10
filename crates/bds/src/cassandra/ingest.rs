use crate::block::BlockArc;
use crate::cassandra::store::CassandraStorage;
use crate::ingest::Store;
use crate::util::compute_fork_base;
use futures::TryStreamExt;
use sqd_primitives::{BlockNumber, BlockRef};
use std::pin::pin;


impl Store for CassandraStorage {
    type Block = BlockArc;

    fn max_pending_writes(&self) -> usize {
        5
    }

    async fn get_chain_head(&self, first_block: BlockNumber, parent_hash: Option<&str>) -> anyhow::Result<Option<BlockRef>> {
        todo!()
    }

    async fn compute_fork_base(
        &self, 
        chain: &BlockRef, 
        mut prev: &[BlockRef]
    ) -> anyhow::Result<Option<BlockRef>> 
    {
        let mut stream = pin! {
            self.list_blocks_in_reversed_order(0, chain.number)
        };

        let prev = &mut prev;
        let mut expected = chain.clone();

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