use crate::chain::HeadChain;
use sqd_primitives::{Block, BlockNumber, BlockPtr, BlockRef};
use std::future::Future;


pub trait Store: Clone + Send + Sync + 'static {
    type Block: Block + Clone + Send + Sync;

    async fn get_chain_head(
        &self,
        first_block: BlockNumber,
        parent_hash: Option<&str>
    ) -> anyhow::Result<HeadChain>;

    fn save(&self, block: &Self::Block) -> impl Future<Output = anyhow::Result<()>> + Send;

    fn set_head(&self, head: BlockPtr) -> impl Future<Output = anyhow::Result<()>> + Send;
    
    fn finalize(&self, from: BlockNumber, to: BlockPtr) -> impl Future<Output = anyhow::Result<()>> + Send;
}