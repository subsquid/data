use std::future::Future;
use std::process::Output;
use sqd_primitives::{Block, BlockNumber, BlockPtr};
use crate::chain::HeadChain;


pub trait Store: Clone + Send + Sync + 'static {
    type Block: Block + Clone + Send + Sync;

    async fn get_chain_head(
        &self,
        first_block: BlockNumber,
        parent_hash: Option<&str>
    ) -> anyhow::Result<HeadChain>;

    fn save(&self, block: &Self::Block) -> impl Future<Output = anyhow::Result<()>> + Send;

    fn set_head(&self, head: BlockPtr) -> impl Future<Output = anyhow::Result<()>> + Send;
    
    async fn finalize(&self, from: BlockNumber, to: BlockPtr<'_>) -> anyhow::Result<()>;
}