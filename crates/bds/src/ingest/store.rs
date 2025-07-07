use sqd_primitives::{Block, BlockNumber, BlockRef};


pub trait Store: Clone {
    type Block: Block + Clone;

    fn max_pending_writes(&self) -> usize;
    
    async fn get_chain_head(
        &self, 
        first_block: BlockNumber, 
        parent_hash: Option<&str>
    ) -> anyhow::Result<Option<BlockRef>>;

    async fn compute_fork(&self, prev: &[BlockRef]) -> anyhow::Result<Option<usize>>;

    async fn save(&self, block: Self::Block) -> anyhow::Result<Self::Block>;
}