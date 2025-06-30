use sqd_primitives::{Block, BlockRef};


pub trait Store: Clone {
    type Block: Block + Clone;

    fn max_pending_writes(&self) -> usize;

    async fn compute_fork(&self, prev: &[BlockRef]) -> anyhow::Result<Option<BlockRef>>;

    async fn save(&self, block: Self::Block) -> anyhow::Result<Self::Block>;
}