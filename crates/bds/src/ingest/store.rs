use sqd_primitives::{Block, BlockNumber, BlockPtr, BlockRef};


pub trait Store: Clone {
    type Block: Block + Clone;

    fn max_pending_writes(&self) -> usize;

    /// Checks, that `first_block` with `parent_hash` is present in the store
    /// and returns the head of the highest chain, that is based on `first_block`.
    ///
    /// Returns None, if there are no blocks greater or equal to `first_block`.
    async fn get_chain_head(
        &self,
        first_block: BlockNumber,
        parent_hash: Option<&str>
    ) -> anyhow::Result<Option<BlockRef>>;

    /// Find the highest stored block of chain `head`, that either:
    ///   1. belongs to `prev`
    ///   2. lies below `prev`
    async fn compute_fork_base(
        &self,
        head: BlockPtr<'_>,
        prev: &[BlockRef]
    ) -> anyhow::Result<Option<BlockRef>>;

    async fn save(&self, block: Self::Block) -> anyhow::Result<Self::Block>;
}