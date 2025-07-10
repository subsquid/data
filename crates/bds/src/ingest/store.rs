use sqd_primitives::{Block, BlockNumber, BlockRef};


pub trait Store: Clone {
    type Block: Block + Clone;

    fn max_pending_writes(&self) -> usize;
    
    /// Checks, that `first_block` with `parent_hash` is present in the store
    /// and returns the head of the highest branch, that is based on `first_block`.
    /// Returns None, otherwise.
    async fn get_chain_head(
        &self, 
        first_block: BlockNumber, 
        parent_hash: Option<&str>
    ) -> anyhow::Result<Option<BlockRef>>;

    /// Find the highest "known" block of `chain`, that could be a part of `prev`.
    /// 
    /// `prev` - any monotonic subsequence of some fork
    ///
    /// The block is "known", when it is either stored or is a parent of a stored block.
    async fn compute_fork_base(
        &self,
        chain: &BlockRef,
        prev: &[BlockRef]
    ) -> anyhow::Result<Option<BlockRef>>;

    async fn save(&self, block: Self::Block) -> anyhow::Result<Self::Block>;
}