use sqd_primitives::{Block, BlockNumber, BlockPtr, BlockRef};


pub trait Store: Clone {
    type Block: Block + Clone;

    /// Checks, that `first_block` with `parent_hash` is present in the store
    /// and returns the head of the highest chain, that is based on `first_block`.
    ///
    /// Returns None, if there are no stored blocks greater or equal to `first_block`.
    async fn get_chain_head(
        &self,
        first_block: BlockNumber,
        parent_hash: Option<&str>
    ) -> anyhow::Result<Option<BlockRef>>;

    async fn save(&self, block: &Self::Block) -> anyhow::Result<()>;
    
    async fn set_head(&self, head: BlockPtr<'_>) -> anyhow::Result<()>;
    
    async fn finalize(&self, from: BlockNumber, to: BlockPtr<'_>) -> anyhow::Result<()>;
}