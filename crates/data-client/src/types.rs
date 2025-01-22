use futures::future::BoxFuture;
use futures::Stream;
use sqd_primitives::{Block, BlockNumber, BlockRef};


#[derive(Debug, Clone)]
pub struct BlockStreamRequest {
    pub first_block: BlockNumber,
    pub parent_block_hash: Option<String>
}


impl BlockStreamRequest {
    pub fn new(first_block: BlockNumber) -> Self {
        Self {
            first_block,
            parent_block_hash: None
        }
    }
    
    pub fn with_parent_block_hash(mut self, parent_block_hash: impl Into<Option<String>>) -> Self {
        self.parent_block_hash = parent_block_hash.into();
        self
    }
}


pub trait DataClient: Sync {
    type BlockStream: BlockStream;
    
    fn stream(
        &self,
        req: BlockStreamRequest
    ) -> BoxFuture<anyhow::Result<Self::BlockStream>>;
}


pub trait BlockStream: Stream<Item = anyhow::Result<Self::Block>> {
    type Block: Block;

    fn take_finalized_head(&mut self) -> anyhow::Result<Option<BlockRef>>;

    fn finalized_head(&self) -> Option<&BlockRef>;
    
    fn take_prev_blocks(&mut self) -> Vec<BlockRef>;

    fn prev_blocks(&self) -> &[BlockRef];
}