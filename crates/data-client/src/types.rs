use futures_core::future::BoxFuture;
use futures_core::Stream;
use sqd_primitives::{Block, BlockNumber, BlockRef};


#[derive(Debug, Copy, Clone)]
pub struct BlockStreamRequest<'a> {
    pub first_block: BlockNumber,
    pub prev_block_hash: Option<&'a str>
}


impl<'a> BlockStreamRequest<'a> {
    pub fn new(first_block: BlockNumber) -> Self {
        Self {
            first_block,
            prev_block_hash: None
        }
    }
    
    pub fn with_prev_block_hash(mut self, prev_block_hash: impl Into<Option<&'a str>>) -> Self {
        self.prev_block_hash = prev_block_hash.into();
        self
    }
}


pub trait DataClient: Sync {
    type BlockStream: BlockStream;
    
    fn stream<'a>(
        &'a self, 
        req: BlockStreamRequest<'a>
    ) -> BoxFuture<'a, anyhow::Result<Self::BlockStream>>;
}


pub trait BlockStream: Stream<Item = anyhow::Result<Self::Block>> {
    type Block: Block;

    fn take_finalized_head(&mut self) -> anyhow::Result<Option<BlockRef>>;

    fn finalized_head(&self) -> Option<&BlockRef>;
    
    fn take_prev_blocks(&mut self) -> Vec<BlockRef>;

    fn prev_blocks(&self) -> &[BlockRef];
}