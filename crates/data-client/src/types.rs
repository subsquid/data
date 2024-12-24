use futures_core::future::BoxFuture;
use futures_core::Stream;
use sqd_primitives::{Block, BlockNumber, BlockRef};


pub trait DataClient: Sync {
    type BlockStream: BlockStream;
    
    fn stream<'a>(
        &'a self, 
        from: BlockNumber, 
        prev_block_hash: &'a str
    ) -> BoxFuture<'a, anyhow::Result<Self::BlockStream>>;
}


pub trait BlockStream: Stream<Item = anyhow::Result<Self::Block>> {
    type Block: Block;

    fn take_finalized_head(&mut self) -> anyhow::Result<Option<BlockRef>>;

    fn finalized_head(&self) -> Option<&BlockRef>;

    fn prev_blocks(&self) -> &[BlockRef];
}