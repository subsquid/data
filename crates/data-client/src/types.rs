use futures_core::future::BoxFuture;
use futures_core::Stream;
use serde::Deserialize;
use sqd_data_types::{Block, BlockNumber};


pub trait DataClient: Sync {
    type BlockStream: BlockStream;
    
    fn stream<'a>(
        &'a self, 
        from: BlockNumber, 
        prev_block_hash: &'a str
    ) -> BoxFuture<'a, anyhow::Result<Self::BlockStream>>
    // where 
    //     Self: 'a
    ;
}


pub trait BlockStream: Stream<Item = anyhow::Result<Self::Block>> {
    type Block: Block;

    fn take_finalized_head(&mut self) -> anyhow::Result<Option<BlockRef>>;

    fn finalized_head(&self) -> Option<&BlockRef>;

    fn prev_blocks(&self) -> &[BlockRef];
}


#[derive(Deserialize, Clone, Debug, Default)]
pub struct BlockRef {
    number: BlockNumber,
    hash: String
}


impl BlockRef {
    pub fn new(number: BlockNumber, hash: &str) -> Self {
        Self {
            number,
            hash: hash.to_string()
        }
    }
    
    pub fn number(&self) -> BlockNumber {
        self.number
    }
    
    pub fn hash(&self) -> &str {
        &self.hash
    }
}