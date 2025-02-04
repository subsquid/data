use futures::future::BoxFuture;
use futures::stream::BoxStream;
use sqd_primitives::{BlockNumber, BlockRef};
use std::fmt::Debug;


#[derive(Debug, Clone, Eq, PartialEq)]
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


pub enum BlockStreamResponse<B> {
    Stream {
        blocks: BoxStream<'static, anyhow::Result<B>>,
        finalized_head: Option<BlockRef>
    },
    Fork(Vec<BlockRef>)
}


pub trait DataClient: Send + Sync + Debug + Unpin {
    type Block;
    
    fn stream(
        &self,
        req: BlockStreamRequest
    ) -> BoxFuture<'static, anyhow::Result<BlockStreamResponse<Self::Block>>>;
    
    fn get_finalized_head(&self) -> BoxFuture<'static, anyhow::Result<Option<BlockRef>>>;
    
    fn is_retryable(&self, err: &anyhow::Error) -> bool;
}