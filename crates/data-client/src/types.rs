use std::fmt::Debug;

use futures::{future::BoxFuture, stream::BoxStream};
use sqd_primitives::{BlockNumber, BlockRef};

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

    pub fn set_parent_block_hash(&mut self, hash: Option<&str>) {
        match (hash, self.parent_block_hash.as_mut()) {
            (Some(src), Some(target)) => {
                target.clear();
                target.push_str(src)
            }
            (Some(src), None) => self.parent_block_hash = Some(src.to_string()),
            (None, _) => self.parent_block_hash = None
        }
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

    fn stream(&self, req: BlockStreamRequest) -> BoxFuture<'static, anyhow::Result<BlockStreamResponse<Self::Block>>>;

    fn get_finalized_head(&self) -> BoxFuture<'static, anyhow::Result<Option<BlockRef>>>;

    fn is_retryable(&self, err: &anyhow::Error) -> bool;

    /// Error class for the `ingest_source_errors` metric label.
    fn error_kind(&self, _err: &anyhow::Error) -> &'static str {
        "other"
    }

    /// Source identity (host) for the `ingest_source_errors` metric label.
    fn source_label(&self) -> String {
        "unknown".to_string()
    }
}
