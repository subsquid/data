use crate::{BlockStream, DataClient};
use futures_core::future::BoxFuture;
use sqd_primitives::{Block, BlockNumber, BlockRef};
use std::sync::Arc;


pub type BlockStreamBox<B> = Box<dyn BlockStream<Block = B, Item = anyhow::Result<B>> + Unpin>;


impl <B: Block> BlockStream for BlockStreamBox<B> {
    type Block = B;

    fn take_finalized_head(&mut self) -> anyhow::Result<Option<BlockRef>> {
        self.as_mut().take_finalized_head()
    }

    fn finalized_head(&self) -> Option<&BlockRef> {
        self.as_ref().finalized_head()
    }

    fn prev_blocks(&self) -> &[BlockRef] {
        self.as_ref().prev_blocks()
    }
}


struct WrappedDataClient<C> {
    inner: C
}


impl <C: DataClient> DataClient for WrappedDataClient<C>
where
    C::BlockStream: Unpin + 'static
{
    type BlockStream = BlockStreamBox<<C::BlockStream as BlockStream>::Block>;

    fn stream<'a>(&'a self, from: BlockNumber, prev_block_hash: &'a str) -> BoxFuture<'a, anyhow::Result<Self::BlockStream>>
    {
        Box::pin(async move {
            let stream = self.inner.stream(from, prev_block_hash).await?;
            Ok(Box::new(stream) as Self::BlockStream)
        })
    }
}


pub type DataClientRef<B> = Arc<dyn DataClient<BlockStream = BlockStreamBox<B>>>;


pub fn make_data_client_ref<C>(client: C) -> DataClientRef<<<C as DataClient>::BlockStream as BlockStream>::Block>
where
    C: DataClient + 'static,
    C::BlockStream: Unpin + 'static
{
    Arc::new(WrappedDataClient {
        inner: client
    })
}