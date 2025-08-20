use crate::{DataEvent, DataSource};
use futures::{Stream, StreamExt};
use sqd_primitives::{Block, BlockNumber};
use std::pin::Pin;
use std::task::{Context, Poll};


pub struct MappedDataSource<S, F> {
    inner: S,
    map: F
}


impl<S, F> MappedDataSource<S, F> {
    pub fn new(inner: S, map: F) -> Self {
        Self {
            inner,
            map
        }       
    }
}


impl<S, F, B> DataSource for MappedDataSource<S, F>
where
    S: DataSource,
    B: Block,
    F: (Fn(S::Block, bool) -> B) + Unpin
{
    type Block = B;

    fn set_position(&mut self, next_block: BlockNumber, parent_block_hash: Option<&str>) {
        self.inner.set_position(next_block, parent_block_hash)
    }

    fn get_next_block(&self) -> BlockNumber {
        self.inner.get_next_block()
    }

    fn get_parent_block_hash(&self) -> Option<&str> {
        self.inner.get_parent_block_hash()
    }
}


impl<S, F, B> Stream for MappedDataSource<S, F>
where
    S: DataSource,
    F: (Fn(S::Block, bool) -> B) + Unpin
{
    type Item = DataEvent<B>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.inner.poll_next_unpin(cx).map(|maybe_event| {
            let event = maybe_event?;
            let event = event.map(&self.map);
            Some(event)
        })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}