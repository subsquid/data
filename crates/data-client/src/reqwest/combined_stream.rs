use std::{marker::PhantomData, pin::Pin, task::Poll};

use futures_core::{FusedFuture, FusedStream, Stream};
use futures_util::{FutureExt, StreamExt};
use sqd_data_types::FromJsonBytes;

use crate::{BlockRef, BlockStream};

pub struct CombinedBlockStream<LeftStream, RightStream, ReturnItem> {
    left_stream: LeftStream,
    right_stream: RightStream,
    front_number: Option<u64>,
    phantom_data: PhantomData<ReturnItem>,
}

impl<LeftStream: BlockStream, RightStream: BlockStream, ReturnItem: sqd_data_types::Block>
    CombinedBlockStream<LeftStream, RightStream, ReturnItem>
{
    pub fn new(left_stream: LeftStream, right_stream: RightStream) -> Self {
        Self {
            left_stream,
            right_stream,
            front_number: None,
            phantom_data: Default::default(),
        }
    }

    pub fn take_finalized_head(&mut self) -> anyhow::Result<Option<BlockRef>> {
        //std::mem::replace(&mut self.finalized_head, Ok(None))
        self.left_stream.take_finalized_head()
    }

    pub fn finalized_head(&self) -> Option<&BlockRef> {
        //self.finalized_head.as_ref().ok()?.as_ref()
        let left = self.left_stream.finalized_head();
        let right = self.right_stream.finalized_head();
        if left.is_none() {
            return right;
        };
        if right.is_none() {
            return left;
        }
        if left.unwrap().number() > right.unwrap().number() {
            left
        } else {
            right
        }
    }

    pub fn prev_blocks(&self) -> &[BlockRef] {
        //&self.prev_blocks
        self.left_stream.prev_blocks()
    }
}

impl<
        LeftStream: Unpin + FusedStream,
        RightStream: Unpin + FusedStream,
        ReturnItem: sqd_data_types::Block + FromJsonBytes + Unpin,
    > Stream for CombinedBlockStream<LeftStream, RightStream, ReturnItem>
where
    // LeftStream: BlockStream<Item = anyhow::Result<ReturnItem>>,
    // RightStream: BlockStream<Item = anyhow::Result<ReturnItem>>,
    LeftStream: BlockStream<Block = ReturnItem>,
    RightStream: BlockStream<Block = ReturnItem>,
{
    type Item = anyhow::Result<ReturnItem>;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let future_left = this.left_stream.select_next_some();
        let future_right = this.right_stream.select_next_some();
        let mut res = futures_util::future::select(future_left, future_right);

        loop {
            match res.poll_unpin(cx) {
                Poll::Ready(omg) => match omg {
                    futures_util::future::Either::Left((item, right)) => {
                        if let Some(res) = Self::process_item(
                            &mut this.front_number,
                            this.left_stream.finalized_head(),
                            item,
                        ) {
                            return res;
                        }
                        let left = this.left_stream.select_next_some();
                        res = futures_util::future::select(left, right);
                    }
                    futures_util::future::Either::Right((item, left)) => {
                        if let Some(res) = Self::process_item(
                            &mut this.front_number,
                            this.right_stream.finalized_head(),
                            item,
                        ) {
                            return res;
                        }
                        let right = this.right_stream.select_next_some();
                        res = futures_util::future::select(left, right);
                    }
                },
                Poll::Pending => {
                    if res.is_terminated() {
                        return Poll::Ready(None);
                    }
                }
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (0, None)
    }
}

impl<LeftStream, RightStream, ReturnItem: sqd_data_types::Block>
    CombinedBlockStream<LeftStream, RightStream, ReturnItem>
{
    fn process_item(
        front_number: &mut std::option::Option<u64>,
        finalized_head: std::option::Option<&BlockRef>,
        item: anyhow::Result<ReturnItem>,
    ) -> std::option::Option<Poll<std::option::Option<anyhow::Result<ReturnItem>>>> {
        let finalized_head = finalized_head?;
        let finalized_number = finalized_head.number();
        let Ok(item) = item else {
            return None;
        };
        let current_number = item.number();
        if current_number > finalized_number {
            return None;
        }
        let last_number = front_number.unwrap_or(0);
        if last_number >= current_number {
            return None;
        }
        *front_number = Some(current_number);
        Some(Poll::Ready(Some(Ok(item))))
    }
}

impl<
        LeftStream: Unpin + FusedStream,
        RightStream: Unpin + FusedStream,
        ReturnItem: sqd_data_types::Block + FromJsonBytes + Unpin + Send,
    > BlockStream for CombinedBlockStream<LeftStream, RightStream, ReturnItem>
where
    LeftStream: BlockStream<Block = ReturnItem>,
    RightStream: BlockStream<Block = ReturnItem>,
{
    type Block = ReturnItem;

    fn take_finalized_head(&mut self) -> anyhow::Result<Option<BlockRef>> {
        self.take_finalized_head()
    }

    fn finalized_head(&self) -> Option<&BlockRef> {
        self.finalized_head()
    }

    fn prev_blocks(&self) -> &[BlockRef] {
        self.prev_blocks()
    }
}
