use std::{marker::PhantomData, pin::Pin, task::Poll};

use futures_core::{FusedStream, Stream};
use futures_util::StreamExt;
use sqd_data_types::FromJsonBytes;

use crate::{BlockRef, BlockStream};

pub struct CombinedBlockStream<LeftStream, RightStream, ReturnItem> {
    left_stream: LeftStream,
    right_stream: RightStream,
    front_number: Option<u64>,
    phantom_data: PhantomData<ReturnItem>,
}

enum ItemAction {
    Skip,
    Return
}

enum StreamSide {
    Left,
    Right
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
    LeftStream: BlockStream<Block = ReturnItem>,
    RightStream: BlockStream<Block = ReturnItem>,
{
    type Item = anyhow::Result<ReturnItem>;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        loop {
            if !this.left_stream.is_terminated() {
                match this.left_stream.poll_next_unpin(cx) {
                    Poll::Ready(Some(item)) => {
                        match Self::process_item(this, &item, StreamSide::Left) {
                            ItemAction::Skip => continue,
                            ItemAction::Return => return Poll::Ready(Some(item)),
                        }
                    }
                    Poll::Ready(None) => {}
                    Poll::Pending => {}
                }
            }
            if !this.right_stream.is_terminated() {
                match this.right_stream.poll_next_unpin(cx) {
                    Poll::Ready(Some(item)) => {
                        match Self::process_item(this, &item, StreamSide::Right) {
                            ItemAction::Skip => continue,
                            ItemAction::Return => return Poll::Ready(Some(item)),
                        }
                    }
                    Poll::Ready(None) => {}
                    Poll::Pending => {}
                }
            }
            if this.left_stream.is_terminated() && this.right_stream.is_terminated() {
                return Poll::Ready(None);
            }
            return Poll::Pending;
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (0, None)
    }
}

impl<LeftStream: BlockStream, RightStream: BlockStream, ReturnItem: sqd_data_types::Block>
    CombinedBlockStream<LeftStream, RightStream, ReturnItem>
{
    fn process_item(
        this: &mut Self,
        item: &anyhow::Result<ReturnItem>,
        side: StreamSide,
    ) -> ItemAction {
        let finalized_head_option = match side {
            StreamSide::Left => this.left_stream.finalized_head(),
            StreamSide::Right => this.right_stream.finalized_head(),
        };
        let Some(finalized_head) = finalized_head_option else {
            return ItemAction::Skip
        };
        let finalized_number = finalized_head.number();
        let Ok(item) = item else {
            return ItemAction::Skip
        };
        let current_number = item.number();
        if current_number > finalized_number {
            return ItemAction::Skip
        }
        let last_number = this.front_number.unwrap_or(0);
        if last_number >= current_number {
            return ItemAction::Skip
        }
        this.front_number = Some(current_number);
        ItemAction::Return
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
