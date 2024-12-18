use crate::reqwest::lines::LineStream;
use crate::{BlockRef, BlockStream};
use anyhow::{anyhow, ensure, Context};
use bytes::Bytes;
use futures_core::Stream;
use reqwest::Response;
use sqd_data_types::{Block, BlockNumber, FromJsonBytes};
use std::marker::PhantomData;
use std::pin::Pin;
use std::task::Poll;


pub(super) fn extract_finalized_head(res: &Response) -> anyhow::Result<Option<BlockRef>> {
    let number = get_finalized_head_number(res)
        .transpose()
        .context("invalid x-sqd-finalized-head-number header")?;

    let hash = get_finalized_head_hash(res)
        .transpose()
        .context("invalid x-sqd-finalized-head-hash header")?;

    match (number, hash) {
        (Some(number), Some(hash)) => Ok(Some(BlockRef::new(number, hash))),
        (None, None) => Ok(None),
        (Some(_), None) => Err(anyhow!(
            "x-sqd-finalized-head-number header is present, but x-sqd-finalized-head-hash is not"
        )),
        (None, Some(_)) => Err(anyhow!(
            "x-sqd-finalized-head-hash header is present, but x-sqd-finalized-head-number is not"
        ))
    }
}


fn get_finalized_head_number(res: &Response) -> Option<anyhow::Result<BlockNumber>> {
    res.headers().get("x-sqd-finalized-head-number").map(|v| {
        let num = v.to_str()?.parse()?;
        Ok(num)
    })
}


fn get_finalized_head_hash(res: &Response) -> Option<anyhow::Result<&str>> {
    res.headers().get("x-sqd-finalized-head-hash").map(|v| {
        let hash = v.to_str()?;
        Ok(hash)
    })
}


pub(crate) type BodyStreamBox = Box<dyn Stream<Item = reqwest::Result<Bytes>> + Unpin>;


pub struct ReqwestBlockStream<B> {
    finalized_head: anyhow::Result<Option<BlockRef>>,
    lines: Option<LineStream>,
    prev_blocks: Vec<BlockRef>,
    prev_block_hash: String,
    phantom_data: PhantomData<B>
}


impl<B> ReqwestBlockStream<B> {
    pub(super) fn new(
        finalized_head: anyhow::Result<Option<BlockRef>>,
        body: Option<BodyStreamBox>,
        prev_blocks: Vec<BlockRef>,
        prev_block_hash: &str
    ) -> Self
    {
        Self {
            finalized_head,
            lines: body.map(LineStream::new),
            prev_blocks,
            prev_block_hash: prev_block_hash.to_string(),
            phantom_data: PhantomData::default()
        }
    }

    pub fn take_finalized_head(&mut self) -> anyhow::Result<Option<BlockRef>> {
        std::mem::replace(&mut self.finalized_head, Ok(None))
    }

    pub fn finalized_head(&self) -> Option<&BlockRef> {
        self.finalized_head.as_ref().ok()?.as_ref()
    }

    pub fn prev_blocks(&self) -> &[BlockRef] {
        &self.prev_blocks
    }
}


impl<B: Block + FromJsonBytes + Unpin> Stream for ReqwestBlockStream<B> {
    type Item = anyhow::Result<B>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if let Some(lines) = this.lines.as_mut() {
            Pin::new(lines).poll_next(cx).map(|maybe_line_result| {
                maybe_line_result.map(|line_result| {
                    let line = line_result?;
                    let block = B::from_json_bytes(line)?;
                    if !this.prev_block_hash.is_empty() {
                        ensure!(
                            &this.prev_block_hash == block.parent_hash(),
                            "chain continuity was violated by upstream service between blocks {} and {}#{}",
                            this.prev_block_hash,
                            block.number(),
                            block.hash()
                        );
                    }
                    this.prev_block_hash.clear();
                    this.prev_block_hash.insert_str(0, block.hash());
                    Ok(block)
                })
            })
        } else {
            Poll::Ready(None)
        }
    }
}


impl<B: Block + FromJsonBytes + Unpin + Send> BlockStream for ReqwestBlockStream<B> {
    type Block = B;
    
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