use super::{CombinedBlockStream, ReqwestBlockStream, ReqwestDataClient};
use anyhow::anyhow;
use futures_util::future::select;
use reqwest::IntoUrl;
use sqd_data_types::{BlockNumber, FromJsonBytes};
use std::{pin::pin, time::Duration};
use async_std::future::timeout;

pub struct CombinedClient<B> {
    left: ReqwestDataClient<B>,
    right: ReqwestDataClient<B>,
}

impl<B: sqd_data_types::Block + std::marker::Unpin + std::marker::Send + FromJsonBytes> CombinedClient<B> {
    pub fn from_url(left_url: impl IntoUrl, right_url: impl IntoUrl) -> Self {
        Self {
            left: ReqwestDataClient::<B>::from_url(left_url),
            right: ReqwestDataClient::<B>::from_url(right_url),
        }
    }

    pub async fn stream(
        &self,
        from: BlockNumber,
        prev_block_hash: &str,
    ) -> anyhow::Result<CombinedBlockStream<ReqwestBlockStream<B>, ReqwestBlockStream<B>, B>> {
        let left_future = pin!(self.left.stream(from, prev_block_hash));
        let right_future = pin!(self.right.stream(from, prev_block_hash));
        let other_future;
        let stream_a;
        let stream_b;
        match select(left_future, right_future).await {
            futures_util::future::Either::Left((item, right)) => {
                println!("RIGHT");
                stream_a = item;
                other_future = right;
            },
            futures_util::future::Either::Right((item, left)) => {
                println!("LEFT");
                stream_a = item;
                other_future = left;
            },
        }
        let dur = Duration::from_millis(500);
        match timeout(dur, other_future).await {
            Ok(item) => {
                println!("GOT other");
                stream_b = item;
            },
            Err(_) => {
                println!("NO ANSWER");
                return Err(anyhow!(""));
            },
        }
        let stream = CombinedBlockStream::new(stream_a.unwrap(), stream_b.unwrap());
        Ok(stream)
        //let timer_future = None;
        //Err(anyhow!(""))
    }
}