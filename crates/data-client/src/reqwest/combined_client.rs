use crate::BlockStream;

use super::{CombinedBlockStream, ReqwestDataClient};
use async_std::future::timeout;
use futures_util::future::select;
use reqwest::IntoUrl;
use anyhow::anyhow;
use sqd_data_types::{BlockNumber, FromJsonBytes};
use std::{
    pin::{pin, Pin},
    time::Duration,
};

const DEFAULT_GRACE_PERIOD: Duration = Duration::from_millis(500);

pub struct CombinedClient<B> {
    left: ReqwestDataClient<B>,
    right: ReqwestDataClient<B>,
    grace_period: Duration,
}

impl<
        B: sqd_data_types::Block + std::marker::Unpin + std::marker::Send + FromJsonBytes + 'static,
    > CombinedClient<B>
{
    pub fn from_url(left_url: impl IntoUrl, right_url: impl IntoUrl) -> Self {
        Self {
            left: ReqwestDataClient::<B>::from_url(left_url),
            right: ReqwestDataClient::<B>::from_url(right_url),
            grace_period: DEFAULT_GRACE_PERIOD,
        }
    }

    pub fn from_url_and_grace_period(left_url: impl IntoUrl, right_url: impl IntoUrl, grace_period: Duration) -> Self {
        Self {
            left: ReqwestDataClient::<B>::from_url(left_url),
            right: ReqwestDataClient::<B>::from_url(right_url),
            grace_period,
        }
    }

    pub async fn stream(
        &self,
        from: BlockNumber,
        prev_block_hash: &str,
    ) -> anyhow::Result<Pin<Box<dyn BlockStream<Block = B, Item = anyhow::Result<B>>>>> {
        let left_future = pin!(self.left.stream(from, prev_block_hash));
        let right_future = pin!(self.right.stream(from, prev_block_hash));
        let other_future;
        let stream_a;
        match select(left_future, right_future).await {
            futures_util::future::Either::Left((item, right)) => {
                stream_a = item;
                other_future = right;
            }
            futures_util::future::Either::Right((item, left)) => {
                stream_a = item;
                other_future = left;
            }
        }
        let dur = self.grace_period;
        let stream_b = match timeout(dur, other_future).await {
            Ok(item) => item,
            Err(err) => {
                if let Ok(res) = stream_a {
                    return Ok(Box::pin(res));
                } else {
                    return Err(anyhow!(err));
                }
            }
        };
        if stream_a.is_err() && stream_b.is_err() {
            return Err(anyhow!("Both connections failed"));
        }
        if stream_a.is_err() {
            return Ok(Box::pin(stream_b?));
        }
        if stream_b.is_err() {
            return Ok(Box::pin(stream_a?));
        }
        let stream = CombinedBlockStream::new(stream_a?, stream_b?);
        Ok(Box::pin(stream))
    }
}
