use super::lines::LineStream;
use crate::types::{BlockStreamRequest, BlockStreamResponse};
use crate::DataClient;
use anyhow::{anyhow, bail, ensure, Context};
use bytes::Bytes;
use futures::future::BoxFuture;
use futures::{FutureExt, StreamExt};
use reqwest::{Client, IntoUrl, Response, Url};
use serde_json::json;
use sqd_primitives::{BlockNumber, BlockRef};
use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use std::time::Duration;


pub fn default_http_client() -> Client {
    Client::builder()
        .read_timeout(Duration::from_secs(20))
        .connect_timeout(Duration::from_secs(20))
        .gzip(true)
        .build()
        .unwrap()
}


#[derive(Clone)]
pub struct ReqwestDataClient {
    http: Client,
    url: Arc<Url>
}


impl Debug for ReqwestDataClient {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReqwestDataClient")
            .field("url", &self.url.as_str())
            .finish()
    }
}


impl ReqwestDataClient {
    pub fn from_url(url: impl IntoUrl) -> Self {
        let http = default_http_client();
        Self::new(http, url)
    }
    
    pub fn new(http: Client, url: impl IntoUrl) -> Self {
        Self {
            http,
            url: Arc::new(url.into_url().unwrap())
        }
    }

    pub async fn stream(
        &self,
        req: BlockStreamRequest
    ) -> anyhow::Result<BlockStreamResponse<Bytes>>
    {
        let mut body = json!({
            "fromBlock": req.first_block
        });

        if req.parent_block_hash.is_some() {
            body.as_object_mut().unwrap().insert(
                "prevBlockHash".into(),
                req.parent_block_hash.clone().into()
            );
        }

        let mut url = self.url.as_ref().clone();
        url.path_segments_mut().unwrap().push("stream");

        let res = self.http
            .post(url)
            .json(&body)
            .send()
            .await?
            .error_for_status()?;

        match res.status().as_u16() {
            200 => {
                let finalized_head = extract_finalized_head(&res)?;
                let blocks = LineStream::new(res.bytes_stream());
                Ok(BlockStreamResponse::Stream {
                    blocks: blocks.boxed(),
                    finalized_head
                })
            },
            204 => {
                let finalized_head = extract_finalized_head(&res)?;
                Ok(BlockStreamResponse::Stream {
                    blocks: futures::stream::empty().boxed(),
                    finalized_head
                })
            },
            409 => {
                let prev_blocks: Vec<BlockRef> = res
                    .json()
                    .await
                    .context(
                        "failed to receive a list of previous blocks after base-block hash mismatch"
                    )?;
                ensure!(
                    !prev_blocks.is_empty(),
                    "got an empty list of prev blocks"
                );
                Ok(BlockStreamResponse::Fork(prev_blocks))
            },
            _ => bail!("unexpected HTTP response status: {}", res.status().as_u16())
        }
    }

    pub async fn get_finalized_head(&self) -> anyhow::Result<Option<BlockRef>> {
        self.fetch_head("finalized-head").await
    }

    pub async fn get_head(&self) -> anyhow::Result<Option<BlockRef>> {
        self.fetch_head("head").await
    }

    async fn fetch_head(&self, slug: &str) -> anyhow::Result<Option<BlockRef>> {
        let mut url = self.url.as_ref().clone();
        url.path_segments_mut().unwrap().push(slug);

        let head: Option<BlockRef> = self.http
            .get(url)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await
            .context("failed to parse returned block reference")?;

        Ok(head)
    }
}

fn extract_finalized_head(res: &Response) -> anyhow::Result<Option<BlockRef>> {
    let number = get_finalized_head_number(res)
        .transpose()
        .context("invalid x-sqd-finalized-head-number header")?;

    let hash = get_finalized_head_hash(res)
        .transpose()
        .context("invalid x-sqd-finalized-head-hash header")?;

    match (number, hash) {
        (Some(number), Some(hash)) => Ok(Some(BlockRef {
            number,
            hash: hash.to_string()
        })),
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


impl DataClient for ReqwestDataClient {
    type Block = Bytes;

    fn stream(&self, req: BlockStreamRequest) -> BoxFuture<'static, anyhow::Result<BlockStreamResponse<Self::Block>>> {
        let this = self.clone();
        async move {
            this.stream(req).await
        }.boxed()
    }

    fn get_finalized_head(&self) -> BoxFuture<'static, anyhow::Result<Option<BlockRef>>> {
        let this = self.clone();
        async move {
            this.get_finalized_head().await
        }.boxed()
    }

    fn is_retryable(&self, _err: &anyhow::Error) -> bool {
        true
    }
}