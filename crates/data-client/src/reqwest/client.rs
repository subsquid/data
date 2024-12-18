use crate::reqwest::stream::{extract_finalized_head, ReqwestBlockStream};
use crate::{BlockRef, DataClient};
use anyhow::{anyhow, Context};
use futures_core::future::BoxFuture;
use reqwest::{Client, IntoUrl, Request, Response, Url};
use serde_json::json;
use sqd_data_types::{Block, BlockNumber, FromJsonBytes};
use std::error::Error;
use std::future::Future;
use std::io::ErrorKind;
use std::marker::PhantomData;
use std::time::Duration;


#[derive(Clone)]
pub struct ReqwestDataClient<B> {
    http: Client,
    url: Url,
    phantom_data: PhantomData<B>
}


impl <B> ReqwestDataClient<B> {
    pub fn from_url(url: impl IntoUrl) -> Self {
        let http = Client::builder()
            .read_timeout(Duration::from_secs(5))
            .connect_timeout(Duration::from_secs(5))
            .gzip(true)
            .build()
            .unwrap();
        
        Self::new(http, url)
    }
    
    pub fn new(http: Client, url: impl IntoUrl) -> Self {
        Self {
            http,
            url: url.into_url().unwrap(),
            phantom_data: PhantomData::default()
        }
    }
    
    pub async fn stream(
        &self,
        from: BlockNumber,
        prev_block_hash: &str
    ) -> anyhow::Result<ReqwestBlockStream<B>>
    {
        let mut body = json!({
            "fromBlock": from
        });

        if !prev_block_hash.is_empty() {
            body.as_object_mut().unwrap().insert(
                "prevBlockHash".into(),
                prev_block_hash.into()
            );
        }

        let mut url = self.url.clone();
        url.path_segments_mut().unwrap().push("stream");

        let req = self.http
            .post(url.clone())
            .json(&body)
            .build()?;

        self.with_retries(&req, |res| async {
            Ok(match res.status().as_u16() {
                200 => {
                    ReqwestBlockStream::new(
                        extract_finalized_head(&res),
                        Some(Box::new(res.bytes_stream())),
                        vec![],
                        prev_block_hash
                    )
                },
                204 => {
                    ReqwestBlockStream::new(
                        extract_finalized_head(&res),
                        None,
                        vec![],
                        ""
                    )
                },
                409 => {
                    let finalized_head = extract_finalized_head(&res);

                    let prev_blocks: Vec<BlockRef> = res
                        .json()
                        .await
                        .context(
                            "failed to receive a list of previous blocks after base-block hash mismatch"
                        )?;

                    ReqwestBlockStream::new(
                        finalized_head,
                        None,
                        prev_blocks,
                        ""
                    )
                },
                status if status < 300 => return Err(
                    anyhow!("unexpected success response status - {}", status)
                ),
                _ => return Err(response_error(res).await)
            })
        }).await
    }

    async fn with_retries<R, F, Fut>(
        &self,
        req: &Request,
        mut cb: F
    ) -> anyhow::Result<R>
    where
        F: FnMut(Response) -> Fut,
        Fut: Future<Output=anyhow::Result<R>>
    {
        let mut retry_attempt = 0;
        let retry_schedule = [0, 100, 200, 500, 1000, 2000];
        loop {
            let _retry_error = match self.http.execute(req.try_clone().unwrap()).await {
                Ok(res) => match res.status().as_u16() {
                    429 | 502 | 503 | 504 | 524 => response_error(res).await,
                    _ => match cb(res).await {
                        Ok(res) => return Ok(res),
                        Err(err) => if is_retryable(err.as_ref()) {
                            err
                        } else {
                            return Err(err)
                        }
                    }
                },
                Err(err) if err.is_timeout() || err.is_connect() || err.is_request() => {
                    anyhow!(err)
                },
                Err(err) => return Err(err.into())
            };
            
            let pause = retry_schedule[std::cmp::max(retry_attempt, retry_schedule.len() - 1)];
            retry_attempt = retry_attempt.saturating_add(1);
            futures_timer::Delay::new(Duration::from_millis(pause)).await;
        }
    }
}


async fn response_error(response: Response) -> anyhow::Error {
    let status = response.status().as_u16();
    if let Some(text) = response.text().await.ok() {
        anyhow!("got HTTP {}: {}", status, text)
    } else {
        anyhow!("got HTTP {}", status)
    }
}


pub(crate) fn is_retryable(err: &(dyn Error + 'static)) -> bool {
    if let Some(io_err) = err.downcast_ref::<std::io::Error>() {
        is_retryable_io(io_err)
    } else {
        err.source().map(is_retryable).unwrap_or(false)
    }
}


fn is_retryable_io(err: &std::io::Error) -> bool {
    match err.kind() {
        ErrorKind::ConnectionReset => true,
        ErrorKind::ConnectionAborted => true,
        ErrorKind::TimedOut => true,
        _ => false
    }
}


impl<B: Block + FromJsonBytes + Unpin + Send> DataClient for ReqwestDataClient<B> {
    type BlockStream = ReqwestBlockStream<B>;

    fn stream<'a>(&'a self, from: BlockNumber, prev_block_hash: &'a str) -> BoxFuture<'a, anyhow::Result<Self::BlockStream>>
    where
        Self: Sync + 'a
    {
        Box::pin(self.stream(from, prev_block_hash))
    }
}