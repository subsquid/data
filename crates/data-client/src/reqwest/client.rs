use crate::reqwest::stream::{extract_finalized_head, ReqwestBlockStream};
use crate::{BlockStreamRequest, DataClient};
use anyhow::{anyhow, Context};
use futures::future::BoxFuture;
use reqwest::{Client, IntoUrl, Request, Response, Url};
use serde_json::json;
use sqd_primitives::{Block, BlockRef, FromJsonBytes};
use std::error::Error;
use std::future::Future;
use std::io::ErrorKind;
use std::marker::PhantomData;
use std::time::Duration;
use tracing::warn;


pub fn default_http_client() -> Client {
    Client::builder()
        // .read_timeout(Duration::from_secs(5))
        // .connect_timeout(Duration::from_secs(5))
        .gzip(true)
        .build()
        .unwrap()
}


#[derive(Clone)]
pub struct ReqwestDataClient<B> {
    http: Client,
    url: Url,
    phantom_data: PhantomData<B>
}


impl <B> ReqwestDataClient<B> {
    pub fn from_url(url: impl IntoUrl) -> Self {
        let http = default_http_client();
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
        stream_req: BlockStreamRequest<'_>
    ) -> anyhow::Result<ReqwestBlockStream<B>>
    {
        let mut body = json!({
            "fromBlock": stream_req.first_block
        });

        if stream_req.prev_block_hash.is_some() {
            body.as_object_mut().unwrap().insert(
                "prevBlockHash".into(),
                stream_req.prev_block_hash.into()
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
                        stream_req.prev_block_hash
                    )
                },
                204 => {
                    ReqwestBlockStream::new(
                        extract_finalized_head(&res),
                        None,
                        vec![],
                        None
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
                        None
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
            let retry_error = match self.http.execute(req.try_clone().unwrap()).await {
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

            let pause = retry_schedule[std::cmp::min(retry_attempt, retry_schedule.len() - 1)];

            warn!(
                url = req.url().as_str(),
                method = req.method().as_str(),
                error = retry_error.as_ref() as &dyn std::error::Error,
                "http request failed, will retry it in {} ms", 
                pause
            );
            
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


impl<B: Block + FromJsonBytes + Unpin + Send + Sync> DataClient for ReqwestDataClient<B> {
    type BlockStream = ReqwestBlockStream<B>;

    fn stream<'a>(&'a self, req: BlockStreamRequest<'a>) -> BoxFuture<'a, anyhow::Result<Self::BlockStream>>
    {
        Box::pin(self.stream(req))
    }
}