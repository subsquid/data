use async_stream::try_stream;
use futures::{Stream, StreamExt, TryStreamExt};
use serde::de::DeserializeOwned;
use serde::Serialize;
use sqd_primitives::{Block, BlockNumber};
use std::io::{Error, ErrorKind};
use std::pin::pin;
use std::time::Duration;
use tokio::io::AsyncBufReadExt;
use tokio_util::io::StreamReader;
use tracing::{error, info};
use url::Url;


#[derive(Serialize)]
struct BlockRange {
    from: BlockNumber,
    to: Option<BlockNumber>,
}


pub fn ingest_from_service<B: Block + DeserializeOwned>(
    url: Url,
    from: BlockNumber,
    to: Option<BlockNumber>,
    block_stream_interval: Duration
) -> impl Stream<Item = anyhow::Result<B>>
{
    try_stream! {
        let mut first_block = from;

        while to.map_or(true, |last_block| first_block <= last_block) {
            let mut stream = pin! {
                fetch::<B>(url.clone(), first_block, to)
            };

            let mut stream_is_empty = true;

            while let Some(block_result) = stream.next().await {
                stream_is_empty = false;
                match block_result {
                    Ok(block) => {
                        first_block = block.number() + 1;
                        yield block
                    },
                    Err(err) => {
                        if first_block > from {
                            error!(err =? err, "data streaming error, will pause for 5 sec and try again");
                            tokio::time::sleep(Duration::from_secs(5)).await;
                        } else {
                            Err(err)?;
                        }
                    }
                }
            }

            if stream_is_empty {
                info!(
                    "no blocks were found. waiting {} sec for a new try",
                    block_stream_interval.as_secs()
                );
                tokio::time::sleep(block_stream_interval).await;
            }
        }
    }
}


fn fetch<B: Block + DeserializeOwned>(
    url: Url,
    from: BlockNumber,
    to: Option<BlockNumber>
) -> impl Stream<Item = anyhow::Result<B>>
{
    try_stream! {
        let byte_stream = reqwest::Client::new()
            .post(url)
            .json(&BlockRange { from, to })
            .send()
            .await?
            .error_for_status()?
            .bytes_stream()
            .map_err(|err| Error::new(ErrorKind::Other, err));
        
        let mut line_stream = StreamReader::new(byte_stream).lines();
        
        while let Some(line) = line_stream.next_line().await? {
            let block = serde_json::from_str(&line)?;
            yield block;
        }
    }
}
