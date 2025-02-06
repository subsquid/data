use async_stream::try_stream;
use futures::{Stream, TryStreamExt};
use serde::Serialize;
use sqd_primitives::BlockNumber;
use std::io::{Error, ErrorKind};
use tokio::io::AsyncBufReadExt;
use tokio_util::io::StreamReader;
use url::Url;


#[derive(Serialize)]
struct BlockRange {
    from: BlockNumber,
    to: Option<BlockNumber>,
}


pub fn ingest_from_service(
    url: Url,
    from: BlockNumber,
    to: Option<BlockNumber>
) -> impl Stream<Item = anyhow::Result<String>>
{
    try_stream! {
        let byte_stream = reqwest::Client::new()
            .post(url.as_str())
            .json(&BlockRange { from, to })
            .send()
            .await?
            .error_for_status()?
            .bytes_stream()
            .map_err(|err| Error::new(ErrorKind::Other, err));
        
        let mut line_stream = StreamReader::new(byte_stream).lines();
        
        while let Some(line) = line_stream.next_line().await? {
            yield line;
        }
    }
}
