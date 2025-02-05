use futures_core::Stream;
use futures_util::TryStreamExt;
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
) -> impl Stream<Item = Result<Vec<u8>, anyhow::Error>>
{
    async_stream::try_stream! {
        let range = BlockRange { from, to };
        let client = reqwest::Client::new();
        let res = client.post(url.as_str()).json(&range).send().await?;
        let stream = res.bytes_stream().map_err(|err| Error::new(ErrorKind::Other, err));
        let mut reader = StreamReader::new(stream);
        loop {
            let mut buf = vec![];
            if 0 == reader.read_until(b'\n', &mut buf).await? {
                break;
            }
            yield buf;
        }
    }
}
