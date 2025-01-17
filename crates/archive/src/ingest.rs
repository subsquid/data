use futures_core::Stream;
use futures_util::TryStreamExt;
use serde::Serialize;
use sqd_primitives::BlockNumber;
use bytes::{Bytes, Buf};
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
) -> impl Stream<Item = Result<Bytes, reqwest::Error>>
{
    async_stream::try_stream! {
        let range = BlockRange { from, to };
        let client = reqwest::Client::new();
        let res = client.post(url.as_str()).json(&range).send().await?;
        let mut stream = res.bytes_stream();
        let mut buf = bytes::BytesMut::new();
        while let Some(bytes) = stream.try_next().await? {
            buf.extend_from_slice(&bytes);
            let pos = buf.iter().position(|b| *b == b'\n');
            if let Some(pos) = pos {
                let line = buf.split_to(pos).freeze();
                buf.advance(1);
                yield line;
            }
        }
    }
}
