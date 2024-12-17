use futures_core::Stream;
use serde::Serialize;
use sqd_primitives::BlockNumber;
use url::Url;


#[derive(Serialize)]
struct BlockRange {
    from: BlockNumber,
    to: Option<BlockNumber>,
}


pub fn ingest_from_service(
    _url: Url,
    _from: BlockNumber,
    _to: Option<BlockNumber>
) -> impl Stream<Item = anyhow::Result<String>>
{
    futures_util::stream::empty()
}
