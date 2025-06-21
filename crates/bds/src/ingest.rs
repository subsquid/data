use crate::types::{Block, Sink};
use sqd_data_source::DataSource;
use sqd_primitives::BlockNumber;


pub async fn ingest(
    data_source: impl DataSource<Block = Block<'static>>,
    sink: impl Sink,
    first_block: BlockNumber,
    parent_block_hash: Option<&str>
) -> anyhow::Result<()> 
{
    Ok(())
}