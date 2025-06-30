use futures::StreamExt;
use tokio::select;
use super::store::Store;
use crate::chain_sender::ChainSender;
use sqd_data_source::DataSource;


pub async fn ingest<S: Store>(
    store: S,
    chain_sender: ChainSender<S::Block>,
    mut data_source: impl DataSource<Block = S::Block>
) -> anyhow::Result<()> 
{
    let first_block = data_source.get_next_block();
    let parent_block_hash = data_source.get_parent_block_hash().map(|s| s.to_string());
    
    loop {
        select! {
            biased;
            event = data_source.next() => {
                let Some(event) = event else {
                    break
                };    
            }
        }
    }
    
    Ok(())
}