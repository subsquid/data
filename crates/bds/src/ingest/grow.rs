use super::store::Store;
use crate::chain_watch::ChainSender;
use crate::util::compute_fork_base;
use anyhow::{anyhow, bail, ensure};
use futures::StreamExt;
use sqd_data_source::{DataEvent, DataSource};
use sqd_primitives::{Block, BlockNumber, BlockPtr, BlockRef};
use std::time::Duration;
use tokio::select;
use tokio::time::{sleep_until, Instant};
use tracing::debug;


pub async fn grow<B: Block>(
    mut data_source: impl DataSource<Block = B>,
    first_block: BlockNumber,
    parent_block_hash: Option<String>,
    chain_sender: ChainSender<B>
) -> anyhow::Result<()>
{
    debug!(
        first_block = data_source.get_next_block(),
        parent_hash =% data_source.get_parent_block_hash().unwrap_or("None"),
        "starting data ingestion"
    );
    
    loop {
        let Some(event) = data_source.next().await else {
            bail!("unexpected end of data source stream")
        };
        
        match event {
            DataEvent::FinalizedHead(head) => {
                // check, that we have all blocks below the head,
                // otherwise, we might get erroneous block mismatch condition 
                // due to forked blocks been still present in the chain
                if head.number < data_source.get_next_block() {
                    chain_sender.finalize(head.ptr())?;
                }
            },
            DataEvent::Block { block, is_final } => {
                if !chain_sender.push(is_final, block)? {
                    chain_sender.wait().await;
                }
            },
            DataEvent::MaybeOnHead => {},
            DataEvent::Fork(prev) => handle_fork(
                &prev,
                first_block,
                parent_block_hash.as_deref(),
                &chain_sender,
                &mut data_source
            )?
        }
    }
}


fn handle_fork<B: Block>(
    mut prev: &[BlockRef],
    first_block: BlockNumber,
    parent_block_hash: Option<&str>,
    chain_sender: &ChainSender<B>,
    data_source: &mut impl DataSource
) -> anyhow::Result<()> 
{
    ensure!(data_source.get_parent_block_hash().is_some());
    
    ensure!(!prev.is_empty(), "got a fork event with no previous blocks");
    
    ensure!(
        prev.windows(2).all(|s| s[0].number < s[1].number),
        "got a fork event with a list of previous blocks not in ascending order"
    );

    if first_block == data_source.get_next_block() && data_source.get_parent_block_hash() == parent_block_hash {
        bail!(
            "first requested block {} expected to have parent hash {}, but its actual base block is {}",
            first_block,
            parent_block_hash.unwrap(),
            prev.last().unwrap()
        );
    }

    {
        // we are not interested in going below `first_block`.
        let Some(offset) = prev.iter().position(|b| b.number >= first_block) else {
            // all `prev` blocks are below `first_block`,
            data_source.set_position(first_block, parent_block_hash.as_deref());
            return Ok(())
        };
        prev = &prev[offset..];
    }
    
    let chain = chain_sender.borrow();
    
    if let Some(head) = chain.compute_fork_base(prev)? {
        if head.number >= first_block {
            data_source.set_position(head.number + 1, Some(head.hash));
            return Ok(())
        }
    }

    data_source.set_position(
        first_block, 
        parent_block_hash
    );

    Ok(())
}