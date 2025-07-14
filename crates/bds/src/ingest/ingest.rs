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


pub async fn grow_chain<S: Store>(
    store: S,
    first_block: BlockNumber,
    parent_block_hash: Option<String>,
    chain_sender: ChainSender<S::Block>,
    mut data_source: impl DataSource<Block = S::Block>
) -> anyhow::Result<()>
{
    if let Some(head) = store.get_chain_head(first_block, parent_block_hash.as_deref()).await? {
        data_source.set_position(head.number + 1, Some(head.hash.as_str()));
    } else {
        data_source.set_position(first_block, parent_block_hash.as_deref());
    }

    let mut last_push = Instant::now();
    let mut buf: Vec<S::Block> = Vec::new();

    loop {
        select! {
            biased;
            event = data_source.next() => {
                let Some(event) = event else {
                    bail!("unexpected end of data source stream")
                };
                match event {
                    DataEvent::FinalizedHead(_) => {},
                    DataEvent::Block { block, .. } => {
                        buf.push(block);
                        let now = Instant::now();
                        if buf.len() >= 5 || now - last_push > Duration::from_millis(10) {
                            last_push = now;
                            chain_sender.extend_and_wait(buf.drain(..)).await;
                        }
                    }, 
                    DataEvent::MaybeOnHead => {
                        if !buf.is_empty() {
                            last_push = Instant::now();
                            chain_sender.extend_and_wait(buf.drain(..)).await
                        }
                    },
                    DataEvent::Fork(prev) => handle_fork(
                        &prev,
                        &store,
                        first_block,
                        parent_block_hash.as_deref(),
                        &mut buf,
                        &chain_sender,
                        &mut data_source
                    ).await?,
                } 
            },
            _ =  sleep_until(last_push + Duration::from_millis(20)), if !buf.is_empty() => {
                last_push = Instant::now();
                chain_sender.extend_and_wait(buf.drain(..)).await
            }
        }
    }
}


async fn handle_fork<S: Store>(
    mut prev: &[BlockRef],
    store: &S,
    first_block: BlockNumber,
    parent_block_hash: Option<&str>,
    buf: &mut Vec<S::Block>,
    chain_sender: &ChainSender<S::Block>,
    data_source: &mut impl DataSource<Block = S::Block>
) -> anyhow::Result<()> 
{
    ensure!(!prev.is_empty(), "got a fork event with no previous blocks");
    
    ensure!(
        prev.windows(2).all(|s| s[0].number < s[1].number),
        "got a fork event with a list of previous blocks not in ascending order"
    );

    macro_rules! return_block {
        ($b:ident) => {
            data_source.set_position($b.number + 1, Some(&$b.hash));
            return Ok(())
        };
    }

    let prev = &mut prev;
    {
        // we are not interested in going below `first_block`.
        let Some(offset) = prev.iter().position(|b| b.number >= first_block) else {
            // all `prev` blocks are below `first_block`,
            match (data_source.get_next_block(), data_source.get_parent_block_hash()) {
                (next_block, Some(parent_hash)) if next_block == first_block => {
                    bail!(
                        "parent hash of the first requested block {} does not have an expected value of {}",
                        first_block,
                        parent_hash
                    );
                },
                _ => {
                    data_source.set_position(first_block, parent_block_hash.as_deref());
                    return Ok(())
                }
            }
        };
        *prev = &prev[offset..];
    }

    if let Some(block_ref) = compute_fork_base(buf.iter().rev(), prev) {
        buf.retain(|b| b.number() <= block_ref.number);
        return_block!(block_ref);
    }

    buf.clear();
    
    {
        let chain = chain_sender.borrow();
        if let Some(b) = compute_fork_base(chain.iter().rev(), prev) {
            return_block!(b);
        }
        if let Some(base) = chain.base() {
            if base.number < data_source.get_next_block() {
                data_source.set_position(base.number + 1, Some(base.hash));
            }
        }
    }

    let head = BlockPtr {
        number: data_source.get_next_block() - 1,
        hash: data_source.get_parent_block_hash().ok_or_else(|| {
            anyhow!("data source got rollback, while no parent_hash was specified")
        })?
    };

    if let Some(b) = store.compute_fork_base(head, prev).await? {
        if b.number >= first_block {
            return_block!(b);
        }
    }
    
    data_source.set_position(
        first_block, 
        parent_block_hash
    );

    Ok(())
}