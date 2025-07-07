use super::store::Store;
use crate::chain_watch::ChainSender;
use crate::util::compute_fork_base;
use anyhow::{bail, ensure};
use futures::StreamExt;
use sqd_data_source::{DataEvent, DataSource};
use sqd_primitives::{Block, BlockNumber, BlockRef};
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
    // FIXME: check already ingested blocks
    data_source.set_position(first_block, parent_block_hash.as_deref());

    let mut last_push = Instant::now();
    let mut buf: Vec<S::Block> = Vec::new();

    loop {
        select! {
            biased;
            event = data_source.next() => {
                let Some(event) = event else {
                    break
                };
                match event {
                    DataEvent::FinalizedHead(_) => {},
                    DataEvent::Block { block, .. } => {
                        buf.push(block);
                        let now = Instant::now();
                        if buf.len() >= 5 || now - last_push > Duration::from_millis(10) {
                            chain_sender.extend(buf.drain(..));
                            last_push = now;
                        }
                    }, 
                    DataEvent::MaybeOnHead => {
                        if !buf.is_empty() {
                            chain_sender.extend(buf.drain(..));
                            last_push = Instant::now()
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
                chain_sender.extend(buf.drain(..));
                last_push = Instant::now()
            }
        }
    }
    
    Ok(())
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
        ($list:expr, $i:expr) => {
            let block = &$list[$i];
            data_source.set_position(block.number() + 1, Some(block.hash()));
            return Ok(())
        };
    }

    let prev = &mut prev;

    if let Some(i) = compute_fork_base(&buf, prev) {
        buf.truncate(i + 1);
        return_block!(buf, i);
    }

    let first_buffered_block = buf.first().cloned();
    buf.clear();
    
    {
        let chain = chain_sender.borrow();
        let (head, tail) = chain.block_slices();
        if let Some(i) = compute_fork_base(tail, prev) {
            return_block!(tail, i);
        }
        if let Some(i) = compute_fork_base(head, prev) {
            return_block!(head, i);
        }
        if prev.is_empty() {
            let (number, hash) = chain.first()
                .map(|b| (b.parent_number(), b.parent_hash()))
                .unwrap_or_else(|| {
                    let b = first_buffered_block.as_ref().expect("block buffer should have been non-empty");
                    (b.parent_number(), b.parent_hash())
                });
            data_source.set_position(number + 1, Some(hash));
            return Ok(())
        }
    }
    
    {
        let Some(offset) = prev.iter().position(|b| b.number >= first_block) else {
            if let Some(parent_hash) = parent_block_hash {
                bail!(
                    "parent hash of the first requested block {} does not have an expected value of {}", 
                    first_block, 
                    parent_hash
                );
            } else {
                data_source.set_position(first_block, None);
                return Ok(())
            }
        };
        *prev = &prev[offset..];
    }
    
    if let Some(i) = store.compute_fork(prev).await? {
        data_source.set_position(prev[i].number + 1, Some(&prev[i].hash));
        return Ok(())
    }
    
    data_source.set_position(
        first_block, 
        parent_block_hash
    );

    Ok(())
}