use crate::chain::Chain;
use crate::chain_sender::ChainSender;
use crate::ingest::store::Store;
use futures::stream::FuturesUnordered;
use futures::{StreamExt, TryStreamExt};
use sqd_primitives::{Block, BlockNumber};
use std::future::Future;
use tokio::select;


pub async fn write_chain<S: Store>(
    store: S,
    chain_sender: ChainSender<S::Block>,
) -> anyhow::Result<()>
{
    let mut chain_receiver = chain_sender.subscribe();
    let mut writes = FuturesUnordered::new();
    let mut position = Position::new();

    loop {
        let max_pending_writes = store.max_pending_writes();
        assert!(max_pending_writes > 0);
        
        if writes.len() < max_pending_writes && !position.waits_for_new_block {
            let to_take = max_pending_writes - writes.len();
            let chain = chain_receiver.borrow_and_update();
            position.advance(to_take, &chain, |b| writes.push(store.save(b)))
        }
        
        select! {
            biased;
            write_result = writes.try_next(), if !writes.is_empty() => {
                let block = write_result?.expect("empty writes are never polled");
                chain_sender.drop(block.number(), block.hash());
            },
            _ = chain_receiver.changed(), if position.waits_for_new_block => {
                position.waits_for_new_block = false;
            }
        }
    }
}


struct Position {
    started: bool,
    last_number: BlockNumber,
    last_hash: String,
    waits_for_new_block: bool,
}


impl Position {
    fn new() -> Self {
        Self {
            started: false,
            last_number: 0,
            last_hash: String::new(),
            waits_for_new_block: false
        }
    }
    
    fn advance<B: Block + Clone>(&mut self, limit: usize, chain: &Chain<B>, mut cb: impl FnMut(B)) {
        todo!()
    }
}