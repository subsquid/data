use crate::chain::Chain;
use crate::chain_watch::ChainSender;
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
    let mut state = State::new(store.max_pending_writes());
    loop {
        state.advance(&mut chain_receiver, |b| {
            writes.push(store.save(b))
        });
        select! {
            biased;
            write_result = writes.try_next(), if !writes.is_empty() => {
                let block = write_result?.expect("empty writes are never polled");
                chain_sender.drop(block.number(), block.hash());
                state.ready(block);
            },
            _ = chain_receiver.changed(), if state.is_waiting_new_blocks() => {
                state.mark_possible_block_arrival();
            }
        }
    }
}


struct State<B> {
    pending: Vec<B>,
    max_pending: usize,
    last_block: BlockNumber,
    waits_new_block: bool
}


impl<B: Block + Clone> State<B> {
    pub fn new(max_pending: usize) -> Self {
        assert!(max_pending > 0);
        Self {
            pending: Vec::with_capacity(max_pending),
            max_pending,
            last_block: 0,
            waits_new_block: false
        }
    }
    
    pub fn advance(
        &mut self,
        chain_receiver: &mut tokio::sync::watch::Receiver<Chain<B>>,
        mut cb: impl FnMut(B)
    ) {
        if self.waits_new_block || self.pending.len() >= self.max_pending {
            return;
        }

        let chain = chain_receiver.borrow_and_update();

        let pos = self.find_position(&chain);
        let end = std::cmp::min(pos + self.max_pending - self.pending.len(), chain.len());
        for i in pos..end {
            let block = chain[i].clone();
            self.last_block = block.number();
            self.pending.push(block.clone());
            cb(block);
        }
        self.waits_new_block = end == chain.len()
    }

    fn find_position(&self, chain: &Chain<B>) -> usize {
        if chain.is_empty() {
            return 0
        }

        let mut pos = chain.bisect(self.last_block).min(chain.len() - 1);

        loop {
            if chain.is_droppable(pos) || self.is_pending(&chain[pos]) {
                return pos + 1
            }
            if pos == 0 {
                return 0
            } else {
                pos -= 1;
            }
        }
    }

    fn is_pending(&self, block: &B) -> bool {
        let ptr = block.ptr();
        self.pending.iter().any(|b| b.ptr() == ptr)
    }

    pub fn mark_possible_block_arrival(&mut self) {
        self.waits_new_block = false
    }

    pub fn is_waiting_new_blocks(&self) -> bool {
        self.waits_new_block
    }

    pub fn ready(&mut self, block: B) {
        let ptr = block.ptr();
        self.pending.retain(|b| b.ptr() != ptr)
    }
}