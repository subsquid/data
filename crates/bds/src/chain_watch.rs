use crate::chain::{Chain, HeadChain};
use sqd_primitives::{Block, BlockNumber, BlockPtr};


pub type ChainReceiver<B> = tokio::sync::watch::Receiver<Chain<B>>;


#[derive(Clone)]
pub struct ChainSender<B> {
    inner: tokio::sync::watch::Sender<Chain<B>>,
    max_size: usize
}


impl<B: Block> ChainSender<B> {
    pub fn new(head_chain: HeadChain, min_size: usize, max_size: usize) -> Self {
        Self {
            inner: tokio::sync::watch::Sender::new(Chain::new(head_chain, min_size)),
            max_size
        }
    }

    pub fn subscribe(&self) -> ChainReceiver<B> {
        self.inner.subscribe()
    }

    pub fn borrow(&self) -> tokio::sync::watch::Ref<'_, Chain<B>> {
        self.inner.borrow()
    }

    pub fn mark_stored(&self, number: BlockNumber, hash: &str) {
        self.inner.send_if_modified(|chain| {
            chain.mark_stored(number, hash) && chain.clean()
        });
    }

    pub fn finalize(&self, head: BlockPtr) -> anyhow::Result<()> {
        let mut res = Ok(false);
        self.inner.send_if_modified(|chain| { 
            res = chain.finalize(head);
            res.as_ref().map_or(false, |changed| *changed)
        });
        res.map(drop)
    }

    pub fn push(&self, is_final: bool, block: B) -> anyhow::Result<bool> {
        let mut res = Ok(());
        let mut size = 0;
        self.inner.send_modify(|chain| {
            res = chain.push(block);
            size = chain.len();
            if is_final && res.is_ok() {
                chain.finalize_all();
            }
        });
        res.map(|_| size <= self.max_size)
    }

    pub async fn wait(&self) {
        let mut recv = self.subscribe();
        while recv.borrow_and_update().len() >= self.max_size {
            recv.changed().await.expect("sender cannot be dropped");
        }
    }
}