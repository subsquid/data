use crate::chain::Chain;
use sqd_primitives::{Block, BlockNumber};


pub type ChainReceiver<B> = tokio::sync::watch::Receiver<Chain<B>>;


#[derive(Clone)]
pub struct ChainSender<B> {
    inner: tokio::sync::watch::Sender<Chain<B>>
}


impl<B: Block> ChainSender<B> {
    pub fn new() -> Self {
        todo!()
    }

    pub fn subscribe(&self) -> ChainReceiver<B> {
        self.inner.subscribe()
    }
    
    pub fn borrow(&self) -> tokio::sync::watch::Ref<'_, Chain<B>> {
        self.inner.borrow()
    }

    pub fn drop(&self, number: BlockNumber, hash: &str) {
        self.inner.send_if_modified(|chain| {
            chain.drop(number, hash)
        });
    }
    
    pub fn extend(&self, blocks: impl IntoIterator<Item = B>) {
        self.inner.send_if_modified(|chain| {
            let mut modified = false; 
            for b in blocks {
                modified = true;
                chain.push(b)
            }
            modified
        });
    }
}