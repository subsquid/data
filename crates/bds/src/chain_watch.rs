use crate::chain::Chain;
use sqd_primitives::{Block, BlockNumber};


pub type ChainReceiver<B> = tokio::sync::watch::Receiver<Chain<B>>;


#[derive(Clone)]
pub struct ChainSender<B> {
    inner: tokio::sync::watch::Sender<Chain<B>>,
    max_size: usize
}


impl<B: Block> ChainSender<B> {
    pub fn new(min_size: usize, max_size: usize) -> Self {
        Self {
            inner: tokio::sync::watch::Sender::new(Chain::new(min_size)),
            max_size
        }
    }

    pub fn subscribe(&self) -> ChainReceiver<B> {
        self.inner.subscribe()
    }
    
    pub fn borrow(&self) -> tokio::sync::watch::Ref<'_, Chain<B>> {
        self.inner.borrow()
    }

    pub fn drop(&self, number: BlockNumber, hash: &str) {
        self.inner.send_if_modified(|chain| {
            let over = chain.len() > self.max_size;
            let dropped = chain.drop(number, hash);
            // notify about change only when crossed max_size threshold
            dropped && over && chain.len() < self.max_size
        });
    }
    
    pub async fn extend_and_wait(&self, blocks: impl IntoIterator<Item = B>) {
        if !self.extend(blocks) {
            self.wait().await;
        }
    }
    
    pub fn extend(&self, blocks: impl IntoIterator<Item = B>) -> bool {
        let mut size = 0;
        self.inner.send_if_modified(|chain| {
            let mut modified = false; 
            for b in blocks {
                modified = true;
                chain.push(b)
            }
            size = chain.len();
            modified
        });
        size <= self.max_size
    }
    
    pub async fn wait(&self) {
        let mut recv = self.subscribe();
        while recv.borrow_and_update().len() >= self.max_size {
            recv.changed().await.expect("sender cannot be dropped");
        }
    }
}