use crate::chain::Chain;
use sqd_primitives::{Block, BlockNumber};


#[derive(Clone)]
pub struct ChainSender<B> {
    inner: tokio::sync::watch::Sender<Chain<B>>
}


impl<B: Block> ChainSender<B> {
    pub fn new() -> Self {
        todo!()
    }
    
    pub fn drop(&self, number: BlockNumber, hash: &str) {
        self.inner.send_if_modified(|chain| {
            chain.drop(number, hash)
        });
    }
    
    pub fn subscribe(&self) -> tokio::sync::watch::Receiver<Chain<B>> {
        todo!()
    }
}