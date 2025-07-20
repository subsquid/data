use crate::chain_watch::ChainReceiver;
use crate::ingest::store::Store;
use anyhow::anyhow;
use sqd_data_source::DataSource;
use sqd_primitives::BlockNumber;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::task::JoinHandle;


pub struct Ingest<S, D> {
    store: S,
    data_source: D,
    first_block: BlockNumber,
    parent_block_hash: Option<String>
}


impl<S, D> Ingest<S, D> 
where
    S: Store,
    D: DataSource<Block = S::Block>
{
    pub fn new(store: S, data_source: D) -> Self {
        Self {
            store,
            data_source,
            first_block: 0,
            parent_block_hash: None
        }
    }
    
    pub async fn start(mut self) -> anyhow::Result<(ChainReceiver<S::Block>, IngestHandle)> {
        todo!()
    }
}


pub struct IngestHandle {
    write: JoinHandle<anyhow::Result<()>>,
    ingest: JoinHandle<anyhow::Result<()>>,
    terminated: bool
}


impl Future for IngestHandle {
    type Output = anyhow::Result<()>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.terminated {
            return Poll::Ready(Err(anyhow!("ingest was already terminated")))
        }
        todo!()
        // Poll::Pending
    }
}


