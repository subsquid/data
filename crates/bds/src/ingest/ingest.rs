use super::grow::grow;
use super::store::Store;
use super::write::write_chain;
use crate::chain_watch::{ChainReceiver, ChainSender};
use crate::util::task_termination_error;
use anyhow::anyhow;
use futures::FutureExt;
use sqd_data_source::DataSource;
use sqd_primitives::BlockNumber;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::task::JoinHandle;
use tracing::debug;


pub struct Ingest<S, D> {
    store: S,
    data_source: D,
    first_block: BlockNumber,
    parent_block_hash: Option<String>,
    min_queue_size: usize,
    max_queue_size: usize
}


impl<S, D> Ingest<S, D>
where
    S: Store,
    D: DataSource<Block = S::Block> + Send + 'static
{
    pub fn new(store: S, data_source: D) -> Self {
        Self {
            store,
            data_source,
            first_block: 0,
            parent_block_hash: None,
            min_queue_size: 5,
            max_queue_size: 50
        }
    }
    
    pub fn set_first_block(mut self, first_block: BlockNumber) -> Self {
        self.first_block = first_block;
        self
    }

    pub fn set_parent_block_hash(mut self, parent_hash: Option<String>) -> Self {
        self.parent_block_hash = parent_hash;
        self
    }

    pub async fn start(mut self) -> anyhow::Result<(ChainReceiver<S::Block>, IngestHandle)> {
        let head_chain = self.store.get_chain_head(
            self.first_block,
            self.parent_block_hash.as_deref()
        ).await?;

        if let Some(head) = head_chain.blocks.last() {
            self.data_source.set_position(head.number + 1, Some(&head.hash));
        } else {
            self.data_source.set_position(self.first_block, self.parent_block_hash.as_deref());
        }

        let chain_sender = ChainSender::<S::Block>::new(
            head_chain,
            self.min_queue_size,
            self.max_queue_size
        );

        let chain_receiver = chain_sender.subscribe();

        let write = tokio::spawn(
            write_chain(self.store.clone(), chain_sender.clone())
        );

        let head_update = tokio::spawn(
            head_update_loop(self.store.clone(), chain_receiver.clone())
        );

        let ingest = tokio::spawn(
            grow(self.data_source, self.first_block, self.parent_block_hash, chain_sender)
        );

        let handle = IngestHandle {
            write,
            head_update,
            ingest,
            terminated: false
        };

        Ok((chain_receiver, handle))
    }
}


async fn head_update_loop<S: Store>(
    store: S,
    mut chain_receiver: ChainReceiver<S::Block>
) -> anyhow::Result<()>
{
    let mut prev = chain_receiver
        .borrow()
        .stored_head()
        .map(|b| b.to_ref());
    loop {
        chain_receiver.changed().await?;
        {
            let chain = chain_receiver.borrow_and_update();
            let head = chain.stored_head();
            if prev.as_ref().map(|b| b.ptr()) == head {
                continue;
            }
            prev = head.map(|b| b.to_ref());
        }
        if let Some(head) = prev.as_ref() {
            store.set_head(head.ptr()).await?;
            debug!(
                number = head.number,
                hash = head.hash,
                "head updated"
            );
        }
    }
}


pub struct IngestHandle {
    write: JoinHandle<anyhow::Result<()>>,
    head_update: JoinHandle<anyhow::Result<()>>,
    ingest: JoinHandle<anyhow::Result<()>>,
    terminated: bool
}


impl IngestHandle {
    pub fn abort(&mut self) {
        self.write.abort();
        self.head_update.abort();
        self.ingest.abort();
        self.terminated = true;
    }
}


impl Future for IngestHandle {
    type Output = anyhow::Result<()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.terminated {
            return Poll::Ready(Err(anyhow!("ingest was already terminated")))
        }

        if let Poll::Ready(res) = self.write.poll_unpin(cx) {
            self.abort();
            return Poll::Ready(
                task_termination_error("write", res)
            )
        }
        
        if let Poll::Ready(res) = self.head_update.poll_unpin(cx) {
            self.abort();
            return Poll::Ready(
                task_termination_error("head update", res)
            )
        }

        if let Poll::Ready(res) = self.ingest.poll_unpin(cx) {
            self.abort();
            return Poll::Ready(
                task_termination_error("ingest", res)
            )
        }

        Poll::Pending
    }
}


impl Drop for IngestHandle {
    fn drop(&mut self) {
        self.abort()
    }
}

