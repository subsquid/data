use super::grow::grow;
use super::store::Store;
use super::write::write_chain;
use crate::chain_watch::{ChainReceiver, ChainSender};
use crate::util::task_termination_error;
use anyhow::{anyhow, ensure, Context};
use futures::FutureExt;
use sqd_data_source::DataSource;
use sqd_primitives::BlockNumber;
use std::future::Future;
use std::pin::Pin;
use std::task::Poll;
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

        let finalize = tokio::spawn(
            finalize_loop(self.store.clone(), chain_receiver.clone(), self.first_block)
        );

        let ingest = tokio::spawn(
            grow(self.data_source, self.first_block, self.parent_block_hash, chain_sender)
        );

        let handle = IngestHandle {
            write,
            head_update,
            finalize,
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


async fn finalize_loop<S: Store>(
    store: S,
    mut chain_receiver: ChainReceiver<S::Block>,
    first_block: BlockNumber
) -> anyhow::Result<()>
{
    let mut prev = chain_receiver
        .borrow()
        .stored_finalized_head()
        .map(|b| b.number);

    loop {
        chain_receiver.changed().await?;

        let (from, to) = {
            let chain = chain_receiver.borrow_and_update();
            let Some(head) = chain.stored_finalized_head() else {
                continue
            };
            if prev.map_or(false, |n| n == head.number) {
                continue
            }
            (
                prev.unwrap_or(first_block),
                head.to_ref()
            )
        };

        ensure!(from <= to.number);

        store.finalize(from, to.ptr()).await.with_context(|| {
            format!("failed to finalize blocks from {} to {}", from, to.ptr())
        })?;
        
        debug!(
            number = to.number,
            hash = to.hash,
            "finalized"
        );

        prev = Some(to.number);
    }
}


pub struct IngestHandle {
    write: JoinHandle<anyhow::Result<()>>,
    head_update: JoinHandle<anyhow::Result<()>>,
    finalize: JoinHandle<anyhow::Result<()>>,
    ingest: JoinHandle<anyhow::Result<()>>,
    terminated: bool
}


impl IngestHandle {
    pub fn abort(&mut self) {
        self.write.abort();
        self.head_update.abort();
        self.finalize.abort();
        self.ingest.abort();
        self.terminated = true;
    }
}


impl Future for IngestHandle {
    type Output = anyhow::Result<()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        if self.terminated {
            return Poll::Ready(Err(anyhow!("ingest was already terminated")))
        }

        macro_rules! poll_task {
            ($name:expr, $handle:ident) => {
                if let Poll::Ready(res) = self.$handle.poll_unpin(cx) {
                    self.abort();
                    return Poll::Ready(
                        task_termination_error($name, res)
                    )
                }
            };
        }

        poll_task!("write", write);
        poll_task!("head update", head_update);
        poll_task!("block finalization", finalize);
        poll_task!("ingest", ingest);
        
        Poll::Pending
    }
}


impl Drop for IngestHandle {
    fn drop(&mut self) {
        self.abort()
    }
}

