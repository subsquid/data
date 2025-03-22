use crate::ingest::ingest::{ingest, DataSource};
use crate::ingest::ingest_generic::{IngestMessage, NewChunk};
use crate::ingest::write_controller::WriteController;
use crate::types::{DBRef, DatasetKind};
use crate::RetentionStrategy;
use anyhow::{bail, Context};
use futures::stream::FuturesUnordered;
use futures::{FutureExt, StreamExt};
use parking_lot::Mutex;
use scopeguard::ScopeGuard;
use sqd_data_client::reqwest::ReqwestDataClient;
use sqd_polars::prelude::len;
use sqd_primitives::{BlockNumber, BlockRef};
use sqd_storage::db::{Chunk, Database, DatasetId};
use std::collections::BTreeMap;
use std::fmt::{Display, Formatter};
use std::future::pending;
use std::ops::{Add, DerefMut};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::select;
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tracing::{error, info, warn};


enum State {
    Init,
    InitHead(u64),
    Ingest {
        handle: IngestHandle,
        head: Option<u64>
    },
    Idle
}


struct IngestHandle {
    msg_recv: tokio::sync::mpsc::Receiver<IngestMessage>,
    task: JoinHandle<anyhow::Result<()>>
}


impl Drop for IngestHandle {
    fn drop(&mut self) {
        self.task.abort()
    }
}


pub struct DatasetController {
    db: DBRef,
    dataset_id: DatasetId,
    dataset_kind: DatasetKind,
    data_sources: Vec<ReqwestDataClient>,
    retention_sender: tokio::sync::watch::Sender<RetentionStrategy>,
    head_sender: tokio::sync::watch::Sender<Option<BlockRef>>,
    head_receiver: tokio::sync::watch::Receiver<Option<BlockRef>>,
    finalized_head_sender: tokio::sync::watch::Sender<Option<BlockRef>>,
    finalized_head_receiver: tokio::sync::watch::Receiver<Option<BlockRef>>,
    write: Mutex<WriteController>
}


impl DatasetController {
    pub fn new(
        db: DBRef,
        dataset_id: DatasetId,
        dataset_kind: DatasetKind,
        retention: RetentionStrategy,
        data_sources: Vec<ReqwestDataClient>
    ) -> anyhow::Result<Arc<Self>>
    {
        let mut write = WriteController::new(db.clone(), dataset_id, dataset_kind)?;

        let state = match &retention {
            RetentionStrategy::FromBlock { number, parent_hash } => {
                write.init_retention(*number, parent_hash.clone())?;
                State::Init
            },
            RetentionStrategy::Head(n) => State::InitHead(*n),
            RetentionStrategy::None => State::Idle
        };

        let (retention_sender, retention_receiver) = tokio::sync::watch::channel(retention);
        let (head_sender, head_receiver) = tokio::sync::watch::channel(None);
        let (finalized_head_sender, finalized_head_receiver) = tokio::sync::watch::channel(None);

        head_sender.send(write.head().cloned());
        finalized_head_sender.send(write.finalized_head().cloned());

        let ctl = Arc::new(Self {
            db,
            dataset_id,
            dataset_kind,
            data_sources,
            retention_sender,
            head_sender,
            head_receiver,
            finalized_head_sender,
            finalized_head_receiver,
            write: Mutex::new(write)
        });

        Ok(ctl)
    }

    async fn write_epoch(
        self: &Arc<Self>,
        mut state: State,
        retention_recv: &mut tokio::sync::watch::Receiver<RetentionStrategy>
    ) -> anyhow::Result<()> {
        loop {
            if self.data_sources.is_empty() {
                state = State::Idle
            }
            match &mut state {
                State::Init => {
                    let write = self.write.lock();
                    state = State::Ingest {
                        handle: self.spawn_ingest(&write),
                        head: None
                    }
                },
                State::InitHead(n) => {
                    let n = *n;
                    let top = fetch_chain_top(&self.data_sources).await;
                    state = self.blocking_write(move |this| {
                        let mut write = this.write.lock();
                        let top = write.head().map_or(top, |h| h.number.max(top));
                        let first_block = top.saturating_sub(n);
                        if first_block > write.start_block() {
                            this.do_retention(&mut write, first_block, None)?;
                        }
                        Ok(State::Ingest {
                            handle: this.spawn_ingest(&write),
                            head: Some(n)
                        })
                    }).await?;
                },
                State::Ingest { handle, head } => {
                    select! {
                        msg = handle.msg_recv.recv() => {
                            if let Some(msg) = msg {
                                self.blocking_write(|this| {
                                    let mut write = this.write.lock();
                                    this.handle_ingest_msg(&mut write, msg)
                                }).await?;
                            } else {
                                
                            }
                        },
                        watch_result = retention_recv.changed() => {
                            assert!(watch_result.is_ok());

                            let strategy = retention_recv.borrow_and_update().clone();

                            match strategy {
                                RetentionStrategy::FromBlock {
                                    number,
                                    parent_hash
                                } => {
                                    let restart_ingest = self.blocking_write(move |this| {
                                        let mut write = this.write.lock();
                                        let prev_head = write.head().cloned();
                                        this.do_retention(&mut write, number, parent_hash)?;
                                        Ok(prev_head.as_ref() != write.head())
                                    }).await?;

                                    if restart_ingest {
                                        state = State::Init
                                    }
                                },
                                RetentionStrategy::Head(n) => {
                                    if head != &Some(n) {
                                        state = State::InitHead(n)
                                    }
                                },
                                RetentionStrategy::None => {
                                    state = State::Idle
                                }
                            }
                        }
                    }
                }
                State::Idle => {
                    assert!(retention_recv.changed().await.is_ok());

                    let strategy = retention_recv.borrow_and_update().clone();

                    match strategy {
                        RetentionStrategy::FromBlock {
                            number,
                            parent_hash
                        } => {
                            self.blocking_write(move |this| {
                                let mut write = this.write.lock();
                                this.do_retention(&mut write, number, parent_hash)
                            }).await?;
                            state = State::Init
                        },
                        RetentionStrategy::Head(n) => {
                            state = State::InitHead(n)
                        },
                        RetentionStrategy::None => {
                            state = State::Idle
                        }
                    }
                }
            }
        }
    }

    fn spawn_ingest(&self, write: &WriteController) -> IngestHandle {
        let (msg_sender, msg_recv) = tokio::sync::mpsc::channel(1);

        let task = tokio::spawn(ingest(
            msg_sender,
            self.data_sources.clone(),
            self.dataset_kind,
            write.next_block(),
            write.head_hash()
        ));

        IngestHandle {
            msg_recv,
            task
        }
    }

    fn do_retention(
        &self,
        write: &mut WriteController,
        from_block: BlockNumber,
        parent_hash: Option<String>
    ) -> anyhow::Result<()>
    {
        write.retain(from_block, parent_hash)?;
        self.notify_finalized_head(write);
        self.notify_head(write);
        Ok(())
    }

    fn handle_ingest_msg(
        &self,
        write: &mut WriteController,
        msg: IngestMessage
    ) -> anyhow::Result<()>
    {
        match msg {
            IngestMessage::FinalizedHead(head) => {
                write.finalize(&head)?;
                self.notify_finalized_head(write);
            },
            IngestMessage::NewChunk(new_chunk) => {
                info!(
                    dataset_id = %self.dataset_id,
                    "received new chunk {}",
                    new_chunk
                );
                let ctx = format!("failed to write new chunk {}", new_chunk);
                write_new_chunk(&self.db, write, new_chunk).context(ctx)?;
                self.notify_head(write);
                self.notify_finalized_head(write);
            },
            IngestMessage::Fork {
                prev_blocks,
                rollback_sender
            } => {
                write.compute_rollback(&prev_blocks).map(|rollback| {
                    let _ = rollback_sender.send(rollback);
                })?;
            }
        }
        Ok(())
    }

    fn notify_head(&self, write: &WriteController) {
        send_if_new(&self.head_sender, write.head().cloned());
    }

    fn notify_finalized_head(&self, write: &WriteController) {
        send_if_new(&self.finalized_head_sender, write.finalized_head().cloned())
    }

    async fn blocking_write<R, F>(self: &Arc<Self>, f: F) -> anyhow::Result<R>
    where
        F: FnOnce(Arc<Self>) -> anyhow::Result<R> + Send + 'static,
        R: Send + 'static
    {
        let this = self.clone();
        tokio::task::spawn_blocking(move || f(this))
            .await
            .context("write task panicked")?
    }

    pub fn dataset_id(&self) -> DatasetId {
        self.dataset_id
    }

    pub fn dataset_kind(&self) -> DatasetKind {
        self.dataset_kind
    }

    pub fn get_finalized_head(&self) -> Option<BlockRef> {
        self.finalized_head_receiver.borrow().clone()
    }

    pub fn get_head(&self) -> Option<BlockRef> {
        self.head_receiver.borrow().clone()
    }

    pub fn get_head_block_number(&self) -> Option<BlockNumber> {
        self.head_receiver.borrow().as_ref().map(|h| h.number)
    }

    pub fn retain(&self, strategy: RetentionStrategy) {
        self.retention_sender.send(strategy).unwrap()
    }

    pub async fn wait_for_block(&self, block_number: BlockNumber) -> BlockNumber {
        let mut recv = self.head_receiver.clone();
        loop {
            if let Some(block) = recv.borrow_and_update().as_ref() {
                if block.number >= block_number {
                    return block.number
                }
            }
            recv.changed().await.unwrap()
        }
    }
}


fn write_new_chunk(
    db: &Database,
    write: &mut WriteController,
    mut new_chunk: NewChunk
) -> anyhow::Result<()> 
{
    let desc = write.dataset_kind().dataset_description();
    let mut tables = BTreeMap::new();

    for (name, prepared) in new_chunk.tables.iter_tables_mut() {
        let mut builder = db.new_table_builder(prepared.schema());

        if let Some(table_desc) = desc.tables.get(name) {
            for (&col, opts) in table_desc.options.column_options.iter() {
                if opts.stats_enable {
                    builder.add_stat_by_name(col)?;
                }
            }
        }

        prepared.read(&mut builder, 0, prepared.num_rows())?;

        tables.insert(
            name.to_string(),
            builder.finish()?
        );
    }

    let chunk = Chunk::V0 {
        parent_block_hash: new_chunk.parent_block_hash,
        first_block: new_chunk.first_block,
        last_block: new_chunk.last_block,
        last_block_hash: new_chunk.last_block_hash,
        tables
    };

    write.new_chunk(new_chunk.finalized_head.as_ref(), &chunk)
}


fn send_if_new<T: Eq>(sender: &tokio::sync::watch::Sender<T>, value: T) {
    sender.send_if_modified(|current| {
        if current == &value {
            false
        } else {
            *current = value;
            true
        }
    });
}


async fn fetch_chain_top(clients: &[ReqwestDataClient]) -> BlockNumber {
    let mut calls: FuturesUnordered<_> = (0..clients.len())
        .map(|i| call_client(clients, i, false))
        .collect();

    let mut completed = 0;
    let mut last_seen = 0;
    let mut deadline = Instant::now();

    loop {
        select! {
                biased;
                result = calls.next() => {
                    match result {
                        None => return last_seen,
                        Some((Ok((bn)), _)) => {
                            last_seen = last_seen.max(bn);
                            completed += 1;
                            if completed > clients.len() / 2 {
                                return last_seen
                            }
                            if completed == 1 {
                                deadline = Instant::now().add(Duration::from_secs(5))
                            }
                        },
                        Some((Err(err), ci)) => {
                            if clients[ci].is_retryable(&err) {
                                warn!(
                                    reason =? err,
                                    data_source =% clients[ci].url(),
                                    "head probe failed"
                                )
                            } else {
                                error!(
                                    reason =? err,
                                    data_source =% clients[ci].url(),
                                    "head probe failed"
                                )
                            };
                            calls.push(
                                call_client(clients, ci, true)
                            );
                        }
                    }
                }
                _ = tokio::time::sleep_until(deadline), if completed > 0 => {
                    return last_seen
                }
            }
    }

    async fn call_client(
        clients: &[ReqwestDataClient],
        idx: usize,
        backoff: bool
    ) -> (anyhow::Result<BlockNumber>, usize)
    {
        if backoff {
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
        clients[idx].get_head().map(move |res| {
            let res = res.map(|maybe_head| maybe_head.map_or(0, |h| h.number));
            (res, idx)
        }).await
    }
}