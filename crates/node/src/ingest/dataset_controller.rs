use crate::ingest::ingest::ingest;
use crate::ingest::ingest_generic::{IngestMessage, NewChunk};
use crate::ingest::write_controller::WriteController;
use crate::types::{DBRef, DatasetKind};
use crate::RetentionStrategy;
use anyhow::{anyhow, bail, Context};
use futures::future::BoxFuture;
use futures::stream::FuturesUnordered;
use futures::{FutureExt, StreamExt};
use parking_lot::Mutex;
use scopeguard::ScopeGuard;
use sqd_data_client::reqwest::ReqwestDataClient;
use sqd_polars::prelude::len;
use sqd_primitives::{BlockNumber, BlockRef};
use sqd_storage::db::{Chunk, CompactionStatus, Database, DatasetId};
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
use tracing::{error, info, instrument, warn};


pub struct DatasetController {
    dataset_id: DatasetId,
    dataset_kind: DatasetKind,
    retention_sender: tokio::sync::watch::Sender<RetentionStrategy>,
    head_receiver: tokio::sync::watch::Receiver<Option<BlockRef>>,
    finalized_head_receiver: tokio::sync::watch::Receiver<Option<BlockRef>>,
    compaction_enabled_sender: tokio::sync::watch::Sender<bool>,
    task: JoinHandle<()>,
    compaction_task: JoinHandle<()>
}


impl Drop for DatasetController {
    fn drop(&mut self) {
        self.task.abort();
        self.compaction_task.abort();
    }
}


impl DatasetController {
    pub fn new(
        db: DBRef,
        dataset_id: DatasetId,
        dataset_kind: DatasetKind,
        retention: RetentionStrategy,
        data_sources: Vec<ReqwestDataClient>
    ) -> anyhow::Result<Self>
    {
        let mut write = WriteController::new(db.clone(), dataset_id, dataset_kind)?;

        if let RetentionStrategy::FromBlock { number, parent_hash } = &retention {
            write.init_retention(*number, parent_hash.clone())?;
        }

        let (retention_sender, retention_recv) = tokio::sync::watch::channel(retention);
        let (head_sender, head_receiver) = tokio::sync::watch::channel(None);
        let (finalized_head_sender, finalized_head_receiver) = tokio::sync::watch::channel(None);
        let (compaction_enabled_sender, compaction_enabled_receiver) = tokio::sync::watch::channel(false);

        head_sender.send(write.head().cloned());
        finalized_head_sender.send(write.finalized_head().cloned());

        let ctl = Ctl {
            db: db.clone(),
            dataset_id,
            dataset_kind,
            data_sources,
            retention_recv,
            head_sender,
            finalized_head_sender
        };

        let task = tokio::spawn(ctl.run(write));

        let compaction_task = tokio::spawn(compaction_loop(
            db,
            dataset_id,
            compaction_enabled_receiver
        ));

        Ok(Self {
            dataset_id,
            dataset_kind,
            retention_sender,
            head_receiver,
            finalized_head_receiver,
            compaction_enabled_sender,
            task,
            compaction_task
        })
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

    pub fn enable_compaction(&self, yes: bool) {
        let _ = self.compaction_enabled_sender.send(yes);
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


struct WriteCtx {
    db: DBRef,
    write: WriteController,
    head_sender: tokio::sync::watch::Sender<Option<BlockRef>>,
    finalized_head_sender: tokio::sync::watch::Sender<Option<BlockRef>>,
}


impl WriteCtx {
    fn handle_ingest_msg(&mut self, msg: IngestMessage, head: Option<u64>) -> anyhow::Result<()> {
        match msg {
            IngestMessage::FinalizedHead(head) => {
                self.write.finalize(&head)?;
                self.notify_finalized_head();
            },
            IngestMessage::NewChunk(new_chunk) => {
                info!(
                    dataset_id = %self.write.dataset_id(),
                    "received new chunk {}",
                    new_chunk
                );
                let ctx = format!("failed to write new chunk {}", new_chunk);
                self.write_new_chunk(new_chunk).context(ctx)?;
                self.notify_head();
                self.notify_finalized_head();
                if let Some(n) = head {
                    if self.write.first_chunk_head().map_or(false, |h| self.write.next_block() - h.number >= n) {
                        self.retain(self.write.next_block() - n, None)?;
                    }
                }
            },
            IngestMessage::Fork {
                prev_blocks,
                rollback_sender
            } => {
                self.write.compute_rollback(&prev_blocks).map(|rollback| {
                    let _ = rollback_sender.send(rollback);
                })?;
            }
        }
        Ok(())
    }

    fn write_new_chunk(&mut self, mut new_chunk: NewChunk) -> anyhow::Result<()> {
        let desc = self.write.dataset_kind().dataset_description();
        let mut tables = BTreeMap::new();

        for (name, prepared) in new_chunk.tables.iter_tables_mut() {
            let mut builder = self.db.new_table_builder(prepared.schema());

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

        self.write.new_chunk(new_chunk.finalized_head.as_ref(), &chunk)
    }

    fn retain(&mut self, from_block: BlockNumber, parent_hash: Option<String>) -> anyhow::Result<()> {
        self.write.retain(from_block, parent_hash)?;
        self.notify_finalized_head();
        self.notify_head();
        Ok(())
    }

    fn notify_head(&self) {
        send_if_new(&self.head_sender, self.write.head().cloned());
    }

    fn notify_finalized_head(&self) {
        send_if_new(&self.finalized_head_sender, self.write.finalized_head().cloned())
    }

    fn starts_at(&self, block_number: BlockNumber, parent_hash: &Option<String>) -> bool {
        self.write.start_block() == block_number &&
            self.write.start_block_parent_hash() == parent_hash.as_ref().map(String::as_str)
    }
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


enum State {
    Idle,
    Init {
        head: Option<u64>
    },
    HeadProbe {
        future: BoxFuture<'static, BlockNumber>,
        head: u64
    },
    Ingest {
        handle: IngestHandle,
        head: Option<u64>
    },
    IngestPause {
        until: Instant,
        head: Option<u64>
    }
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


struct Ctl {
    db: DBRef,
    dataset_id: DatasetId,
    dataset_kind: DatasetKind,
    data_sources: Vec<ReqwestDataClient>,
    retention_recv: tokio::sync::watch::Receiver<RetentionStrategy>,
    head_sender: tokio::sync::watch::Sender<Option<BlockRef>>,
    finalized_head_sender: tokio::sync::watch::Sender<Option<BlockRef>>
}


macro_rules! blocking_write {
    ($write:ident, $body:expr) => {{
        let res = tokio::task::spawn_blocking(move || {
            let result = $body;
            (result, $write)
        }).await.context("write panicked")?;
        $write = res.1;
        res.0
    }};
}


impl Ctl {
    async fn run(mut self, write: WriteController) {
        let mut maybe_write = Some(write);
        loop {
            match self.write_epoch(std::mem::take(&mut maybe_write)).await {
                Ok(_) => return,
                Err(err) => {
                    error!(reason =? err, "dataset update task failed, will restart it in 1 minute");
                    tokio::time::sleep(Duration::from_secs(60)).await
                }
            }
        }
    }

    async fn write_epoch(&mut self, maybe_write: Option<WriteController>) -> anyhow::Result<()> {
        let mut write = self.new_write_ctx(maybe_write).await?;

        macro_rules! blocking {
            ($body:expr) => { blocking_write!(write, $body) }
        }

        // need this variable to please the compiler
        let retention = self.retention_recv.borrow_and_update().clone();
        let mut state = match retention {
            RetentionStrategy::FromBlock { number, parent_hash } => {
                if !write.starts_at(number, &parent_hash) {
                    blocking! {
                        write.retain(number, parent_hash)
                    }?;
                }
                State::Init { head: None }
            },
            RetentionStrategy::Head(n) => State::Init { head: Some(n) },
            RetentionStrategy::None => State::Idle
        };

        loop {
            if self.data_sources.is_empty() {
                state = State::Idle
            }
            match &mut state {
                State::Init { head } => {
                    state = if let Some(n) = head {
                        State::HeadProbe {
                            future: fetch_chain_top(self.data_sources.clone()).boxed(),
                            head: *n
                        }
                    } else {
                        State::Ingest {
                            handle: self.spawn_ingest(&write),
                            head: None
                        }
                    }
                },
                State::HeadProbe { future, head } => {
                    select! {
                        biased;
                        watch_result = self.retention_recv.changed() => {
                            watch_result?;
                            write = self.handle_retention_change(&mut state, write).await?
                        },
                        top = future => {
                            let n = *head;
                            let top = write.write.head().map_or(top, |h| h.number.max(top));
                            let first_block = top.saturating_sub(n);
                            if first_block > write.write.start_block() {
                                blocking! {
                                    write.retain(first_block, None)
                                }?;
                            }
                            state = State::Ingest {
                                handle: self.spawn_ingest(&write),
                                head: Some(n)
                            }
                        }
                    }
                },
                State::Ingest { handle, head } => {
                    select! {
                        biased;
                        watch_result = self.retention_recv.changed() => {
                            watch_result?;
                            write = self.handle_retention_change(&mut state, write).await?
                        },
                        msg = handle.msg_recv.recv() => {
                            if let Some(msg) = msg {
                                let head = *head;
                                blocking! {
                                    write.handle_ingest_msg(msg, head)
                                }?;
                            } else {
                                // ingest task must have failed
                                match (&mut handle.task).await {
                                    Ok(Ok(_)) => {
                                        error!("ingest task unexpectedly terminated")
                                    },
                                    Ok(Err(err)) => {
                                        error!(reason =? err, "ingest task failed")
                                    },
                                    Err(_) => {
                                        error!("ingest task panicked or got canceled")
                                    }
                                }
                                state = State::IngestPause {
                                    until: Instant::now().add(Duration::from_secs(60)),
                                    head: *head
                                }
                            }
                        }
                    }
                },
                State::IngestPause { until, head } => {
                    select! {
                        biased;
                        watch_result = self.retention_recv.changed() => {
                            watch_result?;
                            write = self.handle_retention_change(&mut state, write).await?
                        },
                        _ = tokio::time::sleep_until(*until) => {
                            state = State::Init { head: *head }
                        }
                    }
                },
                State::Idle => {
                    self.retention_recv.changed().await?;
                    write = self.handle_retention_change(&mut state, write).await?
                }
            }
        }
    }

    async fn handle_retention_change(
        &mut self,
        state: &mut State,
        mut write: WriteCtx
    ) -> anyhow::Result<WriteCtx>
    {
        // need this variable to please the compiler
        let retention = self.retention_recv.borrow_and_update().clone();
        match retention {
            RetentionStrategy::FromBlock { number, parent_hash } => {
                let will_erase_head = write.write.head().map_or(false, |h| h.number < number);
                blocking_write!(write, write.retain(number, parent_hash))?;
                match state {
                    State::Ingest { .. } if !will_erase_head => {},
                    _ => *state = State::Init { head: None }
                }
            },
            RetentionStrategy::Head(n) => {
                match state {
                    State::HeadProbe { head, .. } => {
                        *head = n
                    },
                    State::Ingest { head, .. } if head.is_some() => {
                        *head = Some(n)
                    },
                    _ => *state = State::Init { head: Some(n) }
                }
            },
            RetentionStrategy::None => *state = State::Idle
        }
        Ok(write)
    }

    fn spawn_ingest(&self, write: &WriteCtx) -> IngestHandle {
        let (msg_sender, msg_recv) = tokio::sync::mpsc::channel(1);

        let task = tokio::spawn(ingest(
            msg_sender,
            self.data_sources.clone(),
            self.dataset_kind,
            write.write.next_block(),
            write.write.head_hash()
        ));

        IngestHandle {
            msg_recv,
            task
        }
    }

    async fn new_write_ctx(&self, maybe_write: Option<WriteController>) -> anyhow::Result<WriteCtx> {
        let db = self.db.clone();
        let dataset_id = self.dataset_id;
        let dataset_kind = self.dataset_kind;

        let write = if let Some(write) = maybe_write {
            write
        } else {
            tokio::task::spawn_blocking(move || {
                WriteController::new(db, dataset_id, dataset_kind)
            }).await.context("write init task panicked")??
        };

        Ok(WriteCtx {
            db: self.db.clone(),
            write,
            head_sender: self.head_sender.clone(),
            finalized_head_sender: self.finalized_head_sender.clone()
        })
    }
}


async fn fetch_chain_top(clients: Vec<ReqwestDataClient>) -> BlockNumber {
    let mut calls: FuturesUnordered<_> = (0..clients.len())
        .map(|i| call_client(&clients, i, false))
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
                            call_client(&clients, ci, true)
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


#[instrument(name = "compaction", skip(db, enabled))]
async fn compaction_loop(
    db: DBRef,
    dataset_id: DatasetId,
    mut enabled: tokio::sync::watch::Receiver<bool>
) {
    let mut skips = 0;
    let skip_pause = [1, 2, 5, 10, 20];
    loop {
        if enabled.borrow_and_update().clone() {
            let db = db.clone();
            let span = tracing::Span::current();
            let result = match tokio::task::spawn_blocking(move || {
                let _s = span.enter();
                info!("compaction started");
                db.perform_dataset_compaction(dataset_id, None, None)
            }).await {
                Ok(res) => res,
                Err(err) => Err(anyhow!("failed to await compaction task - {}", err))
            };

            match result {
                Ok(CompactionStatus::Ok(merged_chunks)) => {
                    let first_block = merged_chunks[0].first_block;
                    let last_block = merged_chunks.last().unwrap().last_block;
                    info!(
                        chunks =? merged_chunks,
                        "merged {} chunks with block range {}-{}", 
                        merged_chunks.len(),
                        first_block,
                        last_block
                    );
                    skips = 0;
                },
                Ok(CompactionStatus::NotingToCompact) => {
                    info!("nothing to compact");
                    let pause = skip_pause[std::cmp::max(skips, skip_pause.len() - 1)];
                    tokio::time::sleep(Duration::from_secs(pause)).await;
                },
                Ok(CompactionStatus::Canceled) => {
                    skips = 0;
                    info!("data changed during compaction");
                },
                Err(err) => {
                    skips = 0;
                    error!(
                        reason =? err,
                        "compaction failed, will try again in 5 minutes"
                    );
                    tokio::time::sleep(Duration::from_secs(5 * 60)).await;
                }
            }
        } else {
            if enabled.changed().await.is_err() {
                return
            }
        }
    }
}