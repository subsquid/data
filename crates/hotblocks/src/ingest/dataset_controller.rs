use crate::ingest::ingest::{ingest, DataSource};
use crate::ingest::ingest_generic::{IngestMessage, NewChunk};
use crate::ingest::write_controller::WriteController;
use crate::types::{DBRef, DatasetKind};
use anyhow::{bail, Context};
use sqd_primitives::{BlockNumber, BlockRef};
use sqd_storage::db::{Chunk, Database, DatasetId};
use std::collections::BTreeMap;
use std::fmt::{Display, Formatter};
use std::future::pending;
use std::ops::DerefMut;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::select;
use tracing::info;


pub struct DatasetController {
    db: DBRef,
    dataset_id: DatasetId,
    dataset_kind: DatasetKind,
    data_sources: Vec<DataSource>,
    first_block_sender: tokio::sync::watch::Sender<BlockNumber>,
    first_block_receiver: tokio::sync::watch::Receiver<BlockNumber>,
    head_sender: tokio::sync::watch::Sender<Option<BlockRef>>,
    head_receiver: tokio::sync::watch::Receiver<Option<BlockRef>>,
    finalized_head_sender: tokio::sync::watch::Sender<Option<BlockRef>>,
    finalized_head_receiver: tokio::sync::watch::Receiver<Option<BlockRef>>,
    is_running: AtomicBool
}


impl DatasetController {
    pub fn new(
        db: DBRef,
        dataset_kind: DatasetKind,
        dataset_id: DatasetId,
        first_block: BlockNumber,
        data_sources: Vec<DataSource>
    ) -> Self
    {
        let (first_block_sender, first_block_receiver) = tokio::sync::watch::channel(first_block);
        let (head_sender, head_receiver) = tokio::sync::watch::channel(None);
        let (finalized_head_sender, finalized_head_receiver) = tokio::sync::watch::channel(None);
        Self {
            db,
            dataset_id,
            dataset_kind,
            data_sources,
            first_block_sender,
            first_block_receiver,
            head_sender,
            head_receiver,
            finalized_head_sender,
            finalized_head_receiver,
            is_running: AtomicBool::new(false)
        }
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

    pub fn retain(&self, block_number: BlockNumber) {
        self.first_block_sender.send(block_number).unwrap()
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

    pub async fn run(&self) -> anyhow::Result<()> {
        if self.is_running.compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst).is_err() {
            bail!("this dataset is already controlled by another task")
        }

        scopeguard::defer! {
            self.is_running.store(false, Ordering::SeqCst)
        };

        loop {
            self.run_epoch().await?
        }
    }

    async fn run_epoch(&self) -> anyhow::Result<()> {
        let db = self.db.clone();
        let dataset_id = self.dataset_id;
        let dataset_kind = self.dataset_kind;
        let first_block = self.first_block_receiver.borrow().clone();

        let write = tokio::task::spawn_blocking(move || {
            WriteController::new(
                db,
                dataset_kind,
                dataset_id,
                first_block
            )
        }).await.context("failed to await on write init")??;
        
        self.head_sender.send(write.head().cloned());
        self.finalized_head_sender.send(write.finalized_head().cloned());
        
        if self.data_sources.is_empty() {
            return Ok(pending().await)
        }

        let (ingest_msg_sender, ingest_msg_receiver) = tokio::sync::mpsc::channel(1);

        let ingest_handle = tokio::spawn(ingest(
            ingest_msg_sender,
            self.data_sources.clone(),
            dataset_kind,
            write.next_block(),
            write.head_hash()
        ));

        let mut ingest_handle = scopeguard::guard(ingest_handle, |handle| handle.abort());

        // No need to guard the write handle.
        // Without ingest it will end on its own.
        let write_handle = tokio::spawn(write_loop(
            self.db.clone(),
            write,
            ingest_msg_receiver,
            self.first_block_receiver.clone(),
            self.head_sender.clone(),
            self.finalized_head_sender.clone()
        ));

        match write_handle.await.context("failed to await on write task")? {
            Ok(_) => {
                // Write loop exited without errors.
                // The problem must be on ingest side
                match ingest_handle.deref_mut().await.context("failed to await on ingest task")? {
                    Ok(_) => bail!("ingest task finished unexpectedly and without errors"),
                    Err(err) => Err(err)
                }
            },
            Err(err) => {
                match err.downcast_ref::<BehindFirstBlock>() {
                    Some(_) => Ok(()),
                    None => Err(err)
                }
            }
        }
    }
}


async fn write_loop(
    db: DBRef,
    mut write: WriteController,
    mut ingest_message_recv: tokio::sync::mpsc::Receiver<IngestMessage>,
    mut retain_recv: tokio::sync::watch::Receiver<BlockNumber>,
    mut head_sender: tokio::sync::watch::Sender<Option<BlockRef>>,
    mut finalized_head_sender: tokio::sync::watch::Sender<Option<BlockRef>>
) -> anyhow::Result<()>
{
    macro_rules! blocking {
        ($body:expr) => {{
            let res = tokio::task::spawn_blocking(move || {
                let result = $body;
                (result, write)
            }).await.unwrap();
            write = res.1;
            res.0
        }};
    }
    macro_rules! notify_finalized_head {
        () => {
            send_if_new(&mut finalized_head_sender, write.finalized_head().cloned())
        };
    }
    macro_rules! notify_head {
        () => {
            send_if_new(&mut head_sender, write.head().cloned())
        };
    }

    loop {
        select! {
            biased;
            msg = ingest_message_recv.recv() => {
                let msg = match msg {
                    Some(msg) => msg,
                    None => return Ok(())
                };
                match msg {
                    IngestMessage::FinalizedHead(head) => {
                        blocking! {
                            write.finalize(&head)
                        }?;
                        notify_finalized_head!();
                    },
                    IngestMessage::NewChunk(new_chunk) => {
                        info!(
                            dataset_id = %write.dataset_id(), 
                            "received new chunk {}", new_chunk
                        );
                        let db = db.clone();
                        blocking! {{
                            let ctx = format!("failed to write new chunk {}", new_chunk);
                            write_new_chunk(&db, &mut write, new_chunk).context(ctx)
                        }}?;
                        notify_head!();
                        notify_finalized_head!();
                    },
                    IngestMessage::Fork {
                        prev_blocks,
                        rollback_sender
                    } => {
                        blocking! {
                            write.compute_rollback(&prev_blocks).map(|rollback| {
                                let _ = rollback_sender.send(rollback);
                            })
                        }?;
                    },
                }
            },
            watch_result = retain_recv.changed() => {
                if watch_result.is_err() {
                    return Ok(())
                }

                let block_number = retain_recv.borrow_and_update().clone();

                if write.next_block() < block_number {
                    bail!(BehindFirstBlock(block_number))
                }

                blocking! {
                    write.retain_head(block_number).with_context(|| {
                        format!("failed to retain dataset from block {}", block_number)
                    })
                }?;

                notify_head!();
                notify_finalized_head!();
            }
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


#[derive(Debug)]
struct BehindFirstBlock(BlockNumber);


impl Display for BehindFirstBlock {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "data ingestion process felt behind the desired first block {}", self.0)
    }
}


impl std::error::Error for BehindFirstBlock {}


fn send_if_new<T: Eq>(sender: &mut tokio::sync::watch::Sender<T>, mut value: T) {
    sender.send_if_modified(|current| {
        if current == &value {
            false
        } else {
            std::mem::swap(current, &mut value);
            true
        }
    });
}