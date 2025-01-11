use crate::ingest::ingest::ingest;
use crate::ingest::ingest_generic::{IngestMessage, NewChunk};
use crate::ingest::write_controller::WriteController;
use crate::types::{DBRef, DatasetKind};
use anyhow::{bail, Context};
use reqwest::Url;
use sqd_primitives::BlockNumber;
use sqd_storage::db::{Chunk, Database, DatasetId};
use std::collections::BTreeMap;
use std::fmt::{Display, Formatter};
use std::ops::DerefMut;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::select;


pub struct DatasetController {
    db: DBRef,
    dataset_id: DatasetId,
    dataset_kind: DatasetKind,
    dataset_url: Url,
    first_block_sender: tokio::sync::watch::Sender<BlockNumber>,
    first_block_receiver: tokio::sync::watch::Receiver<BlockNumber>,
    is_running: AtomicBool
}


impl DatasetController {
    pub fn new(
        db: DBRef,
        dataset_id: DatasetId,
        dataset_kind: DatasetKind,
        dataset_url: Url,
        first_block: BlockNumber
    ) -> Self
    {
        let (first_block_sender, first_block_receiver) = tokio::sync::watch::channel(first_block);
        Self {
            db,
            dataset_id,
            dataset_kind,
            dataset_url,
            first_block_sender,
            first_block_receiver,
            is_running: AtomicBool::new(false)
        }
    }

    pub fn retain_head(&self, block_number: BlockNumber) {
        self.first_block_sender.send(block_number).unwrap()
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
                dataset_id,
                dataset_kind,
                first_block
            )
        }).await.context("failed to await on write init")??;

        let (ingest_msg_sender, ingest_msg_receiver) = tokio::sync::mpsc::channel(1);

        let ingest_handle = tokio::spawn(ingest(
            ingest_msg_sender,
            self.dataset_url.clone(),
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
            self.first_block_receiver.clone()
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
    mut retain_recv: tokio::sync::watch::Receiver<BlockNumber>
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
                    },
                    IngestMessage::NewChunk(new_chunk) => {
                        let db = db.clone();
                        blocking! {
                            write_new_chunk(&db, &mut write, new_chunk)
                        }?;
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

                let block_number = *retain_recv.borrow_and_update();

                if write.next_block() < block_number {
                    bail!(BehindFirstBlock(block_number))
                }

                blocking! {
                    write.retain_head(block_number)
                }?;
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