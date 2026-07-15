use std::{collections::BTreeMap, time::Instant as StdInstant};

use anyhow::{Context, anyhow, bail, ensure};
use sqd_primitives::{BlockNumber, BlockRef};
use sqd_storage::db::{Chunk as StorageChunk, Chunk, DatasetId, HashIndexWriteMetrics};
use tokio::sync::watch;
use tracing::{debug, field::valuable, info, instrument, warn};

use crate::{
    dataset_controller::ingest_generic::{IngestMessage, NewChunk},
    metrics::{WriteStage, report_hash_index_write_metrics, report_write_duration},
    types::{DBRef, DatasetKind}
};

#[derive(Debug)]
pub struct Rollback {
    pub first_block: BlockNumber,
    pub parent_block_hash: Option<String>
}

/// Single writer for a dataset. Owns head/finalized-head as its working copy of
/// committed state (WP-1) and publishes them through `set_head`/
/// `set_finalized_head`, which update field and channel together only after the
/// commit — so a published watermark is always already durable (INV-31/CN-4).
#[derive(Debug)]
pub struct WriteController {
    db: DBRef,
    dataset_id: DatasetId,
    dataset_kind: DatasetKind,
    first_block: BlockNumber,
    parent_block_hash: Option<String>,
    first_chunk_head: Option<BlockRef>,
    head: Option<BlockRef>,
    finalized_head: Option<BlockRef>,
    head_sender: watch::Sender<Option<BlockRef>>,
    finalized_head_sender: watch::Sender<Option<BlockRef>>
}

impl WriteController {
    pub fn new(
        db: DBRef,
        dataset_id: DatasetId,
        dataset_kind: DatasetKind,
        head_sender: watch::Sender<Option<BlockRef>>,
        finalized_head_sender: watch::Sender<Option<BlockRef>>
    ) -> anyhow::Result<Self> {
        db.create_dataset_if_not_exists(dataset_id, dataset_kind.storage_kind())?;

        let snapshot = db.snapshot();
        let label = snapshot.get_label(dataset_id)?;
        let first_chunk = snapshot.get_first_chunk(dataset_id)?;
        let last_chunk = snapshot.get_last_chunk(dataset_id)?;

        let this = Self {
            db: db.clone(),
            dataset_id,
            dataset_kind,
            first_block: first_chunk.as_ref().map_or(0, |c| c.first_block()),
            parent_block_hash: first_chunk.as_ref().map(|c| c.last_block_hash().to_string()),
            first_chunk_head: first_chunk.as_ref().map(get_chunk_head),
            head: last_chunk.as_ref().map(get_chunk_head),
            finalized_head: label.and_then(|l| l.finalized_head().cloned()),
            head_sender,
            finalized_head_sender
        };

        // Reseed subscribers to committed state (CN-9: recovery on writer rebuild).
        this.publish_head();
        this.publish_finalized_head();

        Ok(this)
    }

    pub fn dataset_kind(&self) -> DatasetKind {
        self.dataset_kind
    }

    pub fn start_block(&self) -> BlockNumber {
        self.first_block
    }

    pub fn start_block_parent_hash(&self) -> Option<&str> {
        self.parent_block_hash.as_ref().map(String::as_str)
    }

    pub fn next_block(&self) -> BlockNumber {
        self.head.as_ref().map_or(self.first_block, |h| h.number + 1)
    }

    pub fn head_hash(&self) -> Option<&str> {
        self.head
            .as_ref()
            .map(|h| h.hash.as_str())
            .or_else(|| self.start_block_parent_hash())
    }

    pub fn head(&self) -> Option<&BlockRef> {
        self.head.as_ref()
    }

    pub fn first_chunk_head(&self) -> Option<&BlockRef> {
        self.first_chunk_head.as_ref()
    }

    /// Publish only after the commit that produced `head`, never inside the txn
    /// closure (INV-31: a published watermark must already be durable).
    fn set_head(&mut self, head: Option<BlockRef>) {
        self.head = head;
        self.publish_head();
    }

    fn set_finalized_head(&mut self, finalized_head: Option<BlockRef>) {
        self.finalized_head = finalized_head;
        self.publish_finalized_head();
    }

    fn publish_head(&self) {
        publish(&self.head_sender, self.head.clone());
    }

    fn publish_finalized_head(&self) {
        publish(&self.finalized_head_sender, self.finalized_head.clone());
    }

    pub fn compute_rollback(&self, mut prev: &[BlockRef]) -> anyhow::Result<Rollback> {
        // FIXME: self.first_block rollback limit
        ensure!(!prev.is_empty(), "no previous blocks where provided");
        ensure!(
            prev.windows(2).all(|s| s[0].number < s[1].number),
            "list of previous blocks does not have ascending order"
        );

        let snapshot = self.db.snapshot();

        let label = snapshot
            .get_label(self.dataset_id)?
            .ok_or_else(|| anyhow!("dataset {} no longer exists", self.dataset_id))?;

        if let Some(finalized_head) = label.finalized_head() {
            let pos = match prev.iter().position(|b| b.number >= finalized_head.number) {
                Some(pos) => pos,
                None => bail!("all passed prev blocks lie below finalized head")
            };
            if prev[pos].number == finalized_head.number {
                ensure!(prev[pos].hash == finalized_head.hash);
            }
            prev = &prev[pos..]
        }

        let existing_chunks = snapshot
            .list_chunks(self.dataset_id, 0, Some(prev.last().unwrap().number))
            .into_reversed();

        let mut prev_blocks = prev.iter().rev().peekable();

        for chunk_result in existing_chunks {
            let head = chunk_result?;

            if prev_blocks.peek().map_or(false, |b| b.number < head.last_block()) {
                continue;
            }

            while prev_blocks.peek().map_or(false, |b| b.number > head.last_block()) {
                prev_blocks.next();
            }

            if let Some(&b) = prev_blocks.peek() {
                if b.number == head.last_block() && b.hash == head.last_block_hash() {
                    return Ok(Rollback {
                        first_block: b.number + 1,
                        parent_block_hash: Some(b.hash.clone())
                    });
                }
            } else {
                return Ok(Rollback {
                    first_block: head.last_block() + 1,
                    parent_block_hash: Some(head.last_block_hash().to_string())
                });
            }
        }

        Ok(Rollback {
            first_block: self.first_block,
            parent_block_hash: self.parent_block_hash.clone()
        })
    }

    #[instrument(name = "retain", skip(self, delete_mismatch))]
    fn _retain(
        &mut self,
        from_block: BlockNumber,
        parent_block_hash: Option<String>,
        delete_mismatch: bool,
        metrics: &mut HashIndexWriteMetrics
    ) -> anyhow::Result<()> {
        #[derive(Eq, PartialEq)]
        enum Status {
            Range {
                first_chunk: Chunk,
                head: Chunk,
                finalized_head: Option<BlockRef>
            },
            HashMismatch,
            Gap(BlockNumber),
            Clear
        }

        let status = self
            .db
            .update_dataset_with_hash_index_metrics(self.dataset_id, metrics, |tx| {
                let mut status = Status::Clear;
                for chunk_result in tx.list_chunks(0, None) {
                    let chunk = chunk_result?;
                    if chunk.last_block() < from_block {
                        tx.delete_chunk(&chunk)?;
                    } else if from_block < chunk.first_block() {
                        if delete_mismatch {
                            tx.delete_chunk(&chunk)?;
                        } else {
                            bail!(
                                "there is a gap between first requested block {} and already existing chunk {}, \
                            that could not be filled",
                                from_block,
                                chunk
                            );
                        }
                        if status == Status::Clear {
                            status = Status::Gap(chunk.first_block());
                        }
                    } else {
                        let hash_check = if let Some(parent_block_hash) = parent_block_hash.as_ref() {
                            tx.validate_parent_block_hash(&chunk, from_block, parent_block_hash)?
                        } else {
                            Ok(())
                        };
                        if let Some(actual_hash) = hash_check.err() {
                            if delete_mismatch {
                                tx.delete_chunk(&chunk)?;
                                status = Status::HashMismatch;
                            } else {
                                bail!(
                                    "hash mismatch: expected the parent of {} to have hash {}, but got {}",
                                    from_block,
                                    parent_block_hash.as_ref().unwrap(),
                                    actual_hash
                                );
                            }
                        } else {
                            let head = tx
                                .list_chunks(0, None)
                                .into_reversed()
                                .next()
                                .expect("bottom chunk can't exist without head chunk")?;

                            let finalized_head = tx
                                .label()
                                .finalized_head()
                                .filter(|h| chunk.first_block() <= h.number)
                                .cloned();

                            if finalized_head.is_none() {
                                tx.set_finalized_head(None)
                            }

                            return Ok(Status::Range {
                                first_chunk: chunk,
                                head,
                                finalized_head
                            });
                        }
                    }
                }
                tx.set_finalized_head(None);
                Ok(status)
            })?;

        match status {
            Status::Range {
                first_chunk,
                head,
                finalized_head
            } => {
                self.set_head(Some(get_chunk_head(&head)));
                self.set_finalized_head(finalized_head);
                self.first_chunk_head = Some(get_chunk_head(&first_chunk));
                info!(
                    "retained blocks from {} to {}",
                    first_chunk.first_block(),
                    head.last_block()
                );
            }
            Status::HashMismatch => {
                self.clear_heads();
                warn!("cleared dataset due to parent block hash mismatch")
            }
            Status::Gap(existed) => {
                self.clear_heads();
                warn!(
                    "cleared dataset, because there was a gap between first requested block {} and already existed {}",
                    from_block, existed
                )
            }
            Status::Clear => {
                self.clear_heads();
                info!("dataset was cleared")
            }
        }

        self.first_block = from_block;
        self.parent_block_hash = parent_block_hash;
        Ok(())
    }

    fn clear_heads(&mut self) {
        self.set_head(None);
        self.set_finalized_head(None);
        self.first_chunk_head = None;
    }

    pub fn retain(&mut self, from_block: BlockNumber, parent_block_hash: Option<String>) -> anyhow::Result<()> {
        let dataset_id = self.dataset_id;
        observe_storage_write(dataset_id, WriteStage::Retention, |metrics| {
            self._retain(from_block, parent_block_hash, true, metrics)
        })
    }

    pub fn init_retention(&mut self, from_block: BlockNumber, parent_block_hash: Option<String>) -> anyhow::Result<()> {
        let dataset_id = self.dataset_id;
        observe_storage_write(dataset_id, WriteStage::Retention, |metrics| {
            self._retain(from_block, parent_block_hash, false, metrics)
        })
    }

    #[instrument(skip_all, fields(
        block_number = new_finalized_head.number,
        block_hash = %new_finalized_head.hash
    ))]
    pub fn finalize(&mut self, new_finalized_head: &BlockRef) -> anyhow::Result<()> {
        let Some(head) = self.head.as_ref() else { return Ok(()) };

        let update = self.db.update_dataset(self.dataset_id, |tx| {
            ensure!(
                tx.label().finalized_head() == self.finalized_head.as_ref(),
                "seems like the dataset is controlled by multiple processes"
            );

            if let Some(current) = tx.label().finalized_head() {
                if current.number > new_finalized_head.number {
                    return Ok(None);
                }
                if current.number == new_finalized_head.number {
                    ensure!(current.hash == new_finalized_head.hash);
                    return Ok(None);
                }
            }

            let maybe_head_chunk = tx.list_chunks(0, None).into_reversed().next().transpose()?;

            let head_chunk = match maybe_head_chunk {
                Some(c) if c.last_block_hash() == head.hash => c,
                _ => bail!("seems like the dataset is controlled by multiple processes")
            };

            let new_finalized_head = if new_finalized_head.number > head_chunk.last_block() {
                get_chunk_head(&head_chunk)
            } else if new_finalized_head.number == head_chunk.last_block() {
                ensure!(new_finalized_head.hash == head_chunk.last_block_hash());
                new_finalized_head.clone()
            } else {
                new_finalized_head.clone()
            };

            tx.set_finalized_head(new_finalized_head.clone());

            Ok(Some(new_finalized_head))
        })?;

        if let Some(new_head) = update {
            debug!(
                block_number = new_head.number,
                block_hash = new_head.hash,
                "saved new finalized head"
            );
            self.set_finalized_head(Some(new_head));
        } else {
            debug!("finalized head was ignored")
        }

        Ok(())
    }

    #[instrument(skip_all, fields(
        first_block = chunk.first_block(),
        last_block = chunk.last_block(),
        last_block_hash = %chunk.last_block_hash(),
        finalized_head = valuable(&finalized_head),
    ))]
    pub fn new_chunk(&mut self, finalized_head: Option<&BlockRef>, chunk: &StorageChunk) -> anyhow::Result<()> {
        // FIXME: accept self.first_block rollback limit
        let dataset_id = self.dataset_id;
        let finalized_head = observe_storage_write(dataset_id, WriteStage::Commit, |metrics| {
            self.db
                .update_dataset_with_hash_index_metrics(dataset_id, metrics, |tx| {
                    let new_finalized_head = match (finalized_head, tx.label().finalized_head()) {
                        (Some(new), None) => Some(new),
                        (Some(new), Some(current)) if new.number >= current.number => Some(new),
                        (_, Some(current)) if current.number < chunk.first_block() => Some(current),
                        (_, Some(_)) => bail!(
                            "can't fork safely, because fork base is below the current finalized head \
                        and finalized head of the data pack is below the current"
                        ),
                        (None, None) => None
                    };

                    let new_finalized_head = new_finalized_head.map(|head| {
                        if head.number < chunk.last_block() {
                            head.clone()
                        } else {
                            get_chunk_head(&chunk)
                        }
                    });

                    tx.set_finalized_head(new_finalized_head.clone());
                    tx.insert_fork(chunk)?;
                    Ok(new_finalized_head)
                })
        })?;

        debug!(finalized_head = valuable(&finalized_head), "saved new chunk");

        // Head before finalized, so a subscriber never observes finalized > head (INV-5).
        self.set_head(Some(get_chunk_head(&chunk)));
        self.set_finalized_head(finalized_head);
        if self
            .first_chunk_head
            .as_ref()
            .map_or(true, |h| chunk.first_block() <= h.number)
        {
            self.first_chunk_head = self.head.clone();
        }

        Ok(())
    }

    /// `retain_from_head` is the `Head(n)` window size, if set; on EXTEND the
    /// window is trimmed to keep at most `n` blocks behind the head.
    pub fn handle_ingest_msg(&mut self, msg: IngestMessage, retain_from_head: Option<u64>) -> anyhow::Result<()> {
        match msg {
            IngestMessage::FinalizedHead(finalized_head) => {
                self.finalize(&finalized_head)?;
            }
            IngestMessage::NewChunk(new_chunk) => {
                let ctx = format!("failed to write new chunk {}", new_chunk);
                self.write_new_chunk(new_chunk).context(ctx)?;
                if let Some(n) = retain_from_head {
                    let first_chunk_head = self.first_chunk_head().map(|h| h.number);
                    if let Some(floor) = trim_floor(first_chunk_head, self.next_block(), n) {
                        self.retain(floor, None)?;
                    }
                }
            }
            IngestMessage::Fork {
                prev_blocks,
                rollback_sender
            } => {
                self.compute_rollback(&prev_blocks).map(|rollback| {
                    let _ = rollback_sender.send(rollback);
                })?;
            }
        }
        Ok(())
    }

    fn write_new_chunk(&mut self, mut new_chunk: NewChunk) -> anyhow::Result<()> {
        let desc = self.dataset_kind().dataset_description();
        let started = StdInstant::now();
        let tables: anyhow::Result<_> = (|| {
            let mut tables = BTreeMap::new();

            for (name, prepared) in new_chunk.tables.iter_mut() {
                let mut builder = self.db.new_table_builder(prepared.schema());

                if let Some(table_desc) = desc.tables.get(name) {
                    for (&col, opts) in table_desc.options.column_options.iter() {
                        if opts.stats_enable {
                            builder.add_stat_by_name(col)?;
                        }
                    }
                }

                prepared.read(&mut builder, 0, prepared.num_rows())?;

                tables.insert(name.to_string(), builder.finish()?);
            }

            Ok(tables)
        })();
        report_write_duration(self.dataset_id, WriteStage::Tables, started.elapsed(), tables.is_ok());
        let tables = tables?;

        let chunk = Chunk::V1 {
            parent_block_hash: new_chunk.parent_block_hash,
            first_block: new_chunk.first_block,
            last_block: new_chunk.last_block,
            last_block_hash: new_chunk.last_block_hash,
            first_block_time: new_chunk.first_block_time,
            last_block_time: new_chunk.last_block_time,
            tables
        };

        self.new_chunk(new_chunk.finalized_head.as_ref(), &chunk)
    }

    pub fn starts_at(&self, block_number: BlockNumber, parent_hash: &Option<String>) -> bool {
        self.start_block() == block_number && self.start_block_parent_hash() == parent_hash.as_ref().map(String::as_str)
    }
}

fn get_chunk_head(chunk: &Chunk) -> BlockRef {
    BlockRef {
        number: chunk.last_block(),
        hash: chunk.last_block_hash().to_string()
    }
}

// Returns the new floor when the tail has to be trimmed to keep
// `max_blocks` behind the tip, or `None` when the window still fits.
//
// `max_blocks` is a soft limit. Since `retain()` only drops whole chunks, trimming
// may keep a part of the first chunk.
fn trim_floor(first_chunk_head: Option<BlockNumber>, next_block: BlockNumber, max_blocks: u64) -> Option<BlockNumber> {
    let first_chunk_head = first_chunk_head?;
    (next_block - first_chunk_head > max_blocks).then(|| next_block - max_blocks)
}

/// Publish only on a real change: a no-op FINALIZE must not wake
/// `wait_for_finalized_block` waiters.
fn publish(sender: &watch::Sender<Option<BlockRef>>, value: Option<BlockRef>) {
    sender.send_if_modified(|current| {
        if *current == value {
            false
        } else {
            *current = value;
            true
        }
    });
}

/// Time a storage write and report its duration plus the hash-index counters the
/// storage transaction fills into `metrics`.
fn observe_storage_write<R>(
    dataset_id: DatasetId,
    stage: WriteStage,
    write: impl FnOnce(&mut HashIndexWriteMetrics) -> anyhow::Result<R>
) -> anyhow::Result<R> {
    let mut hash_metrics = HashIndexWriteMetrics::default();
    let started = StdInstant::now();
    let result = write(&mut hash_metrics);
    let success = result.is_ok();
    report_write_duration(dataset_id, stage, started.elapsed(), success);
    report_hash_index_write_metrics(dataset_id, &hash_metrics, success);
    result
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, sync::Arc};

    use sqd_primitives::BlockRef;
    use sqd_storage::db::{Chunk, DatabaseSettings, DatasetId};
    use tokio::sync::watch;

    use super::{WriteController, trim_floor};
    use crate::types::{DBRef, DatasetKind};

    #[test]
    fn nothing_is_trimmed_while_the_window_fits() {
        assert_eq!(trim_floor(None, 500, 100), None);
        // The whole dataset is one chunk [0..50], well inside the cap.
        assert_eq!(trim_floor(Some(50), 51, 100), None);
        // Exactly at the cap: the first chunk still has a block in the window.
        assert_eq!(trim_floor(Some(0), 100, 100), None);
    }

    #[test]
    fn the_tail_is_trimmed_once_the_first_chunk_leaves_the_window() {
        // First chunk ends at 0, so trimming starts one block past the cap.
        assert_eq!(trim_floor(Some(0), 101, 100), Some(1));
        // The soft-limit overshoot: [0..150K] under a 100K cap survives until 250K.
        assert_eq!(trim_floor(Some(150_000), 250_000, 100_000), None);
        assert_eq!(trim_floor(Some(150_000), 250_001, 100_000), Some(150_001));
    }

    fn block(number: u64, hash: &str) -> BlockRef {
        BlockRef {
            number,
            hash: hash.to_string()
        }
    }

    /// Head/linkage metadata only — no Arrow tables (an empty table set skips
    /// hash indexing; the head comes from `last_block`/`last_block_hash`).
    fn chunk(first: u64, last: u64, last_hash: &str, parent_hash: &str) -> Chunk {
        Chunk::V1 {
            first_block: first,
            last_block: last,
            last_block_hash: last_hash.to_string(),
            parent_block_hash: parent_hash.to_string(),
            first_block_time: None,
            last_block_time: None,
            tables: BTreeMap::new()
        }
    }

    struct Fixture {
        db: DBRef,
        dataset_id: DatasetId,
        head_rx: watch::Receiver<Option<BlockRef>>,
        fin_rx: watch::Receiver<Option<BlockRef>>,
        wc: WriteController,
        // Dropped last so RocksDB closes before the directory is removed.
        _dir: tempfile::TempDir
    }

    fn fixture() -> Fixture {
        let dir = tempfile::tempdir().unwrap();
        let db: DBRef = Arc::new(DatabaseSettings::default().open(dir.path()).unwrap());
        let dataset_id = DatasetId::from_str("evm-test");
        let (head_tx, head_rx) = watch::channel(None);
        let (fin_tx, fin_rx) = watch::channel(None);
        let wc = WriteController::new(db.clone(), dataset_id, DatasetKind::Evm, head_tx, fin_tx).unwrap();
        Fixture {
            db,
            dataset_id,
            head_rx,
            fin_rx,
            wc,
            _dir: dir
        }
    }

    // INV-30/31: the published head equals what is durable in storage.
    #[test]
    fn new_chunk_publishes_committed_head() {
        let mut f = fixture();
        assert_eq!(*f.head_rx.borrow(), None);

        f.wc.new_chunk(None, &chunk(1, 10, "h10", "h0")).unwrap();

        assert_eq!(*f.head_rx.borrow(), Some(block(10, "h10")));
        let stored = f.db.snapshot().get_last_chunk(f.dataset_id).unwrap().unwrap();
        assert_eq!(stored.last_block(), 10);
        assert_eq!(stored.last_block_hash(), "h10");
    }

    // INV-30: the published finalized head equals the storage label.
    #[test]
    fn finalize_publishes_committed_finalized_head() {
        let mut f = fixture();
        f.wc.new_chunk(None, &chunk(1, 10, "h10", "h0")).unwrap();
        assert_eq!(*f.fin_rx.borrow(), None);

        f.wc.finalize(&block(5, "h5")).unwrap();

        assert_eq!(*f.fin_rx.borrow(), Some(block(5, "h5")));
        let label = f.db.snapshot().get_label(f.dataset_id).unwrap().unwrap();
        assert_eq!(label.finalized_head(), Some(&block(5, "h5")));
    }

    // INV-40/CN-9: a rebuilt writer reseeds subscribers from committed storage.
    #[test]
    fn rebuilt_writer_reseeds_watermarks_from_storage() {
        let mut f = fixture();
        f.wc.new_chunk(None, &chunk(1, 10, "h10", "h0")).unwrap();
        f.wc.finalize(&block(5, "h5")).unwrap();
        drop(f.wc);

        let (head_tx, head_rx) = watch::channel(None);
        let (fin_tx, fin_rx) = watch::channel(None);
        let _wc = WriteController::new(f.db.clone(), f.dataset_id, DatasetKind::Evm, head_tx, fin_tx).unwrap();

        assert_eq!(*head_rx.borrow(), Some(block(10, "h10")));
        assert_eq!(*fin_rx.borrow(), Some(block(5, "h5")));
    }

    // Head-only progress must not fire the finalized channel — no spurious
    // `wait_for_finalized_block` wakeups. Guards the `publish` dedup.
    #[test]
    fn head_only_progress_does_not_wake_finalized_waiters() {
        let mut f = fixture();
        f.wc.new_chunk(None, &chunk(1, 10, "h10", "h0")).unwrap();
        // observe the current (still-None) finalized head
        assert_eq!(*f.fin_rx.borrow_and_update(), None);

        f.wc.new_chunk(None, &chunk(11, 20, "h20", "h10")).unwrap();

        assert_eq!(*f.head_rx.borrow(), Some(block(20, "h20")));
        assert!(
            !f.fin_rx.has_changed().unwrap(),
            "finalized head must stay unchanged while unfinalized blocks arrive"
        );
    }

    // Perf probe over the real read (`get_head` == borrow+clone) and write
    // (commit→set_head→publish) paths. Run:
    //   cargo test -p sqd-hotblocks --bin sqd-hotblocks -- --ignored --nocapture watermark_hotpath
    #[test]
    #[ignore]
    fn watermark_hotpath_throughput() {
        use std::time::Instant;

        let mut f = fixture();
        f.wc.new_chunk(None, &chunk(1, 10, "h10", "h0")).unwrap();

        let reads = 2_000_000u32;
        let t = Instant::now();
        let mut last = None;
        for _ in 0..reads {
            last = f.head_rx.borrow().clone();
        }
        let read_ns = t.elapsed().as_nanos() as f64 / reads as f64;
        assert_eq!(last, Some(block(10, "h10")));
        eprintln!("watermark read: {read_ns:.1} ns/op");

        let commits = 5_000u64;
        let mut parent = "h10".to_string();
        let mut last_block = 10u64;
        let t = Instant::now();
        for _ in 0..commits {
            let first = last_block + 1;
            let last = last_block + 10;
            let hash = format!("h{last}");
            f.wc.new_chunk(None, &chunk(first, last, &hash, &parent)).unwrap();
            parent = hash;
            last_block = last;
        }
        let commit_us = t.elapsed().as_micros() as f64 / commits as f64;
        eprintln!("chunk commit + publish: {commit_us:.1} us/op");
        assert_eq!(*f.head_rx.borrow(), Some(block(last_block, &parent)));
    }
}
