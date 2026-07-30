use std::{collections::BTreeMap, time::Instant as StdInstant};

use anyhow::{Context, anyhow, bail, ensure};
use sqd_primitives::{BlockNumber, BlockRef};
use sqd_storage::db::{Chunk as StorageChunk, Chunk, DatasetId, HashIndexWriteMetrics};
use tokio::sync::watch;
use tracing::{debug, field::valuable, info, instrument, warn};

use crate::{
    dataset_controller::ingest_generic::{IngestMessage, NewChunk},
    errors::{UnapplicableFork, UnapplicableForkReason as ForkReason},
    metrics::{WriteStage, report_hash_index_write_metrics, report_write_duration},
    types::{DBRef, DatasetKind}
};

/// Source position selected after resolving a fork against stored history.
///
/// `resume_from` lands on a stored chunk boundary, so it may sit at or below `fin` when the
/// common ancestor is inside a finality-straddling chunk; the finalized prefix is guarded on the
/// write path instead ([`WriteController::new_chunk`]).
#[derive(Debug)]
pub struct Rollback {
    /// Lowest block number the source may return after resolving the fork.
    pub resume_from: BlockNumber,
    /// Hash that must anchor the first returned block, when an anchor is known.
    pub expected_parent_hash: Option<String>,
    /// Lowest block the first replayed chunk must reach, set when `resume_from` sits at or below
    /// `fin`. The swap deletes the chunk holding the finalized block; a chunk stopping short never
    /// puts it back, and the retry repeats the identical cut forever.
    pub reach_at_least: Option<BlockNumber>
}

pub(super) enum FlushFloorUpdate {
    Unchanged,
    Set(Option<BlockNumber>)
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

        let finalized_head = label.finalized_head().cloned();

        if let Some(finalized_head) = finalized_head.as_ref() {
            let pos = match prev.iter().position(|b| b.number >= finalized_head.number) {
                Some(pos) => pos,
                None => bail!(UnapplicableFork {
                    reason: ForkReason::HintsBelowFinalizedHead
                })
            };
            if prev[pos].number == finalized_head.number {
                ensure!(
                    prev[pos].hash == finalized_head.hash,
                    UnapplicableFork {
                        reason: ForkReason::HintConflictsWithFinalizedHead
                    }
                );
            }
            prev = &prev[pos..]
        }

        let (resume_from, expected_parent_hash) = 'resume: {
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
                        break 'resume (b.number + 1, Some(b.hash.clone()));
                    }
                } else {
                    break 'resume (head.last_block() + 1, Some(head.last_block_hash().to_string()));
                }
            }

            // Retention trims whole chunks, so `self.first_block` can sit inside the surviving
            // one — a position `insert_fork` refuses as overlapping. Fall back to the physical
            // start of the window instead.
            match snapshot.get_first_chunk(self.dataset_id)? {
                Some(chunk) => (chunk.first_block(), Some(chunk.parent_block_hash().to_string())),
                None => (self.first_block, self.parent_block_hash.clone())
            }
        };

        Ok(Rollback {
            resume_from,
            expected_parent_hash,
            reach_at_least: finalized_head
                .filter(|fin| resume_from <= fin.number)
                .map(|fin| fin.number)
        })
    }

    #[instrument(name = "retain", skip(self, delete_mismatch))]
    fn _retain(
        &mut self,
        from_block: BlockNumber,
        parent_block_hash: Option<String>,
        delete_mismatch: bool,
        metrics: &mut HashIndexWriteMetrics
    ) -> anyhow::Result<bool> {
        // Nothing left to delete below a floor that is already there, and `Api` repeats the same
        // floor often. Answering from the window's emptiness would restart an untouched ingest.
        if self.starts_at(from_block, &parent_block_hash) {
            return Ok(true);
        }

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

        let kept_head = match status {
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
                true
            }
            Status::HashMismatch => {
                self.clear_heads();
                warn!("cleared dataset due to parent block hash mismatch");
                false
            }
            Status::Gap(existed) => {
                self.clear_heads();
                warn!(
                    "cleared dataset, because there was a gap between first requested block {} and already existed {}",
                    from_block, existed
                );
                false
            }
            Status::Clear => {
                self.clear_heads();
                info!("dataset was cleared");
                false
            }
        };

        self.first_block = from_block;
        self.parent_block_hash = parent_block_hash;
        Ok(kept_head)
    }

    fn clear_heads(&mut self) {
        self.set_head(None);
        self.set_finalized_head(None);
        self.first_chunk_head = None;
    }

    /// `false` when this call cleared the window instead of trimming it: the head a live ingest is
    /// building on is gone and it must restart. A call that changes nothing reports `true`.
    pub fn retain(&mut self, from_block: BlockNumber, parent_block_hash: Option<String>) -> anyhow::Result<bool> {
        let dataset_id = self.dataset_id;
        observe_storage_write(dataset_id, WriteStage::Retention, |metrics| {
            self._retain(from_block, parent_block_hash, true, metrics)
        })
    }

    pub fn init_retention(
        &mut self,
        from_block: BlockNumber,
        parent_block_hash: Option<String>
    ) -> anyhow::Result<bool> {
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
        let logical_floor = self.first_block;

        let update = self.db.update_dataset(self.dataset_id, |tx| {
            ensure!(
                tx.label().finalized_head() == self.finalized_head.as_ref(),
                "seems like the dataset is controlled by multiple processes"
            );

            let maybe_head_chunk = tx.list_chunks(0, None).into_reversed().next().transpose()?;

            let _stored_head = match maybe_head_chunk {
                Some(c) if c.last_block_hash() == head.hash => c,
                _ => bail!("seems like the dataset is controlled by multiple processes")
            };

            match resolve_finality(tx, tx.label().finalized_head(), None, logical_floor, new_finalized_head)? {
                FinalityDecision::Applied(new_head) => {
                    tx.set_finalized_head(new_head.clone());
                    Ok(CommittedFinalityUpdate::Applied(new_head))
                }
                FinalityDecision::Ignored(reason) => Ok(CommittedFinalityUpdate::Ignored(reason)),
                FinalityDecision::IntegrityFault { reason, detail } => Err(unapplicable_fork(reason, detail))
            }
        })?;

        match update {
            CommittedFinalityUpdate::Applied(new_head) => {
                debug!(
                    block_number = new_head.number,
                    block_hash = new_head.hash,
                    "saved new finalized head"
                );
                self.set_finalized_head(Some(new_head));
            }
            CommittedFinalityUpdate::Ignored(reason) => {
                debug!(reason = %reason, "finalized head was ignored")
            }
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
        let logical_floor = self.first_block;
        let commit = observe_storage_write(dataset_id, WriteStage::Commit, |metrics| {
            self.db
                .update_dataset_with_hash_index_metrics(dataset_id, metrics, |tx| {
                    // A rollback resolved before a trim can aim below the window that survived it;
                    // `insert_fork` would then drop every surviving chunk and leave the head under
                    // the floor. The bound is the first *stored* chunk where there is one:
                    // retention trims whole chunks, so the logical floor can sit inside the
                    // surviving one, which a fork legitimately resumes at. With nothing stored
                    // there is no such chunk, and `compute_rollback` falls back to the logical
                    // floor too.
                    let window_start = match tx.list_chunks(0, None).next().transpose()? {
                        Some(chunk) => chunk.first_block(),
                        None => logical_floor
                    };
                    ensure!(
                        chunk.first_block() >= window_start,
                        unapplicable_fork(
                            ForkReason::BelowRetainedWindow,
                            format!(
                                "chunk {}-{} starts below window start {}",
                                chunk.first_block(),
                                chunk.last_block(),
                                window_start
                            )
                        )
                    );

                    // Compaction may merge across a boundary after `compute_rollback` selected it.
                    // Starting inside that merged chunk is a stale plan, not a storage failure:
                    // leave the old state intact and let the next epoch resolve against the new
                    // layout. FUTURE: feed this verdict back to the live ingest and re-resolve
                    // immediately instead of paying the one-minute epoch restart.
                    let boundary_owner = tx
                        .list_chunks(chunk.first_block(), Some(chunk.first_block()))
                        .next()
                        .transpose()?;
                    if let Some(existing) = boundary_owner
                        && existing.first_block() < chunk.first_block()
                        && chunk.first_block() <= existing.last_block()
                    {
                        return Err(unapplicable_fork(
                            ForkReason::StaleRollbackBoundary,
                            format!(
                                "chunk {}-{} starts inside compacted chunk {}-{}",
                                chunk.first_block(),
                                chunk.last_block(),
                                existing.first_block(),
                                existing.last_block()
                            )
                        ));
                    }

                    let current_finalized_head = tx.label().finalized_head().cloned();

                    // A fork resuming at a chunk boundary can reach below `fin`. Admit it only if
                    // it reproduces that region exactly — spanning `fin`, matching every stored
                    // hash up to it. `fin`'s own hash alone proves nothing: hashes come from the
                    // source, so a reproduced boundary says nothing about what leads to it.
                    if let Some(current) = current_finalized_head.as_ref()
                        && chunk.first_block() <= current.number
                    {
                        ensure!(
                            chunk.last_block() >= current.number,
                            unapplicable_fork(
                                ForkReason::DropsFinalizedBlock,
                                format!(
                                    "chunk {}-{} does not reach finalized block {}",
                                    chunk.first_block(),
                                    chunk.last_block(),
                                    current.number
                                )
                            )
                        );
                        if let Err(divergence) = tx.validate_finalized_prefix(chunk, current.number)? {
                            return Err(unapplicable_fork(ForkReason::RewritesFinalizedHistory, divergence));
                        }
                    }

                    // `fin` anchors the guard above and every later `compute_rollback`, and the
                    // report behind it is a header, not a block the source served (WP-8).
                    let new_finalized_head = match finalized_head {
                        None => current_finalized_head.clone(),
                        Some(report) => {
                            match resolve_finality(
                                tx,
                                current_finalized_head.as_ref(),
                                Some(chunk),
                                logical_floor,
                                report
                            )? {
                                FinalityDecision::Applied(new_head) => Some(new_head),
                                FinalityDecision::Ignored(reason) => {
                                    debug!(reason = %reason, "finalized head was ignored");
                                    current_finalized_head.clone()
                                }
                                FinalityDecision::IntegrityFault { reason, detail } => {
                                    return Err(unapplicable_fork(reason, detail));
                                }
                            }
                        }
                    };

                    tx.set_finalized_head(new_finalized_head.clone());
                    tx.insert_fork(chunk)?;
                    Ok(new_finalized_head)
                })
        });

        // The caller materialized these tables before we could judge the chunk, and only a
        // committed `write_chunk` clears their dirty markers — the orphan sweep runs at startup
        // only. A source refused on every retry would leak a chunk a minute.
        let finalized_head = match commit {
            Ok(head) => head,
            Err(err) => {
                self.abandon_tables(chunk);
                return Err(err);
            }
        };

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
    pub fn handle_ingest_msg(
        &mut self,
        msg: IngestMessage,
        retain_from_head: Option<u64>
    ) -> anyhow::Result<FlushFloorUpdate> {
        let update = match msg {
            IngestMessage::FinalizedHead(finalized_head) => {
                self.finalize(&finalized_head)?;
                FlushFloorUpdate::Unchanged
            }
            IngestMessage::NewChunk(new_chunk) => {
                let ctx = format!("failed to write new chunk {}", new_chunk);
                self.write_new_chunk(new_chunk).context(ctx)?;
                if let Some(n) = retain_from_head {
                    let first_chunk_head = self.first_chunk_head().map(|h| h.number);
                    if let Some(floor) = trim_floor(first_chunk_head, self.next_block(), n) {
                        // Verdict ignored: `trim_floor` fires above the first chunk's last block
                        // and `n >= 1` keeps it at or below the head, so with chunks tiling by
                        // `last_block + 1` the floor always lands inside one. `Head(0)` aside.

                        self.retain(floor, None)?;
                    }
                }
                FlushFloorUpdate::Set(None)
            }
            IngestMessage::Fork {
                prev_blocks,
                rollback_sender
            } => {
                let rollback = self.compute_rollback(&prev_blocks)?;
                let flush_floor = rollback.reach_at_least;
                if rollback_sender.send(rollback).is_ok() {
                    FlushFloorUpdate::Set(flush_floor)
                } else {
                    FlushFloorUpdate::Set(None)
                }
            }
        };
        Ok(update)
    }

    fn abandon_tables(&self, chunk: &StorageChunk) {
        let tables = chunk.tables().values().copied().collect::<Vec<_>>();
        if let Err(err) = self.db.delete_tables(&tables) {
            warn!(reason =? err, "failed to abandon the tables of a refused chunk");
        }
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

/// Keeps [`UnapplicableFork`] in the chain — `report_dataset_epoch_failure` buckets on the type,
/// and the message can never carry block numbers or hashes into a metric label.
fn unapplicable_fork(reason: ForkReason, detail: String) -> anyhow::Error {
    anyhow::Error::new(UnapplicableFork { reason }).context(detail)
}

#[derive(Debug)]
enum CommittedFinalityUpdate {
    Applied(BlockRef),
    Ignored(FinalityIgnoreReason)
}

#[derive(Debug)]
enum FinalityDecision {
    Applied(BlockRef),
    Ignored(FinalityIgnoreReason),
    IntegrityFault { reason: ForkReason, detail: String }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum FinalityIgnoreReason {
    Regressive,
    AlreadyFinalized,
    BelowRetainedWindow,
    NoBlockAtHeight
}

impl FinalityIgnoreReason {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Regressive => "regressive",
            Self::AlreadyFinalized => "already_finalized",
            Self::BelowRetainedWindow => "below_retained_window",
            Self::NoBlockAtHeight => "no_block_at_height"
        }
    }
}

impl std::fmt::Display for FinalityIgnoreReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Resolves one report against the state that will exist after this transaction.
///
/// A replacement owns its whole numeric range; stored history owns heights below it. A report
/// above the resulting head is clamped to that head. At an exact height, the owner must carry the
/// reported hash. A genuine sparse hole is ignored, while a replacement omitting a stored block
/// that the same response calls final is an integrity fault (WP-8).
fn resolve_finality(
    tx: &sqd_storage::db::DatasetUpdate<'_>,
    current: Option<&BlockRef>,
    replacement: Option<&StorageChunk>,
    logical_floor: BlockNumber,
    report: &BlockRef
) -> anyhow::Result<FinalityDecision> {
    let post_head = match replacement {
        Some(chunk) => get_chunk_head(chunk),
        None => {
            let head = tx
                .list_chunks(0, None)
                .into_reversed()
                .next()
                .transpose()?
                .ok_or_else(|| anyhow!("can't finalize an empty dataset"))?;
            get_chunk_head(&head)
        }
    };

    let (effective, clamped) = if report.number > post_head.number {
        (post_head, true)
    } else {
        (report.clone(), false)
    };

    if effective.number < logical_floor {
        return Ok(FinalityDecision::Ignored(FinalityIgnoreReason::BelowRetainedWindow));
    }

    if let Some(current) = current {
        if effective.number < current.number {
            return Ok(FinalityDecision::Ignored(FinalityIgnoreReason::Regressive));
        }
        if effective.number == current.number {
            if effective.hash != current.hash {
                return Ok(FinalityDecision::IntegrityFault {
                    reason: ForkReason::FinalityHashChanged,
                    detail: format!(
                        "block {}: expected {}, got {}",
                        current.number, current.hash, effective.hash
                    )
                });
            }
            return Ok(FinalityDecision::Ignored(FinalityIgnoreReason::AlreadyFinalized));
        }
    }

    // A claim about a descendant proves the resulting head final without asserting the
    // descendant's hash exists in this dataset.
    if clamped {
        return Ok(FinalityDecision::Applied(effective));
    }

    let in_batch = replacement.filter(|c| c.first_block() <= report.number && report.number <= c.last_block());

    if let Some(chunk) = in_batch {
        if let Some(hash) = tx.find_block_hash_in_chunk(chunk, report.number)? {
            return Ok(if hash == report.hash {
                FinalityDecision::Applied(report.clone())
            } else {
                FinalityDecision::IntegrityFault {
                    reason: ForkReason::FinalityContradictsChunk,
                    detail: format!(
                        "block {}: chunk carries {}, finality reports {}",
                        report.number, hash, report.hash
                    )
                }
            });
        }
        // Missing from the batch is only a hole if stored history agrees: otherwise this commit
        // deletes a block the same response calls final.
        if let Some(hash) = tx.find_stored_block_hash(report.number)? {
            return Ok(FinalityDecision::IntegrityFault {
                reason: ForkReason::FinalityEvictsFinalizedBlock,
                detail: format!(
                    "block {}#{} is dropped by the chunk whose response reports it finalized",
                    report.number, hash
                )
            });
        }
        warn!(
            block_number = report.number,
            block_hash = %report.hash,
            "finality reported at a height neither the chunk nor stored history carries; ignoring it"
        );
        return Ok(FinalityDecision::Ignored(FinalityIgnoreReason::NoBlockAtHeight));
    }

    match tx.find_stored_block_hash(report.number)? {
        Some(hash) => Ok(if hash == report.hash {
            FinalityDecision::Applied(report.clone())
        } else {
            FinalityDecision::IntegrityFault {
                reason: ForkReason::FinalityContradictsStoredBlock,
                detail: format!(
                    "block {}: stored history carries {}, finality reports {}",
                    report.number, hash, report.hash
                )
            }
        }),
        None => {
            warn!(
                block_number = report.number,
                block_hash = %report.hash,
                "finality reported at a height stored history does not carry; ignoring it"
            );
            Ok(FinalityDecision::Ignored(FinalityIgnoreReason::NoBlockAtHeight))
        }
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

    use arrow::{
        array::{RecordBatch, StringArray, UInt64Array},
        datatypes::{DataType, Field, Schema}
    };
    use sqd_primitives::BlockRef;
    use sqd_storage::db::{Chunk, CompactionStatus, DatabaseSettings, DatasetId};
    use tokio::sync::watch;

    use super::{WriteController, get_chunk_head, trim_floor};
    use crate::{
        errors::{UnapplicableFork, UnapplicableForkReason},
        types::{DBRef, DatasetKind}
    };

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

    // INV-30: the published finalized head equals the storage label. Real tables, because a report
    // below the head chunk's last block is now checked against the block stored there.
    #[test]
    fn finalize_publishes_committed_finalized_head() {
        let mut f = fixture();
        f.wc.new_chunk(None, &linked_chunk(&f.db, 1, 10, "h0", "h").unwrap())
            .unwrap();
        assert_eq!(*f.fin_rx.borrow(), None);

        f.wc.finalize(&block(5, "h-5")).unwrap();

        assert_eq!(*f.fin_rx.borrow(), Some(block(5, "h-5")));
        let label = f.db.snapshot().get_label(f.dataset_id).unwrap().unwrap();
        assert_eq!(label.finalized_head(), Some(&block(5, "h-5")));
    }

    // INV-40/CN-9: a rebuilt writer reseeds subscribers from committed storage.
    #[test]
    fn rebuilt_writer_reseeds_watermarks_from_storage() {
        let mut f = fixture();
        f.wc.new_chunk(None, &linked_chunk(&f.db, 1, 10, "h0", "h").unwrap())
            .unwrap();
        f.wc.finalize(&block(5, "h-5")).unwrap();
        drop(f.wc);

        let (head_tx, head_rx) = watch::channel(None);
        let (fin_tx, fin_rx) = watch::channel(None);
        let _wc = WriteController::new(f.db.clone(), f.dataset_id, DatasetKind::Evm, head_tx, fin_tx).unwrap();

        assert_eq!(*head_rx.borrow(), Some(block(10, "h-10")));
        assert_eq!(*fin_rx.borrow(), Some(block(5, "h-5")));
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

    /// Blocks carry `{tag}-{number}` hashes: two chunks agree on a range exactly when built with
    /// the same tag.
    fn hashes(tag: &str, first_block: u64, last_block: u64) -> Vec<String> {
        (first_block..=last_block).map(|n| format!("{tag}-{n}")).collect()
    }

    /// A chunk with a real `blocks` table — the finalized-prefix guard reads hashes out of
    /// storage, so the table-less `chunk` above would exercise nothing.
    fn chunk_with_hashes(db: &DBRef, first_block: u64, parent_hash: &str, hashes: &[String]) -> anyhow::Result<Chunk> {
        let blocks = hashes
            .iter()
            .enumerate()
            .map(|(i, hash)| (first_block + i as u64, hash.clone()))
            .collect::<Vec<_>>();
        chunk_with_numbers(db, parent_hash, &blocks)
    }

    /// The same, with the block numbers given explicitly — a chain with holes (Solana slots) is
    /// not expressible as a contiguous range.
    fn chunk_with_numbers(db: &DBRef, parent_hash: &str, blocks: &[(u64, String)]) -> anyhow::Result<Chunk> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("number", DataType::UInt64, false),
            Field::new("hash", DataType::Utf8, false),
            Field::new("parent_hash", DataType::Utf8, false),
        ]));

        let numbers = blocks.iter().map(|(number, _)| *number).collect::<Vec<_>>();
        let hashes = blocks.iter().map(|(_, hash)| hash.clone()).collect::<Vec<_>>();
        let parent_hashes = std::iter::once(parent_hash)
            .chain(hashes.iter().map(String::as_str))
            .take(hashes.len())
            .collect::<Vec<_>>();

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt64Array::from(numbers.clone())),
                Arc::new(StringArray::from(hashes.clone())),
                Arc::new(StringArray::from(parent_hashes)),
            ]
        )?;

        let mut builder = db.new_table_builder(schema);
        builder.write_record_batch(&batch)?;
        let table_id = builder.finish()?;

        Ok(Chunk::V1 {
            first_block: *numbers.first().unwrap(),
            last_block: *numbers.last().unwrap(),
            last_block_hash: hashes.last().unwrap().clone(),
            parent_block_hash: parent_hash.to_string(),
            first_block_time: None,
            last_block_time: None,
            tables: BTreeMap::from([("blocks".to_string(), table_id)])
        })
    }

    fn linked_chunk(
        db: &DBRef,
        first_block: u64,
        last_block: u64,
        parent_hash: &str,
        tag: &str
    ) -> anyhow::Result<Chunk> {
        chunk_with_hashes(db, first_block, parent_hash, &hashes(tag, first_block, last_block))
    }

    fn seed_chain(f: &mut Fixture) -> anyhow::Result<(Chunk, Chunk)> {
        let first = linked_chunk(&f.db, 0, 5, "genesis", "old")?;
        let second = linked_chunk(&f.db, 6, 9, "old-5", "old")?;
        f.wc.new_chunk(None, &first)?;
        f.wc.new_chunk(None, &second)?;
        Ok((first, second))
    }

    fn stored_chunks(f: &Fixture) -> anyhow::Result<Vec<Chunk>> {
        f.db.snapshot()
            .list_chunks(f.dataset_id, 0, None)
            .collect::<anyhow::Result<Vec<_>>>()
    }

    // INV-12/13: a replacement reaching the finalized height with a different hash there is a
    // source equivocating below its own finality; refusing it must leave nothing behind.
    #[test]
    fn replacement_rewriting_the_finalized_block_is_rejected() -> anyhow::Result<()> {
        let mut f = fixture();
        let (first, second) = seed_chain(&mut f)?;
        let current_finalized = block(5, "old-5");
        f.wc.finalize(&current_finalized)?;

        let replacement = linked_chunk(&f.db, 0, 5, "other-genesis", "new")?;
        let result = f.wc.new_chunk(Some(&block(5, "new-5")), &replacement);

        let err = result.unwrap_err();
        assert!(
            err.chain().any(|e| e.is::<UnapplicableFork>()),
            "finality rejections must stay in the unapplicable_fork metric bucket: {err:#}"
        );
        assert_eq!(*f.fin_rx.borrow(), Some(current_finalized.clone()));
        assert_eq!(stored_chunks(&f)?, vec![first, second]);
        assert_eq!(
            f.db.snapshot()
                .get_label(f.dataset_id)?
                .and_then(|label| label.finalized_head().cloned()),
            Some(current_finalized)
        );
        Ok(())
    }

    // INV-12: finality is immutable at a fixed height, even when carried by an ordinary append.
    #[test]
    fn composed_finality_rejects_hash_change_at_fixed_height() -> anyhow::Result<()> {
        let mut f = fixture();
        let (first, second) = seed_chain(&mut f)?;
        let current_finalized = block(5, "old-5");
        f.wc.finalize(&current_finalized)?;

        let append = linked_chunk(&f.db, 10, 12, "old-9", "old")?;
        let result = f.wc.new_chunk(Some(&block(5, "other-5")), &append);

        assert!(result.is_err(), "finalized hash changed at a fixed height");
        assert_eq!(*f.fin_rx.borrow(), Some(current_finalized));
        assert_eq!(stored_chunks(&f)?, vec![first, second]);
        Ok(())
    }

    // Accepting a replacement that stops below `fin` would leave the finalized block absent until
    // some later flush caught up, so it is refused instead.
    #[test]
    fn replacement_below_finalized_that_misses_it_is_rejected() -> anyhow::Result<()> {
        let mut f = fixture();
        let (first, second) = seed_chain(&mut f)?;
        let current_finalized = block(7, "old-7");
        f.wc.finalize(&current_finalized)?;

        let replacement = linked_chunk(&f.db, 6, 6, "old-5", "new")?;
        let result = f.wc.new_chunk(None, &replacement);

        assert!(
            result.is_err(),
            "replacement that drops the finalized block was accepted"
        );
        assert_eq!(*f.fin_rx.borrow(), Some(current_finalized));
        assert_eq!(stored_chunks(&f)?, vec![first, second]);
        Ok(())
    }

    // INV-13: the replacement carries `fin`'s own hash unchanged and a different one below it —
    // what a guard checking only the boundary would admit.
    #[test]
    fn replacement_reproducing_finality_but_rewriting_below_it_is_rejected() -> anyhow::Result<()> {
        let mut f = fixture();
        let (first, second) = seed_chain(&mut f)?;
        let current_finalized = block(7, "old-7");
        f.wc.finalize(&current_finalized)?;

        let replacement = chunk_with_hashes(
            &f.db,
            6,
            "old-5",
            &["fork-6", "old-7", "old-8", "old-9"].map(str::to_string)
        )?;
        let result = f.wc.new_chunk(None, &replacement);

        let err = result.unwrap_err();
        assert!(
            err.chain().any(|e| e.is::<UnapplicableFork>()),
            "finality rejections must stay in the unapplicable_fork metric bucket: {err:#}"
        );
        assert_eq!(*f.fin_rx.borrow(), Some(current_finalized));
        assert_eq!(stored_chunks(&f)?, vec![first, second]);
        // The refused chunk's tables were already durable; leaving them would leak one chunk
        // per 60-second retry, since the orphan sweep only runs at startup.
        assert_eq!(f.db.cleanup()?, 1, "the refused chunk's tables were not abandoned");
        assert_eq!(f.db.purge_orphan_dirty_tables()?, 0);
        Ok(())
    }

    // The honest dual: a reorg above `fin` rewrites the whole straddling chunk and must land.
    #[test]
    fn replacement_reproducing_the_finalized_range_is_accepted() -> anyhow::Result<()> {
        let mut f = fixture();
        let (first, _) = seed_chain(&mut f)?;
        let current_finalized = block(7, "old-7");
        f.wc.finalize(&current_finalized)?;

        let replacement = chunk_with_hashes(
            &f.db,
            6,
            "old-5",
            &["old-6", "old-7", "new-8", "new-9"].map(str::to_string)
        )?;
        f.wc.new_chunk(None, &replacement)?;

        assert_eq!(f.wc.head(), Some(&get_chunk_head(&replacement)));
        assert_eq!(*f.fin_rx.borrow(), Some(current_finalized));
        assert_eq!(stored_chunks(&f)?, vec![first, replacement]);
        // Exactly one table is collected — the replaced chunk's. The accepted chunk keeps its
        // own, so an over-eager abandon would show up here as two.
        assert_eq!(f.db.cleanup()?, 1);
        Ok(())
    }

    // With no matching boundary the fallback is the window start, below `fin`: the resume position
    // is not clamped, the write path guards instead. The anchor comes from the stored chunk, so a
    // full-window replay is still linkage-checked.
    #[test]
    fn rollback_without_matching_hints_resumes_from_window_start() -> anyhow::Result<()> {
        let mut f = fixture();
        seed_chain(&mut f)?;
        f.wc.finalize(&block(4, "old-4"))?;
        let hints = vec![block(5, "fork-5"), block(9, "fork-9")];

        let rollback = f.wc.compute_rollback(&hints)?;

        assert_eq!(rollback.resume_from, f.wc.start_block());
        assert_eq!(rollback.expected_parent_hash.as_deref(), Some("genesis"));
        assert_eq!(rollback.reach_at_least, Some(4));
        Ok(())
    }

    // The wedge regression: clamping to `fin + 1` (8) gave a mid-chunk position `insert_fork`
    // could not satisfy, so resolution stops at the chunk boundary below `fin` instead.
    #[test]
    fn rollback_from_straddling_chunk_resumes_at_chunk_boundary() -> anyhow::Result<()> {
        let mut f = fixture();
        seed_chain(&mut f)?;
        f.wc.finalize(&block(7, "old-7"))?;

        let rollback = f.wc.compute_rollback(&[block(8, "fork-8")])?;

        assert_eq!(rollback.resume_from, 6);
        assert_eq!(rollback.expected_parent_hash.as_deref(), Some("old-5"));
        // The replay must carry the chunk back up to 7, or the swap drops the finalized block.
        assert_eq!(rollback.reach_at_least, Some(7));
        Ok(())
    }

    // A rollback plan carries the finality seen while it was resolved. If finality advances over
    // an unchanged block before the first cut replay flushes, that short chunk is refused; the next
    // resolution raises the floor and must converge instead of repeating the stale cut forever.
    #[test]
    fn replay_re_resolves_after_finality_advances_past_its_floor() -> anyhow::Result<()> {
        let mut f = fixture();
        let (first, second) = seed_chain(&mut f)?;
        f.wc.finalize(&block(7, "old-7"))?;

        let stale = f.wc.compute_rollback(&[block(9, "new-9")])?;
        assert_eq!(stale.resume_from, 6);
        assert_eq!(stale.reach_at_least, Some(7));

        // The source's fork begins at 9, so block 8 is still common and may become final while the
        // replay is in flight.
        let advanced_finality = block(8, "old-8");
        f.wc.finalize(&advanced_finality)?;
        let short_replay = linked_chunk(&f.db, 6, 7, "old-5", "old")?;

        let err = f.wc.new_chunk(None, &short_replay).unwrap_err();
        let refusal = err
            .chain()
            .find_map(|e| e.downcast_ref::<UnapplicableFork>())
            .expect("a stale finality floor is a typed fork refusal");
        assert_eq!(refusal.reason, UnapplicableForkReason::DropsFinalizedBlock);
        assert_eq!(stored_chunks(&f)?, vec![first.clone(), second]);
        assert_eq!(*f.fin_rx.borrow(), Some(advanced_finality.clone()));

        let fresh = f.wc.compute_rollback(&[block(9, "new-9")])?;
        assert_eq!(fresh.resume_from, 6);
        assert_eq!(fresh.reach_at_least, Some(8));
        let replacement = chunk_with_hashes(
            &f.db,
            6,
            "old-5",
            &["old-6", "old-7", "old-8", "new-9"].map(str::to_string)
        )?;
        f.wc.new_chunk(None, &replacement)?;

        assert_eq!(stored_chunks(&f)?, vec![first, replacement]);
        assert_eq!(*f.fin_rx.borrow(), Some(advanced_finality));
        Ok(())
    }

    // Compaction is logically transparent but may erase a physical boundary selected by an
    // in-flight rollback. The stale replay is attributable and atomic; resolving again against the
    // merged layout goes one chunk deeper and succeeds.
    #[test]
    fn replay_re_resolves_after_compaction_consumes_its_boundary() -> anyhow::Result<()> {
        let mut f = fixture();
        seed_chain(&mut f)?;
        let current_finality = block(7, "old-7");
        f.wc.finalize(&current_finality)?;

        let stale = f.wc.compute_rollback(&[block(9, "new-9")])?;
        assert_eq!(stale.resume_from, 6);
        assert_eq!(stale.reach_at_least, Some(7));

        assert!(matches!(
            f.db.perform_dataset_compaction(f.dataset_id, Some(100), Some(1.25), None)?,
            CompactionStatus::Ok(_)
        ));
        let compacted = stored_chunks(&f)?;
        assert_eq!(compacted.len(), 1);
        assert_eq!((compacted[0].first_block(), compacted[0].last_block()), (0, 9));

        let stale_replay = chunk_with_hashes(
            &f.db,
            6,
            "old-5",
            &["old-6", "old-7", "new-8", "new-9"].map(str::to_string)
        )?;
        let err = f.wc.new_chunk(None, &stale_replay).unwrap_err();
        let refusal = err
            .chain()
            .find_map(|e| e.downcast_ref::<UnapplicableFork>())
            .expect("a compacted rollback boundary is a typed fork refusal");
        assert_eq!(refusal.reason, UnapplicableForkReason::StaleRollbackBoundary);
        assert_eq!(stored_chunks(&f)?, compacted, "the stale replay must be atomic");

        let fresh = f.wc.compute_rollback(&[block(9, "new-9")])?;
        assert_eq!(fresh.resume_from, 0);
        assert_eq!(fresh.expected_parent_hash.as_deref(), Some("genesis"));
        assert_eq!(fresh.reach_at_least, Some(7));

        let mut replacement_hashes = hashes("old", 0, 7);
        replacement_hashes.extend(["new-8".to_string(), "new-9".to_string()]);
        let replacement = chunk_with_hashes(&f.db, 0, "genesis", &replacement_hashes)?;
        f.wc.new_chunk(None, &replacement)?;

        assert_eq!(stored_chunks(&f)?, vec![replacement]);
        assert_eq!(*f.fin_rx.borrow(), Some(current_finality));
        Ok(())
    }

    // Storage owns the replacement boundary verdict. In-memory watermarks are only a published
    // working copy, so even an impossible drift there must not downgrade a stale rollback to an
    // untyped overlap error or let finality resolve against the wrong owner.
    #[test]
    fn stale_boundary_guard_reads_storage_when_memory_head_is_missing() -> anyhow::Result<()> {
        let mut f = fixture();
        seed_chain(&mut f)?;
        assert!(matches!(
            f.db.perform_dataset_compaction(f.dataset_id, Some(100), Some(1.25), None)?,
            CompactionStatus::Ok(_)
        ));
        let compacted = stored_chunks(&f)?;
        assert_eq!((compacted[0].first_block(), compacted[0].last_block()), (0, 9));
        f.db.cleanup()?;

        f.wc.head = None;
        let stale_replay = linked_chunk(&f.db, 6, 12, "old-5", "new")?;
        let err = f.wc.new_chunk(None, &stale_replay).unwrap_err();
        let refusal = err
            .chain()
            .find_map(|e| e.downcast_ref::<UnapplicableFork>())
            .expect("the transaction must classify the stale stored boundary");

        assert_eq!(refusal.reason, UnapplicableForkReason::StaleRollbackBoundary);
        assert_eq!(stored_chunks(&f)?, compacted);
        assert_eq!(f.db.cleanup()?, 1, "the refused chunk's tables were not abandoned");
        Ok(())
    }

    // Nothing finalized is being replaced, so the ingest keeps its own flush boundaries.
    #[test]
    fn rollback_above_finalized_head_sets_no_reach_floor() -> anyhow::Result<()> {
        let mut f = fixture();
        seed_chain(&mut f)?;
        f.wc.finalize(&block(5, "old-5"))?;

        let rollback = f.wc.compute_rollback(&[block(9, "fork-9")])?;

        assert_eq!(rollback.resume_from, 6);
        assert_eq!(rollback.reach_at_least, None);
        Ok(())
    }

    // Retention trims whole chunks, so its floor can land inside the surviving one. Resuming at
    // that logical floor gives `insert_fork` an overlapping position and wedges the dataset the
    // same way the `fin + 1` clamp did (GAP-3).
    #[test]
    fn rollback_fallback_resumes_at_the_physical_window_start() -> anyhow::Result<()> {
        let mut f = fixture();
        seed_chain(&mut f)?;
        f.wc.finalize(&block(7, "old-7"))?;
        // Drops [0, 5] and keeps [6, 9] whole, with the logical floor at 7.
        f.wc.retain(7, None)?;
        assert_eq!(f.wc.start_block(), 7);

        let rollback = f.wc.compute_rollback(&[block(8, "fork-8")])?;

        assert_eq!(rollback.resume_from, 6);
        assert_eq!(rollback.expected_parent_hash.as_deref(), Some("old-5"));
        assert_eq!(rollback.reach_at_least, Some(7));
        Ok(())
    }

    // A fork that cannot reach up to finality is refused, never resumed below it.
    #[test]
    fn rollback_with_all_hints_below_finalized_head_is_refused() -> anyhow::Result<()> {
        let mut f = fixture();
        seed_chain(&mut f)?;
        f.wc.finalize(&block(7, "old-7"))?;

        let err =
            f.wc.compute_rollback(&[block(3, "fork-3"), block(5, "fork-5")])
                .unwrap_err();
        assert!(
            err.to_string().contains("hints_below_finalized_head"),
            "unexpected error: {err}"
        );
        Ok(())
    }

    // The validated range spans two stored chunks, so `validate_finalized_prefix` has to pull the
    // second one in mid-scan. A broken advance shows up here as a false divergence at the boundary.
    #[test]
    fn replacement_spanning_two_stored_chunks_reproducing_both_is_accepted() -> anyhow::Result<()> {
        let mut f = fixture();
        seed_chain(&mut f)?;
        assert_eq!(
            stored_chunks(&f)?.len(),
            2,
            "the scan must cross a stored chunk boundary"
        );
        let current_finalized = block(7, "old-7");
        f.wc.finalize(&current_finalized)?;

        let mut replayed = hashes("old", 0, 7);
        replayed.extend(hashes("new", 8, 9));
        let replacement = chunk_with_hashes(&f.db, 0, "genesis", &replayed)?;
        f.wc.new_chunk(None, &replacement)?;

        assert_eq!(stored_chunks(&f)?, vec![replacement.clone()]);
        assert_eq!(f.wc.head(), Some(&get_chunk_head(&replacement)));
        assert_eq!(*f.fin_rx.borrow(), Some(current_finalized));
        Ok(())
    }

    // The same layout with the divergence in the *second* stored chunk: reaching it at all proves
    // the cursor crossed the boundary, rather than running out of stored blocks at 6.
    #[test]
    fn replacement_spanning_two_stored_chunks_rewriting_the_second_is_rejected() -> anyhow::Result<()> {
        let mut f = fixture();
        let (first, second) = seed_chain(&mut f)?;
        let current_finalized = block(7, "old-7");
        f.wc.finalize(&current_finalized)?;

        let mut replayed = hashes("old", 0, 6);
        replayed.push("fork-7".to_string());
        replayed.extend(hashes("new", 8, 9));
        let err =
            f.wc.new_chunk(None, &chunk_with_hashes(&f.db, 0, "genesis", &replayed)?)
                .unwrap_err();

        assert!(
            format!("{err:#}").contains("expected finalized block 7#old-7"),
            "the divergence must be reported at 7, inside the second stored chunk: {err:#}"
        );
        assert_eq!(stored_chunks(&f)?, vec![first, second]);
        assert_eq!(*f.fin_rx.borrow(), Some(current_finalized));
        Ok(())
    }

    // A rollback resolved before a trim, committed after it. Without the window guard `insert_fork`
    // drops every surviving chunk — the head ends up below the retention floor and the trimmed
    // blocks are back.
    #[test]
    fn replacement_below_the_retained_window_is_refused() -> anyhow::Result<()> {
        let mut f = fixture();
        let (_, second) = seed_chain(&mut f)?;
        f.wc.retain(6, None)?;
        // Drain the trimmed chunk's table, so the count below is the refused chunk's alone.
        f.db.cleanup()?;

        let stale = linked_chunk(&f.db, 0, 9, "genesis", "new")?;
        let err = f.wc.new_chunk(None, &stale).unwrap_err();

        assert!(
            err.chain().any(|e| e.is::<UnapplicableFork>()),
            "a stale rollback must stay in the unapplicable_fork metric bucket: {err:#}"
        );
        assert_eq!(stored_chunks(&f)?, vec![second.clone()]);
        assert_eq!(f.wc.head(), Some(&get_chunk_head(&second)));
        assert_eq!(f.db.cleanup()?, 1, "the refused chunk's tables were not abandoned");
        assert_eq!(f.db.purge_orphan_dirty_tables()?, 0);
        Ok(())
    }

    // The boundary the guard must not reject: retention's logical floor sits inside the surviving
    // chunk, so `compute_rollback` resumes at that chunk's first block — below `start_block()`.
    #[test]
    fn replacement_at_the_physical_window_start_is_accepted() -> anyhow::Result<()> {
        let mut f = fixture();
        seed_chain(&mut f)?;
        f.wc.retain(7, None)?;
        assert_eq!(f.wc.start_block(), 7);

        let replacement = linked_chunk(&f.db, 6, 9, "old-5", "new")?;
        f.wc.new_chunk(None, &replacement)?;

        assert_eq!(stored_chunks(&f)?, vec![replacement.clone()]);
        assert_eq!(f.wc.head(), Some(&get_chunk_head(&replacement)));
        Ok(())
    }

    // The controller keeps a live ingest exactly while the head it builds on survives, so `retain`
    // reports what it did instead of leaving the caller to predict it from block numbers — which
    // missed every path that clears the window without moving the floor past the head.
    #[test]
    fn retain_reports_a_cleared_window() -> anyhow::Result<()> {
        let mut f = fixture();
        // Nothing stored yet: an ingest started at the old floor is already stale.
        assert!(!f.wc.retain(200, None)?);

        let mut f = fixture();
        seed_chain(&mut f)?;
        assert!(!f.wc.retain(100, None)?, "a floor above the head clears the window");

        let mut f = fixture();
        seed_chain(&mut f)?;
        assert!(
            !f.wc.retain(7, Some("fork-6".to_string()))?,
            "a parent hash mismatch inside the window clears it"
        );
        assert_eq!(f.wc.head(), None);
        Ok(())
    }

    #[test]
    fn retain_keeps_the_head_it_only_trims_behind() -> anyhow::Result<()> {
        let mut f = fixture();
        let (_, second) = seed_chain(&mut f)?;
        assert!(f.wc.retain(6, None)?);
        assert_eq!(f.wc.head(), Some(&get_chunk_head(&second)));
        Ok(())
    }

    // An emptied window leaves `list_chunks` with no bound to offer, so the floor guard has to fall
    // back to the logical floor — otherwise an ingest that outlived the clear writes below it.
    #[test]
    fn replacement_below_the_floor_of_an_empty_window_is_refused() -> anyhow::Result<()> {
        let mut f = fixture();
        assert!(!f.wc.retain(200, None)?);

        let stale = linked_chunk(&f.db, 100, 150, "genesis", "stale")?;
        let err = f.wc.new_chunk(None, &stale).unwrap_err();

        assert!(
            err.chain().any(|e| e.is::<UnapplicableFork>()),
            "a stale ingest must stay in the unapplicable_fork metric bucket: {err:#}"
        );
        assert_eq!(stored_chunks(&f)?, vec![]);
        assert_eq!(f.wc.head(), None);
        assert_eq!(f.db.cleanup()?, 1, "the refused chunk's tables were not abandoned");
        Ok(())
    }

    // An equivocation at the finalized height is refused, never absorbed.
    #[test]
    fn rollback_with_conflicting_hint_at_finalized_height_is_refused() -> anyhow::Result<()> {
        let mut f = fixture();
        seed_chain(&mut f)?;
        f.wc.finalize(&block(7, "old-7"))?;

        let err =
            f.wc.compute_rollback(&[block(7, "fork-7"), block(9, "fork-9")])
                .unwrap_err();
        assert!(
            err.to_string().contains("hint_conflicts_with_finalized_head"),
            "unexpected error: {err}"
        );
        Ok(())
    }

    // A push repeating the current floor is the common case, not the corner: answering it from the
    // window's emptiness restarts an untouched ingest, once per push on a fresh dataset.
    #[test]
    fn an_idempotent_trim_reports_no_loss() -> anyhow::Result<()> {
        let mut f = fixture();

        assert!(f.wc.retain(0, None)?, "the floor is already 0 on an empty dataset");
        assert!(
            !f.wc.retain(200, None)?,
            "a floor moving over an empty window does strand a live ingest"
        );
        assert!(f.wc.retain(200, None)?, "repeating that floor changes nothing");
        Ok(())
    }

    #[test]
    fn an_idempotent_trim_keeps_the_head_and_the_window() -> anyhow::Result<()> {
        let mut f = fixture();
        let (_, second) = seed_chain(&mut f)?;
        let current_finalized = block(7, "old-7");
        f.wc.finalize(&current_finalized)?;

        assert!(f.wc.retain(6, None)?);
        assert!(f.wc.retain(6, None)?);

        assert_eq!(stored_chunks(&f)?, vec![second.clone()]);
        assert_eq!(f.wc.head(), Some(&get_chunk_head(&second)));
        assert_eq!(*f.fin_rx.borrow(), Some(current_finalized));
        Ok(())
    }

    // `fin` anchors the finalized-prefix guard and every later `compute_rollback`, so a report
    // naming a hash the chunk does not carry is an equivocation, not a detail.
    #[test]
    fn finality_report_contradicting_the_chunk_is_rejected() -> anyhow::Result<()> {
        let mut f = fixture();
        let chunk = linked_chunk(&f.db, 0, 9, "genesis", "old")?;

        let err = f.wc.new_chunk(Some(&block(5, "forged-5")), &chunk).unwrap_err();

        assert!(
            err.chain().any(|e| e.is::<UnapplicableFork>()),
            "finality rejections must stay in the unapplicable_fork metric bucket: {err:#}"
        );
        assert_eq!(stored_chunks(&f)?, vec![]);
        assert_eq!(*f.fin_rx.borrow(), None);
        assert_eq!(f.db.cleanup()?, 1, "the refused chunk's tables were not abandoned");
        Ok(())
    }

    #[test]
    fn finality_report_matching_the_chunk_is_accepted() -> anyhow::Result<()> {
        let mut f = fixture();
        let chunk = linked_chunk(&f.db, 0, 9, "genesis", "old")?;

        f.wc.new_chunk(Some(&block(5, "old-5")), &chunk)?;

        assert_eq!(stored_chunks(&f)?, vec![chunk]);
        assert_eq!(*f.fin_rx.borrow(), Some(block(5, "old-5")));
        Ok(())
    }

    // Sparse numbering is ordinary (Solana slots): refusing would park the dataset, recording would
    // put a block nobody has behind FINALIZED-HEAD. The chunk still commits.
    #[test]
    fn finality_at_a_height_the_chunk_skips_is_ignored() -> anyhow::Result<()> {
        let mut f = fixture();
        let chunk = chunk_with_numbers(&f.db, "genesis", &[(4, "old-4".to_string()), (6, "old-6".to_string())])?;

        f.wc.new_chunk(Some(&block(5, "reported-5")), &chunk)?;

        assert_eq!(stored_chunks(&f)?, vec![chunk]);
        assert_eq!(*f.fin_rx.borrow(), None);
        Ok(())
    }

    // Missing from the replacement, so it looks like a hole — but stored history carries it and this
    // commit deletes the chunk holding it. A source may not finalize a block and evict it at once.
    #[test]
    fn finality_report_evicting_a_stored_block_is_rejected() -> anyhow::Result<()> {
        let mut f = fixture();
        let stored = chunk_with_numbers(
            &f.db,
            "genesis",
            &[
                (4, "old-4".to_string()),
                (5, "old-5".to_string()),
                (6, "old-6".to_string())
            ]
        )?;
        f.wc.new_chunk(None, &stored)?;
        let current_finalized = block(4, "old-4");
        f.wc.finalize(&current_finalized)?;

        // Same range, same finalized prefix up to 4, but block 5 is gone.
        let replacement = chunk_with_numbers(&f.db, "genesis", &[(4, "old-4".to_string()), (6, "new-6".to_string())])?;
        let err = f.wc.new_chunk(Some(&block(5, "old-5")), &replacement).unwrap_err();

        assert!(
            err.chain().any(|e| e.is::<UnapplicableFork>()),
            "finality rejections must stay in the unapplicable_fork metric bucket: {err:#}"
        );
        assert_eq!(stored_chunks(&f)?, vec![stored]);
        assert_eq!(*f.fin_rx.borrow(), Some(current_finalized));
        assert_eq!(f.db.cleanup()?, 1, "the refused chunk's tables were not abandoned");
        Ok(())
    }

    // The dominant arm: finality lags the tip, so the report usually names a height below the batch,
    // where stored history owns the hash and survives the commit.
    #[test]
    fn finality_below_the_chunk_contradicting_stored_history_is_rejected() -> anyhow::Result<()> {
        let mut f = fixture();
        let (first, second) = seed_chain(&mut f)?;

        let append = linked_chunk(&f.db, 10, 12, "old-9", "old")?;
        let err = f.wc.new_chunk(Some(&block(5, "forged-5")), &append).unwrap_err();

        assert!(
            err.chain().any(|e| e.is::<UnapplicableFork>()),
            "finality rejections must stay in the unapplicable_fork metric bucket: {err:#}"
        );
        assert_eq!(stored_chunks(&f)?, vec![first, second]);
        assert_eq!(*f.fin_rx.borrow(), None);
        assert_eq!(f.db.cleanup()?, 1, "the refused chunk's tables were not abandoned");
        Ok(())
    }

    #[test]
    fn finality_below_the_chunk_matching_stored_history_is_accepted() -> anyhow::Result<()> {
        let mut f = fixture();
        let (first, second) = seed_chain(&mut f)?;

        let append = linked_chunk(&f.db, 10, 12, "old-9", "old")?;
        f.wc.new_chunk(Some(&block(5, "old-5")), &append)?;

        assert_eq!(stored_chunks(&f)?, vec![first, second, append]);
        assert_eq!(*f.fin_rx.borrow(), Some(block(5, "old-5")));
        Ok(())
    }

    // The direct FINALIZE path checked the hash only at the head chunk's last block.
    #[test]
    fn finalize_rejects_a_report_contradicting_stored_history() -> anyhow::Result<()> {
        let mut f = fixture();
        seed_chain(&mut f)?;

        let err = f.wc.finalize(&block(5, "forged-5")).unwrap_err();

        assert!(
            err.chain().any(|e| e.is::<UnapplicableFork>()),
            "finality rejections must stay in the unapplicable_fork metric bucket: {err:#}"
        );
        assert_eq!(*f.fin_rx.borrow(), None);
        Ok(())
    }

    // The WP-8 exception on the direct path: a height stored history skips is ignored, not a fault.
    #[test]
    fn finalize_ignores_a_report_at_a_height_stored_history_skips() -> anyhow::Result<()> {
        let mut f = fixture();
        let stored = chunk_with_numbers(&f.db, "genesis", &[(4, "old-4".to_string()), (6, "old-6".to_string())])?;
        f.wc.new_chunk(None, &stored)?;

        f.wc.finalize(&block(5, "reported-5"))?;

        assert_eq!(*f.fin_rx.borrow(), None);
        assert_eq!(stored_chunks(&f)?, vec![stored]);
        Ok(())
    }

    // INV-5/WP-8: retention's logical floor can sit inside the first physical chunk. A report
    // naming the chunk's retained overshoot still lies outside the dataset window and is ignored.
    #[test]
    fn finalize_ignores_a_report_below_the_retention_floor() -> anyhow::Result<()> {
        let mut f = fixture();
        let (_, retained) = seed_chain(&mut f)?;
        f.wc.retain(7, None)?;

        f.wc.finalize(&block(6, "old-6"))?;

        assert_eq!(stored_chunks(&f)?, vec![retained]);
        assert_eq!(*f.fin_rx.borrow(), None);
        Ok(())
    }

    // The same floor applies when finality is composed with a replacement at the first physical
    // chunk boundary: the chunk commits, but its below-window finality report does not.
    #[test]
    fn composed_finality_below_the_retention_floor_is_ignored() -> anyhow::Result<()> {
        let mut f = fixture();
        seed_chain(&mut f)?;
        f.wc.retain(7, None)?;
        let replacement = linked_chunk(&f.db, 6, 9, "old-5", "new")?;

        f.wc.new_chunk(Some(&block(6, "new-6")), &replacement)?;

        assert_eq!(stored_chunks(&f)?, vec![replacement]);
        assert_eq!(*f.fin_rx.borrow(), None);
        Ok(())
    }

    // The same report once a real height is named: the drop above must not have poisoned anything.
    #[test]
    fn finality_recovers_after_a_skipped_height_report() -> anyhow::Result<()> {
        let mut f = fixture();
        let chunk = chunk_with_numbers(&f.db, "genesis", &[(4, "old-4".to_string()), (6, "old-6".to_string())])?;
        f.wc.new_chunk(Some(&block(5, "reported-5")), &chunk)?;

        let next = linked_chunk(&f.db, 7, 9, "old-6", "old")?;
        f.wc.new_chunk(Some(&block(6, "old-6")), &next)?;

        assert_eq!(*f.fin_rx.borrow(), Some(block(6, "old-6")));
        Ok(())
    }
}
