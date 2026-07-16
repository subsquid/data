use anyhow::{anyhow, bail, ensure};
use sqd_primitives::{BlockNumber, BlockRef};
use sqd_storage::db::{Chunk as StorageChunk, Chunk, DatasetId, HashIndexWriteMetrics};
use tracing::{debug, field::valuable, info, instrument, warn};

use crate::types::{DBRef, DatasetKind};

/// Source position selected after resolving a fork against stored history.
///
/// `resume_from` always lands on a stored chunk boundary, so it may sit at or below the
/// finalized head when the fork's common ancestor is inside a finality-straddling chunk. The
/// finalized prefix is protected on the write path instead (see [`WriteController::new_chunk`]),
/// which rejects a replacement that would rewrite an already-finalized block.
#[derive(Debug)]
pub struct Rollback {
    /// Lowest block number the source may return after resolving the fork.
    pub resume_from: BlockNumber,
    /// Hash that must anchor the first returned block, when an anchor is known.
    pub expected_parent_hash: Option<String>
}

#[derive(Debug)]
pub struct WriteController {
    db: DBRef,
    dataset_id: DatasetId,
    dataset_kind: DatasetKind,
    first_block: BlockNumber,
    parent_block_hash: Option<String>,
    first_chunk_head: Option<BlockRef>,
    head: Option<BlockRef>,
    finalized_head: Option<BlockRef>
}

impl WriteController {
    pub fn new(db: DBRef, dataset_id: DatasetId, dataset_kind: DatasetKind) -> anyhow::Result<Self> {
        db.create_dataset_if_not_exists(dataset_id, dataset_kind.storage_kind())?;

        let snapshot = db.snapshot();
        let label = snapshot.get_label(dataset_id)?;
        let first_chunk = snapshot.get_first_chunk(dataset_id)?;
        let last_chunk = snapshot.get_last_chunk(dataset_id)?;

        Ok(Self {
            db: db.clone(),
            dataset_id,
            dataset_kind,
            first_block: first_chunk.as_ref().map_or(0, |c| c.first_block()),
            parent_block_hash: first_chunk.as_ref().map(|c| c.last_block_hash().to_string()),
            first_chunk_head: first_chunk.as_ref().map(get_chunk_head),
            head: last_chunk.as_ref().map(get_chunk_head),
            finalized_head: label.and_then(|l| l.finalized_head().cloned())
        })
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

    pub fn finalized_head(&self) -> Option<&BlockRef> {
        self.finalized_head.as_ref()
    }

    pub fn first_chunk_head(&self) -> Option<&BlockRef> {
        self.first_chunk_head.as_ref()
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
                ensure!(
                    prev[pos].hash == finalized_head.hash,
                    "fork hint at finalized block {} conflicts: expected {}, got {}",
                    finalized_head.number,
                    finalized_head.hash,
                    prev[pos].hash
                );
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
                        resume_from: b.number + 1,
                        expected_parent_hash: Some(b.hash.clone())
                    });
                }
            } else {
                return Ok(Rollback {
                    resume_from: head.last_block() + 1,
                    expected_parent_hash: Some(head.last_block_hash().to_string())
                });
            }
        }

        Ok(Rollback {
            resume_from: self.first_block,
            expected_parent_hash: self.parent_block_hash.clone()
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
                self.head = Some(get_chunk_head(&head));
                self.finalized_head = finalized_head;
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
        self.head = None;
        self.finalized_head = None;
        self.first_chunk_head = None;
    }

    pub fn retain(
        &mut self,
        from_block: BlockNumber,
        parent_block_hash: Option<String>,
        metrics: &mut HashIndexWriteMetrics
    ) -> anyhow::Result<()> {
        self._retain(from_block, parent_block_hash, true, metrics)
    }

    pub fn init_retention(
        &mut self,
        from_block: BlockNumber,
        parent_block_hash: Option<String>,
        metrics: &mut HashIndexWriteMetrics
    ) -> anyhow::Result<()> {
        self._retain(from_block, parent_block_hash, false, metrics)
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
            self.finalized_head = Some(new_head);
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
    pub fn new_chunk(
        &mut self,
        finalized_head: Option<&BlockRef>,
        chunk: &StorageChunk,
        metrics: &mut HashIndexWriteMetrics
    ) -> anyhow::Result<()> {
        // FIXME: accept self.first_block rollback limit
        let finalized_head = self
            .db
            .update_dataset_with_hash_index_metrics(self.dataset_id, metrics, |tx| {
                let current_finalized_head = tx.label().finalized_head().cloned();

                // A fork whose common ancestor sits inside a finality-straddling chunk resumes at
                // that chunk's boundary, at or below `fin`, so the replacement can reach into the
                // finalized region. Permit it only when it reproduces the finalized block exactly:
                // it must span `fin` (else the swap would momentarily drop the finalized block) and
                // carry `fin`'s hash unchanged. A mismatch is a source equivocating below its own
                // finality and is refused, leaving the accepted prefix intact (INV-12/13/14).
                if let Some(current) = current_finalized_head.as_ref()
                    && chunk.first_block() <= current.number
                {
                    ensure!(
                        chunk.last_block() >= current.number,
                        "replacement chunk {}-{} would drop finalized block {} without reproducing it",
                        chunk.first_block(),
                        chunk.last_block(),
                        current.number
                    );
                    if let Err(actual) = tx.validate_parent_block_hash(chunk, current.number + 1, &current.hash)? {
                        bail!(
                            "replacement would rewrite finalized block {}: expected hash {}, got {}",
                            current.number,
                            current.hash,
                            actual
                        );
                    }
                }

                let new_finalized_head = match (finalized_head, current_finalized_head.as_ref()) {
                    (Some(new), Some(current)) if new.number < current.number => Some(current.clone()),
                    (Some(new), Some(current)) if new.number == current.number => {
                        ensure!(
                            new.hash == current.hash,
                            "finalized hash mismatch at block {}: expected {}, got {}",
                            current.number,
                            current.hash,
                            new.hash
                        );
                        Some(current.clone())
                    }
                    (Some(new), _) => Some(new.clone()),
                    (None, Some(current)) => Some(current.clone()),
                    (None, None) => None
                };

                let new_finalized_head = new_finalized_head.map(|head| {
                    if head.number < chunk.last_block() {
                        head
                    } else {
                        get_chunk_head(&chunk)
                    }
                });

                tx.set_finalized_head(new_finalized_head.clone());
                tx.insert_fork(chunk)?;
                Ok(new_finalized_head)
            })?;

        debug!(finalized_head = valuable(&finalized_head), "saved new chunk");

        self.finalized_head = finalized_head;
        self.head = Some(get_chunk_head(&chunk));
        if self
            .first_chunk_head
            .as_ref()
            .map_or(true, |h| chunk.first_block() <= h.number)
        {
            self.first_chunk_head = self.head.clone();
        }

        Ok(())
    }
}

fn get_chunk_head(chunk: &Chunk) -> BlockRef {
    BlockRef {
        number: chunk.last_block(),
        hash: chunk.last_block_hash().to_string()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use sqd_storage::db::{DatabaseSettings, HashIndexWriteMetrics};

    use super::*;

    fn chunk(first_block: BlockNumber, last_block: BlockNumber, parent_hash: &str, last_hash: &str) -> Chunk {
        Chunk::V1 {
            first_block,
            last_block,
            last_block_hash: last_hash.to_string(),
            parent_block_hash: parent_hash.to_string(),
            first_block_time: None,
            last_block_time: None,
            tables: Default::default()
        }
    }

    fn setup() -> anyhow::Result<(tempfile::TempDir, DBRef, DatasetId, WriteController)> {
        let db_dir = tempfile::tempdir()?;
        let db = Arc::new(DatabaseSettings::default().open(db_dir.path())?);
        let dataset_id = DatasetId::from_str("finality-regression");
        let write = WriteController::new(db.clone(), dataset_id, DatasetKind::Evm)?;
        Ok((db_dir, db, dataset_id, write))
    }

    fn seed_chain(write: &mut WriteController, metrics: &mut HashIndexWriteMetrics) -> anyhow::Result<(Chunk, Chunk)> {
        let first = chunk(0, 5, "genesis", "old-5");
        let second = chunk(6, 9, "old-5", "old-9");
        write.new_chunk(None, &first, metrics)?;
        write.new_chunk(None, &second, metrics)?;
        Ok((first, second))
    }

    #[test]
    fn replacement_rewriting_the_finalized_block_is_rejected() -> anyhow::Result<()> {
        // Arrange: block 5 is finalized on the original chain.
        let (_db_dir, db, dataset_id, mut write) = setup()?;
        let mut metrics = HashIndexWriteMetrics::default();
        let (first, second) = seed_chain(&mut write, &mut metrics)?;
        let current_finalized = BlockRef {
            number: 5,
            hash: "old-5".to_string()
        };
        write.finalize(&current_finalized)?;

        // Act: an honest reorg would resume at a boundary below fin, but this replacement reaches
        // the finalized height carrying a different hash there — a source equivocating below finality.
        let replacement = chunk(0, 5, "other-genesis", "new-5");
        let new_finalized = BlockRef {
            number: 5,
            hash: "new-5".to_string()
        };
        let result = write.new_chunk(Some(&new_finalized), &replacement, &mut metrics);

        // Assert (INV-12/13/14): the finalized block is immutable and rejection is atomic.
        assert!(
            result.is_err(),
            "replacement rewriting the finalized block was accepted"
        );
        assert_eq!(write.finalized_head(), Some(&current_finalized));

        let snapshot = db.snapshot();
        let stored = snapshot
            .list_chunks(dataset_id, 0, None)
            .collect::<anyhow::Result<Vec<_>>>()?;
        assert_eq!(stored, vec![first, second]);
        assert_eq!(
            snapshot
                .get_label(dataset_id)?
                .and_then(|label| label.finalized_head().cloned()),
            Some(current_finalized)
        );
        Ok(())
    }

    #[test]
    fn composed_finality_rejects_hash_change_at_fixed_height() -> anyhow::Result<()> {
        // Arrange: the finalized block is the boundary of the first stored chunk.
        let (_db_dir, db, dataset_id, mut write) = setup()?;
        let mut metrics = HashIndexWriteMetrics::default();
        let (first, second) = seed_chain(&mut write, &mut metrics)?;
        let current_finalized = BlockRef {
            number: 5,
            hash: "old-5".to_string()
        };
        write.finalize(&current_finalized)?;

        // Act: a normal append carries a conflicting hash at the already-finalized height.
        let append = chunk(10, 12, "old-9", "old-12");
        let conflicting_finality = BlockRef {
            number: 5,
            hash: "other-5".to_string()
        };
        let result = write.new_chunk(Some(&conflicting_finality), &append, &mut metrics);

        // Assert (INV-12): fixed-height finality is immutable and the append is not partially committed.
        assert!(result.is_err(), "finalized hash changed at a fixed height");
        assert_eq!(write.finalized_head(), Some(&current_finalized));

        let snapshot = db.snapshot();
        let stored = snapshot
            .list_chunks(dataset_id, 0, None)
            .collect::<anyhow::Result<Vec<_>>>()?;
        assert_eq!(stored, vec![first, second]);
        assert_eq!(
            snapshot
                .get_label(dataset_id)?
                .and_then(|label| label.finalized_head().cloned()),
            Some(current_finalized)
        );
        Ok(())
    }

    #[test]
    fn replacement_below_finalized_that_misses_it_is_rejected() -> anyhow::Result<()> {
        // Arrange: finality lies strictly inside [6, 9].
        let (_db_dir, db, dataset_id, mut write) = setup()?;
        let mut metrics = HashIndexWriteMetrics::default();
        let (first, second) = seed_chain(&mut write, &mut metrics)?;
        let current_finalized = BlockRef {
            number: 7,
            hash: "old-7".to_string()
        };
        write.finalize(&current_finalized)?;

        // Act: a partial replacement reaches below the finalized height but stops before reproducing
        // it — accepting it would drop the finalized block until a later flush caught up.
        let replacement = chunk(6, 6, "old-5", "new-6");
        let result = write.new_chunk(None, &replacement, &mut metrics);

        // Assert: refuse so the finalized block is never transiently absent, atomically.
        assert!(
            result.is_err(),
            "replacement that drops the finalized block was accepted"
        );
        assert_eq!(write.finalized_head(), Some(&current_finalized));
        let snapshot = db.snapshot();
        let stored = snapshot
            .list_chunks(dataset_id, 0, None)
            .collect::<anyhow::Result<Vec<_>>>()?;
        assert_eq!(stored, vec![first, second]);
        Ok(())
    }

    #[test]
    fn rollback_without_matching_hints_resumes_from_window_start() -> anyhow::Result<()> {
        // Arrange: finality lies inside a storage chunk and every supplied fork hint mismatches.
        let (_db_dir, _db, _dataset_id, mut write) = setup()?;
        let mut metrics = HashIndexWriteMetrics::default();
        seed_chain(&mut write, &mut metrics)?;
        write.finalize(&BlockRef {
            number: 4,
            hash: "old-4".to_string()
        })?;
        let hints = vec![
            BlockRef {
                number: 5,
                hash: "fork-5".to_string()
            },
            BlockRef {
                number: 9,
                hash: "fork-9".to_string()
            },
        ];

        // Act.
        let rollback = write.compute_rollback(&hints)?;

        // Assert: with no matching boundary the fallback is the window start; the finalized prefix is
        // guarded on the write path (new_chunk), not by clamping the resume position.
        assert_eq!(rollback.resume_from, write.start_block());
        assert_eq!(rollback.expected_parent_hash, None);
        Ok(())
    }

    #[test]
    fn rollback_from_straddling_chunk_resumes_at_chunk_boundary() -> anyhow::Result<()> {
        // Arrange: finality lies strictly inside [6, 9], and the lowest hint is in that chunk.
        let (_db_dir, _db, _dataset_id, mut write) = setup()?;
        let mut metrics = HashIndexWriteMetrics::default();
        seed_chain(&mut write, &mut metrics)?;
        write.finalize(&BlockRef {
            number: 7,
            hash: "old-7".to_string()
        })?;
        let hints = vec![BlockRef {
            number: 8,
            hash: "fork-8".to_string()
        }];

        // Act: the hint mismatches inside the chunk that straddles finality.
        let rollback = write.compute_rollback(&hints)?;

        // Assert: resume at the straddling chunk's lower boundary (block 6, below fin); new_chunk then
        // verifies the finalized block survives the whole-chunk rewrite. Previously this clamped to 8
        // (fin + 1), a mid-chunk position insert_fork could not satisfy — the wedge.
        assert_eq!(rollback.resume_from, 6);
        assert_eq!(rollback.expected_parent_hash.as_deref(), Some("old-5"));
        Ok(())
    }

    #[test]
    fn rollback_with_all_hints_below_finalized_head_is_refused() -> anyhow::Result<()> {
        // Arrange: finality at 7; every fork hint lies strictly below it.
        let (_db_dir, _db, _dataset_id, mut write) = setup()?;
        let mut metrics = HashIndexWriteMetrics::default();
        seed_chain(&mut write, &mut metrics)?;
        write.finalize(&BlockRef {
            number: 7,
            hash: "old-7".to_string()
        })?;
        let hints = vec![
            BlockRef {
                number: 3,
                hash: "fork-3".to_string()
            },
            BlockRef {
                number: 5,
                hash: "fork-5".to_string()
            },
        ];

        // Act + Assert: a fork that cannot reach up to finality is refused, never resumed below it.
        let err = write.compute_rollback(&hints).unwrap_err();
        assert!(
            err.to_string().contains("below finalized head"),
            "unexpected error: {err}"
        );
        Ok(())
    }

    #[test]
    fn rollback_with_conflicting_hint_at_finalized_height_is_refused() -> anyhow::Result<()> {
        // Arrange: finality at 7; a hint sits exactly at 7 but disagrees on its hash.
        let (_db_dir, _db, _dataset_id, mut write) = setup()?;
        let mut metrics = HashIndexWriteMetrics::default();
        seed_chain(&mut write, &mut metrics)?;
        write.finalize(&BlockRef {
            number: 7,
            hash: "old-7".to_string()
        })?;
        let hints = vec![
            BlockRef {
                number: 7,
                hash: "fork-7".to_string()
            },
            BlockRef {
                number: 9,
                hash: "fork-9".to_string()
            },
        ];

        // Act + Assert: an equivocation at the finalized height is refused, never absorbed.
        let err = write.compute_rollback(&hints).unwrap_err();
        assert!(err.to_string().contains("finalized block 7"), "unexpected error: {err}");
        Ok(())
    }
}
