use crate::errors::{BlockItemIsNotAvailable, QueryKindMismatch};
use crate::errors::{BlockRangeMissing, QueryIsAboveTheHead};
use crate::metrics::{QUERIED_BLOCKS, QUERIED_CHUNKS};
use crate::query::static_snapshot::{StaticChunkIterator, StaticChunkReader, StaticSnapshot};
use crate::types::{DBRef, DatasetKind};
use anyhow::{anyhow, bail, ensure};
use bytes::{BufMut, Bytes, BytesMut};
use flate2::Compression;
use flate2::write::GzEncoder;
use sqd_primitives::{BlockNumber, BlockRef};
use sqd_query::{JsonLinesWriter, Plan, Query};
use sqd_storage::db::{Chunk as StorageChunk, DatasetId};
use std::io::Write;

struct LeftOver {
    chunk: StaticChunkReader,
    next_block: BlockNumber,
}

pub struct RunningQueryStats {
    pub chunks_read: u64,
    pub blocks_read: u64,
}

impl RunningQueryStats {
    pub fn new() -> Self {
        Self {
            chunks_read: 0,
            blocks_read: 0,
        }
    }

    pub fn report_metrics(&self, dataset_id: &DatasetId) {
        let labels = vec![("dataset_id".to_owned(), dataset_id.as_str().to_owned())];

        QUERIED_BLOCKS
            .get_or_create(&labels)
            .observe(self.blocks_read as f64);
        QUERIED_CHUNKS
            .get_or_create(&labels)
            .observe(self.chunks_read as f64);
    }
}

pub struct RunningQuery {
    plan: Plan,
    last_block: Option<BlockNumber>,
    left_over: Option<LeftOver>,
    next_chunk: Option<anyhow::Result<StorageChunk>>,
    chunk_iterator: StaticChunkIterator,
    finalized_head: Option<BlockRef>,
    buf: GzEncoder<bytes::buf::Writer<BytesMut>>,
    stats: RunningQueryStats,
}

impl RunningQuery {
    pub fn new(
        db: DBRef,
        dataset_id: DatasetId,
        query: &Query,
        only_finalized: bool,
    ) -> anyhow::Result<Self> {
        let snapshot = StaticSnapshot::new(db);

        let finalized_head = match snapshot.get_label(dataset_id)? {
            None => bail!("dataset {} does not exist", dataset_id),
            Some(label) => {
                let kind = DatasetKind::from_query(query);
                ensure!(
                    kind.storage_kind() == label.kind(),
                    QueryKindMismatch {
                        query_kind: kind.storage_kind(),
                        dataset_kind: label.kind()
                    }
                );
                label.finalized_head().cloned()
            }
        };

        let mut chunk_iterator =
            StaticChunkIterator::new(snapshot, dataset_id, query.first_block(), None);

        let Some(first_chunk) = chunk_iterator.next().transpose()? else {
            bail!(QueryIsAboveTheHead {
                finalized_head: None
            })
        };

        ensure!(
            first_chunk.first_block() <= query.first_block(),
            BlockRangeMissing {
                first_block: query.first_block(),
                last_block: first_chunk.first_block() - 1
            }
        );

        let mut stats = RunningQueryStats::new();
        stats.chunks_read += 1;

        let plan = if query.first_block() == first_chunk.first_block() {
            if let Some(parent_hash) = query.parent_block_hash() {
                ensure!(
                    parent_hash == first_chunk.parent_block_hash(),
                    sqd_query::UnexpectedBaseBlock {
                        prev_blocks: vec![BlockRef {
                            number: first_chunk.first_block().saturating_sub(1),
                            hash: first_chunk.parent_block_hash().to_string()
                        }],
                        expected_hash: parent_hash.to_string()
                    }
                );
            }
            let mut plan = query.compile();
            plan.set_first_block(None);
            plan.set_parent_block_hash(None);
            plan
        } else {
            query.compile()
        };

        let last_block = if only_finalized {
            // Cap the query's last_block to the finalized head
            if let Some(finalized_head) = &finalized_head {
                let capped_last = query
                    .last_block()
                    .map(|end| end.min(finalized_head.number))
                    .or(Some(finalized_head.number));
                capped_last
            } else {
                anyhow::bail!("Finalized head is not available yet");
            }
        } else {
            query.last_block()
        };

        Ok(Self {
            plan,
            last_block,
            left_over: None,
            next_chunk: Some(Ok(first_chunk)),
            chunk_iterator,
            finalized_head,
            buf: GzEncoder::new(BytesMut::new().writer(), Compression::fast()),
            stats,
        })
    }

    pub fn take_finalized_head(&mut self) -> Option<BlockRef> {
        self.finalized_head.take()
    }

    pub fn stats(&self) -> &RunningQueryStats {
        &self.stats
    }

    pub fn buffered_bytes(&self) -> usize {
        self.buf.get_ref().get_ref().len()
    }

    pub fn take_buffered_bytes(&mut self) -> Bytes {
        self.buf.get_mut().get_mut().split().freeze()
    }

    pub fn finish(self) -> Bytes {
        self.buf
            .finish()
            .expect("IO errors are not possible")
            .into_inner()
            .freeze()
    }

    pub fn has_next_chunk(&self) -> bool {
        self.next_chunk.is_some() || self.left_over.is_some()
    }

    /// Query the next chunk and write results to buffer.
    ///
    /// Everything written to the buffer is always well-formed.
    pub fn write_next_chunk(&mut self) -> anyhow::Result<()> {
        let (chunk, first_block_queried) = if let Some(left_over) = self.left_over.take() {
            let first_block = left_over.next_block;
            self.plan.set_first_block(first_block);
            (left_over.chunk, first_block)
        } else {
            let storage_chunk = self.next_chunk()?;
            // Increment chunks_downloaded when we fetch a new chunk
            self.stats.chunks_read += 1;
            let chunk = self
                .chunk_iterator
                .snapshot()
                .create_chunk_reader(storage_chunk);
            let first_block = chunk.first_block();
            (chunk, first_block)
        };

        if self
            .last_block
            .map_or(false, |end| end < chunk.last_block())
        {
            let last_block = self.last_block;
            self.plan.set_last_block(last_block);
        } else {
            self.plan.set_last_block(None);
        }

        let query_result = chunk
            .with_reader(|reader| self.plan.execute(reader))
            .map_err(|err| {
                if let Some(err) = err.downcast_ref::<sqd_query::TableDoesNotExist>() {
                    return anyhow!(BlockItemIsNotAvailable {
                        item_name: err.table_name,
                        first_block: chunk.first_block(),
                        last_block: chunk.last_block()
                    });
                }
                err
            });

        // no matter what, we are moving to the next chunk
        self.plan.set_first_block(None);
        self.plan.set_parent_block_hash(None);

        let Some(mut block_writer) = query_result? else {
            return Ok(());
        };

        let blocks_written = block_writer.last_block() - first_block_queried + 1;
        self.stats.blocks_read += blocks_written;

        if chunk.last_block() > block_writer.last_block()
            && self
                .last_block
                .map_or(true, |end| end > block_writer.last_block())
        {
            self.left_over = Some(LeftOver {
                chunk,
                next_block: block_writer.last_block() + 1,
            })
        }

        let mut json_lines_writer = JsonLinesWriter::new(&mut self.buf);

        json_lines_writer
            .write_blocks(&mut block_writer)
            .expect("IO errors are not possible");

        json_lines_writer
            .finish()
            .expect("IO errors are not possible");

        self.buf.flush().expect("IO errors are not possible");

        Ok(())
    }

    fn next_chunk(&mut self) -> anyhow::Result<StorageChunk> {
        let Some(chunk) = self.next_chunk.take().transpose()? else {
            bail!("no more chunks left")
        };

        self.next_chunk = self
            .chunk_iterator
            .next()
            .transpose()
            .map(|maybe_next_chunk| {
                let next_chunk = maybe_next_chunk?;
                let is_continuous = chunk.last_block() + 1 == next_chunk.first_block();
                let is_requested = self
                    .last_block
                    .map_or(true, |end| next_chunk.first_block() <= end);
                if is_continuous && is_requested {
                    Some(next_chunk)
                } else {
                    None
                }
            })
            .transpose();

        Ok(chunk)
    }

    /// Size of the next chunk (in blocks)
    pub fn next_chunk_size(&self) -> usize {
        self.left_over
            .as_ref()
            .map(|lo| lo.chunk.last_block() - lo.chunk.first_block() + 1)
            .or_else(|| {
                let chunk = self.next_chunk.as_ref()?.as_ref().ok()?;
                let size = chunk.last_block() - chunk.first_block() + 1;
                Some(size)
            })
            .unwrap_or(0) as usize
    }
}
