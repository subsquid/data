use std::io::Write;
use std::time::Instant;
use crate::query::static_snapshot::{StaticChunkIterator, StaticChunkReader, StaticSnapshot};
use crate::query::user_error::QueryKindMismatch;
use crate::types::{DBRef, DatasetKind};
use anyhow::{bail, ensure};
use bytes::{BufMut, Bytes, BytesMut};
use flate2::Compression;
use flate2::write::GzEncoder;
use sqd_primitives::{BlockNumber, BlockRef};
use sqd_query::{JsonLinesWriter, Plan, Query};
use sqd_storage::db::{Chunk as StorageChunk, DatasetId};


struct LeftOver {
    chunk: StaticChunkReader,
    next_block: BlockNumber
}


pub struct QueryRunner {
    plan: Option<Plan>,
    last_block: Option<BlockNumber>,
    left_over: Option<LeftOver>,
    next_chunk: Option<StorageChunk>,
    chunk_iterator: StaticChunkIterator,
    finalized_head: Option<BlockRef>,
    buf: GzEncoder<bytes::buf::Writer<BytesMut>>
}


impl QueryRunner {
    pub fn new(
        db: DBRef,
        dataset_id: DatasetId,
        query: &Query
    ) -> anyhow::Result<Self>
    {
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

        let mut chunk_iterator = StaticChunkIterator::new(
            snapshot,
            dataset_id,
            query.first_block(),
            None
        );

        let (first_chunk, plan) = match chunk_iterator.next().transpose()? {
            None => (None, None),
            Some(chunk) => {
                ensure!(
                    chunk.first_block() <= query.first_block(),
                    "blocks from {} to {} are missed in the database",
                    query.first_block(),
                    chunk.first_block()
                );

                let plan = if query.first_block() == chunk.first_block() {
                    if let Some(parent_hash) = query.parent_block_hash() {
                        ensure!(
                            parent_hash == chunk.parent_block_hash(),
                            sqd_query::UnexpectedBaseBlock {
                                prev_blocks: vec![BlockRef {
                                    number: chunk.first_block().saturating_sub(1),
                                    hash: chunk.parent_block_hash().to_string()
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
                (Some(chunk), Some(plan))
            }
        };

        let buf_capacity = if plan.is_some() {
            1024 * 1024
        } else {
            0
        };

        Ok(Self {
            plan,
            last_block: query.last_block(),
            left_over: None,
            next_chunk: first_chunk,
            chunk_iterator,
            finalized_head,
            buf: GzEncoder::new(
                BytesMut::with_capacity(buf_capacity).writer(),
                Compression::default()
            )
        })
    }

    pub fn finalized_head(&self) -> Option<&BlockRef> {
        self.finalized_head.as_ref()
    }

    pub fn has_next_pack(&self) -> bool {
        self.next_chunk.is_some() || self.left_over.is_some()
    }

    pub fn next_pack(&mut self) -> anyhow::Result<Bytes> {
        ensure!(self.has_next_pack());

        let start = Instant::now();
        let mut step = 0;
        
        loop {
            step += 1;
            self.write_next_chunk()?;

            if !self.has_next_pack() {
                let dummy_buf = GzEncoder::new(BytesMut::new().writer(), Compression::default());
                let bytes = std::mem::replace(&mut self.buf, dummy_buf)
                    .finish()
                    .expect("IO errors are not possible")
                    .into_inner()
                    .freeze();
                return Ok(bytes)
            }

            if self.buf.get_ref().get_ref().len() > 256 * 1024 || worked_long_enough(start, step) {
                let bytes = self.buf.get_mut().get_mut().split().freeze();
                return Ok(bytes)
            }
        }
    }

    pub fn finish(self) -> Bytes {
        self.buf
            .finish()
            .expect("IO errors are not possible")
            .into_inner()
            .freeze()
    }

    fn write_next_chunk(&mut self) -> anyhow::Result<()> {
        let chunk = if let Some(left_over) = self.left_over.take() {
            self.plan_mut().set_first_block(left_over.next_block);
            left_over.chunk
        } else {
            let chunk = self.next_chunk()?;
            self.chunk_iterator.snapshot().create_chunk_reader(chunk)
        };

        if self.last_block.map_or(false, |end| end < chunk.last_block()) {
            let last_block = self.last_block;
            self.plan_mut().set_last_block(last_block);
        } else {
            self.plan_mut().set_last_block(None);
        }

        let query_result = chunk.with_reader(|reader| self.plan().execute(reader));

        // no matter what, we are moving to the next chunk
        self.plan_mut().set_first_block(None);
        self.plan_mut().set_parent_block_hash(None);

        let mut block_writer = query_result?;

        if chunk.last_block() > block_writer.last_block()
            && self.last_block.map_or(true, |end| end > block_writer.last_block())
        {
            self.left_over = Some(LeftOver {
                chunk,
                next_block: block_writer.last_block() + 1
            })
        }

        JsonLinesWriter::new(&mut self.buf)
            .write_blocks(&mut block_writer)
            .expect("IO errors are not possible");

        self.buf.flush().expect("IO errors are not possible");

        Ok(())
    }

    fn next_chunk(&mut self) -> anyhow::Result<StorageChunk> {
        let chunk = self.next_chunk.take().expect("no more chunks left");

        self.next_chunk = self.chunk_iterator.next()
            .transpose()
            .map(|maybe_chunk| {
                let next_chunk = maybe_chunk?;
                let is_continuous = chunk.last_block() + 1 == next_chunk.first_block();
                let is_requested = self.last_block.map_or(true, |end| {
                    next_chunk.first_block() <= end
                });
                (is_continuous && is_requested).then_some(next_chunk)
            })?;

        Ok(chunk)
    }

    fn plan(&self) -> &Plan {
        self.plan.as_ref().unwrap()
    }

    fn plan_mut(&mut self) -> &mut Plan {
        self.plan.as_mut().unwrap()
    }
}


fn worked_long_enough(start: Instant, steps: usize) -> bool {
    let time = start.elapsed().as_millis();
    let next_eta = time + time / steps as u128;
    next_eta > 100
}