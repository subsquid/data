use std::{io::Write, sync::Arc};

use anyhow::{anyhow, bail, ensure};
use bytes::{BufMut, Bytes, BytesMut};
use flate2::{Compression, write::GzEncoder};
use sqd_primitives::{BlockNumber, BlockRef};
use sqd_query::{JsonLinesWriter, Plan, Query};
use sqd_query_engine::metadata::DatasetDescription;
use sqd_storage::db::{Chunk as StorageChunk, DatasetId};
use zstd::stream::write::Encoder as ZstdEncoder;

use crate::{
    encoding::ContentEncoding,
    errors::{BlockItemIsNotAvailable, BlockRangeMissing, QueryIsAboveTheHead, QueryKindMismatch},
    metrics::{QUERIED_BLOCKS, QUERIED_CHUNKS},
    query::{
        QueryRange,
        static_snapshot::{StaticChunkIterator, StaticChunkReader, StaticSnapshot}
    },
    query_engine,
    shadow::{ChunkBounds, ShadowQuery},
    types::{ClientId, DBRef}
};

/// The engine that answers a query.
pub enum Engine {
    /// The legacy engine, shadowed by the new one when the query is sampled.
    Legacy { query: Query, shadow: Option<ShadowQuery> },
    /// The new engine alone, with the request body to compile against the catalog.
    New {
        catalog: Arc<DatasetDescription>,
        body: Bytes
    }
}

/// An [`Engine`] once the service has compiled the new engine's plan, which it
/// does before the query waits for the head.
pub enum ReadyEngine {
    Legacy { query: Query, shadow: Option<ShadowQuery> },
    New(query_engine::Plan)
}

/// An [`Engine`] with its plan compiled.
enum ChunkEngine {
    Legacy { plan: Plan, shadow: Option<ShadowQuery> },
    New(query_engine::Plan)
}

struct LeftOver {
    chunk: StaticChunkReader,
    next_block: BlockNumber
}

/// What one chunk wrote to the response.
struct Written {
    blocks: usize,
    last_block: BlockNumber
}

#[derive(Clone, Copy)]
pub struct RunningQueryStats {
    pub chunks_read: u64,
    pub blocks_read: u64,
    pub chunks_returned: u64,
    pub blocks_returned: u64
}

impl RunningQueryStats {
    pub fn new() -> Self {
        Self {
            chunks_read: 0,
            blocks_read: 0,
            chunks_returned: 0,
            blocks_returned: 0
        }
    }

    pub fn report_metrics(&self, dataset_id: &DatasetId, client_id: &ClientId) {
        let labels = vec![
            ("client_id", client_id.as_str().to_owned()),
            ("dataset_id", dataset_id.as_str().to_owned()),
        ];

        QUERIED_BLOCKS.get_or_create(&labels).observe(self.blocks_read as f64);
        QUERIED_CHUNKS.get_or_create(&labels).observe(self.chunks_read as f64);

        // blocks_returned and chunks_returned are reported by the streaming part
    }
}

enum Compressor {
    Gzip(GzEncoder<bytes::buf::Writer<BytesMut>>),
    Zstd(ZstdEncoder<'static, bytes::buf::Writer<BytesMut>>)
}

impl Write for Compressor {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self {
            Compressor::Gzip(e) => e.write(buf),
            Compressor::Zstd(e) => e.write(buf)
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        match self {
            Compressor::Gzip(e) => e.flush(),
            Compressor::Zstd(e) => e.flush()
        }
    }
}

impl Compressor {
    fn new(encoding: ContentEncoding) -> anyhow::Result<Self> {
        let writer = BytesMut::new().writer();
        Ok(match encoding {
            ContentEncoding::Gzip => Compressor::Gzip(GzEncoder::new(writer, Compression::fast())),
            ContentEncoding::Zstd => Compressor::Zstd(ZstdEncoder::new(writer, 1)?)
        })
    }

    fn get_ref(&self) -> &BytesMut {
        match self {
            Compressor::Gzip(e) => e.get_ref().get_ref(),
            Compressor::Zstd(e) => e.get_ref().get_ref()
        }
    }

    fn get_mut(&mut self) -> &mut BytesMut {
        match self {
            Compressor::Gzip(e) => e.get_mut().get_mut(),
            Compressor::Zstd(e) => e.get_mut().get_mut()
        }
    }

    fn finish(self) -> BytesMut {
        match self {
            Compressor::Gzip(e) => e.finish().expect("IO errors are not possible").into_inner(),
            Compressor::Zstd(e) => e.finish().expect("IO errors are not possible").into_inner()
        }
    }
}

pub struct RunningQuery {
    engine: ChunkEngine,
    /// Where the next chunk is read from, when not from its first block.
    first_block: Option<BlockNumber>,
    /// The hash the next chunk's first block must have as its parent, when the
    /// chunk is to check it.
    parent_block_hash: Option<String>,
    last_block: Option<BlockNumber>,
    left_over: Option<LeftOver>,
    next_chunk: Option<anyhow::Result<StorageChunk>>,
    chunk_iterator: StaticChunkIterator,
    finalized_head: Option<BlockRef>,
    buf: Compressor,
    stats: RunningQueryStats
}

fn finalized_query_last_block(
    last_block: Option<BlockNumber>,
    finalized_head: Option<&BlockRef>
) -> anyhow::Result<BlockNumber> {
    let Some(finalized_head) = finalized_head else {
        bail!(QueryIsAboveTheHead { finalized_head: None })
    };
    Ok(last_block.map_or(finalized_head.number, |end| end.min(finalized_head.number)))
}

impl RunningQuery {
    pub fn new(
        db: DBRef,
        dataset_id: DatasetId,
        range: &QueryRange,
        only_finalized: bool,
        encoding: ContentEncoding,
        engine: ReadyEngine
    ) -> anyhow::Result<Self> {
        let snapshot = StaticSnapshot::new(db);

        let finalized_head = match snapshot.get_label(dataset_id)? {
            None => bail!("dataset {} does not exist", dataset_id),
            Some(label) => {
                let kind = range.kind;
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

        let mut chunk_iterator = StaticChunkIterator::new(snapshot, dataset_id, range.first_block, None);

        let mut stats = RunningQueryStats::new();
        let Some(first_chunk) = chunk_iterator.next().transpose()? else {
            bail!(QueryIsAboveTheHead { finalized_head: None })
        };
        stats.chunks_read += 1;
        stats.blocks_read += first_chunk.last_block() - first_chunk.first_block() + 1;

        ensure!(
            first_chunk.first_block() <= range.first_block,
            BlockRangeMissing {
                first_block: range.first_block,
                last_block: first_chunk.first_block() - 1
            }
        );

        let (first_block, parent_block_hash) = if range.first_block == first_chunk.first_block() {
            if let Some(parent_hash) = range.parent_block_hash.as_deref() {
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
            (None, None)
        } else {
            (Some(range.first_block), range.parent_block_hash.clone())
        };

        let engine = match engine {
            ReadyEngine::Legacy { query, shadow } => ChunkEngine::Legacy {
                plan: query.compile(),
                shadow
            },
            ReadyEngine::New(plan) => ChunkEngine::New(plan)
        };

        let last_block = if only_finalized {
            Some(finalized_query_last_block(range.last_block, finalized_head.as_ref())?)
        } else {
            range.last_block
        };

        Ok(Self {
            engine,
            first_block,
            parent_block_hash,
            last_block,
            left_over: None,
            next_chunk: Some(Ok(first_chunk)),
            chunk_iterator,
            finalized_head,
            buf: Compressor::new(encoding)?,
            stats
        })
    }

    pub fn take_finalized_head(&mut self) -> Option<BlockRef> {
        self.finalized_head.take()
    }

    pub fn stats(&self) -> &RunningQueryStats {
        &self.stats
    }

    pub fn buffered_bytes(&self) -> usize {
        self.buf.get_ref().len()
    }

    pub fn take_buffered_bytes(&mut self) -> Bytes {
        self.buf.get_mut().split().freeze()
    }

    pub fn finish(self) -> Bytes {
        self.buf.finish().freeze()
    }

    pub fn has_next_chunk(&self) -> bool {
        self.next_chunk.is_some() || self.left_over.is_some()
    }

    /// Query the next chunk and write results to buffer.
    ///
    /// Everything written to the buffer is always well-formed.
    pub fn write_next_chunk(&mut self) -> anyhow::Result<()> {
        let (chunk, is_new_chunk) = if let Some(left_over) = self.left_over.take() {
            self.first_block = Some(left_over.next_block);
            (left_over.chunk, false)
        } else {
            let storage_chunk = self.next_chunk()?;
            let chunk = self.chunk_iterator.snapshot().create_chunk_reader(storage_chunk);
            (chunk, true)
        };

        // Taken: no matter what, we are moving to the next chunk.
        let bounds = ChunkBounds {
            first_block: self.first_block.take(),
            last_block: self.last_block.filter(|&end| end < chunk.last_block()),
            parent_block_hash: self.parent_block_hash.take()
        };
        let written = match &mut self.engine {
            ChunkEngine::Legacy { plan, shadow } => {
                if let Some(shadow) = shadow {
                    shadow.submit(&chunk, bounds.clone());
                }
                plan.set_first_block(bounds.first_block);
                plan.set_last_block(bounds.last_block);
                plan.set_parent_block_hash(bounds.parent_block_hash);
                write_legacy(plan, &chunk, &mut self.buf)
            }
            ChunkEngine::New(plan) => {
                plan.set_range(
                    bounds.first_block.unwrap_or(chunk.first_block()),
                    bounds.last_block,
                    bounds.parent_block_hash
                );
                write_new(plan, &chunk, &mut self.buf)
            }
        };

        let Some(written) = written? else {
            return Ok(());
        };

        if is_new_chunk {
            self.stats.chunks_returned += 1;
        }
        self.stats.blocks_returned += written.blocks as u64;

        let chunk_has_more = chunk.last_block() > written.last_block;
        let query_wants_more = self.last_block.map_or(true, |end| end > written.last_block);
        if chunk_has_more && query_wants_more {
            self.left_over = Some(LeftOver {
                chunk,
                next_block: written.last_block + 1
            })
        }

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
                self.stats.chunks_read += 1;
                self.stats.blocks_read += chunk.last_block() - chunk.first_block() + 1;

                let is_continuous = chunk.last_block() + 1 == next_chunk.first_block();
                let is_requested = self.last_block.map_or(true, |end| next_chunk.first_block() <= end);
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

fn write_legacy(plan: &Plan, chunk: &StaticChunkReader, buf: &mut Compressor) -> anyhow::Result<Option<Written>> {
    let query_result = chunk.with_reader(|reader| plan.execute(reader)).map_err(|err| {
        if let Some(err) = err.downcast_ref::<sqd_query::TableDoesNotExist>() {
            return anyhow!(BlockItemIsNotAvailable {
                item_name: err.table_name,
                first_block: chunk.first_block(),
                last_block: chunk.last_block()
            });
        }
        err
    });

    let Some(mut block_writer) = query_result? else {
        return Ok(None);
    };
    let written = Written {
        blocks: block_writer.num_blocks(),
        last_block: block_writer.last_block()
    };

    let mut json_lines_writer = JsonLinesWriter::new(&mut *buf);

    json_lines_writer
        .write_blocks(&mut block_writer)
        .expect("IO errors are not possible");

    json_lines_writer.finish().expect("IO errors are not possible");

    buf.flush().expect("IO errors are not possible");

    Ok(Some(written))
}

fn write_new(
    plan: &query_engine::Plan,
    chunk: &StaticChunkReader,
    buf: &mut Compressor
) -> anyhow::Result<Option<Written>> {
    let Some(output) = chunk.with_reader(|reader| plan.execute(reader))? else {
        return Ok(None);
    };
    let written = Written {
        blocks: output.num_blocks(),
        last_block: output.last_block()
    };

    buf.write_all(&output.into_json_lines())
        .expect("IO errors are not possible");

    buf.flush().expect("IO errors are not possible");

    Ok(Some(written))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn finalized_snapshot_without_a_head_is_no_data() {
        let query = Query::from_json_value(serde_json::json!({
            "type": "evm",
            "fromBlock": 10
        }))
        .unwrap();

        let err = finalized_query_last_block(query.last_block(), None).unwrap_err();
        assert!(err.is::<QueryIsAboveTheHead>());
    }
}
