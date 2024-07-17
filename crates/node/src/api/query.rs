use std::io::Write;
use std::sync::Arc;

use anyhow::anyhow;
use async_stream::try_stream;
use bytes::buf::Writer;
use bytes::{Bytes, BytesMut};
use flate2::write::GzEncoder;
use futures::Stream;
use ouroboros::self_referencing;

use sqd_query::{BlockNumber, JsonLinesWriter, Plan, Query, StorageChunk};
use sqd_storage::db::{Chunk as RawChunk, ChunkReader, Database, DatasetId, ReadSnapshot};

use crate::api::error::ApiError;
use crate::dataset_kind::DatasetKind;


pub fn check_query_kind(dataset_kind: DatasetKind, query: &Query) -> Result<(), ApiError> {
    macro_rules! ensure_kind {
        ($expected:ident, $query_name:expr) => {
            if DatasetKind::$expected == dataset_kind {
                Ok(())
            } else {
                Err(ApiError::UserError(
                    format!("{} query was issued to {} dataset", $query_name, dataset_kind.as_str())
                ))
            }
        };
    }
    match query {
        Query::Eth(_) => ensure_kind!(Eth, "eth"),
        Query::Solana(_) => ensure_kind!(Solana, "solana")
    }
}


pub async fn query_chunk<W: Write + Send + 'static>(
    chunk: StaticChunk,
    plan: Arc<Plan>,
    out: W
) -> anyhow::Result<(BlockNumber, W)>
{
    let (tx, rx) = tokio::sync::oneshot::channel();
    
    let run = move || -> anyhow::Result<(BlockNumber, W)> {
        let mut block_writer = plan.execute(
            &StorageChunk::new(chunk.reader())
        )?;
        let mut json_lines = JsonLinesWriter::new(out);
        json_lines.write_blocks(&mut block_writer)?;
        let out = json_lines.finish()?;
        Ok((block_writer.last_block(), out))
    };
    
    sqd_polars::POOL.spawn(move || {
        let result = run();
        let _ = tx.send(result);
    });
    
    rx.await.unwrap_or_else(|_| {
        Err(anyhow!("query execution panicked"))
    })
}


pub fn stream_data(
    gz: GzEncoder<Writer<BytesMut>>,
    mut chunks: impl Iterator<Item = anyhow::Result<StaticChunk>>,
    plan: Arc<Plan>
) -> impl Stream<Item = anyhow::Result<Bytes>>
{
    try_stream! {
        let mut gz = gz;
        while let Some(chunk) = chunks.next().transpose()? {
            let (_last_block, gz_ret) = query_chunk(chunk, plan.clone(), gz).await?;
            gz = gz_ret;
            let bytes = gz.get_mut().get_mut().split().freeze();
            yield bytes;
        }
        yield gz.finish()?.into_inner().freeze()
    }
}


#[self_referencing]
struct StaticSnapshotInner {
    db: Arc<Database>,
    #[borrows(db)]
    #[covariant]
    snapshot: ReadSnapshot<'this>,
}


#[derive(Clone)]
pub struct StaticSnapshot {
    inner: Arc<StaticSnapshotInner>
}


impl StaticSnapshot {
    pub fn new(db: Arc<Database>) -> Self {
        Self {
            inner: Arc::new(
                StaticSnapshotInnerBuilder {
                    db,
                    snapshot_builder: |db: &Arc<Database>| db.get_snapshot()
                }.build()
            )
        }
    }

    pub fn list_chunks(
        &self,
        dataset_id: DatasetId,
        from_block: sqd_primitives::BlockNumber,
        to_block: Option<sqd_primitives::BlockNumber>
    ) -> impl Iterator<Item = anyhow::Result<StaticChunk>> + Send
    {
        StaticChunkIterBuilder {
            snapshot: self.clone(),
            iter_builder: |s: &StaticSnapshot| Box::new(
                s.inner.borrow_snapshot()
                    .list_raw_chunks(dataset_id, from_block, to_block)
                    .map(|result| {
                        result.map(|chunk| StaticChunk::new(s.clone(), chunk))
                    })
            )
        }.build()
    }
}


#[self_referencing]
struct StaticChunkIter {
    snapshot: StaticSnapshot,
    #[borrows(snapshot)]
    #[covariant]
    iter: Box<dyn Iterator<Item = anyhow::Result<StaticChunk>> + Send + 'this>
}


impl Iterator for StaticChunkIter {
    type Item = anyhow::Result<StaticChunk>;

    fn next(&mut self) -> Option<Self::Item> {
        self.with_iter_mut(|it| it.next())
    }
}


#[self_referencing]
struct StaticChunkInner {
    snapshot: StaticSnapshot,
    #[borrows(snapshot)]
    #[covariant]
    reader: ChunkReader<'this>,
}


pub struct StaticChunk {
    inner: StaticChunkInner
}


impl StaticChunk {
    fn new(snapshot: StaticSnapshot, chunk: RawChunk) -> Self {
        Self {
            inner: StaticChunkInnerBuilder {
                snapshot,
                reader_builder: |s: &StaticSnapshot| {
                    s.inner.borrow_snapshot().create_chunk_reader(chunk)
                }
            }.build()
        }
    }

    pub fn reader(&self) -> &ChunkReader<'_> {
        self.inner.borrow_reader()
    }
}