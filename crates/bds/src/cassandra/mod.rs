mod ingest_store;
mod row_batch;
mod types;


pub use self::row_batch::RowBatch;
use crate::block::{Block, BlockHeader, BlockRange};
use anyhow::Context;
use async_stream::try_stream;
use futures::{Stream, StreamExt, TryStream};
use scylla::client::session::Session;
use scylla::response::query_result::QueryRowsResult;
use scylla::response::{PagingState, PagingStateResponse};
use scylla::statement::prepared::PreparedStatement;
use scylla::statement::Consistency;
use sqd_primitives::BlockNumber;
use std::ops::{Range, RangeBounds};
use std::sync::Arc;


#[derive(Clone)]
pub struct CassandraStorage {
    inner: Arc<Inner>
}


struct Inner {
    session: Arc<Session>,
    update_statement: PreparedStatement,
    fetch_statement: PreparedStatement,
    list_statement: PreparedStatement,
    partition_size: u64
}


impl CassandraStorage {
    pub async fn new(session: Arc<Session>, keyspace: &str) -> anyhow::Result<Self> {
        let partition_size = 10;
        
        let mut update_statement = session.prepare(format!(
            "UPDATE {}.blocks SET parent_number = ?, parent_hash = ?, timestamp = ?, data = ? WHERE partition = ? AND number = ? AND hash = ?", 
            keyspace
        )).await?;
        update_statement.set_consistency(Consistency::Quorum);
        
        let mut fetch_statement = session.prepare(format!(
            "SELECT number, hash, parent_number, parent_hash, timestamp, data FROM {}.blocks WHERE partition = ? AND number >= ? AND number < ?",
            keyspace
        )).await?;
        fetch_statement.set_consistency(Consistency::One);
        fetch_statement.set_is_idempotent(true);
        fetch_statement.set_page_size((partition_size * 2) as i32);
        
        let mut list_statement = session.prepare(format!(
            "SELECT number, hash, parent_number, parent_hash, timestamp FROM {}.blocks WHERE partition = ? AND number >= ? AND number < ?",
            keyspace
        )).await?;
        list_statement.set_consistency(Consistency::One);
        list_statement.set_is_idempotent(true);
        list_statement.set_page_size((partition_size * 10) as i32);

        let inner = Inner {
            session,
            update_statement,
            fetch_statement,
            list_statement,
            partition_size
        };

        Ok(Self {
            inner: Arc::new(inner)
        })
    }
    
    pub async fn save_block(&self, block: &Block<'_>) -> anyhow::Result<()> {
        self.inner.session.execute_unpaged(&self.inner.update_statement, (
            block.header.parent_number as i64,
            block.header.parent_hash.as_ref(),
            block.header.timestamp,
            block.data.as_ref(),
            get_partition(self.inner.partition_size, block.header.number) as i64,
            block.header.number as i64,
            block.header.hash.as_ref()
        )).await?;
        Ok(())
    }

    pub fn split_into_partitions(&self, range: BlockRange) -> impl Iterator<Item = BlockRange> {
        split_into_partitions(self.inner.partition_size, range)
    }
    
    pub fn fetch_blocks(
        &self,
        range: BlockRange
    ) -> impl Stream<Item = anyhow::Result<RowBatch<Block<'static>>>>
    {
        self.execute_block_list(
            &|inner| &inner.fetch_statement,
            range
        ).map(|rows| {
            rows.and_then(RowBatch::new)
        })
    }

    pub fn list_blocks(
        &self,
        range: BlockRange
    ) -> impl Stream<Item = anyhow::Result<RowBatch<BlockHeader<'static>>>>
    {
        self.execute_block_list(
            &|inner| &inner.list_statement,
            range
        ).map(|rows| {
            rows.and_then(RowBatch::new)
        })
    }

    fn execute_block_list(
        &self,
        statement: &'static dyn Fn(&Inner) -> &PreparedStatement,
        range: Range<BlockNumber>
    ) -> impl Stream<Item = anyhow::Result<QueryRowsResult>>
    {
        let inner = self.inner.clone();
        try_stream! {
            for r in split_into_partitions(inner.partition_size, range.clone()) {
                let partition = get_partition(inner.partition_size, r.start);
                let mut page = PagingState::start();
                loop {
                    let (res, paging_response) = inner.session.execute_single_page(
                        statement(&inner),
                        &(partition as i64, r.start as i64, r.end as i64),
                        page
                    ).await?;

                    yield res.into_rows_result()?;
                    
                    match paging_response {
                        PagingStateResponse::HasMorePages { state } => {
                            page = state;
                        },
                        PagingStateResponse::NoMorePages => {
                            break;
                        }
                    }
                }
            }
        }
    }
}


fn get_partition(partition_size: u64, block_number: BlockNumber) -> u64 {
    (block_number / partition_size) * partition_size
}


fn split_into_partitions(partition_size: u64, mut range: Range<BlockNumber>) -> impl Iterator<Item = Range<BlockNumber>> {
    std::iter::from_fn(move || {
        if range.is_empty() {
            return None
        }
        let end = get_partition(partition_size, range.start) + partition_size;
        let next = Some(range.start..end);
        range.start = end;
        next
    })
}