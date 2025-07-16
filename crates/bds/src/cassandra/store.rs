use super::row_batch::{Row, RowBatch};
use crate::block::{Block, BlockHeader};
use crate::cassandra::types::WriteState;
use crate::cassandra::BlockBatch;
use anyhow::{anyhow, bail, Context};
use async_stream::try_stream;
use futures::{Stream, StreamExt};
use scylla::client::session::Session;
use scylla::response::query_result::QueryRowsResult;
use scylla::response::{PagingState, PagingStateResponse};
use scylla::statement::prepared::PreparedStatement;
use scylla::statement::Consistency;
use sqd_primitives::{BlockNumber, BlockRef};
use std::sync::Arc;
use uuid::Uuid;


#[derive(Clone)]
pub struct CassandraStorage {
    inner: Arc<Inner>
}


struct Inner {
    session: Arc<Session>,
    update_statement: PreparedStatement,
    fetch_statement: PreparedStatement,
    list_statement: PreparedStatement,
    reversed_list_statement: PreparedStatement,
    fetch_write_states_statement: PreparedStatement,
    set_write_head_statement: PreparedStatement,
    set_write_finalized_head_statement: PreparedStatement,
    partition_size: u64,
    id: Uuid
}


impl CassandraStorage {
    pub async fn new(session: Arc<Session>, keyspace: &str) -> anyhow::Result<Self> {
        let partition_size = 10;

        let mut update_statement = session.prepare(format!(
            "UPDATE {}.blocks SET parent_number = ?, parent_hash = ?, timestamp = ?, data = ? WHERE partition = ? AND number = ? AND hash = ?",
            keyspace
        )).await?;
        update_statement.set_consistency(Consistency::One); // FIXME

        let mut fetch_statement = session.prepare(format!(
            "SELECT number, hash, parent_number, parent_hash, timestamp, is_final, data FROM {}.blocks WHERE partition = ? AND number >= ? AND number <= ?",
            keyspace
        )).await?;
        fetch_statement.set_consistency(Consistency::One);
        fetch_statement.set_is_idempotent(true);
        fetch_statement.set_page_size((partition_size * 2) as i32);

        let mut list_statement = session.prepare(format!(
            "SELECT number, hash, parent_number, parent_hash, timestamp, is_final FROM {}.blocks WHERE partition = ? AND number >= ? AND number <= ?",
            keyspace
        )).await?;
        list_statement.set_consistency(Consistency::One);
        list_statement.set_is_idempotent(true);
        list_statement.set_page_size((partition_size * 10) as i32);

        let mut reversed_list_statement = session.prepare(format!(
            "SELECT number, hash, parent_number, parent_hash, timestamp, is_final FROM {}.blocks WHERE partition = ? AND number >= ? AND number < ? ORDER BY number DESC",
            keyspace
        )).await?;
        reversed_list_statement.set_consistency(Consistency::One);
        reversed_list_statement.set_is_idempotent(true);
        reversed_list_statement.set_page_size((partition_size * 10) as i32);

        let mut fetch_write_states_statement = session.prepare(format!(
            "SELECT id, head_number, head_hash, finalized_head_number, finalized_head_hash \
            FROM {}.writers WHERE dummy_partition = 0",
            keyspace
        )).await?;
        fetch_write_states_statement.set_consistency(Consistency::One);
        fetch_write_states_statement.set_is_idempotent(true);

        let mut set_write_head_statement = session.prepare(format!(
            "UPDATE {}.writers SET head_number = ?, head_hash = ? \
            WHERE dummy_partition = 0 AND id = ?",
            keyspace
        )).await?;
        // use Consistency::One, because it is ok to miss few blocks
        set_write_head_statement.set_consistency(Consistency::One);

        let mut set_write_finalized_head_statement = session.prepare(format!(
            "UPDATE {}.writers SET finalized_head_number = ?, finalized_head_hash = ? \
            WHERE dummy_partition = 0 AND id = ?",
            keyspace
        )).await?;
        // use Consistency::One, because it is ok to miss few blocks
        set_write_finalized_head_statement.set_consistency(Consistency::One);

        let inner = Inner {
            session,
            update_statement,
            fetch_statement,
            list_statement,
            reversed_list_statement,
            fetch_write_states_statement,
            set_write_head_statement,
            set_write_finalized_head_statement,
            partition_size,
            id: Uuid::now_v7()
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

    pub async fn set_head(&self, number: BlockNumber, hash: &str) -> anyhow::Result<()> {
        self.inner.session.execute_unpaged(&self.inner.set_write_head_statement, (
            number as i64,
            hash,
            self.inner.id
        )).await?;
        Ok(())
    }

    pub async fn set_finalized_head(&self, number: BlockNumber, hash: &str) -> anyhow::Result<()> {
        self.inner.session.execute_unpaged(&self.inner.set_write_finalized_head_statement, (
            number as i64,
            hash,
            self.inner.id
        )).await?;
        Ok(())
    }

    pub async fn fetch_write_states(&self) -> anyhow::Result<Vec<WriteState>> {
        let res = self.inner.session.execute_unpaged(&self.inner.fetch_write_states_statement, ()).await?;
        let rows = res.into_rows_result()?;

        let rows = rows.rows::<(
            Uuid,
            Option<i64>,
            Option<String>,
            Option<i64>,
            Option<String>
        )>()?;

        fn into_block_ref(number: Option<i64>, hash: Option<String>) -> anyhow::Result<Option<BlockRef>> {
            match (number, hash) {
                (Some(number), Some(hash)) => {
                    let number: BlockNumber = number
                        .try_into()
                        .context("invalid block number")?;
                    Ok(Some(BlockRef {
                        number,
                        hash
                    }))
                },
                (Some(_), None) => bail!("block number is present, but hash is not"),
                (None, Some(_)) => bail!("block hash is present, but block number is not"),
                (None, None) => Ok(None)
            }
        }

        rows.map(|row_result| {
            let (id, head_number, head_hash, finalized_head_number, finalized_head_hash) = row_result?;

            let mut head = into_block_ref(head_number, head_hash).with_context(|| {
                format!("invalid head in a write state {}", id)
            })?;

            let finalized_head = into_block_ref(finalized_head_number, finalized_head_hash).with_context(|| {
                format!("invalid finalized head in a write state {}", id)
            })?;

            if head.is_none() {
                head = finalized_head.clone();
            }

            let head = head.ok_or_else(|| {
                anyhow!("write state {} has no heads", id)
            })?;

            Ok(WriteState {
                id,
                head,
                finalized_head
            })
        }).collect()
    }

    pub fn fetch_blocks(
        &self,
        first_block: BlockNumber,
        last_block: BlockNumber
    ) -> impl Stream<Item = anyhow::Result<BlockBatch<Block<'static>>>>
    {
        self.execute_block_list(
            &|inner| &inner.fetch_statement,
            first_block,
            last_block,
            false
        )
    }

    pub fn list_blocks(
        &self,
        first_block: BlockNumber,
        last_block: BlockNumber,
    ) -> impl Stream<Item = anyhow::Result<BlockBatch<BlockHeader<'static>>>>
    {
        self.execute_block_list(
            &|inner| &inner.list_statement,
            first_block,
            last_block,
            false
        )
    }

    pub fn list_blocks_in_reversed_order(
        &self,
        first_block: BlockNumber,
        last_block: BlockNumber,
    ) -> impl Stream<Item = anyhow::Result<BlockBatch<BlockHeader<'static>>>>
    {
        self.execute_block_list(
            &|inner| &inner.reversed_list_statement,
            first_block,
            last_block,
            true
        )
    }

    fn execute_block_list<R: Row>(
        &self,
        statement: &'static (dyn (Fn(&Inner) -> &PreparedStatement) + Sync),
        first_block: BlockNumber,
        last_block: BlockNumber,
        reverse_partitions: bool
    ) -> impl Stream<Item = anyhow::Result<BlockBatch<R>>>
    {
        self.execute_untyped_block_list(
            statement,
            first_block,
            last_block,
            reverse_partitions
        ).map(|rows| {
            let (rows, partition_start, partition_end) = rows?;
            let batch = RowBatch::new(rows)?;
            Ok(BlockBatch::new(
                batch,
                partition_start,
                partition_end
            ))
        })
    }

    fn execute_untyped_block_list(
        &self,
        statement: &'static (dyn (Fn(&Inner) -> &PreparedStatement) + Sync),
        first_block: BlockNumber,
        last_block: BlockNumber,
        reverse_partitions: bool
    ) -> impl Stream<Item = anyhow::Result<(QueryRowsResult, BlockNumber, BlockNumber)>>
    {
        let inner = self.inner.clone();
        try_stream! {
            for r in split_into_partitions(reverse_partitions, inner.partition_size, first_block, last_block) {
                let partition = get_partition(inner.partition_size, r.0);
                let mut page = PagingState::start();
                loop {
                    let (res, paging_response) = inner.session.execute_single_page(
                        statement(&inner),
                        &(partition as i64, r.0 as i64, r.1 as i64),
                        page
                    ).await?;

                    yield (
                        res.into_rows_result()?,
                        partition,
                        partition + inner.partition_size - 1
                    );

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


fn split_into_partitions(
    reverse: bool,
    partition_size: u64,
    mut first_block: BlockNumber,
    mut last_block: BlockNumber
) -> impl Iterator<Item = (BlockNumber, BlockNumber)>
{
    let mut range = first_block..last_block + 1;
    std::iter::from_fn(move || {
        if range.is_empty() {
            return None
        }
        if reverse {
            let start = std::cmp::max(range.start, get_partition(partition_size, range.end - 1));
            let next = (start, range.end - 1);
            range.end = start;
            Some(next)
        } else {
            let end = get_partition(partition_size, range.start) + partition_size;
            let next = (range.start, end - 1);
            range.start = end;
            Some(next)
        }
    })
}