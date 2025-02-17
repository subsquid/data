use crate::error::Busy;
use crate::hotblocks::query_executor::QueryExecutorRef;
use crate::query::runner::QueryRunner;
use crate::types::DBRef;
use anyhow::bail;
use bytes::Bytes;
use sqd_primitives::BlockRef;
use sqd_query::Query;
use sqd_storage::db::DatasetId;
use std::time::Instant;


pub struct QueryResponse {
    next_pack: Bytes,
    runner: Option<QueryRunner>,
    executor: QueryExecutorRef,
    finalized_head: Option<BlockRef>,
    start: Instant
}


impl QueryResponse {
    pub(super) async fn new(
        executor: QueryExecutorRef,
        db: DBRef,
        dataset_id: DatasetId,
        query: Query,
    ) -> anyhow::Result<Self>
    {
        let start = Instant::now();
        
        let (runner, next_pack) = executor.run(move || -> anyhow::Result<_> {
            let mut runner = QueryRunner::new(db, dataset_id, &query)?;
            let pack = runner.next_pack()?;
            Ok((runner, pack))
        })
        .await
        .unwrap_or_else(|| bail!(Busy))?;

        let finalized_head = runner.finalized_head().cloned();

        let response = Self {
            executor,
            runner: runner.has_next_pack().then_some(runner),
            next_pack,
            finalized_head,
            start
        };

        Ok(response)
    }

    pub fn finalized_head(&self) -> Option<&BlockRef> {
        self.finalized_head.as_ref()
    }

    pub async fn next_bytes(&mut self) -> anyhow::Result<Option<Bytes>> {
        if !self.next_pack.is_empty() {
            return Ok(Some(std::mem::take(&mut self.next_pack)))
        }

        let runner = match self.runner.take() {
            None => return Ok(None),
            Some(runner) => runner
        };

        if !runner.has_next_pack() {
            return Ok(None)
        }
        
        if self.start.elapsed().as_secs() > 10 {
            return Ok(Some(runner.finish()))
        }

        match self.executor.run_with_ctx(runner, |mut runner| {
            runner.next_pack().map(|pack| (pack, runner))
        }).await
        {
            Ok(result) => {
                let (pack, runner) = result?;
                self.runner = runner.has_next_pack().then_some(runner);
                Ok(Some(pack))
            },
            Err(runner) => {
                // service is busy, end the stream
                Ok(Some(runner.finish()))
            }
        }
    }
}