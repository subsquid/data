use super::executor::{QueryExecutor, QuerySlot};
use super::running::RunningQuery;
use crate::errors::Busy;
use crate::types::DBRef;
use anyhow::bail;
use bytes::Bytes;
use sqd_primitives::BlockRef;
use sqd_query::Query;
use sqd_storage::db::DatasetId;
use std::time::Instant;


pub struct QueryResponse {
    executor: QueryExecutor,
    runner: Option<Box<RunningQuery>>,
    start: Instant,
    finalized_head: Option<BlockRef>
}


impl QueryResponse {
    pub(super) async fn new(
        executor: QueryExecutor,
        db: DBRef,
        dataset_id: DatasetId,
        query: Query,
    ) -> anyhow::Result<Self>
    {
        let Some(slot) = executor.get_slot() else {
            bail!(Busy)
        };

        let start = Instant::now();

        let mut runner = slot.run(move |slot| -> anyhow::Result<_> {
            let mut runner = RunningQuery::new(db, dataset_id, &query).map(Box::new)?;
            next_run(&mut runner, slot)?;
            Ok(runner)
        }).await?;

        let response = Self {
            executor,
            finalized_head: runner.take_finalized_head(),
            runner: Some(runner),
            start
        };

        Ok(response)
    }

    pub fn finalized_head(&self) -> Option<&BlockRef> {
        self.finalized_head.as_ref()
    }

    pub async fn next_data_pack(&mut self) -> anyhow::Result<Option<Bytes>> {
        let Some(mut runner) = self.runner.take() else {
            return Ok(None)
        };

        if !runner.has_next_chunk() || self.start.elapsed().as_secs() > 10 {
            return Ok(Some(runner.finish()))
        }

        if runner.buffered_bytes() > 0 {
            let bytes = runner.take_buffered_bytes();
            self.runner = Some(runner);
            return Ok(Some(bytes))
        }

        let Some(slot) = self.executor.get_slot() else {
            self.runner = Some(runner);
            bail!(Busy);
        };

        let (mut runner, result) = slot.run(move |slot| {
            let mut runner = runner;
            let result = next_run(&mut runner, slot);
            (runner, result)
        }).await;

        if let Err(err) = result {
            self.runner = Some(runner);
            return Err(err)
        }

        if !runner.has_next_chunk() || self.start.elapsed().as_secs() > 10  {
            Ok(Some(runner.finish()))
        } else {
            let bytes = runner.take_buffered_bytes();
            self.runner = Some(runner);
            Ok(Some(bytes))
        }
    }

    pub fn finish(&mut self) -> Bytes {
        self.runner.take().map(|runner| runner.finish()).unwrap_or_default()
    }
}


fn next_run(runner: &mut RunningQuery, slot: &QuerySlot) -> anyhow::Result<()> {
    let start = Instant::now();
    let mut elapsed = 0;
    loop {
        let beg = elapsed;
        let processed = runner.next_chunk_size();

        runner.write_next_chunk()?;

        if !runner.has_next_chunk() || runner.buffered_bytes() > 256 * 1024 {
            return Ok(())
        }

        elapsed = start.elapsed().as_millis();

        let chunk_time = elapsed - beg;
        let next_chunk_eta = chunk_time * runner.next_chunk_size() as u128 / processed as u128;
        let next_chunk_eta = next_chunk_eta.min(chunk_time * 5).max(chunk_time / 5);
        let eta = elapsed + next_chunk_eta;
        if eta > slot.time_limit() as u128 {
            return Ok(())
        }
    }
}