use crate::error::{QueryKindMismatch, UnknownDataset};
use crate::ingest::DatasetController;
use crate::node::query_executor::QueryExecutorRef;
use crate::node::query_response::QueryResponse;
use crate::types::{DBRef, DatasetKind};
use anyhow::ensure;
use futures::stream::FuturesUnordered;
use futures::{FutureExt, StreamExt};
use sqd_primitives::BlockRef;
use sqd_query::Query;
use sqd_storage::db::DatasetId;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tracing::error;


pub struct Node {
    db: DBRef,
    datasets: HashMap<DatasetId, Arc<DatasetController>>,
    executor: QueryExecutorRef,
    ingest_handle: tokio::task::JoinHandle<()>
}


impl Node {
    pub async fn query(
        &self,
        dataset_id: DatasetId,
        query: Query
    ) -> anyhow::Result<
        Result<QueryResponse, Option<BlockRef>>
    >
    {
        let ds = self.get_dataset(dataset_id)?;

        ensure!(
            ds.dataset_kind() == DatasetKind::from_query(&query),
            QueryKindMismatch {
                query_kind: DatasetKind::from_query(&query).storage_kind(),
                dataset_kind: ds.dataset_kind().storage_kind()
            }
        );

        if ds.get_head_block_number().map_or(false, |head| head < query.first_block()) {
            // FIXME: there should be a bound on a maximum number of per-dataset waiters
            match tokio::time::timeout(
                Duration::from_secs(5),
                ds.wait_for_block(query.first_block())
            ).await {
                Ok(_) => {}
                Err(_) => return Ok(Err(ds.get_finalized_head()))
            }
        }

        QueryResponse::new(
            self.executor.clone(),
            self.db.clone(),
            dataset_id,
            query
        ).await.map(Ok)
    }

    pub fn get_finalized_head(&self, dataset_id: DatasetId) -> Result<Option<BlockRef>, UnknownDataset> {
        self.get_dataset(dataset_id).map(|d| d.get_finalized_head())
    }

    pub fn get_head(&self, dataset_id: DatasetId) -> Result<Option<BlockRef>, UnknownDataset> {
        self.get_dataset(dataset_id).map(|d| d.get_head())
    }

    fn get_dataset(&self, dataset_id: DatasetId) -> Result<&DatasetController, UnknownDataset> {
        self.datasets.get(&dataset_id).map(|arc| arc.as_ref()).ok_or(UnknownDataset {
            dataset_id
        })
    }
}


impl Drop for Node {
    fn drop(&mut self) {
        self.ingest_handle.abort()
    }
}


fn run(datasets: Vec<Arc<DatasetController>>) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut completion_stream: FuturesUnordered<_> = datasets.iter()
            .map(|c| {
                let cc = c.clone();
                c.run().map(|res| (res, cc))
            })
            .collect();

        while let Some((result, c)) = completion_stream.next().await {
            match result {
                Ok(_) => {
                    error!(
                        "data ingestion was terminated for dataset '{}', it will be no longer updated",
                        c.dataset_id()
                    );
                },
                Err(err) => {
                    let err: &dyn std::error::Error = err.as_ref();
                    error!(
                        reason = err,
                        "data ingestion was terminated for dataset '{}', it will be no longer updated",
                        c.dataset_id(),
                    )
                }
            }
        }
    })
}