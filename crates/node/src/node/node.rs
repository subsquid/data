use crate::error::{QueryKindMismatch, UnknownDataset};
use crate::ingest::DatasetController;
use crate::node::node_builder::NodeBuilder;
use crate::node::query_executor::{QueryExecutor, QueryExecutorRef};
use crate::node::query_response::QueryResponse;
use crate::types::{DBRef, DatasetKind, RetentionStrategy};
use anyhow::ensure;
use futures::stream::FuturesUnordered;
use futures::{FutureExt, StreamExt};
use reqwest::Url;
use sqd_primitives::BlockRef;
use sqd_query::Query;
use sqd_storage::db::DatasetId;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tracing::{error, info};


pub struct Node {
    executor: QueryExecutorRef,
    db: DBRef,
    datasets: HashMap<DatasetId, Arc<DatasetController>>,
    ingest_handle: tokio::task::JoinHandle<()>
}


impl Node {
    pub(super) fn new(builder: NodeBuilder) -> Self {
        let datasets: HashMap<_, _> = builder.datasets.into_iter().map(|cfg| {
            let first_block = match cfg.retention {
                RetentionStrategy::FromBlock(first_block) => first_block,
                RetentionStrategy::Head(_) => unimplemented!("head retention strategy is not implemented")
            };
            
            let controller = DatasetController::new(
                builder.db.clone(),
                cfg.dataset_kind,
                cfg.dataset_id,
                first_block,
                cfg.data_sources
            );
            
            (cfg.dataset_id, Arc::new(controller))
        }).collect();
        
        let ingest_handle = run(datasets.values().cloned().collect());

        Self {
            executor: Arc::new(QueryExecutor::new(builder.max_pending_query_tasks)),
            db: builder.db,
            datasets,
            ingest_handle
        }
    }

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
    
    pub fn retain(&self, dataset_id: DatasetId, retention_strategy: RetentionStrategy) {
        let first_block = match retention_strategy {
            RetentionStrategy::FromBlock(first_block) => first_block,
            RetentionStrategy::Head(_) => unimplemented!("head retention strategy is not implemented")
        };
        self.get_dataset(dataset_id).unwrap().retain(first_block)
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
        let mut completion_stream: FuturesUnordered<_> = datasets.into_iter()
            .map(|c| run_dataset_controller(false, c))
            .collect();

        while let Some((result, c)) = completion_stream.next().await {
            match result {
                Ok(_) => {
                    error!(
                        "data ingestion was terminated for dataset '{}'",
                        c.dataset_id()
                    );
                },
                Err(err) => {
                    error!(
                        reason = ?err,
                        "data ingestion was terminated for dataset '{}'",
                        c.dataset_id()
                    )
                }
            }
            completion_stream.push(run_dataset_controller(true, c))
        }
    })
}


async fn run_dataset_controller(
    pause: bool,
    c: Arc<DatasetController>
) -> (anyhow::Result<()>, Arc<DatasetController>)
{
    if pause {
        tokio::time::sleep(Duration::from_secs(120)).await;
    }
    let result = c.run().await;
    (result, c)
}