use crate::error::{QueryIsAboveTheHead, QueryKindMismatch, UnknownDataset};
use crate::ingest::DatasetController;
use crate::node::node_builder::NodeBuilder;
use crate::node::query_executor::QueryExecutor;
use crate::node::query_response::QueryResponse;
use crate::types::{DBRef, DatasetKind, RetentionStrategy};
use anyhow::{anyhow, bail, ensure, Context};
use futures::{FutureExt, StreamExt, TryStreamExt};
use sqd_primitives::{BlockNumber, BlockRef};
use sqd_query::Query;
use sqd_storage::db::{DatabaseMetrics, DatasetId};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;


pub struct Node {
    db: DBRef,
    datasets: HashMap<DatasetId, Arc<DatasetController>>,
    executor: QueryExecutor
}


impl Node {
    pub(super) async fn new(builder: NodeBuilder) -> anyhow::Result<Self> {
        let db = builder.db.clone();
        let mut datasets = HashMap::with_capacity(builder.datasets.len());

        let mut controllers = futures::stream::iter(builder.datasets.into_iter())
            .map(|cfg| {
                let db = db.clone();
                let dataset_id = cfg.dataset_id;
                tokio::task::spawn_blocking(move || {
                    DatasetController::new(
                        db,
                        cfg.dataset_id,
                        cfg.dataset_kind,
                        cfg.retention,
                        cfg.data_sources
                    ).map(|c| {
                        c.enable_compaction(cfg.enable_compaction);
                        Arc::new(c)
                    })
                }).map(move |res| {
                    res.with_context(|| {
                        anyhow!("failed to initialize dataset {}", dataset_id)
                    })
                })
            })
            .buffered(5);

        while let Some(ctl) = controllers.try_next().await?.transpose()? {
            datasets.insert(ctl.dataset_id(), ctl);
        }

        Ok(Self {
            executor: QueryExecutor::new(builder.query_queue, builder.query_urgency),
            db: builder.db,
            datasets,
        })
    }

    pub async fn query(
        &self,
        dataset_id: DatasetId,
        query: Query
    ) -> anyhow::Result<QueryResponse>
    {
        let ds = self.get_dataset(dataset_id)?;

        ensure!(
            ds.dataset_kind() == DatasetKind::from_query(&query),
            QueryKindMismatch {
                query_kind: DatasetKind::from_query(&query).storage_kind(),
                dataset_kind: ds.dataset_kind().storage_kind()
            }
        );
        
        let should_wait = match ds.get_head() {
            Some(head) if head.number >= query.first_block() => false,
            Some(head) if head.number + 1 == query.first_block() => {
                if let Some(parent_hash) = query.parent_block_hash() {
                    ensure!(
                        head.hash == parent_hash,
                        sqd_query::UnexpectedBaseBlock {
                            prev_blocks: vec![head],
                            expected_hash: parent_hash.to_string()
                        }
                    );
                }
                true
            },
            Some(_) | None => true
        };
        
        if should_wait {
            // FIXME: there should be a bound on a maximum number of per-dataset waiters
            match tokio::time::timeout(
                Duration::from_secs(5),
                ds.wait_for_block(query.first_block())
            ).await {
                Ok(_) => {}
                Err(_) => {
                    // FIXME: here we ignore possible finalization progress,
                    // that's because ds.get_finalized_head() and ds.get_head() are not synchronized
                    bail!(QueryIsAboveTheHead {
                        finalized_head: None
                    });
                }
            }
        }

        QueryResponse::new(
            self.executor.clone(),
            self.db.clone(),
            dataset_id,
            query
        ).await
    }

    pub fn get_finalized_head(&self, dataset_id: DatasetId) -> Result<Option<BlockRef>, UnknownDataset> {
        self.get_dataset(dataset_id).map(|d| d.get_finalized_head())
    }

    pub fn get_head(&self, dataset_id: DatasetId) -> Result<Option<BlockRef>, UnknownDataset> {
        self.get_dataset(dataset_id).map(|d| d.get_head())
    }

    pub fn get_first_block(&self, dataset_id: DatasetId) -> Result<BlockNumber, UnknownDataset> {
        self.get_dataset(dataset_id).map(|d| d.get_first_block_number())
    }

    
    pub fn retain(&self, dataset_id: DatasetId, retention_strategy: RetentionStrategy) {
        self.get_dataset(dataset_id).unwrap().retain(retention_strategy)
    }

    pub fn get_db_statistics(&self) -> Option<String> {
        self.db.get_statistics()
    }

    pub fn get_db_metrics(&self) -> anyhow::Result<DatabaseMetrics> {
        self.db.get_metrics()
    }

    pub fn get_all_datasets(&self) -> Vec<DatasetId> {
        self.datasets.keys().copied().collect()
    }

    fn get_dataset(&self, dataset_id: DatasetId) -> Result<&DatasetController, UnknownDataset> {
        self.datasets.get(&dataset_id).map(|arc| arc.as_ref()).ok_or(UnknownDataset {
            dataset_id
        })
    }
}