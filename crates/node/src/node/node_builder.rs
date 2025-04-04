use crate::node::node::Node;
use crate::types::{DBRef, DatasetKind, RetentionStrategy};
use sqd_data_client::reqwest::ReqwestDataClient;
use sqd_storage::db::DatasetId;


#[derive(Clone)]
pub struct DatasetConfig {
    pub(super) dataset_id: DatasetId,
    pub(super) dataset_kind: DatasetKind,
    pub(super) data_sources: Vec<ReqwestDataClient>,
    pub(super) retention: RetentionStrategy
}


pub struct NodeBuilder {
    pub(super) db: DBRef,
    pub(super) datasets: Vec<DatasetConfig>,
    pub(super) max_pending_query_tasks: usize 
}


impl NodeBuilder {
    pub fn new(db: DBRef) -> Self {
        Self {
            db,
            datasets: vec![],
            max_pending_query_tasks: sqd_polars::POOL.current_num_threads() * 50
        }
    }
    
    pub fn add_dataset(
        &mut self,
        dataset_kind: DatasetKind,
        dataset_id: DatasetId,
        data_sources: Vec<ReqwestDataClient>,
        retention: RetentionStrategy
    ) {
        self.datasets.push(DatasetConfig {
            dataset_id,
            dataset_kind,
            retention,
            data_sources
        })
    }
    
    pub fn set_max_pending_query_tasks(&mut self, n: usize) {
        self.max_pending_query_tasks = n
    }
    
    pub async fn build(self) -> anyhow::Result<Node> {
        Node::new(self).await
    }
}