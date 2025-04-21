use crate::node::node::Node;
use crate::types::{DBRef, DatasetKind, RetentionStrategy};
use sqd_data_client::reqwest::ReqwestDataClient;
use sqd_storage::db::DatasetId;


#[derive(Clone)]
pub struct DatasetConfig {
    pub(super) dataset_id: DatasetId,
    pub(super) dataset_kind: DatasetKind,
    pub(super) data_sources: Vec<ReqwestDataClient>,
    pub(super) retention: RetentionStrategy,
    pub(super) enable_compaction: bool
}


impl DatasetConfig {
    pub fn enable_compaction(&mut self, yes: bool) {
        self.enable_compaction = yes;
    }
}


pub struct NodeBuilder {
    pub(super) db: DBRef,
    pub(super) datasets: Vec<DatasetConfig>,
    pub(super) query_queue: usize,
    pub(super) query_urgency: usize
}


impl NodeBuilder {
    pub fn new(db: DBRef) -> Self {
        Self {
            db,
            datasets: vec![],
            query_queue: sqd_polars::POOL.current_num_threads() * 200,
            query_urgency: 500
        }
    }
    
    pub fn add_dataset(
        &mut self,
        dataset_id: DatasetId,
        dataset_kind: DatasetKind,
        data_sources: Vec<ReqwestDataClient>,
        retention: RetentionStrategy
    ) -> &mut DatasetConfig 
    {
        self.datasets.push(DatasetConfig {
            dataset_id,
            dataset_kind,
            retention,
            data_sources,
            enable_compaction: false
        });
        self.datasets.last_mut().unwrap()
    }
    
    pub fn set_query_queue_size(&mut self, n: usize) {
        self.query_queue = n
    }
    
    pub fn set_query_urgency(&mut self, n: usize) {
        self.query_urgency = n
    }
    
    pub async fn build(self) -> anyhow::Result<Node> {
        Node::new(self).await
    }
}