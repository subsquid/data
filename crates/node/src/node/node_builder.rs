use crate::ingest::DataSource;
use crate::node::node::Node;
use crate::types::{DBRef, DatasetKind};
use reqwest::IntoUrl;
use sqd_primitives::BlockNumber;
use sqd_storage::db::DatasetId;


#[derive(Clone)]
pub struct DatasetConfig {
    pub(super) dataset_id: DatasetId,
    pub(super) dataset_kind: DatasetKind,
    pub(super) first_block: BlockNumber,
    pub(super) data_sources: Vec<DataSource>,
    pub(super) default_http_client: reqwest::Client
}


impl DatasetConfig {
    pub fn add_data_source(&mut self, url: impl IntoUrl) -> &mut Self
    {
        self.data_sources.push((
            self.default_http_client.clone(),
            url.into_url().unwrap()
        ));
        self
    }
}


pub struct NodeBuilder {
    pub(super) db: DBRef,
    pub(super) datasets: Vec<DatasetConfig>,
    pub(super) default_http_client: reqwest::Client,
    pub(super) max_pending_query_tasks: usize 
}


impl NodeBuilder {
    pub fn new(db: DBRef) -> Self {
        Self {
            db,
            datasets: vec![],
            default_http_client: sqd_data_client::default_http_client(),
            max_pending_query_tasks: sqd_polars::POOL.current_num_threads() * 50
        }
    }
    
    pub fn set_default_http_client(&mut self, http_client: reqwest::Client) {
        self.default_http_client = http_client;
    }
    
    pub fn add_dataset(
        &mut self, 
        dataset_kind: DatasetKind, 
        dataset_id: DatasetId, 
        first_block: BlockNumber
    ) -> &mut DatasetConfig {
        self.datasets.push(DatasetConfig {
            dataset_id,
            dataset_kind,
            first_block,
            data_sources: vec![],
            default_http_client: self.default_http_client.clone()
        });
        self.datasets.last_mut().unwrap()
    }
    
    pub fn build(self) -> Node {
        Node::new(self)
    }
}