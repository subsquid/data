use sqd_query::Query;
use sqd_storage::db::DatasetId;
use std::collections::HashMap;
use crate::ingest::DatasetController;
use crate::types::DBRef;


pub struct Node {
    db: DBRef,
    datasets: HashMap<DatasetId, DatasetController>
}


impl Node {
    pub async fn query(&self, dataset_id: DatasetId, query: Query) {
        
    }
}