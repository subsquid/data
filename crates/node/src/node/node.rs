use crate::ingest::DatasetController;
use crate::node::query_executor::QueryExecutorRef;
use crate::node::query_response::QueryResponse;
use crate::types::DBRef;
use sqd_query::Query;
use sqd_storage::db::DatasetId;
use std::collections::HashMap;


pub struct Node {
    db: DBRef,
    datasets: HashMap<DatasetId, DatasetController>,
    executor: QueryExecutorRef
}


impl Node {
    pub async fn query(&self, dataset_id: DatasetId, query: Query) -> anyhow::Result<QueryResponse> {
        todo!()
    }
}