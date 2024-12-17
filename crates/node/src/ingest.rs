use sqd_data_client::DataClient;
use crate::types::{DBRef, DatasetKind};


pub struct Ingest<B> {
    db: DBRef,
    data_client: DataClient<B>,
    dataset_kind: DatasetKind,
}