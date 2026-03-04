use serde::Deserialize;
use std::collections::HashMap;

pub type DatasetId = String; // s3://<bucket-name>

#[derive(Debug, Clone, Deserialize)]
pub struct DatasetProps {
    pub id: Option<DatasetId>,
}

pub type DatasetsConfig = HashMap<String, Option<DatasetProps>>;
