use serde::Deserialize;

pub type DatasetId = String; // s3://<bucket-name>

#[derive(Debug, Clone, Deserialize)]
pub struct DatasetConfig {
    pub name: String,
    #[serde(default)]
    pub id: Option<DatasetId>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct DatasetsConfig {
    pub datasets: Vec<DatasetConfig>,
}
