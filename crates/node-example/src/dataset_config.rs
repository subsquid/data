use serde::{Deserialize, Serialize};
use sqd_node::{DatasetKind, RetentionStrategy};
use sqd_storage::db::DatasetId;
use std::collections::BTreeMap;
use url::Url;


#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DatasetConfig {
    pub kind: DatasetKind,
    pub retention: RetentionStrategy,
    #[serde(default)]
    pub enable_compaction: bool,
    pub data_sources: Vec<Url>
}


impl DatasetConfig {
    pub fn read_config_file(file: &str) -> anyhow::Result<BTreeMap<DatasetId, DatasetConfig>> {
        let reader = std::io::BufReader::new(std::fs::File::open(file)?);
        let config = serde_json::from_reader(reader)?;
        Ok(config)
    }
}