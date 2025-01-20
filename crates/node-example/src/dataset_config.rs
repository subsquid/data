use std::collections::BTreeMap;
use serde::{Deserialize, Serialize};
use sqd_node::DatasetKind;
use url::Url;
use sqd_storage::db::DatasetId;


#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DatasetConfig {
    pub kind: DatasetKind,
    pub first_block: u64,
    pub data_sources: Vec<Url>
}


impl DatasetConfig {
    pub fn read_config_file(file: &str) -> anyhow::Result<BTreeMap<DatasetId, DatasetConfig>> {
        let reader = std::io::BufReader::new(std::fs::File::open(file)?);
        let config = serde_json::from_reader(reader)?;
        Ok(config)
    }
}