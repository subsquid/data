use crate::types::DatasetKind;
use serde::{Deserialize, Serialize};
use sqd_query::BlockNumber;
use sqd_storage::db::DatasetId;
use std::collections::BTreeMap;
use url::Url;


#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub enum RetentionConfig {
    FromBlock {
        number: BlockNumber,
        parent_hash: Option<String>
    },
    Head(u64),
    Api,
    None
}


#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DatasetConfig {
    pub kind: DatasetKind,
    pub retention_strategy: RetentionConfig,
    #[serde(default)]
    pub disable_compaction: bool,
    pub data_sources: Vec<Url>
}


impl DatasetConfig {
    pub fn read_config_file(file: &str) -> anyhow::Result<BTreeMap<DatasetId, DatasetConfig>> {
        let reader = std::io::BufReader::new(std::fs::File::open(file)?);
        let config = serde_yaml::from_reader(reader)?;
        Ok(config)
    }
}