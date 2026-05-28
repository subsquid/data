use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use sqd_query::BlockNumber;
use sqd_storage::db::DatasetId;
use url::Url;

use crate::types::DatasetKind;

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub enum RetentionConfig {
    // Fixed, starting from the block number
    FromBlock {
        number: BlockNumber,
        parent_hash: Option<String>
    },
    // Moving window that keeps up to N blocks
    Head(u64),
    // Retention is set dynamically from the portal
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
        let deser = serde_yaml::Deserializer::from_reader(reader);
        let config = serde_yaml::with::singleton_map_recursive::deserialize(deser)?;
        Ok(config)
    }
}
