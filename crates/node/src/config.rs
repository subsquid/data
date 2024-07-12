use std::collections::{HashMap, HashSet};

use anyhow::{Context, ensure};
use serde::{Deserialize, Serialize};
use sqd_storage::db::DatasetId;
use crate::dataset_kind::DatasetKind;


#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Config {
    pub datasets: HashMap<DatasetId, DatasetConfig>
}


#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatasetConfig {
    pub kind: DatasetKind,
    #[serde(default)]
    pub aliases: Vec<DatasetId>,
    #[serde(default)]
    pub push_api: bool
}


impl Config {
    pub fn read(file: &str) -> anyhow::Result<Self> {
        let config: Self = serde_json::from_reader(
            std::io::BufReader::new(std::fs::File::open(file)?)
        )?;
        config.validate().context("invalid config")?;
        Ok(config)
    }

    pub fn validate(&self) -> anyhow::Result<()> {
        let mut aliases = HashSet::<DatasetId>::new();

        for dataset in self.datasets.keys().chain(
            self.datasets.values().flat_map(|options| options.aliases.iter())
        ) {
            ensure!(
                !aliases.contains(dataset),
                "dataset alias {} is used more than once",
                dataset
            );
            aliases.insert(*dataset);
        }
        
        Ok(())
    }
}