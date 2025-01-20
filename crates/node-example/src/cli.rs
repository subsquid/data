use crate::dataset_config::DatasetConfig;
use clap::Parser;
use sqd_node::{Node, NodeBuilder};
use sqd_storage::db::DatabaseSettings;
use std::sync::Arc;
use anyhow::Context;


#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
pub struct CLI {
    /// Config file to get dataset specs from
    #[arg(short, long, value_name = "FILE")]
    pub datasets: String,

    /// Database directory
    #[arg(long = "db", default_value = "node.db")]
    pub database_dir: String,

    #[arg(long, value_name = "MB", default_value = "4096")]
    pub data_cache_size: usize
}


impl CLI {
    pub fn build_node(&self) -> anyhow::Result<Arc<Node>> {
        let datasets = DatasetConfig::read_config_file(&self.datasets)
            .context("failed to read datasets config")?;

        let db = DatabaseSettings::default()
            .set_data_cache_size(self.data_cache_size)
            .open(&self.database_dir)
            .map(Arc::new)
            .context("failed to open rocksdb database")?;
        
        let mut builder = NodeBuilder::new(db);
        
        for (id, cfg) in datasets {
            let ds = builder.add_dataset(cfg.kind, id, cfg.first_block);
            for url in cfg.data_sources {
                ds.add_data_source(url);
            }
        }
        
        Ok(Arc::new(builder.build()))
    }
}