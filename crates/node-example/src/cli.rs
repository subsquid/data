use crate::dataset_config::DatasetConfig;
use anyhow::Context;
use clap::Parser;
use sqd_data_client::reqwest::ReqwestDataClient;
use sqd_node::{DBRef, Node, NodeBuilder};
use sqd_storage::db::DatabaseSettings;
use std::sync::Arc;


#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
pub struct CLI {
    /// Config file to get dataset specs from
    #[arg(short, long, value_name = "FILE")]
    pub datasets: String,

    /// Database directory
    #[arg(long = "db")]
    pub database_dir: String,

    #[arg(long, value_name = "MB", default_value = "4096")]
    pub data_cache_size: usize,
    
    /// Max number of threads to use for query execution
    #[arg(long)]
    pub query_threads: Option<usize>
}


impl CLI {
    pub async fn build_node(&self) -> anyhow::Result<(Arc<Node>, DBRef)> {
        let datasets = DatasetConfig::read_config_file(&self.datasets)
            .context("failed to read datasets config")?;

        let db = DatabaseSettings::default()
            .set_data_cache_size(self.data_cache_size)
            .open(&self.database_dir)
            .map(Arc::new)
            .context("failed to open rocksdb database")?;
        
        let mut builder = NodeBuilder::new(db.clone());
        
        let http_client = sqd_data_client::reqwest::default_http_client();
        
        for (id, cfg) in datasets {
            let data_sources = cfg.data_sources.into_iter()
                .map(|url| ReqwestDataClient::new(http_client.clone(), url))
                .collect();
            
            builder.add_dataset(
                cfg.kind, 
                id,
                data_sources,
                cfg.retention
            );
        }
        
        let node = builder.build().await.map(Arc::new)?;
        
        Ok((node, db))
    }
}