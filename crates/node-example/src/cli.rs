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
    pub query_threads: Option<usize>,

    #[arg(long, hide = true)]
    pub query_queue_size: Option<usize>,

    #[arg(long, hide = true)]
    pub query_urgency: Option<usize>,

    #[arg(long, default_value = "3000")]
    pub port: u16
}


impl CLI {
    pub async fn build_node(&self) -> anyhow::Result<(Arc<Node>, DBRef)> {
        let datasets = DatasetConfig::read_config_file(&self.datasets)
            .context("failed to read datasets config")?;

        let db = DatabaseSettings::default()
            .with_data_cache_size(self.data_cache_size)
            .open(&self.database_dir)
            .map(Arc::new)
            .context("failed to open rocksdb database")?;
        
        let mut builder = NodeBuilder::new(db.clone());

        if let Some(size) = self.query_queue_size {
            builder.set_query_queue_size(size);
        }

        if let Some(n) = self.query_urgency {
            builder.set_query_urgency(n);
        }
        
        let http_client = sqd_data_client::reqwest::default_http_client();
        
        for (id, cfg) in datasets {
            let data_sources = cfg.data_sources.into_iter()
                .map(|url| ReqwestDataClient::new(http_client.clone(), url))
                .collect();
            
            let ds = builder.add_dataset(
                id,
                cfg.kind,
                data_sources,
                cfg.retention
            );
            ds.enable_compaction(cfg.enable_compaction);
        }
        
        let node = builder.build().await.map(Arc::new)?;
        
        Ok((node, db))
    }
}