use std::{
    collections::{BTreeSet, HashSet},
    sync::Arc,
    time::Duration
};

use anyhow::Context;
use clap::Parser;
use sqd_storage::db::{DatabaseSettings, DatasetId};

use crate::{
    data_service::{DataService, DataServiceRef},
    dataset_config::{DatasetConfig, RetentionConfig},
    metrics::{ColumnFamilySizes, DatasetMetricsCollector, StorageMetricsCollector},
    query::{QueryService, QueryServiceRef},
    types::DBRef
};

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
pub struct CLI {
    /// Config file to get dataset specs from
    #[arg(short, long, value_name = "FILE")]
    pub datasets: String,

    /// Database directory
    #[arg(long = "db")]
    pub database_dir: String,

    #[arg(long, value_name = "MB", default_value = "256")]
    pub data_cache_size: usize,

    /// Max number of threads to use for query tasks
    #[arg(long, value_name = "N")]
    pub query_threads: Option<usize>,

    #[arg(long, hide = true)]
    pub query_task_queue: Option<usize>,

    #[arg(long, hide = true)]
    pub query_urgency: Option<usize>,

    /// Max number of queries waiting for new block arrival
    #[arg(long, value_name = "N", default_value = "64000")]
    pub query_max_data_waiters: usize,

    #[arg(long, default_value = "3000")]
    pub port: u16,

    /// Enable rocksdb stats collection
    #[arg(long)]
    pub rocksdb_stats: bool,

    #[arg(long)]
    pub rocksdb_disable_direct_io: bool,

    /// Max size of a single RocksDB info log file in MB (0 - unlimited)
    #[arg(long, value_name = "MB", default_value = "10")]
    pub rocksdb_max_log_file_size: usize,

    /// Max number of RocksDB info log files to keep
    #[arg(long, value_name = "N", default_value = "10")]
    pub rocksdb_keep_log_file_num: usize,

    /// Known client IDs for metrics labeling. Client IDs not in this list
    /// will be reported as "unknown" to prevent metrics cardinality abuse.
    #[arg(long = "known-client", value_name = "ID")]
    pub known_clients: Vec<String>,

    /// Refresh interval for hotblocks_dataset_size_bytes and hotblocks_column_family_size_bytes
    #[arg(long, value_name = "SECONDS", default_value = "60")]
    pub storage_stats_interval_secs: u64
}

pub struct App {
    pub db: DBRef,
    pub data_service: DataServiceRef,
    pub query_service: QueryServiceRef,
    pub api_controlled_datasets: BTreeSet<DatasetId>,
    pub metrics_registry: prometheus_client::registry::Registry,
    pub known_clients: HashSet<String>,
    pub storage_metrics_sender: tokio::sync::watch::Sender<ColumnFamilySizes>,
    pub storage_stats_interval: Duration
}

impl CLI {
    pub async fn build_app(&self) -> anyhow::Result<App> {
        let datasets = DatasetConfig::read_config_file(&self.datasets).context("failed to read datasets config")?;

        let db = DatabaseSettings::default()
            .with_data_cache_size(self.data_cache_size)
            .with_rocksdb_stats(self.rocksdb_stats)
            .with_direct_io(!self.rocksdb_disable_direct_io)
            .with_max_log_file_size(self.rocksdb_max_log_file_size)
            .with_keep_log_file_num(self.rocksdb_keep_log_file_num)
            .open(&self.database_dir)
            .map(Arc::new)
            .context("failed to open rocksdb database")?;

        let mut metrics_registry = crate::metrics::build_metrics_registry();

        let dataset_ids: Vec<DatasetId> = datasets.keys().copied().collect();

        let api_controlled_datasets = datasets
            .iter()
            .filter_map(|(id, cfg)| (cfg.retention_strategy == RetentionConfig::Api).then_some(*id))
            .collect();

        let storage_stats_interval = Duration::from_secs(self.storage_stats_interval_secs);

        let data_service = DataService::start(db.clone(), datasets, storage_stats_interval)
            .await
            .map(Arc::new)?;

        metrics_registry.register_collector(Box::new(DatasetMetricsCollector {
            data_service: data_service.clone(),
            datasets: dataset_ids
        }));

        let (storage_metrics_sender, storage_metrics_receiver) = tokio::sync::watch::channel(ColumnFamilySizes::new());
        metrics_registry.register_collector(Box::new(StorageMetricsCollector {
            receiver: storage_metrics_receiver
        }));

        let query_service = {
            let mut builder = QueryService::builder(db.clone());
            builder.set_max_data_waiters(self.query_max_data_waiters);

            if let Some(size) = self.query_task_queue {
                builder.set_max_pending_query_tasks(size);
            }

            if let Some(ms) = self.query_urgency {
                builder.set_urgency(ms);
            }
            let service = builder.build();
            metrics_registry.register_collector(Box::new(service.metrics_collector()));

            Arc::new(service)
        };

        let known_clients: HashSet<String> = self.known_clients.iter().cloned().collect();

        Ok(App {
            db,
            data_service,
            query_service,
            api_controlled_datasets,
            metrics_registry,
            known_clients,
            storage_metrics_sender,
            storage_stats_interval
        })
    }
}
