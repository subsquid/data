use std::{
    collections::{BTreeSet, HashSet},
    sync::Arc,
    time::Instant
};

use anyhow::Context;
use clap::Parser;
use sqd_storage::db::{DatabaseSettings, DatasetId};
use tracing::info;

use crate::{
    data_service::{DataService, DataServiceRef},
    dataset_config::{DatasetConfig, RetentionConfig},
    metrics::{DatasetMetricsCollector, RocksDbCollector},
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

    /// After SIGTERM, keep serving for this long while `/ready` reports 503, so the
    /// orchestrator withdraws our endpoint before any connection closes.
    #[arg(long, value_name = "SECS", default_value = "25")]
    pub pre_drain_grace_secs: u64,

    /// Hard cap on the drain that follows. The orchestrator's kill timeout must exceed
    /// `pre_drain_grace + drain_timeout`.
    #[arg(long, value_name = "SECS", default_value = "25")]
    pub drain_timeout_secs: u64,

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

    /// Concurrent RocksDB background flush + compaction jobs.
    /// Defaults to the core count, clamped to 2..=8.
    #[arg(long, value_name = "N")]
    pub rocksdb_max_background_jobs: Option<usize>,

    /// Rewrite every table SST older than this, collecting dead data that never made a file
    /// tombstone-dense enough for the deletion collector. Lower means faster reclaim and
    /// proportionally more write amplification. 0 disables it, leaving RocksDB's 30-day
    /// `ttl` as the only backstop.
    #[arg(long, value_name = "SECS", default_value_t = sqd_storage::db::DEFAULT_PERIODIC_COMPACTION_SECS)]
    pub rocksdb_periodic_compaction_secs: u64,

    /// Reclaim dead disk at startup: purge orphaned dirty-table markers, then unlink whole
    /// SST files below the live-table watermark. Off by default -- the unlink ignores
    /// snapshots, so it is only safe before any query exists. Use the `reclaim-measure`
    /// binary (shipped in this image) to size the win first.
    ///
    /// Note that this runs *after* the database opens, and opening replays the WAL and
    /// flushes it to L0. A volume at literally zero free bytes needs a few hundred MB
    /// cleared by hand before the process can get far enough to reclaim anything.
    #[arg(long)]
    pub startup_disk_reclaim: bool,

    /// Index block hashes of newly ingested chunks, enabling
    /// `GET /datasets/{id}/hashes/{hash}/block`. EVM datasets only.
    ///
    /// No backfill: pre-existing chunks stay unresolvable until they roll off
    /// via retention. Entries drain as chunks are pruned after switching off.
    #[arg(long)]
    pub block_hash_index: bool,

    /// Index transaction hashes of newly ingested chunks, enabling
    /// `GET /datasets/{id}/hashes/{hash}/transaction`. EVM datasets only.
    ///
    /// Independent of `--block-hash-index` and off by default because this
    /// index has one entry per transaction. No backfill; entries drain through
    /// retention after switching it off.
    #[arg(long)]
    pub transaction_hash_index: bool,

    /// Known client IDs for metrics labeling. Client IDs not in this list
    /// will be reported as "unknown" to prevent metrics cardinality abuse.
    #[arg(long = "known-client", value_name = "ID")]
    pub known_clients: Vec<String>
}

pub struct App {
    pub db: DBRef,
    pub data_service: DataServiceRef,
    pub query_service: QueryServiceRef,
    pub api_controlled_datasets: BTreeSet<DatasetId>,
    pub metrics_registry: prometheus_client::registry::Registry,
    pub known_clients: HashSet<String>
}

impl CLI {
    pub async fn build_app(&self) -> anyhow::Result<App> {
        let datasets = DatasetConfig::read_config_file(&self.datasets).context("failed to read datasets config")?;

        let mut settings = DatabaseSettings::default()
            .with_data_cache_size(self.data_cache_size)
            .with_rocksdb_stats(self.rocksdb_stats)
            .with_direct_io(!self.rocksdb_disable_direct_io)
            .with_max_log_file_size(self.rocksdb_max_log_file_size)
            .with_keep_log_file_num(self.rocksdb_keep_log_file_num)
            .with_periodic_compaction_secs(self.rocksdb_periodic_compaction_secs)
            .with_block_hash_index(self.block_hash_index)
            .with_transaction_hash_index(self.transaction_hash_index);

        if let Some(jobs) = self.rocksdb_max_background_jobs {
            settings = settings.with_max_background_jobs(jobs);
        }

        let db_open_started = Instant::now();
        info!("opening RocksDB");
        let db = settings
            .open(&self.database_dir)
            .map(Arc::new)
            .context("failed to open rocksdb database")?;
        info!(
            elapsed_ms = db_open_started.elapsed().as_millis() as u64,
            "RocksDB opened"
        );

        let mut metrics_registry = crate::metrics::build_metrics_registry();
        metrics_registry.register_collector(Box::new(DatasetMetricsCollector {
            db: db.clone(),
            datasets: datasets.keys().copied().collect()
        }));
        metrics_registry.register_collector(Box::new(RocksDbCollector { db: db.clone() }));

        let api_controlled_datasets = datasets
            .iter()
            .filter_map(|(id, cfg)| matches!(cfg.retention_strategy, RetentionConfig::Api { .. }).then_some(*id))
            .collect();

        let data_service = DataService::start(db.clone(), datasets, self.startup_disk_reclaim)
            .await
            .map(Arc::new)?;

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
            known_clients
        })
    }
}
