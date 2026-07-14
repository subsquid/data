use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
    time::Instant
};

use anyhow::{Context, anyhow};
use futures::{FutureExt, StreamExt, TryStreamExt};
use sqd_data_client::reqwest::ReqwestDataClient;
use sqd_storage::db::{CF_TABLES, DatasetId};
use tracing::{error, info, warn};

use crate::{
    dataset_config::{DatasetConfig, RetentionConfig},
    dataset_controller::DatasetController,
    errors::UnknownDataset,
    types::{DBRef, RetentionStrategy}
};

pub type DataServiceRef = Arc<DataService>;

pub struct DataService {
    datasets: HashMap<DatasetId, Arc<DatasetController>>
}

impl DataService {
    pub async fn start(
        db: DBRef,
        datasets: BTreeMap<DatasetId, DatasetConfig>,
        disk_reclaim: bool
    ) -> anyhow::Result<Self> {
        let unconfigured: Vec<DatasetId> = db
            .get_all_datasets()?
            .into_iter()
            .filter(|ds| !datasets.contains_key(&ds.id))
            .map(|ds| ds.id)
            .collect();

        // Must run before any controller spawns -- see `startup_disk_recovery`.
        {
            let db = db.clone();
            let recovery = tokio::task::spawn_blocking(move || startup_disk_recovery(&db, &unconfigured, disk_reclaim));
            if let Err(err) = recovery.await {
                error!(error =? err, "startup disk recovery panicked");
            }
        }

        let mut controllers = futures::stream::iter(datasets.into_iter())
            .map(|(dataset_id, cfg)| {
                let db = db.clone();

                let http_client = sqd_data_client::reqwest::default_http_client();

                let data_sources = cfg
                    .data_sources
                    .into_iter()
                    .map(|url| ReqwestDataClient::new(http_client.clone(), url))
                    .collect();

                let retention = match cfg.retention_strategy {
                    RetentionConfig::FromBlock { number, parent_hash } => {
                        RetentionStrategy::FromBlock { number, parent_hash }
                    }
                    RetentionConfig::Head(n) => RetentionStrategy::Head(n),
                    RetentionConfig::Api | RetentionConfig::None => RetentionStrategy::None
                };

                tokio::task::spawn_blocking(move || {
                    DatasetController::new(db, dataset_id, cfg.kind, retention, data_sources).map(|c| {
                        c.enable_compaction(!cfg.disable_compaction);
                        Arc::new(c)
                    })
                })
                .map(move |res| res.with_context(|| anyhow!("failed to initialize dataset {}", dataset_id)))
            })
            .buffered(5);

        let mut datasets = HashMap::new();

        while let Some(ctl) = controllers.try_next().await?.transpose()? {
            datasets.insert(ctl.dataset_id(), ctl);
        }

        Ok(Self { datasets })
    }

    pub fn get_dataset(&self, dataset_id: DatasetId) -> Result<Arc<DatasetController>, UnknownDataset> {
        self.datasets
            .get(&dataset_id)
            .map(Arc::clone)
            .ok_or(UnknownDataset { dataset_id })
    }
}

/// Startup-only disk recovery; must run before any ingest or query exists (the file
/// unlink ignores snapshots, and the orphan purge treats every dirty marker as an orphan
/// from a dead build).
///
/// `disk_reclaim` gates both reclaim steps (`--startup-disk-reclaim`, off by default);
/// deleting unconfigured datasets always runs. Ordering matters on a near-full disk:
/// 1. an unlink pass first -- it needs no scratch space, so it frees below-watermark space
///    where every write below would fail with ENOSPC;
/// 2. bookkeeping writes that lift the reclaim watermark: purge orphan dirty markers,
///    delete unconfigured datasets;
/// 3. a second unlink pass to free whatever step 2 unpinned.
///
/// Every step is best-effort: a failure leaves the watermark pinned until a later startup
/// succeeds, but never blocks startup.
///
/// Does not rescue a volume at literally zero free bytes: the database has already opened
/// by the time we get here, and opening replays the WAL and flushes it to L0.
fn startup_disk_recovery(db: &DBRef, unconfigured: &[DatasetId], disk_reclaim: bool) {
    let started = Instant::now();
    let bytes_before = table_sst_bytes(db);

    let mut orphans_purged = 0usize;
    let mut unconfigured_deleted = 0usize;

    if disk_reclaim {
        if let Err(err) = db.reclaim_disk_space() {
            error!(error =? err, "startup disk reclaim (first pass) failed");
        }

        match db.purge_orphan_dirty_tables() {
            Ok(n) => orphans_purged = n,
            Err(err) => warn!(error =? err, "failed to purge orphan dirty tables")
        }
    }

    for dataset_id in unconfigured {
        match db.delete_dataset(*dataset_id) {
            Ok(()) => unconfigured_deleted += 1,
            Err(err) => {
                error!("failed to delete dataset {dataset_id}: {err}; its chunks keep pinning the reclaim watermark")
            }
        }
    }

    if disk_reclaim {
        if let Err(err) = db.reclaim_disk_space() {
            error!(error =? err, "startup disk reclaim (second pass) failed");
        }
    }

    let bytes_after = table_sst_bytes(db);
    // Only meaningful when both probes answered; a missing probe must not read as "freed 0".
    let freed_bytes = bytes_before
        .zip(bytes_after)
        .map(|(before, after)| before.saturating_sub(after));

    info!(
        reclaim_enabled = disk_reclaim,
        orphans_purged,
        unconfigured_datasets = unconfigured.len(),
        unconfigured_deleted,
        table_sst_bytes_before = bytes_before,
        table_sst_bytes_after = bytes_after,
        freed_bytes,
        elapsed_ms = started.elapsed().as_millis() as u64,
        "startup disk recovery complete"
    );

    if !disk_reclaim {
        // FUTURE: ungate the orphan purge once the rollout measurement is done. It is safe
        // without the unlink, and while gated an interrupted build leaks its data for good;
        // it shares the flag only so `reclaim-measure` can still contrast the two watermarks.
        info!("startup disk reclaim is off; enable with --startup-disk-reclaim");
    }
}

/// Live SST bytes of `CF_TABLES`, straight from RocksDB metadata -- no SST reads. `None`
/// when the property is unavailable, so a probe failure stays distinguishable from zero.
fn table_sst_bytes(db: &DBRef) -> Option<u64> {
    match db.get_property(CF_TABLES, "rocksdb.live-sst-files-size") {
        Ok(value) => value.and_then(|v| v.parse().ok()),
        Err(err) => {
            warn!(error =? err, "failed to read live SST size");
            None
        }
    }
}
