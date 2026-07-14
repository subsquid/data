use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc
};

use anyhow::{Context, anyhow};
use futures::{FutureExt, StreamExt, TryStreamExt};
use sqd_data_client::reqwest::ReqwestDataClient;
use sqd_storage::db::DatasetId;
use tracing::{debug, error, info, warn};

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
/// `disk_reclaim` gates only whole-file unlink (`--startup-disk-reclaim`, off by default).
/// Orphan cleanup and deleting unconfigured datasets always run. Ordering matters on a
/// near-full disk:
/// 1. an optional unlink pass first -- it needs no scratch space, so it can free room for
///    the bookkeeping writes below;
/// 2. bookkeeping writes that lift the watermark: purge orphan dirty markers and delete
///    unconfigured datasets;
/// 3. an optional second unlink pass to free whatever step 2 unpinned.
///
/// Every step is best-effort: a failure leaves the watermark pinned until a later startup
/// succeeds, but never blocks startup.
///
/// Does not rescue a volume at literally zero free bytes: the database has already opened
/// by the time we get here, and opening replays the WAL and flushes it to L0.
fn startup_disk_recovery(db: &DBRef, unconfigured: &[DatasetId], disk_reclaim: bool) {
    if disk_reclaim {
        if let Err(err) = db.reclaim_disk_space() {
            error!(error =? err, "startup disk reclaim (first pass) failed");
        }
    } else {
        info!("startup disk reclaim is off; enable with --startup-disk-reclaim");
    }

    match db.purge_orphan_dirty_tables() {
        Ok(0) => {}
        Ok(n) => info!("purged {n} orphan dirty table(s) left by an interrupted build"),
        Err(err) => warn!(error =? err, "failed to purge orphan dirty tables")
    }

    for dataset_id in unconfigured {
        info!("deleting unconfigured dataset {dataset_id}");
        if let Err(err) = db.delete_dataset(*dataset_id) {
            error!("failed to delete dataset {dataset_id}: {err}; its chunks keep pinning the reclaim watermark");
        }
    }

    if disk_reclaim {
        if let Err(err) = db.reclaim_disk_space() {
            error!(error =? err, "startup disk reclaim (second pass) failed");
        }
    }

    debug!("startup disk recovery complete");
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::{DataType, Field, Schema};
    use sqd_storage::db::DatabaseSettings;

    use super::*;

    #[test]
    fn startup_always_purges_orphans_when_file_unlink_is_disabled() {
        let dir = tempfile::tempdir().unwrap();
        {
            let db = DatabaseSettings::default().open(dir.path()).unwrap();
            let schema = Arc::new(Schema::new(vec![Field::new("data", DataType::UInt32, true)]));
            db.new_table_builder(schema).finish().unwrap();
        }
        let db = Arc::new(DatabaseSettings::default().open(dir.path()).unwrap());

        startup_disk_recovery(&db, &[], false);

        assert_eq!(db.purge_orphan_dirty_tables().unwrap(), 0);
    }
}
