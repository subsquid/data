use crate::dataset_config::{DatasetConfig, RetentionConfig};
use crate::dataset_controller::DatasetController;
use crate::errors::UnknownDataset;
use crate::types::{DBRef, RetentionStrategy};
use anyhow::{anyhow, Context};
use futures::FutureExt;
use futures::{StreamExt, TryStreamExt};
use sqd_data_client::reqwest::ReqwestDataClient;
use sqd_storage::db::DatasetId;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;


pub type DataServiceRef = Arc<DataService>;


pub struct DataService {
    datasets: HashMap<DatasetId, Arc<DatasetController>>,
}


impl DataService {
    pub async fn start(db: DBRef, datasets: BTreeMap<DatasetId, DatasetConfig>) -> anyhow::Result<Self> {
        let mut controllers = futures::stream::iter(datasets.into_iter())
            .map(|(dataset_id, cfg)| {
                let db = db.clone();

                let http_client = sqd_data_client::reqwest::default_http_client();

                let data_sources = cfg.data_sources.into_iter()
                    .map(|url| ReqwestDataClient::new(http_client.clone(), url))
                    .collect();

                let retention = match cfg.retention {
                    RetentionConfig::FromBlock { number, parent_hash } => RetentionStrategy::FromBlock {
                        number ,
                        parent_hash
                    },
                    RetentionConfig::Head(n) => RetentionStrategy::Head(n),
                    RetentionConfig::Admin | RetentionConfig::None =>RetentionStrategy::None
                };

                tokio::task::spawn_blocking(move || {
                    DatasetController::new(
                        db,
                        dataset_id,
                        cfg.kind,
                        retention,
                        data_sources
                    ).map(|c| {
                        c.enable_compaction(!cfg.disable_compaction);
                        Arc::new(c)
                    })
                }).map(move |res| {
                    res.with_context(|| {
                        anyhow!("failed to initialize dataset {}", dataset_id)
                    })
                })
            })
            .buffered(5);

        let mut datasets = HashMap::new();

        while let Some(ctl) = controllers.try_next().await?.transpose()? {
            datasets.insert(ctl.dataset_id(), ctl);
        }

        Ok(Self {
            datasets
        })
    }

    pub fn get_dataset(&self, dataset_id: DatasetId) -> Result<Arc<DatasetController>, UnknownDataset> {
        self.datasets.get(&dataset_id)
            .map(Arc::clone)
            .ok_or(UnknownDataset {
                dataset_id
            })
    }
}