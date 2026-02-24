mod cli;
mod datasets;
mod hotblocks;
mod status;
mod types;

use anyhow::Context;
use clap::Parser;
use cli::Cli;
use std::collections::HashMap;
use std::time::Duration;
use tokio::time::Instant;
use types::{DatasetConfig, DatasetId, DatasetsConfig};
use url::Url;

fn main() -> anyhow::Result<()> {
    let args = Cli::parse();

    let datasets_config: DatasetsConfig = {
        let contents = std::fs::read_to_string(&args.datasets_config)
            .with_context(|| format!("failed to read {}", args.datasets_config.display()))?;
        serde_yaml::from_str(&contents)
            .with_context(|| format!("failed to parse {}", args.datasets_config.display()))?
    };

    init_tracing();

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(
            HotblocksRetain::new(
                args.hotblocks_url,
                args.status_url,
                args.datasets_url,
                datasets_config.datasets,
                Duration::from_secs(args.poll_interval_secs),
                Duration::from_secs(args.datasets_update_interval_secs),
            )
            .run(),
        )?;

    Ok(())
}

struct HotblocksRetain {
    client: reqwest::Client,
    hotblocks_url: Url,
    status_url: Url,
    datasets_url: Url,
    datasets: Vec<DatasetConfig>,
    poll_interval: Duration,
    datasets_update_interval: Duration,
    name_to_id: HashMap<String, DatasetId>,
    last_datasets_refresh: Instant,
}

impl HotblocksRetain {
    fn new(
        hotblocks_url: Url,
        status_url: Url,
        datasets_url: Url,
        datasets: Vec<DatasetConfig>,
        poll_interval: Duration,
        datasets_update_interval: Duration,
    ) -> Self {
        Self {
            client: reqwest::Client::new(),
            hotblocks_url,
            status_url,
            datasets_url,
            datasets,
            poll_interval,
            datasets_update_interval,
            name_to_id: HashMap::new(),
            last_datasets_refresh: Instant::now() - datasets_update_interval,
        }
    }

    async fn run(&mut self) -> anyhow::Result<()> {
        let mut interval = tokio::time::interval(self.poll_interval);

        loop {
            interval.tick().await;
            self.maybe_refresh_datasets().await;

            if let Err(err) = self.poll_once().await {
                tracing::warn!(error = ?err, "failed to refresh retention settings");
            }
        }
    }

    async fn maybe_refresh_datasets(&mut self) {
        if self.last_datasets_refresh.elapsed() < self.datasets_update_interval {
            return;
        }

        match datasets::get_name_to_id(&self.client, &self.datasets_url).await {
            Ok(map) => {
                tracing::info!("refreshed datasets manifest");
                self.name_to_id = map;
                self.last_datasets_refresh = Instant::now();
            }
            Err(err) => {
                tracing::warn!(error = ?err, "failed to refresh datasets manifest");
            }
        }
    }

    async fn poll_once(&self) -> anyhow::Result<()> {
        let status = status::get_status(&self.client, self.status_url.as_str()).await?;

        let statuses = status
            .datasets
            .into_iter()
            .map(|dataset| (dataset.id, dataset.height))
            .collect::<HashMap<_, _>>();

        for dataset in &self.datasets {
            let dataset_name = dataset.name.as_str();
            let dataset_id = if let Some(id) = &dataset.id {
                id.as_str()
            } else {
                match self.name_to_id.get(dataset_name) {
                    Some(id) => id.as_str(),
                    None => {
                        tracing::warn!(dataset = dataset_name, "dataset not found in manifest, skipping");
                        continue;
                    }
                }
            };

            match statuses.get(dataset_id) {
                Some(Some(height)) => {
                    hotblocks::set_retention(&self.client, &self.hotblocks_url, dataset_name, *height)
                        .await
                        .with_context(|| format!("failed to update retention for {dataset_name}"))?;
                    tracing::info!(dataset = dataset_name, height, "updated retention policy");
                }
                Some(None) => {
                    tracing::info!(dataset = dataset_name, "dataset has no reported height yet");
                }
                None => {
                    tracing::warn!(dataset = dataset_name, "dataset not found in status json");
                }
            }
        }

        Ok(())
    }
}

fn init_tracing() {
    use std::io::IsTerminal;

    let env_filter = tracing_subscriber::EnvFilter::builder().parse_lossy(
        std::env::var(tracing_subscriber::EnvFilter::DEFAULT_ENV).unwrap_or("info".to_string()),
    );

    if std::io::stdout().is_terminal() {
        tracing_subscriber::fmt()
            .with_env_filter(env_filter)
            .compact()
            .init();
    } else {
        tracing_subscriber::fmt()
            .with_env_filter(env_filter)
            .json()
            .with_current_span(false)
            .init();
    }
}
