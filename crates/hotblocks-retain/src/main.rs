mod cli;
mod datasets;
mod hotblocks;
mod status;
mod types;

use anyhow::Context;
use clap::Parser;
use cli::Cli;
use std::collections::HashMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::time::Instant;
use types::{DatasetId, DatasetsConfig};
use url::Url;

fn main() -> anyhow::Result<()> {
    let args = Cli::parse();

    let datasets: DatasetsConfig = {
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
                datasets,
                Duration::from_secs(args.datasets_update_interval_secs),
                Duration::from_secs(args.retain_delay_secs),
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
    datasets: DatasetsConfig,
    datasets_update_interval: Duration,
    retain_delay: Duration,
    name_to_id: HashMap<String, DatasetId>,
    last_datasets_refresh: Instant,
    last_effective_from: Option<u64>,
}

impl HotblocksRetain {
    fn new(
        hotblocks_url: Url,
        status_url: Url,
        datasets_url: Url,
        datasets: DatasetsConfig,
        datasets_update_interval: Duration,
        retain_delay: Duration,
    ) -> Self {
        Self {
            client: reqwest::Client::new(),
            hotblocks_url,
            status_url,
            datasets_url,
            datasets,
            datasets_update_interval,
            retain_delay,
            name_to_id: HashMap::new(),
            last_datasets_refresh: Instant::now() - datasets_update_interval,
            last_effective_from: None,
        }
    }

    async fn run(&mut self) -> anyhow::Result<()> {
        loop {
            self.maybe_refresh_datasets().await;

            let status = match status::get_status(&self.client, self.status_url.as_str()).await {
                Ok(status) => status,
                Err(err) => {
                    tracing::warn!(error = ?err, "failed to fetch status");
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    continue;
                }
            };

            if self.last_effective_from == Some(status.effective_from) {
                tracing::info!("effective_from unchanged, re-checking in 5 minutes");
                tokio::time::sleep(Duration::from_secs(300)).await;
                continue;
            }

            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs();

            let apply_at = status.effective_from + self.retain_delay.as_secs();
            if now < apply_at {
                let wait_secs = apply_at - now;
                tracing::info!(
                    wait_secs,
                    effective_from = status.effective_from,
                    retain_delay_secs = self.retain_delay.as_secs(),
                    "waiting for effective time + retain delay"
                );
                tokio::time::sleep(Duration::from_secs(wait_secs)).await;
            }

            if self.apply_retention(&status).await {
                self.last_effective_from = Some(status.effective_from);
            } else {
                tokio::time::sleep(Duration::from_secs(30)).await;
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

    async fn apply_retention(&self, status: &status::SchedulingStatus) -> bool {
        let statuses = status
            .datasets
            .iter()
            .map(|dataset| (dataset.id.as_str(), dataset.height))
            .collect::<HashMap<_, _>>();

        let mut all_success = true;

        for (dataset, props) in &self.datasets {
            let dataset_id = if let Some(id) = props.as_ref().and_then(|p| p.id.as_deref()) {
                id
            } else {
                match self.name_to_id.get(dataset) {
                    Some(id) => id.as_str(),
                    None => {
                        tracing::warn!(dataset, "dataset not found in manifest, skipping");
                        continue;
                    }
                }
            };

            match statuses.get(dataset_id) {
                Some(Some(height)) => {
                    match hotblocks::set_retention(
                        &self.client,
                        &self.hotblocks_url,
                        dataset,
                        *height,
                    )
                    .await
                    {
                        Ok(()) => {
                            tracing::info!(dataset, height, "applied retention policy");
                        }
                        Err(err) => {
                            all_success = false;
                            tracing::warn!(dataset, height, error = ?err, "failed to apply retention");
                        }
                    }
                }
                Some(None) => {
                    tracing::info!(dataset, "dataset has no reported height yet");
                }
                None => {
                    tracing::warn!(dataset, "dataset not found in status");
                }
            }
        }

        all_success
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
