mod cli;
mod hotblocks;
mod status;

use anyhow::Context;
use clap::Parser;
use cli::Cli;
use std::collections::HashMap;
use std::time::Duration;

fn main() -> anyhow::Result<()> {
    let args = Cli::parse();

    init_tracing();

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(run(args))?;

    Ok(())
}

async fn run(args: Cli) -> anyhow::Result<()> {
    if args.poll_interval_secs == 0 {
        anyhow::bail!("poll interval must be greater than 0");
    }

    let client = reqwest::Client::new();
    let poll_interval = Duration::from_secs(args.poll_interval_secs);
    let mut interval = tokio::time::interval(poll_interval);

    loop {
        let result = poll_once(&client, &args).await;
        if let Err(err) = result {
            tracing::warn!(error = ?err, "failed to refresh retention settings");
        }

        interval.tick().await;
    }
}

async fn poll_once(client: &reqwest::Client, args: &Cli) -> anyhow::Result<()> {
    let status = status::get_status(client, args.status_url.as_str()).await?;

    let statuses = status
        .datasets
        .into_iter()
        .map(|dataset| (dataset.id, dataset.height))
        .collect::<HashMap<_, _>>();

    for dataset in &args.dataset {
        match statuses.get(dataset) {
            Some(Some(height)) => {
                hotblocks::set_retention(client, &args.hotblocks_url, dataset, *height)
                    .await
                    .with_context(|| format!("failed to update retention for {dataset}"))?;
                tracing::info!(dataset, height, "updated retention policy");
            }
            Some(None) => {
                tracing::info!(dataset, "dataset has no reported height yet");
            }
            None => {
                tracing::warn!(dataset, "dataset not found in status json");
            }
        }
    }

    Ok(())
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
