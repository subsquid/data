use std::path::PathBuf;

use clap::Parser;
use url::Url;

#[derive(Parser, Debug)]
#[command(version, about = "Hotblocks retain service", long_about = None)]
pub struct Cli {
    /// URL of the Hotblocks service to send dataset information to
    #[arg(long)]
    pub hotblocks_url: Url,

    /// URL of the status endpoint to poll for dataset updates
    #[arg(long)]
    pub status_url: Url,

    /// Path to the YAML config file listing datasets to track
    #[arg(long)]
    pub datasets_config: PathBuf,

    /// URL of the datasets YAML file listing available network datasets
    #[arg(long)]
    pub datasets_url: Url,

    /// Interval in seconds between refreshing the datasets list
    #[arg(long, default_value = "3600", value_parser = clap::value_parser!(u64).range(1..))]
    pub datasets_update_interval_secs: u64,

    /// Interval in seconds between polling the status endpoint
    #[arg(long, default_value = "60", value_parser = clap::value_parser!(u64).range(1..))]
    pub poll_interval_secs: u64,
}
