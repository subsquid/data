use clap::Parser;
use url::Url;

#[derive(Parser, Debug)]
#[command(version, about = "Hotblocks sidecar service", long_about = None)]
pub struct Cli {
    /// URL of the Hotblocks service to send dataset information to
    #[arg(long)]
    pub hotblocks_url: Url,

    /// URL of the status endpoint to poll for dataset updates
    #[arg(long)]
    pub status_url: Url,

    /// Dataset identifiers to track (can be specified multiple times)
    #[arg(long = "dataset")]
    pub dataset: Vec<String>,

    /// Interval in seconds between polling the status endpoint
    #[arg(long, default_value = "60")]
    pub poll_interval_secs: u64,
}
