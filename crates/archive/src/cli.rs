use clap::{value_parser, Parser, ValueEnum};
use sqd_primitives::BlockNumber;
use url::Url;


#[derive(ValueEnum, Clone, Debug)]
pub enum NetworkKind {
    Solana,
    Hyperliquid,
    Evm,
}


#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
pub struct Cli {
    /// First block of a range to write
    #[arg(long, value_name = "N", default_value_t = 0)]
    pub first_block: BlockNumber,

    /// Last block of a range to write
    #[arg(long, value_name = "N")]
    pub last_block: Option<BlockNumber>,

    /// URL of the data ingestion service
    #[arg(short, long, value_name = "URL")]
    pub src: Url,

    /// Target dir or s3 location to write data to
    #[arg(short, long, value_name = "ARCHIVE")]
    pub dest: String,

    /// Number of chunks in top-level dir
    #[arg(long, value_name = "N", default_value_t = 1000)]
    pub top_dir_size: usize,

    /// Data chunk size in megabytes
    #[arg(long, value_name = "MB", default_value_t = 2048)]
    pub chunk_size: usize,

    /// Upper limit on the per file rows
    #[arg(long, value_name = "N", default_value_t = 200_000)]
    pub max_num_rows: usize,

    /// Network kind
    #[arg(long, value_enum)]
    pub network_kind: NetworkKind,

    /// Whether the logs should be structured in JSON format
    #[arg(long)]
    pub json_log: bool,

    /// Port to use for built-in prometheus metrics server
    #[arg(long)]
    pub prom_port: Option<u16>,

    // Interval between attempts to stream new blocks in seconds
    #[arg(long, value_parser = value_parser!(u16).range(1..), default_value_t = 300)]
    pub block_stream_interval: u16,
}
