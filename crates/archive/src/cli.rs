use clap::{Parser, ValueEnum};


#[derive(ValueEnum, Clone, Debug)]
pub enum NetworkKind {
    Solana,
}


#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
pub struct Cli {
    /// First block of a range to write
    #[arg(long, value_name = "N", default_value_t = 0)]
    pub first_block: u64,

    /// Last block of a range to write
    #[arg(long, value_name = "N")]
    pub last_block: Option<u64>,

    /// URL of the data ingestion service
    #[arg(short, long, value_name = "URL")]
    pub src: String,

    /// Target dir or s3 location to write data to
    #[arg(short, long, value_name = "ARCHIVE")]
    pub dest: String,

    /// Number of chunks in top-level dir
    #[arg(long, value_name = "N", default_value_t = 500)]
    pub top_dir_size: usize,

    /// Data chunk size in megabytes
    #[arg(long, value_name = "MB", default_value_t = 1024)]
    pub chunk_size: usize,

    /// Network kind
    #[arg(long, value_enum)]
    pub network_kind: NetworkKind,
}
