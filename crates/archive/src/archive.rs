use crate::chain_builder::{ChainBuilder, ChainBuilderBox};
use crate::cli::{Cli, NetworkKind};
use crate::fs::create_fs;
use crate::ingest::ingest_from_service;
use crate::layout::Layout;
use crate::writer::ParquetWriter;
use anyhow::ensure;
use sqd_data::solana::tables::SolanaChunkBuilder;
use sqd_data_types::BlockNumber;


pub async fn run(args: &Cli) -> anyhow::Result<()> {
    ensure!(
        args.first_block <= args.last_block.unwrap_or(BlockNumber::MAX),
        "--first-block is greater than --last-block"
    );

    let fs = create_fs(&args.dest).await?;
    let layout = Layout::new(fs.clone());

    let chunk_writer = layout.create_chunk_writer(
        &chunk_check,
        args.top_dir_size,
        args.first_block,
        args.last_block
    ).await?;

    if let Some(last_block) = args.last_block {
        if chunk_writer.next_block() > last_block {
            println!("nothing to do");
            return Ok(());
        }
    }

    let chunk_builder: ChainBuilderBox = match args.network_kind {
        NetworkKind::Solana => Box::new(
            ChainBuilder::<SolanaChunkBuilder>::default()
        ),
    };

    let writer = ParquetWriter::new(chunk_builder);

    let block_stream = ingest_from_service(
        args.src.clone(),
        chunk_writer.next_block(),
        args.last_block
    );
    
    let prev_chunk_hash = chunk_writer.prev_chunk_hash();

    todo!()
}


fn chunk_check(filelist: &[String]) -> bool {
    for file in filelist {
        if file.starts_with("blocks.parquet") {
            return true;
        }
    }
    false
}


fn short_hash(value: &str) -> &str {
    &value[value.len().saturating_sub(5)..]
}