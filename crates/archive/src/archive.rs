use crate::cli::{Cli, NetworkKind};
use crate::fs::create_fs;
use crate::ingest::ingest_from_service;
use crate::layout::Layout;
use crate::metrics;
use crate::proc::Proc;
use crate::server::run_server;
use crate::writer::Writer;
use anyhow::{ensure, Context};
use prometheus_client::registry::Registry;
use sqd_data::evm::tables::EvmChunkBuilder;
use sqd_data::hyperliquid::tables::HyperliquidChunkBuilder;
use sqd_data::solana::tables::SolanaChunkBuilder;
use sqd_primitives::BlockNumber;
use std::time::Duration;


pub async fn run(args: Cli) -> anyhow::Result<()> {
    ensure!(
        args.first_block <= args.last_block.unwrap_or(BlockNumber::MAX),
        "--first-block is greater than --last-block"
    );

    let fs = create_fs(&args.dest).await?;
    let layout = Layout::new(fs.clone());

    let chunk_tracker = layout.create_chunk_tracker(
        &chunk_check,
        args.top_dir_size,
        args.first_block,
        args.last_block
    ).await?;

    if let Some(last_block) = args.last_block {
        if chunk_tracker.next_block() > last_block {
            tracing::info!("nothing to do");
            return Ok(());
        }
    }

    if let Some(prom_port) = args.prom_port {
        let mut metrics_registry = Registry::default();
        metrics::register_metrics(&mut metrics_registry);
        let server = run_server(metrics_registry, prom_port);
        tokio::spawn(server);
    }

    let (chunk_sender, chunk_receiver) = tokio::sync::mpsc::channel(5);

    macro_rules! proc {
        ($chunk_builder:expr) => {{
            let block_stream = ingest_from_service(
                args.src,
                chunk_tracker.next_block(),
                args.last_block,
                Duration::from_secs(args.block_stream_interval.into())
            );
            let mut proc = Proc::new($chunk_builder, chunk_tracker, chunk_sender)?;
            proc.set_max_chunk_size(args.chunk_size);
            proc.set_max_num_rows(args.max_num_rows);
            tokio::spawn(proc.run(block_stream))
        }};
    }

    let proc_task = match args.network_kind {
        NetworkKind::Solana => proc!(SolanaChunkBuilder::default()),
        NetworkKind::Hyperliquid => proc!(HyperliquidChunkBuilder::default()),
        NetworkKind::Evm => proc!(EvmChunkBuilder::default())
    };

    let attach_idx_field = args.attach_idx_field;
    let write_task = tokio::spawn(async move {
        let mut writer = Writer::new(fs, chunk_receiver, attach_idx_field);
        writer.start().await 
    });
    
    match write_task.await.context("write task panicked") {
        Ok(Ok(_)) => {
            proc_task.await.context("processing task panicked")??;
        },
        Ok(Err(err)) => {
            proc_task.abort();
            return Err(err)
        },
        Err(err) => {
            proc_task.abort();
            return Err(err)
        }
    }

    Ok(())
}


fn chunk_check(filelist: &[String]) -> bool {
    for file in filelist {
        if file.starts_with("blocks.parquet") {
            return true;
        }
    }
    false
}