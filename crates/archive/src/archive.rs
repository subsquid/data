use crate::chain_builder::{ChainBuilder, ChainBuilderBox};
use crate::cli::{Cli, NetworkKind};
use crate::fs::create_fs;
use crate::layout::Layout;
use crate::metrics;
use crate::processor::LineProcessor;
use crate::server::run_server;
use crate::sink::Sink;
use crate::writer::{Writer, WriterItem};
use anyhow::{ensure, Context};
use prometheus_client::registry::Registry;
use sqd_data::solana::tables::SolanaChunkBuilder;
use sqd_data::hyperliquid::tables::HyperliquidChunkBuilder;
use sqd_primitives::BlockNumber;
use std::time::Duration;


pub async fn run(args: &Cli) -> anyhow::Result<()> {
    ensure!(
        args.first_block <= args.last_block.unwrap_or(BlockNumber::MAX),
        "--first-block is greater than --last-block"
    );

    init_logging(args.json_log);

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

    let chunk_builder: ChainBuilderBox = match args.network_kind {
        NetworkKind::Solana => Box::new(
            ChainBuilder::<SolanaChunkBuilder>::default(),
        ),
        NetworkKind::Hyperliquid => Box::new(
            ChainBuilder::<HyperliquidChunkBuilder>::default(),
        ),
    };

    let processor = LineProcessor::new(chunk_builder)?;

    let (chunk_sender, chunk_receiver) = tokio::sync::mpsc::channel(5);

    let block_stream_interval = Duration::from_secs(args.block_stream_interval.into());
    
    let mut sink = Sink::new(
        processor,
        chunk_writer,
        args.chunk_size,
        args.max_num_rows,
        args.src.clone(),
        block_stream_interval,
        args.last_block,
        chunk_sender,
    );
    
    let sink_task = tokio::spawn(async move { 
        sink.r#loop().await 
    });

    let write_task = tokio::spawn(async move {
        let mut writer = Writer::new(fs, chunk_receiver);
        writer.start().await 
    });
    
    match write_task.await.context("write task panicked") {
        Ok(Ok(_)) => {
            sink_task.await.context("sink task panicked")??;
        },
        Ok(Err(err)) => {
            sink_task.abort();
            return Err(err)
        },
        Err(err) => {
            sink_task.abort();
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


fn init_logging(json: bool) {
    let env_filter = tracing_subscriber::EnvFilter::builder().parse_lossy(
        std::env::var(tracing_subscriber::EnvFilter::DEFAULT_ENV)
            .unwrap_or(format!("{}=info", env!("CARGO_CRATE_NAME"))),
    );

    if json {
        tracing_subscriber::fmt()
            .with_env_filter(env_filter)
            .with_target(false)
            .json()
            .flatten_event(true)
            .init();
    } else {
        tracing_subscriber::fmt()
            .with_env_filter(env_filter)
            .with_target(false)
            .init();
    }
}
