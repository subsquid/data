use crate::chain_builder::{ChainBuilder, ChainBuilderBox};
use crate::cli::{Cli, NetworkKind};
use crate::fs::create_fs;
use crate::layout::Layout;
use crate::metrics;
use crate::processor::LineProcessor;
use crate::server::run_server;
use crate::sink::Sink;
use crate::writer::{Writer, WriterItem};
use anyhow::ensure;
use futures_util::FutureExt;
use sqd_data::solana::tables::SolanaChunkBuilder;
use sqd_data_types::BlockNumber;
use std::time::Duration;
use prometheus_client::registry::Registry;


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
    };

    let processor = LineProcessor::new(chunk_builder);

    let (chunk_sender, chunk_receiver) = tokio::sync::mpsc::unbounded_channel::<WriterItem>();

    let block_stream_interval = Duration::from_secs(args.block_stream_interval.into());
    let mut sink = Sink::new(
        processor,
        chunk_writer,
        args.chunk_size,
        args.src.clone(),
        block_stream_interval,
        args.last_block,
        chunk_sender,
    );
    let mut writer = Writer::new(fs, chunk_receiver);

    tokio::try_join!(
        async {
            let res = sink.r#loop().await;
            // manual drop should close writer's channel
            drop(sink);
            res
        },
        writer.start()
    )?;

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
            .unwrap_or(format!("{}=info", std::env!("CARGO_CRATE_NAME"))),
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
