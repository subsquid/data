use crate::chain_builder::{ChainBuilder, ChainBuilderBox};
use crate::cli::{Cli, NetworkKind};
use crate::fs::create_fs;
use crate::layout::Layout;
use crate::metrics;
use crate::server::run_server;
use crate::sink::Sink;
use crate::writer::{Writer, WriterItem};
use anyhow::ensure;
use sqd_data::solana::tables::SolanaChunkBuilder;
use sqd_data_types::BlockNumber;
use std::ops::DerefMut;
use prometheus_client::registry::Registry;


pub async fn run(args: &Cli) -> anyhow::Result<()> {
    ensure!(
        args.first_block <= args.last_block.unwrap_or(BlockNumber::MAX),
        "--first-block is greater than --last-block"
    );

    init_logging(args.json_log);

    let fs = create_fs(&args.dest).await?;
    let layout = Layout::new(fs.clone());

    let mut chunk_writer = layout.create_chunk_writer(
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

    let (builder_sender, builder_receiver) = std::sync::mpsc::channel::<ChainBuilderBox>();
    let (chunk_sender, chunk_receiver) = tokio::sync::mpsc::unbounded_channel::<WriterItem>();
    let mut processor = chunk_builder.chunk_builder().new_chunk_processor();
    let spare_processor = std::sync::Arc::new(std::sync::Mutex::new(None));
    let description = chunk_builder.chunk_builder().dataset_description();
    let mut sink = Sink::new(chunk_builder, args.chunk_size, builder_sender);
    let mut writer = Writer::new(fs, chunk_receiver);

    let writer_task = tokio::spawn(async move {
        writer.start().await
    });
    let builder_thread = std::thread::spawn(move || {
        let mut first_block = chunk_writer.next_block();
        let mut last_block = 0;
        let mut last_hash = String::new();
        while let Ok(builder) = builder_receiver.recv() {
            builder.chunk_builder().submit_to_processor(&mut processor).unwrap();
            last_block = builder.last_block_number();
            let last_block_hash = builder.last_block_hash();
            last_hash = short_hash(last_block_hash).to_string();
            if processor.byte_size() > 128 * 1024 * 1024 {
                let data = processor.finish().unwrap();
                let mut spare = spare_processor.lock().unwrap();
                processor = std::mem::take(spare.deref_mut()).unwrap_or_else(|| {
                    println!("new processor");
                    builder.chunk_builder().new_chunk_processor()
                });
                let chunk = chunk_writer.next_chunk(first_block, last_block, last_hash.clone());
                let item = WriterItem { description: description.clone(), data, chunk, processor: spare_processor.clone() };
                first_block = last_block + 1;
                chunk_sender.send(item).unwrap();
            }
        }

        if processor.max_num_rows() > 0 {
            let data = processor.finish().unwrap();
            let chunk = chunk_writer.next_chunk(first_block, last_block, last_hash);
            let item = WriterItem { description, data, chunk, processor: spare_processor.clone() };
            chunk_sender.send(item).unwrap();
        }
        drop(chunk_sender);
    });
    sink.write().await?;
    drop(sink);
    builder_thread.join().unwrap();
    writer_task.await??;

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


fn short_hash(value: &str) -> &str {
    &value[value.len().saturating_sub(5)..]
}