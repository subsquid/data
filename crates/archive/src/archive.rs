use crate::chain_builder::{ChainBuilder, ChainBuilderBox};
use crate::cli::{Cli, NetworkKind};
use crate::fs::create_fs;
use crate::ingest::ingest_from_service;
use crate::layout::Layout;
use crate::processor::LineProcessor;
use crate::sink::Sink;
use crate::writer::{Writer, WriterItem};
use anyhow::ensure;
use futures_util::TryStreamExt;
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
            tracing::info!("nothing to do");
            return Ok(());
        }
    }

    let chunk_builder: ChainBuilderBox = match args.network_kind {
        NetworkKind::Solana => Box::new(
            ChainBuilder::<SolanaChunkBuilder>::default(),
        ),
    };

    let processor = LineProcessor::new(chunk_builder);

    let block_stream = ingest_from_service(
        args.src.clone(),
        chunk_writer.next_block(),
        args.last_block
    );

    let (line_sender, line_receiver) = std::sync::mpsc::sync_channel::<bytes::Bytes>(100);
    let (chunk_sender, chunk_receiver) = tokio::sync::mpsc::unbounded_channel::<WriterItem>();

    let mut sink = Sink::new(processor, chunk_writer, args.chunk_size, line_receiver, chunk_sender);
    let mut writer = Writer::new(fs, chunk_receiver);

    let sink_thread = std::thread::spawn(move || sink.start());
    let writer_task = tokio::spawn(async move {
        writer.start().await
    });

    let mut block_stream = std::pin::pin!(block_stream);
    while let Some(line) = block_stream.try_next().await? {
        line_sender.send(line)?;
    }

    drop(line_sender);
    sink_thread.join().unwrap()?;
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
