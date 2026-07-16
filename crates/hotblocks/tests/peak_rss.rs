//! Peak-RSS under head-path load (PR #98 checklist). Manual:
//!
//!   cargo test -p sqd-hotblocks --test peak_rss -- --ignored --nocapture
//!
//! `SQD_RSS_BIN=<path>` overrides the service binary (e.g. a master build for an
//! old-vs-new comparison). Block production is paced so each response should carry one
//! block and take the per-flush prepare path, but a binary slower than the pacing
//! batches the accumulated tail into fewer, larger flushes — the report prints the
//! achieved blocks/response so runs with different flush granularity are not compared
//! silently. RSS is sampled from `ps` throughout.

use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering}
    },
    time::Duration
};

use anyhow::Result;
use sqd_hotblocks_harness::{
    chain::Evm,
    harness::{Harness, HarnessConfig}
};

const START: u64 = 1_000;

fn blocks() -> u32 {
    std::env::var("SQD_RSS_BLOCKS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(3_000)
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "manual load test — run with --ignored --nocapture"]
async fn peak_rss_under_head_load() -> Result<()> {
    let bin = std::env::var("SQD_RSS_BIN").unwrap_or_else(|_| env!("CARGO_BIN_EXE_sqd-hotblocks").to_string());
    println!("binary: {bin}");

    let mut h = Harness::start(HarnessConfig::from_block(&bin, Arc::new(Evm), START)).await?;
    let pid = h.sut.pid().expect("SUT pid");

    let peak = Arc::new(AtomicU64::new(0));
    let samples = Arc::new(std::sync::Mutex::new(Vec::<u64>::new()));
    let sampler = tokio::spawn(sample_rss(pid, peak.clone(), samples.clone()));

    let started = std::time::Instant::now();
    let blocks = blocks();
    for i in 0..blocks {
        h.produce(1)?;
        if i % 500 == 499 {
            h.finalize_with_lag(5)?;
            h.settle().await?;
        }
        tokio::time::sleep(Duration::from_millis(3)).await;
    }
    h.finalize_with_lag(5)?;
    h.settle().await?;
    h.assert_conforms().await?;
    let elapsed = started.elapsed();

    sampler.abort();
    let samples = samples.lock().unwrap().clone();
    let avg = |s: &[u64]| s.iter().sum::<u64>() / s.len().max(1) as u64;
    let q = samples.len() / 4;
    println!(
        "blocks: {blocks} in {:.0}s ({:.0} blocks/s), samples: {}",
        elapsed.as_secs_f64(),
        blocks as f64 / elapsed.as_secs_f64(),
        samples.len()
    );
    let stats = h.sim.stats(&h.dataset);
    let data_responses = stats.stream_requests - stats.no_data - stats.fork_signals - stats.below_history;
    println!(
        "flush granularity: {} blocks over {data_responses} data responses ({:.2} blocks/response; 1.00 = one flush per block)",
        stats.blocks_served,
        stats.blocks_served as f64 / data_responses.max(1) as f64
    );
    println!(
        "RSS MB: peak {:.0}, quartile avgs {:.0} → {:.0} → {:.0} → {:.0}",
        peak.load(Ordering::Relaxed) as f64 / 1024.0,
        avg(&samples[..q]) as f64 / 1024.0,
        avg(&samples[q..2 * q]) as f64 / 1024.0,
        avg(&samples[2 * q..3 * q]) as f64 / 1024.0,
        avg(&samples[3 * q..]) as f64 / 1024.0
    );
    Ok(())
}

/// `ps -o rss=` is KiB on both macOS and Linux.
async fn sample_rss(pid: u32, peak: Arc<AtomicU64>, samples: Arc<std::sync::Mutex<Vec<u64>>>) {
    loop {
        if let Ok(out) = tokio::process::Command::new("ps")
            .args(["-o", "rss=", "-p", &pid.to_string()])
            .output()
            .await
            && let Ok(kb) = String::from_utf8_lossy(&out.stdout).trim().parse::<u64>()
        {
            peak.fetch_max(kb, Ordering::Relaxed);
            samples.lock().unwrap().push(kb);
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}
