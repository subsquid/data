//! Churn bench for ADR 0002: hotblocks-shaped write workload (chunk commits,
//! chunk merges, retention trims) against libmdbx and a production-tuned
//! RocksDB reference. Reports latency percentiles, disk footprint and, on
//! Linux, OS-level write amplification from /proc/self/io.

mod data;
mod engine;
mod kv_mdbx;

use std::{
    collections::VecDeque,
    path::PathBuf,
    sync::atomic::{AtomicBool, AtomicU64, Ordering},
    time::{Duration, Instant}
};

use anyhow::Result;
use clap::{Parser, ValueEnum};

use crate::{
    data::{gen_page, latency_report, Rng},
    engine::{meta_key, page_key, Engine, MdbxEngine, RocksEngine}
};

#[derive(Clone, Copy, ValueEnum)]
enum EngineKind {
    Mdbx,
    Rocks
}

#[derive(Clone, Copy, ValueEnum)]
enum SyncOpt {
    Durable,
    Safe
}

#[derive(Parser)]
struct Args {
    #[arg(long, value_enum, default_value = "mdbx")]
    engine: EngineKind,

    /// data directory; wiped on start
    #[arg(long)]
    dir: PathBuf,

    #[arg(long, default_value_t = 8)]
    datasets: usize,

    /// chunk commits per dataset
    #[arg(long, default_value_t = 200)]
    chunks: usize,

    #[arg(long, default_value_t = 4)]
    pages_per_commit: usize,

    /// raw page size before compression (prod avg: 42.6 KiB)
    #[arg(long, default_value_t = 43_000)]
    page_bytes: usize,

    /// duplication factor of generated pages; 6 lands near the prod 4x LZ4
    /// ratio (match-encoding overhead eats the difference)
    #[arg(long, default_value_t = 6)]
    dup: usize,

    /// app-level LZ4 before put (the ADR 0002 precondition); rocks compresses
    /// in the engine instead, so its reference runs with this off
    #[arg(long, default_value_t = false)]
    compress: bool,

    #[arg(long, default_value_t = false)]
    per_dataset_env: bool,

    #[arg(long, value_enum, default_value = "safe")]
    sync: SyncOpt,

    /// merge every N chunks into one table; 0 = off
    #[arg(long, default_value_t = 8)]
    merge_fanin: usize,

    /// retention window in chunks per dataset; 0 = keep everything
    #[arg(long, default_value_t = 64)]
    window: usize,

    /// per-dataset delay between commits; 0 = free-run
    #[arg(long, default_value_t = 0)]
    paced_ms: u64,

    /// hold a read txn/snapshot this long in a loop (stale-reader probe); 0 = off
    #[arg(long, default_value_t = 0)]
    reader_hold_secs: u64,

    #[arg(long, default_value_t = 64 << 30)]
    max_db_bytes: usize,

    /// mdbx page size
    #[arg(long, default_value_t = 65_536)]
    mdbx_page: usize,

    /// durable-sync cadence; bounds SafeNoSync file growth ≈ churn × this
    /// window (0 = off, file grows until exit)
    #[arg(long, default_value_t = 1000)]
    sync_every_ms: u64
}

static NEXT_TABLE_ID: AtomicU64 = AtomicU64::new(1);
static LOGICAL_RAW: AtomicU64 = AtomicU64::new(0);
static STORED: AtomicU64 = AtomicU64::new(0);

struct WorkerOut {
    commit_us: Vec<u64>,
    merge_us: Vec<u64>,
    delete_us: Vec<u64>
}

fn run_worker(engine: &Engine, args: &Args, ds: usize) -> Result<WorkerOut> {
    let mut rng = Rng::new(0x5EED ^ ((ds as u64) << 17));
    let mut out = WorkerOut {
        commit_us: Vec::with_capacity(args.chunks),
        merge_us: Vec::new(),
        delete_us: Vec::new()
    };
    // (table_id, chunks it holds), oldest first
    let mut live: VecDeque<(u64, usize)> = VecDeque::new();
    let mut unmerged: Vec<u64> = Vec::new();
    let mut live_chunks = 0usize;

    for _ in 0..args.chunks {
        let id = NEXT_TABLE_ID.fetch_add(1, Ordering::Relaxed);
        let mut entries: Vec<(Vec<u8>, Vec<u8>)> = Vec::with_capacity(args.pages_per_commit + 1);
        entries.push((meta_key(id), vec![0u8; 64]));
        for idx in 0..args.pages_per_commit {
            let raw = gen_page(&mut rng, args.page_bytes, args.dup);
            LOGICAL_RAW.fetch_add(raw.len() as u64, Ordering::Relaxed);
            let value = if args.compress {
                lz4_flex::compress_prepend_size(&raw)
            } else {
                raw
            };
            STORED.fetch_add(value.len() as u64, Ordering::Relaxed);
            entries.push((page_key(id, idx as u32), value));
        }

        let t0 = Instant::now();
        engine.write_chunk(ds, &entries)?;
        out.commit_us.push(t0.elapsed().as_micros() as u64);

        live.push_back((id, 1));
        live_chunks += 1;
        unmerged.push(id);

        if args.merge_fanin > 0 && unmerged.len() >= args.merge_fanin {
            let new_id = NEXT_TABLE_ID.fetch_add(1, Ordering::Relaxed);
            let t0 = Instant::now();
            engine.merge_tables(ds, &unmerged, new_id)?;
            out.merge_us.push(t0.elapsed().as_micros() as u64);
            for _ in 0..unmerged.len() {
                live.pop_back();
            }
            live.push_back((new_id, unmerged.len()));
            unmerged.clear();
        }

        while args.window > 0 && live_chunks > args.window {
            let Some((old, n)) = live.pop_front() else { break };
            live_chunks -= n;
            unmerged.retain(|t| *t != old);
            let t0 = Instant::now();
            engine.delete_tables(ds, &[old])?;
            out.delete_us.push(t0.elapsed().as_micros() as u64);
        }

        if args.paced_ms > 0 {
            std::thread::sleep(Duration::from_millis(args.paced_ms));
        }
    }
    Ok(out)
}

#[cfg(target_os = "linux")]
fn proc_io() -> Option<(u64, u64)> {
    let text = std::fs::read_to_string("/proc/self/io").ok()?;
    let mut read = None;
    let mut write = None;
    for line in text.lines() {
        if let Some(v) = line.strip_prefix("read_bytes: ") {
            read = v.trim().parse().ok();
        } else if let Some(v) = line.strip_prefix("write_bytes: ") {
            write = v.trim().parse().ok();
        }
    }
    Some((read?, write?))
}

#[cfg(not(target_os = "linux"))]
fn proc_io() -> Option<(u64, u64)> {
    None
}

/// proc write_bytes counts page dirtying, not proven device traffic
/// (see docs/measurements/2026-07-16-flush-bench); cgroup io.stat is the
/// authoritative number when running in a pod.
fn cgroup_io() -> Option<(u64, u64)> {
    let text = std::fs::read_to_string("/sys/fs/cgroup/io.stat").ok()?;
    let mut read = 0u64;
    let mut write = 0u64;
    for line in text.lines() {
        for field in line.split_whitespace() {
            if let Some(v) = field.strip_prefix("rbytes=") {
                read += v.parse::<u64>().unwrap_or(0);
            } else if let Some(v) = field.strip_prefix("wbytes=") {
                write += v.parse::<u64>().unwrap_or(0);
            }
        }
    }
    Some((read, write))
}

fn mb(bytes: u64) -> f64 {
    bytes as f64 / 1e6
}

fn main() -> Result<()> {
    let args = Args::parse();

    if args.dir.exists() {
        std::fs::remove_dir_all(&args.dir)?;
    }
    std::fs::create_dir_all(&args.dir)?;

    let engine = match args.engine {
        EngineKind::Mdbx => {
            let n_envs = if args.per_dataset_env { args.datasets } else { 1 };
            let sync_mode = match args.sync {
                SyncOpt::Durable => libmdbx::SyncMode::Durable,
                SyncOpt::Safe => libmdbx::SyncMode::SafeNoSync
            };
            Engine::Mdbx(MdbxEngine::open(
                &args.dir,
                n_envs,
                sync_mode,
                args.mdbx_page,
                args.max_db_bytes
            )?)
        }
        EngineKind::Rocks => Engine::Rocks(RocksEngine::open(&args.dir, 256)?)
    };

    // verify generated pages actually compress like production data
    {
        let mut rng = Rng::new(1);
        let sample = gen_page(&mut rng, args.page_bytes, args.dup);
        let ratio = sample.len() as f64 / lz4_flex::compress_prepend_size(&sample).len() as f64;
        println!("sample page lz4 ratio: {ratio:.2}x (prod tables: 4.03x)");
    }

    let io_before = proc_io();
    let cg_before = cgroup_io();
    let started = Instant::now();
    let done = AtomicBool::new(false);
    let peak_file = AtomicU64::new(0);

    let (outs, peak) = std::thread::scope(|scope| {
        let engine = &engine;
        let args = &args;
        let done = &done;
        let peak_file = &peak_file;

        let sampler = scope.spawn(move || {
            while !done.load(Ordering::Relaxed) {
                if let Ok(u) = engine.disk_usage() {
                    peak_file.fetch_max(u.file_bytes, Ordering::Relaxed);
                }
                std::thread::sleep(Duration::from_millis(500));
            }
        });
        let syncer = (args.sync_every_ms > 0).then(|| {
            scope.spawn(move || {
                while !done.load(Ordering::Relaxed) {
                    let _ = engine.periodic_sync();
                    std::thread::sleep(Duration::from_millis(args.sync_every_ms));
                }
            })
        });
        let reader = (args.reader_hold_secs > 0).then(|| {
            scope.spawn(move || {
                while !done.load(Ordering::Relaxed) {
                    let _ = engine.hold_reader(args.reader_hold_secs);
                }
            })
        });

        let handles: Vec<_> = (0..args.datasets)
            .map(|ds| scope.spawn(move || run_worker(engine, args, ds)))
            .collect();
        let outs = handles
            .into_iter()
            .map(|h| h.join().expect("worker panicked"))
            .collect::<Result<Vec<_>>>();

        done.store(true, Ordering::Relaxed);
        sampler.join().expect("sampler panicked");
        if let Some(s) = syncer {
            s.join().expect("syncer panicked");
        }
        if let Some(r) = reader {
            r.join().expect("reader panicked");
        }
        (outs, peak_file.load(Ordering::Relaxed))
    });
    let outs = outs?;

    let elapsed = started.elapsed();
    engine.sync()?;
    let usage = engine.disk_usage()?;
    let io_after = proc_io();
    let cg_after = cgroup_io();

    let mut commit_us = Vec::new();
    let mut merge_us = Vec::new();
    let mut delete_us = Vec::new();
    for o in outs {
        commit_us.extend(o.commit_us);
        merge_us.extend(o.merge_us);
        delete_us.extend(o.delete_us);
    }

    let logical = LOGICAL_RAW.load(Ordering::Relaxed);
    let stored = STORED.load(Ordering::Relaxed);

    println!("\n=== result ===");
    println!(
        "elapsed: {:.1}s, commits: {} ({:.1}/s)",
        elapsed.as_secs_f64(),
        commit_us.len(),
        commit_us.len() as f64 / elapsed.as_secs_f64()
    );
    println!(
        "logical raw: {:.1} MB, stored (post-compress): {:.1} MB",
        mb(logical),
        mb(stored)
    );
    for (name, lat) in [("commit", commit_us), ("merge", merge_us), ("delete", delete_us)] {
        let r = latency_report(lat);
        println!(
            "{name}: n={} p50={}us p90={}us p99={}us max={}us",
            r.count, r.p50, r.p90, r.p99, r.max
        );
    }
    println!(
        "disk: file={:.1} MB (peak {:.1} MB), live={:.1} MB",
        mb(usage.file_bytes),
        mb(peak.max(usage.file_bytes)),
        mb(usage.live_bytes)
    );
    println!("engine: {}", engine.describe());
    if let Engine::Mdbx(e) = &engine {
        let (entries, raw) = e.verify_scan(args.compress)?;
        println!(
            "verify: {entries} live entries read back via the KvRead seam, {:.1} MB raw",
            mb(raw)
        );
    }
    match (io_before, io_after) {
        (Some((r0, w0)), Some((r1, w1))) => {
            let (dr, dw) = (r1 - r0, w1 - w0);
            println!(
                "os io: read={:.1} MB write={:.1} MB, write_amp vs stored: {:.2}x, vs raw: {:.2}x",
                mb(dr),
                mb(dw),
                dw as f64 / stored.max(1) as f64,
                dw as f64 / logical.max(1) as f64
            );
        }
        _ => println!("os io: /proc/self/io unavailable (run on Linux for write amplification)")
    }
    match (cg_before, cg_after) {
        (Some((r0, w0)), Some((r1, w1))) => {
            let (dr, dw) = (r1.saturating_sub(r0), w1.saturating_sub(w0));
            println!(
                "cgroup io (authoritative): read={:.1} MB write={:.1} MB, write_amp vs stored: {:.2}x, vs raw: {:.2}x",
                mb(dr),
                mb(dw),
                dw as f64 / stored.max(1) as f64,
                dw as f64 / logical.max(1) as f64
            );
        }
        _ => println!("cgroup io: io.stat unavailable (fine outside a cgroup v2 pod)")
    }

    Ok(())
}
