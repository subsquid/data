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
    sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
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

    /// run merges on a background thread (max 1 in flight per dataset), so
    /// commits contend with the merge for the env writer lock — the
    /// production compaction_loop shape; inline merges (runs 01–17) never
    /// exercise that contention
    #[arg(long, default_value_t = false)]
    concurrent_merges: bool,

    /// pre-build the hash-index tree to this many entries per dataset before
    /// the timed run; the random-insert cost only shows against a tree much
    /// larger than one batch (ADR 0002 porting note 4). 0 = no hash stream
    #[arg(long, default_value_t = 0)]
    hash_preseed: u64,

    /// hash-index entries inserted per chunk commit (same txn/batch as the
    /// commit — the production shape; ~tx count per chunk) and deleted with
    /// the chunk when retention prunes it
    #[arg(long, default_value_t = 0)]
    hash_per_commit: u64,

    /// accumulate hash inserts and flush every K commits, sorted, in that
    /// commit's txn — the write-behind design candidate; 1 = per-commit
    #[arg(long, default_value_t = 1)]
    hash_flush_every: u64,

    /// retention window in chunks per dataset; 0 = keep everything
    #[arg(long, default_value_t = 64)]
    window: usize,

    /// grow the window to this many chunks for the middle of the run
    /// (commits 20%..50%), then trim back — a downstream-stall wave; 0 = off
    #[arg(long, default_value_t = 0)]
    window_wave: usize,

    /// per-dataset delay between commits; 0 = free-run
    #[arg(long, default_value_t = 0)]
    paced_ms: u64,

    /// hold a read txn/snapshot this long in a loop (stale-reader probe); 0 = off
    #[arg(long, default_value_t = 0)]
    reader_hold_secs: u64,

    /// concurrent readers, each scanning a random live table per iteration
    #[arg(long, default_value_t = 0)]
    readers: usize,

    /// per-reader delay between scans; 0 = free-run
    #[arg(long, default_value_t = 0)]
    read_paced_ms: u64,

    #[arg(long, default_value_t = 64 << 30)]
    max_db_bytes: usize,

    /// mdbx page size
    #[arg(long, default_value_t = 65_536)]
    mdbx_page: usize,

    /// mdbx geometry growth step
    #[arg(long, default_value_t = 256)]
    growth_step_mb: usize,

    /// durable-sync cadence; bounds SafeNoSync file growth ≈ churn × this
    /// window (0 = off, file grows until exit)
    #[arg(long, default_value_t = 1000)]
    sync_every_ms: u64,

    /// rocks block cache
    #[arg(long, default_value_t = 256)]
    rocks_cache_mb: usize,

    /// rocks: direct reads + direct flush/compaction. **Production parity** —
    /// verified 2026-07-27 that no stack passes `--rocksdb-disable-direct-io`
    /// and that the flag is inverted (`cli.rs:149`), so prod runs direct I/O.
    /// Runs 01–26 were buffered and are NOT parity on this axis.
    #[arg(long, default_value_t = false)]
    rocks_direct_io: bool,

    /// rocks memtable size; RocksDB's default 64 MB is what production runs
    #[arg(long, default_value_t = 64)]
    rocks_write_buffer_mb: usize,

    /// rocks memtables before writes stop; the default 2 is the measured
    /// production stall condition (`num_immutable` peaks at exactly 2 on every
    /// stalled pod and 1 on every healthy one)
    #[arg(long, default_value_t = 2)]
    rocks_max_write_buffers: i32,

    /// rocks: universal compaction for the tables CF instead of leveled.
    /// Production's leveled ladder costs 6.4x write amplification (measured
    /// 2026-07-28); this is the arm that prices the alternative.
    #[arg(long, default_value_t = false)]
    rocks_universal: bool,

    /// rocks universal: `max_size_amplification_percent`. Lower means the DB
    /// stays closer to its live size and pays more compaction to hold there.
    #[arg(long, default_value_t = 200)]
    rocks_size_amp_pct: i32
}

static NEXT_TABLE_ID: AtomicU64 = AtomicU64::new(1);
static LOGICAL_RAW: AtomicU64 = AtomicU64::new(0);
static STORED: AtomicU64 = AtomicU64::new(0);
// logical live stored bytes (workers add on commit, subtract on trim) — the
// race-free reference for what the wave peak *should* occupy on disk
static LIVE_STORED: AtomicU64 = AtomicU64::new(0);
static PEAK_LIVE_STORED: AtomicU64 = AtomicU64::new(0);
// rocks write-stall observability: prod stalls on the memtable ceiling, so a
// tuning arm is only judgeable if the bench reports the same three signals
static STALL_SAMPLES: AtomicU64 = AtomicU64::new(0);
static STALL_STOPPED: AtomicU64 = AtomicU64::new(0);
static STALL_DELAYED: AtomicU64 = AtomicU64::new(0);
static STALL_MAX_IMMUTABLE: AtomicU64 = AtomicU64::new(0);

struct WorkerOut {
    commit_us: Vec<u64>,
    merge_us: Vec<u64>,
    delete_us: Vec<u64>
}

/// A live table: id, how many chunks it holds, stored bytes, and the
/// [c_lo, c_hi) chunk-counter span whose hash-index entries it owns.
#[derive(Clone, Copy)]
struct LiveEntry {
    id: u64,
    chunks: usize,
    bytes: u64,
    c_lo: u64,
    c_hi: u64
}

struct PendingMerge<'scope> {
    handle: std::thread::ScopedJoinHandle<'scope, Result<u64>>,
    batch: Vec<u64>,
    new_id: u64,
    n_chunks: usize,
    moved_bytes: u64,
    c_lo: u64,
    c_hi: u64
}

/// Joins the in-flight merge and applies its bookkeeping: the merged entry
/// enters `live` and the registry swaps sources for the merged table (readers
/// keep hitting the sources until then — the production read-vs-merge race).
fn finish_merge(
    pending: &mut Option<PendingMerge<'_>>,
    live: &mut VecDeque<LiveEntry>,
    reg: &std::sync::Mutex<Vec<u64>>,
    out: &mut WorkerOut
) -> Result<()> {
    let Some(p) = pending.take() else { return Ok(()) };
    let us = p.handle.join().expect("merge thread panicked")?;
    out.merge_us.push(us);
    live.push_back(LiveEntry {
        id: p.new_id,
        chunks: p.n_chunks,
        bytes: p.moved_bytes,
        c_lo: p.c_lo,
        c_hi: p.c_hi
    });
    let mut g = reg.lock().expect("registry");
    g.retain(|t| !p.batch.contains(t));
    g.push(p.new_id);
    Ok(())
}

fn run_worker<'scope>(
    engine: &'scope Engine,
    args: &Args,
    ds: usize,
    reg: &'scope std::sync::Mutex<Vec<u64>>,
    wave_done: &AtomicUsize,
    tail_file: &AtomicU64,
    scope: &'scope std::thread::Scope<'scope, '_>
) -> Result<WorkerOut> {
    let mut rng = Rng::new(0x5EED ^ ((ds as u64) << 17));
    let mut out = WorkerOut {
        commit_us: Vec::with_capacity(args.chunks),
        merge_us: Vec::new(),
        delete_us: Vec::new()
    };
    // oldest first
    let mut live: VecDeque<LiveEntry> = VecDeque::new();
    let mut unmerged: Vec<u64> = Vec::new();
    let mut live_chunks = 0usize;
    let mut in_tail = false;
    let mut pending: Option<PendingMerge<'scope>> = None;
    // chunk i owns hash-index counters [P + i·n, P + (i+1)·n)
    let hp = args.hash_preseed;
    let hn = args.hash_per_commit;
    let hk = args.hash_flush_every.max(1);

    for i in 0..args.chunks {
        let id = NEXT_TABLE_ID.fetch_add(1, Ordering::Relaxed);
        let mut entries: Vec<(Vec<u8>, Vec<u8>)> = Vec::with_capacity(args.pages_per_commit + 1);
        entries.push((meta_key(id), vec![0u8; 64]));
        let mut chunk_bytes = 64u64;
        for idx in 0..args.pages_per_commit {
            let raw = gen_page(&mut rng, args.page_bytes, args.dup);
            LOGICAL_RAW.fetch_add(raw.len() as u64, Ordering::Relaxed);
            let value = if args.compress {
                lz4_flex::compress_prepend_size(&raw)
            } else {
                raw
            };
            STORED.fetch_add(value.len() as u64, Ordering::Relaxed);
            chunk_bytes += value.len() as u64;
            entries.push((page_key(id, idx as u32), value));
        }

        let hash_inserts = (hn > 0 && (i as u64 + 1) % hk == 0).then(|| {
            let hi = i as u64 + 1;
            hp + (hi - hk) * hn..hp + hi * hn
        });
        let t0 = Instant::now();
        engine.write_chunk(ds, &entries, hash_inserts)?;
        out.commit_us.push(t0.elapsed().as_micros() as u64);

        let g = LIVE_STORED.fetch_add(chunk_bytes, Ordering::Relaxed) + chunk_bytes;
        PEAK_LIVE_STORED.fetch_max(g, Ordering::Relaxed);
        live.push_back(LiveEntry {
            id,
            chunks: 1,
            bytes: chunk_bytes,
            c_lo: i as u64,
            c_hi: i as u64 + 1
        });
        live_chunks += 1;
        unmerged.push(id);
        reg.lock().expect("registry").push(id);

        if args.merge_fanin > 0 && unmerged.len() >= args.merge_fanin {
            if args.concurrent_merges {
                finish_merge(&mut pending, &mut live, reg, &mut out)?;
                let batch = std::mem::take(&mut unmerged);
                let new_id = NEXT_TABLE_ID.fetch_add(1, Ordering::Relaxed);
                // sources leave `live` now so retention can't trim them out
                // from under the merge txn; they re-enter as one entry at join
                let mut moved_bytes = 0u64;
                let mut n_chunks = 0usize;
                let mut c_lo = u64::MAX;
                let mut c_hi = 0u64;
                live.retain(|e| {
                    let merged = batch.contains(&e.id);
                    if merged {
                        moved_bytes += e.bytes;
                        n_chunks += e.chunks;
                        c_lo = c_lo.min(e.c_lo);
                        c_hi = c_hi.max(e.c_hi);
                    }
                    !merged
                });
                let batch2 = batch.clone();
                let handle = scope.spawn(move || {
                    let t0 = Instant::now();
                    engine.merge_tables(ds, &batch2, new_id)?;
                    Ok(t0.elapsed().as_micros() as u64)
                });
                pending = Some(PendingMerge {
                    handle,
                    batch,
                    new_id,
                    n_chunks,
                    moved_bytes,
                    c_lo,
                    c_hi
                });
            } else {
                let new_id = NEXT_TABLE_ID.fetch_add(1, Ordering::Relaxed);
                let t0 = Instant::now();
                engine.merge_tables(ds, &unmerged, new_id)?;
                out.merge_us.push(t0.elapsed().as_micros() as u64);
                let mut moved_bytes = 0u64;
                let mut c_lo = u64::MAX;
                let mut c_hi = 0u64;
                for _ in 0..unmerged.len() {
                    if let Some(e) = live.pop_back() {
                        moved_bytes += e.bytes;
                        c_lo = c_lo.min(e.c_lo);
                        c_hi = c_hi.max(e.c_hi);
                    }
                }
                live.push_back(LiveEntry {
                    id: new_id,
                    chunks: unmerged.len(),
                    bytes: moved_bytes,
                    c_lo,
                    c_hi
                });
                let mut g = reg.lock().expect("registry");
                g.retain(|t| !unmerged.contains(t));
                g.push(new_id);
                drop(g);
                unmerged.clear();
            }
        }

        let frac = i as f64 / args.chunks as f64;
        let window = if args.window_wave > 0 && (0.2..0.5).contains(&frac) {
            args.window_wave.max(args.window)
        } else {
            args.window
        };
        while window > 0 && live_chunks > window {
            let Some(e) = live.pop_front() else { break };
            live_chunks -= e.chunks;
            LIVE_STORED.fetch_sub(e.bytes, Ordering::Relaxed);
            unmerged.retain(|t| *t != e.id);
            let hash_deletes = (hn > 0).then(|| hp + e.c_lo * hn..hp + e.c_hi * hn);
            let t0 = Instant::now();
            engine.delete_tables(ds, &[e.id], hash_deletes)?;
            out.delete_us.push(t0.elapsed().as_micros() as u64);
            reg.lock().expect("registry").retain(|t| *t != e.id);
        }
        if args.window_wave > 0 && !in_tail && frac >= 0.5 {
            in_tail = true;
            // last worker through the trim snapshots the post-wave file size
            if wave_done.fetch_add(1, Ordering::Relaxed) + 1 == args.datasets {
                if let Ok(u) = engine.disk_usage() {
                    tail_file.store(u.file_bytes.max(1), Ordering::Relaxed);
                }
            }
        }

        if args.paced_ms > 0 {
            std::thread::sleep(Duration::from_millis(args.paced_ms));
        }
    }
    finish_merge(&mut pending, &mut live, reg, &mut out)?;
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
        // stacked devices (md 9:*, dm 253:*) re-count bytes already charged
        // to the physical device below them
        if line.starts_with("9:") || line.starts_with("253:") {
            continue;
        }
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
                args.max_db_bytes,
                args.growth_step_mb << 20
            )?)
        }
        EngineKind::Rocks => Engine::Rocks(RocksEngine::open(
            &args.dir,
            engine::RocksOpts {
                cache_mb: args.rocks_cache_mb,
                direct_io: args.rocks_direct_io,
                write_buffer_mb: args.rocks_write_buffer_mb,
                max_write_buffers: args.rocks_max_write_buffers,
                universal: args.rocks_universal,
                size_amp_pct: args.rocks_size_amp_pct
            }
        )?)
    };

    // verify generated pages actually compress like production data
    {
        let mut rng = Rng::new(1);
        let sample = gen_page(&mut rng, args.page_bytes, args.dup);
        let ratio = sample.len() as f64 / lz4_flex::compress_prepend_size(&sample).len() as f64;
        println!("sample page lz4 ratio: {ratio:.2}x (prod tables: 4.03x)");
    }

    if args.hash_per_commit > 0 {
        println!(
            "hash stream: preseed={} per_commit={} flush_every={}",
            args.hash_preseed, args.hash_per_commit, args.hash_flush_every
        );
    }
    if args.hash_preseed > 0 {
        let t0 = Instant::now();
        let per_ds = args.hash_preseed;
        std::thread::scope(|s| {
            let engine = &engine;
            let handles: Vec<_> = (0..args.datasets)
                .map(|ds| s.spawn(move || engine.preseed_hashes(ds, per_ds)))
                .collect();
            handles
                .into_iter()
                .map(|h| h.join().expect("preseed panicked"))
                .collect::<Result<Vec<_>>>()
        })?;
        engine.sync()?;
        println!("hash preseed done in {:.1}s", t0.elapsed().as_secs_f64());
    }

    let io_before = proc_io();
    let cg_before = cgroup_io();
    let started = Instant::now();
    let done = AtomicBool::new(false);
    let peak_file = AtomicU64::new(0);
    let wave_done = AtomicUsize::new(0);
    // file size once every worker finished its post-wave trim; growth past
    // this at constant live = the freelist failing to serve the churn
    let tail_file = AtomicU64::new(0);
    let registry: Vec<std::sync::Mutex<Vec<u64>>> =
        (0..args.datasets).map(|_| std::sync::Mutex::new(Vec::new())).collect();

    let (outs, reads, peak) = std::thread::scope(|scope| {
        let engine = &engine;
        let args = &args;
        let done = &done;
        let peak_file = &peak_file;
        let wave_done = &wave_done;
        let tail_file = &tail_file;
        let registry = &registry;

        let sampler = scope.spawn(move || {
            while !done.load(Ordering::Relaxed) {
                if let Ok(u) = engine.disk_usage() {
                    peak_file.fetch_max(u.file_bytes, Ordering::Relaxed);
                }
                std::thread::sleep(Duration::from_millis(500));
            }
        });
        // 20 ms, not the sampler's 500 ms: a stall interval on a 20–100 s run is
        // short, and prod only sees these at scrape resolution
        let staller = engine.stall_probe().is_some().then(|| {
            scope.spawn(move || {
                while !done.load(Ordering::Relaxed) {
                    if let Some((stopped, immutable, delayed)) = engine.stall_probe() {
                        STALL_SAMPLES.fetch_add(1, Ordering::Relaxed);
                        if stopped > 0 {
                            STALL_STOPPED.fetch_add(1, Ordering::Relaxed);
                        }
                        if delayed > 0 {
                            STALL_DELAYED.fetch_add(1, Ordering::Relaxed);
                        }
                        STALL_MAX_IMMUTABLE.fetch_max(immutable, Ordering::Relaxed);
                    }
                    std::thread::sleep(Duration::from_millis(20));
                }
            })
        });
        let syncer = (args.sync_every_ms > 0).then(|| {
            scope.spawn(move || {
                while !done.load(Ordering::Relaxed) {
                    engine.periodic_sync().expect("periodic durable sync failed");
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

        let scanners: Vec<_> = (0..args.readers)
            .map(|r| {
                scope.spawn(move || {
                    let mut rng = Rng::new(0x0EAD ^ ((r as u64) << 9));
                    let mut scan_us = Vec::new();
                    let mut pages = 0usize;
                    let mut raw = 0u64;
                    let mut misses = 0usize;
                    while !done.load(Ordering::Relaxed) {
                        let ds = (rng.next() as usize) % args.datasets;
                        let id = {
                            let g = registry[ds].lock().expect("registry");
                            (!g.is_empty()).then(|| g[(rng.next() as usize) % g.len()])
                        };
                        let Some(id) = id else {
                            std::thread::sleep(Duration::from_millis(1));
                            continue;
                        };
                        let t0 = Instant::now();
                        match engine.scan_table(ds, id, args.compress) {
                            Ok((0, _)) | Err(_) => misses += 1,
                            Ok((p, r)) => {
                                scan_us.push(t0.elapsed().as_micros() as u64);
                                pages += p;
                                raw += r;
                            }
                        }
                        if args.read_paced_ms > 0 {
                            std::thread::sleep(Duration::from_millis(args.read_paced_ms));
                        }
                    }
                    (scan_us, pages, raw, misses)
                })
            })
            .collect();

        let handles: Vec<_> = (0..args.datasets)
            .map(|ds| {
                scope.spawn(move || run_worker(engine, args, ds, &registry[ds], wave_done, tail_file, scope))
            })
            .collect();
        let outs = handles
            .into_iter()
            .map(|h| h.join().expect("worker panicked"))
            .collect::<Result<Vec<_>>>();

        done.store(true, Ordering::Relaxed);
        sampler.join().expect("sampler panicked");
        if let Some(s) = staller {
            s.join().expect("stall sampler panicked");
        }
        if let Some(s) = syncer {
            s.join().expect("syncer panicked");
        }
        if let Some(r) = reader {
            r.join().expect("reader panicked");
        }
        let reads: Vec<_> = scanners
            .into_iter()
            .map(|h| h.join().expect("scanner panicked"))
            .collect();
        (outs, reads, peak_file.load(Ordering::Relaxed))
    });
    let outs = outs?;

    let elapsed = started.elapsed();
    engine.sync()?;
    if args.window_wave > 0 {
        // rocks keeps compacting after the last flush; give the wave A/B a
        // comparable end state (mdbx files are static by then)
        std::thread::sleep(Duration::from_secs(10));
    }
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
    if args.readers > 0 {
        let mut scan_us = Vec::new();
        let (mut pages, mut raw, mut misses) = (0usize, 0u64, 0usize);
        for (us, p, r, m) in reads {
            scan_us.extend(us);
            pages += p;
            raw += r;
            misses += m;
        }
        let rep = latency_report(scan_us);
        println!(
            "read: n={} p50={}us p90={}us p99={}us max={}us, pages/scan={:.1}, raw {:.1} MB, misses={}",
            rep.count,
            rep.p50,
            rep.p90,
            rep.p99,
            rep.max,
            pages as f64 / rep.count.max(1) as f64,
            mb(raw),
            misses
        );
    }
    println!(
        "disk: file={:.1} MB (peak {:.1} MB), live={:.1} MB",
        mb(usage.file_bytes),
        mb(peak.max(usage.file_bytes)),
        mb(usage.live_bytes)
    );
    if args.window_wave > 0 {
        let base = tail_file.load(Ordering::Relaxed);
        if base == 0 {
            println!("wave: post-wave baseline not captured (run too short?)");
        } else {
            println!(
                "wave: stored live peak={:.1} MB, file@post-wave={:.1} MB, file@end={:.1} MB, post-wave growth={:.1} MB",
                mb(PEAK_LIVE_STORED.load(Ordering::Relaxed)),
                mb(base),
                mb(usage.file_bytes),
                mb(usage.file_bytes.saturating_sub(base))
            );
        }
    }
    println!("engine: {}", engine.describe());
    let stall_n = STALL_SAMPLES.load(Ordering::Relaxed);
    if stall_n > 0 {
        let pct = |v: u64| 100.0 * v as f64 / stall_n as f64;
        println!(
            "rocks stalls: write_stopped {:.2}% of {stall_n} samples, delayed {:.2}%, \
             max_immutable_memtables {} (ceiling {}); wbuf {} MB",
            pct(STALL_STOPPED.load(Ordering::Relaxed)),
            pct(STALL_DELAYED.load(Ordering::Relaxed)),
            STALL_MAX_IMMUTABLE.load(Ordering::Relaxed),
            args.rocks_max_write_buffers,
            args.rocks_write_buffer_mb
        );
    }
    if let Engine::Rocks(e) = &engine {
        let (flush, compact, wal) = e.write_bytes();
        println!(
            "rocks writes: flush={:.1} MB compaction={:.1} MB wal={:.1} MB, \
             lsm_write_amp={:.2}x, style={}",
            mb(flush),
            mb(compact),
            mb(wal),
            (flush + compact) as f64 / flush.max(1) as f64,
            if args.rocks_universal { "universal" } else { "leveled" }
        );
    }
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
