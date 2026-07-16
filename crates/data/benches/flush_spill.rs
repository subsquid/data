//! Decomposed cost of one hotblocks flush (docs/adr/0001-in-memory-chunk-prepare.md;
//! results in docs/measurements/).
//!
//! Reproduces the ingest hot path stage by stage on a synthetic EVM block with N rows, in
//! three modes:
//!   fresh — the old prod path: push → new_chunk_processor (488 temp files) →
//!           submit+finish → readback → drop
//!   reuse — fix #3 idealized: construct once, then per flush push → submit+finish →
//!           readback → reconstruct (per-table `into_processor`, replaces construct+drop)
//!   mem   — fix #1: push → prepare_in_memory → readback → drop; no temp files
//! Reports wall/user/sys time per stage and, on Linux, /proc/self/io deltas.
//!
//!   cargo bench -p sqd-data --bench flush_spill                 # full sweep, all modes
//!   SQD_BENCH_QUICK=1 cargo bench -p sqd-data --bench flush_spill
//!   TMPDIR=/dev/shm ...                                         # Bench C (Linux; docker: --shm-size=2g)
//!
//! Verdicts to extract: per-stage cost = fixed + marginal×rows (Bench A); fresh−reuse =
//! idealized ceiling of processor reuse (Bench B); /tmp vs /dev/shm = fs sensitivity (Bench C).

use std::{
    collections::BTreeMap,
    time::{Duration, Instant}
};

use anyhow::Context;
use serde_json::{json, Value};
use sqd_data::evm::{model::Block, tables::EvmChunkBuilder};
use sqd_data_core::{BlockChunkBuilder, ChunkProcessor};

const N_STAGES: usize = 5;

#[derive(Clone, Copy, PartialEq)]
enum Mode {
    Fresh,
    Reuse,
    /// Fix #1: `prepare_in_memory` — no temp files at all.
    Mem
}

impl Mode {
    fn stages(self) -> [&'static str; N_STAGES] {
        match self {
            Mode::Fresh => ["push", "construct", "submit+finish", "readback", "drop"],
            Mode::Reuse => ["push", "reconstruct", "submit+finish", "readback", ""],
            Mode::Mem => ["push", "", "prepare(mem)", "readback", "drop"]
        }
    }

    fn name(self) -> &'static str {
        match self {
            Mode::Fresh => "fresh",
            Mode::Reuse => "reuse",
            Mode::Mem => "mem"
        }
    }
}

fn main() -> anyhow::Result<()> {
    let nofile_limit = raise_nofile_limit()?;

    // rows = transactions per block (logs scale 1:1); iterations shrink as rows grow
    let plan: &[(usize, usize)] = if std::env::var_os("SQD_BENCH_QUICK").is_some() {
        &[(1, 20), (100, 10)]
    } else {
        &[(1, 200), (10, 200), (100, 100), (1_000, 50), (10_000, 10), (100_000, 3)]
    };

    println!("flush_spill: EVM chunk, temp dir = {}", std::env::temp_dir().display());
    println!("RLIMIT_NOFILE soft limit: {nofile_limit}");
    println!("CPU accounting scope: {}", cpu_scope());
    println!(
        "/proc/self/io: {}",
        if io_snapshot().is_some() {
            "available"
        } else {
            "NOT available (macOS?) — io columns will be zero"
        }
    );

    let mut cases = Vec::new();
    for &(rows, iters) in plan {
        cases.push(run_case(rows, iters)?);
    }
    println!("\ncross-mode output equality: OK (every measured iteration)");

    print_fixed_marginal(&cases);
    print_mode_comparison(&cases);
    Ok(())
}

struct CaseRun {
    rows: usize,
    // p50 wall per stage — median resists warmup/writeback outliers
    p50_wall: [Duration; N_STAGES],
    // p50 of per-iteration totals: median(A+B) != median(A)+median(B)
    total_p50: Duration,
    total_p95: Duration
}

/// Interleaved fresh+reuse execution needs ~1,000 fds; default soft limits (256 on
/// macOS) are too low.
fn raise_nofile_limit() -> anyhow::Result<libc::rlim_t> {
    const TARGET: libc::rlim_t = 8192;
    const REQUIRED: libc::rlim_t = 2048;

    // SAFETY: `rlimit` is valid when zero-initialized and `getrlimit` receives a valid,
    // writable pointer for the duration of the call.
    let mut lim: libc::rlimit = unsafe { std::mem::zeroed() };
    // SAFETY: `lim` points to initialized writable storage and RLIMIT_NOFILE is a valid
    // resource selector on every supported Unix target.
    let rc = unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut lim) };
    if rc != 0 {
        return Err(std::io::Error::last_os_error()).context("getrlimit(RLIMIT_NOFILE) failed");
    }

    if lim.rlim_cur < TARGET {
        let requested = TARGET.min(lim.rlim_max);
        let new_lim = libc::rlimit {
            rlim_cur: requested,
            rlim_max: lim.rlim_max
        };
        // SAFETY: `new_lim` is initialized, its soft limit does not exceed its hard limit,
        // and the pointer remains valid for the duration of the call.
        let rc = unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &new_lim) };
        if rc != 0 {
            return Err(std::io::Error::last_os_error()).context("setrlimit(RLIMIT_NOFILE) failed");
        }
        lim = new_lim;
    }

    anyhow::ensure!(
        lim.rlim_cur >= REQUIRED,
        "RLIMIT_NOFILE={} is below the {REQUIRED} required by the interleaved benchmark",
        lim.rlim_cur
    );
    Ok(lim.rlim_cur)
}

const MODE_ORDERS: [[Mode; 3]; 6] = [
    [Mode::Fresh, Mode::Reuse, Mode::Mem],
    [Mode::Reuse, Mode::Mem, Mode::Fresh],
    [Mode::Mem, Mode::Fresh, Mode::Reuse],
    [Mode::Fresh, Mode::Mem, Mode::Reuse],
    [Mode::Mem, Mode::Reuse, Mode::Fresh],
    [Mode::Reuse, Mode::Fresh, Mode::Mem]
];

impl Mode {
    const fn index(self) -> usize {
        match self {
            Mode::Fresh => 0,
            Mode::Reuse => 1,
            Mode::Mem => 2
        }
    }
}

struct ModeRun {
    mode: Mode,
    acc: [StageAcc; N_STAGES],
    totals: Vec<Duration>,
    spare: Option<ChunkProcessor>,
    fds_baseline: usize,
    fd_delta: usize
}

impl ModeRun {
    fn new(mode: Mode, iters: usize) -> anyhow::Result<Self> {
        let spare = match mode {
            Mode::Reuse => Some(EvmChunkBuilder::new().new_chunk_processor()?),
            _ => None
        };
        Ok(Self {
            mode,
            acc: Default::default(),
            totals: Vec::with_capacity(iters),
            spare,
            fds_baseline: 0,
            fd_delta: 0
        })
    }

    fn reset_measurements(&mut self) {
        self.acc = Default::default();
        self.totals.clear();
        self.fds_baseline = open_fds();
        self.fd_delta = 0;
    }

    fn run_iteration(&mut self, block: &Block) -> anyhow::Result<BTreeMap<&'static str, arrow::array::RecordBatch>> {
        let mut total = Duration::ZERO;
        let mut builder = EvmChunkBuilder::new();

        let (_, wall) = measured(&mut self.acc[0], || builder.push(block))?;
        total += wall;

        let mut prepared = match self.mode {
            Mode::Mem => {
                let (prepared, wall) = measured(&mut self.acc[2], || builder.prepare_in_memory())?;
                total += wall;
                prepared
            }
            Mode::Fresh | Mode::Reuse => {
                let mut processor = match self.mode {
                    Mode::Fresh => {
                        let fds_before = open_fds();
                        let (processor, wall) = measured(&mut self.acc[1], || builder.new_chunk_processor())?;
                        total += wall;
                        if self.fd_delta == 0 {
                            self.fd_delta = open_fds().saturating_sub(fds_before);
                        }
                        processor
                    }
                    Mode::Reuse => self.spare.take().context("spare processor is missing")?,
                    Mode::Mem => unreachable!()
                };
                let (prepared, wall) = measured(&mut self.acc[2], || {
                    builder.submit_to_processor(&mut processor)?;
                    builder.clear();
                    processor.finish()
                })?;
                total += wall;
                prepared
            }
        };

        let (sample, wall) = measured(&mut self.acc[3], || {
            prepared
                .iter_mut()
                .map(|(name, table)| {
                    let batch = table.read_record_batch(0, table.num_rows())?;
                    Ok((*name, batch))
                })
                .collect::<anyhow::Result<BTreeMap<_, _>>>()
        })?;
        total += wall;

        match self.mode {
            Mode::Fresh | Mode::Mem => {
                let (_, wall) = measured(&mut self.acc[4], || {
                    drop(prepared);
                    Ok(())
                })?;
                total += wall;
            }
            Mode::Reuse => {
                let (spare, wall) = measured(&mut self.acc[1], || {
                    let tables = prepared
                        .into_iter()
                        .map(|(name, table)| table.into_processor().map(|processor| (name, processor)))
                        .collect::<anyhow::Result<BTreeMap<_, _>>>()?;
                    Ok(ChunkProcessor::new(tables))
                })?;
                total += wall;
                self.spare = Some(spare);
            }
        }

        self.totals.push(total);
        Ok(sample)
    }

    fn report(&mut self, rows: usize) -> CaseRun {
        println!("\n-- mode={} --", self.mode.name());
        match self.mode {
            Mode::Fresh => println!("temp files created in `construct` (fd delta): {}", self.fd_delta),
            Mode::Mem => print_fd_stability("temp files: none; fds stable", self.fds_baseline),
            Mode::Reuse => print_fd_stability("fds stable across iterations", self.fds_baseline)
        }

        println!(
            "{:<15} {:>9} {:>9} {:>9} {:>9} {:>9} {:>8} {:>8} {:>10} {:>10} {:>8} {:>9} {:>9}",
            "stage",
            "p50 ms",
            "p95 ms",
            "mean ms",
            "user ms",
            "sys ms",
            "syscr",
            "syscw",
            "wchar KB",
            "rchar KB",
            "rd MB",
            "dirty MB",
            "cancel MB"
        );

        let stages = self.mode.stages();
        let mut p50_wall = [Duration::ZERO; N_STAGES];
        for (idx, acc) in self.acc.iter_mut().enumerate() {
            if acc.wall.is_empty() {
                continue;
            }
            acc.wall.sort_unstable();
            let p50 = percentile(&acc.wall, 50);
            let p95 = percentile(&acc.wall, 95);
            let mean = acc.wall.iter().sum::<Duration>() / acc.wall.len() as u32;
            p50_wall[idx] = p50;
            let n = acc.wall.len() as f64;
            let per = |value: u64| value as f64 / n;
            println!(
                "{:<15} {:>9.3} {:>9.3} {:>9.3} {:>9.3} {:>9.3} {:>8.0} {:>8.0} {:>10.1} {:>10.1} {:>8.2} {:>9.2} {:>9.2}",
                stages[idx],
                p50.as_secs_f64() * 1e3,
                p95.as_secs_f64() * 1e3,
                mean.as_secs_f64() * 1e3,
                acc.user.as_secs_f64() * 1e3 / n,
                acc.sys.as_secs_f64() * 1e3 / n,
                per(acc.io.syscr),
                per(acc.io.syscw),
                per(acc.io.wchar) / 1024.0,
                per(acc.io.rchar) / 1024.0,
                per(acc.io.read_bytes) / (1024.0 * 1024.0),
                per(acc.io.write_bytes) / (1024.0 * 1024.0),
                per(acc.io.cancelled_write_bytes) / (1024.0 * 1024.0)
            );
        }

        self.totals.sort_unstable();
        let total_p50 = percentile(&self.totals, 50);
        let total_p95 = percentile(&self.totals, 95);
        println!(
            "per-iteration measured-stage total: p50 {:.3} ms, p95 {:.3} ms",
            total_p50.as_secs_f64() * 1e3,
            total_p95.as_secs_f64() * 1e3
        );

        CaseRun {
            rows,
            p50_wall,
            total_p50,
            total_p95
        }
    }
}

fn print_fd_stability(label: &str, baseline: usize) {
    let end = open_fds();
    println!(
        "{label}: {} (start {baseline}, end {end})",
        if end == baseline { "yes" } else { "NO — LEAK" }
    );
}

fn run_case(rows: usize, iters: usize) -> anyhow::Result<(CaseRun, CaseRun, CaseRun)> {
    let block: Block = serde_json::from_value(gen_block_json(rows, 0))?;

    let mut builder = EvmChunkBuilder::new();
    builder.push(&block)?;
    let block_bytes = builder.byte_size();
    builder.clear();

    println!(
        "\n== rows={rows}  iters={iters}  in-memory block ~{} KB ==",
        block_bytes / 1024
    );

    let mut runs = [
        ModeRun::new(Mode::Fresh, iters)?,
        ModeRun::new(Mode::Reuse, iters)?,
        ModeRun::new(Mode::Mem, iters)?
    ];

    for iteration in 0..2 {
        let block: Block = serde_json::from_value(gen_block_json(rows, iteration + 10_000))?;
        run_round(&mut runs, &block, iteration, rows)?;
    }
    for run in &mut runs {
        run.reset_measurements();
    }

    for iteration in 0..iters {
        let block: Block = serde_json::from_value(gen_block_json(rows, iteration))?;
        run_round(&mut runs, &block, iteration, rows)?;
    }

    let mut reports = runs.iter_mut().map(|run| run.report(rows));
    Ok((
        reports.next().unwrap(),
        reports.next().unwrap(),
        reports.next().unwrap()
    ))
}

fn run_round(runs: &mut [ModeRun; 3], block: &Block, iteration: usize, rows: usize) -> anyhow::Result<()> {
    let mut outputs = std::array::from_fn::<_, 3, _>(|_| None);
    for mode in MODE_ORDERS[iteration % MODE_ORDERS.len()] {
        outputs[mode.index()] = Some(runs[mode.index()].run_iteration(block)?);
    }

    let fresh = outputs[Mode::Fresh.index()].as_ref().unwrap();
    anyhow::ensure!(
        Some(fresh) == outputs[Mode::Reuse.index()].as_ref(),
        "fresh vs reuse output mismatch at rows={rows}, iteration={iteration}"
    );
    anyhow::ensure!(
        Some(fresh) == outputs[Mode::Mem.index()].as_ref(),
        "fresh vs mem output mismatch at rows={rows}, iteration={iteration}"
    );
    Ok(())
}

fn percentile(sorted: &[Duration], percent: usize) -> Duration {
    let index = ((sorted.len() - 1) * percent + 50) / 100;
    sorted[index]
}

fn print_fixed_marginal(cases: &[(CaseRun, CaseRun, CaseRun)]) {
    let (first, last) = (&cases[0].0, &cases[cases.len() - 1].0);
    if first.rows == last.rows {
        return;
    }
    println!(
        "\n== fresh mode: fixed vs marginal (fixed ≈ p50 at rows={}, marginal from rows={}) ==",
        first.rows, last.rows
    );
    println!("{:<15} {:>12} {:>18}", "stage", "fixed ms", "marginal ms/1k rows");
    let dr = (last.rows - first.rows) as f64;
    for (idx, stage) in Mode::Fresh.stages().iter().enumerate() {
        let fixed = first.p50_wall[idx].as_secs_f64() * 1e3;
        let marginal = (last.p50_wall[idx].as_secs_f64() - first.p50_wall[idx].as_secs_f64()) * 1e3 / dr * 1000.0;
        println!("{:<15} {:>12.3} {:>18.3}", stage, fixed, marginal);
    }
    let fixed_total = first.total_p50.as_secs_f64() * 1e3;
    let marginal_total = (last.total_p50.as_secs_f64() - first.total_p50.as_secs_f64()) * 1e3 / dr * 1000.0;
    println!("{:<15} {:>12.3} {:>18.3}", "TOTAL", fixed_total, marginal_total);
    println!(
        "rows where marginal overtakes fixed: ~{:.0}",
        if marginal_total > 0.0 {
            fixed_total / marginal_total * 1000.0
        } else {
            f64::INFINITY
        }
    );
}

fn print_mode_comparison(cases: &[(CaseRun, CaseRun, CaseRun)]) {
    println!("\n== measured-stage total: fresh (prod) vs reuse (fix #3 ceiling) vs mem (fix #1) ==");
    println!(
        "{:>8} {:>10} {:>10} {:>10} {:>10} {:>8} {:>10} {:>10} {:>8}",
        "rows", "fresh p50", "fresh p95", "reuse p50", "reuse p95", "saves", "mem p50", "mem p95", "saves"
    );
    for (fresh, reuse, mem) in cases {
        let f = fresh.total_p50.as_secs_f64() * 1e3;
        let r = reuse.total_p50.as_secs_f64() * 1e3;
        let m = mem.total_p50.as_secs_f64() * 1e3;
        println!(
            "{:>8} {:>10.3} {:>10.3} {:>10.3} {:>10.3} {:>7.0}% {:>10.3} {:>10.3} {:>7.0}%",
            fresh.rows,
            f,
            fresh.total_p95.as_secs_f64() * 1e3,
            r,
            reuse.total_p95.as_secs_f64() * 1e3,
            (f - r) / f * 100.0,
            m,
            mem.total_p95.as_secs_f64() * 1e3,
            (f - m) / f * 100.0
        );
    }
}

// -- measurement plumbing ---------------------------------------------------

#[derive(Default)]
struct StageAcc {
    wall: Vec<Duration>,
    user: Duration,
    sys: Duration,
    io: IoStats
}

fn measured<T>(acc: &mut StageAcc, f: impl FnOnce() -> anyhow::Result<T>) -> anyhow::Result<(T, Duration)> {
    let io0 = io_snapshot();
    let cpu0 = cpu_times()?;
    let t0 = Instant::now();
    let out = f();
    let wall = t0.elapsed();
    let cpu1 = cpu_times()?;
    let io1 = io_snapshot();

    acc.wall.push(wall);
    acc.user += cpu1.0.saturating_sub(cpu0.0);
    acc.sys += cpu1.1.saturating_sub(cpu0.1);
    if let (Some(a), Some(b)) = (io0, io1) {
        acc.io.add_delta(&a, &b);
    }
    Ok((out?, wall))
}

fn cpu_scope() -> &'static str {
    if cfg!(target_os = "linux") {
        "current thread (RUSAGE_THREAD)"
    } else {
        "process (RUSAGE_SELF; benchmark is single-threaded)"
    }
}

fn cpu_times() -> anyhow::Result<(Duration, Duration)> {
    #[cfg(target_os = "linux")]
    const WHO: libc::c_int = libc::RUSAGE_THREAD;
    #[cfg(not(target_os = "linux"))]
    const WHO: libc::c_int = libc::RUSAGE_SELF;

    // SAFETY: `rusage` is valid when zero-initialized and `getrusage` receives a valid,
    // writable pointer for the duration of the call.
    let mut ru: libc::rusage = unsafe { std::mem::zeroed() };
    // SAFETY: `WHO` is a supported selector for this target and `ru` is writable.
    anyhow::ensure!(unsafe { libc::getrusage(WHO, &mut ru) } == 0, "getrusage failed");
    let tv = |t: libc::timeval| Duration::new(t.tv_sec as u64, t.tv_usec as u32 * 1000);
    Ok((tv(ru.ru_utime), tv(ru.ru_stime)))
}

#[derive(Default, Clone, Copy)]
struct IoStats {
    rchar: u64,
    wchar: u64,
    syscr: u64,
    syscw: u64,
    read_bytes: u64,
    write_bytes: u64,
    cancelled_write_bytes: u64
}

impl IoStats {
    fn add_delta(&mut self, before: &IoStats, after: &IoStats) {
        self.rchar += after.rchar - before.rchar;
        self.wchar += after.wchar - before.wchar;
        self.syscr += after.syscr - before.syscr;
        self.syscw += after.syscw - before.syscw;
        self.read_bytes += after.read_bytes - before.read_bytes;
        self.write_bytes += after.write_bytes - before.write_bytes;
        // cancelled can regress vs a racing writeback; clamp instead of panicking in release
        self.cancelled_write_bytes += after.cancelled_write_bytes.saturating_sub(before.cancelled_write_bytes);
    }
}

fn io_snapshot() -> Option<IoStats> {
    let s = std::fs::read_to_string("/proc/self/io").ok()?;
    let get = |k: &str| {
        s.lines()
            .find(|l| l.starts_with(k))
            .and_then(|l| l.split_whitespace().nth(1))
            .and_then(|v| v.parse().ok())
            .unwrap_or(0)
    };
    Some(IoStats {
        rchar: get("rchar"),
        wchar: get("wchar"),
        syscr: get("syscr"),
        syscw: get("syscw"),
        read_bytes: get("read_bytes"),
        write_bytes: get("write_bytes"),
        cancelled_write_bytes: get("cancelled_write_bytes")
    })
}

fn open_fds() -> usize {
    std::fs::read_dir("/dev/fd").map(|d| d.count()).unwrap_or(0)
}

// -- synthetic block --------------------------------------------------------

const TRANSFER_TOPIC: &str = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";

fn hex32(seed: u64) -> String {
    format!("0x{seed:064x}")
}

fn addr(seed: u64) -> String {
    format!("0x{seed:040x}")
}

/// One block, `n` transactions, one log per transaction. Field content varies by index so
/// the sort path sees non-degenerate keys; payload sizes are mid-range mainnet-ish.
fn gen_block_json(n: usize, iteration: usize) -> Value {
    let series = (iteration as u64).wrapping_mul(1_000_003);
    let transactions: Vec<Value> = (0..n)
        .map(|i| {
            let item = series.wrapping_add(i as u64);
            json!({
                "transactionIndex": i as u32,
                "hash": hex32(0x1000_0000u64.wrapping_add(item)),
                "nonce": item,
                "from": addr(0x2222 + (item % 7)),
                "to": addr(0x3333 + (item % 11)),
                "input": format!("0x{item:0128x}"),
                "value": "0xde0b6b3a7640000",
                "type": 2,
                "gas": "0x5208",
                "gasPrice": "0x3b9aca00",
                "cumulativeGasUsed": format!("0x{:x}", 21_000u64 * (i as u64 + 1)),
                "effectiveGasPrice": "0x3b9aca00",
                "gasUsed": "0x5208",
                "logsBloom": "0x00",
                "status": 1
            })
        })
        .collect();

    let logs: Vec<Value> = (0..n)
        .map(|i| {
            let item = series.wrapping_add(i as u64);
            json!({
                "logIndex": i as u32,
                "transactionIndex": i as u32,
                "transactionHash": hex32(0x1000_0000u64.wrapping_add(item)),
                "address": addr(0x4444 + (item % 5)),
                "data": format!("0x{:0128x}", item.wrapping_mul(31)),
                "topics": [TRANSFER_TOPIC, hex32(item), hex32(item ^ 0xffff)]
            })
        })
        .collect();

    json!({
        "header": {
            "number": 20_000_000u64.wrapping_add(iteration as u64),
            "hash": hex32(0xb10cu64.wrapping_add(series)),
            "parentHash": hex32(0xb10bu64.wrapping_add(series)),
            "timestamp": 1_760_000_000i64.saturating_add(iteration as i64),
            "transactionsRoot": hex32(1),
            "receiptsRoot": hex32(2),
            "stateRoot": hex32(3),
            "logsBloom": "0x00",
            "sha3Uncles": hex32(4),
            "extraData": "0x",
            "miner": addr(0x1111),
            "size": 100_000u64,
            "gasLimit": "0x1c9c380",
            "gasUsed": "0x5208"
        },
        "transactions": transactions,
        "logs": logs,
        // present-but-empty keeps the full data-availability mask → all 5 tables built,
        // matching a full-mask prod dataset
        "traces": [],
        "stateDiffs": []
    })
}
