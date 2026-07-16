//! Decomposed cost of one hotblocks flush (CPU_OVERUSAGE.md, Bench A + B + C).
//!
//! Reproduces the ingest hot path stage by stage on a synthetic EVM block with N rows, in
//! two modes:
//!   fresh — what prod does: push → new_chunk_processor (488 temp files) → submit+finish →
//!           readback → drop
//!   reuse — fix #3 idealized: construct once, then per flush push → submit+finish →
//!           readback → reconstruct (per-table `into_processor`, replaces construct+drop)
//! Reports wall/user/sys time per stage and, on Linux, /proc/self/io deltas.
//!
//!   cargo bench -p sqd-data --bench flush_spill                 # full sweep, both modes
//!   SQD_BENCH_QUICK=1 cargo bench -p sqd-data --bench flush_spill
//!   TMPDIR=/dev/shm ...                                         # Bench C (Linux; docker: --shm-size=2g)
//!
//! Verdicts to extract: per-stage cost = fixed + marginal×rows (Bench A); fresh−reuse =
//! idealized ceiling of processor reuse (Bench B); /tmp vs /dev/shm = fs sensitivity (Bench C).

use std::{
    collections::BTreeMap,
    time::{Duration, Instant}
};

use serde_json::{json, Value};
use sqd_data::evm::{model::Block, tables::EvmChunkBuilder};
use sqd_data_core::{BlockChunkBuilder, ChunkProcessor};

const N_STAGES: usize = 5;

#[derive(Clone, Copy, PartialEq)]
enum Mode {
    Fresh,
    Reuse
}

impl Mode {
    fn stages(self) -> [&'static str; N_STAGES] {
        match self {
            Mode::Fresh => ["push", "construct", "submit+finish", "readback", "drop"],
            Mode::Reuse => ["push", "reconstruct", "submit+finish", "readback", ""]
        }
    }
}

fn main() -> anyhow::Result<()> {
    // rows = transactions per block (logs scale 1:1); iterations shrink as rows grow
    let plan: &[(usize, usize)] = if std::env::var_os("SQD_BENCH_QUICK").is_some() {
        &[(1, 20), (100, 10)]
    } else {
        &[(1, 200), (10, 200), (100, 100), (1_000, 50), (10_000, 10), (100_000, 3)]
    };

    println!("flush_spill: EVM chunk, temp dir = {}", std::env::temp_dir().display());
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
        let fresh = run_case(rows, iters, Mode::Fresh)?;
        let reuse = run_case(rows, iters, Mode::Reuse)?;
        cases.push((fresh, reuse));
    }

    print_fixed_marginal(&cases);
    print_fresh_vs_reuse(&cases);
    Ok(())
}

struct CaseRun {
    rows: usize,
    // p50 wall per stage — median resists warmup/writeback outliers
    p50_wall: [Duration; N_STAGES]
}

impl CaseRun {
    fn total(&self) -> Duration {
        self.p50_wall.iter().sum()
    }
}

fn run_case(rows: usize, iters: usize, mode: Mode) -> anyhow::Result<CaseRun> {
    let block: Block = serde_json::from_value(gen_block_json(rows))?;

    let mut builder = EvmChunkBuilder::new();
    builder.push(&block)?;
    let block_bytes = builder.byte_size();
    builder.clear();

    let mode_name = match mode {
        Mode::Fresh => "fresh",
        Mode::Reuse => "reuse"
    };
    println!(
        "\n== mode={mode_name}  rows={rows}  iters={iters}  in-memory block ~{} KB ==",
        block_bytes / 1024
    );

    // warmup
    for _ in 0..2 {
        let mut b = EvmChunkBuilder::new();
        b.push(&block)?;
        let mut p = b.new_chunk_processor()?;
        b.submit_to_processor(&mut p)?;
        p.finish()?;
    }

    let mut acc: [StageAcc; N_STAGES] = Default::default();
    let mut fd_delta = 0usize;

    // reuse mode: the one construction whose cost is amortized away
    let mut spare: Option<ChunkProcessor> = match mode {
        Mode::Fresh => None,
        Mode::Reuse => Some(EvmChunkBuilder::new().new_chunk_processor()?)
    };
    let fds_baseline = open_fds();

    for i in 0..iters {
        let mut b = EvmChunkBuilder::new();

        measured(&mut acc[0], || b.push(&block))??;

        let mut processor = match mode {
            Mode::Fresh => {
                let fds_before = open_fds();
                let p = measured(&mut acc[1], || b.new_chunk_processor())??;
                if i == 0 {
                    fd_delta = open_fds().saturating_sub(fds_before);
                }
                p
            }
            Mode::Reuse => spare.take().expect("spare processor present")
        };

        let mut prepared = measured(&mut acc[2], || -> anyhow::Result<_> {
            b.submit_to_processor(&mut processor)?;
            b.clear();
            processor.finish()
        })??;

        measured(&mut acc[3], || -> anyhow::Result<()> {
            for table in prepared.values_mut() {
                table.read_record_batch(0, table.num_rows())?;
            }
            Ok(())
        })??;

        match mode {
            Mode::Fresh => measured(&mut acc[4], || drop(prepared))?,
            Mode::Reuse => {
                // per-table reconstruction — chunk-level into_processor was removed in 0550396
                spare = Some(measured(&mut acc[1], || -> anyhow::Result<_> {
                    let tables = prepared
                        .into_iter()
                        .map(|(name, table)| table.into_processor().map(|p| (name, p)))
                        .collect::<anyhow::Result<BTreeMap<_, _>>>()?;
                    Ok(ChunkProcessor::new(tables))
                })??);
            }
        }
    }

    match mode {
        Mode::Fresh => println!("temp files created in `construct` (fd delta): {fd_delta}"),
        Mode::Reuse => println!(
            "fds stable across iterations: {} (start {}, end {})",
            if open_fds() == fds_baseline {
                "yes"
            } else {
                "NO — LEAK"
            },
            fds_baseline,
            open_fds()
        )
    }

    println!(
        "{:<15} {:>9} {:>9} {:>9} {:>9} {:>8} {:>8} {:>10} {:>10} {:>9} {:>9}",
        "stage",
        "p50 ms",
        "mean ms",
        "user ms",
        "sys ms",
        "syscr",
        "syscw",
        "wchar KB",
        "rchar KB",
        "wr MB",
        "cancel MB"
    );

    let stages = mode.stages();
    let mut p50_wall = [Duration::ZERO; N_STAGES];
    for (idx, a) in acc.iter_mut().enumerate() {
        if a.wall.is_empty() {
            continue;
        }
        a.wall.sort();
        let p50 = a.wall[a.wall.len() / 2];
        let mean = a.wall.iter().sum::<Duration>() / a.wall.len() as u32;
        p50_wall[idx] = p50;
        let n = a.wall.len() as f64;
        let per = |v: u64| v as f64 / n;
        println!(
            "{:<15} {:>9.3} {:>9.3} {:>9.3} {:>9.3} {:>8.0} {:>8.0} {:>10.1} {:>10.1} {:>9.2} {:>9.2}",
            stages[idx],
            p50.as_secs_f64() * 1e3,
            mean.as_secs_f64() * 1e3,
            a.user.as_secs_f64() * 1e3 / n,
            a.sys.as_secs_f64() * 1e3 / n,
            per(a.io.syscr),
            per(a.io.syscw),
            per(a.io.wchar) / 1024.0,
            per(a.io.rchar) / 1024.0,
            per(a.io.write_bytes) / (1024.0 * 1024.0),
            per(a.io.cancelled_write_bytes) / (1024.0 * 1024.0)
        );
    }

    Ok(CaseRun { rows, p50_wall })
}

fn print_fixed_marginal(cases: &[(CaseRun, CaseRun)]) {
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
    let mut fixed_total = 0f64;
    let mut marginal_total = 0f64;
    for (idx, stage) in Mode::Fresh.stages().iter().enumerate() {
        let fixed = first.p50_wall[idx].as_secs_f64() * 1e3;
        let marginal = (last.p50_wall[idx].as_secs_f64() - first.p50_wall[idx].as_secs_f64()) * 1e3 / dr * 1000.0;
        fixed_total += fixed;
        marginal_total += marginal;
        println!("{:<15} {:>12.3} {:>18.3}", stage, fixed, marginal);
    }
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

fn print_fresh_vs_reuse(cases: &[(CaseRun, CaseRun)]) {
    println!("\n== Bench B: fresh vs reuse, p50 total per flush (delta = idealized reuse ceiling) ==");
    println!(
        "{:>8} {:>11} {:>11} {:>11} {:>8}",
        "rows", "fresh ms", "reuse ms", "delta ms", "saves"
    );
    for (fresh, reuse) in cases {
        let f = fresh.total().as_secs_f64() * 1e3;
        let r = reuse.total().as_secs_f64() * 1e3;
        println!(
            "{:>8} {:>11.3} {:>11.3} {:>11.3} {:>7.0}%",
            fresh.rows,
            f,
            r,
            f - r,
            (f - r) / f * 100.0
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

fn measured<T>(acc: &mut StageAcc, f: impl FnOnce() -> T) -> anyhow::Result<T> {
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
    Ok(out)
}

fn cpu_times() -> anyhow::Result<(Duration, Duration)> {
    let mut ru: libc::rusage = unsafe { std::mem::zeroed() };
    anyhow::ensure!(
        unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut ru) } == 0,
        "getrusage failed"
    );
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
fn gen_block_json(n: usize) -> Value {
    let transactions: Vec<Value> = (0..n)
        .map(|i| {
            json!({
                "transactionIndex": i as u32,
                "hash": hex32(0x1000_0000 + i as u64),
                "nonce": i as u64,
                "from": addr(0x2222 + (i % 7) as u64),
                "to": addr(0x3333 + (i % 11) as u64),
                "input": format!("0x{:0128x}", i),
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
            json!({
                "logIndex": i as u32,
                "transactionIndex": i as u32,
                "transactionHash": hex32(0x1000_0000 + i as u64),
                "address": addr(0x4444 + (i % 5) as u64),
                "data": format!("0x{:0128x}", (i as u64).wrapping_mul(31)),
                "topics": [TRANSFER_TOPIC, hex32(i as u64), hex32(i as u64 ^ 0xffff)]
            })
        })
        .collect();

    json!({
        "header": {
            "number": 20_000_000u64,
            "hash": hex32(0xb10c),
            "parentHash": hex32(0xb10b),
            "timestamp": 1_760_000_000i64,
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
