# LSM depth, compaction style, and the ADR 0002 gate at production scale

2026-07-28. Bare-metal Xeon E-2136 / 2× NVMe md RAID0, docker `--cpus 8 --memory
16g --cgroupns private`. Device writes from cgroup `io.stat`, stacked md/dm
majors excluded.

## Why these runs exist

Every earlier bench arm (runs 01–39) used `--window 64`, which leaves **119 MB
live**. Production `CF_TABLES` is 63 GB (internal) to 75 GB (mainnet). At 119 MB
RocksDB has a single populated level: no ladder, no compaction cascade, and an
`lsm_write_amp` of ~1×. The engine comparison the ADR gate rests on was
therefore run against a RocksDB that never did the thing that costs it money.

`--window 6000` puts 11.8 GB live and three populated levels on the bench.
That is not production's four levels at 75 GB, but it is the same regime.

| | bench, 119 MB | bench, 11.8 GB | prod mainnet, 75 GB |
|---|---|---|---|
| populated levels | 1 | 3 | 4 |
| `lsm_write_amp` | ~1× | 4.52× | 6.39× |
| device writes / raw ingest | 1.05× | 1.61× | 1.81× |

## Production attribution (for reference)

Read from `/run/db/LOG`, which RocksDB dumps every `stats_dump_period_sec`
(600 s, unset in our options). `--rocksdb-stats` was **not** needed and would not
have helped: it only calls `enable_statistics()`, and `RocksDbCollector` reads
integer properties, never tickers.

Per-pod, cumulative over 9.3 h, `mainnet-internal-db-0`:

| source | MiB/s | share |
|---|---|---|
| compaction L0→L4 | 169.9 | 69.8 % |
| flush memtable→L0 | 32.9 | 13.5 % |
| WAL (post-Zstd, by residual) | 20.3 | 8.3 % |
| compaction L4→L5 | 10.9 | 4.5 % |
| compaction L5→L6 | 7.4 | 3.0 % |
| hash indexes (both) | 1.9 | 0.8 % |
| **device, measured** | **243.3** | |

The L4 concentration is **not** the natural shape: internal was running
`--rocksdb-level-base-mb 2048` at the time, the setting measured at +16 % device
writes and reverted. `mainnet`, on the 256 MB default, spreads the same total
across the ladder — L3 21.8 %, L4 23.6 %, L5 23.4 %, L6 8.5 %. The
source split (compaction ~77 %, flush ~14 %, WAL ~8 %) holds on both.

WAL compression cross-checks: the bench's `rocksdb.wal.bytes` ticker reports
232.5 GB against 187.0 GB of measured device writes, so Zstd compresses the WAL
~5.8×. The production residual gives ~6.3× by an independent route.

Absolute device-write figures reported before 2026-07-28 (401 / 465 MiB/s) were
~1.9× high: the PromQL summed a duplicated device dimension. `io.stat` shows it
directly — `259:0` and `253:4` carry identical `wbytes`.

## Compaction style

45 datasets × 15000 chunks, window 6000, 512 MB × 4 memtable, direct I/O.

| run | style | `lsm_write_amp` | compaction | device | file / live | commit p99 / max |
|---|---|---|---|---|---|---|
| 40 | leveled | **4.52×** | 114.6 GB | **187.0 GB** | 11.9 / 11.8 GB | **57.7 / 185 ms** |
| 41 | universal, amp 200 | 7.21× | 201.6 GB | 274.0 GB | 15.4 / 14.2 GB | 68.2 / 661 ms |
| 42 | universal, amp 400 | 7.05× | 196.0 GB | 268.4 GB | 41.2 / 17.7 GB | 68.2 / 713 ms |

Universal loses on every axis simultaneously — it is not a space-for-writes
trade here. `max_size_amplification_percent` triggers a **full** rewrite of the
CF whenever accumulated garbage doubles the live size, and retention produces
that garbage continuously. Raising the bound to 400 % barely moves the write
volume (7.21 → 7.05) and inflates the file to 41 GB against 17.7 GB live.

The reasoning that motivated the arm — "universal has lower write amplification"
— holds for insert-dominated workloads. This one is inserts plus a dense delete
stream.

## L0 compaction trigger

Same workload, leveled, `level0_slowdown/stop` held at the stock 1:5:9 ratio.

| run | `level0_file_num_compaction_trigger` | `lsm_write_amp` | device |
|---|---|---|---|
| 40 | 4 (default) | 4.52× | 187.0 GB |
| 43 | 8 | **4.24×** | **177.7 GB** |
| 44 | 16 | 4.31× | 180.2 GB |
| 45 | 32 | 4.29× | 179.7 GB |

A one-off 5 % at 4→8, then a plateau; 8/16/32 sit within 2 % of each other. No
stalls in any arm (`write_stopped` 0.00 %).

The model that motivated this — each L0→base merge rewrites the whole
overlapping base level, so the step costs `1 + base_size/L0_batch` — is right
about production (internal's L4 measures W-Amp 5.2 with `Rnp1/Rn` = 4.9) but
does not transfer to the bench, where the base level is *smaller* than one L0
batch:

| level | size | Write | W-Amp |
|---|---|---|---|
| L0 | 71 MB | 29.0 GB | 1.0 |
| L4 (base) | 71 MB | 32.7 GB | 2.2 |
| L5 | 998 MB | 19.7 GB | 10.2 |
| L6 | 9.99 GB | 40.1 GB | 2.8 |

At 11.8 GB live the ladder puts the base at 71 MB against a 568 MB L0 batch, so
the base rewrite was already cheap and there was no denominator to grow.

## Engine comparison at depth

Same workload and depth; mdbx at 4 KiB pages, `--compress`, single env, default
1 s sync cadence.

| | rocks leveled (40) | mdbx 4 KiB (46) | ratio |
|---|---|---|---|
| device writes | 187.0 GB | **112.0 GB** | **1.67×** |
| device / raw ingest | 1.61× | 0.97× | |
| throughput | 1073 commit/s | **2816 commit/s** | 2.6× |
| commit p50 | 33.9 ms | **7.8 ms** | 4.3× |
| commit p99 | 57.7 ms | 61.9 ms | parity |
| commit max | **185 ms** | 1965 ms | 10.6× worse |
| merge p50 | 41.4 ms | **0.49 ms** | 85× |
| file / live | 11.9 / 11.8 GB | 14.5 / 13.4 GB | |

Depth widens mdbx's write advantage over the 1.23× / 1.43× that runs 27–34
measured at 119 MB live, but only to **1.67×**. The ADR 0002 gate of ≥3× fails
at production depth as well — the objection that the tuned-RocksDB verdict was
measured at the wrong scale is now closed.

The commit tail is the honest cost: 1.96 s max against RocksDB's 185 ms, from
the durable-sync cadence serialized against write txns. For a service whose SLI
is head freshness that is a separate problem to solve, tracked as P-DUR-SYSTEM.

## What tuning has and has not bought

| change | effect | status |
|---|---|---|
| memtable 512 MB × 4 | stalls 1.206 % → 0, device writes −40 % (internal) | shipped |
| `max_bytes_for_level_base` 2048 | +16 % device writes | reverted (infra#634) |
| universal compaction | +46 % device writes, +18 % commit p99 | rejected |
| `level0_file_num_compaction_trigger` | −5 %, plateaus at 8 | not worth a flag |

The residual ~6.4× is the level count, not one bad step, and no knob removes a
level without paying more per merge than it saves.
