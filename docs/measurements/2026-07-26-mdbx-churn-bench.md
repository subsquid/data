# 2026-07-26 — mdbx vs rocks churn bench (ADR 0002 decision runs)

Supporting measurements for [ADR 0002](../adr/0002-storage-engine-libmdbx.md).
Bench: `crates/mdbx-spike` (branch `spike/libmdbx`, with the sync-serialization
and io.stat fixes made during these runs — see "incidents" below).

**Environment:** a bare-metal Debian 12 / Linux 6.1 node — Xeon E-2136
(6c/12t), 62 GiB RAM, 2× NVMe (WDC SN720) in md RAID0, ext4. Bench ran in a
docker container (`--cpus 8 --memory 16g`, private cgroupns), so cgroup
`io.stat` isolates bench IO from the box's light resident workload (~10% CPU).
Device-write numbers below are cgroup `io.stat` after excluding stacked md/dm
devices (they re-count bytes already charged to the physical NVMe); on every
run the corrected figure matched `/proc/self/io` write_bytes to <1%.

Workload shape per the run matrix in `crates/mdbx-spike/README.md`: 45
datasets, 4 pages × ~43 KiB per commit, merge fan-in 8, retention window 64,
`SafeNoSync` + 1 s durable-sync cadence, app-level LZ4 for mdbx (4.00× on
generated pages vs 4.03× prod), engine-level LZ4 for the rocks reference.

## Free-run churn — engine write amplification at matched volume

45 datasets × 2000 chunks = 15.48 GB raw churned (mdbx stores 3.87 GB
post-LZ4; rocks compresses internally).

| run | engine | mdbx page | elapsed | commits/s | device writes | amp vs raw | commit p50/p90/p99/max |
|---|---|---|---|---|---|---|---|
| 02 | rocks | — | 64.0 s | 1406 | 49.0 GB | 3.16× | 18.8 / 31.7 / 212 / 286 ms |
| 01 | mdbx | 64 KiB | 53.0 s | 1697 | 69.5 GB | 4.49× | 14.5 / 31.4 / 438 / 1360 ms |
| 06 | mdbx | 16 KiB | 25.5 s | 3529 | 24.3 GB | 1.57× | 6.5 / 13.7 / 32.6 / 2042 ms |
| 07 | mdbx | 4 KiB | 20.9 s | 4305 | 13.3 GB | 0.86× | 6.0 / 12.6 / 29.4 / 758 ms |

Page size is the whole story. At 64 KiB every COW leaf write costs a full
page for a ~10 KiB value, and mdbx writes **1.4× more** than the rocks
reference. At 4 KiB it writes **3.7× less** — the ADR 0002 gate (≥3×) passes,
and throughput more than doubles on top. Free-run max-latency tails (0.7–2 s)
are growth-step/sync stalls at 25–70× production pace; they do not appear at
production pace (below).

## Production pace — latency under merge contention

45 datasets × 600 chunks, `--paced-ms 750` → ~60 commits/s aggregate
(≈ prod 1.3/s/dataset × 45). 4.64 GB raw churned.

| run | engine | config | device writes | amp vs raw | commit p50/p90/p99/max | file / live |
|---|---|---|---|---|---|---|
| 08 | rocks | — | 20.8 GB | 4.48× | 275 µs / 388 µs / 2.4 ms / 37.5 ms | 183 MB / 123 MB |
| 03 | mdbx | 64 KiB, 1 env | 13.6 GB | 2.93× | 215 µs / 697 µs / 7.9 ms / 55 ms | 268 MB / 142 MB |
| 04 | mdbx | 64 KiB, env/dataset | 9.7 GB | 2.09× | 174 µs / 248 µs / 463 µs / 12.8 ms | 12.1 GB / 189 MB |
| 09 | mdbx | 4 KiB, 1 env | 3.8 GB | 0.81× | 174 µs / 486 µs / 4.0 ms / 20.6 ms | 268 MB / 143 MB |
| 10 | mdbx | 4 KiB, env/dataset | 3.3 GB | 0.71× | 116 µs / 198 µs / 1.1 ms / 11.0 ms | 12.1 GB / 144 MB |

Notes:

- Rocks amplification is *worse* at production pace than free-running (4.48×
  vs 3.16×): compact-on-deletion and periodic flushes churn more per stored
  byte when writes trickle in.
- At production pace with 4 KiB pages mdbx writes **5.5× less** than the
  rocks reference single-env and **6.3× less** with per-dataset envs, with
  better tails in both configs.
- Per-dataset envs are the latency winner (no cross-dataset writer-lock
  contention, syncs naturally staggered) but cost a file-footprint floor:
  45 envs × ~268 MB = 12.1 GB for <200 MB live, driven by the 256 MiB
  `growth_step` — page size does not move it (runs 04 and 10 land on the
  same file size). Growth-step tuning is a real porting decision.

## Read latency under production write load

8 readers, each scanning one random live table per 10 ms (~700–780 scans/s
aggregate; a scan = fresh snapshot/ro-txn, seek to the table prefix, cursor
over ~23 pages ≈ 1 MB raw, ending with raw bytes in hand — mdbx decompresses
app-side through the `KvRead` seam, rocks serves uncompressed block-cache
blocks). Write load = the paced production shape. The live set fits page
cache, so this compares engine read-path CPU cost, not disk-bound reads.

| run | engine | config | scan p50 | p90 | p99 | max |
|---|---|---|---|---|---|---|
| 11 | rocks | engine LZ4 | 84 µs | 461 µs | 734 µs | 40.1 ms |
| 12 | mdbx | 4 KiB, 1 env, app LZ4 | 1.51 ms | 1.79 ms | 2.59 ms | 5.0 ms |
| 13 | mdbx | 4 KiB, env/dataset, app LZ4 | 1.51 ms | 1.79 ms | 2.60 ms | 8.2 ms |
| 14 | mdbx | 4 KiB, 1 env, **no compress** | 136 µs | 256 µs | 396 µs | 4.6 ms |

- The engine read paths are comparable: mdbx 136 µs vs rocks 84 µs at p50,
  and mdbx tails are *tighter* (p99 396 µs vs 734 µs, max 4.6 ms vs 40 ms —
  no compaction jitter).
- The gap in runs 12/13 is entirely the app-compression precondition:
  ~1.4 ms per MB scanned in `lz4_flex` safe decode (~700 MB/s single-thread).
  RocksDB hides the same work inside its uncompressed block cache. Runs 12
  and 13 are identical, so the cost is per-scan CPU, not env contention. A
  faster decoder (C lz4 is 3–5×) or a small decompressed-page cache closes
  it if per-request read volume makes it matter.
- Readers do not perturb writes on either engine (commit percentiles match
  the reader-less runs), and reads never miss the write path's mutex: max
  scan stayed ≤8 ms while commits, merges and 1 s durable syncs ran.

## Stale-reader file growth (GAP-29 shape)

Run 05: 8 datasets free-running 60 s with a read txn held 60 s in a loop
(64 KiB pages): file ballooned to **8.3 GB against 33 MB live** (freelist
126 248 of 126 757 pages), commit max 1.24 s on file-extension stalls. Growth
≈ device-write rate × reader-hold duration, confirming the ADR 0002 estimate:
long readers park the entire churn window. At production pace (~30 MB/s
device) a 60 s reader pins ~2 GB per env.

## Retention wave — file high-water and post-wave reuse

Runs 15–17 answer the reclaim question ADR 0002 left open: the working set
grows when the downstream consumer stalls (retention is downstream-driven)
and shrinks back — what does the file do? `--window-wave` holds the window at
N chunks for the middle of the run (commits 20%..50%), then trims back to 64
and keeps churning; the post-wave tail measures whether a fragmented freelist
still serves the churn (at 4 KiB pages every ~10.6 KiB value needs a
3-contiguous-page overflow run). mdbx runs with an explicit
`shrink_threshold` (2× growth step), so tail truncation was armed.

| run | engine | config | peak stored live | peak file | file@end | Δfile after wave | live@end |
|---|---|---|---|---|---|---|---|
| 15 | mdbx | 4 KiB, free-run, wave 64→640→64 | 1203 MB | 2416 MB | 2416 MB | +268 MB (= 1 growth step) | 146 MB |
| 16 | mdbx | 4 KiB, paced 250 ms, wave 64→300→64 | 582 MB | 805 MB | 805 MB | **0.0 MB** | 143 MB |
| 17 | rocks | free-run, wave 64→640→64 | 4944 MB (raw; engine LZ4) | 1975 MB | **152 MB** | −1507 MB (reclaimed) | 124 MB |

- **The mdbx file is a permanent high-water mark.** Tail truncation never
  fired in either run despite the armed `shrink_threshold` and 80–93% of
  pages sitting in the freelist at the end — churn keeps the file tail
  occupied, so the only shrink path mdbx has is unreachable in practice.
  Run 15 ended at 16.6× live; the wave's footprint (Δ vs the no-wave run 07:
  2416 − 1342 ≈ 1.07 GB) matches the wave's live delta almost exactly.
- **But it does not creep: reuse is intact.** The paced run churned 27 000
  commits after the trim at constant live and grew the file by **zero
  bytes** — freelist fragmentation does not block overflow-run allocation at
  production-like cadence. Free-running overshot by exactly one growth step
  (the burst outran the 1 s durable cadence — the known SafeNoSync parking,
  bounded by cadence), then held.
- **Sizing rule**: file ≈ 1.35× peak-ever stored live, rounded up to the
  256 MiB growth step (run 16: 805.3 MB = exactly 3 steps for 582 MB peak
  live). Volumes must budget that per env, permanently; `max_size` geometry
  turns the budget into a hard quota (`MDBX_MAP_FULL` as backpressure).
- **What rocks charges for its reclaim**: same logical churn (15.48 GB raw),
  rocks wrote 49.2 GB to the device vs mdbx's 13.2 GB (3.7×) and its
  free-run tails under the wave were brutal (commit p99 200 ms, merge p99
  220 ms, delete p99 114 ms) — but the file came back to ~live within ~10 s
  of the last flush. That is the trade in one table.

## Incidents hit during the runs (both are ADR-relevant findings)

1. **Concurrent `mdbx_env_sync` aborts the process.** `mdbx_env_sync(force)`
   from a cadence thread racing `SafeNoSync` commits reliably trips the
   always-on `ENSURE(legal4overwrite)` in `dxb_sync_locked` (libmdbx 0.13.11
   via crates.io `libmdbx` 0.6.6; reproduced 2/2 on Linux within a minute,
   never seen on macOS). No upstream fix found in the 0.13.x changelog; the
   crate does not expose the built-in `MDBX_opt_sync_period` (which has its
   own assert history, upstream issue #248). A port must serialize durable
   sync with the write path; the bench now does exactly that (per-env mutex),
   so commit tails honestly include fsync waits — and at production pace they
   stayed sub-millisecond (run 04).
2. **cgroup `io.stat` double-counts on stacked block devices.** md/dm layers
   re-count bytes charged to the physical device; the bench now skips
   major 9/253 lines. Runs 01–05 predate the fix and were halved (validated
   against `/proc/self/io`, which matched to <1% on this workload — unlike
   the tempfile-spill case in the 2026-07-16 flush bench, there is no
   dirty-page rewrite gap here).

## Verdict vs the ADR 0002 gate

With 4 KiB pages, app-level LZ4 and a 1 s sync cadence, libmdbx clears the
gate on the churn shape: 3.7× fewer device writes free-running and 5.5–6.3×
fewer at production pace, with better commit tails than the rocks reference
in every paced config. 64 KiB pages — the initial default — *fail* the gate
by writing 1.4× more than rocks; the recommendation is page-size 4 KiB (or
16 KiB if read-scan cost proves dominant, at 2× the device writes).

The wave runs close the reclaim question: the mdbx file is a permanent
high-water mark (~1.35× peak stored live; tail truncation never fires) but
does not creep at constant live, while rocks buys its reclaim for 3.7× the
device writes. Consequences are codified as ADR 0002 "Disk policy": per-env
`max_size` quota, ring-past-the-floor trimming under quota pressure (coverage
gap instead of unbounded growth), and env-granular defrag (reset-reingest
required, compact-swap optional).
