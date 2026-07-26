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

Two reference caveats: the rocks reference runs buffered I/O — which,
corrected 2026-07-26, *matches* production (the chart passes
`--rocksdb-disable-direct-io` on every stack; an earlier ADR 0002 revision
claimed production ran direct I/O) — though the read runs stay scoped to
CPU cost, since the bench cache geometry differs from prod's 8 GiB block
cache; and peak-file is sampled at 500 ms — exact for mdbx (the file only
grows), able to undershoot a rocks transient between compactions.

Workload shape per the run matrix in `crates/mdbx-spike/README.md`: 45
datasets, 4 pages × ~43 KiB per commit, merge fan-in 8, retention window 64,
`SafeNoSync` + 1 s durable-sync cadence, app-level LZ4 for mdbx (4.00× on
generated pages vs 4.03× prod), engine-level LZ4 for the rocks reference.

## Free-run churn — engine write amplification at matched volume

45 datasets × 2000 chunks = 15.48 GB raw churned (mdbx stores 3.87 GB
post-LZ4; rocks compresses internally).

| run | engine | mdbx page | elapsed | commits/s | device writes | amp vs raw | commit p50/p90/p99/max |
|---|---|---|---|---|---|---|---|
| 02 | rocks (pre-parity) | — | 64.0 s | 1406 | 49.0 GB | 3.16× | 18.8 / 31.7 / 212 / 286 ms |
| 18 | rocks (**prod parity**) | — | 98.0 s | 918 | 47.1 GB | 3.04× | 37.5 / 52.7 / 126 / 239 ms |
| 01 | mdbx | 64 KiB | 53.0 s | 1697 | 69.5 GB | 4.49× | 14.5 / 31.4 / 438 / 1360 ms |
| 06 | mdbx | 16 KiB | 25.5 s | 3529 | 24.3 GB | 1.57× | 6.5 / 13.7 / 32.6 / 2042 ms |
| 07 | mdbx | 4 KiB | 20.9 s | 4305 | 13.3 GB | 0.86× | 6.0 / 12.6 / 29.4 / 758 ms |

Page size is the whole story. At 64 KiB every COW leaf write costs a full
page for a ~10 KiB value, and mdbx writes **1.4× more** than the rocks
reference. At 4 KiB it writes **3.5× less** — the ADR 0002 gate (≥3×) passes,
and throughput more than doubles on top. Free-run max-latency tails (0.7–2 s)
are growth-step/sync stalls at 25–70× production pace; they do not appear at
production pace (below).

Run 18 re-runs the reference at production parity after external review
caught the gap (runs 01–17 rocks lacked Zstd WAL compression and ran the
default 2 background jobs; prod sets both — `db.rs` `db_options`). Free-run
the correction nets out to −4%: the ~11 GB the compressed WAL saves is eaten
by the extra in-window compaction eight jobs complete (elapsed 64→98 s —
free-running 45 writers now share 8 CPUs with 8 compaction jobs, which is
also why its commit p50 doubles). Gate ratio 49.0→47.1 GB: **3.54×**, robust.

## Production pace — latency under merge contention

45 datasets × 600 chunks, `--paced-ms 750` → ~60 commits/s aggregate
(≈ prod 1.3/s/dataset × 45). 4.64 GB raw churned.

| run | engine | config | device writes | amp vs raw | commit p50/p90/p99/max | file / live |
|---|---|---|---|---|---|---|
| 08 | rocks (pre-parity) | — | 20.8 GB | 4.48× | 275 µs / 388 µs / 2.4 ms / 37.5 ms | 183 MB / 123 MB |
| 19 | rocks (**prod parity**) | — | 12.5 GB | 2.69× | 524 µs / 669 µs / 3.5 ms / 42.8 ms | 135 MB / 123 MB |
| 03 | mdbx | 64 KiB, 1 env | 13.6 GB | 2.93× | 215 µs / 697 µs / 7.9 ms / 55 ms | 268 MB / 142 MB |
| 04 | mdbx | 64 KiB, env/dataset | 9.7 GB | 2.09× | 174 µs / 248 µs / 463 µs / 12.8 ms | 12.1 GB / 189 MB |
| 09 | mdbx | 4 KiB, 1 env | 3.8 GB | 0.81× | 174 µs / 486 µs / 4.0 ms / 20.6 ms | 268 MB / 143 MB |
| 10 | mdbx | 4 KiB, env/dataset | 3.3 GB | 0.71× | 116 µs / 198 µs / 1.1 ms / 11.0 ms | 12.1 GB / 144 MB |

Notes:

- **Parity matters most exactly here.** Free-run the WAL correction nets out
  (run 18); at production pace the uncompressed WAL was ~40% of the old
  reference's device writes — run 19 lands at 12.5 GB (amp 2.69× vs 4.48×),
  and the mdbx advantage honestly reads **3.3× (single env) / 3.8×
  (env-per-dataset)**, not the 5.5–6.3× the pre-parity reference suggested.
  The gate still passes; the Zstd WAL work moves rocks' commit p50 to
  ~2× mdbx's and its tails stay compaction-shaped (p99 3.5 ms vs 1.1 ms).
- The pre-parity claim "rocks amplification is worse at production pace than
  free-running" (4.48× vs 3.16×) was mostly the uncompressed-WAL artifact:
  at trickle pace the WAL is the dominant write stream, free-running it is
  compaction. On the parity reference the ordering inverts — paced 2.69×
  vs free-run 3.04×.
- Per-dataset envs are the latency winner (no cross-dataset writer-lock
  contention, syncs naturally staggered) but cost a file-footprint floor:
  45 envs × ~268 MB = 12.1 GB for <200 MB live, driven by the 256 MiB
  `growth_step` — page size does not move it (runs 04 and 10 land on the
  same file size). Growth-step tuning is a real porting decision.

## Concurrent merges — head commits vs the writer lock (runs 20–21)

External review caught that runs 01–17 merged *inline* in the worker loop:
with env-per-dataset, commits never waited on a writer lock held by a
running merge, so those latency tables measure no head-behind-merge
queueing — while production's `compaction_loop` merges concurrently with
ingest. `--concurrent-merges` moves each merge to a background thread
(max 1 in flight per dataset) and re-runs the paced shape:

| run | engine | config | device writes | commit p50/p90/p99/max | merge p50/p99/max |
|---|---|---|---|---|---|
| 20 | mdbx | 4 KiB, env/dataset, concurrent | 3.28 GB (0.71×) | 128 µs / 204 µs / **323 µs** / 14.1 ms | 0.6 / 9.7 / 15.1 ms |
| 21 | rocks | prod parity, concurrent | 13.0 GB (2.81×) | 577 µs / 987 µs / 3.9 ms / 60.2 ms | 4.8 / 57.1 / 113.5 ms |
| 10 | mdbx | same as 20, inline (reference) | 3.3 GB (0.71×) | 116 µs / 198 µs / 1.1 ms / 11.0 ms | — |

Device writes are unchanged for mdbx and the commit p99 stays
sub-millisecond under contention; the commit *max* is exactly one merge-txn
hold (14.1 ms commit vs 15.1 ms merge max) — a queued head flush waits out
the running merge txn, nothing more. That bound is the load-bearing fact:
it scales with merge-txn size, which is why the port carries the
bounded-write-txn invariant (8 MiB sub-commits, lock released between —
ADR Stage 1) and the S4 envelope re-asserts it at production merge sizes.
The feared trade also inverts: rocks has no writer lock to queue on, yet
its head tails under the same concurrent-merge load are ~12× worse at p99
and ~4× at max (compaction jitter) — the lock mdbx pays for is cheaper
than the compaction rocks pays with. Write ratio in this shape: 3.97×.

## Hash-index random-insert stream (runs 22–26, ADR porting note 4)

`CF_BLOCK_HASHES`/`CF_TRANSACTION_HASHES` are hash-keyed — uniformly random
inserts, one per transaction, enabled on mainnet-internal today. An LSM
absorbs them in the memtable; a B+tree COWs a leaf path per touched leaf per
txn. Priced here: 16 datasets at production pace with concurrent merges,
plus a hash stream of 300 entries/commit (~internal's per-chunk tx count,
32 B keys / 12 B values) inserted in the commit txn and deleted with the
chunk at retention, against a **preseeded 1 M-entry tree per dataset**
(~21 k leaves — a small tree understates the cost by orders of magnitude;
prod tidx is 10–100× deeper). mdbx keeps hash keys in the same tree under a
sentinel prefix (COW-equivalent to a named subtable); rocks gets a dedicated
CF mirroring production (`hash_index_cf_options`: bloom, LZ4, deletion
collector).

| run | engine | hash stream | device writes | Δ index cost | commit p50/p99/max | delete p50 |
|---|---|---|---|---|---|---|
| 25 | mdbx | — | 1.17 GB | — | 132 µs / 418 µs / 5.9 ms | 86 µs |
| 22 | mdbx | per-commit (K=1) | 32.34 GB | **+31.2 GB** | 1.58 ms / 2.7 ms / 125 ms | 10.7 ms |
| 23 | mdbx | write-behind K=16 | 23.97 GB | +22.8 GB | 213 µs / 26 ms / 64 ms | 8.8 ms |
| 26 | rocks | — | 3.13 GB | — | 583 µs / 3.1 ms / 22.8 ms | 515 µs |
| 24 | rocks | per-commit (K=1) | 7.13 GB | **+4.0 GB** | 1.28 ms / 4.5 ms / 16.7 ms | 4.6 ms |

- **The index stream inverts the engine decision on this shape.** The same
  stream costs mdbx +31.2 GB (27× its own data stream; ~3.25 MB per commit =
  ~600 random leaf COWs × 4 KiB, matching the model) vs rocks +4.0 GB —
  total 32.3 vs 7.1 GB, i.e. with global hash tables mdbx writes **4.5×
  more** than rocks. The raw index churn is ~127 MB — mdbx pays ~245× on it.
- **Write-behind batching does not rescue it**: K=16 saves only 26% (4 800
  sorted keys still touch mostly-distinct leaves of a 21 k-leaf tree) and
  moves the cost into flush-commit tails (p99 26 ms). Amortization arrives
  only when a batch approaches the tree width — hours of accumulation at
  this rate.
- Latency degrades across the board under K=1: commit p50 12× (132 µs →
  1.58 ms), delete p50 124× (86 µs → 10.7 ms — 300 random-key deletes COW
  as many leaves as inserts).
- Conclusion: under mdbx the hash indexes MUST NOT port as global
  hash-keyed tables. The append-shaped restructure — per-chunk index
  tables written with the chunk (~12 KB/commit here, ~270× cheaper) and
  lookups fanning out over chunk tables (bounded by chunk count; per-chunk
  bloom if the fan-out ever hurts) — or an explicit decision to keep the
  feature rocks-side/elsewhere. ADR porting note 4 and Stage 2 carry the
  verdict.

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
  and 13 are identical, so the cost is per-scan CPU, not env contention.
  Decoder shoot-out on the same generated page shape, hot single-thread on
  this node (2026-07-26; an M2 Max agrees on the ratios within ~10%):
  C `lz4` into a reused buffer 7.7 GB/s = **5.8×** over the `lz4_flex`
  safe-decode-with-alloc used here (1.3 GB/s); `lz4_flex` without
  safe-decode only 2.0×. The hot-loop safe figure (0.76 ms/MB) vs the
  in-situ ~1.4 ms/MB above shows ~1.9× of mmap-cold + cursor overhead that
  any decoder keeps — in service expect C `lz4` at ~0.25 ms/MB, not 0.13.
  A small decompressed-page cache stays the fallback if per-request read
  volume still makes decode matter.
- Readers do not perturb writes on either engine (commit percentiles match
  the reader-less runs), and reads never miss the write path's mutex: max
  scan stayed ≤8 ms while commits, merges and 1 s durable syncs ran.

## Stale-reader file growth (GAP-29 shape)

Run 05: 8 datasets free-running 60 s with a read txn held 60 s in a loop
(64 KiB pages): file ballooned to **8.3 GB against 33 MB live** (freelist
126 248 of 126 757 pages), commit max 1.24 s on file-extension stalls. Growth
≈ device-write rate × reader-hold duration, confirming the ADR 0002 estimate:
long readers park the entire churn window. At production pace a 60 s reader
parks ~0.5 GB in a shared 4 KiB env (8.4 MB/s device, run 09), ~10 MB per
env in the recommended per-dataset layout (7.3 MB/s pod-wide across 45
envs, run 10), and ~1.8 GB only in the 64 KiB single-env shape (30 MB/s,
run 03).

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
- **Sizing rule (paced)**: file ≈ 1.35× peak-ever stored live, rounded up to
  the 256 MiB growth step (run 16: 805.3 MB = exactly 3 steps for 582 MB
  peak live). Free-run bursts add the SafeNoSync parking term — roughly
  device-write rate × sync cadence — on top: run 15 peaked at 2.0× peak
  live, and the no-wave free-run file (run 07, 1342 MB over ~145 MB live)
  is almost entirely parking. Volumes must budget the paced figure plus
  catch-up-burst parking per env, permanently; `max_size` geometry turns
  the budget into a hard quota (`MDBX_MAP_FULL` as backpressure).
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
gate on the churn shape against the **production-parity** reference (runs
18–19: Zstd WAL + 8 background jobs): **3.5× fewer device writes
free-running and 3.3–3.8× fewer at production pace**, with better commit
tails in every paced config. The pre-parity reference (runs 02/08, no WAL
compression) overstated the paced advantage as 5.5–6.3× — the free-run
ratio barely moved. 64 KiB pages — the initial default — *fail* the gate
by writing 1.4× more than rocks; the recommendation is page-size 4 KiB (or
16 KiB if read-scan cost proves dominant, at 2× the device writes).

One scale caveat on the gate: bench live is ~150 MB against production's
~68 GB. LSM leveling amplification grows with live size (more levels, more
rewrite generations) while mdbx COW spine depth grows logarithmically — the
error is in mdbx's favor, but the production-size magnitude is confirmed
only by the S4 soak (ADR Stage 4), not by this bench. The bench also models
neither metadata CFs nor the hash indexes (ADR porting note 4 — a
random-insert stream that an LSM absorbs and a B+tree pays leaf-path COW
for; internal runs both indexes).

The wave runs close the reclaim question: the mdbx file is a permanent
high-water mark (~1.35× peak stored live at paced rate, plus a sync-cadence
parking window on free-run bursts; tail truncation never fires) but does
not creep at constant live, while rocks buys its reclaim for ~3.5× the
device writes (wave run 17 ran the pre-parity reference; the free-run
parity correction is −4%). Consequences are codified as ADR 0002 "Disk policy": per-env
`max_size` quota, ring-past-the-floor trimming under quota pressure (coverage
gap instead of unbounded growth), and env-granular defrag (reset-reingest
required, compact-swap optional).
