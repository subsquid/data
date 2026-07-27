# 2026-07-27 — RocksDB memtable tuning: device writes and write stalls

Production runs `CF_TABLES` on RocksDB's stock memtable defaults — `db.rs` sets no
memtable, L0, file-size or compaction-style option, so `write_buffer_size` is 64 MB and
`max_write_buffer_number` is 2. Both of the costs that motivated a storage-engine
replacement turn out to be attributable to that.

## The production stall is the memtable ceiling, not compaction debt

Measured over 7 d on all four stacks (VictoriaMetrics, `hotblocks_rocksdb_*`):

| signal | stalled pods | healthy pods |
|---|---|---|
| `write_stopped` max | 1 (internal ×2, testnet-dev ×2) | 0 (mainnet ×2, morpho ×2) |
| `immutable_memtables` max | **2** | **1** |
| `files_at_level0` max | 15 | 7–11 |
| `pending_compaction_bytes` max | 16.2 GB | 2.3–6.0 GB |

`max_write_buffer_number` is 2, so `immutable_memtables == 2` *is* the stop condition, and
it partitions the pods exactly. The alternatives do not: `files_at_level0` peaks at 15
against a default `level0_stop_writes_trigger` of 36 and is the same order on both groups,
and `pending_compaction_bytes` peaks at 5.9 % of the 256 GB default hard limit. Cost today:
`mainnet-internal-db-1` spends **1.2 % of wall time** write-stopped (~2 h/week),
`internal-db-0` 0.089 %.

## Bench sweep

`crates/mdbx-spike --engine rocks`, free-run 45 datasets × 2000 chunks = 15.48 GB raw
churned, on a bare-metal Xeon E-2136 / 2× NVMe md RAID0 box, in docker `--cpus 8
--memory 16g --cgroupns private`. Device writes are cgroup `io.stat` excluding the stacked
md/dm majors. All arms carry production's Zstd WAL compression, `max_background_jobs=8`,
and **direct I/O** — production parity on all three (verified 2026-07-27 that no stack
passes `--rocksdb-disable-direct-io`, and that `cli.rs` inverts the flag, so prod runs
`set_use_direct_reads` + `set_use_direct_io_for_flush_and_compaction`).

| run | write_buffer | max_buffers | device writes | amp vs raw | write_stopped | max immutable | commit p99/max |
|---|---|---|---|---|---|---|---|
| 27 | 64 MB | 2 | 47.73 GB | 3.08× | 14.26 % | 2 (ceiling 2) | 160 / 2779 ms |
| 28 | 64 MB | 2 | 39.07 GB | 2.52× | 5.97 % | 2 (ceiling 2) | 83 / 1569 ms |
| 35 | 128 MB | 4 | 38.26 GB | 2.47× | **0.00 %** | 2 (ceiling 4) | — |
| 36 | 256 MB | 4 | 23.45 GB | 1.51× | 0.00 % | 2 (ceiling 4) | — |
| 37 | 512 MB | 2 | 16.29 GB | 1.05× | 0.00 % | 1 (ceiling 2) | — |
| 29 | 512 MB | 4 | **16.28 GB** | 1.05× | 0.00 % | 1 (ceiling 4) | 56 / 118 ms |
| 39 | 1024 MB | 4 | 11.98 GB | 0.77× | 0.00 % | 1 (ceiling 4) | — |

Run 27 is buffered I/O; every other arm is direct. Run 28 is therefore the honest
stand-in for production as deployed today.

Two effects separate cleanly:

- **Buffer count fixes the stall; it does nothing for write volume.** 128 MB × 4 already
  takes `write_stopped` from 5.97 % to zero while device writes move 39.07 → 38.26 GB
  (noise). The stall is purely a headroom problem: with a ceiling of 2 there is exactly one
  spare buffer while its predecessor flushes.
- **Buffer size drives write volume; count is irrelevant to it.** 512 MB × 2 and 512 MB × 4
  land at 16.29 vs 16.28 GB. A 64 MB buffer flushes small L0 files continuously and the
  resulting compaction generations are most of the device bill; an 8× larger buffer flushes
  8× less often into 8× larger files.

The curve has **not** plateaued at 1024 MB, and 512 MB is not a knee — it is a point on a
still-falling slope chosen for its memory cost.

## Consequence for ADR 0002

The mdbx arm of the same bench, same build, same session: **13.27 GB** free-run (run 33),
3.28 GB at production pace (run 34). Against the tuned reference the engine gap is:

| shape | mdbx | rocks 512 MB × 4 | ratio |
|---|---|---|---|
| free-run 45 × 2000 | 13.27 GB | 16.28 GB | 1.23× |
| paced 45 × 600 @ 750 ms | 3.28 GB | 4.69 GB (run 32) | 1.43× |

ADR 0002 gates the port on "device writes dropping ≥ 3×". Against a reference at
production I/O parity with one memtable change, the measured advantage is 1.2–1.4×, and at
1024 MB × 4 RocksDB writes **less** than mdbx (11.98 vs 13.27 GB). The gate does not pass.

What tuning does not address, and what an engine decision should now be argued on: commit
tails (mdbx paced p99 379 µs vs 3743 µs, ~10×) and per-dataset isolation (INV-35 / LIV-8,
known-violated as GAP-1).

## Recommended values, and what is unmeasured

Ship `--rocksdb-write-buffer-mb 512 --rocksdb-max-write-buffers 4`. It removes the stall
class outright and cuts device writes ~2.4× against production as deployed, at 2 GB of
memtable — scoped to `CF_TABLES`, which takes essentially all the write volume, so the cost
is paid once rather than per column family.

Not measured, and required before or during rollout:

- **Boot time.** A larger memtable means a longer WAL replay at startup. This service has a
  documented startup-probe kill-loop at a 90 s budget, so the recovery path must be timed
  on internal before mainnet.
- **Level sizing.** `max_bytes_for_level_base` is still at the 256 MB default, now smaller
  than a single memtable. The textbook pairing is L1 ≈ `level0_file_num_compaction_trigger`
  × `write_buffer_size`. The numbers above were obtained *without* touching it, so they are
  a floor on what tuning can do, not a ceiling.
- **1024 MB.** Better on writes (11.98 GB) but 4 GB of memtable against a 12 G request, and
  a proportionally longer replay. Revisit once boot time is known.
- **Scale.** Bench live is ~130 MB against production's ~62 GB. The stall mechanism is
  scale-free (it is a flush-vs-fill race), but the write-volume ratios are not guaranteed to
  hold at production live size.

Production prediction to verify on rollout: device writes per mainnet pod ~397 MB/s today
(corrected figure — see `2026-07-27-production-sli-baseline.md` for the double-counting
traps in `container_fs_*`) should fall to roughly 140–170 MB/s.
