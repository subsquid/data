# ADR 0002 — Replace RocksDB with libmdbx in hotblocks storage

Status: proposed (spike pending) · Date: 2026-07-25 · Scope: `sqd-storage`, `sqd-hotblocks`

## Context

The original complaint — "we burn CPU on compaction" — is stale: the CPU cost was
tempfile spill and was removed by ADR 0001 (a production replica now runs at ~2–3
cores). What remains is the disk bill of the LSM itself, measured on a production
replica serving 45 datasets (2026-07-25):

| metric | value |
|---|---|
| device writes | ~640 MB/s sustained (≈55 TB/day) |
| device reads | ~690 MB/s sustained |
| live SST size | ~62 GB |
| concurrent compactions | 1.4–2.3, continuously |
| commit rate | ~59 chunk commits/s pod-wide (~72 blocks/s ingested) |
| reader (snapshot) lifetime | avg ~12 s, max 233 s/day, ≤42 concurrent |

Writing ~640 MB/s to rewrite a 62 GB working set is an amplification factor the
data does not ask for: keys are append-mostly (UUIDv7 table prefixes, ascending
chunk keys), values are 16–64 KiB opaque pages, and the whole store is a rolling
retention window. Write stalls (`is-write-stopped`) fired on two stacks within the
last day. A B+tree engine with in-place page reuse (libmdbx — the engine under
reth and erigon) charges COW page churn (~2–3×) instead of LSM leveling (~10×+),
and has no background compaction to interfere with read tails.

One honest caveat: a second, application-level write stream — the chunk
`compaction_loop` that merges small chunks up to 200k rows — rewrites the
retention window log-many times and survives any engine swap. libmdbx removes
only the LSM multiplier stacked on top of it.

Decision weights, agreed up front: latency ×3, disk amplification ×2, migration
cost ×1.

- **Latency (×3)**: no compaction ⇒ no stalls, no L0 spikes, flatter read tails.
  New structural risk: libmdbx has one writer per environment, and a chunk-merge
  transaction holds that lock long enough to stall head freshness of every
  dataset in the env. Mitigation is one env per dataset (also restores INV-35
  isolation and demotes HZ-1 from structural to per-dataset).
- **Disk (×2)**: device writes expected to drop severalfold — but only with
  application-level page compression in place, because libmdbx does not compress
  and the measured LZ4 ratio on `CF_TABLES` is 4× (see Measurements). The seam
  for it already exists (`BufferPageWriter::write_page`). The entire deferred-space
  machinery (10 s point-delete sweep, deletion-collector, periodic compaction,
  startup `DeleteFilesInRange`, the `TableId` watermark, `reclaim-measure`)
  becomes unnecessary: pages free at commit. RS-5/6/10, LIV-7 and GAP-6 close by
  construction.
- **Migration (×1)**: no data conversion exists to pay for. The store is fully
  re-ingestable from upstream (NG1 "not an archive"; DEF-17 hash indexes are
  expendable; GAP-28 Api retention floors are memory-only and re-pushed by
  hotblocks-retain within ≤300 s; GAP-15 no format gate). Rollout is blue/green
  on an empty volume. The cost is code: the `table::read`/`table::write` codec is
  already generic over `kv.rs` (`KvRead`/`KvWrite`/`KvReadCursor`) and ports
  nearly verbatim, but `crates/storage/src/db/*` (~1300 lines) threads RocksDB
  types throughout — transactions, metrics, CLI, shutdown, tests. Estimate: 3–6
  weeks to port and roll out.

## Porting notes (not blockers)

1. **GAP-29 (zombie readers)** costs more under libmdbx — one stale read txn
   blocks reuse of every page freed after it started — but the engine ships the
   containment: the reth `libmdbx` binding aborts read transactions past
   `MaxReadTransactionDuration` (~5 min default; the stalled query errors out
   through the existing abort path), and `mdbx_env_set_hsr` fires under space
   pressure, naming the laggard reader and letting the callback kill its txn
   before the file grows. Measured reader tail today (233 s max/day) sits under
   both bounds. No hotblocks-side deadline machinery is required; GAP-29 stays
   open as its own P2.
2. **Direct I/O** is on in production (`--rocksdb-disable-direct-io` unset) and is
   incompatible with an mmap engine; page-cache behavior must be re-validated.

## Decision

Do not port yet. Run a 1–2 week spike and decide on numbers:

1. Implement `KvRead`/`KvWrite`/`KvReadCursor` over libmdbx (64 KiB pages match
   the table page size; geometry auto-grow instead of a fixed map size), with
   LZ4 at the `BufferPageWriter::write_page` seam from the start — the 4× ratio
   makes an uncompressed spike unrepresentative.
2. Drive the S4 churn-soak envelope (`spec/10-performance.md`) through
   `crates/hotblocks-harness`: device writes, file growth under retention churn,
   head-flush latency under ingest × chunk-merge × retention contention — one
   shared env vs env-per-dataset.
3. Compare against the production baseline in this document.

Proceed to the port only if the spike shows device writes dropping ≥3× without
head-freshness regressions.

## Measurements

- `CF_TABLES` LZ4 ratio, production replica 2026-07-25
  (`rocksdb.aggregated-table-properties`): raw value size 238.0 GB → data
  blocks 59.1 GB = **4.03×** (5.58 M entries, avg raw value 42.6 KiB → ~10.6 KiB
  compressed; keys negligible at 169 MB).
- Consequence: application-level page compression is a **precondition**, not a
  contingency. Uncompressed pages inflate both the file (~62 → ~250 GB) and the
  COW write stream by 4×, which cancels the expected write-amplification win
  (LSM ~10× on compressed bytes ≈ COW ~2.5× on 4×-inflated bytes). The spike
  must therefore run with LZ4 at the `BufferPageWriter::write_page` seam from
  day one.

## Alternatives considered

- **Tune RocksDB harder** (universal compaction, larger memtables, relaxed
  leveling): reduces but keeps the multiplier, keeps stalls and the reclaim
  machinery; the workload shape (rolling window, append keys, big values) is the
  textbook non-LSM case.
- **Vanilla LMDB**: fixed map size must be pre-declared and nothing in the system
  bounds disk today (`P-DISK-FLOOR` undefined); 4 KiB pages force overflow chains
  for every table page. libmdbx auto-grows and supports 64 KiB pages.
- **Reduce the application chunk-merge churn instead**: engine-independent and
  worth doing regardless, but it cannot remove LSM leveling amplification on the
  ingest stream itself.
