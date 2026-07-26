# ADR 0002 — Replace RocksDB with libmdbx in hotblocks storage

Status: proposed — spike done 2026-07-26, write gate passed
([measurements](../measurements/2026-07-26-mdbx-churn-bench.md)); Stage 0
(spec amendment) landed 2026-07-26; flips to accepted after the Stage 1
vertical slice — real table build → chunk merge → retention → crash recovery
through the mdbx seam, with the checksummed codec and MAP_FULL /
stale-reader / kill-9 / cold-read-smoke / online-reset tests. The
production-size cold-cache read gate is deliberately *not* an acceptance
gate: it bounds rollout (Stage 4, before Stage 5), and the accept-time read
exposure is bounded instead by the measured production scan bracket
(Measurements). · Date: 2026-07-25 · Scope:
`sqd-storage`, `sqd-hotblocks`, `crates/hotblocks/spec`

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
  for it already exists (`BufferPageWriter::write_page`). The engine-reclaim
  machinery (10 s point-delete sweep, deletion-collector, periodic compaction,
  startup `DeleteFilesInRange`, the `TableId` watermark, `reclaim-measure`)
  becomes unnecessary: pages free at commit. Not the application half: table
  builds flush in 8 MiB sub-commits before the chunk metadata publishes
  (`TableStorage::put`), so a crash leaves committed orphan pages the engine
  cannot recognize — the dirty-table journal and the startup orphan purge
  survive the port (cheaper there: one delete txn at boot, no compaction
  wait). RS-5/6, LIV-7 and GAP-6 close by construction; RS-10 stays
  app-level.
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
   blocks reuse of every page freed after it started. The containment must be
   application-owned: the published `signet-libmdbx` 0.8.3 ships no working
   read-timeout (its `timer.rs` is an unwired dead file, no feature flag),
   and reth-libmdbx's `MaxReadTransactionDuration` (~5 min) is the wrong
   scale next to a 10 s query budget while today's legitimate tail (233 s
   streams) exceeds `P-QUERY-TIME` anyway — so the deadline must bound the
   *response lifecycle*, owned by the service (or park/re-establish the
   snapshot), with `mdbx_env_set_hsr` as the space-pressure backstop. The
   backstop *names* the laggard — in-process it cannot safely abort another
   thread's txn, so the kill is the service cancelling that response; hsr
   detects, the deadline enforces. GAP-29 stays open as its own P2.
2. **Memory regime** (corrected 2026-07-26: an earlier revision claimed
   production runs direct I/O — false; the chart passes
   `--rocksdb-disable-direct-io` on every stack, so production reads are
   buffered and the page cache already lives inside the cgroup limit).
   What the port changes is the cache split, not the regime: today an 8 GiB
   *uncompressed* block cache (anon) + page cache of compressed SSTs;
   under mdbx the block cache disappears and the compressed file page cache
   is the only cache, with decode on every read. Consequences to carry:
   the freed 8 GiB anon budget is the natural size bound for the deferred
   decompressed-page LRU; `working_set` ≈ limit is the *normal* steady
   state under mmap (active file pages count), so leak detection moves to
   anon RSS and pressure alerting to memory PSI + major-fault rate; and
   Stage 4 must run under the production values — 12 G request / 64 G
   limit, 2-CPU request — not an unconstrained bench box.
3. **File high-water (measured, wave runs 15–17).** mdbx recycles freed pages
   but returns nothing to the filesystem: across the retention-wave runs tail
   truncation never fired even with an armed `shrink_threshold` and 80–93% of
   pages free — the file is the high-water mark of the working set,
   permanently. Working-set spikes are real (retention is downstream-driven; a
   stalled consumer grows the window at ~2.5 MB/s stored pod-wide ≈ 9 GB/h).
   Measured shape at production pace: file ≈ 1.35× peak stored live plus
   256 MiB growth-step rounding (paced wave run 16); a free-run burst
   additionally parks freed pages up to the sync cadence (≈ device rate ×
   cadence) and peaked at 2.0× (run 15). Reuse is intact — zero file
   growth across 27 k post-wave
   commits at constant live, so the bloat does not compound; untouched pages
   occupy no RAM and tree depth tracks the live set. The rocks reference
   reclaims the same wave for ~3.5× the device writes (parity-corrected;
   measurements "Verdict") — that is the trade being made. Consequences and
   the reclaim plan: see "Disk policy" below.
4. **Hash indexes must not port as global hash-keyed tables — priced, runs
   22–26.** `CF_BLOCK_HASHES`/`CF_TRANSACTION_HASHES` are hash-keyed;
   mainnet-internal runs both today (mainnet runs neither). An LSM absorbs
   random inserts in the memtable; a B+tree COWs a leaf path per touched
   leaf per txn. Measured at internal's shape (300 entries/commit against a
   1 M-entry tree): the index stream costs mdbx **27× its own data stream**
   (+31.2 GB vs a 1.17 GB baseline; rocks pays +4.0 GB for the same) —
   with global tables mdbx writes 4.5× *more* than rocks and the whole
   Context table inverts. Write-behind batching recovers only 26% at
   K=16 and buys 26 ms flush tails. The port therefore restructures:
   per-chunk index tables written append-shaped with the chunk
   (~270× cheaper), lookups fan out over chunk tables (bounded by chunk
   count; per-chunk bloom if the fan-out ever hurts) — or the feature is
   explicitly kept off mdbx-backed deployments. The Stage 4 envelope still
   includes the internal shape (both indexes on, per-chunk layout). Full
   design and prior art: "Hash-index design" below.

## Binding (decided 2026-07-26)

Port on **vorot93 `libmdbx` 0.6.x** (crates.io; `mdbx-sys` exact-pinned at
13.11.0 = C v0.13.11) — the crate the spike measured on. It covers geometry,
SafeNoSync + `sync(force)`, `last_pgno`/freelist; the gaps — `mdbx_env_set_hsr`,
`MDBX_opt_sync_period`/`sync_bytes`, `mdbx_env_copy` — are single FFI calls
shimmed over the pinned sys crate (bindgen already exposes the full
`^(MDBX|mdbx)_.*` surface). Runners-up, for the record: *reth-libmdbx* is
technically strongest (C 0.13.12, wired read-tx timeouts, safe hsr/sync_period)
but is not on crates.io — a git dependency on the reth monorepo, pinned to
tags whose cadence serves reth; its timeout scale is wrong for us anyway (the
read deadline is service-owned, porting note 1). *signet-libmdbx* 0.8.3 is
rejected: its sys crate has never been re-released — production C frozen at
0.13.7, *missing the 0.13.8 SIGBUS-on-full-filesystem fix* that sits directly
on our MAP_FULL path — and its advertised read-timeout support is dead code
(feature undeclared, module unwired). Supply-chain posture: all bindings
vendor the C amalgamation, so a C fix ships only when the binding re-vendors;
vorot93 is single-maintainer with a multi-year ~1–2-month lag record.
Upstream now releases from SourceCraft (RF-hosted; GitHub is a mirror) —
review vendored-C diffs on every bump. Watch item: libmdbx 0.14.x goes
stable 2026-08-08 with native defragmentation and `mdbx_txn_checkpoint` —
re-evaluate at Stage 3, it may hand DP-3 compact-swap a supported primitive.

## Hash-index design (per-chunk; priced 2026-07-26, runs 22–26)

**Why the global table fails under mdbx — the mechanism.** A tx hash is a
uniformly random key; in a COW B+tree the unit of write is the page, and in
a large tree each entry of a batch lands on a *distinct* leaf (300 random
keys vs ~21 k leaves collide rarely). One chunk commit therefore rewrites
~300 leaves for inserts plus ~300 for the pruned chunk's deletes ≈ 600 ×
4 KiB ≈ 2.4 MB of device writes for ~26 KB of payload — measured 27× the
entire data stream (245× on the index churn itself), inverting the engine
decision. The data stream is immune for the same reason in reverse: chunk
keys are append-shaped (fresh `table_id` range), so pages are created
adjacent and written once. An LSM absorbs the same random inserts in its
memtable and sorts them on flush — rocks paid +4.0 GB where mdbx paid
+31.2 GB.

**Design: the index is one more table of the chunk.** Stop maintaining one
sorted-by-hash order over the whole window; keep many small sorted runs,
one per chunk, that are only ever created and dropped — never edited.

- *Layout.* Key `table_id ‖ tag_hash ‖ hash(32 B)` → value 8 B (block
  number) / 12 B (block + tx index). A fresh `table_id` puts the whole
  chunk index into a contiguous fresh key range: ~300 sorted entries ≈
  3–4 new adjacent leaves per commit — ~270× cheaper than the global
  table, and never touched again (chunks are immutable).
- *Write path.* The source stays `for_each_*_hash` at chunk build
  (`tx.rs:311–328`); sort in memory, write through the normal table-build
  path with its 8 MiB sub-commits. The optimistic staging with scans and
  retries goes away.
- *Deletion disappears as a class.* Retention drops the chunk's key
  ranges; the index dies with them (a sequential range-delete). Today's
  delete path re-reads the dying chunk to re-derive keys for the global
  CF (`tx.rs:361–393`) — deleted code.
- *Reorg/REPLACE.* Consistency for free: the index is atomic with its
  chunk, so the global-CF obligation to purge the forked branch's hashes
  (dangling positions otherwise) vanishes structurally.
- *Merge.* `compaction_loop` merge-sorts the sources' sorted runs into
  the merged chunk's index — ~0.3% of chunk bytes, riding the existing
  merge stream.
- *Lookup.* Fan-out: snapshot → live chunks (`CF_CHUNKS` enumerates) →
  point-get `index_table_id ‖ hash` per chunk, newest first, early exit.
  O(1) → O(chunks): ~50–150 chunks ⇒ ~0.1–1 ms hot, ~10–15 ms cold; a
  miss pays the full fan-out. `Ok(None)` semantics and the no-backfill
  property are unchanged (chunks without an index table are skipped). No
  decode on this path — index entries are raw KV in the tree, not codec
  pages (the LZ4 seam covers column pages only); today's rocks hash CFs
  decompress a block per lookup, so the port removes decode here. A cold
  fan-out touches ~0.5–1.8 MB of page cache (spine + one leaf per chunk);
  spines stay resident.
- *Escalation, behind a trigger like the read LRU:* per-chunk bloom
  (~10 bits/entry, stored as one more entry of the chunk's index table,
  lazily RAM-cached) → expected cost = in-RAM filter checks + 1 real get.
  Ship fan-out first; add blooms only if the endpoint p99 or miss
  read-amp misbehaves.
- *Spec.* DEF-17 (indexes expendable) stands; a chunk-scoped note and a
  CT for fan-out + mixed-window miss semantics ride Stage 2.

**Prior art — reth/erigon run the same global table and survive it; their
mitigations do not transfer.** reth's `TransactionHashNumbers` is one
global mdbx table, viable because (a) the bulk volume bypasses random
inserts entirely — the `TransactionLookup` stage accumulates into ETL
collectors (500 MB sorted spills, 5 M-tx batches) and writes mdbx in
sorted order, in append mode when the table is empty; batch ≫ tree width
is where batching actually amortizes (our K=16 recovered 26% — a rolling
tip cannot accumulate for hours); (b) the live tip is ~25 random
inserts/s on L1 with *zero deletes* — an archive index, kept whole by
full nodes for `eth_getTransactionByHash` — vs our thousands/s plus equal
deletes across 45 datasets, permanently; (c) reth moved *data* to static
files but kept the *index* global because their lookup is a hot O(1) RPC
over thousands of segments — fan-out is unaffordable there and affordable
here (rare endpoint, 50–150 chunks). The symmetry: reth pays the
global-table write tax because it cannot afford fan-out reads; we take
fan-out reads because we cannot afford the write tax. Per-chunk index
runs are the static-file idea applied to the index itself — the same
split the sealed-tier escalation below already cites.

## Disk policy (per-dataset env)

Decided on the wave runs and one hard requirement: **one stuck dataset must
never stop the rest**. The product half of this policy already shipped:
PR #77 (NET-896, merged 2026-07-17) lets `Api { max_blocks }` datasets
sacrifice the tail past the portal floor to keep the head fresh — a cap in
block *positions*, opt-in per dataset, soft (whole-chunk trims), with
downward portal instructions clamped to `first(D)` so gap mode does not
resync-loop. What remains unbounded is bytes (blocks vary in size),
uncapped datasets, and the shared volume itself — and none of it is
observable yet. The port closes the class by construction; DP-2/DP-4
generalize NET-896 rather than replace it.

- **DP-1 Quota.** Geometry `max_size` = the dataset's disk budget, enforced
  by the engine. Sizing from the waves: intended peak window ×1.4, rounded up
  to the growth step — the paced-churn shape (run 16). Catch-up and
  reset-reingest bursts add the SafeNoSync parking term on top (≈ per-env
  device rate × sync cadence; the free-run wave peaked at 2.0×, run 15):
  leave that headroom, or accept a gap-mode blip while the burst runs.
  Deployment check: Σ quotas plus an operating reserve MUST fit the volume
  (INV-43) — per-env quotas alone do not bound the shared volume. The
  refusal is dataset-scoped: the dataset(s) whose addition or quota raise
  created the overcommit are refused and alarmed while the admitted set
  boots (N2/GAP-24 containment — a config mistake on dataset 46 must not
  take down 45); whole-pod startup failure is reserved for an overcommit
  with no identifiable marginal dataset.
- **DP-2 Ring past the floor.** The effective floor is the max of every
  governing bound: the instructed floor, NET-896's position cap
  (`next(D) − max_blocks`), and a byte watermark (~90% of quota). The
  watermark needs **two signals**, because "free" and "reusable" diverge
  exactly when it matters: the *retention* signal is occupied pages
  (allocated minus freelist — never the file, which is a permanent
  high-water and would pin gap-mode forever after the first peak); the
  *availability* signal is reusable headroom = growth headroom
  (`max_size − file`) plus the freelist share actually recyclable now —
  excluding pages parked behind the oldest reader txn and pages freed
  since the last durable sync (SafeNoSync parks them until the next sync;
  the stale-reader run held 8.3 GB of file against 33 MB live with nearly
  the whole file "free"). Occupied low + headroom low means parking, and
  trimming more data does not help — the escalation ladder on a
  low-headroom signal is: force a durable sync (unparks SafeNoSync frees)
  → cancel laggard readers (service deadline; hsr names them, porting
  note 1) → trim → pause + alarm (FM-STOR-6). Trimming
  past the downstream floor keeps the head fresh and opens a temporary
  coverage gap between the archive's top and our tail: queries in the hole
  get `RANGE_UNAVAILABLE`, every other dataset is untouched, and the gap
  closes from below as the archive catches up from its own source. Shipped
  semantics are kept — soft whole-chunk trims, downward instructions
  clamped on any position-capped dataset (the byte bound clamps only while
  it governs — no resync loops), but the clamp becomes observable instead
  of silent. `MDBX_MAP_FULL` is the backstop, not the plan: deletes
  themselves need COW pages and can return `MAP_FULL` on an exhausted map,
  so the watermark must trim early enough that the emergency-trim txn still
  fits the remaining freelist (headroom guarantee, `TXN_FULL`-bounded
  batches); if even the trim cannot commit, the dataset degrades to
  pause + alarm (FM-STOR-6), never a crash loop. After a successful
  emergency trim, retry the commit. Precondition (must hold): no archive ingests
  through hotblocks (NG1) — otherwise trimming past the floor would hole
  the archive permanently.
- **DP-3 Defrag, env-granular.** The reclaim unit is the env file, not pages
  — in-file defragmentation is compaction under another name and is exactly
  the write stream this ADR removes. Two mechanisms:
  - *reset-reingest* (required): drop the env dir, re-ingest the window from
    upstream — the boot path narrowed to one dataset. Minutes, pod stays up,
    the sibling replica covers. Sufficient for every reclaim case (slack
    return, quota shrink, decommission).
  - *compact-swap* (optional tooling, later): take the env's write lock (the
    sync-serialization mutex the port already carries), `mdbx_env_copy` with
    `MDBX_CP_COMPACT` (sequential, ~seconds per live-GB, transient ~live of
    extra space), swap files, reopen. Freshness dip of seconds, no upstream
    traffic. Neither `libmdbx` 0.6.6 nor `signet-libmdbx` 0.8.3 wraps
    env-copy — needs a one-call FFI shim over `mdbx-sys` or an upstream PR.
  Trigger on pressure, not cadence: `file − live > max(abs, k × live)`
  sustained *and* node headroom low → worst offender first, one env at a
  time, never both replicas of a dataset at once. `max_size` can only be
  lowered onto a file that already fits — quota shrink implies defrag first.
- **DP-4 Observability & acceptance.** Per-dataset gauges for file/live/quota
  bytes, gap-mode flag + gap width, slack alert. Port acceptance: resetting a
  single env runs online with other datasets' commit tails unaffected
  (CT-covered in the harness), and the portal serves `RANGE_UNAVAILABLE`
  across the hole instead of failing (prior art: the portal crash-looped on
  an unexpected hotblocks response).

Escalation if env-granular defrag becomes routine: sealed immutable chunk
files from the terminal `compaction_loop` merge, retention by `unlink` —
reth's mdbx + static-files split; its own ADR.

## Decision

*(Written 2026-07-25; the spike it commissioned is done. Kept as the record —
with one premise overturned by the runs: 64 KiB pages fail the write gate,
4 KiB pass. The port plan carries the corrected geometry.)*

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

The write gate passed (Measurements). Reads did not get a spike gate:
app-LZ4 scans measured 18× p50 over rocks on page-cache-hot runs (the C
decoder cuts it ~6×; engine paths are comparable and tails tighter). The
accept-time exposure is bounded by production numbers instead: at the
measured query mix the decode ceiling is ~0.15 core/pod (Measurements,
scan bracket) — decode CPU cannot invert the ADR 0001 win even with no
decompressed-page cache. What production cannot bound today is cold-cache
behavior at full live size, so Stage 4 carries the read gate as a
*rollout* gate: cold-cache S1/S2 query mix at production live size under
the production memory values (porting note 2) — SLI-2/3 within agreed
bounds of the rocks baseline, major faults and PSI captured. Abort
criterion: if SLI-2/3 breach and a decompressed-page LRU sized within the
freed ~8 GiB block-cache budget does not restore them, the port stops
before Stage 5 — rocks stays, the seam and codec remain as the
measurement record.

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
- **Production read-scan bracket** (2026-07-26, VictoriaMetrics, 30 m rates).
  Mainnet: ~6.3 k queried blocks/s per pod over a ~2.8 M-block window holding
  ~68 GB live SST → ~98 KB raw per block → **≤ ~620 MB/s raw scanned per pod**
  as the upper bound (assumes every queried block reads all its tables; the
  response-byte lower bound is ~4.2 MB/s). At in-service C-`lz4` decode
  (~0.25 ms/MB, decoder shoot-out) the ceiling is **~0.15 core/pod**;
  morpho ≈ 0.06, internal ≈ 0.02. The deferred-LRU trigger (~2 cores) is
  unreachable at today's mix by an order of magnitude. The bracket collapses
  to a real number once the raw-scanned-bytes counter ships (Stage 2
  observability) — the same counter is the post-port LRU-trigger SLI.

## Spec compliance (checked 2026-07-26)

The disk policy was audited against `crates/hotblocks/spec/`. The *doctrine*
already points this way: RS-2 lets retention trim finalized data because this
is "a bounded hot store, not an archive" (NG1), and DP-2 extends the same
dominance one level up — space bounds retention. The read path needs no
changes at all: a query below the new `first` is already specified
(`RANGE_UNAVAILABLE`, RP-4) with the client rule "re-anchor upward", and the
ring trim is mechanically an ordinary `RETAIN` (INV-15/18 and WP §2.5 apply
unchanged). The isolation doctrine (INV-35/36, LIV-8, FM-3) is what DP-1
finally makes enforceable — today LIV-8 is marked known-violated (GAP-1).

The *letter* conflicts in seven places; amending them is Stage 0 below and
the gate for flipping this ADR to accepted. Half of DP-2 is not a proposal
at all: PR #77 (NET-896) shipped the position cap on 2026-07-17 — five days
*after* the spec audit — so part of Stage 0 is reconciling the spec with
behavior already in production:

1. **RS-1/RS-3.** `External` guarantees "everything ≥ instructed bound
   kept" — factually stale since PR #77; `Window` floors "err on the side
   of keeping more, never less". DP-2 trims past both under pressure. Fix:
   new **RS-13 (space dominates retention)** — the effective bound is the
   max of the instructed bound, the position cap (`max_blocks`, shipped),
   and the byte bound (quota watermark, the port); while any space bound
   governs, the dataset is in an alarmed, observable gap-mode.
2. **INV-44.** Data may leave only via "RETAIN *per policy*". With RS-13 the
   space bound becomes part of policy semantics, so INV-44 needs a pointer,
   not a new destructive path.
3. **RS-6 + SLI-8.** `disk ≤ P-SPACE-AMP × live + const` (target ≤ 2.0×) is
   violated by the measured mdbx high-water (5.6–16.6× vs *current* live
   after a wave). Restate per dataset: hard bound `env ≤ P-DISK-QUOTA`
   always; the amplification bound holds against *peak* live since the last
   env reset (ratchet), restored tight by DP-3.
4. **LIV-7.** "Disk returns to within RS-6": deletion *debt* converges
   faster than today (pages free at the next durable point — no sweep, no
   compaction wait), but the *file* converges only through DP-3; restate
   against the RS-6 ratchet.
5. **FM-STOR-2.** The documented degrade is "writes MAY pause". Replace with
   ring-mode: writes continue, floor overridden, gap alarmed;
   `MDBX_MAP_FULL` backstop = emergency trim + retry. The recovery-path
   clause ("no scratch space proportional to reclaimable data") is satisfied
   by reset-reingest and NOT by compact-swap — the latter stays routine
   tooling, never the FM-STOR-2/3 recovery path.
6. **Parameters.** New `P-DISK-QUOTA(D)`, `P-DISK-WATERMARK` (soft
   fraction), `P-REORG-KEEP` (minimum span the ring may never trim into —
   the INV-14 interaction: under pressure the window must still absorb a
   realistic reorg, else RESET+alarm), plus registering the shipped
   `max_blocks` (positions, DEF-9). `P-DISK-FLOOR` stays as the node-level
   threshold. A quota below the minimum viable span is an INV-43 boot
   refusal, not a runtime surprise.
7. **WP §2.5 / WP-11 (downward instructions).** The audit's N3 resolution
   made a downward SET-RETENTION an explicit alarmed RESET. PR #77's
   `clamp_floor` deliberately breaks that for capped datasets: a portal
   floor below `first(D)` is clamped up, silently — necessarily, because a
   dataset in gap mode would otherwise resync-loop against a stuck archive
   (the portal keeps re-asking for the pre-gap floor). RS-13 carves the
   exception to match the ship: on a position-capped dataset downward
   instructions always clamp (`clamp_floor` keys on the cap's presence, not
   its governance), while the byte bound clamps only while it governs — so
   the WP §2.5 RESET stays reachable once every dataset carries a quota —
   and the clamp is observable (today it is silent).

Plus observability and conformance rows: OB-6 gains env file/live/quota
gauges and the gap-mode flag + gap width; CT-7 gains the wave scenario (the
harness analog of spike run 16); CT-8 gains stuck-consumer (one dataset's
floor frozen forever — assert the quota ceiling, the gap alarm, untouched
neighbors); a CT covers online env reset (LIV-5b scoped to one dataset).

Four pre-existing audit findings become port prerequisites because DP leans
on them: **N5/GAP-2** (anchor hash dropped on Window trims — INV-18; PR #77's
`trim_floor` passes `None` for the hash too, so the new path has the same
one-line bug to fix), **N6** (External retention bound not durable — persists
in the label the port rewrites anyway), **N2/GAP-24** (one dataset's boot
failure kills the whole service — per-env containment is the natural fix and
DP-4 acceptance depends on it), and **GAP-7** (readiness is all-or-nothing:
`/ready` gates on full init and models no per-dataset readability — RS-14's
online reset is inoperable without per-dataset readiness plus snapshot-handle
draining and dataset-aware portal retry).

## Port plan

- **Stage 0 — spec amendment (docs, ~2 d).** The seven items above (the
  parameter registry is item 6) + OB/CT rows; cross-references checked by
  hand — no spec CI gate exists yet. The accepted flip additionally waits
  on the Stage 1 vertical slice (Status).
- **Stage 1 — engine seam (~1 wk).** `kv_mdbx` from the spike into
  `crates/storage` as a real backend: env-per-dataset, 4 KiB pages,
  SafeNoSync with per-env serialized durable sync (the abort constraint),
  geometry from config (`P-DISK-QUOTA` as `max_size`, tuned growth step,
  armed shrink threshold), LZ4 at `BufferPageWriter::write_page` behind a
  versioned page codec — header with raw length and checksum, because mdbx
  has no data-page checksums (meta `validator_id` is reserved-zero) while
  RocksDB verifies blocks on every read: FM-STOR-4 regresses without it —
  decoded by C `lz4` into pooled buffers (measured 5.8× over `lz4_flex`
  safe-decode on the decision-runs Xeon, spike page shape; flex without
  safe-decode is only 2.0× — not worth the unsafe path); read deadline
  owned by the service, bounding the response lifecycle (porting note 1),
  with `mdbx_env_set_hsr` via the Binding FFI shim as the space-pressure
  backstop. **Bounded write txns as a stated invariant**: table builds keep
  the 8 MiB sub-commit granularity, the writer lock is released between
  sub-commits, and head flushes interleave — a merge holds the writer one
  sub-commit at a time, never for the whole rebuild. Differential tests
  against the rocks backend, corruption-injection tests on the codec, and
  a CT-2 kill matrix at the seam. kill-9 verifies integrity and recovery
  only — under SafeNoSync dirty pages survive process death in the page
  cache, so the loss window it observes is ~zero; the real loss window is
  a *system*-crash property (CN-6b) resting on mdbx's steady-sync-point
  guarantee under `NoWriteMap` (dm-log-writes power-cut injection stays
  optional hardening). Pin `P-DUR-SYSTEM`, today "make explicit ⚠", to
  **sync cadence + max write-txn hold** — durable sync serializes behind
  the writer lock, so the hold bound is part of the durability claim, and
  the cadence itself joins the parameter registry. Exit: the vertical
  slice the accepted flip waits on (Status).
- **Stage 2 — db layer (~2–3 wk).** The ~1300 lines of
  `crates/storage/src/db/*`: transactions, snapshots, label/meta (persist
  the retention bound — N6), chunk index, metrics, CLI, shutdown. Delete the
  engine-reclaim machinery (10 s sweep, deletion collector, periodic
  compaction, startup `DeleteFilesInRange`, `TableId` watermark,
  `reclaim-measure`); keep the dirty-table journal and the startup orphan
  purge — multi-commit table builds still leave committed orphans at a
  crash (RS-10). Fix N5 anchor carry-over in the trim path it touches.
  Hash-index layout is decided by measurement (porting note 4, runs
  22–26): per-chunk index tables written with the chunk — global
  hash-keyed tables cost 27× the data stream and are rejected; the
  `hashes/{hash}` endpoints re-implement as fan-out over chunk index
  tables. Ship the raw-scanned-bytes counter (the Measurements bracket's
  replacement and the LRU-trigger SLI).
- **Stage 3 — disk policy (~1 wk).** DP-1 quota config; DP-2 = extend
  NET-896's trim with the byte watermark (effective floor = max of
  instructed, position cap, byte bound) + gap-mode flag/alarm + observable
  clamp + `MAP_FULL` backstop + `P-REORG-KEEP` guard + INV-43 boot check;
  DP-3 reset-reingest as a dataset-scoped observable RESET; DP-4 gauges and
  alerts.
- **Stage 4 — conformance & perf (~1 wk, overlaps 3).** CT-7 wave, CT-8
  stuck-consumer and online-reset CT in `crates/hotblocks-harness`; S4
  churn-soak on a prod-class node against the rocks baseline (the Decision
  step 2 envelope: device writes, head-flush tails under ingest × merge ×
  retention, wave high-water) — merges run *concurrently* with ingest (the
  bench's `--concurrent-merges` shape; inline merges measure no writer-lock
  queueing) and the matrix includes the internal shape with both hash
  indexes on (porting note 4). Run the Decision read gate: cold-cache S1/S2
  at production live size under the production memory values (porting
  note 2), SLI-2/3 against the rocks baseline with major-fault and PSI
  capture (the spike's read runs were page-cache-hot by construction).
  Confirm the decode-CPU share against the Measurements bracket via the
  Stage 2 raw-scan counter; the decompressed-page LRU (coherence is
  trivial — chunk tables are immutable) stays deferred behind its trigger:
  decode CPU > ~2 cores/pod or read p99 against SLO. Portal contract
  check: a gap range must pass through as `RANGE_UNAVAILABLE`, not break
  the portal (cross-repo).
- **Stage 5 — rollout (~1 wk).** Blue/green on an empty volume:
  internal → morpho → mainnet; watch device writes (expect ≥3× drop), commit
  tails, env file/live/quota, gap-mode. mmap makes `working_set` read as
  page cache — dashboards must not mistake it for a leak. Rollback =
  previous image on the old volume (the store is re-ingestable).

Sums to ~5–7 weeks against the 3–6 week code estimate above. Deliberately
out of scope, recorded: the compact-swap FFI shim (DP-3 optional tooling),
the sealed-tier ADR, and portal-side gap handling if the Stage 4 contract
check fails.

## Alternatives considered

- **Tune RocksDB harder** (universal compaction, larger memtables, relaxed
  leveling): reduces but keeps the multiplier, keeps stalls and the reclaim
  machinery; the workload shape (rolling window, append keys, big values) is the
  textbook non-LSM case.
- **Vanilla LMDB**: fixed map size must be pre-declared — no growth/shrink
  geometry to hang `P-DISK-QUOTA` on — and there is no HSR hook for the
  stale-reader backstop. (The original draft also held its 4 KiB pages
  against it; the spike made that moot — 4 KiB won the write gate and the
  3-contiguous-page overflow runs allocate fine under churn.)
- **Reduce the application chunk-merge churn instead**: engine-independent and
  worth doing regardless, but it cannot remove LSM leveling amplification on the
  ingest stream itself.
- **An engine that reclaims disk online** (staying on RocksDB, fjall, SQLite
  `auto_vacuum`, redb `compact()`): online reclaim is compaction under another
  name — every candidate pays the rewrite multiplier this ADR removes (the wave
  A/B prices it: rocks returns the file to ~live for ~3.5× the device writes).
  With deletes already partitioned by age, unlink-based reclaim (DP-3, and the
  sealed-tier escalation) is the structural answer if high-water hoarding ever
  bites.
