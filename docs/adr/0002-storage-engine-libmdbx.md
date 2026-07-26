# ADR 0002 — Replace RocksDB with libmdbx in hotblocks storage

Status: proposed — spike done 2026-07-26, write gate passed
([measurements](../measurements/2026-07-26-mdbx-churn-bench.md)); flips to
accepted after Stage 0 (spec amendment) plus a Stage 1 vertical slice — real
table build → chunk merge → retention → crash recovery through the mdbx seam,
with the checksummed codec and MAP_FULL / stale-reader / kill-9 / cold-read /
online-reset tests · Date: 2026-07-25 · Scope:
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
   snapshot), with `mdbx_env_set_hsr` as the space-pressure backstop that
   names the laggard reader and can kill its txn before the file grows.
   GAP-29 stays open as its own P2.
2. **Direct I/O** is on in production (`--rocksdb-disable-direct-io` unset) and is
   incompatible with an mmap engine; page-cache behavior must be re-validated.
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
   reclaims the same wave for 3.7× the device writes — that is the trade
   being made. Consequences and the reclaim plan: see "Disk policy" below.

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
  (boot refusal otherwise, INV-43) — per-env quotas alone do not bound the
  shared volume.
- **DP-2 Ring past the floor.** The effective floor is the max of every
  governing bound: the instructed floor, NET-896's position cap
  (`next(D) − max_blocks`), and a byte watermark (~90% of quota, measured
  against occupied pages — allocated minus freelist — never against the
  file, which is a permanent high-water and would pin gap-mode forever
  after the first peak). Trimming
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

The write gate passed (Measurements). Reads did not get a gate and must:
app-LZ4 scans measured 18× p50 over rocks on page-cache-hot runs (the C
decoder cuts it ~6×; engine paths are comparable and tails tighter), while
production serves ~62 GB live under direct I/O. Stage 4 therefore carries a
read gate: cold-cache S1/S2 query mix at production live size under the
cgroup memory limit — SLI-2/3 within agreed bounds of the rocks baseline,
major faults and PSI captured.

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
  owned by the service, bounding the response lifecycle (porting note 1 —
  no binding feature to lean on), with `mdbx_env_set_hsr` as the
  space-pressure backstop. Differential tests against the rocks backend,
  corruption-injection tests on the codec, and a CT-2 kill matrix at the
  seam (SafeNoSync loss window ≤ the sync cadence — pin `P-DUR-SYSTEM`,
  today "make explicit ⚠", to it). Exit: the vertical slice the accepted
  flip waits on (Status).
- **Stage 2 — db layer (~2–3 wk).** The ~1300 lines of
  `crates/storage/src/db/*`: transactions, snapshots, label/meta (persist
  the retention bound — N6), chunk index, metrics, CLI, shutdown. Delete the
  engine-reclaim machinery (10 s sweep, deletion collector, periodic
  compaction, startup `DeleteFilesInRange`, `TableId` watermark,
  `reclaim-measure`); keep the dirty-table journal and the startup orphan
  purge — multi-commit table builds still leave committed orphans at a
  crash (RS-10). Fix N5 anchor carry-over in the trim path it touches.
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
  retention, wave high-water). Run the Decision read gate: cold-cache S1/S2
  at production live size under the cgroup memory limit, SLI-2/3 against
  the rocks baseline with major-fault and PSI capture (the spike's read
  runs were page-cache-hot by construction). Measure per-pod raw-scan
  bytes/s and the decode-CPU share at the production query mix; a
  decompressed-page LRU
  (coherence is trivial — chunk tables are immutable) stays deferred behind
  a trigger: decode CPU > ~2 cores/pod or read p99 against SLO. Portal
  contract check: a gap range must pass
  through as `RANGE_UNAVAILABLE`, not break the portal (cross-repo).
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
- **Vanilla LMDB**: fixed map size must be pre-declared and nothing in the system
  bounds disk today (`P-DISK-FLOOR` undefined); 4 KiB pages force overflow chains
  for every table page. libmdbx auto-grows and supports 64 KiB pages.
- **Reduce the application chunk-merge churn instead**: engine-independent and
  worth doing regardless, but it cannot remove LSM leveling amplification on the
  ingest stream itself.
- **An engine that reclaims disk online** (staying on RocksDB, fjall, SQLite
  `auto_vacuum`, redb `compact()`): online reclaim is compaction under another
  name — every candidate pays the rewrite multiplier this ADR removes (the wave
  A/B prices it: rocks returns the file to ~live for 3.7× the device writes).
  With deletes already partitioned by age, unlink-based reclaim (DP-3, and the
  sealed-tier escalation) is the structural answer if high-water hoarding ever
  bites.
