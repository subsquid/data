# 09 — Retention and space

Retention (which blocks the dataset logically keeps) and space (what the store physically
occupies) are deliberately decoupled. This document specifies both and the contract
between them.

## 1. Retention policies

- **RS-1 (Policy semantics).** As defined in DEF-9 and WP §2.5:

| Policy | Guarantee | Trim trigger |
|---|---|---|
| `Window(k)` | availability floor RS-3 + excess bound RS-4 (space exception: RS-13) | automatic, after commits that advance `next(D)` |
| `Pinned(from, h?)` | everything ≥ `from` kept; anchor asserted at boot (WP-9 refusal on mismatch); never trimmed by space — pauses instead (RS-13) | only when `from` is raised by reconfiguration |
| `External` | everything ≥ last instructed bound kept, within the space bounds of RS-13; unbounded until first instruction; a *downward* instruction is a destructive re-bootstrap (RESET, WP §2.5) — clamped instead of executed while a space bound governs or a position cap is configured (RS-13) | SET-RETENTION (WP-11); position cap / space watermark (RS-13) |
| `Unbounded` | nothing trimmed; never trimmed by space — pauses instead (RS-13) | never |

- **RS-2 (Retention dominates finality).** Trimming ignores `fin`: finalized blocks below
  the retention bound are deleted, and `fin` becomes `⊥` when the window passes above it
  (03 §2.5). Rationale: this is a bounded hot store, not an archive (NG1). Consequence
  for clients: "finalized" means *irreversible while retained*, not *retained forever*
  (INV-24 note).
- **RS-13 (Space dominates retention).** RS-2's bounded-store doctrine, one
  level up: a dataset's space budget outranks its retention promise — for the
  policies that can afford it. Policies split into two classes:
  - *Self-healing* (`Window`, `External`): trimmed data is re-acquirable — it
    exists (or will) in the archival tier, and clients below the cut re-anchor
    upward (RP-4). For these the **effective trim bound** is the maximum of
    the policy bound (RS-3/WP-10, or the WP-11 instructed bound), the position
    cap `next(D) − P-MAX-BLOCKS` (`External` only, where configured), and the
    byte bound derived from the dataset's quota watermark
    (`P-DISK-WATERMARK × P-DISK-QUOTA`, compared against *occupied* storage —
    allocated minus reusable pages — never against the file, which under a
    high-water engine never shrinks (RS-6b) and would pin gap-mode forever
    after one peak). A trim to the effective bound is an
    ordinary RETAIN — INV-15/18 hold, the anchor hash is carried. While a
    space bound governs (effective > policy bound) the dataset is in
    **gap-mode**: onset, exit, and the gap width MUST be observable
    (OB-6/OB-9). In gap-mode a retention instruction below `first(D)` is
    clamped to `first(D)` instead of executing the WP §2.5 downward RESET —
    honoring it would re-bootstrap in a loop against the very consumer lag
    that opened the gap; the clamp MUST be observable. Where a position cap
    is *configured*, the clamp applies even outside gap-mode: the cap is a
    standing election of freshness over depth, and the instructed floor
    tracks downstream coverage, so an instruction below `first(D)` signals
    the same consumer lag whether or not the cap is the bound currently
    governing (shipped semantics — PR #77's `clamp_floor` keys on the cap's
    presence, not its governance). The byte bound clamps only while it
    governs: under a quota-per-dataset regime every dataset carries
    `P-DISK-QUOTA`, and quota-configured must not mean the WP §2.5 downward
    RESET is unreachable. Instructions at or above `first(D)` apply
    normally.
  - *Promises* (`Pinned`, `Unbounded`): space never triggers a trim. At the
    watermark the dataset alarms (OB-9); at the quota its writes pause
    (FM-STOR-6) — reads keep serving, other datasets are unaffected
    (INV-35/36), and recovery is an operator action (raise the quota, raise
    the policy bound, or DROP). This is WP-9/INV-43's doctrine at runtime:
    pinned history is never destroyed silently.

  Ring floor, both classes: the effective bound never rises into the last
  `P-REORG-KEEP` positions below `next(D)` — trimming closer would strip the
  reorg-absorption depth INV-14 relies on. A self-healing dataset whose quota
  cannot hold even that span degrades like a promise (pause + alarm) rather
  than trim further; a *configured* quota that cannot hold
  `P-REORG-KEEP + P-RETENTION-SLACK` worth of data is a boot-time refusal
  (INV-43), not a runtime surprise.
- **RS-3 (Availability floor).** For `Window(k)`: at every committed state,
  all blocks in `[next(D) − k, next(D) − 1] ∩ [window start after initial fill, ∞)` are
  present and queryable (an interval of *positions* — DEF-9; on a slot-numbered chain it
  holds ≤ `k` blocks). Trimming MUST err on the side of keeping more, never less. The
  single sanctioned exception is RS-13's space bound — alarmed and observable, never
  silent.
- **RS-4 (Excess bound).** For `Window(k)`: eventually (once steady-state is reached and
  within `P-RETENTION-APPLY` of each trigger), `first(D) ≥ next(D) − k − P-RETENTION-SLACK`.
  Slack exists because trimming may be batch-granular; it is bounded, not best-effort.
- **RS-9 (Dataset removal).** DROP removes all the dataset's data logically at once;
  physical reclamation follows RS-5/6. Re-creating the same identity yields a fresh
  dataset (WP §2.7).

## 2. Space model

Definitions for accounting (all per store unless noted):

- `live_bytes` — bytes attributable to blocks currently in some dataset's segment
  (+ bounded metadata).
- `debt_bytes` — bytes attributable to logically deleted or superseded data not yet
  physically reclaimed, plus invisible residue from interrupted writes.
- `disk_bytes` — actual storage footprint.

Requirements:

- **RS-5 (Two-phase deletion).** Logical deletion (RETAIN/REPLACE/RESET/DROP commits) is
  immediate and cheap; physical reclamation is asynchronous. Between the two, deleted
  data is `debt_bytes` — invisible to all reads (INV-41 keeps live readers safe via
  versioning, not via keeping data visible).
- **RS-6 (Amplification bound).** Two bounds, per dataset where the storage layout
  permits attribution:
  (a) *hard quota* — where `P-DISK-QUOTA` is configured, the dataset's `disk_bytes`
  MUST NOT exceed it, ever; this is the bound RS-13 and FM-STOR-6 enforce against.
  Σ of configured quotas plus an operating reserve MUST fit the volume — a boot-time
  check (INV-43): per-dataset quotas alone do not bound the shared volume;
  (b) *amplification ratchet* — in steady state,
  `disk_bytes ≤ P-SPACE-AMP × peak_live_bytes + P-SPACE-CONST`, where
  `peak_live_bytes` is the maximum `live_bytes` since the dataset's storage was last
  reset (RS-14). For an engine that reclaims in place, peak and current live coincide
  and this is the familiar `P-SPACE-AMP × live_bytes`; for a copy-on-write engine whose
  file is a high-water mark, current live may sit far below the file — that slack is
  bounded by the peak term and returned by RS-14, not by background reclaim.
  Both MUST hold under continuous churn (window datasets trim continuously — churn IS
  the steady state), and unbounded `debt_bytes` growth under any configuration is a
  defect in either reading. (GAP-6 history:
  until 2026-07 the system reclaimed physically only in an optional boot mode and default
  configurations violated this clause; routine reclaim now runs in default
  configurations — deferred point deletes swept every `P-CLEANUP-PERIOD` plus engine
  compaction. The bound itself is still unmeasured under churn: CT-7.)
- **RS-7 (Reclamation safety).** = INV-41. Reclamation never affects logical state or
  live readers. Corollary: reclamation strategies that cannot honor reader-safety MUST be
  confined to RS-8.
- **RS-8 (Boot maintenance mode).** A reader-free maintenance window at startup, before
  serving begins, in which the service MAY run reader-unsafe physical operations
  (bulk file reclamation, residue purge). Requirements: strictly before any reader can
  exist; bounded contribution to LIV-5 budgets; idempotent (INV-42); effective on a
  nearly-full disk (its purpose — FM-STOR-2/3 recovery) without scratch space
  proportional to the data being reclaimed.
- **RS-10 (Residue convergence).** Invisible residue (from crashes: torn builds; from
  operation: superseded internal structures) MUST be collected: residue does not
  accumulate without bound across crashes (INV-42) and — critically — residue from one
  dataset MUST NOT indefinitely block reclamation for other datasets. (Status: the
  global low-watermark coupling now applies only to the boot-phase file unlink (RS-8);
  routine compaction-based reclaim is not pinned by it. Interrupted-build residue still
  leaks in default configurations because the purge is confined to the same gated boot
  mode — GAP-6.) Residue age is observable (OB-6).
- **RS-11 (Deletion cost bounds).** Logical deletion cost is O(deleted-range metadata),
  not O(bytes); physical reclamation runs at bounded amortized cost without violating
  LIV-2 (deletion-induced maintenance debt counts inside the stall budget). Deleting a
  large dataset MUST have bounded peak memory (not proportional to the dataset's size).
- **RS-12 (Derived index space).** Index bytes (DEF-17) count toward `live_bytes` and fall
  under RS-6 like any other bytes: enabling an index raises the denominator, so the
  amplification bound neither loosens nor tightens. Entries are removed in the same commit
  as their blocks (INV-46), so their space becomes ordinary `debt_bytes` and converges per
  RS-5/LIV-7 — **an index is never a leak path**, in either flag direction: enabling one
  does not backfill existing blocks, disabling one does not eagerly erase entries, and both
  states converge within one retention period as the window turns over.

  Sizing is where the two indexes part company, and operators MUST budget them separately.
  `bidx` costs one entry per *block*. `tidx` costs one entry per *transaction* — on a busy
  EVM chain roughly two orders of magnitude more, so its footprint tracks the transaction
  rate × retention, not the block rate. A retention window that makes `bidx` a rounding
  error can make `tidx` the largest single consumer in the store. This asymmetry is why the
  two are independently enabled (`P-BLOCK-INDEX`, `P-TX-INDEX`) rather than sharing a
  switch.

  A controlled Snappy/LZ4 sizing run on 2026-07-15 used identical random-looking 32-byte
  hashes, production key/value encodings and Bloom filters. Each sample was flushed and fully
  compacted; the figures are live SST bytes for the named index only, excluding WAL,
  memtables and table data.

  | Entries/index | Block, Snappy | Block, LZ4 | Transaction, Snappy | Transaction, LZ4 |
  | ---: | ---: | ---: | ---: | ---: |
  | 100,000 | 8,487,039 (84.87 B/e) | 8,486,380 (84.86 B/e) | 8,903,864 (89.04 B/e) | 8,904,184 (89.04 B/e) |
  | 1,000,000 | 72,238,847 (72.24 B/e) | 71,885,934 (71.89 B/e) | 75,310,111 (75.31 B/e) | 75,037,601 (75.04 B/e) |

  At one million entries LZ4 saves only 0.49% for `bidx` and 0.36% for `tidx`. That is not
  enough to justify recompressing the existing block index, so `bidx` stays on Snappy while
  the new `tidx` uses LZ4. Use **70–90 B per retained entry** as the initial planning range:
  approximately 67 GiB per billion blocks and 70 GiB per billion transactions after
  compaction. Reproduce with the ignored `measure_hash_index_compression_disk_size` release
  test, then measure the deployment's real hash/key distribution and compaction state.

- **RS-14 (Dataset storage reset).** The store MUST support resetting one dataset's
  physical storage online: drop the dataset's physical artifacts and re-acquire its
  window per policy — a dataset-scoped RESET (a sanctioned INV-44 path, observable per
  OB-9). Bounds: unavailability is confined to that dataset (LIV-5b-scoped readiness;
  other datasets' progress and tails unaffected, LIV-8), and the operation MUST be
  effective at a full quota without scratch space proportional to the dataset's data
  (the drop precedes the re-acquisition — the same clause as RS-8). This is the
  sanctioned return path for RS-6's high-water slack and the recovery verb of
  FM-STOR-6.

## 3. Interactions

- **Retention × finality:** RS-2 (dominates). FINALIZE below `first(D)` is ignored (WP
  §2.4).
- **Retention × space:** RS-13 (space dominates — self-healing policies only; promise
  policies convert space pressure into a write-pause, FM-STOR-6, never a trim). The
  high-water return path is RS-14.
- **Retention × forks:** the fork floor is the window start (INV-14): retention determines
  how deep a reorg can be absorbed in place. Operators choosing `k` MUST size it above the
  chain's realistic reorg depth; deeper reorgs are RESET events (alarmed).
- **Retention × queries:** trimming during a running query does not affect it (INV-20/41);
  the *next* query below the new `first` gets `RANGE_UNAVAILABLE` (RP-4).
- **Retention × recovery:** recovered state reflects committed trims exactly (INV-40);
  a trim's anchor carry-over (INV-18) survives restarts.
- **Retention × hash indexes:** retention is what bounds an index (RS-12) *and* what makes
  it forget — a hash resolvable today stops resolving once its block leaves the window, and
  a client cannot distinguish that from a hash that was never indexed (RP-19). Retention is
  also the only mechanism that repairs an index: history missed while the flag was off
  drains out on its own.
- **Space × liveness:** reclamation lag is bounded (LIV-7); maintenance debt feeds back
  into the write path only within the stall budget (LIV-2, HZ-2/HZ-5).
