# 09 — Retention and space

Retention (which blocks the dataset logically keeps) and space (what the store physically
occupies) are deliberately decoupled. This document specifies both and the contract
between them.

## 1. Retention policies

- **RS-1 (Policy semantics).** As defined in DEF-9 and WP §2.5:

| Policy | Guarantee | Trim trigger |
|---|---|---|
| `Window(k)` | availability floor RS-3 + excess bound RS-4 | automatic, after commits that advance `next(D)` |
| `Pinned(from, h?)` | everything ≥ `from` kept; anchor asserted at boot (WP-9 refusal on mismatch) | only when `from` is raised by reconfiguration |
| `External` | everything ≥ last instructed bound kept; unbounded until first instruction; a *downward* instruction is a destructive re-bootstrap (RESET, WP §2.5) | SET-RETENTION (WP-11) |
| `Unbounded` | nothing trimmed | never |

- **RS-2 (Retention dominates finality).** Trimming ignores `fin`: finalized blocks below
  the retention bound are deleted, and `fin` becomes `⊥` when the window passes above it
  (03 §2.5). Rationale: this is a bounded hot store, not an archive (NG1). Consequence
  for clients: "finalized" means *irreversible while retained*, not *retained forever*
  (INV-24 note).
- **RS-3 (Availability floor).** For `Window(k)`: at every committed state,
  all blocks in `[next(D) − k, next(D) − 1] ∩ [window start after initial fill, ∞)` are
  present and queryable (an interval of *positions* — DEF-9; on a slot-numbered chain it
  holds ≤ `k` blocks). Trimming MUST err on the side of keeping more, never less.
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
- **RS-6 (Amplification bound).** In steady state,
  `disk_bytes ≤ P-SPACE-AMP × live_bytes + P-SPACE-CONST`. This MUST hold under
  continuous churn (window datasets trim continuously — churn IS the steady state).
  Unbounded `debt_bytes` growth under any configuration is a defect. (GAP-6 history:
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

## 3. Interactions

- **Retention × finality:** RS-2 (dominates). FINALIZE below `first(D)` is ignored (WP
  §2.4).
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
