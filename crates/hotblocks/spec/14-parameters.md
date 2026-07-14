# 14 — Parameter registry

Normative documents reference parameters symbolically; this registry records their role
and current values. **"Observed"** = the value the current system generation exhibits
(informative, may drift); **"Target"** = a value this spec requires or proposes where no
setting exists yet (⚠ = to be ratified). A conformance run records the parameter set it
ran against.

## Read path

| Parameter | Role (where used) | Observed | Target |
|---|---|---|---|
| `P-QUERY-TIME` | wall-time budget per query response (RP-17, LIV-3, CN-7) | 10 s | keep |
| `P-HEAD-WAIT` | bounded long-poll wait above the head (RP-5, LIV-4) | 5 s | keep |
| `P-RESP-WEIGHT` | per-response emission weight budget (RP-17, INV-25) | 20 MB (weighted) | keep |
| `P-RESP-FLUSH` | streaming buffer flush threshold (RP-17) | 512 KB | keep |
| `P-CONFLICT-WINDOW` | max hints in a CONFLICT payload; also the anchored-check lookback in *positions* (RP-11) | ~100 (varies by detection site: 1…~101); a number gap deeper than the lookback yields INTERNAL (GAP-21) | ≥ 100 uniformly ⚠ |
| `P-MAX-ITEM-REQ` | max item requests per query (RP-1) | 100 | keep |
| `P-BODY-LIMIT` | max request body size (RP-17, IB-2) | ~2 MB (platform default) | make explicit ⚠ |
| `P-EXEC-SLOTS` | global concurrent query work units (RP-3, PF-3) | executor threads × 200 | keep; revisit per-dataset fairness (GAP-14) |
| `P-WAITERS` | global cap on head-waiting queries (RP-5) | 64 000 | keep; same fairness note |
| `P-SCHED-SLACK` | scheduling tolerance added to termination bounds (LIV-3/4) | — | 1 s ⚠ |
| `P-HASH-MAXLEN` | max accepted hash length on a lookup, rejected before store access (RP-20) | 256 chars | keep |

## Write path

| Parameter | Role | Observed | Target |
|---|---|---|---|
| `P-BATCH-ROWS` | batch flush bound, rows (WP-3, HZ-6) | 200 000 rows | keep |
| `P-BATCH-BYTES` | batch flush bound, bytes (WP-3, PF-1) | ~30 MB (soft) | hard ceiling ⚠ (GAP-13) |
| `P-MAX-BLOCK-BYTES` | ceiling on one block's encoded size, enforced at ingest (WP-2 rejection + source fault); what makes the read-side "+ one block" allowance (RP-17, INV-25) and PF-1's ceiling finite | absent — a single oversized block stores (the batch bound is soft) and must later be emitted whole (GAP-37) | define ⚠ |
| `P-FORK-CONSENSUS` | arbitration timeout before accepting a fork signal (WP-4) | 2 s | keep |
| `P-SOURCE-BACKOFF` | per-source retry backoff schedule (WP-17, FM-SRC-1) | 0→10 s exponential steps | keep |
| `P-EPOCH-RETRY` | pause before restarting a failed ingestion epoch (WP-17) | 60 s | keep; add alarm coupling (GAP-5) |
| `P-SOURCE-STRIKES` | consecutive rejected runs before quarantining a source (FM-SRC-4); also the escalation threshold for unresolvable divergence (WP-6 fallback → FM-SRC-5, WP-6b → RESET) | absent — no rejection counting exists (GAP-30) | define ⚠ |
| `P-SOURCE-DOWN-ALARM` | continuous all-source unavailability before alarm (FM-SRC-1) | — | 5 min ⚠ |
| `P-PROBE-WAIT` | initial tip-probe quorum wait (WP-5) | 5 s | keep |

## Retention and space

| Parameter | Role | Observed | Target |
|---|---|---|---|
| `P-RETENTION-SLACK` | allowed window excess beyond `k` (RS-4, WP-10) | one *merged* storage batch (compaction merges old batches up to 200 k rows — the effective trim granularity) | keep, document per deployment |
| `P-RETENTION-APPLY` | External instruction → committed trim (WP-11, LIV-11) | prompt (unbounded formally) | ≤ 60 s ⚠ |
| `P-CLEANUP-PERIOD` | deferred logical-deletion sweep cadence (RS-5) | 10 s | keep |
| `P-CLEANUP-BACKOFF` | sweep retry after failure | 30 s | keep |
| `P-SPACE-AMP` | steady-state disk/live amplification bound (RS-6, SLI-8) | bounded since 2026-07 (PR #79: point-delete sweep + compaction); unmeasured (CT-7, GAP-6) | ≤ 2.0× ⚠ |
| `P-SPACE-CONST` | fixed overhead allowance (RS-6) | — | size per deployment ⚠ |
| `P-RECLAIM-LAG` | logical delete → physical space convergence (LIV-7) | sweep ≤ 10 s + compaction (typically minutes–hours); ≤ 7 d worst case via periodic compaction; interrupted-build residue: ∞ in default config (GAP-6) | ≤ 24 h ⚠ |
| `P-DISK-FLOOR` | free-disk alarm/degrade threshold (FM-STOR-2) | — | define ⚠ |
| `P-BLOCK-INDEX` | block hash index enabled (DEF-17, RS-12) | off by default (`--block-hash-index`); EVM only | keep |
| `P-TX-INDEX` | transaction hash index enabled (DEF-17, RS-12) | absent — not implemented (GAP-38) | define ⚠; must stay independent of `P-BLOCK-INDEX` (RS-12 sizing asymmetry) |

## Liveness, durability, lifecycle

| Parameter | Role | Observed | Target |
|---|---|---|---|
| `P-STALL-BUDGET` | max zero-progress interval under healthy conditions (LIV-2, SLI-9) | violated: 351–458 s freezes observed | ≤ 5 s ⚠ (GAP-1) |
| `P-LAG-STEADY` | steady-state ingest lag bound (LIV-1, SLI-1) | — | ≤ 2 s + batch quantum ⚠ |
| `P-CATCHUP-RATE` | minimum backlog drain rate (LIV-1) | — | per deployment ⚠ |
| `P-FORK-CONVERGE` | fork signal → REPLACE committed (LIV-9a, SLI-12) | — | ≤ 2 s + one batch ⚠ |
| `P-ALARM` | integrity fault → observable alarm (WP-17, LIV-9b, OB-9) | ∞ (no alarm states exist: GAP-5) | ≤ 10 s ⚠ |
| `P-STARTUP-ACCEPT` | process start → accepting connections (LIV-5a, SLI-5) | ~35 s observed (GAP-7) | ≤ 3 s ⚠ |
| `P-STARTUP-READY(state)` | per-dataset readable bound (LIV-5b, SLI-6) | — | budget curve vs state size ⚠ |
| `P-SHUTDOWN` | drain-and-exit bound (LIV-12) | — | ≤ 30 s ⚠ |
| `P-DUR-PROCESS` | commits lost on process crash (CN-6) | 0 | 0 |
| `P-DUR-SYSTEM` | commit-suffix loss window on host/power failure (CN-6b) | bounded, engine-managed (not explicitly configured) | make explicit ⚠ |
| `P-QUIESCENCE` | harness settling period before model comparison (12 §1) | — | 2× `P-CLEANUP-PERIOD` ⚠ |
| `P-RECOVERY-SETTLE` | post-overload return-to-normal bound (LIV-10) | — | ≤ 30 s ⚠ |

## Encoding

| Parameter | Role | Observed | Target |
|---|---|---|---|
| `P-ENCODINGS` | supported response codecs (IB-2) | gzip (default, fast level), zstd (level 1) | keep |
