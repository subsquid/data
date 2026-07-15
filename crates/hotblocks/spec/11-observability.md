# 11 — Observability

Observability is normative here for two reasons: (1) several liveness properties are only
*testable* through observables; (2) the known failure modes of this system class (global
stalls, silent space growth, silent divergence loops) are exactly the ones that are
invisible without the signals below. "Expose" means: available through the observability
surface of the binding (13 §5) with bounded cardinality.

## 1. Required signals

- **OB-1 (Chain state gauges).** Per dataset: `first`, `head.number`, `fin.number`,
  commit version (or an equivalent monotone commit counter), last block time (when known),
  and the retention policy in force. These are the raw material for lag/progress alerting
  and for CT harness quiescence detection.
- **OB-2 (Progress heartbeat).** Per dataset: time since last commit, and time since last
  *offered-but-uncommitted* source data (the difference distinguishes "idle chain" from
  "stalled service"). LIV-1/LIV-2 are decided from this signal.
- **OB-3 (Write-pressure signals).** Global and per shared resource: whether writes are
  currently halted/throttled by the storage layer, current throttle rate, queue depths of
  the write pipeline stages, commit-retry counts (HZ-11), maintenance backlog size
  (HZ-2). The *existence* of a global write halt MUST be directly observable — the
  historical multi-minute freezes were diagnosable only by inference (GAP-1).
  `hotblocks_write_duration_seconds{dataset,stage,outcome}` exposes the bounded stages
  `prepare`, `tables`, `commit`, `retention`, `block_hash_index`, and
  `transaction_hash_index`; hash-index samples are nested within `commit` or `retention`
  and include work repeated by optimistic-transaction retries.
- **OB-4 (Query metrics).** Per dataset × query class × outcome (success / each error
  class / truncation per RP-15): counts, latency distributions (TTFB, total), emitted
  bytes/blocks, coverage sizes; current in-flight and waiting counts against their caps
  (`P-EXEC-SLOTS`, `P-WAITERS`).
- **OB-5 (Error budget accounting).** `INTERNAL` occurrences individually traceable
  (request-correlatable); `OVERLOADED` rates; source-rejection and quarantine counts per
  source (FM-SRC-4).
- **OB-6 (Space accounting).** Per store and attributable per dataset where feasible:
  `live_bytes` estimate, `debt_bytes` estimate (logically-deleted-not-reclaimed +
  residue), `disk_bytes`, age of oldest unreclaimed debt, and the current reclamation
  blocker if any (what pins the watermark — GAP-6 made this a first-class question).
- **OB-7 (Source health).** Per dataset × source: reachability, current backoff state,
  last successful delivery, rejected-run counts, arbitration disagreement events
  (FM-SRC-6), finality-conflict events (FM-SRC-5).
- **OB-8 (Lifecycle phases).** Startup: timestamped phase transitions (recover → boot
  maintenance → per-dataset ready → accepting) so SLI-5/6/7 are measurable in production,
  not only in the harness. Liveness/readiness probes: process-accepting vs per-dataset
  readable are distinct signals (LIV-5c). Shutdown: drain progress.
- **OB-9 (Alarm states).** Distinct, queryable, per-dataset alarm conditions with reason
  codes: integrity conflict (WP-8/FM-SRC-5), RESET occurred (WP §2.6), boot validation
  refusal (INV-43), dataset stopped (CN-10), dual-writer detected (WP-15), disk floor
  breached (FM-STOR-2). Alarms are edge-triggered events *and* level-readable states.
- **OB-10 (Bounded cardinality).** All label spaces are bounded by configuration (datasets,
  sources, classes, outcome enums); unbounded client-derived labels MUST be sanitized
  (e.g. allowlisted client identities, "other" bucket).
- **OB-11 (Stall forensics).** When OB-2/OB-3 cross the stall budget, the service SHOULD
  capture a self-diagnostic snapshot (thread/task states, pipeline queue depths, storage
  engine state) sufficient to attribute the stall post-hoc. Rationale: the observed
  production freezes could not be root-caused from standard metrics; capture-on-stall
  turns the next occurrence into data (GAP-1).
- **OB-12 (Hash index state).** Per dataset × index (DEF-17): whether it is enabled, its
  entry count and estimated bytes (feeding OB-6's live/debt accounting — RS-12), and lookup
  counts split by outcome (hit / miss) with latency. Rationale: because a miss is
  indistinguishable from a genuine absence by design (RP-19), the miss *rate* is the only
  signal an operator has. A rate near 1 is the signature of an index that is empty for a
  structural reason — flag switched on after the window had filled, unsupported kind — and
  without this signal it looks exactly like a chain on which nobody queries real hashes.

## 2. Property → observable mapping

| Property | Decided by |
|---|---|
| LIV-1 ingest progress | OB-1 head vs simulator tip, OB-2 |
| LIV-2 stall budget | OB-2, OB-3 |
| LIV-3/4 query/waiter termination | OB-4 |
| LIV-5/6 startup/recovery | OB-8 |
| LIV-7 reclamation | OB-6 |
| LIV-8 isolation | OB-1/2/3 per dataset under CT-8 |
| LIV-9 fork convergence / alarmed divergence | OB-1 (version/head), OB-9, OB-7 |
| LIV-10 overload recovery | OB-4, OB-5 |
| LIV-11 retention keep-up | OB-1 (`span` vs `k`) |
| LIV-12 shutdown | OB-8 |
| RS-6 amplification | OB-6 |
| FM alarms | OB-9 |

## 3. Requirements on the harness (informative)

The conformance harness treats observables as part of the SUT contract: OB signals are
scraped continuously during every CT run; a liveness verdict derived from harness-side
black-box observation MUST agree with the verdict derived from OB signals — disagreement
means the observability layer itself is defective (lying metrics are treated as failures,
same as lying data).
