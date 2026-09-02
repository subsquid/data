# 05 — Consistency and durability

This document defines the consistency model binding the write path (03) to the read path
(04), and what survives failures. The corresponding invariants are INV-10, INV-20,
INV-30/31, INV-35/36, INV-40…44.

## 1. Commit model

- **CN-1 (Total order per dataset).** All transitions of one dataset are totally ordered;
  each committed state carries a version `ver` (DEF-10). There is no cross-dataset order.
- **CN-2 (Atomic visibility).** Readers observe committed states only. No intermediate
  state — a partially applied batch, a truncated-but-not-yet-replaced fork, a half-trimmed
  window — is ever observable (INV-10). "Observable" includes every read operation and
  every crash-recovered state.

## 2. Isolation

- **CN-3 (Snapshot isolation for queries).** Each query executes against one committed
  state (INV-20). Concurrent transitions never affect a running query's results; a query
  never blocks a transition and a transition never blocks admitted queries (performance
  coupling aside — see LIV-8).
- **CN-4 (Real-time monotonic reads).** If any read reflecting version `v` completes
  before another read of the same dataset starts (wall-clock order, any clients), the
  later read reflects `v' ≥ v` (INV-31). Together with CN-1 this makes watermark reads
  linearizable per dataset.
- **CN-5 (Watermark coherence).** All values inside one response's watermark set (head,
  fin, first) MUST come from one committed state. Response metadata MAY additionally
  expose a fresher head *number* than the query snapshot, but never an older one, and
  never a mixed (number, hash) pair.

## 3. Durability

Durability is two-tiered; both tiers preserve well-formedness — the difference is only
*how much of the recent suffix of commits* survives.

- **CN-6 (Process-crash durability).** After a process crash or kill (host survives),
  recovery MUST restore, per dataset, exactly the last committed state
  (`P-DUR-PROCESS = 0` lost commits).
- **CN-6b (System-crash durability).** After a host/OS/power failure, recovery MUST
  restore, per dataset, *some* committed state whose age is bounded by `P-DUR-SYSTEM`
  (bounded suffix loss). Never a state that was not committed; never a mixture of two
  states (INV-40).
- **CN-7 (Snapshot lifetime).** Read snapshots are short-lived — bounded by
  `P-QUERY-TIME` — so resources pinned by MVCC (superseded data kept alive for readers)
  are bounded (HZ-9). No API hands out unbounded-lifetime snapshots.
- **CN-8 (Clock independence).** Correctness (all INV-*) MUST NOT depend on wall-clock
  values: not on block timestamps, not on host clock monotonicity across restarts. Clocks
  parameterize only budgets, backoffs, and observability.

## 4. Recovery contract

- **CN-9 (Recovery = committed state).** The recovered state per dataset is a committed
  state in every field — segment, anchor (number *and* hash), fin, retention, kind.
  Derived/cached values MUST be reconstructed to exactly the committed values; a
  reconstruction that differs from what was committed (e.g. a wrong recovered anchor
  hash) violates INV-40 even if the segment itself is intact.
- **CN-10 (Recovery independence).** Datasets recover independently; one dataset's
  recovery failure (corrupt state, config mismatch) MUST NOT prevent recovery and serving
  of others (INV-36). The failed dataset enters a distinct alarmed state (OB-9).
- **CN-11 (Recovery idempotence).** Crash during recovery, followed by another recovery,
  converges: repeated crash/recover cycles do not accumulate damage or unbounded residue
  (INV-42). Build residue from interrupted writes (data written but never committed) is
  invisible (CN-2) and MUST eventually be collected (RS-10).
- **CN-12 (Format compatibility).** On startup the service MUST verify it can interpret
  the persisted state (format/version compatibility). Incompatibility MUST be refused
  loudly at startup, not discovered as scattered runtime decode errors (INV-43).
- **CN-13 (Maintenance transparency).** Background maintenance — physical reorganization,
  deferred deletion, space reclamation — MUST NOT change any committed logical state or
  any observable result (INV-17, INV-41). The single sanctioned exception: reader-free
  boot-phase maintenance (RS-8), which still MUST NOT change logical state, only physical
  representation. One narrow allowance on the read side: the choice of RP-9 *boundary
  markers* (header-only emissions of non-matching covered blocks) MAY vary with physical
  layout and hence with maintenance; matching content, item sets, coverage, and all
  watermarks MUST NOT.

## 5. Concurrency between subsystems

Per dataset, three actors touch state concurrently: the writer (single, WP-15), readers
(many), maintenance (background). The matrix of required non-interference:

| | vs Writer | vs Readers | vs Maintenance |
|---|---|---|---|
| **Writer** | single writer (WP-15) | never blocks admitted readers; readers never see partial writes (CN-2/3) | maintenance never alters what the writer committed (CN-13) |
| **Readers** | — | independent snapshots | reclamation never invalidates a live reader's data (INV-41) |
| **Maintenance** | — | — | idempotent, crash-safe (CN-11) |

Cross-dataset: logical independence is absolute (INV-35). *Performance* independence is a
liveness/performance requirement (LIV-8, HZ-1), not an isolation invariant — shared
resources exist, but starvation must be bounded.
