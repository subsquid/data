# 07 — Liveness properties

Safety (06) says nothing bad happens; this document says the system actually makes
progress. Liveness properties are stated with explicit environmental preconditions and
bounds, so they are testable (CT-6/CT-7) and monitorable (11-observability).

## 0. Environmental definitions

- **Healthy source set**: at least one configured source is reachable, serves the
  dataset's chain from the service's resume position with parent-hash-consistent data,
  and its own tip advances (or holds) normally.
- **Adequate resources**: host CPU, memory, and storage I/O below saturation; free disk
  above the minimum operating threshold (`P-DISK-FLOOR`); no fault injection active.
- **Quiescent**: no source input, no retention instructions, no client load for a settling
  period `P-QUIESCENCE`.

All time bounds refer to sustained conditions — transient blips inside a bound are
compliant. Each LIV property names the observable(s) that witness it.

---

**LIV-1 — Ingestion progress.**
Given a healthy source set and adequate resources, `head(D)` converges to the source tip:
ingest lag (SLI-1) is eventually bounded by `P-LAG-STEADY`, and from any backlog state the
service catches up at a rate ≥ `P-CATCHUP-RATE` until steady lag is reached.
*Witness:* OB-2 progress heartbeat, SLI-1. *Tests:* CT-6, CT-7.

**LIV-2 — No unbounded write stall.**
Under the LIV-1 preconditions, the interval during which a dataset makes **zero** commit
progress while its sources offer new data never exceeds `P-STALL-BUDGET`. This bound
covers all internal causes: storage backpressure, maintenance debt, shared-pool
contention, deferred-deletion churn.
*Why this exists:* multi-minute all-dataset freezes have been observed post-deploy; this
property is the formal target the stall harness must enforce (GAP-1).
*Witness:* OB-3 stall detector. *Tests:* CT-7 soak + CT-6 stress.

**LIV-3 — Query termination.**
Every admitted query terminates (success or defined error) within
`P-QUERY-TIME + P-SCHED-SLACK`, regardless of concurrent write/fork/trim/maintenance
activity. No query hangs indefinitely.
*Witness:* SLI-2 latency distributions, OB-4. *Tests:* CT-3, CT-6.

**LIV-4 — Waiter release.**
Every head-waiting query (RP-5) is released within `P-HEAD-WAIT + P-SCHED-SLACK` — by
data arrival or `NO_DATA` — including during ingestion stalls and shutdown.
*Witness:* OB-4 waiter gauges. *Tests:* CT-3.

**LIV-5 — Bounded, decoupled startup.**
After process start: (a) the service begins *accepting* connections within
`P-STARTUP-ACCEPT`, independent of state size; (b) each dataset becomes readable within
`P-STARTUP-READY(state)` — a bound that is a function of that dataset's own state size,
not of other datasets'; (c) readiness is externally observable per dataset (OB-8).
Startup work that scales with total state (recovery scans, boot maintenance) MUST NOT
push (a) beyond its bound.
*Witness:* OB-8 phase timings. *Tests:* CT-2, CT-6 (startup benchmark).

**LIV-6 — Recovery liveness.**
After any crash, the service reaches LIV-5 readiness and resumes LIV-1 progress without
operator intervention, for every dataset whose persisted state is intact; damaged datasets
alarm (CN-10) without blocking the rest.
*Tests:* CT-2.

**LIV-7 — Reclamation liveness.**
After logical deletions (retention, forks, drops), physical space converges: within
`P-RECLAIM-LAG`, disk usage returns to within the amplification bound RS-6. Deletion debt
(logically deleted but physically present data) is eventually zero in a quiescent system.
*Witness:* OB-6 space accounting. *Tests:* CT-7 (GAP-6).

**LIV-8 — No cross-dataset starvation.**
One dataset's workload — ingest volume, query load, fork storms, retention churn,
maintenance debt — MUST NOT stall another dataset's ingestion (beyond `P-STALL-BUDGET`)
or starve its queries indefinitely. Shared resources may slow neighbors proportionally;
they may not halt them.
*Why:* the shared write path makes global halts the observed failure mode of the current
system (GAP-1); this property forbids the class.
*Tests:* CT-8 noisy-neighbor.

**LIV-9 — Fork convergence and alarmed divergence.**
(a) After an arbitrated fork signal, the dataset commits the corresponding REPLACE and
resumes tip-following within `P-FORK-CONVERGE` (bounds the WP-7 stale-fork visibility
window). (b) Divergences that cannot be applied safely — hints entirely below `fin`,
finality contradictions, un-linkable sources (below-window divergence, WP-6b) — reach a
distinct alarmed state within
`P-ALARM` instead of silent infinite retry (GAP-5). Reads keep being served from the last
good state throughout.
*Tests:* CT-4.

**LIV-10 — Overload recovery.**
When offered load exceeds capacity, the service sheds with `OVERLOADED` (RP-3) while
continuing to serve admitted work; when load subsides, service returns to normal within
`P-RECOVERY-SETTLE` with no residual degradation (no leaked slots/waiters/buffers, no
hysteresis lockup).
*Tests:* CT-6 load steps.

**LIV-11 — Retention keeps up.**
Under LIV-1 preconditions, window-trim debt is bounded: `next(D) − first(D)` never exceeds
`k + P-RETENTION-SLACK + (catchup transient)` for `Window(k)` datasets (positions, DEF-9 —
on a slot-numbered chain `span(D)` is smaller still); External
instructions apply within `P-RETENTION-APPLY` (WP-11).
*Tests:* CT-7.

**LIV-12 — Graceful shutdown.**
On a shutdown request, the service stops admitting work, completes or cleanly truncates
(RP-15) in-flight responses, commits or abandons in-flight batches atomically (INV-10),
and exits within `P-SHUTDOWN` — without panic-class failures and while remaining
crash-equivalent for durability purposes (a shutdown is never *worse* than a crash:
INV-40 still holds).
*Tests:* CT-2 (shutdown as a scheduled kill-point class).
