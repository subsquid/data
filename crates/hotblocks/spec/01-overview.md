# 01 — Overview

## 1. What Hotblocks is

Hotblocks is a **real-time block database**. For each configured **dataset** it maintains a
bounded, contiguous, fork-aware **window** of a blockchain — typically the most recent
portion, close to the chain tip — and serves **range queries** over the blocks and their
contents (transactions, logs, instructions, etc., depending on the chain family).

It is the "hot" complement to an archival store: archives hold the deep, immutable history;
Hotblocks holds the volatile tip where blocks arrive continuously, forks and rollbacks
happen, and freshness is measured in fractions of a second.

Alongside range queries it offers **point lookups by hash** — resolving a block hash, or a
transaction hash, to a position in the window — for consumers that hold a bare hash from a
log, an event, or a third party and need a block number to query with. These lookups are
served from optional derived indexes ([DEF-17](02-data-model.md)) and come with a sharp
caveat spelled out in NG7 below.

The system is a single service instance hosting many datasets concurrently (tens of
datasets is the normal operating point). Datasets are independent chains: different
networks, different chain families ("kinds"), different retention policies.

## 2. Actors

| Actor | Role |
|---|---|
| **Block sources** | Upstream providers that stream blocks, fork signals, and finality signals for a dataset. Several redundant sources MAY feed one dataset. |
| **Query clients** | Consumers issuing ranged queries, watermark reads, and continuation/fork-recovery round trips. Clients are typically automated pollers following the chain tip. |
| **Retention controller** | An external agent that (for datasets in the *External* retention mode) instructs the service which lower bound of history to keep. |
| **Operator** | Configures datasets (kind, sources, retention policy), deploys, restarts, monitors. |

## 3. Design goals

- **G1 — Freshness.** A block published by a healthy source becomes queryable quickly; the
  visible head tracks the source tip with bounded lag ([LIV-1](07-liveness.md), SLI-1).
- **G2 — Fork correctness.** The stored window is always a single consistent chain; forks
  replace a suffix atomically; clients anchored by a parent hash can never silently read
  across a reorg ([INV-1..3](06-invariants.md), [INV-23](06-invariants.md)).
- **G3 — Bounded resources.** Storage, memory, and startup work are bounded functions of
  configured retention — never of total chain history or uptime ([RS-6](09-retention-and-space.md), PF-*).
- **G4 — Isolation.** Datasets are logically independent; one dataset's load, faults, or
  maintenance must not corrupt — and must not indefinitely stall — another
  ([INV-35/36](06-invariants.md), [LIV-8](07-liveness.md)).
- **G5 — Self-healing.** Crashes, source misbehavior, and transient overload are absorbed
  without operator intervention; unrecoverable conditions are surfaced loudly rather than
  retried silently forever ([INV-40..44](06-invariants.md), [FM-*](08-failure-model.md)).
- **G6 — Testability.** Every guarantee in this spec is stated so that a black-box harness
  with a scripted source and client can verify it ([12](12-conformance-tdd.md)).

## 4. Non-goals (explicit)

- **NG1 — Not an archive.** History outside the retention window is not available and its
  absence is a defined error, not a defect. **Retention dominates finality**: finalized
  blocks are deleted when the window moves past them ([RS-2](09-retention-and-space.md)).
- **NG2 — No multi-fork serving.** At any instant a dataset presents exactly one chain.
  Competing forks are never simultaneously queryable; the service converges to what its
  sources present as canonical ([DEF-6](02-data-model.md)).
- **NG3 — Not a consensus participant.** The service does not validate consensus rules,
  signatures, or state transitions. It validates *structural* consistency (numbering,
  parent-hash linkage, finality monotonicity) and otherwise trusts its sources
  (see Trust model, §5).
- **NG4 — No cross-dataset transactions.** No operation spans datasets atomically.
- **NG5 — No server-side query cursors.** Continuation is client-driven and stateless on
  the server ([RP-10](04-read-path.md)).
- **NG6 — No exactly-once source consumption.** Source delivery is at-least-once;
  ingestion is idempotent with respect to redelivery ([WP-16](03-write-path.md)).
- **NG7 — Hash lookups are a convenience, not an oracle.** The hash indexes are optional,
  derived, and deliberately incomplete: never backfilled, enabled per deployment, defined
  only for kinds that expose the hash. A lookup **hit** is authoritative; a lookup **miss**
  is not evidence that the block or transaction is absent from the window
  ([RP-19](04-read-path.md)). Making misses meaningful would require backfilling history
  the hot store has already accepted without an index — work whose cost scales with the
  window, for a guarantee a range query already provides.

## 5. Trust model

- **Sources are semi-trusted.** The service verifies everything it can verify locally:
  ascending numbering, parent linkage against its own stored chain, finality
  monotonicity, and cross-source agreement where multiple sources exist. It cannot verify
  that a hash corresponds to a *valid* block, so the *content* of blocks and the *choice*
  of canonical fork and finality are trusted from sources. A source that violates the
  structural rules MUST be rejected/quarantined per [FM-SRC-*](08-failure-model.md); it
  MUST NOT be able to corrupt stored state or crash the process.
- **Clients are untrusted.** Any client input — malformed, oversized, adversarial — MUST
  yield a defined error and MUST NOT affect other clients' correctness, crash the process,
  or wedge ingestion ([FM-1](08-failure-model.md)).
- **The operator is trusted** but fallible: configuration mistakes MUST fail loudly and
  safely at startup rather than silently corrupt or delete the wrong data
  ([INV-43/44](06-invariants.md)).

## 6. Lifecycle at a glance

```
                     ┌──────────────────────────── service instance ───────────────────────────┐
 sources ──blocks──▶ │ per-dataset ingestion ──▶ atomic transitions ──▶ committed chain window │ ──▶ query clients
          fork sigs  │      validation             (extend/replace/       (snapshot reads)     │      (ranged queries,
          finality   │      arbitration             finalize/retain)                           │       watermarks)
                     │                                                                         │
 retention ─────────▶│ retention policy ──▶ logical trim ──▶ deferred physical reclamation     │ ──▶ observability
 controller          └─────────────────────────────────────────────────────────────────────────┘     (metrics, health)
```

A dataset's life: **created** (kind fixed forever) → **window established** (per retention
policy) → steady state of *extend / replace-on-fork / finalize / retain* transitions →
possibly **reset** (self-healing on unrecoverable divergence) → **dropped** (removed from
configuration). The service's life: **boot** (recover, validate, optional reader-free
maintenance) → **ready** (serving + ingesting) → **draining shutdown**.

## 7. Reading guide

The formal core is 02 (state) → 03/04 (transitions and reads) → 06/07 (what must always /
eventually hold). 05, 08, 09 elaborate cross-cutting guarantees. 10/11 make the operational
qualities testable. 12 turns all of it into a test plan; 13/14 pin the concrete surface.
