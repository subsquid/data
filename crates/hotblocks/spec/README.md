# Hotblocks — Behavioral Specification

This folder contains the abstract behavioral specification of **Hotblocks**: a real-time
block database that maintains a bounded, fork-aware window of one or more blockchains and
serves range queries over it.

The specification is **implementation-free**. It describes *what* the system must do — its
state model, operations, invariants, liveness properties, failure behavior, and performance
obligations — never *how* any particular implementation does it. It is written so that:

1. A conformance / TDD framework can be built against it (black-box, via the interface
   binding), using the reference model in [12-conformance-tdd.md](12-conformance-tdd.md)
   as the oracle.
2. Gaps between intended and actual behavior can be identified, prioritized, and closed.
3. Performance and robustness regressions can be detected against explicit, measurable
   properties instead of anecdotes.

## Document map

| Doc | Contents | Normative? |
|---|---|---|
| [01-overview.md](01-overview.md) | Purpose, actors, trust model, design goals and non-goals | Yes |
| [02-data-model.md](02-data-model.md) | Definitions: blocks, segments, watermarks, datasets, snapshots, queries | Yes |
| [03-write-path.md](03-write-path.md) | Ingestion, validation, transitions (extend / replace / finalize / retain / reset) | Yes |
| [04-read-path.md](04-read-path.md) | Query contract, coverage, continuation, conflict protocol, error taxonomy | Yes |
| [05-consistency-and-durability.md](05-consistency-and-durability.md) | Atomicity, isolation, visibility, durability, recovery | Yes |
| [06-invariants.md](06-invariants.md) | **The invariant catalog** (safety) — INV-* | Yes |
| [07-liveness.md](07-liveness.md) | Progress properties — LIV-* | Yes |
| [08-failure-model.md](08-failure-model.md) | Fault taxonomy and required responses — FM-* | Yes |
| [09-retention-and-space.md](09-retention-and-space.md) | Retention policies, logical vs physical space, reclamation — RS-* | Yes |
| [10-performance.md](10-performance.md) | Workload model, SLIs/SLOs, resource bounds, hazards — PF-*, SLI-*, HZ-* | Yes (SLO targets provisional) |
| [11-observability.md](11-observability.md) | Required observables so properties are testable/operable — OB-* | Yes |
| [12-conformance-tdd.md](12-conformance-tdd.md) | Reference model, test taxonomy, traceability matrix, **gap register** | Matrix/gaps informative, dated |
| [13-interface-binding.md](13-interface-binding.md) | Mapping of abstract operations to the wire protocol (HTTP) | Yes, for conformance testing |
| [14-parameters.md](14-parameters.md) | Registry of all symbolic parameters `P-*` and their current values | Values informative |

Dated **audit records** (`audit-YYYY-MM-DD.md`, e.g. [audit-2026-07-12.md](audit-2026-07-12.md))
capture spec↔implementation reviews: findings with code references, per-GAP verdicts, and the
spec amendments they motivated. They are informative and frozen at their date; living statuses
stay in 12 and 14.

## Conventions

- **RFC 2119 keywords.** MUST / MUST NOT / SHOULD / SHOULD NOT / MAY carry their standard
  normative meanings.
- **Identifiers** are stable and referenced across documents:
  - `DEF-n` — definitions (02).
  - `WP-n`, `RP-n`, `CN-n`, `RS-n`, `FM-n`, `PF-n`, `OB-n`, `IB-n` — requirements in the
    write-path, read-path, consistency, retention/space, failure-model, performance,
    observability, and interface-binding documents.
  - `INV-n` — safety invariants (06). `LIV-n` — liveness properties (07).
  - `SLI-n` — service-level indicators, `HZ-n` — performance hazards (10).
  - `CT-n` — conformance test classes, `GAP-n` — known/suspected gaps (12).
  - `P-NAME` — symbolic parameters. Normative text uses only the symbol; concrete values
    live in [14-parameters.md](14-parameters.md).
- **Numbering is banded** (gaps in numbering are intentional) so new items can be added to
  a category without renumbering.
- **Math notation.** Block numbers are natural numbers; hashes are opaque non-empty
  strings compared by exact equality; `⊥` denotes "absent/undefined"; sequences are
  written `⟨b_1 … b_n⟩`.
- **Two documents are expected to change often**: 12 (statuses, gap register) and
  14 (parameter values). All others change only when intended behavior changes, and any
  such change MUST be reflected in the traceability matrix of 12.

## How to use this spec for TDD

1. Build the harness described in 12 against the binding in 13: a scripted **source
   simulator** on one side, a **client driver** on the other, the system under test in the
   middle as a black box.
2. Implement the reference model (pseudocode in 12) as the oracle.
3. Work through the gap register (12 §6) in priority order; every test cites the `INV` /
   `LIV` / `RS` / `FM` / `PF` identifiers it verifies, keeping the traceability matrix
   honest.
