# 06 — Invariant catalog (safety)

Every invariant below MUST hold. Scopes:

- **[state]** — holds in every observable committed state (every snapshot, every
  recovered state).
- **[transition]** — relates consecutive committed states of one dataset.
- **[response]** — holds for every read response.
- **[recovery]** — holds across crash/restart boundaries.

Each entry: statement, why it matters, and how a black-box harness checks it
(test classes CT-* are defined in [12-conformance-tdd.md](12-conformance-tdd.md)).

---

## A. Structural — the stored chain

**INV-1 — No missing blocks.** [state]
Numbers strictly ascend, and every stored block is the child of the one below it:
`∀ i < n: b_{i+1}.parent_number = b_i.number`. The window holds every *block* of the chain
between its ends — but not every *number*: a slot-numbered chain (Solana) leaves holes where no
block was produced (DEF-1).
*Why:* the window is a contiguous run of the chain. A hole in the numbering is not a hole in the
data, and treating one as the other is how a correct Solana window gets rejected.
*Check:* CT-1 model comparison, dense and sparse scripts; CT-5 scanning reads.

**INV-2 — Internal linkage.** [state]
`∀ i < n: b_{i+1}.parent_hash = b_i.hash`.
*Why:* the window is one chain, not a set of blocks.
*Check:* CT-1/CT-5 with linkage fields projected.

**INV-3 — Anchor linkage.** [state]
`anchor.number < first(D)`, and if `anchor.hash ≠ ⊥` then `b_1.parent_hash = anchor.hash`.
(On a densely-numbered chain `anchor.number = first(D) − 1`; on a slot-numbered one the parent
of `b_1` may sit further down.)
*Why:* the window's attachment below is what fork recovery at the window edge relies on.
*Check:* CT-1; CT-2 after restarts (see INV-40).

**INV-4 — Kind and schema conformance.** [state]
`kind` never changes for a dataset identity (except through DROP+CREATE); every stored
block's payload conforms to the kind's schema; every item's block number equals its
enclosing block's number.
*Check:* CT-1, CT-5 structural validators.

**INV-5 — Watermark bounds.** [state]
If `seg = ∅` then `fin = ⊥`. Otherwise, if `fin ≠ ⊥` then
`first(D) ≤ fin.number ≤ head(D).number`.
*Why:* a finalized pointer outside the window is meaningless and breaks fork-floor logic.
*Check:* CT-1 (assert on every STATUS read).

**INV-6 — Finalized-on-chain.** [state]
If `fin ≠ ⊥` then the stored block at height `fin.number` has hash `fin.hash`.
*Why:* finality must describe the chain actually served, else finalized-only reads lie.
*Check:* CT-1; CT-4 with equivocating-finality sources (GAP-4).

**INV-7 — Provenance fidelity.** [state]
Every stored block was delivered by a configured source, and all queryable field values
equal the source-delivered values for that `⟨number, hash⟩`. The service never invents,
mutates, or transplants data between blocks.
*Check:* CT-1/CT-6 round-trip against the source simulator's ledger.

---

## B. Transition legality

**INV-10 — Atomic transitions.** [transition]
Every observable state change is exactly one transition from the catalog (03 §2), applied
atomically: no reader or recovery ever observes an intermediate (partial batch, partial
block, truncation-without-replacement, half-trim).
*Check:* CT-3 concurrent readers during heavy write/fork/trim traffic; CT-2 crash points.

**INV-11 — Append at the tail only.** [transition]
`EXTEND` leaves `seg[‥next]` and the anchor unchanged; new blocks attach only above the
previous head.
*Check:* CT-1 model.

**INV-12 — Finality monotonicity.** [transition]
`fin.number` never decreases while defined, and at a fixed height the finalized hash never
changes. `fin` may become `⊥` only via RETAIN (window passing above it), RESET, or DROP.
*Why:* clients treat finalized data as irreversible-while-retained.
*Check:* CT-1 (monitor every FINALIZED-HEAD read); CT-4 stale/regressing finality sources.

**INV-13 — Finalized immutability.** [transition]
No `REPLACE` removes or alters blocks at heights `≤ fin.number`. Finalized blocks leave
the store only via RETAIN / RESET / DROP.
*Check:* CT-1/CT-4 fork storms around the finality boundary.

**INV-14 — Fork floor.** [transition]
For every `REPLACE(from, B)`: `from > fin.number` (when `fin ≠ ⊥`) **and**
`from ≥ first(D)`. A deeper divergence is representable only as RESET (explicit, alarmed).
*Why:* silent rollback below the window or below finality corrupts continuation clients.
*Check:* CT-4 deep-fork corpus (GAP-3).

**INV-15 — Retention trims prefix only.** [transition]
`RETAIN` removes only blocks with numbers below its `from`; it never creates gaps, never
touches the suffix, never lowers `first(D)`.
*Check:* CT-1 with retention churn.

**INV-16 — Frame condition.** [transition]
Without a triggering input (source event, retention trigger/instruction, operator action),
the logical state does not change: `ver` stable ⇒ identical observable state.
*Check:* CT-1 quiescence comparisons; CT-7 soak with idle periods.

**INV-17 — Maintenance transparency.** [transition]
Background maintenance never changes logical state: for any query, results are identical
with maintenance on or off (metamorphic property) — identical up to the RP-9
boundary-marker allowance (CN-13): *which* non-matching covered blocks appear as
header-only records may differ; matching content, item sets, and coverage may not.
*Check:* CT-7 A/B soak; CT-3.

**INV-18 — Anchor carry-over.** [transition]
After a trimming `RETAIN(from)`, the new anchor is `⟨from − 1, hash of from's preceding
block in the pre-trim state⟩` (`hash_at(from − 1)`, DEF-16) — preserved from data, not
recomputed or dropped.
*Check:* CT-1 (STATUS/first-block checks after trims + CONFLICT hints at the window edge).

---

## C. Read semantics

**INV-20 — Snapshot isolation.** [response]
Every response is computed against exactly one committed state; no response mixes two
states (torn reads are impossible, including during forks and trims).
*Check:* CT-3 — readers hammering during fork/trim storms; any response mixing old and new
fork blocks at linked heights fails INV-2 within the response.

**INV-21 — Response well-formedness.** [response]
Emitted blocks: strictly ascending, unique numbers, all inside coverage `[from, L]`;
coverage gap-free; every emitted item belongs to its emitting block; blocks truncate whole
(never partial item sets due to budget).
*Check:* CT-5/CT-6 structural validators on every response.

**INV-22 — Completeness and determinism within coverage.** [response]
For the response's snapshot `S` and coverage `[from, L]`: every block in coverage matching
the selection is emitted (all covered blocks when `include_all`), each with **all** its
matching items projected to the selected fields; the coverage-end block is always emitted
(RP-9 carrier, header-only when unmatched). Matching blocks and their item content are a
deterministic function of `(S, query, coverage)`. Additional covered non-matching blocks
MAY appear as header-only boundary markers and are exempt from the determinism clause
(they may vary with physical layout, CN-13) — they MUST still be true blocks of `S`
inside coverage.
*Check:* CT-1/CT-6 against the reference model evaluated over the same coverage, with the
marker exemption.

**INV-23 — Anchored ancestry.** [response]
If a query supplies `expected_parent` and succeeds, every emitted block lies on the unique
chain extending that parent (the first covered stored block's
`parent_hash = expected_parent = hash_at(from − 1)`, DEF-16). If the assertion fails
against the snapshot, the response is `CONFLICT` with zero blocks and hints that are true
`⟨p, hash_at(p)⟩` entries of the snapshot's chain ending at `from − 1` (or the nearest
stored position below), ascending, non-empty.
*Check:* CT-4 fork corpus with anchored clients (the client-recovery algorithm of 04 §7
must never assemble a cross-fork chain).

**INV-24 — Finalized-only containment.** [response]
A `QUERY-FINALIZED` response's coverage never exceeds `fin(S).number`, and all emitted
blocks lie on the finalized prefix of `S`. While retained and while `fin` is defined, the
content of a once-finalized height never changes across responses (follows from INV-12/13).
*Check:* CT-4: finalized-only pollers must never observe two hashes at one height.

**INV-25 — Progress.** [response]
A successful query whose effective range is non-empty has coverage of at least one block
(`L ≥ from`), and if the first covered block matches the selection it is emitted even if
it alone exceeds the response budget.
*Why:* rules out client livelock on oversized blocks or zero-progress responses.
*Check:* CT-6 with pathological giant blocks; CT-5.

**INV-26 — Error soundness.** [response]
Every failure maps to exactly one class of the taxonomy (04 §8) under exactly its
specified trigger; error responses carry no block data; no trigger condition yields
`INTERNAL`.
*Check:* CT-5 error-matrix suite.

**INV-27 — Range honesty.** [response]
No response emits a block outside `[from, min(to, hi(S))]`; no response emits a block not
present in its snapshot (never fabricated, never resurrected-after-trim).
*Check:* CT-1/CT-3.

---

## D. Watermark reporting

**INV-30 — Watermark honesty.** [response]
Every HEAD / FINALIZED-HEAD / STATUS value equals the corresponding field of some
committed state of that dataset; `⟨number, hash⟩` pairs are never mixed across states.
*Check:* CT-1 — every reported watermark must appear in the model's committed history.

**INV-31 — Real-time monotonic reporting.** [response]
Reads reflect versions monotonically w.r.t. real-time order (CN-4): once any read reflects
version `v`, no later-started read reflects `v' < v`. In particular a reported head is
immediately queryable: a query admitted after a HEAD response reflecting version `v` runs
on a snapshot `≥ v`.
*Check:* CT-3 interleaved HEAD+QUERY clients (advertise-then-404 races fail this).

---

## E. Multi-dataset isolation

**INV-35 — Logical independence.** [state]
The committed state of dataset `D` is a function of `D`'s own inputs (its sources, its
retention instructions, its config) only. No operation on `D'` changes any observable of
`D`.
*Check:* CT-8 noisy-neighbor suites (differential: run D's script with and without D'
active; observables of D identical up to timing).

**INV-36 — Failure containment.** [state]
A dataset in any failure/alarm/reset state does not corrupt other datasets' state or
render their operations incorrect. (Shared-fate *performance* effects are governed by
LIV-8, not permitted to become correctness effects.)
*Check:* CT-8 with one dataset driven into every FM-* fault while others run CT-1 checks.

---

## F. Durability and recovery

**INV-40 — Committed-state recovery.** [recovery]
After process crash/kill: recovery restores exactly the last committed state per dataset
(CN-6). After system crash: some committed state within `P-DUR-SYSTEM` (CN-6b). All
fields, including `anchor.hash` and `fin`, equal those of that committed state — recovery
never yields a state that was never committed, a mixture, or a well-formed-but-different
reconstruction.
*Check:* CT-2 kill-point matrix + model comparison after restart (GAP-2).

**INV-41 — Reclamation invisibility.** [state]
Physical space operations (deferred deletion, reorganization, file-level reclamation)
never change logical state and never invalidate data reachable by any live reader.
Exception: the boot-phase reader-free mode (RS-8), in which no reader can exist by
construction — logical state still MUST be unchanged.
*Check:* CT-3 readers during cleanup churn; CT-7 soak.

**INV-42 — Recovery idempotence and residue convergence.** [recovery]
Repeated crash/recovery cycles converge to the same committed state; invisible residue
from interrupted writes is bounded and eventually collected (RS-10) — residue never
becomes visible and never permanently blocks reclamation.
*Check:* CT-2 crash-during-recovery; CT-7 residue accounting via OB-6.

**INV-43 — Boot validation.** [recovery]
On startup, configuration is validated against persisted state: kind mismatch, pinned
anchor mismatch, or format incompatibility (CN-12) MUST cause an explicit refusal
(dataset-level alarm or startup failure with a distinct diagnostic) — never silent
reinterpretation, never silent destruction (see INV-44 for the sanctioned destructive
paths).
*Check:* CT-5 boot matrix (config × pre-state).

**INV-44 — Destructive operations are explicit.** [transition]
Data leaves the store only through: RETAIN per policy, REPLACE of the volatile suffix,
RESET under its defined triggers (WP-6b / WP-9 / downward retention bound, WP §2.5 /
operator), or DROP of datasets removed from configuration. Each destructive path is deliberate, documented, and observable (OB-9).
There is no other code path by which committed blocks disappear. One non-transition
exception, scoped by CN-6b: a host/power failure may rewind a dataset to an earlier
*committed* state — a bounded suffix of recent commits lost, never a targeted deletion,
never a state that was not committed. A process-level crash loses nothing (CN-6).
*Check:* CT-1 model (any unexplained disappearance fails); CT-2 (a process crash must not
eat committed blocks; a system crash may only rewind to a committed state within
`P-DUR-SYSTEM` — ties INV-40).

---

## Reading the catalog in tests

A minimal harness assertion set that touches most of the catalog on every step:

1. After every observed commit (via watermark polling): STATUS + full-window structural
   scan ⇒ INV-1..6, 15, 18, 27, 30.
2. Every response through validators ⇒ INV-20..27.
3. Model diff at quiescence ⇒ INV-7, 10, 11, 16, 17, 35, 44.
4. Kill/restart cycles ⇒ INV-40, 42, 43.
5. Fork/finality corpora ⇒ INV-12, 13, 14, 23, 24, 31, 36.
