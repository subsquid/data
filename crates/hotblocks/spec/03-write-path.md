# 03 — Write path (ingestion and transitions)

This document specifies how a dataset's state advances: what is accepted from sources, how
it is validated, and the exact pre/postconditions of every transition. All requirements are
per dataset unless stated otherwise.

## 1. Ingestion loop (abstract)

For each dataset the service runs a conceptual loop:

```
loop:
  pos ← resume position = ⟨ next(D), expected parent hash ⟩
        where expected parent hash = head(D).hash if seg ≠ ∅ else anchor.hash (may be ⊥)
        (= hash_at(next(D) − 1), DEF-16)
  request blocks from sources starting at pos
  react to source events:
    Blocks(B)          → validate, accumulate, commit EXTEND / REPLACE batches
    ForkSignal(hints)  → resolve rollback point, resume from it (next commit is a REPLACE)
    FinalitySignal(r)  → commit FINALIZE (possibly composed with a batch commit)
    OnTip              → flush any accumulated partial batch
  apply retention policy after commits (RETAIN when the policy demands it)
```

- **WP-1** The service MUST always request continuation from its own committed state
  (`next(D)` and the corresponding expected parent hash), never from transient in-memory
  positions that could disagree with committed state.
- **WP-2 (Validation).** Before commit, a candidate run of blocks MUST be verified to be:
  internally linked (`b_{i+1}.parent_number = b_i.number ∧ b_{i+1}.parent_hash = b_i.hash`)
  and correctly attached at the front (per the transition preconditions in §3). Since
  `parent_number < number` (DEF-4), linkage implies strictly ascending numbers — but an
  implementation that checks only *hash* linkage MUST verify ascending numbers separately:
  hashes are opaque, source-controlled strings (DEF-2), so hash linkage alone implies no
  number order (GAP-20). A run failing validation MUST be discarded without any
  state change, and the offending source penalized ([FM-SRC-4](08-failure-model.md)).
- **WP-3 (Batching).** Blocks MAY be accumulated and committed in batches. A batch is
  bounded by `P-BATCH-ROWS` rows / `P-BATCH-BYTES` bytes and MUST be flushed no later
  than: the bound being reached, an `OnTip` event, a `ForkSignal`, or an item-availability
  change. Batching MUST NOT reorder or drop blocks. (Freshness consequence: the head
  advances in batch-sized steps; see HZ-6 and SLI-11.)
- **WP-4 (Multi-source arbitration).** With multiple sources, the service MUST present the
  effect of a single coherent source: duplicate deliveries deduplicated, positions below
  `next(D)` ignored, and a `ForkSignal` acted upon only when corroborated — by a majority
  of sources, by all currently live sources, or after `P-FORK-CONSENSUS` time without
  contradiction. Arbitration MUST NOT interleave blocks from sources on different forks
  into one run (WP-2 makes such a run unommittable).
- **WP-5 (Initial window).** On first activation, a dataset's initial anchor is
  established per policy: `Pinned(from, h?)` → `anchor = ⟨from − 1, h?⟩` (`from = 0` ⇒
  `h? = ⊥`: there is no position below genesis to assert a hash against — DEF-7, and no
  wire representation of position `−1`, IB §3);
  `Window(k)` → probe the sources' current tip `T` and set
  `anchor = ⟨max(0, T − k) − 1 …⟩` semantics such that roughly the last `k` blocks will be
  ingested; `External`/`Unbounded` → resume from the existing window when the dataset is
  non-empty; an **empty** dataset has no defined start position and MUST idle (no source
  consumption) until one is established — by the first SET-RETENTION instruction
  (External) or by reconfiguration (Unbounded). Idling is not a fault; STATUS reports
  the empty state. Probing MUST NOT block the rest of the service (LIV-5) and MUST be
  retried with bounded backoff while sources are unavailable.

## 2. Transition catalog — preconditions and effects

Notation: unprimed = state before, primed = state after. Every transition increments
`ver` by one and is atomically visible (INV-10). `seg[x‥]` / `seg[‥x]` denote the suffix
from number `x` / prefix below number `x`.

### 2.1 CREATE(kind, ret)

- Pre: no dataset with this identity, or an existing dataset with the **same** kind
  (idempotent re-attach). An existing dataset with a different kind MUST be refused
  (INV-43); the service MUST NOT silently reinterpret or overwrite it.
- Post, fresh create: `⟨kind, seg=∅, anchor per WP-5, fin=⊥, ret, ver=0⟩`.
- Post, re-attach: the persisted committed state is preserved in every field — `seg`,
  `anchor`, `fin`, `ver` (INV-40); only `ret` is taken from configuration and validated
  against that state (WP-9 / INV-43). Re-attach is **not** a destructive path (INV-44):
  an attach that loses or resets stored state is a defect, not CREATE semantics.

### 2.2 EXTEND(B, f?)

- Pre: `B ≠ ∅` and valid per WP-2, and:
  - `seg ≠ ∅` ⇒ `B[0].number ≥ next(D)` ∧ `B[0].parent_number = head(D).number` ∧
    `B[0].parent_hash = head(D).hash`;
  - `seg = ∅` ⇒ `B[0].number ≥ anchor.number + 1` ∧
    (`anchor.hash ≠ ⊥` ⇒ `B[0].parent_hash = anchor.hash`).
- Post: `seg' = seg ⧺ B`; `anchor' = anchor`; `fin'` per composed FINALIZE if `f?` given,
  else `fin`.

(`=` in place of `≥` on a densely-numbered chain; on a slot-numbered one the first block
of a run may sit above the requested position — `next(D)` is the lowest *admissible*
position, not a promised block number, DEF-1.)

### 2.3 REPLACE(from, B, f?) — fork application

- Pre: `seg ≠ ∅`; `B ≠ ∅` and valid per WP-2; `B[0].number ≥ from`; and
  - **fork floor (finality):** `fin = ⊥ ∨ from > fin.number`  — MUST (INV-13/14);
  - **fork floor (window):** `from ≥ first(D)` — MUST (INV-14). A required rollback below
    `first(D)` is not representable as REPLACE; it MUST be handled as RESET (§2.6) and
    surfaced as an event ([OB-9](11-observability.md));
  - linkage: `from > first(D)` ⇒ `B[0].parent_hash` = the hash of `from`'s preceding
    block (`hash_at(from − 1)`, DEF-16 — `from − 1` itself may be a hole on a
    slot-numbered chain); `from = first(D)` ⇒ (`anchor.hash ≠ ⊥` ⇒
    `B[0].parent_hash = anchor.hash`).
- Post: `seg' = seg[‥from] ⧺ B`; `anchor' = anchor`; `fin'` per composed FINALIZE or `fin`.
- `REPLACE(next(D), B)` degenerates to `EXTEND(B)`.

**WP-6 (Fork resolution).** Upon an arbitrated `ForkSignal(hints)` the service MUST
compute the resume point as follows: consider hints with `number ≥ fin.number` only — if
none exist, or the hint at `fin.number` carries a different hash than `fin.hash`, the
signal contradicts finality and MUST be treated as a source integrity fault
([FM-SRC-5](08-failure-model.md)), not applied. Otherwise resume from
`⟨m + 1, hint(m).hash⟩` where `m` is the highest hint number whose `⟨number, hash⟩` equals
the stored chain (or the anchor). If no hint matches stored state, the fallback MUST
respect the finality floor:

- `fin ≠ ⊥` → resume from `⟨fin.number + 1, fin.hash⟩`: the finalized block is common to
  both chains unless the source contradicts finality, so only the volatile suffix is
  replaced (a full-window replacement would violate the fork floor anyway, INV-14). If
  the source then repeatedly fails to link at this position, that *is* a finality
  contradiction: after `P-SOURCE-STRIKES` consecutive rejections it MUST be classified as
  FM-SRC-5 (fault + alarm, keep serving) — never RESET;
- `fin = ⊥` → resume from `⟨first(D), anchor.hash⟩` (full-window replacement); WP-6b
  governs escalation if the divergence turns out to lie below the window.

The subsequent commit is `REPLACE(m + 1, …)` (resp. `REPLACE(fin.number + 1, …)`,
`REPLACE(first(D), …)`). The resume point MAY be conservatively deeper than the optimal
`m + 1` by at most one storage batch (an implementation that matches hints only at batch
boundaries): the replacement then re-commits blocks identical to those it replaces, which
is correctness-neutral. It MUST NOT be shallower than `m + 1` and MUST NOT cross the
floors of §2.3.

**WP-6b (Divergence below the window → RESET).** WP-6's fallback cannot represent a fork
deeper than the window; the service MUST detect that case and escalate to RESET (alarmed,
OB-9) instead of retrying a replacement that can never link:

- **direct evidence:** an arbitrated hint at position `anchor.number` whose hash
  contradicts `anchor.hash ≠ ⊥` — the divergence is at or below the anchor;
  `RESET(⟨anchor.number, hint hash⟩)`;
- **indirect evidence:** after a full-window-replacement resume (reachable only when
  `fin = ⊥` — WP-6 fallback), the source's runs
  repeatedly fail attachment at `first(D)` (`B[0].parent_hash ≠ anchor.hash`, WP-2).
  After `P-SOURCE-STRIKES` consecutive such rejections the condition MUST be classified
  as a below-window divergence — `RESET(⟨anchor.number, ⊥⟩)` — not retried silently
  forever (LIV-9b, GAP-3/GAP-5).

**WP-7 (Fork visibility window).** Between acting on a `ForkSignal` and committing the
first `REPLACE`, the pre-fork state remains the committed, visible state — the service
MUST NOT truncate without replacement. Consequences: orphaned blocks remain readable to
un-anchored clients during this interval; anchored clients are protected by the CONFLICT
protocol (RP-11). The interval is bounded by LIV-9.

### 2.4 FINALIZE(r)

Let `e = min(r.number, head(D).number)` (a finality report above the head is clamped to
the head; the excess is not forgotten by sources and will re-arrive).

- Pre: `seg ≠ ∅` (else the report is deferred/ignored); `e ≥ first(D)` (a report below the
  window is ignored); **hash verification:** when `e = r.number` there MUST be a stored
  block at height `e` with hash equal to `r.hash` — a report naming a height that is a
  hole in the stored chain (slot-numbered chains, DEF-1) contradicts the stored chain
  exactly like a hash mismatch and MUST be treated as a source integrity fault
  (FM-SRC-5, WP-8); when clamped (`e < r.number`), the stored head is taken as finalized
  on the strength of the source's claim about its descendant.
- Monotonicity: if `fin ≠ ⊥ ∧ e < fin.number` → ignore (no transition). If
  `e = fin.number` with a different hash → source integrity fault (FM-SRC-5), no
  transition.
- Post: `fin' = ⟨e, stored hash at e⟩`.

**WP-8** A finality report whose hash contradicts the stored block at the same height
(either `fin` itself or the block at `e`) MUST NOT be applied and MUST raise an integrity
alarm — silently dropping it hides either a source fault or a wrong stored chain.

### 2.5 RETAIN(from, h?)

Trims history below `from`; the inverse direction (lowering `from`) is not a transition —
history cannot be re-acquired through RETAIN.

- Case `from = first(D)` (or `seg = ∅` and `from ≤ anchor.number + 1`): no-op (except
  optional anchor-hash validation per WP-9).
- Case `from < first(D)` — the bound moves *down*: history below the window cannot be
  re-acquired in place (there is no downward backfill), so in self-healing modes
  (`Window`, `External` runtime application) the instruction MUST be executed as
  `RESET(⟨from − 1, h?⟩)`: the window is discarded and re-ingested from `from` upward.
  Like every RESET this MUST be observable (OB-9) — a downward `SET-RETENTION` is a
  destructive re-bootstrap, not a widening, and controllers MUST treat it as such
  (FM-OP-4). During boot validation of a `Pinned` policy whose `from` lies below the
  stored window, the service MUST refuse the dataset (INV-43) rather than destroy data —
  symmetric with WP-9. *Finality note:* like every RESET (§2.6) and like upward trims
  (RS-2), this discards the finalized prefix and clears `fin` — retention dominates
  finality. It is **not** a rollback below `fin`: the fork floor (INV-13/14) is
  untouched — *sources* can never replace anything at or below `fin` — and this path is
  reachable only through an explicit retention instruction (operator-class actor,
  FM-OP-4), never through source input.
- Case `first(D) < from ≤ next(D)`:
  - Post: `seg' = seg[from‥]`; `anchor' = ⟨from − 1, hash of from's preceding block in
    the pre-trim state⟩` (= `hash_at(from − 1)`, DEF-16). The hash MUST be carried over
    from previously stored data, never recomputed or dropped (INV-18);
  - `fin' = fin` if `fin.number ≥ from`, else `fin' = ⊥` (retention dominates finality,
    RS-2).
- Case `from > next(D)`: equivalent to `RESET(⟨from − 1, h?⟩)` — the whole window is
  discarded and the dataset awaits future blocks.
- **WP-9 (Mismatch policy).** If `h?` is provided and disagrees with the stored chain's
  `hash_at(from − 1)` (DEF-16): in self-healing modes (`Window`, `External` runtime
  application) the service MUST `RESET(⟨from − 1, h?⟩)`; during boot validation of a
  `Pinned` policy it MUST refuse the dataset (INV-43) rather than destroy data.
- **WP-10 (Trigger, `Window(k)`).** After every commit that advances `next(D)`, if
  `next(D) − first(D) > k` (`k` counts positions — DEF-9) the service MUST trim such that
  the availability floor RS-3
  (at least the last `k` blocks) always holds and the excess bound RS-4
  (`first(D) ≥ next(D) − k − P-RETENTION-SLACK`, eventually) is met. Trimming MAY be
  batch-granular.
- **WP-11 (External application).** A SET-RETENTION instruction accepted through the API
  MUST be applied (the corresponding RETAIN committed) within `P-RETENTION-APPLY`, and be
  observable via the retention read afterwards. Acceptance and application are distinct
  points: the acknowledgement means the instruction is recorded and scheduled, not that
  the trim has happened. Between the two, GET-RETENTION reports the *instructed* bound —
  an acknowledged instruction is never silently forgotten (CN-9; violated today, GAP-28) —
  and `ver` advances with the RETAIN/RESET commit itself, not at acceptance. What
  instruction payloads other than a block bound mean for an External dataset (policy-mode
  changes, e.g. the binding's `"None"`) is unspecified, and the current behavior diverges
  from the binding's reading (GAP-35).

### 2.6 RESET(a)

- Post: `seg' = ∅`, `anchor' = a`, `fin' = ⊥`.
- RESET is the only self-healing transition that discards the volatile *and* finalized
  parts of a window together. It MUST be committed atomically, MUST be surfaced as an
  event/observable (OB-9) — never silent — and MUST only occur through the defined
  triggers: WP-6b (fork deeper than the window), WP-9 (retention mismatch), a downward
  retention bound (§2.5), operator action (INV-44).

### 2.7 DROP

- Removes the dataset and all stored data. Triggered by operator/configuration only
  (INV-44). Space follows RS-5/RS-6 (logical removal immediate, physical reclamation
  deferred). A DROP followed by CREATE of the same identity MUST behave as a fresh dataset.

## 3. Commit and acknowledgement

- **WP-12 (Commit point).** A transition is *committed* when a read issued afterwards can
  observe it. All invariant evaluation, version numbering, and watermark reporting refer
  to commit points.
- **WP-13 (No partial blocks).** No reader may ever observe a block with a subset of its
  items, a batch with a subset of its blocks, or metadata referring to absent payloads
  (INV-10). This MUST hold across crashes (INV-40).
- **WP-14 (Write acknowledgement to sources).** The only acknowledgement a source receives
  is the position of the next request (WP-1). Therefore the service MUST tolerate
  redelivery of anything not yet durable, and deduplicate blocks below `next(D)`
  (**WP-16, idempotency**: redelivery of already-stored blocks causes no state change and
  no error).
- **WP-15 (Single writer).** At most one writer per dataset store may exist. If a second
  concurrent controller of the same dataset is detected (state changed under the writer's
  feet), the writer MUST stop mutating that dataset and raise an alarm (FM-OP-3) — both
  writers continuing is forbidden.
- **WP-20 (Derived index maintenance).** A derived index (DEF-17) is maintained *inside*
  the transition that changes what it names, never by a follow-up pass: entries appear and
  disappear in the same commit as their blocks (INV-46). A reader therefore can never
  observe an index that disagrees with the window it was read from, and a crash can never
  land between a block and its entry. Enabling or disabling an index is a deployment
  change, not a transition: it decides whether *newly* ingested blocks are named and never
  rewrites committed history — which is precisely why the indexes are partial (DEF-17).
  Removal is unconditional: entries are dropped with their blocks whether or not the index
  is currently enabled, so a disabled index drains within one retention period rather than
  rotting into stale entries.

## 4. Ingestion error handling

- **WP-17 (Transient vs integrity).** Failures MUST be classified: *transient* (source
  unreachable, timeouts, storage temporarily saturated) → retry with bounded backoff
  (`P-SOURCE-BACKOFF`, `P-EPOCH-RETRY`), unbounded in count; *integrity* (finality
  contradiction, unremovable validation failure, config/state mismatch) → stop retrying
  blindly, surface a distinct alarm state within `P-ALARM` (LIV-9, OB-9), keep serving
  reads.
- **WP-18 (Robustness).** No content of any source event — arbitrary bytes, sizes,
  encodings, value ranges — may terminate the process, corrupt other datasets, or wedge
  the write path permanently (FM-1). Malformed events are rejected per WP-2/WP-17.
- **WP-19 (Restart continuity).** After any restart, ingestion MUST resume from the
  recovered committed state per WP-1 — including the recovered anchor hash being exactly
  the committed one (INV-40); resuming from a wrong expected hash manifests as a
  perpetual source rejection loop and is a defect, not a source fault.
