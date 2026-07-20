# 12 — Conformance and TDD framework

This document turns the spec into a test program: the reference model (oracle), the
harness architecture, the test-class taxonomy, the traceability matrix, and the dated gap
register that seeds the hardening backlog.

Statuses and the gap register reflect the state of knowledge as of **2026-07-12** and are
expected to change; everything else in this document is stable methodology.

The harness described here exists: [`crates/hotblocks-harness`](../../hotblocks-harness).
Phase 0 of §7 is done — CT-1 runs a happy-path script green against the real binary.

## 1. Harness architecture

```
┌────────────────┐   source protocol   ┌─────────────┐   interface binding   ┌───────────────┐
│ Source         │────────────────────▶│             │◀──────────────────────│ Client driver │
│ simulator      │  blocks/forks/      │  SUT        │   queries/watermarks/ │ + validators  │
│ (scripted      │  finality/faults    │  (black     │   retention           │               │
│  chain gen)    │                     │   box)      │                       └───────┬───────┘
└──────┬─────────┘                     └──────┬──────┘                               │
       │                                      │ observability (OB-*)                │
       │            ┌─────────────┐           ▼                                      │
       └───────────▶│ Reference   │◀── scraper ─┘                                    │
        same script │ model       │◀───────────────── responses/verdicts ────────────┘
                    │ (oracle)    │
                    └─────────────┘
   + fault injectors: process kill/restart, storage throttle/fill, network faults
```

Components:

1. **Source simulator** — a deterministic, scriptable chain generator implementing the
   source side of the binding: block production at `W-BLOCK-RATE`, forks per `W-REORG`,
   finality per `W-FINALITY-LAG`, plus the full FM-SRC fault repertoire on demand. It
   keeps a **ledger** of everything it served (for INV-7 provenance checks). Payload
   generation is kind-parametric with adversarial knobs (sizes, encodings, densities).
2. **Client driver** — implements the client algorithms of 04 §7 (anchored follower,
   backfill scanner, watermark poller) and the structural validators (§6 below).
3. **Reference model** — §2. Fed the *same* script the simulator executes plus the
   accepted-commit observations, it predicts every observable.
4. **Fault injectors** — process kill at scheduled/random points (CT-2), storage-level
   throttling and disk-fill (CT-7), connection faults.
5. **Comparator** — at **quiescence** (no pending input and OB-1/OB-2 stable for
   `P-QUIESCENCE`), diff SUT observables against the model: STATUS/HEAD values, full-window
   scan results, query results over sampled ranges/selections.

Determinism rules: the harness controls all inputs and timestamps; SUT-side
nondeterminism (batch boundaries, coverage cuts) is absorbed by comparing at quiescence
and by INV-22's determinism-modulo-coverage.

## 2. Reference model (normative pseudocode)

One instance per dataset. This is the oracle for every CT class; ~everything else in the
harness is plumbing around it.

Lookups and slices below are by *block number*, and numbering may be sparse (DEF-1):
`block_at(n)` is the stored block numbered exactly `n` (`⊥` at a hole); `hash_at(p)` is
the chain hash at a position (DEF-16).

```
model Dataset:
  kind; ret
  seg    : list<Block> = []          # ascending, parent-linked; numbers may be sparse
  anchor : (number, hash|⊥)
  fin    : Ref|⊥ = ⊥
  ver    : int = 0

  first() = seg[0].number if seg else ⊥
  head()  = ref(seg[-1])   if seg else ⊥
  next()  = seg[-1].number + 1 if seg else anchor.number + 1   # lowest admissible position

  block_at(n) = the b in seg with b.number == n, else ⊥
  hash_at(p)  = b.hash for the highest b in seg with b.number <= p,
                else anchor.hash if p >= anchor.number else undefined   # DEF-16

  bidx(h)     = b.number for the b in seg with b.hash == h, else ⊥      # DEF-17
  tidx(h)     = (b.number, i) for the i-th tx of the b in seg
                with tx.hash == h, else ⊥

  wf():   # well-formedness — assert after every transition (INV-1..6)
    assert ascending_and_linked(seg)                # INV-1/2: parent_number + parent_hash
    assert anchor.number < first()                  if seg
    assert seg[0].parent_hash == anchor.hash        if seg and anchor.hash != ⊥
    assert fin == ⊥ or (seg and first() <= fin.number <= head().number
                        and block_at(fin.number).hash == fin.hash)

  extend(B, f=⊥):                      # WP §2.2
    require B and valid_run(B)                      # WP-2: ascending + linked
    require B[0].number >= next()
    require B[0].parent_hash == (head().hash if seg else anchor.hash) or anchor.hash == ⊥ and not seg
    require B[0].parent_number == head().number     if seg
    seg += B; ver += 1; if f: finalize_inline(f)
    wf()

  replace(from_, B, f=⊥):              # WP §2.3
    require seg and B and valid_run(B) and B[0].number >= from_
    require fin == ⊥ or from_ > fin.number          # INV-13/14
    require from_ >= first()                        # INV-14
    require B[0].parent_hash == hash_at(from_ - 1)  # DEF-16 (⊥ accepted at the window edge)
    seg = [b in seg | b.number < from_] + B; ver += 1; if f: finalize_inline(f)
    wf()

  finalize(r):                         # WP §2.4
    if not seg: return IGNORED
    e = min(r.number, head().number)
    if e < first(): return IGNORED
    if fin != ⊥ and e < fin.number: return IGNORED
    if e == r.number and block_at(e) == ⊥: return INTEGRITY_FAULT   # hole = chain mismatch
    if e == r.number and block_at(e).hash != r.hash: return INTEGRITY_FAULT   # WP-8 (alarm)
    if fin != ⊥ and e == fin.number and block_at(e).hash != fin.hash: return INTEGRITY_FAULT
    fin = (e, block_at(e).hash); ver += 1; wf()     # block_at(e) exists on every accepting path

  retain(from_, h=⊥):                  # WP §2.5
    if from_ == (first() if seg else anchor.number + 1) or (not seg and from_ <= anchor.number + 1):
        return validate_anchor(h)                    # no-op / WP-9 policy
    if seg and from_ < first():
        return reset((from_ - 1, h))                 # downward bound = destructive
                                                     # re-bootstrap (§2.5); alarmed (OB-9)
    if seg and from_ <= next():
        if h != ⊥ and h != hash_at(from_ - 1): return reset((from_ - 1, h))   # WP-9 self-heal
        new_anchor = (from_ - 1, hash_at(from_ - 1))                          # INV-18 (DEF-16)
        seg = [b in seg | b.number >= from_]; anchor = new_anchor
        if fin != ⊥ and fin.number < from_: fin = ⊥                           # RS-2
        ver += 1; wf()
    else:
        reset((from_ - 1, h))                        # from_ > next()

  reset(a):                            # WP §2.6 — must be alarmed/observable (OB-9)
    seg = []; anchor = a; fin = ⊥; ver += 1; wf()

  resolve_fork(hints):                 # WP-6/WP-6b — REPLACE position, RESET, or fault
    require hints ascending, non-empty
    if fin != ⊥:
        hints = [x for x in hints if x.number >= fin.number]
        if not hints: return INTEGRITY_FAULT                       # below finality
        if hints[0].number == fin.number and hints[0].hash != fin.hash: return INTEGRITY_FAULT
    if anchor.hash != ⊥ and any(x.number == anchor.number and x.hash != anchor.hash for x in hints):
        return RESET((anchor.number, hint hash at that position))  # WP-6b: below-window divergence
    m = max({x in hints | stored_or_anchor(x)}, default=⊥)
    if m != ⊥: return (m.number + 1, m.hash)
    if fin != ⊥: return (fin.number + 1, fin.hash)   # volatile suffix only; repeated
                                                     # rejection here = FM-SRC-5, never RESET
    return (first(), anchor.hash)                    # full-window replacement *probe*;
                                                     # repeated WP-2 rejection at first(D)
                                                     # escalates to RESET per WP-6b

  query(q):                            # 04 — evaluated on a copy = snapshot (INV-20)
    if q.dialect != kind: return KIND_MISMATCH
    hi = fin.number if q.finalized_only else (head().number if seg else ⊥)
    if q.to != ⊥ and q.to < q.from: return MALFORMED_REQUEST
    if seg and q.from < first(): return RANGE_UNAVAILABLE
    T = fin if q.finalized_only else head()                          # RP-5b tip anchor
    if q.expected_parent != ⊥ and T != ⊥ and q.from == T.number + 1 and q.expected_parent != T.hash:
        return CONFLICT(hints = chain_suffix_ending_at(q.from - 1, P_CONFLICT_WINDOW))
        # RP-5b: the assertion references the watermark block's exact position —
        # evaluable and definite; MUST precede the NO_DATA / long-poll path
    if hi == ⊥ or q.from > hi: return NO_DATA                       # after RP-5 wait
    if q.expected_parent != ⊥ and hash_at(q.from - 1) not in {⊥, q.expected_parent}:
        return CONFLICT(hints = chain_suffix_ending_at(q.from - 1, P_CONFLICT_WINDOW))
        # hints are ⟨p, hash_at(p)⟩ entries (RP-11, DEF-16)
    if selection_unavailable_at(q, q.from): return ITEM_UNAVAILABLE # RP-8: range *start* only
    L = coverage_end_chosen_by_SUT(q, hi)            # free variable: from <= L <= min(q.to or hi, hi),
                                                     # never crossing an availability boundary (RP-8)
    emitted = [render(b, q) for b in seg
               if q.from <= b.number <= L and (matches(b, q) or q.include_all)]
    end = highest b in seg with b.number <= L                        # RP-9 carrier
    if end != ⊥ and end not in emitted: emitted += [header_only(end)]
    # the SUT MAY add further header-only boundary markers of covered blocks;
    # layout-dependent, exempt from the INV-22 determinism check
    return OK(emitted, L, watermarks = (head(), fin))
```

Two **free variables** are granted to the SUT: `L` (coverage end) and the set of extra
header-only boundary markers (RP-9/INV-22). The comparator checks every response for: a
valid `L`; matching blocks and their item content equal to the model evaluated *at that
`L`*; the coverage-end record present; every extra record being a header-only true block
of the snapshot inside coverage. Everything else must match exactly.

`bidx`/`tidx` are the one place the model is **stronger than the contract**, and the
comparator must not confuse the two. The model is always complete over `seg`; the SUT is
only required to be *sound* (RP-19), because an index is not backfilled and may be disabled.
So the comparator asserts, unconditionally:

- every SUT **hit** matches the model exactly (a hit for a hash the model does not hold, or
  at a position the model disagrees with, is a hard failure — INV-45);
- every hash the model has **dropped** (forked away, trimmed, dropped) resolves to "none"
  (INV-46) — this is the assertion that catches a stale index, and it is the one that
  matters;
- a transaction re-included by a fork resolves to its **new** position (INV-47).

Full completeness — *every* block of `seg` resolves — may be asserted only by runs that
guarantee the preconditions themselves: a fresh store, the index enabled for the whole run,
and an indexed kind. Every script today satisfies all three, so
`Harness::assert_hash_index_conforms` asserts completeness legitimately; the moment a script
restarts the SUT with the flag flipped, or seeds a pre-existing store, that assertion must
weaken to soundness or it will fail on correct behavior.

## 3. Test-class taxonomy

| CT | Name | Method | Primary properties |
|---|---|---|---|
| CT-1 | **Stateful property tests** | randomized scripts of source events + retention ops + reads; model diff continuously and at quiescence | INV-1..7, 10..18, 20..27, 30/31, 44; WP-*; RP-* |
| CT-2 | **Crash-recovery** | kill-point matrix (during batch write, during fork, during trim, during boot, during shutdown) × restart → model diff; repeated-crash convergence | INV-40, 42, 43; CN-6/9/11; LIV-5/6/12; GAP-2 |
| CT-3 | **Concurrency** | reader swarms hammering during write/fork/trim/maintenance storms; interleaved HEAD+QUERY sequencing checks | INV-20/21/23/31/41; CN-3/4; LIV-3/4 |
| CT-4 | **Source-fault corpus** | scripted FM-SRC-1..8 scenarios incl. fork storms, deep forks, finality conflicts, equivocation | INV-12/13/14/23/24; WP-6/8; LIV-9; FM-SRC-*; GAP-3/4/5 |
| CT-5 | **Interface conformance** | exhaustive request/response matrix against the binding: error taxonomy, watermark headers, encodings, hash-lookup matrix, boot config matrix | RP-1..16, 19/20; INV-26/43; IB-*; GAP-8/9/39 |
| CT-6 | **Performance benchmarks** | reference scenarios S1–S6; SLI capture; SLO gates; saturation knees | SLI-1..12; PF-1..9; LIV-1/3/10; GAP-13 |
| CT-7 | **Soak / endurance** | multi-day S4 churn with fault sprinkling; space, memory, stall, residue tracking | LIV-2/7/11; RS-6/10; INV-16/17; HZ-2/5; GAP-1/6 |
| CT-8 | **Isolation / noisy neighbor** | S6: one dataset saturated/faulted, others measured differentially | INV-35/36; LIV-8; PF-4; GAP-14 |
| CT-9 | **Fuzzing** | source-side payload fuzz (write path) + client-side request fuzz (read path); crash/hang/leak oracles + invariant spot checks | FM-1; WP-18; RP-1/2; GAP-12 |

Every test cites the IDs it verifies; CI reports coverage as "properties exercised", not
lines.

## 4. Kind-agnostic structural validators (client driver)

Applicable to any response without understanding kind semantics — the cheap 80% of
INV-21/22/23 (checks in parentheses):

1. decodable stream, one block object per record (IB-4);
2. strictly ascending, unique block numbers (INV-21);
3. header projection includes number/hash/parent-hash when requested → linkage verifiable
   across records and across responses (INV-2/23);
4. every item's block number equals its enclosing block's (INV-21);
5. blocks within `[from, min(to, reported head)]` (INV-27);
6. anchored continuation across responses never breaks parent-hash chains (INV-23);
7. watermark coherence: `first ≤ fin ≤ head` whenever reported together (INV-5/30).

## 5. Traceability matrix (status @ 2026-07-15)

Legend: **C** covered, **P** partial (some storage-layer or fixture coverage exists;
service-level black-box coverage absent), **U** untested. Rows that changed with Phase 0 name
the test that moved them; unless a row says otherwise, "covered" means *on the happy path* —
the same property under forks, crashes and retention is the business of CT-2/CT-4.

| Property | CT class | Status | Note |
|---|---|---|---|
| INV-1..3 structural chain | CT-1 | **C** | `ct1_evm` / `ct1_solana` / `ct1_hyperliquid_fills`: full-window scan, parent linkage + anchor, dense and slot-numbered |
| INV-4 kind/schema | CT-1/5 | **C** for evm/solana/hyperliquid-fills | payload round-trips against the emission oracle per kind; bitcoin, tron and hl-replica-cmds unmodeled |
| INV-5/6 watermark bounds/on-chain | CT-1 | **P — known-violated** | happy path C; finality below the window is accepted after a trim clears `fin` (GAP-27); hash below head unverified (GAP-4) |
| INV-7 provenance | CT-1/6 | **C** | `ct1_happy_path`: what the source served is read back, payload included |
| INV-10 atomic transitions | CT-2/3 | U | |
| INV-11 append | CT-1 | **C** | |
| INV-12/13 finality monotone/immutable | CT-1/4 | **P — known-violated** | monotone advance observed; composed-finality REPLACE below `fin` admitted (GAP-22); regression/conflict paths await CT-4 |
| INV-14 fork floor | CT-4 | **U — known-violated** | GAP-3; composed-finality bypass (GAP-22) |
| INV-15/18 retention trim/anchor | CT-1 | **U — known-violated** | INV-18: trims drop the anchor hash (GAP-23), restart rebuilds it wrong (GAP-2); comparator needs RS-4 slack first (see §7) |
| INV-16 frame | CT-1/7 | U | |
| INV-17 maintenance transparency | CT-7 | P | merge-equivalence tested storage-level |
| INV-20 snapshot isolation | CT-3 | P | single-threaded snapshot test only |
| INV-21/22 response shape/completeness | CT-1/5/6 | P | structural validators + emission diff under `include_all`; coverage cuts and filtered emission await CT-5; comparator must implement the RP-9 marker exemption (spec change 2026-07-12) |
| INV-23 anchored ancestry | CT-1/4 | P | anchored continuation across responses covered; the CONFLICT path awaits CT-4 |
| INV-24 finalized-only | CT-4 | U | |
| INV-25 progress | CT-1/6 | P | a successful response must cover ≥ 1 block — asserted by the scanner |
| INV-26 error soundness | CT-5 | **P — known-violated** | `ct5_error_soundness`: unsupported dialect containment/accounting and mid-stream worker-panic abort pinned; finalized-snapshot race pinned unit-level. Anchored eval across large holes (GAP-21) reverted; shared-status families keep free-text discrimination (GAP-36/39) |
| INV-27 range honesty | CT-1 | **C** | validator: no block outside `[from, min(to, head)]` |
| INV-30/31 reporting | CT-1/3 | P | INV-30 asserted at quiescence; INV-31 (real-time monotonicity) untested |
| INV-35/36 isolation | CT-8 | U | all existing tests are single-dataset |
| INV-40 recovery | CT-2 | **U — known-suspect** | GAP-2, GAP-28; restart primitives exist (`Sut::restart`), the kill-point matrix does not |
| INV-41 reclamation invisibility | CT-3/7 | P | snapshot-safety + reader-break tests exist storage-level |
| INV-42 residue convergence | CT-2/7 | P | synthetic orphan purge tested; no crash-driven test |
| INV-43 boot validation | CT-5 | U | |
| INV-44 explicit destruction | CT-1/2 | U | |
| INV-45 index soundness | CT-1/2 | **P** | `block_hash_index.rs` + `transaction_hash_index.rs`: both indexes match the model on ingest and replacement; storage tests cover trim / DROP / compaction. Crash coverage remains open |
| INV-46 index maintenance | CT-1/4/7 | **P** | both fork paths are covered black-box; trim / DROP / compaction are storage-level only (`crates/storage/tests/{block,transaction}_hash_index.rs`) |
| INV-47 fork re-inclusion | CT-4 | **C** | black-box reorg re-includes the same transaction at a new block and asserts the new position; storage test independently pins remove-before-insert ordering |
| RP-19/20 lookup contract | CT-5 | **P** | hit/miss, disabled-index behavior, over-limit rejection before dataset lookup, and the 256-byte boundary are pinned; the NOT_FOUND vs UNKNOWN_DATASET split remains open (GAP-39) |
| LIV-1/2 progress/stall | CT-6/7 | **U — known-violated** | GAP-1 |
| LIV-3 query termination | CT-3/6 | U | |
| LIV-4 waiter termination | CT-1/3 | P | `ct1_happy_path`: a query above the head answers `NO_DATA` within `P-HEAD-WAIT` |
| LIV-5/6 startup/recovery | CT-2/6 | **U — known-violated** | GAP-7; `Sut::last_startup` records SLI-5 per boot |
| LIV-7 reclamation | CT-7 | U | runtime reclaim on by default since 2026-07 (GAP-6 residual: boot-gated residue purge); convergence unmeasured |
| LIV-8 isolation | CT-8 | **U — known-violated** | GAP-1/14 |
| LIV-9 fork convergence/alarm | CT-4 | **U — known-suspect** | GAP-5 |
| LIV-10 overload recovery | CT-6 | U | |
| LIV-11 retention keep-up | CT-7 | U | |
| LIV-12 shutdown | CT-2 | **U — known-suspect** | GAP-17; `Sut::stop` measures the drain |
| RS-3/4 window floor/excess | CT-1/7 | U | |
| RS-6 amplification | CT-7 | U | reclaim path fixed 2026-07 (GAP-6); bound unmeasured under churn |
| RS-8 boot maintenance | CT-2/7 | P | unlink/orphan-purge behaviors have storage-level tests |
| RS-10/11 residue/deletion cost | CT-7 | P | GAP-6/13 |
| FM-1 robustness | CT-9 | **P — known-violated** | GAP-12 open; unsupported query and query-worker panic classes are closed (§6.1), and the unterminated-record class is pinned by `ct9_source_faults` |
| FM-SRC-* corpus | CT-4 | U | one stale-pack crash-loop already occurred (GAP-5 class); no strike/quarantine substrate (GAP-30) |
| FM-STOR-2/3 disk pressure | CT-7 | U | incident-derived; no automated test |
| FM-OP-1..5 | CT-5 | U | |
| SLI-1..12 / PF-* | CT-6 | U | no scenario benchmark harness exists; the transaction-index ingest/lookup Criterion microbench is a component baseline only |
| OB-1 chain gauges | CT-1 | P | `first_block` / `last_block` / `last_finalized_block` diffed against the model; commit version and retention policy not exported (GAP-34) |
| OB-2..11 | all | P | query metrics exist; stall gauges pending on PR #83 (unmerged); OB-2 heartbeat, OB-6 debt accounting, OB-9 alarms, OB-11 forensics absent |
| OB-12 index state | CT-1 | **P** | CF-wide estimated keys / live SST bytes are exported for both indexes; per-dataset enabled/count/bytes and lookup hit/miss/latency remain absent (GAP-40) |

## 6. Gap register (dated 2026-07-15, informative)

Known or strongly suspected divergences between this spec and the current system, from
incident history, code-level review, and coverage analysis. Priorities: P0 = active
production risk, P1 = correctness/robustness hole with plausible trigger, P2 = bounded or
rare, P3 = polish. **First test** names the cheapest failing-test-first entry point.

| GAP | Statement | Violates | Prio | First test |
|---|---|---|---|---|
| GAP-1 | Whole-service ingest freezes (≈6 min) observed post-deploy; all datasets stall simultaneously; root cause unconfirmed (shared write-path backpressure suspected); no stall observability to attribute it | LIV-2, LIV-8, OB-3/11 | **P0** | CT-7 stall harness: S1 + storage-pressure injection, assert SLI-9 ≤ budget; build OB-11 capture first |
| GAP-2 | Recovered anchor hash is reconstructed from the wrong value after restart — `WriteController::new` takes the first batch's *last*-block hash where the correct value sits in its `parent_block_hash` field; latent until the fork-fallback path consumes it, then ingestion resumes with a wrong expected parent (perpetual source-rejection loop) | INV-40, WP-19, LIV-6 | P1 | CT-2: restart, then force full-window fork fallback; assert resume position equals model |
| GAP-3 | Fork floor at the window start is not enforced (marked-as-known in the system); a deeper-than-window divergence may be mishandled instead of becoming an explicit RESET per WP-6b | INV-14, WP-6/6b | P1 | CT-4 deep-fork case: hints strictly below `first(D)` |
| GAP-4 | Finality reports strictly below the head are applied without verifying the hash against the stored block — in both the standalone FINALIZE path and the batch-composed path; the window floor is not checked either (GAP-27) | INV-6, WP §2.4 | P2 | CT-4: finality with corrupted hash below head; assert INTEGRITY_FAULT not acceptance |
| GAP-5 | Unapplicable divergence (fork below finality, finality conflicts) results in silent bounded-pause retry forever — no alarm state, no distinct observable; one such class already caused a crash-loop incident | LIV-9b, FM-SRC-5, OB-9 | P1 | CT-4: fork-below-finality script; assert alarmed state within `P-ALARM` while reads keep serving |
| GAP-6 | ~~Default deployments never reclaim~~ — routine reclaim fixed 2026-07 (PR #79: 10 s point-delete sweep + deletion-collector + periodic-compaction backstop). REMAINING: the interrupted-build residue purge and the whole-file unlink are confined to the gated boot mode (off by default) — a torn build's residue leaks for good in default config and pins the boot-unlink watermark; SLI-8 under churn still unmeasured | RS-10, RS-8 (RS-6 residual) | P2 (was P0) | CT-7: churn soak in default config; assert SLI-8 bound + residue-age bound |
| GAP-7 | Serving is gated on full initialization: tens of seconds of refused connections after deploy, scaling with state size and dataset count; readiness not observable per dataset | LIV-5, OB-8 | P1 | CT-6 S5: SLI-5 vs state-size regression curve |
| GAP-8 | ~~Zero-emission responses do not convey the coverage end~~ — a de-facto carrier existed all along (the coverage-end block is always emitted, header-only when unmatched) and was adopted as normative RP-9 on 2026-07-12. REMAINING: (a) a zero-emission success now *asserts* full-range coverage, but under time-budget truncation over a blockless range (explicit `to` inside a hole run) the implementation can return an empty 200 having covered only part of it — the client then silently skips the rest; (b) the comparator must implement the INV-22 boundary-marker exemption; (c) in one reachable corner RP-9's clauses are jointly unsatisfiable — an effective range whose covered part contains no stored block and whose coverage cannot legally reach the range end (an availability boundary, RP-8, or a hard `P-QUERY-TIME` stop inside a long hole run): zero emission is legal only at full coverage, and the carrier (highest stored block ≤ `L`) lies below `from`, which INV-27 forbids emitting — here the *spec*, not just the implementation, owes an answer (candidate remedies: an explicit in-band terminal coverage record — a wire change — or forbidding coverage to end inside a hole except at the range end) | RP-9, INV-22, RP-8, INV-27 | P2 | CT-5: hole-range query with explicit `to` + tight budget; assert empty-200 only with full coverage |
| GAP-9 | `NO_DATA` responses carry no watermarks (long-poll clients learn nothing about the head from a timeout). Mechanism: both construction sites of the above-head outcome hardcode "no finalized head", so the header-attaching code is unreachable | RP-5 SHOULD, IB-6 | P3 | CT-5 header assertion |
| GAP-10 | Mid-stream admission failures truncate silently and are not counted; truncation rate invisible | RP-15, OB-4 | P2 | CT-6 overload phase: assert truncation counter ≥ observed truncations |
| GAP-12 | At least one payload-content class (invalid text encoding in stats-tracked fields) panics the write path; payload fuzz has never been run | FM-1, WP-18 | P1 | CT-9 source-payload fuzz with crash oracle |
| GAP-13 | Ingest batch accumulation has content-dependent unbounded memory (no hard byte ceiling on some structures) | PF-1 | P2 | CT-6 adversarial `W-BLOCK-SIZE`/`W-ITEM-DENSITY`; RSS ceiling assertion |
| GAP-14 | Read-side capacity (execution slots, waiter slots) is a single global pool: one dataset's query herd can starve all datasets | PF-4, LIV-8 | P2 | CT-8: herd on D′, tip-follower SLOs on D |
| GAP-15 | No explicit store-format compatibility gate at boot; incompatibility surfaces as runtime decode errors | CN-12, INV-43 | P3 | CT-5 boot matrix with future-format fixture |
| GAP-16 | ~~The service layer has essentially zero automated tests~~ — **closed by Phase 0**, see §6.1 | all | — | done |
| GAP-17 | Shutdown can take a panic-class exit path in ingestion cancellation (observed at redeploy) | LIV-12, FM-PROC-4 | P2 | CT-2 shutdown class: SIGTERM under load ×100, zero panic exits |
| GAP-18 | Dual-writer detection exists only on some paths (finality/head updates), not all mutations | WP-15, FM-OP-3 | P3 | CT-5: two harness-driven writers, assert loser stops on every mutation type |
| GAP-20 | `parent_number` linkage is never validated on any layer (the block trait exposes it; nothing reads it): a hash-linked run can claim an arbitrarily higher number for the next block, storing a false hole on a densely-numbered chain — a silent data gap served as if it were a slot gap. (The originally-filed non-monotonic-numbers scenario is unreachable today: the source-position advance forces ascending numbers.) | WP-2, DEF-4, INV-1 | P2 | CT-4/CT-9: hash-linked run with a number jump on a dense chain; the run MUST be rejected with no state change |
| GAP-21 | An anchored query whose `from` sits mid-chunk above a number gap larger than the conflict-check lookback (a hard-coded 100 positions in the plan's base-block check) fails `INTERNAL` instead of evaluating the assertion. A >100-position hole with an anchor landing just above it is probably unrealistic, hence low priority. A correct all-predecessors scan was tried and reverted 2026-07-15 — it regressed `check_parent_block` into an unbounded per-chunk scan+sort; needs a lazy sort-desc + limit(100) | RP-11, INV-26 | P3 | CT-5 `ct5_anchor_is_evaluated_across_a_large_number_hole` (`#[ignore]` until fixed): >100-position hole in one chunk; anchored query just above must yield OK/CONFLICT, never 500 |
| GAP-22 | Deep-fork handling can silently replace the finalized prefix: the fork-resolution fallback ignores `fin` (resumes from the window start instead of `⟨fin + 1, fin.hash⟩`), the composed-finality guard admits a REPLACE whose base lies at/below `fin` whenever the pack carries a finality mark ≥ current, and Window trims have dropped the anchor hash (GAP-23) so the replacement attaches unchecked. If the first replacement batch reaches past the old `fin`, the finalized prefix is replaced with no RESET event and no alarm — finalized-only clients observe two hashes at one height (INV-24 broken); otherwise the commit trips the fork-floor check ("can't fork safely") and the epoch parks on the blind 60 s retry loop. Fix: fallback → `fin + 1`; enforce the fork floor at commit unconditionally; carry the anchor hash | WP-6, INV-13/14, INV-24, FM-SRC-5, LIV-9 | **P1** | CT-4: fork with all-mismatching hints on a dataset with `fin` defined; assert REPLACE from `fin + 1` (or alarmed fault) — never a commit whose base ≤ `fin` |
| GAP-23 | Window trims drop the anchor hash: the automatic trim passes no hash and the retained state stores `⊥`, though the correct value sits unused in the first batch's `parent_block_hash`. Disables below-window divergence detection (WP-6b has nothing to contradict) and feeds GAP-22 | INV-18, DEF-7, WP-6b | P1 | CT-1: CONFLICT hints / STATUS at the window edge after a trim; CT-4: below-window fork after a trim must RESET, not absorb silently |
| GAP-24 | One dataset's init failure aborts the whole service: startup propagates the first controller error (kind mismatch, retention bail, corrupt state) instead of alarming that dataset and serving the rest | CN-10, FM-OP-1, INV-36, INV-43 | P1 | CT-5 boot matrix: corrupt one dataset's persisted state; assert the others serve and the broken one alarms |
| GAP-25 | Downward retention (`from < first(D)`) executes as an *implicit, unobservable* RESET (WP §2.5 as amended 2026-07-12 legalizes the destruction, but requires OB-9 observability) — no event, indistinguishable from a trim; and the boot-time `Pinned` equivalent aborts the entire service (via GAP-24) instead of a dataset-level refusal | WP §2.5, OB-9, INV-43 | P2 | CT-1: SET-RETENTION below `first`; assert a RESET observable + serving continuity; CT-5: boot with lowered `Pinned.from` |
| GAP-27 | FINALIZE never checks `e ≥ first(D)`: with `fin = ⊥` (e.g. after a trim passed above it) a lagging source's report below the window commits `fin < first(D)` | WP §2.4, INV-5 | P2 | CT-4: finality below the window on a trimmed dataset; assert the report is ignored |
| GAP-28 | The External retention instruction and the empty-dataset anchor are memory-only (the persisted label holds kind/version/fin) — a restart forgets the instructed bound (the dataset re-idles awaiting a new instruction) and resets an empty dataset's anchor | CN-9, INV-40, WP-11 | P2 | CT-2: SET-RETENTION, restart; assert the bound and anchor are recovered |
| GAP-29 | A stalled-but-open connection pins the response's storage snapshot indefinitely (no overall response deadline / server write timeout; only disconnects release it); pinned snapshots block physical reclaim of point-deleted data | CN-7, RP-18, HZ-9, RS-6 | P2 | CT-3/CT-7: zombie client that stops reading; assert snapshot lifetime ≤ `P-QUERY-TIME` and reclaim proceeds |
| GAP-30 | No strike counting exists anywhere: a linkage-mismatch rejection resets the source session and re-requests in a zero-backoff hot loop; sources are never quarantined; cross-source equivocation never alarms. `P-SOURCE-STRIKES` has no substrate, so the WP-6b/FM-SRC-5 escalation rules are currently unimplementable | FM-SRC-3/4/6, WP-2 | P2 | CT-4: source serving a permanently mis-linked run; assert bounded request rate, quarantine after N strikes, alarm |
| GAP-31 | A finality advance landing at/above the resume position while no block arrives is deferred (standalone reports are forwarded only when below the position; suppressed until the position is source-confirmed) — the finalized head stalls although the source declared the stored chain final | WP §2.4 clamping | P3 | CT-4: finality = head during a production stall; assert `fin` advances within a bound |
| GAP-33 | The live-stream head-number header is computed from the *current* head, not the snapshot's: a head-lowering fork between snapshot and response start yields a header below blocks actually emitted (IB-6 requires ≥ the snapshot head) | IB-6, CN-5 | P3 | CT-3/CT-4: fork lowering the head during streaming; assert header ≥ snapshot head |
| GAP-34 | OB-1 partially unimplemented: the commit version (present in the persisted label) and the retention policy in force are not exported | OB-1 | P3 | CT-1: scrape assertion once exported |
| GAP-35 | The runtime External instruction `"None"` parks the dataset: the controller maps it to Idle and stops ingestion even on a non-empty dataset (`RetentionStrategy::None → State::Idle`, dataset_controller.rs), while the binding documents `"None"` as Unbounded (13 §6) and WP-5 requires a non-empty dataset to keep ingesting from its window. Whether an External instruction may change the policy *mode* at all is unspecified (WP-11) | WP-5, DEF-9, WP-11, IB §6 | P2 | CT-1/CT-5: SET-RETENTION `"None"` on a non-empty External dataset; assert ingestion continues (or the instruction is refused with a defined error) — the head must keep advancing |
| GAP-36 | `MALFORMED_REQUEST`, `RANGE_UNAVAILABLE`, `ITEM_UNAVAILABLE` and `KIND_MISMATCH` share HTTP 400 and retain free-text bodies for compatibility; no machine-readable discriminant exists and IB-7 forbids keying on text. Clients cannot distinguish "re-anchor upward" from "fix the request", and CT-5 cannot verify the taxonomy at the binding | INV-26, IB-7, 04 §8 | P2 | CT-5: choose a backward-compatible discriminant, trigger each 400 class, and assert clients can distinguish them without parsing text |
| GAP-37 | PF-1's memory ceiling is not configuration-derivable on the read side: INV-25/RP-17 require emitting the first covered block whole even above `P-RESP-WEIGHT`, and nothing bounds a single block at ingest (`P-BATCH-BYTES` is a soft *batch* bound — one oversized block still stores), so per-response memory is bounded only by the largest block a source ever served. No `P-MAX-BLOCK-BYTES` exists | PF-1, RP-17/INV-25, FM-CLI-2 | P2 | CT-6/CT-9: ingest a pathological giant block, query it; assert bounded RSS and whole-block emission (INV-25) |
| GAP-39 | A hash-lookup miss and an unknown dataset are both 404 with only a free-text body between them, and IB-7 forbids keying on text. This is worse than the GAP-36 family it belongs to: RP-19 makes "this hash is not indexed" a *deliberately uninformative* answer, so a client that cannot separate it from "this dataset does not exist" cannot tell a misconfiguration from a legitimate miss at all | INV-26, IB-7, RP-19 | P3 | CT-5: unknown dataset vs unknown hash; assert a structured discriminant |
| GAP-40 | Hash-index CFs export only engine-wide estimated keys and live SST bytes. OB-12 still lacks per-dataset enabled state / entry count / bytes and hit-vs-miss lookup counts with latency. Because a miss is uninformative by design, an index empty for a structural reason — enabled after the window had filled, wrong kind — remains indistinguishable from one receiving only unknown hashes | OB-12, OB-6 | P3 | CT-1: scrape per-dataset state and exercise hit/miss counters once exported |

### 6.1 Closed

| GAP | Statement | Closed by |
|---|---|---|
| GAP-11 | Unsupported `substrate` / `fuel` queries and query-worker panics escaped the HTTP error taxonomy by panicking their request task | Typed `UNSUPPORTED_QUERY` admission errors, panic containment in the query executor, CT-5 dialect requests, and an executor unit test (2026-07-15) |
| GAP-16 | No service-level automated tests | [`crates/hotblocks-harness`](../../hotblocks-harness) + `ct1_happy_path` (Phase 0, 2026-07-12) |
| GAP-19 | A source response whose final JSONL record carried no trailing newline panicked the line reader (`LineStream::take_final_line` left its scan position past the emptied buffer). The ingest task died, its buffered batch was lost, and the dataset parked for `P-EPOCH-RETRY` — then crash-looped, since the source served the same body on retry. Violated FM-1, LIV-2 | Found by CT-1 on the harness's first run; fixed in `crates/data-client/src/reqwest/lines.rs`; pinned by a unit test there and by `ct9_source_faults` (2026-07-12) |
| GAP-26 | A block timestamp outside the datetime-conversion range killed the ingest flush — the conversion existed only for a log line | Log formatting is best-effort and the raw value is stored unchanged; evm/solana seconds→millis saturate so an absurd source value neither panics nor wraps (PR #100, 2026-07-20). No regression test — CT-9: serve a block with `time = i64::MAX`; assert the batch commits and serving continues |
| GAP-32 | A finalized-head trim/reset race could turn an admitted finalized query into `INTERNAL` when its snapshot no longer had a finalized head | Snapshot-time absence now maps to `NO_DATA`; pinned by `finalized_snapshot_without_a_head_is_no_data` (2026-07-15) |
| GAP-38 | `TX-BY-HASH` / `tidx` absent; fork re-inclusion ordering unimplemented | Transaction hash CF + independent flag + HTTP binding; storage transition suite and black-box ingest/reorg/re-inclusion tests (2026-07-15) |

## 7. Build order (recommended)

- **Phase 0 — skeleton (unblocks everything). ✅ done 2026-07-12.** Source simulator (happy
  path), client driver + §4 validators, reference model, quiescence comparator. Exit: CT-1 runs
  a happy-path script green end-to-end. (GAP-16)

  Delivered as [`crates/hotblocks-harness`](../../hotblocks-harness), driving the real
  binary as a child process over the binding of 13. Its README records the design decisions the
  next phases must not undo. Three things the later phases need are built but unexercised, and
  three are missing:

  | Built, awaiting scripts | Missing |
  |---|---|
  | `Sut::crash/stop/restart` (same db, same port) → CT-2 | the CT-2 kill-point matrix |
  | `Harness::fork` + `Model::resolve_fork` + the follower's CONFLICT recovery → CT-4 | the CT-4 fork/finality corpus |
  | `Model::predict_query` + initial `ct5_error_soundness` matrix | remaining CT-5 binding, boot, and overload rows |
  | `SimFaults` injection point → CT-9 | the rest of the FM-SRC repertoire |

  One correctness note for whoever writes the retention tests: the comparator compares the
  window's first block *exactly*. That is right today (no test trims) and wrong the moment one
  does — the service trims whole chunks, so its window may legally be larger than the model's
  (RS-3/RS-4, `P-RETENTION-SLACK`). Give the comparator that tolerance before, not after.

- **Phase 1 — the P0s.** Stall harness + OB-2/3/11 signals (GAP-1); churn soak + space
  accounting via OB-6 (GAP-6). These target the two production incidents.
- **Phase 2 — correctness core.** CT-2 crash matrix (GAP-2), CT-4 fork/finality corpus
  (GAP-3/4/5), CT-5 remaining error taxonomy + boot matrix (GAP-8/9/15/36/39).
- **Phase 3 — robustness.** CT-9 fuzz both surfaces (GAP-12), CT-3 concurrency swarms,
  CT-8 isolation (GAP-14), shutdown class (GAP-17).
- **Phase 4 — performance regime.** CT-6 scenarios S1–S6, SLO gates, saturation knees,
  baselines (GAP-7/13); wire into CI with tolerances.

Each phase ends by updating §5 statuses and re-prioritizing §6.
