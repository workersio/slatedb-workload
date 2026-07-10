---
key: clone-consistency
area: clone
title: A zero-copy clone is a consistent, isolated, durable point-in-time copy
claim: >-
  A clone reflects exactly the parent's committed state at clone time, stays
  fully readable while the parent keeps writing/compacting/GC-ing its shared
  external SSTs, and never leaks writes to or from the parent.
status: active
provenance: "slatedb/src/clone.rs:177-207; checkpoint.rs; commits 6a131a9/#1907 (external SST resolve in DbReader), 016b676/#1909 (RetryingObjectStore into Clone), 1b3093f/#1851 (external_dbs unbounded/wrong-checkpoint), ba98f68/#1811; backlog scout-commits/scout-api"
explorations:
  - key: clone-consistency-baseline
    title: Clone equals parent snapshot at clone time
    description: >-
      No faults. Populate a parent DB, create a clone at checkpoint C, open the
      clone; for every key clone.get(k) MUST equal the parent's committed value
      at C. Proves the oracle observes clone fidelity at all (non-vacuity).
    status: planned
    result: null
    reason: null
    workload: workloads/clone_consistency.py
    command: python3 .workers/workloads/clone_consistency.py --case baseline
    faults: []
    depth: 10
    replay: null
    freshness: new-current
    reported: null
    published: null
  - key: clone-consistency-parent-churn
    title: Clone stays consistent + readable under parent write/compact/GC
    description: >-
      After cloning at C, the parent keeps writing new keys, compacting L0→SR,
      and running GC that may reclaim SSTs the clone shares. Assert: (a) clone
      reads still equal the C snapshot (post-C parent writes never appear in the
      clone — forward isolation); (b) clone writes never appear in the parent
      (reverse isolation); (c) NO clone read ever surfaces FileNotFound/IO error
      for a key present at C (GC/compaction must not delete an external SST a
      live clone references); (d) external_dbs stays bounded across repeated
      clone generations (falsifies #1851 unbounded growth / wrong-checkpoint-win).
    status: planned
    result: null
    reason: null
    workload: workloads/clone_consistency.py
    command: python3 .workers/workloads/clone_consistency.py --case parent-churn
    faults: []
    depth: 20
    replay: null
    freshness: new-current
    reported: null
    published: null
  - key: clone-consistency-creation-crash
    title: Crash during clone creation before the permanent parent pin lands
    description: >-
      REFRAMED (strategy-critic): the original "crash during parent GC of a
      clone-referenced SST" is STRUCTURALLY GREEN — create_clone writes a
      lifetime:None (non-expiring) parent checkpoint (clone.rs:197-212) and
      compacted GC excludes checkpointed SSTs (compacted_gc.rs:224), so a
      single-source clone's SSTs are pinned and GC/compaction can never reclaim
      them. The REAL crash corridor: SIGKILL the clone PROCESS between the
      clone-manifest write and the permanent-parent-checkpoint write (clone.rs:
      178-212). Before that loop completes the parent is pinned only by the
      EPHEMERAL 300s checkpoint (get_or_create_parent_checkpoint, clone.rs:302,
      lifetime Some(300s)); if it expires before a retry re-establishes the pin,
      parent GC can reap the clone's SSTs → dangling clone (data-loss). This is
      the path #1907 (resolve external SSTs in DbReader) freshly touches.
    status: planned
    result: null
    reason: null
    workload: workloads/clone_consistency.py
    command: python3 .workers/workloads/clone_consistency.py --case crash-mid-gc
    faults: [process-kill]
    depth: 20
    replay: null
    freshness: new-current
    reported: null
    published: null
---
# A zero-copy clone is a consistent, isolated, durable point-in-time copy

## Status: PLANNED — strategy-critic reframe pending before ready (2026-07-10)

The critic source-verified two headline corridors as unreachable/low-value; the
promise stays `planned` until the reframe below is fully folded in (deferred
behind fencing + compacted-gc, which are the higher-value reachable reds):
- **Invariant 3 (referential FileNotFound) is PINNED** — a single-source clone's
  SSTs are held by a `lifetime:None` parent checkpoint (`clone.rs:197-212`) that
  compacted GC excludes (`compacted_gc.rs:224`). Parent GC/compaction can never
  reclaim a live clone's SST. The `parent-churn` rung must DROP the FileNotFound
  assertion and keep only **fidelity + bidirectional isolation**.
- **Invariant 4 (bounded external_dbs) is a FIXED bug** (#1851 merged at this
  ref) — demote to a `freshness: regression-guard` check inside baseline, not a
  data-loss finding. It IS inspectable via `Admin::read_manifest` →
  `Manifest::external_dbs()` (manifest/mod.rs:859).
- **Rung 3 reframed** to the clone-**creation** crash window (see the
  `clone-consistency-creation-crash` entry) — the only reachable crash corridor.
- **Reachable weight is correctness (3), not data-loss (4)** — the data-loss
  framing rested on the pinned invariant-3. Certified at ~2 reachable fault
  models (fidelity+isolation; creation-crash pin-gap).
Union (multi-source) clones set `final_checkpoint_id:None` (manifest/mod.rs:1348)
— a narrower ephemeral-expiry corridor worth a later rung.

## The claim and its invariants

Zero-copy clones reference the parent's external SSTs rather than copying them
(`clone.rs`, `checkpoint.rs:177-207`). This is the **freshest correctness churn**
in the repo with **zero workload coverage** (no DST actor calls
`create_clone`/`CloneBuilder`/`Admin`). Falsifiable invariants:

1. **Fidelity** — `clone.get(k) == parent_snapshot_at_C.get(k)` for every key at
   clone time C (data-loss/correctness if a clone value is wrong or missing).
2. **Bidirectional isolation** — post-C parent writes never appear in the clone;
   clone writes never appear in the parent.
3. **Referential durability** — parent GC/compaction never deletes an external
   SST a live clone references; a clone read never surfaces
   `FileNotFound`/IO error for a key present at C (**data-loss**, weight 4).
4. **Bounded external_dbs** — the clone's `external_dbs` set stays bounded across
   repeated clone generations (falsifies #1851, which let it grow unbounded /
   the wrong checkpoint win).

## Adversarial model & fault dimensions

- **parent-churn** — after cloning, drive the parent hard: new writes,
  compaction (L0→sorted-run), and GC with a low `min_age` so it actively
  reclaims. The shared-SST reclamation vs live-clone-read race is the seam
  (#1907 "resolve external SSTs in DbReader" is exactly this path, freshly
  changed).
- **crash-mid-gc** — SIGKILL mid-reclamation (ack-progress trigger) to catch a
  half-completed GC that could strand the clone on a deleted external SST.

## Oracle

Differential: capture the parent's committed key→value map at C (before churn);
after each phase, diff `clone.scan()` against it (fidelity + isolation). A clone
read raising NotFound/IO = referential-durability FAIL. Count `external_dbs`
across N clone generations for the bound. Plus the universal oracle plane
(liveness watchdog, terminal-state, durawatch re-observe the clone read-set on a
delay ladder — a clone key present then erased by a delayed parent GC is delayed
erasure, crashclock for the kill).

## Workload plan — DRIVER EXTENSION REQUIRED (executor build)

The current `slatedb-driver` has no clone capability. The executor must add:
- `clone --parent-root <dir> --clone-root <dir> [--checkpoint <id>]` — create a
  checkpoint on the parent (or use the clone-at-latest path) and
  `Admin::create_clone` / `CloneBuilder` the clone into `clone-root`. VERIFY the
  exact API against `slatedb/src/clone.rs` + `admin.rs` (`create_clone_builder_from_source`
  admin.rs:661; `create_detached_checkpoint` admin.rs:483) at the pinned ref.
- `clone-scan --root <dir>` / reuse `verify` against a captured snapshot manifest
  so the clone's full key set can be diffed value-exact.
- A parent-churn driver mode: writer + `run_compactor` (admin) + GC enabled
  (`Settings.garbage_collector_options` with low min_age) running concurrently
  while the clone is read.
- Reuse the ack-progress SIGKILL for crash-mid-gc.
`workloads/clone_consistency.py` orchestrates parent populate → clone → capture C
snapshot → (churn|crash) → diff. Reuses the crash/fsync-log harness idioms from
durable_ack.py. Note: `ActorCtx` in DST has no clone handle — this is genuinely
new driver code (backlog + scout-runtime flagged it).

## Execution / evidence notes

(executor appends.)
