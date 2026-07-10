---
key: compacted-gc-vs-reader
area: gc
title: Compacted GC never deletes an SST a live reader still needs
claim: >-
  With compacted GC enabled, a reader/scan that outlives its checkpoint never
  reads a GC-deleted SST — no FileNotFound is ever surfaced to a read, and GC
  never reclaims an object a live checkpoint or in-flight reader pins.
status: active
provenance: "slatedb-dst/tests/bank.rs:193-199 (compacted GC DISABLED until #319); slatedb/src/garbage_collector/compacted_gc.rs:599; garbage_collector.rs defaults interval 60s/min_age 300s; backlog scout-tests"
explorations:
  - key: compacted-gc-baseline
    title: Reader + compactor + compacted GC coexist
    description: >-
      Writer streams keys; a compactor merges L0→SR; compacted GC is ENABLED
      (the config the authors disable in bank.rs). A concurrent long-lived
      DbReader scans and reads back. No faults, gentle timing. Every read
      succeeds with the right value and NO FileNotFound. Proves the
      reader+compactor+GC harness observes the invariant at all (non-vacuity).
    status: ready
    result: null
    reason: null
    workload: workloads/compacted_gc.py
    command: python3 .workers/workloads/compacted_gc.py --case baseline
    faults: []
    depth: 10
    replay: null
    freshness: new-current
    reported: null
    published: null
  - key: compacted-gc-reader-outlives-checkpoint
    title: Reader outliving its checkpoint under aggressive compaction + GC
    description: >-
      A DbReader establishes a scan/checkpoint, then holds it while the writer
      compacts aggressively and compacted GC runs with a LOW min_age so it
      actively reclaims the pre-compaction SSTs the reader still references.
      Assert the reader NEVER surfaces FileNotFound and every scanned key keeps
      its value (a checkpoint reaped mid-read must re-establish, not fatal —
      #1900). This is the exact #319 window bank.rs disables — the highest
      data-loss value. CRITICAL (strategy-critic): the red is only
      reachable via a LONG-LIVED scan ITERATOR drained slowly across a
      compaction->reestablish->GC(low min_age) cycle — discrete get()s each
      resolve fresh state and give a VACUOUS green. The workload MUST carry an
      armed-fault witness (iterator provably in-flight during a GC pass, like
      inflight-probe's put_was_blocked) or the green is void.
    status: ready
    result: null
    reason: null
    workload: workloads/compacted_gc.py
    command: python3 .workers/workloads/compacted_gc.py --case reader-outlives-checkpoint
    faults: []
    depth: 20
    replay: null
    freshness: new-current
    reported: null
    published: null
---
# Compacted GC never deletes an SST a live reader still needs

## The claim and its invariant

The GC (`garbage_collector.rs`, defaults interval 60s / min_age 300s) reclaims
compacted SSTs no longer referenced. The authors **explicitly disable compacted
GC in their flagship consistency test** (`bank.rs:193-199` "Disable `compacted`
GC until #319 is done") and have only an isolated in-process unit repro
(`compacted_gc.rs:599`). The falsifiable invariant:

> With compacted GC enabled, a reader/scan that outlives its checkpoint must
> never read a GC-deleted SST — **no `FileNotFound` is ever surfaced to a
> read** (data-loss, weight 4) — and GC must never reclaim an object a live
> checkpoint or in-flight reader pins.

Our axis vs upstream: run GC *enabled* (the config they avoid) with a **real GC
loop + slow readers + aggressive compaction** under whole-process faults —
exactly the window their whole-system sim refuses to run.

## Ladder-floor certification (strategy-critic, 2 rungs)

Certified at **2 rungs** (baseline + reader-outlives-checkpoint). The critic
source-verified: #319 is **still open** at this pinned ref (only `#319`
reference is `bank.rs:193-199`) and the FileNotFound window is real — but a
crash-mid-gc rung is **near-vacuous** because compacted GC deletes only objects
already absent from `active_ssts` and never rewrites the live manifest
(`compacted_gc.rs:245-254`), so a mid-delete SIGKILL cannot strand a *referenced*
SST. The reachable red is entirely the in-flight-iterator gap, not a crash.
Overlap note: `reader-checkpoint-reestablish` (backlog 288) shares this
GC+reader harness (both are `db_reader.rs` #1900 reestablish) — do not
double-count the build cost.

## Adversarial model & fault dimensions

- **reader-outlives-checkpoint** — the reader's referenced SSTs become GC
  candidates while it is still reading them. Force it with aggressive compaction
  (small `l0_sst_size`, frequent compaction) + low GC `min_age` so reclamation
  races the read. Checkpoint-reap-then-reestablish (#1900) is the availability
  sub-case (a self-established reader checkpoint reaped mid-read must
  re-establish, not fail permanently). **The decisive lever is iterator
  lifetime**, not min_age tuning — the scan iterator must be held across a
  compaction→reestablish→GC cycle, with an armed-fault witness proving it was
  in-flight during a GC pass (else vacuous green).

## Oracle

Reader-side: every scan/get succeeds with the correct value; a `FileNotFound`/IO
error surfaced to a read is the FAIL. A conservation/all-keys-present check over
the writer's committed set is the value oracle. Universal plane: liveness
watchdog (a wedged reader is a FAIL), terminal-state, durawatch (re-observe the
read-set on a delay ladder — a key readable then GC-erased is delayed erasure),
crashclock for the kill.

## Workload plan — DRIVER EXTENSION REQUIRED (executor build)

The current driver has no compactor/GC/long-reader orchestration. The executor
must add:
- A driver mode that opens the DB with **compacted GC enabled**
  (`Settings.garbage_collector_options` — VERIFY the exact field + low min_age
  against `config.rs`), an active compactor (embedded, or `Admin::run_compactor`
  admin.rs), and small `l0_sst_size`/frequent-compaction settings to force SST
  turnover.
- A long-lived **DbReader** (`Db::builder(...).build_reader()` / `DbReader` —
  VERIFY the reader-open API against `db_reader.rs`) doing repeated scans while
  the writer + compactor + GC churn, logging every read result; a
  `FileNotFound`/IO error is emitted as `INVARIANT compacted_gc_no_filenotfound FAIL`.
- An **armed-fault witness**: prove the scan iterator was mid-drain during a GC
  pass (a counter like inflight-probe's `put_was_blocked`) — a green with no
  witness is void. VERIFY `DbReader` open API (`db_reader.rs`), the
  compactor/GC run APIs (`Admin::run_compactor` admin.rs:339,
  `run_gc_once`/`run_gc_with_options` admin.rs:278,307), and
  `GarbageCollectorOptions`/`DbReaderOptions` fields (`config.rs:1003,1379`;
  `checkpoint_lifetime > 2×manifest_poll_interval`, config.rs:1013).
`workloads/compacted_gc.py` orchestrates writer + compactor + GC + reader,
mines the reader log for FileNotFound, and checks all-keys-present. Reuses
durable_ack.py harness idioms. Mine `slatedb-dst/src/actors/bank/auditor.rs`
(conservation oracle) + `compacted_gc.rs:599` (the unit repro) for oracle ideas.

## Execution / evidence notes

(executor appends.)
