---
key: writebatch-atomicity-across-crash
area: durability
title: WriteBatch is all-or-nothing across a crash
claim: >-
  A db.write(batch) of put/delete operations is applied atomically — a reader
  never observes a strict subset of a committed batch, even after crash
  recovery.
status: parked
provenance: "slatedb/src/batch.rs:21-24 (\"applied atomically\"); slatedb/src/db.rs:1660-1697; lib.rs:99 (batch_write)"
explorations:
  - key: writebatch-atomicity-baseline
    title: WriteBatch atomicity baseline
    description: >-
      No faults. Linked-key batches {A=v, B=v, ...} sharing a batch-id encoded
      in the value; a concurrent scan/reopen never sees a partial batch. Proves
      the paired-key oracle observes atomicity at all.
    status: planned
    result: null
    reason: null
    workload: workloads/writebatch_atomicity.py
    command: python3 .workers/workloads/writebatch_atomicity.py --case baseline
    faults: []
    depth: 10
    replay: null
    freshness: new-current
    reported: null
    published: null
  - key: writebatch-atomicity-torn-replay
    title: Torn last WAL record on replay is rejected, not half-applied
    description: >-
      A guest ObjectStore wrapper truncates/corrupts the tail of the last WAL
      object before reopen; replay must reject the torn record atomically (whole
      batch absent), never surface a half-applied batch. MUST use
      block-spanning batches (a batch large enough to cross a WAL-SST block
      boundary) — otherwise a truncated tail rejects the whole single-block SST
      and the rung is vacuous (strategy-critic, tablestore block format).
      Exercises wal_replay.rs record-boundary handling.
    status: planned
    result: null
    reason: null
    workload: workloads/writebatch_atomicity.py
    command: python3 .workers/workloads/writebatch_atomicity.py --case torn-replay
    faults: [objectstore-torn-tail, process-kill]
    depth: 20
    replay: null
    freshness: new-current
    reported: null
    published: null
---
# WriteBatch is all-or-nothing across a crash

## Status: PARKED — surface certified below the 3-rung floor (strategy-critic 2026-07-10)

The critic certified this promise's reachable failure surface as smaller than
the ladder floor and source-proved the naive crash rung vacuous:
- A batch is appended in **one locked call** to a single `current_wal`
  (`batch_write.rs:252-258`, whose comment states this design intent), so a
  batch is never split across WAL SSTs.
- `LocalFileSystem` PUT is atomic (temp-file + rename), so a SIGKILL leaves the
  batch's WAL SST wholly present or wholly absent — **`crash-mid-batch` cannot
  produce a strict subset** (dropped as vacuous; identical to baseline).
- The only reachable partial-batch is an **intra-SST cross-block torn record**
  (`torn-replay`), and only with block-spanning batches. Torn/partial objects
  are not a natural crash artifact (atomic PUT everywhere) — this rung tests
  defense against actively-corrupted objects: valid but lower-severity.

Parked behind higher-value work; unpark only if the torn-tail robustness surface
becomes a priority. Kept as a spec record so the certification is auditable.

## The claim and its invariant

`batch.rs:21-24` — a WriteBatch is "applied atomically." Distinct from the
txn-commit path the DST bank exercises: this attacks the raw `db.write(batch)`
`batch_write` path (`lib.rs:99`), whose cross-key atomicity the bank's
disjoint-keyspace WorkloadActor and conservation-sum oracle cannot catch (a
compensating pair masks a torn read in a sum).

> Encode a monotonically increasing `batch-id` in every value. For any batch,
> at every read point and after every recovery, either **all** keys of the
> batch reflect that batch-id or **none** do. A strict subset is a
> **correctness** finding (weight 3; data-loss-adjacent when the surviving
> subset is the durable state).

## Adversarial model & fault dimensions

- **crash-mid-batch** — SIGKILL (crashclock, seed-swept) while `db.write` is
  in flight; the WAL append for the batch is either fully durable or not.
- **torn-replay** — a guest ObjectStore wrapper truncates the tail bytes of the
  final WAL object, simulating a partial/torn last record; `wal_replay.rs` must
  reject the torn record atomically, never half-apply the batch.

## Oracle

Paired-key/batch-id oracle (above) — stronger than a conservation sum. Plus the
universal oracle plane: liveness watchdog, terminal-state sweep, durawatch
(re-observe batches on a delay ladder — a batch present immediately then partly
erased by compaction is delayed erasure), crashclock declared timing.

## Workload plan

Reuses the `slatedb-driver` binary and the crash harness built by
`durable-ack-survives-crash` (do not rebuild the driver — extend its op stream
with `write-batch` linked-key groups and add a `--truncate-wal-tail` fault flag
for `torn-replay`). `workloads/writebatch_atomicity.py` generates linked-key
batches, drives the driver, arms the crash, reopens in verify mode, and checks
the batch-id atomicity oracle. Depends on `durable-ack-survives-crash` landing
the driver first — mark ready only after that baseline is green.

## Execution / evidence notes

(executor appends.)
