---
key: durability-filter-remote
area: consistency
title: A Remote-durability read never shows a value a crash would lose
claim: >-
  get_with_options with DurabilityLevel::Remote returns only data durably stored
  in object storage — a value it returns is never lost by a subsequent crash.
status: active
provenance: "slatedb/src/config.rs:267-284 (DurabilityLevel Remote vs default Memory); rfcs/0008-synchronous-commit.md; strategy-critic counter-promotion 2026-07-10"
explorations:
  - key: durability-filter-remote-baseline
    title: Remote vs Memory read discrimination baseline
    description: >-
      No faults. A value written with await_durable=false is visible to a
      default Memory-filter read but MUST NOT appear in a Remote-filter read
      until it is durable; once flushed it appears to both. Proves the oracle
      discriminates the two levels at all (non-vacuity control).
    status: ready
    result: null
    reason: null
    workload: workloads/durability_filter.py
    command: python3 .workers/workloads/durability_filter.py --case baseline
    faults: []
    depth: 10
    replay: null
    freshness: new-current
    reported: null
    published: null
  - key: durability-filter-remote-crash-confirm
    title: Remote read-set survives crash
    description: >-
      Snapshot the set of (key,value) returned by Remote-filter reads before a
      seed-swept SIGKILL; after reopen the surviving state MUST be a superset of
      that Remote read-set (Remote never showed a value that then vanished).
      Interleave await_durable=false writers so Memory and Remote diverge.
    status: ready
    result: null
    reason: null
    workload: workloads/durability_filter.py
    command: python3 .workers/workloads/durability_filter.py --case crash-confirm
    faults: [process-kill]
    depth: 20
    replay: null
    freshness: new-current
    reported: null
    published: null
  - key: durability-filter-remote-inflight-flush
    title: Remote excludes an in-flight (not-yet-durable) value at the flush boundary
    description: >-
      Read at the boundary where a value is in the memtable/WAL buffer but its
      WAL SST PUT has not completed. Remote MUST exclude it; if Remote returns a
      value whose WAL object is not yet persisted, a crash at that instant loses
      it — falsification (wrong-durable-read). Arms the flush at seed-swept
      crashclock points against the ack/durable_seq gate (wal_buffer.rs:334-338).
    status: ready
    result: null
    reason: null
    workload: workloads/durability_filter.py
    command: python3 .workers/workloads/durability_filter.py --case inflight-flush
    faults: [process-kill]
    depth: 20
    replay: null
    freshness: new-current
    reported: null
    published: null
---
# A Remote-durability read never shows a value a crash would lose

## The claim and its invariant

`ReadOptions.durability_filter` (`config.rs:267-284`) defaults to `Memory`
(may return not-yet-durable data); `Remote` "returns only data durably stored in
object storage." The falsifiable invariant pairs a read observation with crash
survival:

> Let `R_remote` = the set of (key,value) returned by `Remote`-filter reads
> before a crash. After reopen, surviving state ⊇ `R_remote` — a `Remote` read
> never returned a value that a crash then erased. Symmetrically, a value
> written `await_durable=false` and not yet durable must be visible to `Memory`
> but **absent** from `Remote`.

A `Remote` read that surfaces a value whose WAL object is not yet persisted is a
**correctness / wrong-durable-read** finding (weight 3; data-loss-adjacent).

## Why this is a cheap, high-signal slot-2 (breaks the durability anchoring)

Strategy-critic counter-promotion: this reuses the exact same crash driver as
`durable-ack-survives-crash` (add a `--read-durability remote|memory` op and an
`await_durable=false` write op — `C=2`), validates the ack/`durable_seq` gating
from the **read** side, and moves batch 1 off its all-durability fixation into
the consistency area. The baseline is a genuine non-vacuity control (Memory sees
the dirty write, Remote does not).

## Oracle

`R_remote ⊆ survivors` after crash (bespoke), plus the Memory/Remote
discrimination assertion. Universal oracle plane: liveness watchdog,
terminal-state sweep, durawatch (re-observe the Remote read-set on a delay ladder
— a Remote value present immediately then erased is delayed erasure), crashclock
declared timing.

## Workload plan

Reuses the `slatedb-driver` binary from `durable-ack-survives-crash` (do not
rebuild). Extend its op set with `read --durability {memory,remote}` and
`put --no-await-durable`. `workloads/durability_filter.py` interleaves
await_durable=false writers with Remote/Memory readers, records the Remote
read-set, arms the crashclock SIGKILL (skip for baseline), reopens in verify
mode, and checks `R_remote ⊆ survivors`. Depends on the driver landing in
`durable-ack-baseline` first.

## Execution / evidence notes

(executor appends.)
