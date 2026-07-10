---
key: writer-fencing-split-brain
area: fencing
title: A superseded writer cannot make any write durable or visible
claim: >-
  Once a newer writer opens the same object-store path, the older writer's next
  await_durable write fails Fenced and leaves no durably-visible write that
  violates the winner's history — no split-brain, no zombie-writer data.
status: active
provenance: "slatedb/src/fence.rs:105-160 (WriterFencer, Fenced on superseded op); manifest/store.rs:34,621 (epoch version-CAS); manifest/invariants.rs:42 (backward clock skew); map.md reality (epoch-CAS at DB-open); backlog scout-docs"
explorations:
  - key: fencing-split-brain-baseline
    title: Second open fences the first
    description: >-
      Writer A opens the root, writes+acks keys. Writer B opens the SAME root
      (bumping the manifest epoch). Assert (a) B's writes succeed and are durable,
      and (b) A's next await_durable write returns SlateDBError::Fenced. Proves
      the fence is observed at all (non-vacuity control).
    status: done
    result: green
    reason: null
    workload: workloads/fencing.py
    command: python3 .workers/workloads/fencing.py --case baseline
    faults: []
    depth: 10
    replay: {run: "nd7ejybh1y7kg4tzmgr85btthn8a9x1q", case: baseline, seed: 5}
    freshness: new-current
    reported: null
    published: pending
  - key: fencing-split-brain-overlap-writes
    title: Overlapping concurrent writers, no lost update / no zombie data
    description: >-
      A and B write OVERLAPPING keys concurrently around the B-open fence point.
      After both settle, reopen and assert the final durable state is a valid
      single-writer history: no key reflects an A-write that landed AFTER B
      fenced A (no zombie durable write), and no committed B-write is lost or
      resurrected to an A-value. Adversarial: races the fence boundary.
    status: done
    result: blocked
    reason: "zombie window structurally unreachable — every post-epoch-bump victim flush is Fenced (fence.rs:145-171 barrier-advance + tablestore.rs:1125,1133 PutMode::Create); post_fence_suspects=0 across 49 on-box + 10 in-guest seeds → anti-vacuity VOID. Fence is airtight (stronger than the promise). Producer: retire OR re-aim to the IN-FLIGHT buffered-flush variant (converges with stale-epoch-flush rung 3)."
    workload: workloads/fencing.py
    command: python3 .workers/workloads/fencing.py --case overlap-writes
    faults: [process-kill]
    depth: 20
    replay: null
    freshness: new-current
    reported: null
    published: null
  - key: fencing-split-brain-stale-epoch-flush
    title: A flush/write attempted after the epoch bump is rejected
    description: >-
      Drive A to attempt a flush/write with its now-stale epoch AFTER B has
      opened (seed-swept: vary how much A had buffered/in-flight at the fence
      point). Every such attempt MUST fail Fenced (or the whole batch be absent
      after reopen) — a stale-epoch write that lands durably is the falsification.
      Fault-boundary: the manifest epoch-CAS / zero-byte-WAL-barrier window
      (fence.rs:143-172).
    status: ready
    result: null
    reason: null
    workload: workloads/fencing.py
    command: python3 .workers/workloads/fencing.py --case stale-epoch-flush
    faults: [process-kill]
    depth: 20
    replay: null
    freshness: new-current
    reported: null
    published: null
---
# A superseded writer cannot make any write durable or visible

## The claim and its invariants

SlateDB is single-writer per object-store path. Open bumps a manifest epoch via
version-CAS (`manifest/store.rs:34`; conflict ⇒ `TransactionalObjectVersionExists`
`:621`); `WriterFencer::fence` (`fence.rs:105`) bumps the epoch and writes a
zero-byte WAL barrier, so a superseded writer's next op fails
`SlateDBError::Fenced` (`fence.rs:147`).

> After a second `Db` handle opens the same path, writer A's next `await_durable`
> write must fail `Fenced`, and the final durable state must be a valid
> single-writer history — no A-write that landed after the fence is durably
> visible (**availability/correctness**; a zombie durable write is a lost-update
> or resurrection).

## Why this is the cheapest reachable red (strategy-critic counter-promotion)

Reuses the **existing** crash driver (`open_db`/`run`, main.rs) + a second
process on the shared `LocalFileSystem` root — the only new driver surface is an
"expect-Fenced" mode. No clone/checkpoint/compactor/GC/long-iterator machinery
(the reason it was promoted ahead of clone-consistency and compacted-gc-vs-reader,
which need heavy new harnesses). Epoch-CAS-at-DB-open fencing is source-confirmed
(`fence.rs`; map.md reality: prior instance fenced at successor's DB-open).

## Adversarial model & fault dimensions

- **overlap-writes** — A and B write overlapping keys concurrently across the
  B-open instant; the race is the fence boundary. Reachable with two processes
  on one FS root; a small `manifest_poll_interval` (`SL8_MANIFEST_POLL_INTERVAL`)
  tightens detection.
- **stale-epoch-flush** — A has buffered/in-flight writes when B fences it; A's
  flush of the stale-epoch batch must be rejected (the `fence.rs:143-172`
  epoch-CAS + WAL-barrier window). Seed-swept over how much A had in flight.

## Oracle

Bespoke: (1) A's post-fence `await_durable` write returns `Fenced` (the driver's
victim mode reports the error kind — one observation per attempt); (2) after
reopen, the durable key→value map is a valid single-writer history (every value
is B's, or A's only for keys A wrote strictly before the fence). Universal plane:
liveness watchdog, terminal-state, durawatch (re-observe B's acked set — an
A-value resurrecting later is delayed erasure), crashclock declared timing for
the process spawns/kills.

## Workload plan — DRIVER EXTENSION (small; executor build)

Add an "expect-Fenced" capability to the driver:
- `fence-victim --root <dir> --ack-log <path> --seed <s>` — open the Db, write+ack
  keys (log to ack-log), then loop attempting `await_durable` writes and print
  `FENCE_OBSERVED attempt=<i> result=<ok|fenced|other:<err>>` for each; exit when
  Fenced or after a bound. Do NOT swallow the error — report its kind
  (`SlateDBError::Fenced`).
- `fence-usurper --root <dir> --seed <s>` — open the SAME root (fences the victim),
  write+ack its own keys, hold briefly, close.
- Reuse `verify` for the final single-writer-history check against the merged
  ack-logs.
`workloads/fencing.py` spawns victim, waits until it's writing, spawns usurper on
the same root, then asserts the victim observes `Fenced` and the final state is a
valid history. Two-process pattern; reuses the crash/ack-log idioms.
VERIFY `SlateDBError::Fenced` variant + the victim's write-error surface against
`slatedb/src/error.rs` + `fence.rs` before building.

## Execution / evidence notes

(executor appends.)
