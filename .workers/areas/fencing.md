---
key: fencing
title: Writer Fencing
description: Once a newer writer opens the DB, the older writer cannot make any write durable or visible — no split-brain, no zombie-writer data.
order: 30
---
# Writer Fencing

SlateDB is single-writer per object-store path. Open bumps a manifest epoch via
version CAS (`manifest/store.rs:34`; conflict ⇒ `TransactionalObjectVersionExists`
`:621`); `WriterFencer::fence` (`fence.rs:105`) bumps the epoch and writes a
zero-byte WAL barrier, so a superseded writer's next op fails
`SlateDBError::Fenced` (`fence.rs:147`). Startup also does time-based fencing
via `manifest_poll_interval` (default 1s; `SL8_MANIFEST_POLL_INTERVAL`).

**Invariant to falsify:** after a second `Db` handle opens the same path, the
first handle's next `await_durable` write must fail `Fenced` and must leave no
durably-visible write that violates the winner's history (no lost update, no
resurrected key). Also: a backward wall-clock-skewed writer must not publish
(`manifest/invariants.rs:42`).

**Our axis vs upstream.** The DST `fencer.rs` actor simulates a new writer by
reopening *in-process*; genuine two-process overlapping-writer races on a shared
store are the lift here. Novelty is boosted by the WAL-buffer-ownership refactor
(`076834d`) touching the flush path the fenced writer races on.
