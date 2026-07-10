---
key: consistency
title: Consistency & Isolation
description: Reads honor their durability level and isolation level — snapshot/serializable isolation, monotonic durable reads, no torn or phantom reads.
order: 20
---
# Consistency & Isolation

Read-side and transaction contracts:
- **Durability filter** (`config.rs:267-284`): `ReadOptions.durability_filter`
  defaults to `Memory` (may return not-yet-durable data); `Remote` must return
  only data durable in object storage — a `Remote` read must never surface a
  value a crash would then lose.
- **Isolation** (`transaction_manager.rs:13-21`): `IsolationLevel::Snapshot`
  detects write-write conflicts only; `SerializableSnapshot` also detects
  read-write + phantom-range conflicts. `commit()` always ensures
  read-your-writes (rfcs/0008:283). Committed-state GC by
  `min_conflict_check_seq` (`:282-322`) is subtle and unexercised.
- **Snapshot/scan atomicity**: `snapshot()` gives a "consistent view"
  (`db.rs:768-802`); a scan must not observe one key of an atomic batch
  without its siblings, even while compaction rewrites SSTs mid-iteration.
- **Reader checkpoint** (`db_reader.rs`, #1900): a self-established reader
  checkpoint reaped by writer GC during an object-store brownout must
  re-establish, not fail permanently.

**Existing coverage / gap.** The bank DST oracle covers conservation under
`IsolationLevel::Snapshot` only (`bank.rs`); **SSI, durability-level read
semantics, and cross-restart read monotonicity are uncovered.** The
WorkloadActor monotonic-read oracle is per-process and disjoint-keyspace, so it
catches no cross-actor contention and no cross-restart regression.
