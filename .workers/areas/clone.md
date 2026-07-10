---
key: clone
title: Clones & Checkpoints
description: A clone/checkpoint is a consistent point-in-time view that stays fully readable while the parent keeps writing, compacting, and GC-ing.
order: 40
---
# Clones & Checkpoints

Checkpoints pin a consistent+durable point-in-time manifest
(rfcs/0004-checkpoints.md). Zero-copy clones reference the parent's external
SSTs rather than copying them (`clone.rs`, `checkpoint.rs:177-207`).

**Why it is bug-bearing.** This is the freshest correctness churn in the repo
and has **zero workload coverage** (no DST actor calls `create_clone`/
`CloneBuilder`/`Admin`): `6a131a9`/#1907 "Resolve external SSTs in DbReader so
zero-copy clones are readable", `016b676`/#1909 "RetryingObjectStore into Admin
and Clone paths", `1b3093f`/#1851 "merge external DBs by contents not
checkpoint-id" (was letting `external_dbs` grow unbounded / wrong checkpoint
win), key-gap projection panic `6c7f57d`/#1887, union over fence-only WAL
`ba98f68`/#1811, clone-chain detach `2dbcb39`/#1643.

**Invariants to falsify:**
- `clone.get(k) == parent_snapshot_at_C.get(k)` for every key at clone time.
- Bidirectional isolation: post-clone parent writes never appear in the clone;
  clone writes never appear in the parent.
- `external_dbs` count stays bounded across repeated clone generations
  (falsifies #1851).
- GC/compaction on the parent never deletes an external SST a live clone still
  references (no `NotFound`/IO error surfaced to a clone read).

Reachability caveat: `ActorCtx` exposes object stores + `swap_db` but likely no
clone/Admin handle — a clone workload needs `Admin::create_clone` wired via a
new driver op, not the DST harness.
