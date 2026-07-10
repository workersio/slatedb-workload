---
key: durability
title: Durability
description: Acknowledged writes survive process crash and restart; WAL replay neither loses nor double-applies committed data.
order: 10
---
# Durability

SlateDB's headline claim (README:19): `put()` resolves only when data is
**durably persisted** to object storage; `await_durable=false` opts out for
lower latency. Ack is gated on `durable_seq`. The WAL is the crash-recovery
substrate: writes append to a WAL buffer, flush to WAL SSTs on `flush_interval`
(default 100ms; `SL8_FLUSH_INTERVAL` overrides), and replay on reopen from
`replay_after_wal_id` (`wal_replay.rs:113-157`).

**Why it is bug-bearing.** The WAL/flush/replay boundary is the hottest churn
in the repo (git scout: `wal_buffer.rs` 59 commits, `batch_write.rs` 43; recent
refactors `5cdc57d`/#1885 "only advance last_seen_wal_id with flushed wals",
`076834d`/#1882 "buffer owned by batch writer"). Past bugs: replay-point
propagation (#1857), replay→L0 boundary (#1625), post-replay underflow (#1438).

**Our axis vs upstream.** `slatedb-dst` injects only *in-process* object-store
toxics and closes the DB gracefully (`harness.rs:733`) — it has **no process
kill, no crash-mid-flush window**. Whole-process SIGKILL against a real object
store (LocalFileSystem) from an external driver binary is the differentiated
attack this area owns.

Harvested: durable-ack crash survival, WriteBatch atomicity across crash.
Open: WAL-replay boundary under a dropped WAL-SST PUT; await_durable=false's
weaker (non-)promise (do not over-assert — those writes are allowed to vanish).
