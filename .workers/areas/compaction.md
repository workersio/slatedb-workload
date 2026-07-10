---
key: compaction
title: Compaction
description: Compaction preserves every live key; a fenced or unowned compactor never commits a manifest that drops SSTs.
order: 60
---
# Compaction

The compactor merges L0 SSTs into sorted runs. Heavy recent churn (git scout:
`compactor.rs` 123 commits, `compactor_executor.rs` 83, `compactor_state.rs`
64): "stop executing compaction if no longer owned" `da14c59`/#1856, "disable
`.compaction` writes on SST progress" `297b6a1`/#1884, heartbeat rework
`942e4e4`/#1864, flaky segment compaction `bdffed0`/#1906. Ownership is by
`worker_heartbeat_timeout` (30s) + `commit_compacted_interval` (1s).

**Invariants to falsify:**
- L0→sorted-run compaction preserves every live key (no dropped/resurrected
  key across a compaction commit).
- A fenced or unowned compactor never commits a manifest that drops SSTs
  (falsifies #1856/#1884).

**Our axis vs upstream.** DST spawns exactly one `CompactorActor`; two competing
or restarting compactors against a shared writer is uncovered. Reuses the crash
driver + a second admin-run-compactor process.
