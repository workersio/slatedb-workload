# Run evidence — durability-filter-remote-baseline

**Promise:** durability-filter-remote · **Area:** consistency
**Exploration:** durability-filter-remote-baseline · **Verdict:** GREEN
(non-vacuity control: the Memory/Remote discrimination holds)

## What it proves
Driver `durprobe` opens with `flush_interval=None` (auto-flush off, so the dirty
window is deterministic, not a 100ms race — `config.rs:633-637`), then per key:
`put(await_durable=false)` → `get(Memory)` sees it → `get(Remote)` does NOT →
`db.flush()` → `get(Remote)` now sees it. Read-path proof: `reader.rs:112-113`
caps Remote reads at `oracle.last_remote_persisted_seq()`, filtering the
higher-seq unflushed write. In-tree corroboration: `db.rs::test_no_flush_interval`
(:3042), `db.rs:2809 test_scan_prefix_by_recency_durability_remote_filters_unflushed`.

## Invariants (in-guest, exploration nd76yqtq10k1s278byrn0e9q018a971z, depth 3, all succeeded)
```
DURPROBE_SUMMARY keys=8 mem_dirty_hits=8 remote_dirty_hits=0 remote_flushed_hits=8
INVARIANT durability_filter_memory_sees_dirty PASS mem_dirty_hits=8/8
INVARIANT durability_filter_remote_excludes_dirty PASS remote_dirty_hits=0 (MUST be 0)
INVARIANT durability_filter_remote_after_flush PASS remote_flushed_hits=8/8
INVARIANT terminal_state durprobe-keys-resolved PASS 8/8
VERDICT: GREEN — Memory/Remote discrimination holds
```
`remote_dirty_hits=0` is the falsifiable core (a Remote read returning a
not-yet-durable value is the violation). `ORACLE_SELFTEST=1` injects a fake
remote_dirty_hit → `durability_filter_remote_excludes_dirty FAIL` (red path proven).

## Interpretation
Baseline control passes: Remote genuinely excludes not-yet-durable data. The
adversarial rungs (crash-confirm: `R_remote ⊆ survivors` after SIGKILL;
inflight-flush: Remote excludes an in-flight value at the flush boundary) are the
bug hunt — next.
