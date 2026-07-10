# Run evidence — durability-filter-remote-inflight-flush

**Promise:** durability-filter-remote · **Area:** consistency
**Exploration:** durability-filter-remote-inflight-flush · **Verdict:** GREEN
(Remote excludes the in-flight value at the flush boundary — completes the
ladder floor 3/3)

## Attack
A `BlockWalPut` ObjectStore wrapper (mirrors head_fn.rs) holds the WAL SST PUT
(`.../wal/{id:020}.sst`) **in-flight**. Driver `inflight-probe` (flush_interval
=None): write N `await_durable=false` keys → arm the gate → `db.flush()` on a
background task (its WAL PUT blocks) → while blocked, `get_with_options(Remote)`
each key (MUST miss) → release → re-read Remote (MUST hit). Fault genuinely
armed: `put_was_blocked=true` on every run.

## Reachability / why structurally GREEN (source-verified)
The Remote watermark advances **strictly after** the WAL PUT durably lands:
`wal_buffer.rs:351-367` awaits `write_sst` (the PUT, `tablestore.rs:1130`) →
only on Ok does `wal_buffer.rs:322-337` set `last_flushed_seq` + fire
`WalFlushed` → `db.rs:2069` `oracle.advance_durable_seq` → `oracle.rs:60-105`
`last_remote_persisted_seq` → `reader.rs:111-113` caps Remote at it. A blocked
PUT leaves the watermark below the flushing batch, so Remote excludes it. A RED
would require the watermark to advance BEFORE the PUT lands — it does not.

## Evidence (in-guest, exploration nd7962sqbfhq5mdg34z29garas8a9qab, depth 5, all succeeded)
```
INFLIGHT_SUMMARY keys=8 during_block_hits=0 after_release_hits=8 put_was_blocked=true
INVARIANT durability_filter_remote_excludes_inflight PASS during_block_hits=0 (MUST be 0) put_was_blocked=True
INVARIANT durability_filter_remote_after_release PASS after_release_hits=8/8
VERDICT: GREEN
```
Local GREEN across seeds 1,2,3,7,42 (all put_was_blocked=true); `ORACLE_SELFTEST=1`
→ `durability_filter_remote_excludes_inflight FAIL` (red path proven). Baseline +
crash-confirm regressions still green.

## Interpretation
Remote never surfaces a value whose WAL PUT is still in flight — the durability
watermark is correctly PUT-completion-gated. **durability-filter-remote is now
covered to floor (baseline + crash-confirm + inflight-flush, all GREEN).**
