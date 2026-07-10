# Run evidence — durable-ack-wal-head-contiguity

**Promise:** durable-ack-survives-crash · **Area:** durability
**Exploration:** durable-ack-wal-head-contiguity · **Verdict:** GREEN
(the silent-truncation hypothesis is REFUTED — source-verified)

## Attack
Leave an **un-manifested acked WAL tail** (SIGKILL the writer after the K-th
fsync'd ack — no clean close, no L0 flush → manifest `replay_after_wal_id=0`),
then reopen with a guest ObjectStore wrapper that returns a **false-negative
HEAD** (404) for one WAL id in the tail while the object stays readable. If the
reopen frontier search (`tablestore.rs:163-273`, monotone-existence assumption
:177-179) settled on a max below the true one, replay would truncate the acked
suffix → data-loss. Target id is seed-derived from the tail ids below the top
ack-bearing id; anti-vacuity requires ≥1 ack-bearing WAL above the target.

## Reachability (source-verified — the window is REAL)
- Every open fences via the exponential+binary HEAD frontier:
  `builder.rs:574-578` → `empty_wal_id = last_seen_wal_id(...)+1`
  (`tablestore.rs:316-320`, search :180-273).
- `builder.rs:590,772` replay `replay_after_wal_id+1 .. empty_wal_id+1`
  (`fence.rs:165`). Manifest records only `replay_after_wal_id` (L0/GC boundary);
  the un-manifested tail above it is recovered **only** via the frontier — so a
  SIGKILL genuinely leaves acked writes discoverable solely by the HEAD search.

## Result — GREEN (loud, not silent), two guards
- `write_wal_fence` uses `PutMode::Create` (`tablestore.rs:430-443`); the
  fence-barrier walk (`fence.rs:143-172`) increments on every
  AlreadyExists/Fenced, so a false-low frontier is **self-corrected** — the walk
  lands the barrier at true `max+1` and replay covers the full tail. A single
  sequential flusher never creates gaps, so contiguity holds.
- When the false-negated id is inside the replay range, replay's SST read HEADs →
  NotFound → `Db::builder().build()` returns a **loud error**, not a
  truncated-but-successful open.

## Evidence (in-guest, exploration nd7f7ygzkt1tbyzgn6w5n47szh8a9tmk, depth 5, all succeeded)
Sample (seed 3640688248, K=11, WAL ids [1..12], target=8):
```
HEADFN target_wal_id=8 (from 11 tail candidates below top ack-bearing id 12); above=[9,10,11,12]
VERIFY_OPEN_FAILED err=… NotFound path=".../wal/00000000000000000008.sst" source="head-false-negative (fault injection)"
HEADFN_REOPEN_LOUD_FAILURE wal_id=8: reopen returned an error — detected, not silent …
INVARIANT terminal_state acked-keys-resolved PASS [control(fault-free reopen)] 11/11 …
INVARIANT durable_ack_subset acked-subset-readable PASS [control(fault-free reopen)] checked=11 lost=0 mismatch=0 …
VERDICT: GREEN
```
Local: same GREEN across seeds 1,2,3,5,7,11. `ORACLE_SELFTEST=1` → `durable_ack_subset FAIL` (red path proven). Baseline + crash-mid-flush regressions still GREEN.

## Interpretation
SlateDB does not silently truncate the un-manifested acked WAL tail under a
false-negative HEAD — the `PutMode::Create` fence walk and the loud replay-read
failure both prevent it. The control (truthful) reopen reads the full acked set,
proving the bytes are durable. No data-loss finding. The residual is an
availability-flavored loud failure under a **contract-violating** object store →
follow-up corridor `retrying-store-writer-open` (does #1909's RetryingObjectStore
wrap the writer-open frontier path?).
