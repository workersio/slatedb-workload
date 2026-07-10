# Run evidence — durability-filter-remote-crash-confirm

**Promise:** durability-filter-remote · **Area:** consistency
**Exploration:** durability-filter-remote-crash-confirm · **Verdict:** GREEN
(Remote's durability filter holds under crash — no wrong-durable-read)

## Attack
`remote-run` (default 100ms auto-flush) interleaves `await_durable=false` writes
(majority) with `await_durable=true` every 8th (forces flush progress), and
sweeps keys with `get_with_options(Remote)`, fsync-logging every (key,v) Remote
actually returned to a remote-observed log OUTSIDE the db root. SIGKILL the
process group after the seed-derived K-th Remote observation (ack-progress
trigger). Reopen; `verify-remote` asserts `R_remote ⊆ survivors` value-exact.
A Remote-observed value that vanishes after the crash = Remote surfaced
non-durable data = wrong-durable-read RED (weight 3).

Anti-vacuity: run must be killed mid-stream (rc≠0 else VOID), ≥1 Remote
observation, and ≥1 `await_durable=false` write issued (emits `DIVERGENCE` —
Memory/Remote genuinely diverge, else the test is vacuous).

## Reachability
Remote reads cap at the durable watermark: `reader.rs:111-113`
(`max_seq = last_remote_persisted_seq()` when filter is Remote), monotone in seq.
So a Remote read only returns durable data by construction — the crash test
confirms that construction holds across process death.

## Evidence (in-guest, exploration nd7b60mady5185xbrra7382b118a8efr, depth 5, all succeeded)
Sample (seed 309847579, K=5):
```
KILLED mode=sigkill(pg) rc=-9 remote_obs_before_kill=9 kill_after=5 kill_wall=5.810s ops=2000
DIVERGENCE await_durable_false_writes_issued>=8 — Memory/Remote diverge
VERIFY_REMOTE subset_ok=true checked=9 lost=0 mismatch=0
INVARIANT durability_filter_remote_survives_crash acked-remote-durable PASS checked=9 lost=0 mismatch=0
VERDICT: GREEN
```
Local GREEN across seeds 1-5,42,100; `ORACLE_SELFTEST=1` → `durability_filter_remote_survives_crash FAIL` (red path proven). Baseline regression still green.

## Interpretation
Every value SlateDB returned through a Remote-filter read survived an abrupt
mid-stream SIGKILL — Remote never surfaced a value a crash then lost. Expected-
correct. Remaining rung: `inflight-flush` (Remote excludes an in-flight
not-yet-durable value at the flush boundary) completes the ladder floor.
