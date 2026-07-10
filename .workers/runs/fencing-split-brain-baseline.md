# Run evidence — fencing-split-brain-baseline

**Promise:** writer-fencing-split-brain · **Area:** fencing
**Exploration:** fencing-split-brain-baseline · **Verdict:** GREEN
(second open fences the first; non-vacuity control)

## What it proves
Two processes on one `LocalFileSystem` root: `fence-victim` opens + writes,
`fence-usurper` opens the SAME root (bumps the manifest epoch). The victim's
post-open `await_durable` write returns `Fenced` and the usurper's writes are
durable. Verified surface: a superseded put returns
`ErrorKind::Closed(CloseReason::Fenced)` (`error.rs:115,618`; the crate's own
`test_fence` asserts exactly this, `fence.rs:342-354`). Fence = epoch bump + a
zero-byte WAL barrier (`fence.rs:105`). Driver sets `manifest_poll_interval`
100ms (`config.rs:647`) for prompt detection.

## Evidence (in-guest, exploration nd7ejybh1y7kg4tzmgr85btthn8a9x1q, depth 5, all succeeded)
```
FENCE_OBSERVED attempt=0 seq=1 result=ok       ← one ok write AFTER usurper opened
FENCE_OBSERVED attempt=1 seq=2 result=fenced
OBSERVED usurper_opened=True usurper_acks=1 victim_ok_after_prelude=1 victim_fenced=True
INVARIANT fencing_victim_fenced PASS attempts_run=2 ok_after_prelude=1 fenced_attempt=1
INVARIANT fencing_usurper_durable PASS checked=5 lost=0 mismatch=0
```
Local GREEN seeds 1,2,7,42,100,3 (there the fence fired on attempt 0). Selftest
(`ORACLE_SELFTEST=1`) forces no-fence → `fencing_victim_fenced FAIL` (split-brain
red path proven).

## REALITY NOTE → feeds the overlap-writes rung (a real lead)
In-guest the fence is **not instantaneous**: the victim landed **one `ok`
`await_durable=true` write (seq 2)** AFTER the usurper had already opened+acked,
before its next attempt was fenced (`victim_ok_after_prelude=1`). The baseline
only checks that the victim is *eventually* fenced (PASS). **Whether that
post-open `ok` write is a durable ZOMBIE that survives against the usurper's
history is exactly the `fencing-split-brain-overlap-writes` target** — if a
victim write that returned Ok after the epoch bump is durably visible after
reopen, that is a split-brain / lost-update finding. The overlap-writes oracle
must reopen and check the final state is a valid single-writer history, treating
any surviving post-fence victim value as a RED.

## Interpretation
Baseline control passes. SlateDB fences the incumbent on a second open. The
post-open `ok`-write window is the concrete seam for the adversarial rung — a
strong lead the baseline surfaced.
