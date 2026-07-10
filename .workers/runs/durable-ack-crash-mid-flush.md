# Run evidence — durable-ack-crash-mid-flush

**Promise:** durable-ack-survives-crash · **Area:** durability
**Exploration:** durable-ack-crash-mid-flush · **Verdict:** GREEN (SlateDB
recovered every durably-acked write from an abrupt mid-flush SIGKILL)

## Attack
Writer streams `await_durable=true` puts (each ack gated on `notify_durable`
after the WAL SST write, `wal_buffer.rs:334-338`). The driver is SIGKILLed
(whole process group) after the seed-derived **K-th fsync'd ack** (K ∈ [2,12]),
then the store is reopened and `A ⊆ R` checked value-exact. A = the fsync'd
ack-log prefix at kill time.

**Kill trigger = ack-progress, not wall-clock.** First in-guest attempt used a
[300ms,6s] wall window and VOIDed (0 acks by 1.7s — the deterministic sim is
~10x slower per-op than the box). Switched to "SIGKILL after the K-th fsync'd
ack": deterministic w.r.t. the SUT's real flush progress, identical on box and
guest, still mid-flush. K is seed-derived (replayable).

## Evidence (in-guest, exploration nd7c90vthyg00w26fym9v10r4n8a862b, depth 5)
All 5 runs `succeeded`, none in `--violations`. Sample (seed 3508072148):
```
CLOCK crash-mid-flush armed kind=ack_progress kill_after_acks=12 seed=3508072148
KILLED mode=sigkill(pg) rc=-9 acked_before_kill=12 kill_after=12 kill_wall=6.752s ops=2000
INVARIANT terminal_state acked-keys-resolved PASS verify emitted verdict for 12/12 acked keys after SIGKILL+reopen
INVARIANT durable_ack_subset acked-subset-readable PASS checked=12 lost=0 mismatch=0 bad=[]
INVARIANT durability_watch_t0 acked-effect-durable PASS 12/12 observable at t0
INVARIANT durability_watch_t2s acked-effect-durable PASS 12/12 observable at t2s
VERDICT: GREEN — all 12 acked effects durable across 2 rungs
```
Kill landed mid-run (rc=-9, `RUN done` never printed; 12 of 2000 ops acked).

## Red-proof (step-5 gate)
- Local: `ORACLE_SELFTEST=1 --case crash-mid-flush` → `INVARIANT durable_ack_subset FAIL bad=['SELFTEST_MISSING']`, exit 1 (after a real SIGKILL). Proven across seeds 1,2,3,5.
- In-guest: the identical `durable_ack_subset FAIL` path was proven a violation
  by the baseline selftest run (01KX5YB5…, in `--violations`).

## Interpretation
SlateDB's ack→durable gating holds under abrupt mid-flush process death: every
write whose future resolved was recoverable, no torn/stale value, no delayed
erasure on the durawatch ladder. This is the expected-correct outcome. The
genuine-bug candidate on this promise is the next rung **durable-ack-wal-head-
contiguity** (attacks the reopen frontier's HEAD-monotonicity assumption,
`tablestore.rs:177-179`), not plain crash recovery.

## Reality notes
- In-guest per-ack latency ≈ 0.56s (12 acks in 6.75s) vs ~0.1s on the box —
  any progress-based fault trigger must key on SUT-observable progress, never
  wall-clock. (Folded into executor-notes.md.)
