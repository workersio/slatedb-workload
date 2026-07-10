# Run evidence — fencing-split-brain-overlap-writes

**Promise:** writer-fencing-split-brain · **Area:** fencing
**Exploration:** fencing-split-brain-overlap-writes
**On-box verdict:** VOID (fence airtight — post-fence durable-write window never opened
across 49 seeds); oracle proven non-vacuous by ORACLE_SELFTEST (RED).

## What it tests
Victim (tag `A`) and usurper (tag `B`) write the SAME cycled keyspace `k0..k{N-1}`
(`N=5`) concurrently across the B-open fence point. Values encode writer identity +
local seq (`A:{seq}:{key}:{noise}` / `B:...`) so every durable value on final reopen
is attributable to its writer. After both settle, reopen (`dump`) and assert a valid
single-writer history:
  * `fencing_no_zombie_write` FAIL if any POST-FENCE victim value is the durable
    winner for its key (a superseded writer's write survived — split-brain).
  * `fencing_usurper_writes_survive` FAIL if any key the usurper durably acked is
    missing / shows a non-usurper value on reopen (a committed winner write lost or
    resurrected to a victim value).
Plus universal plane: `liveness_watchdog`, `terminal_state`.

## The oracle's partition rule (defensible, non-vacuous)
The usurper fsync's an OPEN-MARKER carrying the wall-clock nanos of the instant its
`open()` returned — the epoch is already bumped by then (`FenceableManifest::init_writer`,
manifest/store.rs:34). Every victim `ok` durable ack carries its own resolve-nanos
(4th ack-log field). A victim ack is a POST-FENCE SUSPECT iff `resolve_nanos >=
open_nanos` — it durably acked a write AFTER a superseded epoch was in effect. The
cutoff is `open()`-return, which is strictly AFTER the actual epoch bump, so the
partition is CONSERVATIVE toward the bump (never over-counts suspects → no false RED).
Cross-process wall clock on one host at ms granularity is the only comparable timebase
(Instant epochs differ per process); no python poll latency enters the partition.
Anti-vacuity: VOID unless >=1 post-fence suspect AND >=1 contended key.

## Source fact — WHEN the victim's write stops landing durably
The fence is enforced at the WAL-SST write, not (only) the manifest poll. WAL SSTs are
written `PutMode::Create` (tablestore.rs:1125); a colliding id → `AlreadyExists` →
`SlateDBError::Fenced` (tablestore.rs:1133-1136). The usurper's `WriterFencer::fence`
writes a zero-byte barrier at `empty_wal_id` and its loop ADVANCES that id past any id
the incumbent grabs (fence.rs:145-171), so the barrier sits at the victim's *next* WAL
id. The victim's very next flush therefore collides and is Fenced. A durable ack that
*causally* follows the epoch bump is thus mechanically prevented: any committed victim
write is BELOW the barrier (incorporated into the usurper's `replay_range`,
fence.rs:165) and superseded by the usurper's higher-seq writes; any post-barrier
victim flush is rejected. **The flush is rejected — it does not persist.**

## On-box smoke (paste)
Selftest — plants a durable post-fence victim value at a usurper key via `put-kv`
(fresh open, newest epoch, unconditional winner) then runs the real oracle:
```
ORACLE_SELFTEST: planting durable post-fence victim value k2=A:SELFTEST:k2:deadbeefdeadbeef
DUMP key=k2 value=A:SELFTEST:k2:deadbeefdeadbeef
INVARIANT fencing_no_zombie_write superseded-writer-value-not-durable FAIL suspects=1 zombies=1 contended=4
ZOMBIE key=k2 durable_value=A:SELFTEST:k2:deadbeefdeadbeef (... SPLIT-BRAIN)
INVARIANT fencing_usurper_writes_survive winner-writes-not-lost FAIL usurper_keys=5 lost_or_overwritten=1
VERDICT: RED — SPLIT-BRAIN / LOST-UPDATE ...   (exit 1)
```
Natural runs — seeds {1,2,3,5,7,13,21,42,100} + a 40-seed sweep = 49 seeds, ALL VOID:
```
OBSERVED usurper_opened=True ... victim_ok_after_prelude=4 victim_fenced=True
         fenced_attempt=4 post_fence_suspects=0 contended_keys=5
VERDICT: VOID — the adversarial race did not happen this seed: post_fence_suspects=0 ...
         The victim landed ZERO durable acks after the usurper's epoch bump — the
         WAL-barrier fence (fence.rs:145 PutMode::Create) rejected its next flush.
```
Per seed the victim landed 2-4 durable `ok` acks in the attempt loop, but EVERY one
resolved BEFORE `open_nanos` (all pre-open); the first flush at/after the epoch bump
was Fenced. Reopen (`dump`) showed all contended keys `k0..k4` = the usurper's B
values, i.e. a valid single-writer history (the victim's pre-open writes correctly
superseded). GREEN-branch logic separately validated on real logs by forcing the
victim oks as suspects: zombies=[], usurper_lost=[] → GREEN.

## Interpretation
On this box's fast LocalFileSystem the fence is instantaneous at the WAL layer: no
durable victim ack ever resolves after the usurper's epoch bump, so the zombie path is
never exercised (VOID, per the anti-vacuity floor — NOT a green, NOT a red). This is
tighter than the in-guest baseline, which observed one `ok` `await_durable=true` write
whose ACK wall-clock-followed the usurper's open (`victim_ok_after_prelude=1`); under
this oracle's `open_nanos` cutoff that write WOULD be a suspect, so the in-guest
environment (slower I/O widening the fence.rs:167 retry window) is where this rung
actually adjudicates the zombie oracle. The oracle is proven correct and non-vacuous
by the selftest; no split-brain / lost-update was found on-box.

## In-guest confirmation (executor #8, exploration nd7d9wr18jjbcbnvr9ffs1gdgd8a8vhx, depth 10)
All 10 in-guest runs VOID — `post_fence_suspects=0` every seed (e.g. seed
1906796196: victim landed 3 `ok` acks but ALL resolved before the usurper's
`open_nanos`; attempt 3 was Fenced). The zombie window never opens in-guest
either. **Conclusion:** SlateDB's fence is airtight — no durable victim write
survives (or even lands) after the usurper's epoch bump. The split-brain/zombie
corridor is structurally unreachable via "victim keeps writing after usurper
opens" (`fence.rs:145-171` advances the WAL barrier past any id the incumbent
grabs → its next flush is Fenced via `PutMode::Create`, `tablestore.rs:1125,1133`;
any already-committed victim write is below the barrier, folded into the
usurper's replay_range `fence.rs:165`, then superseded). Recorded `blocked`
(anti-vacuity VOID, not a hollow green). The remaining reachable fence-boundary
variant — a victim flush already IN FLIGHT when the barrier lands — is
`stale-epoch-flush` (rung 3). Producer to triage: retire or fold into rung 3.
