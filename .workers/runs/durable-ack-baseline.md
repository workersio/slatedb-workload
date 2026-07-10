# Run evidence — durable-ack-baseline

**Promise:** durable-ack-survives-crash · **Area:** durability
**Exploration:** durable-ack-baseline · **Verdict:** GREEN (survived; baseline
proves the oracle observes the invariant with no faults)

## Target / build
- Target commit (pushed, prepared): `776de9136aae958ae965b6f7f3817830fbfa89e1`
- Image commitSha matched HEAD, buildStatus `succeeded`.
- SUT driver: `.workers/vendor/bin/slatedb-driver` — bespoke Rust bin, built
  `x86_64-unknown-linux-musl` static-pie (7.1 MB, stripped), `slatedb` path dep
  `default-features=false` (drops aws+foyer). Built first-try, no dep fights.

## Commands (wio, drafts — no --exploration)
- Baseline draft: `wio simulate create kn7d139qrs5knsawmkq1avw8s18a8se2 --command "python3 .workers/workloads/durable_ack.py --case baseline" --depth 3 --workload-path .workers/workloads/durable_ack.py`
  → exploration `nd79nxsh76d3shj22rkj95zn0h8a9dva`, 3 runs all `succeeded`, none in `--violations`.
- Red-proof draft (step-5 gate): `--command "sh -c 'ORACLE_SELFTEST=1 python3 .workers/workloads/durable_ack.py --case baseline'" --depth 1`
  → exploration `nd7dc20ynq6zwpx1jrbmpb52v18a9yvs`, run `01KX5YB5B27FWY0EY59H50HDMC`
  emitted `INVARIANT durable_ack_subset acked-subset-readable FAIL … bad=['SELFTEST_MISSING']`
  and **appears in `wio workloads ls --violations`** — the oracle's red path is proven.

## Invariant panel (in-guest, baseline run 01KX5Y6CBAG0JDG3GA8CQ38KKD, workload seed 2737476910)
```
INVARIANT terminal_state acked-keys-resolved PASS verify emitted verdict for 24/24 acked keys
INVARIANT durable_ack_subset acked-subset-readable PASS checked=24 lost=0 mismatch=0 bad=[]
INVARIANT liveness_watchdog global-deadline PASS verdict reached (durable_ack_subset PASS)
INVARIANT durability_watch_t0 acked-effect-durable PASS 24/24 acked effects observable at t0
INVARIANT durability_watch_t2s acked-effect-durable PASS 24/24 acked effects observable at t2s
VERDICT: GREEN — all 24 acked effects durable across 2 rungs
```

## Interpretation
- The bespoke driver runs in the guest; python3 + stdlib present; the universal
  oracle plane (liveness/terminal/durawatch) emits correctly. Baseline is the
  expected GREEN — it is not a bug hunt, it validates the harness + oracle.
- Replay: `wio simulate create … --seed 3` reproduces the seed set; the workload
  derives its own /dev/urandom seed (deterministic per wio-seed in the sim), and
  prints it first (`SEED <n>`) as the replay key.

## Reality notes (→ folded into map.md / executor-notes)
- `wio workloads ls [--violations]` is **account-wide**, not project-scoped — it
  shows other fleet targets' runs. Use `wio simulate status <exploration-id>`
  for project-scoped verdicts.
- A workload that exits nonzero shows `state: failed, failureCategory: fault_model`
  BUT an emitted `INVARIANT … FAIL` still registers as a violation (appears in
  `--violations`). Do not read `state: failed` as "no red"; check `--violations`.
- object_store 0.14: `head()` is a blanket-impl routing to
  `get_opts(_, with_head(true))` — the only seam to inject a false-negative HEAD
  is overriding `get_opts` and special-casing `options.head`. WAL object path is
  `.../wal/{id:020}.sst` (paths.rs:75). (For the wal-head-contiguity rung.)
- Driver API (HEAD 016b676): `Db::builder(path, Arc<dyn ObjectStore>).build()`
  db.rs:670; `put_with_options(k,v,&PutOptions,&WriteOptions)` db.rs:1400,
  `WriteOptions::default().await_durable==true` config.rs:474-484; `get` db.rs:842.
