# Executor playbook — SlateDB harness (environment quirks & replay recipes)

## Guest reality (confirmed executor #1)
- Guest has `python3` + stdlib; workloads are python driving the vendored
  `.workers/vendor/bin/slatedb-driver` (static-pie musl, offline, in the image).
- The workload derives its own seed from `/dev/urandom` (deterministic per
  wio-seed in the sim) and prints `SEED <n>` first — that is the replay key.
  Pin the wio entropy with `wio simulate create … --seed <n>` (the flag exists).

## Verdict reading (CRITICAL — do not misread)
- `wio workloads ls [--violations]` is **ACCOUNT-WIDE**, not project-scoped;
  it lists every fleet target's runs. For this project use
  `wio simulate status <exploration-id> --format json`.
- A red is an emitted `INVARIANT … FAIL`, NOT a nonzero exit. A workload that
  exits nonzero shows `state: failed, failureCategory: fault_model` regardless —
  but if it emitted an `INVARIANT … FAIL` line it still registers as a
  violation and appears in `wio workloads ls --violations`. Never treat
  `state: failed` alone as "no red"; confirm via `--violations` and the logs.
- Green baseline runs exit 0 → `state: succeeded`, absent from `--violations`.

## Official-run flow (per unit)
1. Commit + push spec/workload; `wio projects prepare <PID>`; poll
   `wio projects get` until `currentImage.commitSha == HEAD` (build.sh just
   chmods+smokes the vendored binary, so prep is fast).
2. Draft-iterate with `wio simulate create <PID> --command "…" --depth N`
   (no `--exploration` = invisible). Prove the oracle red via
   `ORACLE_SELFTEST=1` in the command before trusting any green.
3. `python3 .workers/publish.py` fires the official `--exploration <key>` run
   for every `status: done` entry (idempotent by key).

## Driver contract reminders (for the next rungs)
- Ack recorded ONLY after the `put` future resolves (durable), then
  write+**fsync** the ack-log line; ack-log lives OUTSIDE the db root.
- `--head-false-negative <wal_id>` wrapper (head_fn.rs) overrides `get_opts`
  and special-cases `options.head` for the one `.../wal/{id:020}.sst` object —
  install it on the REOPEN/verify open and sweep the target id via crashclock
  (wal-head-contiguity rung, not yet built).
- Rebuild: `rustup target add x86_64-unknown-linux-musl` then
  `cd .workers/driver && cargo build --release --target x86_64-unknown-linux-musl`,
  copy to `.workers/vendor/bin/slatedb-driver`. `driver/target/` is gitignored
  (349 MB) — `cargo clean` after big changes.
