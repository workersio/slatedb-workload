---
key: durable-ack-survives-crash
area: durability
title: Durable acks survive crash and restart
claim: >-
  Every write acked with await_durable=true (the default) is readable with its
  correct value after a process SIGKILL and reopen — no acknowledged write is
  ever lost.
status: active
provenance: "README.md:19; slatedb/src/config.rs:474-484 (WriteOptions.await_durable default true); backlog scout-docs/scout-runtime"
explorations:
  - key: durable-ack-baseline
    title: Durable ack baseline
    description: >-
      No faults. Proves the driver + oracle observe the invariant at all: every
      acked key is present with the right value after a clean close+reopen.
    status: done
    result: green
    reason: null
    workload: workloads/durable_ack.py
    command: python3 .workers/workloads/durable_ack.py --case baseline
    faults: []
    depth: 10
    replay: {run: "01KX5Y6CBAG0JDG3GA8CQ38KKD", case: baseline, seed: 3}
    freshness: new-current
    reported: null
    published: nd773r0vv83vyqgz37gawwy7s98a889n
  - key: durable-ack-crash-mid-flush
    title: SIGKILL mid-flush, acked writes survive
    description: >-
      Writer streams acked (await_durable=true) puts against LocalFileSystem;
      the driver is SIGKILLed at seed-swept points spanning flush boundaries,
      then reopened. Every key the driver recorded as durably-acked before the
      kill must be present with the correct value after recovery.
    status: done
    result: green
    reason: null
    workload: workloads/durable_ack.py
    command: python3 .workers/workloads/durable_ack.py --case crash-mid-flush
    faults: [process-kill]
    depth: 20
    replay: {run: "nd7c90vthyg00w26fym9v10r4n8a862b", case: crash-mid-flush, seed: 5}
    freshness: new-current
    reported: null
    published: null
  - key: durable-ack-wal-head-contiguity
    title: HEAD-contiguity frontier truncates replay
    description: >-
      On reopen, a guest ObjectStore wrapper returns 404 on the HEAD/exists
      probe for ONE WAL id while that object is durably present. The reopen
      frontier search (tablestore.rs:163-273) assumes existence is
      monotone-decreasing in id (tablestore.rs:177-179) and binary-searches the
      max; a false-negative HEAD makes it return a max BELOW the true one, so
      replay silently truncates the durable acked suffix — data-loss. Attacks
      the fresh last_seen_wal_id refactor (#1885/#1882). (The earlier
      "drop one WAL PUT mid-stream" framing was certified guaranteed-green: the
      single sequential flusher (wal_buffer.rs:308-316) never acks and never
      writes id>N on a dropped PUT, so no gap and no lost ack arise.)
    status: ready
    result: null
    reason: null
    workload: workloads/durable_ack.py
    command: python3 .workers/workloads/durable_ack.py --case wal-head-contiguity
    faults: [objectstore-head-false-negative, process-kill]
    depth: 20
    replay: null
    freshness: new-current
    reported: null
    published: null
---
# Durable acks survive crash and restart

## The claim and its invariant

README:19 — `put()` "returns a Future that resolves when the data is durably
persisted." Default `WriteOptions.await_durable=true` (`config.rs:474-484`). The
falsifiable invariant is set-inclusion:

> Let `A` = the set of (key,value) the driver observed acked with
> `await_durable=true` before a crash. Let `R` = the state readable after
> reopen. Then `A ⊆ R` (value-exact). Any acked key missing or with a stale
> value after recovery is a **data-loss** finding (severity weight 4).

Do **not** assert anything about `await_durable=false` writes surviving — that
is explicitly *not* promised; those may legitimately vanish (they belong to a
separate weaker-promise exploration, not this one).

## Adversarial model & fault dimensions

- **crash-mid-flush** — `flush_interval` default 100ms local (`config.rs:978`;
  `SL8_FLUSH_INTERVAL` overrides). The kill must be armed at seed-swept virtual
  points (via `.workers/lib/crashclock.py`) that straddle flush boundaries, so
  acks land both before and after WAL-SST persistence. Reachable window
  (scout-runtime): after WAL flush before L0; after L0 upload before manifest
  commit — SIGKILL of a separate process on a shared LocalFileSystem needs no
  internal hooks.
- **wal-head-contiguity** (strategy-critic-reframed) — a guest-authored
  `Arc<dyn ObjectStore>` wrapper (in the driver crate, wrapping LocalFileSystem)
  returns a **false-negative HEAD/exists** for exactly one WAL id during reopen,
  while the object is durably present on disk. The reopen frontier search
  (`tablestore.rs:163-273`) assumes WAL-object existence is monotone-decreasing
  in id (`tablestore.rs:177-179`) and binary-searches the max id; a false HEAD
  makes it settle on a max **below** the true one → `wal_replay.rs` replays only
  a truncated prefix → the acked suffix is silently lost (**data-loss**). This
  is the reachable version of the WAL-refactor seam (#1885/#1882); the naive
  "drop one WAL PUT" was certified guaranteed-green because the single sequential
  flusher (`wal_buffer.rs:308-316`) neither acks nor writes higher ids after a
  failed PUT, so no gap and no lost ack can arise. The fault wrapper must inject
  at the HEAD/`exists` layer (not just PUT) to reach this corner.

## Oracle

Bespoke: `A ⊆ R` value-exact diff after reopen (the driver's `verify`
subcommand replays the recorded ack manifest against the reopened DB).

Universal oracle plane (per executor contract):
- **Liveness watchdog** — global deadline; a hung reopen/replay is a FAIL, not
  a timeout artifact.
- **Terminal-state sweep** — every acked key must resolve to present-or-absent
  after recovery; a driver that exits without a verdict is a FAIL.
- **Acked-durability watch** (`.workers/lib/durawatch.py`) — manifest the acked
  set and re-observe on a delay ladder after reopen to catch *delayed* erasure
  (a key present immediately but GC'd/compacted away shortly after).
- **Declared fault timing** (`.workers/lib/crashclock.py`) — the SIGKILL arms
  at seed-swept points in a declared timing space, never at a magic sleep.

## Workload plan — the driver binary (executor build, greenfield)

`.workers/workloads/` is empty; this is the first workload and it introduces the
**SlateDB workload-driver binary** that every crash/durability/fencing/clone
candidate reuses. Executor tasks:

1. **Driver crate** — a small Rust bin (`.workers/driver/` or a workspace bin)
   depending on `slatedb` by path. Subcommands:
   - `run --root <dir> --ops <file|-> --ack-log <path>` — open
     `Db::builder(path, LocalFileSystem::new_with_prefix(root))` (see
     `examples/src/db_without_compactor.rs:12,20`), execute a seeded op stream
     (put/get/delete/write-batch, `await_durable` per op), append each durable
     ack `(seq,key,value)` to the ack-log as it resolves, and flush on the
     crashclock signal boundary. Must be SIGKILL-safe (no graceful cleanup).
   - `verify --root <dir> --ack-log <path>` — reopen the same store (identical
     feature config) and assert `A ⊆ R` value-exact; emit
     `INVARIANT durable_ack_subset FAIL/PASS`.
   - For `wal-head-contiguity`: a `--head-false-negative <wal_id>` flag installs
     the guest ObjectStore wrapper that 404s the HEAD/`exists` probe for that id
     on reopen while the object stays present.
   - **Ack-log crash-safety (mandatory — else false GREENs):** write **and
     fsync** each ack-log entry the instant the `put` future resolves, before
     issuing the next op; keep the ack-log **outside the DB root** (the fault
     wrapper must not corrupt it); do **no** graceful cleanup on the crash path.
     The DB-durable-but-ack-log-unfsync'd window can only shrink `A`, so an
     unfsync'd log silently hides data-loss — per-ack fsync is not optional.
   - The ack must be recorded only from the resolved `put` future
     (`notify_durable(Ok)`, `wal_buffer.rs:334-338`), never from `put` return.
2. **Build path** — vendored musl static with `default-features = false`:
   `rustup target add x86_64-unknown-linux-musl`, depend on
   `slatedb = { path = "…", default-features = false }` (drop `aws`+`foyer` —
   they pull ring/openssl/io_uring that fight musl static; LocalFileSystem +
   InMemory need only base object_store), then
   `cargo build --release --target x86_64-unknown-linux-musl`, vendor to
   `.workers/vendor/bin/slatedb-driver`, `build.sh` verifies+chmods. **Fallback**
   (only if a transitive pure-Rust dep still balks): build-in-image via
   `build.sh` running cargo — check `wio projects get` carries rust 1.91.1 first.
   Record the realized path in `map.md` §"SUT driver".
3. **Python harness** — `workloads/durable_ack.py` generates the seeded op
   stream, spawns the driver, arms the crashclock SIGKILL (skip for
   `--case baseline`), reopens in `verify` mode, and emits the universal-plane
   oracles. Import `.workers/lib/{crashclock,durawatch,genlib}.py`.

## Execution / evidence notes

(executor appends: chosen build path, first green baseline run id, any red +
replay seed, reality notes on flush timing and ack-vs-persist ordering.)
