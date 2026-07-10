# Map — SlateDB workload harness

Static evidence index. Not a queue: no owners, no claims, no priorities.

## Target

| Fact | Value |
|------|-------|
| Target repo | `slatedb/slatedb` (fork: `workersio/slatedb-workload`) |
| Pinned ref | `016b676ee125f02cb14054cce0cd5a78f3524ac5` (main, 2026-07-09) |
| System under test | **SlateDB** — embedded LSM storage engine that writes to object storage (S3/GCS/ABS/MinIO/local FS/in-memory). Workspace `version = 0.14.1`. |
| Language | Rust (toolchain 1.91.1, edition 2021) |
| wio project | `kn7d139qrs5knsawmkq1avw8s18a8se2` ("slatedb-workload", prod, preparationStatus ready) |
| wio branch | `main` |
| wio binary | `/usr/local/bin/wio` (0.4.0) and `/home/ubuntu/work/product/formal/packages/wio/target/release/wio` |
| Box toolchain | rustc 1.91.1, target `x86_64-unknown-linux-gnu` only (musl NOT installed — `rustup target add x86_64-unknown-linux-musl` before a static vendor build) |

## SUT driver — OPEN (producer/executor decision, not init)

SlateDB is a **library**, not a server, so unlike S2 there is no upstream
binary that runs a put/get workload. `slatedb-cli` (bin name `slatedb`) is an
**admin/ops** tool only — read manifest, list/read compactions, checkpoints,
GC, run-compactor — it does not drive `put`/`get`/`flush`. Driving the DB under
faults needs a **bespoke workload-driver binary** that opens a `Db`, runs a
seeded op stream (put/get/delete/scan, `await_durable` on/off, flush,
checkpoint, clone), prints an ack manifest, and survives SIGKILL/restart to
exercise recovery.

Two build paths (first producer/executor episode picks and records here):
- **Vendored static binary** (S2 pattern, proven first-try): build the driver
  here on the box as `x86_64-unknown-linux-musl` static, commit under
  `.workers/vendor/bin/`, `build.sh` just chmods it. Offline, toolchain-free
  in-image. Requires adding the musl target + a small driver crate.
- **Build in-image**: base image carries cargo 1.91.1 → `build.sh` runs
  `cargo build --release -p <driver>`. Simpler crate story, ~minutes per
  prepare, needs a Rust base image.

Object store is env-selectable via `admin::load_object_store_from_env`
(`AWS_ENDPOINT_URL_S3` + creds → any S3-compatible endpoint; `http://` URL
auto-enables allow_http). Local FS and in-memory stores are also available for
hermetic runs. A local python S3 stub is a guest-drivable dependency-fault
plane (per the S2 map).

## SlateDB reality (inherited from the S2 harness — S2-lite embeds SlateDB)

The S2 fleet target attacked SlateDB *through* s2-lite and accumulated
source-verified internals. Cross-reference `~/work/fleet/s2/repo/.workers/map.md`.
Highlights that transfer directly (verify against this repo's pinned ref
before relying):
- `put()` future resolves only when **durably persisted**; `await_durable:false`
  releases early (README:19). Ack is durability-gated on `durable_seq`.
- `SL8_*` env vars map to **any** SlateDB `Settings` field.
  `SL8_FLUSH_INTERVAL` default 5ms local/in-mem, 50ms S3-only.
  `SL8_MANIFEST_POLL_INTERVAL` also controls startup fencing sleep.
- Startup does **time-based fencing** — a new DB handle fences the prior at
  slatedb **DB-open** (manifest-epoch CAS); a superseded handle self-fences
  ("detected newer DB client" / "database closed while waiting for
  durability").
- Manifest is CAS-guarded (manifest-epoch); `finalize_trim`-style structural
  edits run as single `SerializableSnapshot` txns; SlateDB conflict-detects
  concurrent txns (`TransactionConflict`).
- Deterministic-sim `/dev/urandom` is per-run deterministic — pass explicit
  seeds per trial when distinctness matters.

## Attack surface (source dirs — first scout fan-out expands into areas)

| Dir | What |
|-----|------|
| `slatedb/` | the engine: WAL, memtable, flush, compaction, manifest, GC, checkpoints, reads |
| `slatedb-dst/` | upstream deterministic simulation testing harness — mine for oracle ideas + reachability, our axis is whole-process OS-level faults |
| `slatedb-txn-obj/` | transaction object layer — isolation/atomicity promises |
| `slatedb-cli/` | admin ops (manifest/checkpoint/GC/compactor) — a driving surface for control-plane workloads |
| `slatedb-bencher/` | benchmark harness — op-generation patterns to reuse |
| `examples/` | how to open a `Db` and drive put/get — driver skeleton source |
| `bindings/uniffi/` | FFI surface |

## Areas

| Key | Title | Spec |
|-----|-------|------|
| durability | Durability | areas/durability.md |
| consistency | Consistency & Isolation | areas/consistency.md |
| fencing | Writer Fencing | areas/fencing.md |
| clone | Clones & Checkpoints | areas/clone.md |
| gc | Garbage Collection | areas/gc.md |
| compaction | Compaction | areas/compaction.md |

## Mapping-breadth floor (module reconciliation, producer #1 fan-out 2026-07-10)

Every source module is inside an area's loci or explicitly parked.
- `slatedb/src/{wal_buffer,batch_write,wal_replay,db,tablestore}.rs` → durability
- `slatedb/src/{config,transaction_manager,db_transaction,db_reader,db_iter,reader}.rs`,
  `slatedb-txn-obj/` → consistency
- `slatedb/src/{fence,manifest/}.rs` → fencing
- `slatedb/src/{clone,checkpoint,snapshot_manager}.rs` → clone
- `slatedb/src/garbage_collector/**` → gc
- `slatedb/src/{compactor,compactor_executor,compactor_state,compaction_worker}.rs` → compaction
- `slatedb-cli/` → **not parked**: a control-plane driving surface (checkpoint/GC/
  run-compactor) usable by fencing/gc/compaction workloads (admin ops as a 2nd process).
- `slatedb-bencher/` → **parked: reference-only** — op-generation patterns to reuse, not a SUT.
- `slatedb-dst/` → **parked: reference-only** — upstream in-process determ-sim; our axis is
  whole-process OS-level faults. Mine its op/fault/oracle model, do not wrap it.
- `bindings/uniffi/` → **parked: separate-surface** (FFI; out of the durability/consistency scope).
- `examples/` → **parked: docs/skeleton** — the driver-open reference (db_without_compactor.rs, s3_compatible.rs).
- `schemas/`, `specs/`, `rfcs/`, `website/` → **parked: docs-only** (provenance sources).

## Census (budget allocation basis)

No `.workers/census.md` yet (fresh target, zero confirmed-bug history of our own).
Per producer.md §Budget allocation rule 1, with no red-rate history the split IS
the census mix; with no census either, the first batch is baseline-heavy by the
ladder floor. Git-history exposure (scout-commits) points budget at WAL/durability,
compaction, and clone churn first. Build `census.md` once the first sweeps produce
red-rate data (after the driver lands and a few explorations run).

## SUT driver — decision (producer #1, strategy-critic-verified): vendored musl static, default-features=false

Primary path: build the bespoke `slatedb-driver` bin here as
`x86_64-unknown-linux-musl` static, vendor to `.workers/vendor/bin/slatedb-driver`,
`build.sh` verifies+chmods (S2-proven, offline, toolchain-free in-image).

**Critical (strategy-critic, source-verified):** depend on slatedb with
`default-features = false`. `slatedb/Cargo.toml` default = `["aws","foyer"]`;
`aws` pulls object_store/aws → reqwest/hyper → rustls(ring)/openssl and `foyer`
pulls a C/io_uring hybrid cache — exactly the deps that fight musl static. The
crash/durability/consistency workloads only need `LocalFileSystem` + `InMemory`
(base `object_store`, pure-Rust: tokio, bytes, crc32fast) which links musl-static
cleanly. So build `slatedb = { path = "…", default-features = false }`.
Build-in-image (cargo in a rust base image) is the fallback ONLY if a transitive
pure-Rust dep still balks — feature-gating, not the base image, is the first move.

**Ack-log crash-safety (mandatory — else false GREENs):** the durability oracle
is `A ⊆ R` where `A` = the driver's recorded acked set. If the ack-log is not
fsync'd per ack, a SIGKILL in the window "DB durable, ack-log not yet fsync'd"
drops K from `A` and `A ⊆ R` holds falsely, masking real data-loss. Rules: (1)
write+fsync the ack-log entry the instant each `put` future resolves, before the
next op; (2) ack-log lives OUTSIDE the DB root so the object-store fault wrapper
cannot corrupt it; (3) driver does NO graceful cleanup on crash (SIGKILL-safe);
(4) `verify` reopens with the identical store + feature config.

Full driver contract in `promises/durable-ack-survives-crash.md` §Workload plan.
The executor records the realized build outcome (musl vs in-image) here.

## Promoted findings

| Date | Promise | Exploration | Evidence |
|------|---------|-------------|----------|
