//! slatedb-driver — the SUT harness driver for the durable-ack promise.
//!
//! Two subcommands:
//!   run    — open a Db on a LocalFileSystem root, execute a deterministic
//!            seeded put stream with await_durable=true, and after EACH put's
//!            future resolves durable, append+fsync one `seq\tkey\tvalue` line
//!            to an ack-log kept OUTSIDE the db root.
//!   verify — reopen the same root and assert every acked (key,value) is
//!            readable value-exact (A ⊆ R). Prints LOST/MISMATCH per bad key
//!            and a final machine-readable VERIFY line. Always exits 0 — the
//!            python oracle plane decides the verdict from the printed lines.
//!
//! Crash-safety: the ack-log is line-buffered with per-line fsync so a SIGKILL
//! mid-run leaves a consistent fsync'd prefix. There is no graceful cleanup on
//! the crash path; `run` closes the Db only on the clean baseline exit.

use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::sync::Arc;

use bytes::Bytes;
use object_store::local::LocalFileSystem;
use object_store::ObjectStore;
use slatedb::config::{DurabilityLevel, PutOptions, ReadOptions, Settings, WriteOptions};
use slatedb::object_store::path::Path as OsPath;
use slatedb::{CloseReason, Db, ErrorKind};

mod block_put;
mod head_fn;

// ---------------------------------------------------------------------------
// Deterministic seeded value stream — a plain xorshift64*, no external rng.
// The op index (seq) is the key id; the value encodes (seq,key,seeded-noise)
// so verify is value-exact and a stale/rewritten value is caught.
// ---------------------------------------------------------------------------

struct XorShift64 {
    state: u64,
}

impl XorShift64 {
    fn new(seed: u64) -> Self {
        // Avoid the zero fixed-point.
        Self {
            state: seed ^ 0x9E3779B97F4A7C15,
        }
    }
    fn next_u64(&mut self) -> u64 {
        let mut x = self.state;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.state = x;
        x.wrapping_mul(0x2545F4914F6CDD1D)
    }
}

/// The (key, value) for op `seq`. Keys are unique per op (`k{seq}`) so the
/// acked set is a clean subset with no overwrite ambiguity; the value carries
/// the seq, the key, and a seeded 64-bit noise word for value-exactness.
fn op_kv(rng: &mut XorShift64, seq: u64) -> (String, String) {
    let key = format!("k{seq}");
    let noise = rng.next_u64();
    let value = format!("s{seq}:{key}:{noise:016x}");
    (key, value)
}

// ---------------------------------------------------------------------------
// Tiny hand-rolled arg parsing (keep deps minimal for musl).
// ---------------------------------------------------------------------------

fn flag<'a>(args: &'a [String], name: &str) -> Option<&'a str> {
    let mut it = args.iter();
    while let Some(a) = it.next() {
        if a == name {
            return it.next().map(|s| s.as_str());
        }
        if let Some(rest) = a.strip_prefix(name).and_then(|r| r.strip_prefix('=')) {
            return Some(rest);
        }
    }
    None
}

fn require<'a>(args: &'a [String], name: &str) -> String {
    match flag(args, name) {
        Some(v) => v.to_string(),
        None => {
            eprintln!("missing required flag {name}");
            std::process::exit(2);
        }
    }
}

fn build_object_store(root: &str, head_fn_wal_id: Option<u64>) -> Arc<dyn ObjectStore> {
    let local = LocalFileSystem::new_with_prefix(root)
        .unwrap_or_else(|e| panic!("LocalFileSystem::new_with_prefix({root}): {e}"));
    let base: Arc<dyn ObjectStore> = Arc::new(local);
    match head_fn_wal_id {
        Some(wal_id) => Arc::new(head_fn::HeadFalseNegative::new(base, wal_id)),
        None => base,
    }
}

async fn open_db(root: &str, head_fn_wal_id: Option<u64>) -> Db {
    open_db_result(root, head_fn_wal_id)
        .await
        .unwrap_or_else(|e| panic!("Db::builder({root}).build(): {e}"))
}

/// Fallible open — used by `verify` so a reopen that fails under the injected
/// false-negative HEAD is recorded as a machine-readable outcome
/// (`VERIFY_OPEN_FAILED`) rather than an opaque panic. A LOUD open failure is a
/// distinct outcome from a silently-truncated-but-successful reopen; the oracle
/// must not conflate the two.
async fn open_db_result(root: &str, head_fn_wal_id: Option<u64>) -> Result<Db, slatedb::Error> {
    let store = build_object_store(root, head_fn_wal_id);
    Db::builder(OsPath::from(root), store).build().await
}

// ---------------------------------------------------------------------------
// run
// ---------------------------------------------------------------------------

async fn cmd_run(args: &[String]) {
    let root = require(args, "--root");
    let ack_log = require(args, "--ack-log");
    let seed: u64 = require(args, "--seed").parse().expect("--seed u64");
    let ops: u64 = require(args, "--ops").parse().expect("--ops u64");
    // Baseline never sets this; wired for the wal-head-contiguity case. On the
    // WRITE path we always pass through, so passing it here only matters if a
    // caller reuses `run` to prep a store — harmless pass-through otherwise.
    let head_false_negative: Option<u64> =
        flag(args, "--head-false-negative").map(|s| s.parse().expect("--head-false-negative u64"));

    // Ack-log lives OUTSIDE the db root (the caller passes an absolute path);
    // open append so a resumed/re-run appends rather than truncating history.
    let mut log = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&ack_log)
        .unwrap_or_else(|e| panic!("open ack-log {ack_log}: {e}"));

    let db = open_db(&root, head_false_negative).await;

    let put_opts = PutOptions::default();
    // Explicit, though this is also the default: await_durable=true means the
    // put future resolves only after the write is durably committed.
    let write_opts = WriteOptions {
        await_durable: true,
        ..Default::default()
    };

    let mut rng = XorShift64::new(seed);
    for seq in 0..ops {
        let (key, value) = op_kv(&mut rng, seq);
        // put_with_options(..).await resolves ONLY when the write is durable
        // (await_durable=true). We record the ack strictly AFTER it resolves
        // Ok — never from the call being issued.
        db.put_with_options(key.as_bytes(), value.as_bytes(), &put_opts, &write_opts)
            .await
            .unwrap_or_else(|e| panic!("put seq={seq}: {e}"));

        // Durable ack observed -> append + fsync one line BEFORE the next op.
        // The DB-durable-but-log-unfsync'd window can only shrink A, so an
        // unfsync'd log would silently hide data-loss: per-ack fsync is
        // mandatory here.
        writeln!(log, "{seq}\t{key}\t{value}").expect("write ack-log");
        log.flush().expect("flush ack-log");
        log.sync_all().expect("fsync ack-log");
    }

    // Baseline clean exit: close flushes and releases. The crash cases will
    // SIGKILL before reaching here — durability must not depend on this.
    db.close().await.expect("db.close");
    println!("RUN done ops={ops} ack_log={ack_log}");
}

// ---------------------------------------------------------------------------
// verify
// ---------------------------------------------------------------------------

async fn cmd_verify(args: &[String]) {
    let root = require(args, "--root");
    let ack_log = require(args, "--ack-log");

    // Reopen with the IDENTICAL config (no head fault on the verify open for
    // baseline; the wal-head-contiguity case installs it on this open instead).
    let head_false_negative: Option<u64> =
        flag(args, "--head-false-negative").map(|s| s.parse().expect("--head-false-negative u64"));

    // A reopen that fails under the injected false-negative HEAD is a LOUD,
    // detected failure — not silent data-loss. Emit a machine-readable line and
    // exit 0 so the python oracle plane can classify it (and cross-check the
    // acked set with a fault-free control verify) instead of seeing a panic.
    let db = match open_db_result(&root, head_false_negative).await {
        Ok(db) => db,
        Err(e) => {
            println!("VERIFY_OPEN_FAILED err={e:?}");
            return;
        }
    };

    let f = File::open(&ack_log).unwrap_or_else(|e| panic!("open ack-log {ack_log}: {e}"));
    let reader = BufReader::new(f);

    let mut checked: u64 = 0;
    let mut lost: u64 = 0;
    let mut mismatch: u64 = 0;

    for line in reader.lines() {
        let line = line.expect("read ack-log line");
        let line = line.trim_end_matches('\n');
        if line.is_empty() {
            continue;
        }
        // seq \t key \t value ; split into exactly 3 (value may itself contain
        // no tabs by construction, but be defensive with splitn).
        let mut parts = line.splitn(3, '\t');
        let _seq = parts.next().unwrap_or("");
        let key = match parts.next() {
            Some(k) => k,
            None => continue,
        };
        let value = parts.next().unwrap_or("");
        checked += 1;

        match db.get(key.as_bytes()).await.expect("db.get") {
            None => {
                lost += 1;
                println!("LOST {key}");
            }
            Some(got) => {
                if got != Bytes::from(value.as_bytes().to_vec()) {
                    mismatch += 1;
                    println!("MISMATCH {key}");
                }
            }
        }
    }

    db.close().await.expect("db.close");

    let subset_ok = lost == 0 && mismatch == 0;
    if subset_ok {
        println!("OK {checked}");
    }
    println!(
        "VERIFY subset_ok={subset_ok} checked={checked} lost={lost} mismatch={mismatch}"
    );
    // Always exit 0 — the python oracle plane owns the verdict.
}

// ---------------------------------------------------------------------------
// durprobe — Memory/Remote durability-filter discrimination (non-vacuity ctrl)
//
// Deterministically demonstrates that a value written await_durable=false is:
//   * visible to a Memory-filter read (it lives in the in-memory memtable/WAL),
//   * ABSENT from a Remote-filter read (Remote gates on last_remote_persisted_seq
//     — reader.rs:112-113 — so a not-yet-flushed seq is filtered out),
//   * visible to a Remote-filter read AFTER an explicit db.flush().
//
// Determinism: open with flush_interval=None so an await_durable=false write is
// NEVER auto-flushed to object storage until we call db.flush() explicitly. With
// the default 100ms flush the dirty window is a race; disabling it makes the
// Memory/Remote divergence a hard invariant, not a timing artifact.
//
// Emits one machine-readable DURPROBE line per key plus a DURPROBE_SUMMARY.
// Always exits 0 — the python oracle plane owns the verdict.
// ---------------------------------------------------------------------------

async fn open_db_no_auto_flush(root: &str) -> Db {
    let store = build_object_store(root, None);
    // flush_interval=None disables automatic flushing: an await_durable=false
    // write stays in-memory (Memory-visible, Remote-invisible) until db.flush().
    // (config.rs:633-634; exercised by db.rs::test_no_flush_interval.)
    let settings = Settings {
        flush_interval: None,
        ..Default::default()
    };
    Db::builder(OsPath::from(root), store)
        .with_settings(settings)
        .build()
        .await
        .unwrap_or_else(|e| panic!("Db::builder({root}).with_settings(..).build(): {e}"))
}

fn hit(got: &Option<Bytes>, expected: &str) -> bool {
    matches!(got, Some(b) if b.as_ref() == expected.as_bytes())
}

fn hitstr(h: bool) -> &'static str {
    if h {
        "hit"
    } else {
        "miss"
    }
}

async fn cmd_durprobe(args: &[String]) {
    let root = require(args, "--root");
    let seed: u64 = require(args, "--seed").parse().expect("--seed u64");
    let keys: u64 = flag(args, "--keys")
        .map(|s| s.parse().expect("--keys u64"))
        .unwrap_or(8);

    let db = open_db_no_auto_flush(&root).await;

    let put_opts = PutOptions::default();
    // The core: await_durable=false returns BEFORE the write is object-store
    // durable. With flush_interval=None it stays purely in-memory until flush().
    let write_opts = WriteOptions {
        await_durable: false,
        ..Default::default()
    };
    let read_memory = ReadOptions {
        durability_filter: DurabilityLevel::Memory,
        ..Default::default()
    };
    let read_remote = ReadOptions {
        durability_filter: DurabilityLevel::Remote,
        ..Default::default()
    };

    // Materialize the deterministic (key,value) stream up front so the
    // after-flush Remote re-read checks the exact same values.
    let mut rng = XorShift64::new(seed);
    let kvs: Vec<(String, String)> = (0..keys).map(|seq| op_kv(&mut rng, seq)).collect();

    let mut mem_dirty_hits: u64 = 0;
    let mut remote_dirty_hits: u64 = 0;
    // Per-key dirty-window observations (printed after the flushed re-read so
    // each DURPROBE line carries all three fields together).
    let mut mem_dirty: Vec<bool> = Vec::with_capacity(kvs.len());
    let mut remote_dirty: Vec<bool> = Vec::with_capacity(kvs.len());

    for (key, value) in &kvs {
        // 1. put await_durable=false — returns before durable.
        db.put_with_options(key.as_bytes(), value.as_bytes(), &put_opts, &write_opts)
            .await
            .unwrap_or_else(|e| panic!("put {key}: {e}"));

        // 2. Memory read — EXPECT present (in-memory, seq <= last_committed_seq).
        let mem = db
            .get_with_options(key.as_bytes(), &read_memory)
            .await
            .unwrap_or_else(|e| panic!("get(memory) {key}: {e}"));
        let mem_h = hit(&mem, value);
        if mem_h {
            mem_dirty_hits += 1;
        }
        mem_dirty.push(mem_h);

        // 3. Remote read — EXPECT absent (seq > last_remote_persisted_seq).
        let rem = db
            .get_with_options(key.as_bytes(), &read_remote)
            .await
            .unwrap_or_else(|e| panic!("get(remote) {key}: {e}"));
        let rem_h = hit(&rem, value);
        if rem_h {
            remote_dirty_hits += 1;
        }
        remote_dirty.push(rem_h);
    }

    // 4. Make the whole dirty window durable, then re-read each with Remote.
    db.flush()
        .await
        .unwrap_or_else(|e| panic!("db.flush(): {e}"));

    let mut remote_flushed_hits: u64 = 0;
    for (i, (key, value)) in kvs.iter().enumerate() {
        let rem_after = db
            .get_with_options(key.as_bytes(), &read_remote)
            .await
            .unwrap_or_else(|e| panic!("get(remote,post-flush) {key}: {e}"));
        let raf_h = hit(&rem_after, value);
        if raf_h {
            remote_flushed_hits += 1;
        }
        println!(
            "DURPROBE key={key} memory_dirty={} remote_dirty={} remote_after_flush={}",
            hitstr(mem_dirty[i]),
            hitstr(remote_dirty[i]),
            hitstr(raf_h),
        );
    }

    db.close().await.expect("db.close");

    println!(
        "DURPROBE_SUMMARY keys={} mem_dirty_hits={mem_dirty_hits} \
         remote_dirty_hits={remote_dirty_hits} remote_flushed_hits={remote_flushed_hits}",
        kvs.len(),
    );
}

// ---------------------------------------------------------------------------
// remote-run — the crash-confirm producer.
//
// Combines `run`'s crash-safe fsync'd side-log with `durprobe`'s Remote reads.
// Opens the Db with DEFAULT settings (flush_interval=Some(100ms) — config.rs:978)
// so this is a realistic mix: most writes are await_durable=false (in-memory,
// not yet durable) and a minority await_durable=true (forces flush progress), and
// the 100ms auto-flush steadily promotes older writes to Remote-durable.
//
// Each op writes a fresh unique key (k{seq}), then sweeps the not-yet-logged keys
// oldest-first with a `get_with_options(.., Remote)` read. Remote visibility is a
// per-seq watermark (last_remote_persisted_seq — reader.rs:111-113), so the
// not-yet-durable keys are always a contiguous suffix: we advance a cursor and
// stop at the first Remote miss. Whenever Remote returns Some(value) value-exact,
// we append+fsync `(seq,key,value)` to a remote-observed log kept OUTSIDE the db
// root — the SAME crash-safety rule as the ack-log: per-observation fsync, so a
// SIGKILL mid-stream leaves a trustworthy fsync'd R_remote prefix. ONLY values
// Remote actually returned are logged.
//
// There is no clean exit on the crash path; the caller SIGKILLs mid-stream.
// ---------------------------------------------------------------------------

async fn cmd_remote_run(args: &[String]) {
    let root = require(args, "--root");
    let remote_log = require(args, "--remote-log");
    let seed: u64 = require(args, "--seed").parse().expect("--seed u64");
    let ops: u64 = require(args, "--ops").parse().expect("--ops u64");
    // Every DURABLE_EVERY-th op (seq>0) is await_durable=true — a minority of the
    // stream, present only to force flush progress. All other ops are
    // await_durable=false so Memory and Remote genuinely diverge.
    let durable_every: u64 = flag(args, "--durable-every")
        .map(|s| s.parse().expect("--durable-every u64"))
        .unwrap_or(8);

    // Remote-observed log lives OUTSIDE the db root (the caller passes an absolute
    // path); append so a re-run appends rather than truncating history.
    let mut log = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&remote_log)
        .unwrap_or_else(|e| panic!("open remote-log {remote_log}: {e}"));

    // DEFAULT settings (flush_interval=Some(100ms)) — NOT flush_interval=None. We
    // WANT a realistic durable/not-yet-durable mix here, not the hard divergence.
    let db = open_db(&root, None).await;

    let put_opts = PutOptions::default();
    let write_dirty = WriteOptions {
        await_durable: false,
        ..Default::default()
    };
    let write_durable = WriteOptions {
        await_durable: true,
        ..Default::default()
    };
    let read_remote = ReadOptions {
        durability_filter: DurabilityLevel::Remote,
        ..Default::default()
    };

    let mut rng = XorShift64::new(seed);
    // Every (key,value) written so far, in seq order (keys unique → no overwrite).
    let mut kvs: Vec<(String, String)> = Vec::new();
    // Cursor: index of the oldest key not yet Remote-observed+logged. Remote
    // visibility is monotone in seq, so [cursor..] is exactly the not-yet-durable
    // suffix; we never need to re-check a logged key.
    let mut cursor: usize = 0;

    for seq in 0..ops {
        let (key, value) = op_kv(&mut rng, seq);
        let durable = durable_every > 0 && seq > 0 && seq % durable_every == 0;
        let wo = if durable { &write_durable } else { &write_dirty };
        db.put_with_options(key.as_bytes(), value.as_bytes(), &put_opts, wo)
            .await
            .unwrap_or_else(|e| panic!("put seq={seq}: {e}"));
        kvs.push((key, value));

        // Sweep the not-yet-logged suffix oldest-first; log every value Remote now
        // returns, and STOP at the first Remote miss (nothing after it is durable
        // yet either — the Remote watermark is monotone in seq).
        while cursor < kvs.len() {
            let (k, v) = &kvs[cursor];
            let got = db
                .get_with_options(k.as_bytes(), &read_remote)
                .await
                .unwrap_or_else(|e| panic!("get(remote) {k}: {e}"));
            match got {
                Some(b) if b.as_ref() == v.as_bytes() => {
                    // Remote surfaced this value → the SUT is asserting it is
                    // durable in object storage → a crash MUST NOT lose it.
                    // Append+fsync BEFORE advancing (crash-safe prefix).
                    writeln!(log, "{seq}\t{k}\t{v}").expect("write remote-log");
                    log.flush().expect("flush remote-log");
                    log.sync_all().expect("fsync remote-log");
                    cursor += 1;
                }
                // A value-mismatch under Remote (impossible with unique keys) or a
                // miss: stop the sweep — the suffix is not yet durable.
                _ => break,
            }
        }
    }

    // Clean exit only on the (rare) non-crash path — durability must not rely on it.
    db.close().await.expect("db.close");
    println!("REMOTE_RUN done ops={ops} remote_log={remote_log}");
}

// ---------------------------------------------------------------------------
// verify-remote — reopen the (SIGKILLed) root and assert R_remote ⊆ survivors.
//
// Every (key,value) the remote-observed log recorded MUST be present value-exact
// in the recovered DB. A logged Remote value that is now missing/stale = the SUT
// surfaced non-durable data through Remote = wrong-durable-read. Prints LOST/
// MISMATCH per bad key and a machine-readable VERIFY_REMOTE line. Always exits 0
// — the python oracle plane owns the verdict.
// ---------------------------------------------------------------------------

async fn cmd_verify_remote(args: &[String]) {
    let root = require(args, "--root");
    let remote_log = require(args, "--remote-log");

    let db = match open_db_result(&root, None).await {
        Ok(db) => db,
        Err(e) => {
            println!("VERIFY_REMOTE_OPEN_FAILED err={e:?}");
            return;
        }
    };

    let f = File::open(&remote_log)
        .unwrap_or_else(|e| panic!("open remote-log {remote_log}: {e}"));
    let reader = BufReader::new(f);

    let mut checked: u64 = 0;
    let mut lost: u64 = 0;
    let mut mismatch: u64 = 0;

    for line in reader.lines() {
        let line = line.expect("read remote-log line");
        let line = line.trim_end_matches('\n');
        if line.is_empty() {
            continue;
        }
        let mut parts = line.splitn(3, '\t');
        let _seq = parts.next().unwrap_or("");
        let key = match parts.next() {
            Some(k) => k,
            None => continue,
        };
        let value = parts.next().unwrap_or("");
        checked += 1;

        // Default (Memory) read on the reopened DB reads the recovered/durable
        // state — exactly the "surviving state" the invariant is about.
        match db.get(key.as_bytes()).await.expect("db.get") {
            None => {
                lost += 1;
                println!("LOST {key}");
            }
            Some(got) => {
                if got != Bytes::from(value.as_bytes().to_vec()) {
                    mismatch += 1;
                    println!("MISMATCH {key}");
                }
            }
        }
    }

    db.close().await.expect("db.close");

    let subset_ok = lost == 0 && mismatch == 0;
    if subset_ok {
        println!("OK {checked}");
    }
    println!(
        "VERIFY_REMOTE subset_ok={subset_ok} checked={checked} lost={lost} mismatch={mismatch}"
    );
    // Always exit 0 — the python oracle plane owns the verdict.
}

// ---------------------------------------------------------------------------
// inflight-probe — Remote excludes an in-flight (not-yet-durable) value at the
// exact flush boundary where the WAL SST PUT has been issued but has NOT yet
// completed.
//
// Mechanism: open with flush_interval=None (no auto-flush) behind a BlockWalPut
// wrapper that HOLDS the WAL SST PUT (put_opts on .../wal/{id}.sst) in-flight
// until released. Write N await_durable=false keys (in the WAL buffer, not
// durable). Arm the block, then trigger db.flush() on a background task — its
// WAL PUT enters the wrapper and blocks. WHILE the PUT is blocked (the value's
// WAL object is provably not yet persisted — a crash here loses it), read every
// key with DurabilityLevel::Remote: each MUST be a miss (the Remote watermark,
// last_remote_persisted_seq == last_durable_seq, only advances AFTER write_sst
// returns Ok and WalFlushed fires — wal_buffer.rs:326/335-338, db.rs:2070). Then
// release the PUT, let flush complete, and re-read Remote: each MUST now hit.
//
// Emits one INFLIGHT line per key + an INFLIGHT_SUMMARY. put_was_blocked=true
// asserts the fault actually armed (>=1 WAL PUT caught in-flight); a green with
// put_was_blocked=false is vacuous and the python plane VOIDs it. Always exits 0.
// ---------------------------------------------------------------------------

async fn open_db_blocking_wal_put(root: &str) -> (Db, Arc<block_put::PutGate>) {
    let local = LocalFileSystem::new_with_prefix(root)
        .unwrap_or_else(|e| panic!("LocalFileSystem::new_with_prefix({root}): {e}"));
    let base: Arc<dyn ObjectStore> = Arc::new(local);
    let gate = block_put::PutGate::new();
    let store: Arc<dyn ObjectStore> = Arc::new(block_put::BlockWalPut::new(base, gate.clone()));
    // flush_interval=None: an await_durable=false write is NEVER auto-flushed to
    // object storage until we explicitly db.flush() — so the ONLY WAL PUT is the
    // one we deliberately block, and the in-flight window is deterministic.
    let settings = Settings {
        flush_interval: None,
        ..Default::default()
    };
    let db = Db::builder(OsPath::from(root), store)
        .with_settings(settings)
        .build()
        .await
        .unwrap_or_else(|e| panic!("Db::builder({root}).with_settings(..).build(): {e}"));
    (db, gate)
}

async fn cmd_inflight_probe(args: &[String]) {
    let root = require(args, "--root");
    let seed: u64 = require(args, "--seed").parse().expect("--seed u64");
    let keys: u64 = flag(args, "--keys")
        .map(|s| s.parse().expect("--keys u64"))
        .unwrap_or(8);

    let (db, gate) = open_db_blocking_wal_put(&root).await;

    let put_opts = PutOptions::default();
    let write_dirty = WriteOptions {
        await_durable: false,
        ..Default::default()
    };
    let read_remote = ReadOptions {
        durability_filter: DurabilityLevel::Remote,
        ..Default::default()
    };

    // Deterministic (key,value) stream — same generator as durprobe.
    let mut rng = XorShift64::new(seed);
    let kvs: Vec<(String, String)> = (0..keys).map(|seq| op_kv(&mut rng, seq)).collect();

    // 1. Write every key await_durable=false — lands in the WAL buffer, NOT durable.
    for (key, value) in &kvs {
        db.put_with_options(key.as_bytes(), value.as_bytes(), &put_opts, &write_dirty)
            .await
            .unwrap_or_else(|e| panic!("put {key}: {e}"));
    }

    // 2. Arm the WAL-PUT block, then trigger the flush on a background task. Its
    //    WAL SST PUT enters the wrapper and blocks in-flight.
    gate.arm();
    let db_flush = db.clone();
    let flush_handle = tokio::spawn(async move { db_flush.flush().await });

    // 3. Wait until the WAL PUT is actually blocked in-flight (fault armed).
    let put_was_blocked = gate
        .wait_entered(std::time::Duration::from_secs(10))
        .await;

    // 4. WHILE the PUT is blocked, Remote MUST exclude every key (value not yet
    //    durably persisted — a crash now loses it).
    let mut during_block: Vec<bool> = Vec::with_capacity(kvs.len());
    let mut during_block_hits: u64 = 0;
    for (key, value) in &kvs {
        let got = db
            .get_with_options(key.as_bytes(), &read_remote)
            .await
            .unwrap_or_else(|e| panic!("get(remote,during-block) {key}: {e}"));
        // If the fault never armed, put_was_blocked=false and these reads are not
        // a real in-flight observation; the python plane VOIDs that case. We still
        // record what Remote returned for transparency.
        let h = put_was_blocked && hit(&got, value);
        if h {
            during_block_hits += 1;
        }
        during_block.push(h);
    }

    // 5. Release the PUT and let the flush complete durably.
    gate.release();
    let flush_res = flush_handle.await.expect("flush task join");
    flush_res.unwrap_or_else(|e| panic!("db.flush(): {e}"));

    // 6. After release, Remote MUST now return every key (WAL object landed, the
    //    watermark advanced past the batch).
    let mut after_release: Vec<bool> = Vec::with_capacity(kvs.len());
    let mut after_release_hits: u64 = 0;
    for (key, value) in &kvs {
        let got = db
            .get_with_options(key.as_bytes(), &read_remote)
            .await
            .unwrap_or_else(|e| panic!("get(remote,after-release) {key}: {e}"));
        let h = hit(&got, value);
        if h {
            after_release_hits += 1;
        }
        after_release.push(h);
    }

    db.close().await.expect("db.close");

    for (i, (key, _value)) in kvs.iter().enumerate() {
        println!(
            "INFLIGHT key={key} remote_during_block={} remote_after_release={}",
            hitstr(during_block[i]),
            hitstr(after_release[i]),
        );
    }
    println!(
        "INFLIGHT_SUMMARY keys={} during_block_hits={during_block_hits} \
         after_release_hits={after_release_hits} put_was_blocked={put_was_blocked}",
        kvs.len(),
    );
}

// ---------------------------------------------------------------------------
// fence-victim / fence-usurper — two-process writer-fencing (promise:
// writer-fencing-split-brain).
//
// SlateDB is single-writer per object-store path: opening a Db bumps a manifest
// epoch via version-CAS (manifest/store.rs) and the WriterFencer writes a
// zero-byte WAL barrier (fence.rs:105). A superseded (older-epoch) writer's next
// await_durable write therefore fails — surfaced to the public API as
// `Error::kind() == ErrorKind::Closed(CloseReason::Fenced)` (SlateDBError::Fenced
// mapped at error.rs:618; asserted by the crate's own test_fence).
//
//   fence-victim  — open the Db, ack a prelude of keys (it is the live writer),
//                   then LOOP attempting await_durable=true puts, printing
//                   `FENCE_OBSERVED attempt=<i> result=<ok|fenced|other:<kind>>`
//                   for each. Stops on the first `fenced` (the win) or after
//                   --attempts. The error is CLASSIFIED, never unwrapped.
//   fence-usurper — open the SAME root (this bumps the epoch and fences the
//                   victim), ack its own keys, hold ~1s, close cleanly.
//
// Both open the same LocalFileSystem::new_with_prefix(root) + same Db path with a
// small manifest_poll_interval (config.rs:647) so the victim's background poller
// observes the usurper's epoch bump promptly. Always exit 0 — the python owns the
// verdict.
// ---------------------------------------------------------------------------

async fn open_db_fence(root: &str) -> Db {
    let store = build_object_store(root, None);
    // Small manifest_poll_interval so the incumbent writer's background poller
    // observes the usurper's epoch bump promptly (default is 1s; config.rs:981).
    // The fenced write also surfaces at the next await_durable flush regardless.
    let settings = Settings {
        manifest_poll_interval: std::time::Duration::from_millis(100),
        ..Default::default()
    };
    Db::builder(OsPath::from(root), store)
        .with_settings(settings)
        .build()
        .await
        .unwrap_or_else(|e| panic!("Db::builder({root}).with_settings(..).build(): {e}"))
}

/// Append one crash-safe fsync'd `seq\tkey\tvalue` ack line (same rule as `run`).
fn append_ack(log: &mut File, seq: u64, key: &str, value: &str) {
    writeln!(log, "{seq}\t{key}\t{value}").expect("write ack-log");
    log.flush().expect("flush ack-log");
    log.sync_all().expect("fsync ack-log");
}

/// Classify a put result into the machine-readable FENCE_OBSERVED token.
/// Ok → "ok"; the real Fenced surface → "fenced"; anything else → "other:<kind>".
/// Never swallows or panics on the error — the point is to report its kind.
fn classify_put(result: &Result<slatedb::WriteHandle, slatedb::Error>) -> String {
    match result {
        Ok(_) => "ok".to_string(),
        Err(e) => match e.kind() {
            ErrorKind::Closed(CloseReason::Fenced) => "fenced".to_string(),
            other => format!("other:{other:?}"),
        },
    }
}

async fn cmd_fence_victim(args: &[String]) {
    let root = require(args, "--root");
    let ack_log = require(args, "--ack-log");
    let seed: u64 = require(args, "--seed").parse().expect("--seed u64");
    let attempts: u64 = flag(args, "--attempts")
        .map(|s| s.parse().expect("--attempts u64"))
        .unwrap_or(40);
    // Keys acked BEFORE the usurper opens: proves the victim is the live writer.
    // Default 1 — the python spawns the usurper only AFTER it sees this ack land
    // durably, so the fence can only fire in the classified attempt loop below,
    // never mid-prelude (where it would panic). These MUST succeed.
    let prelude: u64 = flag(args, "--prelude-keys")
        .map(|s| s.parse().expect("--prelude-keys u64"))
        .unwrap_or(1);

    let mut log = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&ack_log)
        .unwrap_or_else(|e| panic!("open ack-log {ack_log}: {e}"));

    let db = open_db_fence(&root).await;

    let put_opts = PutOptions::default();
    let write_opts = WriteOptions {
        await_durable: true,
        ..Default::default()
    };

    let mut rng = XorShift64::new(seed);

    // --- prelude: ack a few keys as the live writer (no fence yet) ------------
    for seq in 0..prelude {
        let (key, value) = op_kv(&mut rng, seq);
        db.put_with_options(key.as_bytes(), value.as_bytes(), &put_opts, &write_opts)
            .await
            .unwrap_or_else(|e| panic!("victim prelude put seq={seq}: {e}"));
        append_ack(&mut log, seq, &key, &value);
    }
    println!("VICTIM prelude_acked={prelude}");

    // --- attempt loop: the usurper opens concurrently; once it bumps the epoch
    //     the victim's next await_durable flush MUST fail Fenced. If EVERY
    //     attempt returns `ok`, the victim was never fenced — split-brain.
    let mut fenced = false;
    let mut ok_count: u64 = 0;
    for i in 0..attempts {
        let seq = prelude + i;
        let (key, value) = op_kv(&mut rng, seq);
        let result = db
            .put_with_options(key.as_bytes(), value.as_bytes(), &put_opts, &write_opts)
            .await;
        let class = classify_put(&result);
        println!("FENCE_OBSERVED attempt={i} seq={seq} result={class}");
        if class == "fenced" {
            fenced = true;
            break;
        }
        if class == "ok" {
            ok_count += 1;
            // A post-fence `ok` is durably-visible zombie data — record it so the
            // python can verify it against the winner's history.
            append_ack(&mut log, seq, &key, &value);
        }
        // Space attempts so the usurper has time to open + fence.
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }

    println!("VICTIM done fenced={fenced} ok_after_prelude={ok_count} attempts={attempts}");
    // Do NOT db.close() on the fenced path — closing a fenced Db can itself error.
    // The ack-log is already fsync'd; exit 0 and let the python own the verdict.
    std::process::exit(0);
}

async fn cmd_fence_usurper(args: &[String]) {
    let root = require(args, "--root");
    let ack_log = require(args, "--ack-log");
    let seed: u64 = require(args, "--seed").parse().expect("--seed u64");
    let keys: u64 = flag(args, "--keys")
        .map(|s| s.parse().expect("--keys u64"))
        .unwrap_or(5);

    let mut log = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&ack_log)
        .unwrap_or_else(|e| panic!("open ack-log {ack_log}: {e}"));

    // Opening the SAME root bumps the manifest epoch and writes the WAL fence
    // barrier → the incumbent victim is fenced from this instant onward.
    let db = open_db_fence(&root).await;
    println!("USURPER opened root={root}");

    let put_opts = PutOptions::default();
    let write_opts = WriteOptions {
        await_durable: true,
        ..Default::default()
    };

    let mut rng = XorShift64::new(seed);
    for seq in 0..keys {
        let (key, value) = op_kv(&mut rng, seq);
        db.put_with_options(key.as_bytes(), value.as_bytes(), &put_opts, &write_opts)
            .await
            .unwrap_or_else(|e| panic!("usurper put seq={seq}: {e}"));
        append_ack(&mut log, seq, &key, &value);
    }
    println!("USURPER acked={keys}");

    // Hold briefly so the victim (spinning its attempt loop) gets a guaranteed
    // post-open window to observe the fence, then close cleanly.
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    db.close().await.expect("db.close");
    println!("USURPER done keys={keys} ack_log={ack_log}");
}

#[tokio::main]
async fn main() {
    let argv: Vec<String> = std::env::args().collect();
    if argv.len() < 2 {
        eprintln!("usage: slatedb-driver <run|verify|durprobe|remote-run|verify-remote|inflight-probe|fence-victim|fence-usurper> [flags]");
        std::process::exit(2);
    }
    let sub = argv[1].as_str();
    let rest = &argv[2..];
    match sub {
        "run" => cmd_run(rest).await,
        "verify" => cmd_verify(rest).await,
        "durprobe" => cmd_durprobe(rest).await,
        "remote-run" => cmd_remote_run(rest).await,
        "verify-remote" => cmd_verify_remote(rest).await,
        "inflight-probe" => cmd_inflight_probe(rest).await,
        "fence-victim" => cmd_fence_victim(rest).await,
        "fence-usurper" => cmd_fence_usurper(rest).await,
        "-h" | "--help" => {
            println!(
                "slatedb-driver <run|verify|durprobe|remote-run|verify-remote|inflight-probe>\n\
                 run            --root <dir> --ack-log <path> --seed <u64> --ops <n> [--head-false-negative <wal_id>]\n\
                 verify         --root <dir> --ack-log <path> [--head-false-negative <wal_id>]\n\
                 durprobe       --root <dir> --seed <u64> [--keys <n>]\n\
                 remote-run     --root <dir> --remote-log <path> --seed <u64> --ops <n> [--durable-every <n>]\n\
                 verify-remote  --root <dir> --remote-log <path>\n\
                 inflight-probe --root <dir> --seed <u64> [--keys <n>]\n\
                 fence-victim   --root <dir> --ack-log <path> --seed <u64> [--attempts <n>] [--prelude-keys <n>]\n\
                 fence-usurper  --root <dir> --ack-log <path> --seed <u64> [--keys <n>]"
            );
        }
        other => {
            eprintln!("unknown subcommand {other:?}");
            std::process::exit(2);
        }
    }
}
