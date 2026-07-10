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
use slatedb::Db;

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

#[tokio::main]
async fn main() {
    let argv: Vec<String> = std::env::args().collect();
    if argv.len() < 2 {
        eprintln!("usage: slatedb-driver <run|verify|durprobe> [flags]");
        std::process::exit(2);
    }
    let sub = argv[1].as_str();
    let rest = &argv[2..];
    match sub {
        "run" => cmd_run(rest).await,
        "verify" => cmd_verify(rest).await,
        "durprobe" => cmd_durprobe(rest).await,
        "-h" | "--help" => {
            println!(
                "slatedb-driver <run|verify|durprobe>\n\
                 run      --root <dir> --ack-log <path> --seed <u64> --ops <n> [--head-false-negative <wal_id>]\n\
                 verify   --root <dir> --ack-log <path> [--head-false-negative <wal_id>]\n\
                 durprobe --root <dir> --seed <u64> [--keys <n>]"
            );
        }
        other => {
            eprintln!("unknown subcommand {other:?}");
            std::process::exit(2);
        }
    }
}
