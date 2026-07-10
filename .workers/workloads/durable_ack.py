#!/usr/bin/env python3
"""durable_ack — the SlateDB durable-ack workload (promise: durable-ack-survives-crash).

Invariant (set-inclusion, value-exact): let A be the (key,value) set the driver
observed acked with await_durable=true, and R the state readable after reopen.
Then A ⊆ R. Any acked key missing or stale after recovery is data-loss (sev 4).

This file drives the vendored `slatedb-driver` (run → optional fault → verify) and
emits the universal oracle plane:
  * liveness_watchdog  — a global-deadline thread; a wedged reopen/replay is a
    FAIL, never a silent timeout artifact.
  * terminal_state     — every acked key must resolve present-or-absent after
    recovery; a driver that exits without a verdict is a FAIL.
  * durability_watch_* — durawatch manifests the acked set and re-observes it on
    a delay ladder across reopen (catches delayed erasure).
  * durable_ack_subset — the bespoke A ⊆ R value-exact verdict.

Cases:
  baseline           — no faults; fully implemented here.
  crash-mid-flush    — SIGKILL at seed-swept flush boundaries (executor fills in).
  wal-head-contiguity— false-negative HEAD on one WAL id on reopen (executor).

ORACLE_SELFTEST=1 injects one fake acked line into the ack-log before verify so
verify reports LOST → durable_ack_subset MUST go FAIL (proves the red path).
"""

from __future__ import annotations

import os
import signal
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path

# --- resolve repo root and the universal oracle-plane lib --------------------
# script lives at <repo>/.workers/workloads/durable_ack.py
_SCRIPT = Path(__file__).resolve()
_REPO_ROOT = _SCRIPT.parents[2]
_LIB = _REPO_ROOT / ".workers" / "lib"
sys.path.insert(0, str(_LIB))

import crashclock  # noqa: E402  (fault-timing + seed source; baseline arms no clock)
import durawatch  # noqa: E402  (acked-durability watch)

DRIVER = _REPO_ROOT / ".workers" / "vendor" / "bin" / "slatedb-driver"

OPS = int(os.environ.get("SLATEDB_ACK_OPS", "24"))
LIVENESS_DEADLINE_S = float(os.environ.get("SLATEDB_LIVENESS_S", "120"))

# crash-mid-flush tunables.
# REALITY (measured on-box): each await_durable=true put blocks ~100ms — the put
# future resolves only on the next 100ms flush tick — so the driver `run` streams
# at ~100ms/op with NO pacing flag needed; a large --ops naturally spans many
# seconds and the SIGKILL always lands mid-run (we kill well before completion).
CRASH_OPS = int(os.environ.get("SLATEDB_CRASH_OPS", "2000"))
# Kill trigger is ACK-PROGRESS, not wall-clock. The deterministic sim runs ~10x+
# slower per-op than the box (measured in-guest: 0 acks by 1.7s vs ~20 on the
# box), so a fixed ms window fires before any ack lands → VOID. Instead SIGKILL
# right after the seed-derived K-th fsync'd ack: deterministic w.r.t. the SUT's
# real flush progress, identical on box and guest, and still mid-flush (the K-th
# ack just resolved while the next put/flush is in flight). K ∈ [2, MAX].
CRASH_KILL_MAX_ACKS = int(os.environ.get("SLATEDB_CRASH_KILL_MAX_ACKS", "12"))
CRASH_KILL_DEADLINE_S = float(os.environ.get("SLATEDB_CRASH_KILL_DEADLINE_S", "180"))


def emit(msg: str) -> None:
    print(msg, flush=True)


def invariant(inv_id: str, name: str, ok: bool, summary: str) -> None:
    emit(f"INVARIANT {inv_id} {name} {'PASS' if ok else 'FAIL'} {summary}")


# ---------------------------------------------------------------------------
# liveness watchdog — global deadline thread
# ---------------------------------------------------------------------------

_LIVENESS_DONE = threading.Event()


def _watchdog(deadline_s: float) -> None:
    if not _LIVENESS_DONE.wait(deadline_s):
        emit(
            f"INVARIANT liveness_watchdog global-deadline FAIL "
            f"wedged: no verdict within {deadline_s:g}s"
        )
        emit("VERDICT: RED — liveness watchdog fired")
        os._exit(1)


def arm_liveness() -> None:
    threading.Thread(target=_watchdog, args=(LIVENESS_DEADLINE_S,), daemon=True).start()


def disarm_liveness(ok: bool, summary: str) -> None:
    _LIVENESS_DONE.set()
    invariant("liveness_watchdog", "global-deadline", ok, summary)


# ---------------------------------------------------------------------------
# driver invocation + verify parsing
# ---------------------------------------------------------------------------


def run_driver(args: list[str]) -> subprocess.CompletedProcess:
    return subprocess.run(
        [str(DRIVER), *args],
        capture_output=True,
        text=True,
    )


def parse_verify(stdout: str):
    """Return (verdict_seen, subset_ok, checked, lost, mismatch, bad_keys)."""
    subset_ok = False
    checked = lost = mismatch = 0
    verdict_seen = False
    bad_keys: set[str] = set()
    for line in stdout.splitlines():
        line = line.strip()
        if line.startswith("LOST "):
            bad_keys.add(line[len("LOST "):])
        elif line.startswith("MISMATCH "):
            bad_keys.add(line[len("MISMATCH "):])
        elif line.startswith("VERIFY "):
            verdict_seen = True
            for tok in line.split()[1:]:
                k, _, v = tok.partition("=")
                if k == "subset_ok":
                    subset_ok = v == "true"
                elif k == "checked":
                    checked = int(v)
                elif k == "lost":
                    lost = int(v)
                elif k == "mismatch":
                    mismatch = int(v)
    return verdict_seen, subset_ok, checked, lost, mismatch, bad_keys


def read_acked(ack_log: str):
    """Parse the ack-log into an ordered list of (seq, key, value)."""
    out = []
    with open(ack_log) as f:
        for line in f:
            line = line.rstrip("\n")
            if not line:
                continue
            parts = line.split("\t", 2)
            if len(parts) == 3:
                out.append((parts[0], parts[1], parts[2]))
    return out


def count_acked(ack_log: str) -> int:
    """Cheap fsync'd-ack progress counter for the kill trigger (line count)."""
    try:
        with open(ack_log) as f:
            return sum(1 for line in f if line.strip())
    except FileNotFoundError:
        return 0


def kill_after_acks(seed: int) -> int:
    """Seed-derived K ∈ [2, CRASH_KILL_MAX_ACKS] — replayable, spans flush depths."""
    import hashlib
    h = int.from_bytes(hashlib.sha256(f"{seed}:crash_mid_flush_ackidx".encode()).digest()[:4], "big")
    return 2 + (h % (CRASH_KILL_MAX_ACKS - 1))


# ---------------------------------------------------------------------------
# WAL-object discovery (wal-head-contiguity case)
# ---------------------------------------------------------------------------


def discover_wal_objects(root: str):
    """Return {wal_id: size_bytes} for every `.../wal/{id:020}.sst` under root.

    The driver opens `LocalFileSystem::new_with_prefix(root)` AND passes `root`
    as the Db path, so objects physically nest at `<root>/<root>/wal/*.sst`;
    rglob makes discovery agnostic to that nesting.
    """
    out: dict[int, int] = {}
    for p in Path(root).rglob("*.sst"):
        if p.parent.name != "wal":
            continue
        try:
            wal_id = int(p.stem)
        except ValueError:
            continue
        out[wal_id] = p.stat().st_size
    return out


def pick_head_fn_target(seed: int, wal_objs: dict[int, int]):
    """Seed-derive the WAL id whose HEAD probe verify will falsify.

    Attack requirement: lie about a REAL object in the un-manifested tail that
    sits BELOW at least one ACK-BEARING WAL id — so that IF the frontier search
    truncated at/below the target, a durable acked write in a higher WAL object
    would be lost. Empty WAL objects (the fence barriers, 0 bytes) carry no ack;
    a non-empty WAL SST is our proxy for "contains acked data".

    Returns (target_id, reason) where target_id is None if no non-vacuous target
    exists (the caller VOIDs with `reason`).
    """
    if not wal_objs:
        return None, "no WAL objects on disk"
    ack_bearing = sorted(i for i, sz in wal_objs.items() if sz > 0)
    if not ack_bearing:
        return None, "no non-empty (ack-bearing) WAL objects"
    top_ack = max(ack_bearing)
    # Candidates: any real WAL object strictly below the highest ack-bearing id,
    # so there is guaranteed to be >=1 acked write in a WAL id > target.
    candidates = sorted(i for i in wal_objs if i < top_ack)
    if not candidates:
        return None, (
            f"only one ack-bearing WAL id ({top_ack}); no tail id below it to "
            f"lie about → truncation could not lose data"
        )
    import hashlib
    h = int.from_bytes(
        hashlib.sha256(f"{seed}:wal_head_contiguity_target".encode()).digest()[:4], "big"
    )
    target = candidates[h % len(candidates)]
    return target, f"chosen from {len(candidates)} tail candidate(s) below top ack-bearing id {top_ack}"


# ---------------------------------------------------------------------------
# baseline case
# ---------------------------------------------------------------------------


def case_baseline(seed: int) -> int:
    selftest = durawatch.selftest_active()

    # ack-log lives OUTSIDE the db root (a fault wrapper must never corrupt it).
    root = tempfile.mkdtemp(prefix="slatedb-ack-root-")
    ack_fd, ack_log = tempfile.mkstemp(prefix="slatedb-ack-", suffix=".log")
    os.close(ack_fd)
    os.unlink(ack_log)  # let the driver create it fresh (append semantics)

    emit(f"CASE baseline seed={seed} ops={OPS} root={root} ack_log={ack_log}")
    emit("CLOCK baseline point=none (baseline arms no fault clock)")

    # --- run: seeded await_durable=true put stream -> fsync'd ack-log ---------
    run = run_driver(
        ["run", "--root", root, "--ack-log", ack_log,
         "--seed", str(seed), "--ops", str(OPS)]
    )
    if run.returncode != 0:
        emit(f"DRIVER run failed rc={run.returncode}\n{run.stderr}")
        disarm_liveness(False, "driver run crashed")
        invariant("durable_ack_subset", "acked-subset-readable", False,
                  "driver run did not complete")
        emit("VERDICT: RED — driver run crashed")
        return 1

    acked = read_acked(ack_log)

    # --- ORACLE_SELFTEST: plant a fake acked key the DB never wrote -----------
    if selftest:
        with open(ack_log, "a") as f:
            f.write("999999\tSELFTEST_MISSING\tselftest-injected\n")
            f.flush()
            os.fsync(f.fileno())
        emit("ORACLE_SELFTEST: injected fake acked line key=SELFTEST_MISSING "
             "(verify must report LOST)")

    # --- verify: reopen + assert A ⊆ R value-exact ---------------------------
    ver = run_driver(["verify", "--root", root, "--ack-log", ack_log])
    verdict_seen, subset_ok, checked, lost, mismatch, bad_keys = parse_verify(ver.stdout)
    sys.stdout.write(ver.stdout)
    sys.stdout.flush()

    # --- terminal-state sweep -------------------------------------------------
    n_acked = len(acked) + (1 if selftest else 0)
    terminal_ok = verdict_seen and checked == n_acked
    invariant(
        "terminal_state", "acked-keys-resolved", terminal_ok,
        f"verify emitted verdict for {checked}/{n_acked} acked keys"
        if terminal_ok else
        f"driver exited without a full verdict (verdict_seen={verdict_seen} "
        f"checked={checked} expected={n_acked})",
    )

    # --- durable_ack_subset: the bespoke A ⊆ R verdict ------------------------
    subset_pass = terminal_ok and subset_ok and lost == 0 and mismatch == 0
    summary = f"checked={checked} lost={lost} mismatch={mismatch} bad={sorted(bad_keys)[:8]}"
    invariant("durable_ack_subset", "acked-subset-readable", subset_pass, summary)

    if not subset_pass:
        disarm_liveness(True, "verdict reached (durable_ack_subset FAIL)")
        emit(f"VERDICT: RED — acked writes not durable after reopen: {summary}")
        return 1

    # --- durawatch: manifest the REAL acked set, re-observe across reopen -----
    # observe() re-reads by reopening the DB (`verify`) at each rung — a delayed
    # erasure (GC/compaction after ack) surfaces as a later-rung miss.
    ladder = (0.0, 2.0)
    m = durawatch.Manifest.start(
        case="durable_ack_baseline",
        path=durawatch.manifest_path("durable_ack_baseline"),
        ladder=ladder,
        void_floor=1,
    )
    for _seq, key, value in acked:
        m.record(eid=key, query=key, payload=value)

    cache = {"ts": 0.0, "bad": set(), "loaded": False}

    def refresh() -> None:
        v = run_driver(["verify", "--root", root, "--ack-log", ack_log])
        _seen, _ok, _chk, _lost, _mm, bad = parse_verify(v.stdout)
        cache["bad"] = bad
        cache["ts"] = time.time()
        cache["loaded"] = True

    recorded = {key: value for _s, key, value in acked}

    def observe(eff):
        if not cache["loaded"] or (time.time() - cache["ts"]) > 0.5:
            refresh()
        if eff.query in cache["bad"]:
            return None
        return recorded.get(eff.query)

    disarm_liveness(True, "verdict reached (durable_ack_subset PASS)")
    # run_ladder emits durability_watch_* invariants and the final VERDICT.
    m.run_ladder(observe)
    return 0


# ---------------------------------------------------------------------------
# durawatch delay-ladder re-observation (shared by baseline + crash cases)
# ---------------------------------------------------------------------------


def run_durawatch_ladder(case_name: str, root: str, ack_log: str, acked) -> None:
    """Manifest the acked set and re-observe it across reopen on a delay ladder.

    observe() re-reads by reopening the DB (`verify`) at each rung, so a *delayed*
    erasure (GC/compaction after the acked write became readable) surfaces as a
    later-rung miss. Emits durability_watch_* invariants and the final VERDICT.
    """
    ladder = (0.0, 2.0)
    m = durawatch.Manifest.start(
        case=case_name,
        path=durawatch.manifest_path(case_name),
        ladder=ladder,
        void_floor=1,
    )
    for _seq, key, value in acked:
        m.record(eid=key, query=key, payload=value)

    cache = {"ts": 0.0, "bad": set(), "loaded": False}

    def refresh() -> None:
        v = run_driver(["verify", "--root", root, "--ack-log", ack_log])
        _seen, _ok, _chk, _lost, _mm, bad = parse_verify(v.stdout)
        cache["bad"] = bad
        cache["ts"] = time.time()
        cache["loaded"] = True

    recorded = {key: value for _s, key, value in acked}

    def observe(eff):
        if not cache["loaded"] or (time.time() - cache["ts"]) > 0.5:
            refresh()
        if eff.query in cache["bad"]:
            return None
        return recorded.get(eff.query)

    m.run_ladder(observe)


# ---------------------------------------------------------------------------
# crash-mid-flush case
# ---------------------------------------------------------------------------


def case_crash_mid_flush(seed: int) -> int:
    selftest = durawatch.selftest_active()

    # ack-log lives OUTSIDE the db root (a fault wrapper / crash must never corrupt it).
    root = tempfile.mkdtemp(prefix="slatedb-ack-root-")
    ack_fd, ack_log = tempfile.mkstemp(prefix="slatedb-ack-", suffix=".log")
    os.close(ack_fd)
    os.unlink(ack_log)  # let the driver create it fresh (append semantics)

    # --- declared fault timing: SIGKILL after the seed-derived K-th ack -------
    # ACK-PROGRESS trigger (portable across box/guest — see CRASH_KILL_* notes).
    kill_k = kill_after_acks(seed)

    emit(f"CASE crash-mid-flush seed={seed} ops={CRASH_OPS} root={root} ack_log={ack_log}")
    emit(f"CLOCK crash-mid-flush armed kind=ack_progress axis=crash_mid_flush "
         f"kill_after_acks={kill_k} max_acks={CRASH_KILL_MAX_ACKS} seed={seed}")

    # --- spawn the acked put stream in its own process group ------------------
    # start_new_session=True → child is the pgid leader; we SIGKILL the whole group
    # so no orphaned tokio worker survives. NO graceful cleanup: SIGKILL is abrupt.
    proc = subprocess.Popen(
        [str(DRIVER), "run", "--root", root, "--ack-log", ack_log,
         "--seed", str(seed), "--ops", str(CRASH_OPS)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        text=True,
        start_new_session=True,
    )
    t_start = time.time()
    deadline = t_start + CRASH_KILL_DEADLINE_S
    # poll fsync'd-ack progress; kill the pg once K acks landed (or the proc exits,
    # or a hard wall deadline as a liveness backstop).
    while True:
        if count_acked(ack_log) >= kill_k:
            break
        if proc.poll() is not None:
            break  # run ended before reaching K acks — VOID handled below
        if time.time() > deadline:
            break  # liveness backstop: kill whatever progress exists
        time.sleep(0.03)

    kill_mode = "already_dead"
    if proc.poll() is None:
        os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
        kill_mode = "sigkill(pg)"
    proc.wait()
    killed_rc = proc.returncode
    kill_wall = time.time() - t_start

    acked = read_acked(ack_log) if os.path.exists(ack_log) else []
    n_pre = len(acked)
    emit(f"KILLED mode={kill_mode} rc={killed_rc} acked_before_kill={n_pre} "
         f"kill_after={kill_k} kill_wall={kill_wall:.3f}s ops={CRASH_OPS}")

    # --- anti-vacuity floors: the kill must land MID-RUN with acks recorded ---
    # rc==0 means the run streamed all CRASH_OPS and exited cleanly before we could
    # kill — the SIGKILL never landed mid-flush, so a green would be theater.
    if killed_rc == 0:
        disarm_liveness(True, "kill missed window (run completed)")
        emit(f"VERDICT: VOID — run completed all {CRASH_OPS} ops before the K-th ack "
             f"kill; raise SLATEDB_CRASH_OPS. seed={seed} kill_after={kill_k}")
        return 3
    if n_pre < 1:
        disarm_liveness(True, "kill too early — no acked writes")
        emit(f"VERDICT: VOID — no acked writes before the kill (driver stalled or "
             f"deadline hit before first ack); seed={seed} kill_after={kill_k}")
        return 3

    # --- ORACLE_SELFTEST: plant a fake acked key the DB never wrote -----------
    # Proves the crash-case A ⊆ R oracle's RED path: verify must report this key
    # LOST → durable_ack_subset FAIL, before any crash-case green is trusted.
    if selftest:
        with open(ack_log, "a") as f:
            f.write("999999\tSELFTEST_MISSING\tselftest-injected\n")
            f.flush()
            os.fsync(f.fileno())
        emit("ORACLE_SELFTEST: injected fake acked line key=SELFTEST_MISSING "
             "(verify must report LOST → crash-case oracle must go RED)")
        acked = read_acked(ack_log)

    # --- verify: reopen the SIGKILLed root + assert A ⊆ R value-exact ---------
    ver = run_driver(["verify", "--root", root, "--ack-log", ack_log])
    verdict_seen, subset_ok, checked, lost, mismatch, bad_keys = parse_verify(ver.stdout)
    sys.stdout.write(ver.stdout)
    sys.stdout.flush()
    if ver.returncode != 0 and not verdict_seen:
        emit(f"DRIVER verify failed rc={ver.returncode}\n{ver.stderr}")

    # --- terminal-state sweep -------------------------------------------------
    n_acked = len(acked)
    terminal_ok = verdict_seen and checked == n_acked
    invariant(
        "terminal_state", "acked-keys-resolved", terminal_ok,
        f"verify emitted verdict for {checked}/{n_acked} acked keys after SIGKILL+reopen"
        if terminal_ok else
        f"driver exited without a full verdict (verdict_seen={verdict_seen} "
        f"checked={checked} expected={n_acked})",
    )

    # --- durable_ack_subset: the bespoke A ⊆ R verdict ------------------------
    subset_pass = terminal_ok and subset_ok and lost == 0 and mismatch == 0
    summary = (f"checked={checked} lost={lost} mismatch={mismatch} "
               f"bad={sorted(bad_keys)[:8]} seed={seed} kill_after={kill_k}")
    invariant("durable_ack_subset", "acked-subset-readable", subset_pass, summary)

    if not subset_pass:
        disarm_liveness(True, "verdict reached (durable_ack_subset FAIL)")
        emit(f"VERDICT: RED — acked writes not durable after SIGKILL+reopen: {summary}")
        emit(f"REPLAY red: SEED={seed} case=crash-mid-flush kill_after={kill_k} "
             f"acked_before_kill={n_pre}")
        return 1

    disarm_liveness(True, "verdict reached (durable_ack_subset PASS)")
    # durawatch: re-observe the acked set across reopen on a delay ladder to catch
    # delayed erasure (compaction/GC after the crash-recovered read became visible).
    run_durawatch_ladder("durable_ack_crash_mid_flush", root, ack_log, acked)
    return 0


# ---------------------------------------------------------------------------
# wal-head-contiguity case
# ---------------------------------------------------------------------------


def case_wal_head_contiguity(seed: int) -> int:
    """SIGKILL leaves an un-manifested WAL tail; reopen (`verify`) runs with a
    false-negative HEAD on one seed-derived tail WAL id. If the frontier search
    (tablestore.rs:163-273) truncates replay, an acked write in a higher WAL
    object is lost = data-loss RED. Otherwise the fence-barrier Create walk
    (fence.rs:143-172) self-corrects → GREEN.
    """
    selftest = durawatch.selftest_active()

    # ack-log lives OUTSIDE the db root (fault wrapper / crash must never corrupt it).
    root = tempfile.mkdtemp(prefix="slatedb-ack-root-")
    ack_fd, ack_log = tempfile.mkstemp(prefix="slatedb-ack-", suffix=".log")
    os.close(ack_fd)
    os.unlink(ack_log)  # let the driver create it fresh (append semantics)

    # --- declared fault timing: SIGKILL after the seed-derived K-th ack -------
    # Same ACK-PROGRESS trigger as crash-mid-flush → leaves acked WAL objects on
    # disk BEYOND the last manifest update (no L0 flush at this depth, so
    # replay_after_wal_id stays 0 and the whole WAL tail is un-manifested).
    kill_k = kill_after_acks(seed)

    emit(f"CASE wal-head-contiguity seed={seed} ops={CRASH_OPS} root={root} ack_log={ack_log}")
    emit(f"CLOCK wal-head-contiguity armed kind=ack_progress axis=wal_head_contiguity "
         f"kill_after_acks={kill_k} max_acks={CRASH_KILL_MAX_ACKS} seed={seed}")

    proc = subprocess.Popen(
        [str(DRIVER), "run", "--root", root, "--ack-log", ack_log,
         "--seed", str(seed), "--ops", str(CRASH_OPS)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        text=True,
        start_new_session=True,
    )
    t_start = time.time()
    deadline = t_start + CRASH_KILL_DEADLINE_S
    while True:
        if count_acked(ack_log) >= kill_k:
            break
        if proc.poll() is not None:
            break
        if time.time() > deadline:
            break
        time.sleep(0.03)

    kill_mode = "already_dead"
    if proc.poll() is None:
        os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
        kill_mode = "sigkill(pg)"
    proc.wait()
    killed_rc = proc.returncode
    kill_wall = time.time() - t_start

    acked = read_acked(ack_log) if os.path.exists(ack_log) else []
    n_pre = len(acked)

    # --- discover the un-manifested WAL tail + pick the false-HEAD target -----
    wal_objs = discover_wal_objects(root)
    wal_ids = sorted(wal_objs)
    layout = ",".join(f"{i}:{wal_objs[i]}b" for i in wal_ids)
    emit(f"KILLED mode={kill_mode} rc={killed_rc} acked_before_kill={n_pre} "
         f"kill_after={kill_k} kill_wall={kill_wall:.3f}s ops={CRASH_OPS}")
    emit(f"WAL_TAIL ids=[{','.join(map(str, wal_ids))}] sizes=[{layout}]")

    # --- anti-vacuity floors --------------------------------------------------
    if killed_rc == 0:
        disarm_liveness(True, "kill missed window (run completed)")
        emit(f"VERDICT: VOID — run completed all {CRASH_OPS} ops before the K-th ack "
             f"kill; raise SLATEDB_CRASH_OPS. seed={seed} kill_after={kill_k}")
        return 3
    if n_pre < 2:
        disarm_liveness(True, "kill too early — <2 acked writes")
        emit(f"VERDICT: VOID — need >=2 acked writes to make truncation lossy; "
             f"n_pre={n_pre} seed={seed} kill_after={kill_k}")
        return 3

    target, reason = pick_head_fn_target(seed, wal_objs)
    if target is None:
        disarm_liveness(True, "no non-vacuous head-fn target")
        emit(f"VERDICT: VOID — {reason}; seed={seed} wal_ids={wal_ids}")
        return 3

    # The wrapper must lie about a REAL object; it came from the on-disk listing.
    assert target in wal_objs, f"target {target} not on disk"
    higher_ack = [i for i in wal_ids if i > target and wal_objs[i] > 0]
    emit(f"HEADFN target_wal_id={target} ({reason}); "
         f"ack_bearing_wal_ids_above_target={higher_ack}")
    if not higher_ack:
        disarm_liveness(True, "no ack-bearing WAL above target")
        emit(f"VERDICT: VOID — no ack-bearing WAL id above target {target}; "
             f"truncation could not lose an acked write. seed={seed}")
        return 3

    # --- ORACLE_SELFTEST: plant a fake acked key the DB never wrote -----------
    if selftest:
        with open(ack_log, "a") as f:
            f.write("999999\tSELFTEST_MISSING\tselftest-injected\n")
            f.flush()
            os.fsync(f.fileno())
        emit("ORACLE_SELFTEST: injected fake acked line key=SELFTEST_MISSING "
             "(verify must report LOST → wal-head-contiguity oracle must go RED)")
        acked = read_acked(ack_log)

    # --- FAULT verify: FIRST reopen with the false-negative HEAD on target ----
    # This is the attack: the false HEAD is active during the un-manifested-tail
    # replay. Three possible outcomes, which the oracle MUST distinguish:
    #   (a) opens + all acked present   → frontier did not truncate (GREEN)
    #   (b) opens + acked keys MISSING  → SILENT truncation = data-loss RED
    #   (c) VERIFY_OPEN_FAILED (loud)   → reopen erred loudly under the fault;
    #       NOT silent loss. Cross-check the acked set with a fault-free CONTROL
    #       verify (ground truth: did the crash actually lose data?).
    fault = run_driver(
        ["verify", "--root", root, "--ack-log", ack_log,
         "--head-false-negative", str(target)]
    )
    sys.stdout.write(fault.stdout)
    sys.stdout.flush()
    fault_open_failed = "VERIFY_OPEN_FAILED" in fault.stdout
    f_verdict, f_ok, f_checked, f_lost, f_mismatch, f_bad = parse_verify(fault.stdout)

    if fault_open_failed:
        # (c) LOUD reopen failure. Record it as a distinct observation, then use a
        # fault-free control verify as the durability ground truth.
        emit(f"HEADFN_REOPEN_LOUD_FAILURE wal_id={target}: reopen returned an error "
             f"(false-negative HEAD on a to-be-replayed WAL SST) — detected, not "
             f"silent. A truthful/retrying object store recovers this; the acked "
             f"bytes are durably present on disk.")
        ctrl = run_driver(["verify", "--root", root, "--ack-log", ack_log])
        sys.stdout.write(ctrl.stdout)
        sys.stdout.flush()
        verdict_seen, subset_ok, checked, lost, mismatch, bad_keys = parse_verify(ctrl.stdout)
        source = "control(fault-free reopen)"
    else:
        # (a)/(b): the fault reopen itself produced a verdict — that IS the
        # answer. A missing/stale key here is genuine SILENT data-loss.
        verdict_seen, subset_ok, checked, lost, mismatch, bad_keys = (
            f_verdict, f_ok, f_checked, f_lost, f_mismatch, f_bad)
        source = "fault(false-HEAD reopen)"
        if fault.returncode != 0 and not verdict_seen:
            emit(f"DRIVER verify failed rc={fault.returncode}\n{fault.stderr}")

    # --- terminal-state sweep -------------------------------------------------
    n_acked = len(acked)
    terminal_ok = verdict_seen and checked == n_acked
    invariant(
        "terminal_state", "acked-keys-resolved", terminal_ok,
        f"[{source}] verify emitted verdict for {checked}/{n_acked} acked keys "
        f"after SIGKILL + false-HEAD(wal_id={target})"
        if terminal_ok else
        f"[{source}] no full verdict (verdict_seen={verdict_seen} "
        f"checked={checked} expected={n_acked})",
    )

    # --- durable_ack_subset: the bespoke A ⊆ R (silent-loss) verdict ----------
    # PASS iff the acked set is fully recoverable (silent loss did NOT occur).
    # A loud reopen failure (c) with a clean control is a PASS for THIS oracle —
    # the promise under attack is *silent* data-loss (sev 4); the loud failure is
    # reported separately above.
    subset_pass = terminal_ok and subset_ok and lost == 0 and mismatch == 0
    summary = (f"[{source}] checked={checked} lost={lost} mismatch={mismatch} "
               f"bad={sorted(bad_keys)[:8]} seed={seed} kill_after={kill_k} "
               f"head_fn_wal_id={target}")
    invariant("durable_ack_subset", "acked-subset-readable", subset_pass, summary)

    if not subset_pass:
        disarm_liveness(True, "verdict reached (durable_ack_subset FAIL)")
        emit(f"VERDICT: RED — acked writes not recoverable: {summary}")
        emit(f"REPLAY red: SEED={seed} case=wal-head-contiguity head_fn_wal_id={target} "
             f"acked_before_kill={n_pre} wal_ids={wal_ids} lost_keys={sorted(bad_keys)} "
             f"source={source}")
        return 1

    disarm_liveness(True, "verdict reached (durable_ack_subset PASS)")
    # durawatch: re-observe across reopen (fault-free rungs, so a delayed erasure
    # would still surface).
    run_durawatch_ladder("durable_ack_wal_head_contiguity", root, ack_log, acked)
    return 0


# ---------------------------------------------------------------------------
# entry
# ---------------------------------------------------------------------------


def main() -> int:
    import argparse

    # Seed FIRST — the replay key. derive_seed() honors SEED/WORKLOAD_SEED else
    # reads /dev/urandom (corpus convention).
    seed = crashclock.derive_seed()
    emit(f"SEED {seed}")
    emit(f"REPLAY key=SEED={seed}")

    ap = argparse.ArgumentParser()
    ap.add_argument("--case", choices=["baseline", "crash-mid-flush", "wal-head-contiguity"],
                    default="baseline")
    args = ap.parse_args()

    if not DRIVER.exists():
        emit(f"VERDICT: VOID — driver not found at {DRIVER} (run .workers/build.sh)")
        return 3

    arm_liveness()

    if args.case == "baseline":
        return case_baseline(seed)
    if args.case == "crash-mid-flush":
        return case_crash_mid_flush(seed)
    if args.case == "wal-head-contiguity":
        return case_wal_head_contiguity(seed)
    raise NotImplementedError(f"unknown case {args.case!r}")


if __name__ == "__main__":
    sys.exit(main())
