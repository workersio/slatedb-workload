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
# The seed-swept kill lands T ms after the run starts, T log-uniform over
# [floor, window] (crashclock LatencyWindowSpace) — different seeds kill at
# different flush offsets. window kept < CRASH_OPS*~100ms so run never finishes.
CRASH_WINDOW_MS = float(os.environ.get("SLATEDB_CRASH_WINDOW_MS", "6000"))
CRASH_FLOOR_MS = float(os.environ.get("SLATEDB_CRASH_FLOOR_MS", "300"))


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

    # --- declared fault timing: derive the SIGKILL point from the seed --------
    # LatencyWindowSpace: kill T ms after the run starts, T log-uniform over the
    # flush arm so seeds straddle flush boundaries. Same seed => same T (replayable).
    space = crashclock.latency_window(
        "crash_mid_flush", window_ms=CRASH_WINDOW_MS, floor_ms=CRASH_FLOOR_MS
    )
    point = crashclock.offsets(seed, space)
    kill_at_s = float(point["T_ms"]) / 1000.0

    emit(f"CASE crash-mid-flush seed={seed} ops={CRASH_OPS} root={root} ack_log={ack_log}")
    crashclock.clock_armed("crash-mid-flush", point)  # emits the CLOCK triage line
    emit(f"KILL armed offset T_ms={point['T_ms']:.3f} (kill_at={kill_at_s:.3f}s) "
         f"window_ms={CRASH_WINDOW_MS:g} seed={seed}")

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
    remaining = kill_at_s - (time.time() - t_start)
    if remaining > 0:
        time.sleep(remaining)

    kill_mode = "already_dead"
    if proc.poll() is None:
        os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
        kill_mode = "sigkill(pg)"
    proc.wait()
    killed_rc = proc.returncode
    kill_wall = time.time() - t_start

    # A T_ms=0 (zero-corner) kill can fire before the driver even creates the
    # ack-log — that is the "too early" VOID case, not a crash of this harness.
    acked = read_acked(ack_log) if os.path.exists(ack_log) else []
    n_pre = len(acked)
    emit(f"KILLED mode={kill_mode} rc={killed_rc} acked_before_kill={n_pre} "
         f"kill_wall={kill_wall:.3f}s ops={CRASH_OPS}")

    # --- anti-vacuity floors: the kill must land MID-RUN with acks recorded ---
    # rc==0 means the run streamed all CRASH_OPS and exited cleanly before the
    # kill fired — the SIGKILL never landed mid-flush, so a green would be theater.
    if killed_rc == 0:
        disarm_liveness(True, "kill missed window (run completed)")
        emit(f"VERDICT: VOID — SIGKILL landed after run completed (all {CRASH_OPS} "
             f"ops acked); raise SLATEDB_CRASH_OPS or shrink window. "
             f"seed={seed} T_ms={point['T_ms']:.3f}")
        return 3
    if n_pre < 1:
        disarm_liveness(True, "kill too early — no acked writes")
        emit(f"VERDICT: VOID — no acked writes before the kill (fired too early); "
             f"seed={seed} T_ms={point['T_ms']:.3f}")
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
               f"bad={sorted(bad_keys)[:8]} seed={seed} kill_T_ms={point['T_ms']:.3f}")
    invariant("durable_ack_subset", "acked-subset-readable", subset_pass, summary)

    if not subset_pass:
        disarm_liveness(True, "verdict reached (durable_ack_subset FAIL)")
        emit(f"VERDICT: RED — acked writes not durable after SIGKILL+reopen: {summary}")
        emit(f"REPLAY red: SEED={seed} case=crash-mid-flush kill_T_ms={point['T_ms']:.3f} "
             f"acked_before_kill={n_pre}")
        return 1

    disarm_liveness(True, "verdict reached (durable_ack_subset PASS)")
    # durawatch: re-observe the acked set across reopen on a delay ladder to catch
    # delayed erasure (compaction/GC after the crash-recovered read became visible).
    run_durawatch_ladder("durable_ack_crash_mid_flush", root, ack_log, acked)
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
        raise NotImplementedError("executor fills next episode")
    raise NotImplementedError(f"unknown case {args.case!r}")


if __name__ == "__main__":
    sys.exit(main())
