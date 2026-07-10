#!/usr/bin/env python3
"""durability_filter — the SlateDB Remote/Memory durability-filter workload.

Promise (durability-filter-remote): a `get_with_options` with
`DurabilityLevel::Remote` returns ONLY data durably stored in object storage; a
`Memory` read may additionally surface not-yet-durable in-memory data.

This file drives the vendored `slatedb-driver durprobe` op and emits the
discrimination oracle. The falsifiable core is `remote_excludes_dirty`: a Remote
read that returns a value written `await_durable=false` and not-yet-flushed is a
value a crash would erase — a wrong-durable-read (correctness, weight 3).

Determinism: the driver opens the Db with `flush_interval=None`, so an
`await_durable=false` write is NEVER auto-flushed until an explicit `db.flush()`
(reader.rs:112-113 gates Remote on `last_remote_persisted_seq`; config.rs:633-634
documents flush_interval=None disabling auto-flush). The dirty window is therefore
a hard invariant, not a 100ms race.

Universal oracle plane:
  * liveness_watchdog — a global-deadline thread; a wedged driver is a FAIL,
    never a silent timeout artifact.
  * terminal_state    — the driver must emit a DURPROBE verdict for every key.

Cases:
  baseline        — no faults; fully implemented here (non-vacuity control).
  crash-confirm   — executor fills next episode (SIGKILL + R_remote ⊆ survivors).
  inflight-flush  — executor fills next episode (flush-boundary Remote exclusion).

ORACLE_SELFTEST=1 corrupts the `remote_excludes_dirty` comparison by injecting a
fake remote_dirty_hit, so the oracle MUST go FAIL — proving the RED path fires.
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
# script lives at <repo>/.workers/workloads/durability_filter.py
_SCRIPT = Path(__file__).resolve()
_REPO_ROOT = _SCRIPT.parents[2]
_LIB = _REPO_ROOT / ".workers" / "lib"
sys.path.insert(0, str(_LIB))

import crashclock  # noqa: E402  (seed source + selftest gate; baseline arms no clock)
import durawatch  # noqa: E402  (R_remote re-observation across reopen)

DRIVER = _REPO_ROOT / ".workers" / "vendor" / "bin" / "slatedb-driver"

KEYS = int(os.environ.get("SLATEDB_DURFILTER_KEYS", "8"))
LIVENESS_DEADLINE_S = float(os.environ.get("SLATEDB_LIVENESS_S", "120"))

# crash-confirm tunables. Same ack-progress kill approach as durable_ack: the
# deterministic sim runs far slower per-op than the box, so a wall-clock kill
# window is unreliable. Instead SIGKILL right after the seed-derived K-th fsync'd
# REMOTE-OBSERVATION — deterministic w.r.t. the SUT's real flush progress and
# identical on box and guest. --ops is large so the kill always lands mid-stream.
CRASH_OPS = int(os.environ.get("SLATEDB_CRASH_OPS", "2000"))
# A minority of the write stream is await_durable=true (forces flush progress);
# the rest are await_durable=false so Memory and Remote genuinely diverge.
DURABLE_EVERY = int(os.environ.get("SLATEDB_REMOTE_DURABLE_EVERY", "8"))
CRASH_KILL_MAX_OBS = int(os.environ.get("SLATEDB_CRASH_KILL_MAX_OBS", "12"))
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
# driver invocation + durprobe parsing
# ---------------------------------------------------------------------------


def run_driver(args: list[str]) -> subprocess.CompletedProcess:
    return subprocess.run(
        [str(DRIVER), *args],
        capture_output=True,
        text=True,
    )


def parse_durprobe(stdout: str):
    """Return (per_key, summary).

    per_key: list of dicts {key, memory_dirty, remote_dirty, remote_after_flush}
             where each value is True (hit) / False (miss).
    summary: dict from DURPROBE_SUMMARY, or None if the driver emitted no summary.
    """
    per_key = []
    summary = None
    for line in stdout.splitlines():
        line = line.strip()
        if line.startswith("DURPROBE_SUMMARY "):
            summary = {}
            for tok in line.split()[1:]:
                k, _, v = tok.partition("=")
                summary[k] = v
        elif line.startswith("DURPROBE "):
            rec = {}
            for tok in line.split()[1:]:
                k, _, v = tok.partition("=")
                rec[k] = v
            per_key.append({
                "key": rec.get("key", ""),
                "memory_dirty": rec.get("memory_dirty") == "hit",
                "remote_dirty": rec.get("remote_dirty") == "hit",
                "remote_after_flush": rec.get("remote_after_flush") == "hit",
            })
    return per_key, summary


# ---------------------------------------------------------------------------
# remote-observed log + verify-remote parsing (crash-confirm case)
# ---------------------------------------------------------------------------


def read_remote_log(remote_log: str):
    """Parse the remote-observed log into an ordered list of (seq, key, value).

    Each line is one `get_with_options(.., Remote)` that returned Some(value),
    appended+fsync'd by the driver — the pre-crash R_remote set.
    """
    out = []
    try:
        with open(remote_log) as f:
            for line in f:
                line = line.rstrip("\n")
                if not line:
                    continue
                parts = line.split("\t", 2)
                if len(parts) == 3:
                    out.append((parts[0], parts[1], parts[2]))
    except FileNotFoundError:
        pass
    return out


def count_remote_obs(remote_log: str) -> int:
    """Cheap fsync'd remote-observation progress counter for the kill trigger."""
    try:
        with open(remote_log) as f:
            return sum(1 for line in f if line.strip())
    except FileNotFoundError:
        return 0


def parse_verify_remote(stdout: str):
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
        elif line.startswith("VERIFY_REMOTE "):
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


def kill_after_obs(seed: int) -> int:
    """Seed-derived K in [2, CRASH_KILL_MAX_OBS] — replayable, spans flush depths."""
    import hashlib
    h = int.from_bytes(
        hashlib.sha256(f"{seed}:crash_confirm_obsidx".encode()).digest()[:4], "big")
    return 2 + (h % (CRASH_KILL_MAX_OBS - 1))


# ---------------------------------------------------------------------------
# baseline case — the non-vacuity control
# ---------------------------------------------------------------------------


def case_baseline(seed: int) -> int:
    selftest = crashclock.selftest_active()

    root = tempfile.mkdtemp(prefix="slatedb-durfilter-root-")

    emit(f"CASE baseline seed={seed} keys={KEYS} root={root}")
    emit("CLOCK baseline point=none (baseline arms no fault clock)")

    # --- run: seeded await_durable=false puts + Memory/Remote probes ---------
    probe = run_driver(
        ["durprobe", "--root", root, "--seed", str(seed), "--keys", str(KEYS)]
    )
    sys.stdout.write(probe.stdout)
    sys.stdout.flush()
    if probe.returncode != 0:
        emit(f"DRIVER durprobe failed rc={probe.returncode}\n{probe.stderr}")
        disarm_liveness(False, "driver durprobe crashed")
        invariant("durability_filter_remote_excludes_dirty",
                  "remote-excludes-not-yet-durable", False,
                  "driver durprobe did not complete")
        emit("VERDICT: RED — driver durprobe crashed")
        return 1

    per_key, summary = parse_durprobe(probe.stdout)

    # --- terminal-state sweep: a verdict for every key -----------------------
    n_keys = len(per_key)
    terminal_ok = summary is not None and n_keys == KEYS and \
        int(summary.get("keys", "-1")) == KEYS
    invariant(
        "terminal_state", "durprobe-keys-resolved", terminal_ok,
        f"durprobe emitted a verdict for {n_keys}/{KEYS} keys (summary={summary})"
        if terminal_ok else
        f"driver exited without a full verdict (per_key={n_keys} expected={KEYS} "
        f"summary={summary})",
    )

    # --- tally the three discrimination facts from the per-key lines ---------
    mem_dirty_hits = sum(1 for r in per_key if r["memory_dirty"])
    remote_dirty_hits = sum(1 for r in per_key if r["remote_dirty"])
    remote_flushed_hits = sum(1 for r in per_key if r["remote_after_flush"])

    # --- ORACLE_SELFTEST: corrupt the remote-excludes-dirty comparison -------
    # Inject a fake Remote-read hit on a not-yet-durable value; a real violation
    # (Remote surfacing dirty data) looks EXACTLY like this. remote_excludes_dirty
    # MUST then go FAIL, proving the falsifiable oracle actually fires.
    if selftest and per_key:
        remote_dirty_hits += 1
        emit("ORACLE_SELFTEST: injected fake remote_dirty_hit "
             "(remote_excludes_dirty must go FAIL)")

    # --- INVARIANT durability_filter_memory_sees_dirty -----------------------
    # Every dirty write must be visible to a Memory-filter read.
    mem_ok = terminal_ok and mem_dirty_hits == KEYS
    invariant(
        "durability_filter_memory_sees_dirty", "memory-sees-not-yet-durable", mem_ok,
        f"mem_dirty_hits={mem_dirty_hits}/{KEYS}",
    )

    # --- INVARIANT durability_filter_remote_excludes_dirty (FALSIFIABLE CORE) -
    # NO not-yet-durable write may be visible to a Remote-filter read. A Remote
    # read that returns a dirty value is the violation (wrong-durable-read).
    remote_excl_ok = terminal_ok and remote_dirty_hits == 0
    invariant(
        "durability_filter_remote_excludes_dirty", "remote-excludes-not-yet-durable",
        remote_excl_ok,
        f"remote_dirty_hits={remote_dirty_hits} (MUST be 0)",
    )

    # --- INVARIANT durability_filter_remote_after_flush ----------------------
    # After db.flush() every value becomes visible to a Remote-filter read.
    remote_after_ok = terminal_ok and remote_flushed_hits == KEYS
    invariant(
        "durability_filter_remote_after_flush", "remote-sees-after-flush", remote_after_ok,
        f"remote_flushed_hits={remote_flushed_hits}/{KEYS}",
    )

    all_ok = terminal_ok and mem_ok and remote_excl_ok and remote_after_ok
    summary_line = (f"mem_dirty_hits={mem_dirty_hits} remote_dirty_hits={remote_dirty_hits} "
                    f"remote_flushed_hits={remote_flushed_hits} keys={KEYS} seed={seed}")

    if not all_ok:
        disarm_liveness(True, "verdict reached (discrimination oracle FAIL)")
        emit(f"VERDICT: RED — durability filter did not discriminate: {summary_line}")
        emit(f"REPLAY red: SEED={seed} case=baseline keys={KEYS}")
        return 1

    disarm_liveness(True, "verdict reached (discrimination oracle PASS)")
    emit(f"VERDICT: GREEN — Memory/Remote discrimination holds: {summary_line}")
    return 0


# ---------------------------------------------------------------------------
# crash-confirm case (executor fills next episode)
# ---------------------------------------------------------------------------


def run_remote_durawatch_ladder(case_name: str, root: str, remote_log: str, robs) -> None:
    """Manifest R_remote and re-observe it across reopen on a delay ladder.

    observe() re-reads by reopening the DB (`verify-remote`) at each rung, so a
    *delayed* erasure (compaction/GC after the crash-recovered value became
    readable) surfaces as a later-rung miss. Emits durability_watch_* invariants.
    """
    ladder = (0.0, 2.0)
    m = durawatch.Manifest.start(
        case=case_name,
        path=durawatch.manifest_path(case_name),
        ladder=ladder,
        void_floor=1,
    )
    for _seq, key, value in robs:
        m.record(eid=key, query=key, payload=value)

    cache = {"ts": 0.0, "bad": set(), "loaded": False}

    def refresh() -> None:
        v = run_driver(["verify-remote", "--root", root, "--remote-log", remote_log])
        _seen, _ok, _chk, _lost, _mm, bad = parse_verify_remote(v.stdout)
        cache["bad"] = bad
        cache["ts"] = time.time()
        cache["loaded"] = True

    recorded = {key: value for _s, key, value in robs}

    def observe(eff):
        if not cache["loaded"] or (time.time() - cache["ts"]) > 0.5:
            refresh()
        if eff.query in cache["bad"]:
            return None
        return recorded.get(eff.query)

    m.run_ladder(observe)


def case_crash_confirm(seed: int) -> int:
    """SIGKILL mid-stream, then assert R_remote ⊆ survivors.

    R_remote = the (key,value) set that `get_with_options(.., Remote)` returned
    BEFORE the crash (the driver's fsync'd remote-observed log). After SIGKILL +
    reopen, every logged value MUST still be present value-exact — a Remote value
    that vanishes is the SUT surfacing non-durable data through Remote, a
    wrong-durable-read (weight 3).
    """
    selftest = durawatch.selftest_active()

    # remote-log lives OUTSIDE the db root (a crash must never corrupt it).
    root = tempfile.mkdtemp(prefix="slatedb-durfilter-root-")
    rl_fd, remote_log = tempfile.mkstemp(prefix="slatedb-remote-", suffix=".log")
    os.close(rl_fd)
    os.unlink(remote_log)  # let the driver create it fresh (append semantics)

    # --- declared fault timing: SIGKILL after the seed-derived K-th observation --
    kill_k = kill_after_obs(seed)

    emit(f"CASE crash-confirm seed={seed} ops={CRASH_OPS} durable_every={DURABLE_EVERY} "
         f"root={root} remote_log={remote_log}")
    emit(f"CLOCK crash-confirm armed kind=obs_progress axis=crash_confirm "
         f"kill_after_obs={kill_k} max_obs={CRASH_KILL_MAX_OBS} seed={seed}")

    # --- spawn the remote-run producer in its own process group -----------------
    # start_new_session=True → child is the pgid leader; we SIGKILL the whole group
    # so no orphaned tokio worker survives. NO graceful cleanup: SIGKILL is abrupt.
    proc = subprocess.Popen(
        [str(DRIVER), "remote-run", "--root", root, "--remote-log", remote_log,
         "--seed", str(seed), "--ops", str(CRASH_OPS),
         "--durable-every", str(DURABLE_EVERY)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        text=True,
        start_new_session=True,
    )
    t_start = time.time()
    deadline = t_start + CRASH_KILL_DEADLINE_S
    # poll fsync'd remote-observation progress; kill the pg once K observations
    # landed (or the proc exits, or a hard wall deadline as a liveness backstop).
    while True:
        if count_remote_obs(remote_log) >= kill_k:
            break
        if proc.poll() is not None:
            break  # run ended before reaching K observations — VOID handled below
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

    robs = read_remote_log(remote_log)
    n_pre = len(robs)
    emit(f"KILLED mode={kill_mode} rc={killed_rc} remote_obs_before_kill={n_pre} "
         f"kill_after={kill_k} kill_wall={kill_wall:.3f}s ops={CRASH_OPS}")

    # --- anti-vacuity floors --------------------------------------------------
    # (1) rc==0 means the run streamed all CRASH_OPS and exited cleanly before we
    #     could kill — the SIGKILL never landed mid-stream, so a green is theater.
    if killed_rc == 0:
        disarm_liveness(True, "kill missed window (run completed)")
        emit(f"VERDICT: VOID — run completed all {CRASH_OPS} ops before the K-th "
             f"observation kill; raise SLATEDB_CRASH_OPS. seed={seed} kill_after={kill_k}")
        return 3
    # (2) at least one Remote observation must have been recorded pre-crash, else
    #     R_remote is empty and there is nothing to falsify.
    if n_pre < 1:
        disarm_liveness(True, "kill too early — no remote observations")
        emit(f"VERDICT: VOID — no Remote observations before the kill (driver stalled "
             f"or deadline hit before first flush); seed={seed} kill_after={kill_k}")
        return 3
    # (3) at least one await_durable=false write must have been issued, else Memory
    #     and Remote never diverge and the test is vacuous. The driver's write mix
    #     is fixed: only every DURABLE_EVERY-th op (seq>0) is await_durable=true, so
    #     any run that reached seq>=1 issued dirty writes. A Remote observation at
    #     op `seq` implies ops 0..seq ran; seq 0 itself is await_durable=false.
    max_obs_seq = max(int(s) for s, _k, _v in robs)
    dirty_issued = max_obs_seq >= 0  # seq 0 is always a dirty write
    if not dirty_issued:
        disarm_liveness(True, "no await_durable=false write issued")
        emit(f"VERDICT: VOID — no await_durable=false write issued; Memory/Remote "
             f"cannot diverge. seed={seed}")
        return 3
    # count of dirty writes definitely issued up to the last observation (for the log).
    n_dirty = sum(1 for i in range(0, max_obs_seq + 1)
                  if not (DURABLE_EVERY > 0 and i > 0 and i % DURABLE_EVERY == 0))
    emit(f"DIVERGENCE await_durable_false_writes_issued>={n_dirty} "
         f"(max_obs_seq={max_obs_seq} durable_every={DURABLE_EVERY}) — Memory/Remote diverge")

    # --- ORACLE_SELFTEST: plant a fake Remote-observed key the DB never wrote ---
    # Proves the RED path: verify-remote must report this key LOST →
    # durability_filter_remote_survives_crash FAIL + nonzero exit.
    if selftest:
        with open(remote_log, "a") as f:
            f.write("999999\tSELFTEST_MISSING\tselftest-injected\n")
            f.flush()
            os.fsync(f.fileno())
        emit("ORACLE_SELFTEST: injected fake remote-observed line key=SELFTEST_MISSING "
             "(verify-remote must report LOST → crash-confirm oracle must go RED)")
        robs = read_remote_log(remote_log)
        n_pre = len(robs)

    # --- verify-remote: reopen the SIGKILLed root + assert R_remote ⊆ survivors -
    ver = run_driver(["verify-remote", "--root", root, "--remote-log", remote_log])
    verdict_seen, subset_ok, checked, lost, mismatch, bad_keys = parse_verify_remote(ver.stdout)
    sys.stdout.write(ver.stdout)
    sys.stdout.flush()
    if ver.returncode != 0 and not verdict_seen:
        emit(f"DRIVER verify-remote failed rc={ver.returncode}\n{ver.stderr}")

    # --- terminal-state sweep: a verdict for every Remote-observed key ---------
    terminal_ok = verdict_seen and checked == n_pre
    invariant(
        "terminal_state", "remote-observed-keys-resolved", terminal_ok,
        f"verify-remote emitted a verdict for {checked}/{n_pre} Remote-observed keys "
        f"after SIGKILL+reopen"
        if terminal_ok else
        f"driver exited without a full verdict (verdict_seen={verdict_seen} "
        f"checked={checked} expected={n_pre})",
    )

    # --- INVARIANT durability_filter_remote_survives_crash (FALSIFIABLE CORE) ---
    # R_remote ⊆ survivors: every value Remote returned before the crash is still
    # present value-exact after reopen. A loss/mismatch is a wrong-durable-read RED.
    subset_pass = terminal_ok and subset_ok and lost == 0 and mismatch == 0
    summary = (f"checked={checked} lost={lost} mismatch={mismatch} "
               f"bad={sorted(bad_keys)[:8]} seed={seed} kill_after={kill_k}")
    invariant("durability_filter_remote_survives_crash", "acked-remote-durable",
              subset_pass, summary)

    if not subset_pass:
        disarm_liveness(True, "verdict reached (remote-survives-crash FAIL)")
        emit(f"VERDICT: RED — a Remote-observed value did not survive SIGKILL+reopen "
             f"(wrong-durable-read): {summary}")
        emit(f"REPLAY red: SEED={seed} case=crash-confirm kill_after={kill_k} "
             f"remote_obs_before_kill={n_pre} lost_keys={sorted(bad_keys)}")
        return 1

    disarm_liveness(True, "verdict reached (remote-survives-crash PASS)")
    # durawatch: re-observe R_remote across reopen on a delay ladder to catch
    # delayed erasure (compaction/GC after the crash-recovered read became visible).
    # run_ladder emits the durability_watch_* invariants and the final VERDICT.
    run_remote_durawatch_ladder("durability_filter_crash_confirm", root, remote_log, robs)
    return 0


# ---------------------------------------------------------------------------
# inflight-flush case (executor fills next episode)
# ---------------------------------------------------------------------------


def parse_inflight(stdout: str):
    """Return (per_key, summary).

    per_key: list of {key, remote_during_block, remote_after_release} bools.
    summary: dict from INFLIGHT_SUMMARY, or None.
    """
    per_key = []
    summary = None
    for line in stdout.splitlines():
        line = line.strip()
        if line.startswith("INFLIGHT_SUMMARY "):
            summary = {}
            for tok in line.split()[1:]:
                k, _, v = tok.partition("=")
                summary[k] = v
        elif line.startswith("INFLIGHT "):
            rec = {}
            for tok in line.split()[1:]:
                k, _, v = tok.partition("=")
                rec[k] = v
            per_key.append({
                "key": rec.get("key", ""),
                "remote_during_block": rec.get("remote_during_block") == "hit",
                "remote_after_release": rec.get("remote_after_release") == "hit",
            })
    return per_key, summary


def case_inflight_flush(seed: int) -> int:
    """Remote excludes an in-flight (not-yet-durable) value at the flush boundary.

    The driver holds the WAL SST PUT in-flight (blocked) while it issues
    `get_with_options(.., Remote)` for every key. At that instant the value lives
    in the WAL buffer but its WAL object is NOT yet persisted — a crash would lose
    it — so Remote MUST return None (during_block_hits == 0). A Remote hit here is
    a wrong-durable-read (weight 3). After the PUT is released Remote MUST return
    every value (after_release_hits == keys).

    Anti-vacuity: the fault must actually arm — put_was_blocked=true and >=1 key —
    else the in-flight window was never observed and a green is theater (VOID).
    """
    selftest = crashclock.selftest_active()

    root = tempfile.mkdtemp(prefix="slatedb-durfilter-root-")

    emit(f"CASE inflight-flush seed={seed} keys={KEYS} root={root}")
    emit("CLOCK inflight-flush armed kind=wal_put_block axis=flush_boundary "
         f"(WAL SST PUT held in-flight while Remote is probed) seed={seed}")

    probe = run_driver(
        ["inflight-probe", "--root", root, "--seed", str(seed), "--keys", str(KEYS)]
    )
    sys.stdout.write(probe.stdout)
    sys.stdout.flush()
    if probe.returncode != 0:
        emit(f"DRIVER inflight-probe failed rc={probe.returncode}\n{probe.stderr}")
        disarm_liveness(False, "driver inflight-probe crashed")
        invariant("durability_filter_remote_excludes_inflight",
                  "remote-excludes-mid-flight-put", False,
                  "driver inflight-probe did not complete")
        emit("VERDICT: RED — driver inflight-probe crashed")
        return 1

    per_key, summary = parse_inflight(probe.stdout)

    # --- terminal-state sweep: a verdict for every key -----------------------
    n_keys = len(per_key)
    terminal_ok = summary is not None and n_keys == KEYS and \
        int(summary.get("keys", "-1")) == KEYS
    invariant(
        "terminal_state", "inflight-keys-resolved", terminal_ok,
        f"inflight-probe emitted a verdict for {n_keys}/{KEYS} keys (summary={summary})"
        if terminal_ok else
        f"driver exited without a full verdict (per_key={n_keys} expected={KEYS} "
        f"summary={summary})",
    )

    # --- fault-arm / anti-vacuity: the WAL PUT must actually have been blocked --
    put_was_blocked = summary is not None and summary.get("put_was_blocked") == "true"

    during_block_hits = sum(1 for r in per_key if r["remote_during_block"])
    after_release_hits = sum(1 for r in per_key if r["remote_after_release"])

    # --- ORACLE_SELFTEST: force a Remote hit during the in-flight window -------
    # A real violation (Remote surfacing a value whose WAL PUT has not landed)
    # looks EXACTLY like this — the falsifiable core must go FAIL.
    if selftest and per_key:
        during_block_hits += 1
        emit("ORACLE_SELFTEST: injected fake remote_during_block hit "
             "(durability_filter_remote_excludes_inflight must go FAIL)")

    # --- anti-vacuity floor: fault must have armed on >=1 key -----------------
    if not put_was_blocked or KEYS < 1:
        disarm_liveness(True, "fault did not arm (WAL PUT not blocked)")
        reason = (f"put_was_blocked={put_was_blocked} keys={KEYS} — the WAL SST PUT "
                  f"was never held in-flight, so the flush-boundary window was not "
                  f"observed; cannot assert Remote exclusion. seed={seed}")
        emit(f"VERDICT: VOID — {reason}")
        return 3

    # --- INVARIANT durability_filter_remote_excludes_inflight (FALSIFIABLE CORE)
    # NO value whose WAL PUT is still in-flight may be visible to a Remote read.
    excl_ok = terminal_ok and during_block_hits == 0
    invariant(
        "durability_filter_remote_excludes_inflight", "remote-excludes-mid-flight-put",
        excl_ok,
        f"during_block_hits={during_block_hits} (MUST be 0) put_was_blocked={put_was_blocked}",
    )

    # --- INVARIANT durability_filter_remote_after_release --------------------
    # Once the WAL PUT lands, Remote MUST return every value.
    after_ok = terminal_ok and after_release_hits == KEYS
    invariant(
        "durability_filter_remote_after_release", "remote-sees-after-release", after_ok,
        f"after_release_hits={after_release_hits}/{KEYS}",
    )

    all_ok = terminal_ok and excl_ok and after_ok
    summary_line = (f"during_block_hits={during_block_hits} after_release_hits={after_release_hits} "
                    f"put_was_blocked={put_was_blocked} keys={KEYS} seed={seed}")

    if not all_ok:
        disarm_liveness(True, "verdict reached (inflight oracle FAIL)")
        emit(f"VERDICT: RED — Remote surfaced a value whose WAL PUT had not completed "
             f"(wrong-durable-read): {summary_line}")
        emit(f"REPLAY red: SEED={seed} case=inflight-flush keys={KEYS}")
        return 1

    disarm_liveness(True, "verdict reached (inflight oracle PASS)")
    emit(f"VERDICT: GREEN — Remote excluded the in-flight value at the flush boundary: "
         f"{summary_line}")
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
    ap.add_argument("--case", choices=["baseline", "crash-confirm", "inflight-flush"],
                    default="baseline")
    args = ap.parse_args()

    if not DRIVER.exists():
        emit(f"VERDICT: VOID — driver not found at {DRIVER} (run .workers/build.sh)")
        return 3

    arm_liveness()

    if args.case == "baseline":
        return case_baseline(seed)
    if args.case == "crash-confirm":
        return case_crash_confirm(seed)
    if args.case == "inflight-flush":
        return case_inflight_flush(seed)
    raise NotImplementedError(f"unknown case {args.case!r}")


if __name__ == "__main__":
    sys.exit(main())
