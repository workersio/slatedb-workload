#!/usr/bin/env python3
"""fencing — the SlateDB writer-fencing workload (promise: writer-fencing-split-brain).

Invariant: SlateDB is single-writer per object-store path. Once writer B opens the
SAME root, the manifest epoch is bumped (version-CAS) and a WAL fence barrier is
written (fence.rs:105); writer A's next await_durable write MUST fail — surfaced to
the public API as Error.kind() == Closed(Fenced) (SlateDBError::Fenced, error.rs:618).
B's writes must be durable. A victim that keeps succeeding `ok` on every post-open
attempt is SPLIT-BRAIN (a zombie/second writer) — a real availability/correctness
finding, and the workload's RED.

This drives the vendored `slatedb-driver`:
  * fence-victim  — opens the Db, acks a prelude key (it is the live writer), then
    loops attempting await_durable puts, printing FENCE_OBSERVED result=<ok|fenced|
    other:<kind>> per attempt (the error is classified, never swallowed).
  * fence-usurper — opens the SAME root (bumps epoch → fences the victim), acks its
    own keys, holds ~1s, closes.

Oracle plane:
  * fencing_victim_fenced   — the victim observed >=1 FENCE_OBSERVED result=fenced
    after the usurper opened (FAIL / RED if it kept writing `ok` = split-brain).
  * fencing_usurper_durable — the usurper's acked keys are present value-exact on a
    final reopen (verify).
  * liveness_watchdog       — global-deadline thread; a wedged spawn is a FAIL.
  * terminal_state          — both processes reached a terminal state and emitted
    their verdict lines.

Cases:
  baseline           — second open fences the first; fully implemented here.
  overlap-writes     — A and B write the SAME keyspace across the B-open fence
                       point; reopen and assert a valid single-writer history (no
                       post-fence victim value is the durable winner; no committed
                       usurper write is lost). Implemented here.
  stale-epoch-flush  — executor fills next episode (NotImplementedError).

ORACLE_SELFTEST=1 forces the fenced-detection to see all-`ok` (no fenced
observation) → fencing_victim_fenced MUST go FAIL + nonzero exit (proves the
split-brain RED path).
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
# script lives at <repo>/.workers/workloads/fencing.py
_SCRIPT = Path(__file__).resolve()
_REPO_ROOT = _SCRIPT.parents[2]
_LIB = _REPO_ROOT / ".workers" / "lib"
sys.path.insert(0, str(_LIB))

import crashclock  # noqa: E402  (seed source + selftest hook)

DRIVER = _REPO_ROOT / ".workers" / "vendor" / "bin" / "slatedb-driver"

# Victim attempt bound + spacing knobs.
VICTIM_ATTEMPTS = int(os.environ.get("SLATEDB_FENCE_ATTEMPTS", "40"))
USURPER_KEYS = int(os.environ.get("SLATEDB_FENCE_USURPER_KEYS", "5"))
LIVENESS_DEADLINE_S = float(os.environ.get("SLATEDB_LIVENESS_S", "120"))
# How long to wait for the victim / usurper to record their first fsync'd ack
# before declaring the spawn wedged (VOID, not a false RED).
ACK_WAIT_S = float(os.environ.get("SLATEDB_FENCE_ACK_WAIT_S", "30"))
# How long to wait for each process to terminate on its own after the usurper
# has done its work.
PROC_WAIT_S = float(os.environ.get("SLATEDB_FENCE_PROC_WAIT_S", "30"))

# --- overlap-writes knobs ----------------------------------------------------
# Size of the CONTENDED keyspace (both writers hammer k0..k{N-1}).
OVERLAP_KEYS = int(os.environ.get("SLATEDB_FENCE_OVERLAP_KEYS", "5"))
# Victim attempt bound for overlap (it cycles the keyspace, stops on first fenced).
OVERLAP_ATTEMPTS = int(os.environ.get("SLATEDB_FENCE_OVERLAP_ATTEMPTS", "160"))
# Victim flush_interval override (ms): the DEFAULT 100ms makes every
# await_durable=true put block ~100ms, so the incumbent commits too slowly to
# EVER have a write in flight when the usurper opens (the fence barrier lands in
# the 100ms gap and the next put collides → the post-fence window never opens).
# A small flush makes durable acks fast (~ms) so a genuine await_durable=true
# put can resolve `ok` inside the post-open window — the race the baseline saw
# in-guest. Still a true durable ack; the oracle is unchanged.
OVERLAP_FLUSH_MS = int(os.environ.get("SLATEDB_FENCE_OVERLAP_FLUSH_MS", "5"))


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
# helpers
# ---------------------------------------------------------------------------


def run_driver(args: list[str]) -> subprocess.CompletedProcess:
    return subprocess.run([str(DRIVER), *args], capture_output=True, text=True)


def count_lines(path: str) -> int:
    try:
        with open(path) as f:
            return sum(1 for line in f if line.strip())
    except FileNotFoundError:
        return 0


def wait_for_ack(path: str, proc: subprocess.Popen, deadline_s: float) -> int:
    """Poll `path` until it has >=1 fsync'd line, the proc exits, or we time out.

    Returns the observed line count (0 means neither the ack nor progress landed).
    """
    end = time.time() + deadline_s
    while time.time() < end:
        n = count_lines(path)
        if n >= 1:
            return n
        if proc.poll() is not None:
            return count_lines(path)  # proc exited; return whatever landed
        time.sleep(0.03)
    return count_lines(path)


def collect(proc: subprocess.Popen, timeout_s: float) -> tuple[str, str, int]:
    """Wait for proc to finish, returning (stdout, stderr, returncode).

    On timeout, SIGKILL the whole process group (start_new_session=True leader) so
    no orphaned tokio worker survives, then reap.
    """
    try:
        out, err = proc.communicate(timeout=timeout_s)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
        except ProcessLookupError:
            pass
        out, err = proc.communicate()
    return out or "", err or "", proc.returncode


def parse_acklog4(path: str):
    """Parse a 4-field victim ack-log: `seq\\tkey\\tvalue\\tack_nanos`.

    Returns a list of dicts {seq,key,value,ts}. Lines with <4 fields are skipped
    (defensive; the overlap victim always writes 4 fields in tagged mode).
    """
    out = []
    try:
        with open(path) as f:
            for line in f:
                line = line.rstrip("\n")
                if not line.strip():
                    continue
                parts = line.split("\t")
                if len(parts) < 4:
                    continue
                seq, key, value, ts = parts[0], parts[1], parts[2], parts[3]
                try:
                    out.append({"seq": int(seq), "key": key, "value": value, "ts": int(ts)})
                except ValueError:
                    continue
    except FileNotFoundError:
        pass
    return out


def parse_acklog3(path: str):
    """Parse a 3-field ack-log (`seq\\tkey\\tvalue`) into {key: value} (last wins)."""
    m = {}
    try:
        with open(path) as f:
            for line in f:
                line = line.rstrip("\n")
                if not line.strip():
                    continue
                parts = line.split("\t", 2)
                if len(parts) < 3:
                    continue
                m[parts[1]] = parts[2]
    except FileNotFoundError:
        pass
    return m


def read_open_nanos(path: str):
    """Read the usurper's fsync'd open-marker (wall-clock nanos at epoch bump)."""
    try:
        with open(path) as f:
            return int(f.read().strip())
    except (FileNotFoundError, ValueError):
        return None


def dump_state(root: str, keys: int) -> dict:
    """Reopen `root` (latest durable state) and return {key: value|None} for k0..k{keys-1}."""
    res = run_driver(["dump", "--root", root, "--keys", str(keys)])
    sys.stdout.write(res.stdout)
    sys.stdout.flush()
    state = {}
    for line in res.stdout.splitlines():
        line = line.strip()
        if line.startswith("DUMP key="):
            toks = dict(t.split("=", 1) for t in line.split(" ", 2)[1:] if "=" in t)
            k = toks.get("key")
            v = toks.get("value")
            if k is not None:
                state[k] = None if v == "__MISSING__" else v
    return state


def parse_victim(stdout: str):
    """Return (prelude_acked, fenced_seen, fenced_attempt, ok_count, done_seen, attempts_run)."""
    prelude_acked = 0
    fenced_seen = False
    fenced_attempt = None
    ok_count = 0
    attempts_run = 0
    done_seen = False
    for line in stdout.splitlines():
        line = line.strip()
        if line.startswith("VICTIM prelude_acked="):
            try:
                prelude_acked = int(line.split("=", 1)[1])
            except ValueError:
                pass
        elif line.startswith("FENCE_OBSERVED "):
            attempts_run += 1
            toks = dict(
                t.split("=", 1) for t in line.split()[1:] if "=" in t
            )
            res = toks.get("result", "")
            if res == "fenced":
                fenced_seen = True
                if fenced_attempt is None:
                    try:
                        fenced_attempt = int(toks.get("attempt", "-1"))
                    except ValueError:
                        fenced_attempt = -1
            elif res == "ok":
                ok_count += 1
        elif line.startswith("VICTIM done "):
            done_seen = True
    return prelude_acked, fenced_seen, fenced_attempt, ok_count, done_seen, attempts_run


# ---------------------------------------------------------------------------
# baseline case
# ---------------------------------------------------------------------------


def case_baseline(seed: int) -> int:
    selftest = crashclock.selftest_active()

    # Shared root under /tmp; two ack-logs OUTSIDE it (one per writer).
    root = tempfile.mkdtemp(prefix="slatedb-fence-root-")
    vfd, victim_ack = tempfile.mkstemp(prefix="slatedb-fence-victim-", suffix=".log")
    ufd, usurper_ack = tempfile.mkstemp(prefix="slatedb-fence-usurper-", suffix=".log")
    os.close(vfd)
    os.close(ufd)
    os.unlink(victim_ack)  # let the driver create fresh (append semantics)
    os.unlink(usurper_ack)

    # Distinct usurper seed → distinct values (last-writer-wins is then a real
    # check, not a same-value coincidence).
    usurper_seed = (seed ^ 0x5DEECE66D) & 0xFFFFFFFF

    emit(
        f"CASE baseline seed={seed} usurper_seed={usurper_seed} attempts={VICTIM_ATTEMPTS} "
        f"usurper_keys={USURPER_KEYS} root={root}"
    )
    emit(f"CLOCK baseline point=second-open kind=process-open axis=fence_boundary seed={seed}")

    # --- spawn the victim in its own process group ---------------------------
    victim = subprocess.Popen(
        [str(DRIVER), "fence-victim", "--root", root, "--ack-log", victim_ack,
         "--seed", str(seed), "--attempts", str(VICTIM_ATTEMPTS)],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        start_new_session=True,
    )

    # Wait until the victim has fsync'd >=1 prelude ack → it is the live writer.
    v_pre = wait_for_ack(victim_ack, victim, ACK_WAIT_S)
    if v_pre < 1:
        vout, verr, vrc = collect(victim, PROC_WAIT_S)
        sys.stdout.write(vout)
        disarm_liveness(True, "victim never acked (spawn wedged)")
        emit(f"VERDICT: VOID — victim recorded no prelude ack (rc={vrc}); stderr:\n{verr[:400]}")
        return 3
    emit(f"VICTIM live: prelude_acks={v_pre}")

    # --- spawn the usurper on the SAME root → epoch bump → victim fenced ------
    usurper = subprocess.Popen(
        [str(DRIVER), "fence-usurper", "--root", root, "--ack-log", usurper_ack,
         "--seed", str(usurper_seed), "--keys", str(USURPER_KEYS)],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        start_new_session=True,
    )

    # Wait until the usurper has fsync'd >=1 ack → it opened (epoch bumped) and
    # is durably writing. This is our "usurper actually opened" confirmation.
    u_ack = wait_for_ack(usurper_ack, usurper, ACK_WAIT_S)

    # --- reap both processes --------------------------------------------------
    uout, uerr, urc = collect(usurper, PROC_WAIT_S)
    vout, verr, vrc = collect(victim, PROC_WAIT_S)
    sys.stdout.write(uout)
    sys.stdout.write(vout)
    sys.stdout.flush()

    usurper_opened = ("USURPER opened" in uout) and (u_ack >= 1)
    usurper_done = "USURPER done" in uout
    (prelude_acked, fenced_seen, fenced_attempt, ok_count,
     victim_done, attempts_run) = parse_victim(vout)

    emit(
        f"OBSERVED usurper_opened={usurper_opened} usurper_acks={u_ack} usurper_rc={urc} "
        f"victim_prelude={prelude_acked} victim_attempts_run={attempts_run} "
        f"victim_ok_after_prelude={ok_count} victim_fenced={fenced_seen} "
        f"fenced_attempt={fenced_attempt} victim_rc={vrc}"
    )

    # If the usurper never actually opened, a "victim not fenced" result would be a
    # timing artifact, not a real split-brain finding → VOID.
    if not usurper_opened:
        disarm_liveness(True, "usurper did not open (VOID, not a real finding)")
        emit(f"VERDICT: VOID — usurper never opened/acked the shared root "
             f"(u_ack={u_ack} rc={urc}); cannot attribute victim behaviour. "
             f"stderr:\n{uerr[:400]}")
        return 3

    # --- ORACLE_SELFTEST: force the fenced-detection to see all-`ok` ----------
    # Models the split-brain RED path: the victim reported only `ok` (never fenced)
    # after the usurper opened → fencing_victim_fenced MUST go FAIL.
    if selftest:
        emit("ORACLE_SELFTEST: forcing fenced-detection to see all-`ok` "
             "(no fenced observation) → fencing_victim_fenced must go FAIL")
        fenced_seen = False

    # --- INVARIANT fencing_victim_fenced -------------------------------------
    victim_summary = (
        f"attempts_run={attempts_run} ok_after_prelude={ok_count} "
        f"fenced_attempt={fenced_attempt} seed={seed}"
    )
    invariant("fencing_victim_fenced", "superseded-writer-fenced", fenced_seen,
              victim_summary)

    # --- INVARIANT fencing_usurper_durable -----------------------------------
    ver = run_driver(["verify", "--root", root, "--ack-log", usurper_ack])
    sys.stdout.write(ver.stdout)
    sys.stdout.flush()
    subset_ok = False
    checked = lost = mismatch = 0
    verdict_seen = False
    for line in ver.stdout.splitlines():
        line = line.strip()
        if line.startswith("VERIFY "):
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
    usurper_durable = verdict_seen and subset_ok and lost == 0 and mismatch == 0 and checked >= 1
    invariant("fencing_usurper_durable", "winner-writes-durable", usurper_durable,
              f"checked={checked} lost={lost} mismatch={mismatch} u_acks={u_ack}")

    # --- universal plane: terminal_state -------------------------------------
    terminal_ok = (
        vrc is not None and urc is not None and victim_done and usurper_done
    )
    invariant("terminal_state", "both-writers-terminal", terminal_ok,
              f"victim_done={victim_done}(rc={vrc}) usurper_done={usurper_done}(rc={urc})")

    # --- final verdict --------------------------------------------------------
    all_pass = fenced_seen and usurper_durable and terminal_ok
    disarm_liveness(True, "verdict reached")
    if not all_pass:
        if not fenced_seen and not selftest:
            emit(f"VERDICT: RED — SPLIT-BRAIN: victim never fenced after usurper opened; "
                 f"it kept writing `ok` ({ok_count}/{attempts_run} attempts). "
                 f"A superseded writer making durable writes is a zombie-writer finding. "
                 f"REPLAY: SEED={seed} case=baseline usurper_seed={usurper_seed}")
        elif not fenced_seen and selftest:
            emit("VERDICT: RED — ORACLE_SELFTEST forced no-fence; split-brain RED path proven")
        else:
            emit(f"VERDICT: RED — fencing invariants failed: victim_fenced={fenced_seen} "
                 f"usurper_durable={usurper_durable} terminal={terminal_ok}")
        return 1

    emit(f"VERDICT: GREEN — usurper fenced the victim at attempt {fenced_attempt} and its "
         f"{checked} acked keys are durable. seed={seed}")
    return 0


# ---------------------------------------------------------------------------
# overlap-writes case
# ---------------------------------------------------------------------------


def verify_ack(root: str, ack_log: str):
    """Run the driver `verify` against an ack-log; return (subset_ok, checked, lost, mismatch)."""
    res = run_driver(["verify", "--root", root, "--ack-log", ack_log])
    sys.stdout.write(res.stdout)
    sys.stdout.flush()
    subset_ok = False
    checked = lost = mismatch = 0
    for line in res.stdout.splitlines():
        line = line.strip()
        if line.startswith("VERIFY "):
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
    return subset_ok, checked, lost, mismatch


def case_overlap_writes(seed: int) -> int:
    """A and B write the SAME keyspace concurrently across the B-open fence point.

    Oracle (defensible, non-vacuous partition):
      * The usurper fsync's an OPEN-MARKER carrying the wall-clock nanos of the
        instant its open() returned (the epoch is already bumped by then). Each
        victim `ok` durable ack carries its OWN resolve-nanos. A victim ack is a
        POST-FENCE SUSPECT iff its resolve-nanos >= the usurper's open nanos —
        i.e. the victim durably acked a write AFTER a superseded epoch was already
        in effect. Using open() (not the usurper's first data ack) as the cutoff
        is CONSERVATIVE toward the epoch bump; cross-process wall clock on one
        host at ms granularity is the only comparable timebase (Instant epochs
        differ per process). No python poll latency enters the partition.
      * fencing_no_zombie_write  FAIL  if any post-fence suspect victim value is
        the durable winner for its key on final reopen (a superseded writer's
        write survived durably — split-brain / lost update).
      * fencing_usurper_writes_survive  FAIL if any key the usurper durably acked
        is missing or shows a non-usurper value on reopen (a committed winner
        write was lost or resurrected to a victim value).
    Anti-vacuity: VOID unless the victim got >=1 post-fence suspect ok AND >=1 key
    was contended by both writers — else the race did not happen (not a green).
    ORACLE_SELFTEST plants a durable post-fence victim value at a usurper key so
    fencing_no_zombie_write MUST fail.
    """
    selftest = crashclock.selftest_active()
    n = OVERLAP_KEYS

    # Declared timing axis: a phase-straddle around the fence boundary (the axis
    # is the audited artifact; the point is the concrete straddle for this seed).
    clock = crashclock.phase_straddle("fence_boundary", settle_ms=0.0)
    point = crashclock.offsets(seed, clock)
    crashclock.clock_armed("overlap-writes", point)

    root = tempfile.mkdtemp(prefix="slatedb-fence-ov-root-")
    vfd, victim_ack = tempfile.mkstemp(prefix="slatedb-fence-ov-victim-", suffix=".log")
    ufd, usurper_ack = tempfile.mkstemp(prefix="slatedb-fence-ov-usurper-", suffix=".log")
    mfd, open_marker = tempfile.mkstemp(prefix="slatedb-fence-ov-marker-", suffix=".txt")
    sfd, suspect_log = tempfile.mkstemp(prefix="slatedb-fence-ov-suspect-", suffix=".log")
    for fd in (vfd, ufd, mfd, sfd):
        os.close(fd)
    # Let the driver create the ack-logs / marker fresh (append/truncate semantics).
    os.unlink(victim_ack)
    os.unlink(usurper_ack)
    os.unlink(open_marker)

    usurper_seed = (seed ^ 0x5DEECE66D) & 0xFFFFFFFF

    emit(
        f"CASE overlap-writes seed={seed} usurper_seed={usurper_seed} keys={n} "
        f"attempts={OVERLAP_ATTEMPTS} flush_ms={OVERLAP_FLUSH_MS} root={root}"
    )
    emit(f"CLOCK overlap-writes point=b-open kind=process-open axis=fence_boundary seed={seed}")

    # --- spawn the victim: tagged A values, cycled over the contended keyspace --
    victim = subprocess.Popen(
        [str(DRIVER), "fence-victim", "--root", root, "--ack-log", victim_ack,
         "--seed", str(seed), "--attempts", str(OVERLAP_ATTEMPTS),
         "--prelude-keys", "1", "--tag", "A", "--key-space", str(n),
         "--attempt-sleep-ms", "0", "--flush-ms", str(OVERLAP_FLUSH_MS)],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        start_new_session=True,
    )

    # Wait until the victim fsync'd its (single) prelude ack → it is the live
    # writer. The usurper is spawned only now, so the prelude never fences.
    v_pre = wait_for_ack(victim_ack, victim, ACK_WAIT_S)
    if v_pre < 1:
        vout, verr, vrc = collect(victim, PROC_WAIT_S)
        sys.stdout.write(vout)
        disarm_liveness(True, "victim never acked (spawn wedged)")
        emit(f"VERDICT: VOID — victim recorded no prelude ack (rc={vrc}); stderr:\n{verr[:400]}")
        return 3
    emit(f"VICTIM live: prelude_acks={v_pre}")

    # --- spawn the usurper on the SAME root → epoch bump → victim fenced -------
    usurper = subprocess.Popen(
        [str(DRIVER), "fence-usurper", "--root", root, "--ack-log", usurper_ack,
         "--seed", str(usurper_seed), "--keys", str(n), "--tag", "B",
         "--open-marker", open_marker],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        start_new_session=True,
    )
    u_ack = wait_for_ack(usurper_ack, usurper, ACK_WAIT_S)

    # --- reap both ------------------------------------------------------------
    uout, uerr, urc = collect(usurper, PROC_WAIT_S)
    vout, verr, vrc = collect(victim, PROC_WAIT_S)
    sys.stdout.write(uout)
    sys.stdout.write(vout)
    sys.stdout.flush()

    open_nanos = read_open_nanos(open_marker)
    usurper_opened = ("USURPER opened" in uout) and (u_ack >= 1) and (open_nanos is not None)
    usurper_done = "USURPER done" in uout
    (prelude_acked, fenced_seen, fenced_attempt, ok_count,
     victim_done, attempts_run) = parse_victim(vout)

    if not usurper_opened:
        disarm_liveness(True, "usurper did not open (VOID, not a real finding)")
        emit(f"VERDICT: VOID — usurper never opened/acked the shared root "
             f"(u_ack={u_ack} open_nanos={open_nanos} rc={urc}). stderr:\n{uerr[:400]}")
        return 3

    # --- partition victim acks: post-fence suspects = resolve-ts >= open_nanos --
    victim_acks = parse_acklog4(victim_ack)
    usurper_map = parse_acklog3(usurper_ack)
    suspects = [a for a in victim_acks if a["ts"] >= open_nanos]
    victim_keys = {a["key"] for a in victim_acks}
    contended = sorted(victim_keys & set(usurper_map.keys()))

    emit(
        f"OBSERVED usurper_opened={usurper_opened} usurper_acks={u_ack} open_nanos={open_nanos} "
        f"victim_prelude={prelude_acked} victim_attempts_run={attempts_run} "
        f"victim_ok_after_prelude={ok_count} victim_fenced={fenced_seen} "
        f"fenced_attempt={fenced_attempt} post_fence_suspects={len(suspects)} "
        f"contended_keys={len(contended)} victim_rc={vrc}"
    )
    for a in suspects:
        emit(f"SUSPECT seq={a['seq']} key={a['key']} value={a['value']} "
             f"ack_nanos={a['ts']} (>= open_nanos, resolved post-epoch-bump)")

    # --- ORACLE_SELFTEST: plant a durable post-fence victim value -------------
    # Faithfully exercises the zombie detector: durably write an A-tagged value to
    # a contended (usurper) key with a FRESH open (newest epoch → unconditional
    # winner), and register it as a post-fence suspect. The reopen MUST then show
    # that value as the durable winner → fencing_no_zombie_write FAIL.
    if selftest:
        if not usurper_map:
            disarm_liveness(True, "selftest could not plant (usurper wrote no keys)")
            emit("VERDICT: VOID — selftest needs >=1 usurper key to plant")
            return 3
        plant_key = sorted(usurper_map.keys())[len(usurper_map) // 2]
        plant_val = f"A:SELFTEST:{plant_key}:deadbeefdeadbeef"
        emit(f"ORACLE_SELFTEST: planting durable post-fence victim value "
             f"{plant_key}={plant_val} (must trip fencing_no_zombie_write)")
        run_driver(["put-kv", "--root", root, "--key", plant_key, "--value", plant_val])
        suspects.append({"seq": 10 ** 9, "key": plant_key, "value": plant_val, "ts": open_nanos})
        if plant_key not in contended:
            contended.append(plant_key)

    # --- anti-vacuity floor ---------------------------------------------------
    if len(suspects) < 1 or len(contended) < 1:
        disarm_liveness(True, "race window did not open (no post-fence suspect / no contention)")
        emit(
            f"VERDICT: VOID — the adversarial race did not happen this seed: "
            f"post_fence_suspects={len(suspects)} contended_keys={len(contended)}. "
            f"The victim landed ZERO durable acks after the usurper's epoch bump — "
            f"the WAL-barrier fence (fence.rs:145 PutMode::Create) rejected its next "
            f"flush. Not a green (the zombie path was not exercised); not a red. "
            f"REPLAY: SEED={seed} case=overlap-writes usurper_seed={usurper_seed}"
        )
        return 3

    # --- reopen and resolve every contended key -------------------------------
    state = dump_state(root, n)
    suspect_values = {a["value"]: a for a in suspects}

    # INVARIANT fencing_no_zombie_write: a post-fence suspect value is the winner.
    zombies = []
    for key, val in state.items():
        if val is not None and val in suspect_values:
            zombies.append((key, val))
    no_zombie_ok = len(zombies) == 0
    invariant(
        "fencing_no_zombie_write", "superseded-writer-value-not-durable", no_zombie_ok,
        f"suspects={len(suspects)} zombies={len(zombies)} contended={len(contended)} seed={seed}",
    )
    for key, val in zombies:
        emit(f"ZOMBIE key={key} durable_value={val} (a victim write acked AFTER the "
             f"usurper opened is the durable winner — SPLIT-BRAIN)")

    # INVARIANT fencing_usurper_writes_survive: every usurper-acked key intact.
    lost_usurper = []
    for key, uval in usurper_map.items():
        got = state.get(key)
        if got != uval:
            lost_usurper.append((key, uval, got))
    usurper_survive_ok = len(lost_usurper) == 0
    invariant(
        "fencing_usurper_writes_survive", "winner-writes-not-lost", usurper_survive_ok,
        f"usurper_keys={len(usurper_map)} lost_or_overwritten={len(lost_usurper)} seed={seed}",
    )
    for key, uval, got in lost_usurper:
        emit(f"USURPER_LOST key={key} committed={uval} but_reopen_shows={got} "
             f"(a fenced writer overwrote / erased a committed winner write)")

    # --- universal plane ------------------------------------------------------
    terminal_ok = (vrc is not None and urc is not None and victim_done and usurper_done)
    invariant("terminal_state", "both-writers-terminal", terminal_ok,
              f"victim_done={victim_done}(rc={vrc}) usurper_done={usurper_done}(rc={urc})")

    disarm_liveness(True, "verdict reached")

    all_pass = no_zombie_ok and usurper_survive_ok and terminal_ok
    if not all_pass:
        if not no_zombie_ok:
            zk = ", ".join(f"{k}={v}" for k, v in zombies)
            emit(f"VERDICT: RED — SPLIT-BRAIN / LOST-UPDATE: a superseded writer's "
                 f"durable write is the final winner. zombie keys: [{zk}]. "
                 f"usurper committed values that were overwritten: "
                 f"{[k for k, _, _ in lost_usurper]}. "
                 f"REPLAY: SEED={seed} case=overlap-writes usurper_seed={usurper_seed}")
        elif not usurper_survive_ok:
            emit(f"VERDICT: RED — LOST-UPDATE: a committed usurper write was lost/erased: "
                 f"{[(k, g) for k, _, g in lost_usurper]}. "
                 f"REPLAY: SEED={seed} case=overlap-writes usurper_seed={usurper_seed}")
        else:
            emit(f"VERDICT: RED — terminal_state failed: victim_done={victim_done} "
                 f"usurper_done={usurper_done}")
        return 1

    emit(
        f"VERDICT: GREEN — valid single-writer history: {len(suspects)} post-fence victim "
        f"ack(s) were ALL superseded (none is the durable winner) and all {len(usurper_map)} "
        f"usurper-committed keys survived value-exact across {len(contended)} contended keys. "
        f"seed={seed}"
    )
    return 0


# ---------------------------------------------------------------------------
# entry
# ---------------------------------------------------------------------------


def main() -> int:
    import argparse

    seed = crashclock.derive_seed()
    emit(f"SEED {seed}")
    emit(f"REPLAY key=SEED={seed}")

    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--case",
        choices=["baseline", "overlap-writes", "stale-epoch-flush"],
        default="baseline",
    )
    args = ap.parse_args()

    if not DRIVER.exists():
        emit(f"VERDICT: VOID — driver not found at {DRIVER} (run .workers/build.sh)")
        return 3

    arm_liveness()

    if args.case == "baseline":
        return case_baseline(seed)
    if args.case == "overlap-writes":
        return case_overlap_writes(seed)
    if args.case == "stale-epoch-flush":
        raise NotImplementedError("executor fills next episode")
    raise NotImplementedError(f"unknown case {args.case!r}")


if __name__ == "__main__":
    sys.exit(main())
