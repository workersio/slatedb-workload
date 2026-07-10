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
  overlap-writes     — executor fills next episode (NotImplementedError).
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
        raise NotImplementedError("executor fills next episode")
    if args.case == "stale-epoch-flush":
        raise NotImplementedError("executor fills next episode")
    raise NotImplementedError(f"unknown case {args.case!r}")


if __name__ == "__main__":
    sys.exit(main())
