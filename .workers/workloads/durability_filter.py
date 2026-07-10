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
import subprocess
import sys
import tempfile
import threading
from pathlib import Path

# --- resolve repo root and the universal oracle-plane lib --------------------
# script lives at <repo>/.workers/workloads/durability_filter.py
_SCRIPT = Path(__file__).resolve()
_REPO_ROOT = _SCRIPT.parents[2]
_LIB = _REPO_ROOT / ".workers" / "lib"
sys.path.insert(0, str(_LIB))

import crashclock  # noqa: E402  (seed source + selftest gate; baseline arms no clock)

DRIVER = _REPO_ROOT / ".workers" / "vendor" / "bin" / "slatedb-driver"

KEYS = int(os.environ.get("SLATEDB_DURFILTER_KEYS", "8"))
LIVENESS_DEADLINE_S = float(os.environ.get("SLATEDB_LIVENESS_S", "120"))


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


def case_crash_confirm(seed: int) -> int:
    raise NotImplementedError("executor fills next episode")


# ---------------------------------------------------------------------------
# inflight-flush case (executor fills next episode)
# ---------------------------------------------------------------------------


def case_inflight_flush(seed: int) -> int:
    raise NotImplementedError("executor fills next episode")


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
