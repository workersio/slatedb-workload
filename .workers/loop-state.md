# Loop state
- rails: { loops: 100, workloads: 250 }   # defaults — safety rails, not targets
- counters: { episodes: 5, producer: 1, executor: 4, workloads: 4 }
- no-new-info: { streak: 0, K: 5 }
- in-flight unit: none
- re-entry: durability-filter-remote-baseline → deepen — GREEN (non-vacuity control: Remote excludes not-yet-durable data). Next rung crash-confirm (R_remote ⊆ survivors after SIGKILL) is the adversarial bug hunt, already ready. No L change (control green, weak evidence).
- last-scanned-sha: 016b676ee125f02cb14054cce0cd5a78f3524ac5
- target-head-sha: 016b676ee125f02cb14054cce0cd5a78f3524ac5
- re-plan triggers: none
- publish-pending: []   # all 3 durable-ack officials live (baseline nd7db1rk, crash-mid-flush nd72ajfy, wal-head-contiguity nd7epmpg)
- last episode summary: |
    Executor #4 (durability-filter-remote-baseline) — GREEN (non-vacuity
    control). Driver gained `durprobe` (flush_interval=None → deterministic dirty
    window): await_durable=false write visible to Memory, excluded from Remote
    until db.flush(). reader.rs:112-113 caps Remote at last_remote_persisted_seq;
    in-tree tests corroborate. remote_dirty_hits=0 is the falsifiable core.
    Selftest red proven.

    RESUME POINTER (fresh session): dispatcher row 5 → executor on
    **durability-filter-remote-crash-confirm** (status: ready) — the adversarial
    rung: snapshot the Remote read-set, SIGKILL (reuse crash-mid-flush's
    ack-progress kill), reopen, assert R_remote ⊆ survivors (a Remote value that
    then vanishes = wrong-durable-read). Interleave await_durable=false writers so
    Memory/Remote diverge. Then durability-filter-remote-inflight-flush, then the
    backlog (top: clone-consistency 400, compacted-gc-vs-reader 400 — both need
    Admin::create_clone / a GC loop wired into the driver = a bigger build).
    Driver rebuild recipe + verdict-reading gotchas + durprobe in
    runs/executor-notes.md. Nothing in flight; all state committed.
