# Loop state
- rails: { loops: 100, workloads: 250 }   # defaults — safety rails, not targets
- counters: { episodes: 6, producer: 1, executor: 5, workloads: 5 }
- no-new-info: { streak: 0, K: 5 }
- in-flight unit: none
- re-entry: durability-filter-remote-crash-confirm → deepen — GREEN (every Remote-observed value survived SIGKILL; Remote never surfaced non-durable data). Last rung inflight-flush completes the ladder floor, already ready. No L change (adversarial green, distinct harness — not a same-corridor superset).
- last-scanned-sha: 016b676ee125f02cb14054cce0cd5a78f3524ac5
- target-head-sha: 016b676ee125f02cb14054cce0cd5a78f3524ac5
- re-plan triggers: none
- publish-pending: []   # all 3 durable-ack officials live (baseline nd7db1rk, crash-mid-flush nd72ajfy, wal-head-contiguity nd7epmpg)
- last episode summary: |
    Executor #5 (durability-filter-remote-crash-confirm) — GREEN. Driver gained
    `remote-run` (logs each Remote-observed (k,v) fsync'd outside root) +
    `verify-remote`. SIGKILL after K-th Remote observation; every Remote-observed
    value survived reopen (R_remote ⊆ survivors) across the seed sweep;
    DIVERGENCE confirms Memory/Remote actually diverged. Remote watermark
    reader.rs:111-113. Selftest red proven.

    RESUME POINTER (fresh session): dispatcher row 5 → executor on
    **durability-filter-remote-inflight-flush** (status: ready) — the LAST rung
    of this promise (completes the ladder floor). Read at the boundary where a
    value is in the memtable/WAL buffer but its WAL SST PUT has not completed;
    Remote MUST exclude it (a Remote hit on a not-yet-persisted value = crash at
    that instant loses it = wrong-durable-read RED). Arm the flush at seed-swept
    crashclock points against the ack/durable_seq gate (wal_buffer.rs:334-338).
    Reuses remote-run/durprobe. After it: the BACKLOG (top clone-consistency 400,
    compacted-gc-vs-reader 400 — both need Admin::create_clone / a GC loop wired
    into the driver = a bigger build; consider a producer episode to plan those
    driver extensions). Driver recipe + durprobe/remote-run + verdict gotchas in
    runs/executor-notes.md. Nothing in flight; all state committed.
