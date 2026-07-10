# Loop state
- rails: { loops: 100, workloads: 250 }   # defaults — safety rails, not targets
- counters: { episodes: 4, producer: 1, executor: 2, workloads: 2 }
- no-new-info: { streak: 0, K: 5 }
- in-flight unit: none
- re-entry: durable-ack-crash-mid-flush → deepen — green (SlateDB recovers acked writes from mid-flush kill); next rung wal-head-contiguity is the genuine-bug candidate AND the ladder-floor completer. No L change (2 greens on distinct harnesses, not same-corridor supersets).
- last-scanned-sha: 016b676ee125f02cb14054cce0cd5a78f3524ac5
- target-head-sha: 016b676ee125f02cb14054cce0cd5a78f3524ac5
- re-plan triggers: none
- publish-pending: [durable-ack-crash-mid-flush]   # transient convex 503/query blips on 2026-07-10; re-fire publish.py (idempotent) — baseline official already live (nd773r0v)
- last episode summary: |
    Executor #2 (durable-ack-crash-mid-flush) — GREEN. SIGKILL after the
    seed-derived K-th fsync'd ack (ack-progress trigger, portable: the sim is
    ~0.56s/ack vs box ~0.1s, so wall-clock windows VOID — keyed on SUT progress
    instead). All acked writes recovered across the seed sweep; durawatch clean.
    Published officials for baseline + crash-mid-flush (idempotent; one publish
    hit a transient convex 503 OCC, retried).

    RESUME POINTER (fresh session): dispatcher row 5 → executor on
    **durable-ack-wal-head-contiguity** (status: ready) — the genuine-bug rung.
    It attacks the reopen frontier's HEAD-monotonicity assumption
    (tablestore.rs:177-179): install the driver's --head-false-negative wrapper
    (already built, head_fn.rs, overrides get_opts on options.head for one
    .../wal/{id:020}.sst) on the VERIFY/reopen open, sweep the target wal_id, and
    check A ⊆ R — a false-negative HEAD that truncates replay is a data-loss RED.
    Then producer re-entry, then the standing backlog (top: clone-consistency 400,
    compacted-gc-vs-reader 400 — both need Admin/clone + GC-loop wiring in the
    driver). Driver rebuild recipe + verdict-reading gotchas in
    runs/executor-notes.md. Nothing in flight; all state committed.
