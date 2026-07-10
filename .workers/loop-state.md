# Loop state
- rails: { loops: 100, workloads: 250 }   # defaults — safety rails, not targets
- counters: { episodes: 2, producer: 1, executor: 1, workloads: 1 }
- no-new-info: { streak: 0, K: 5 }
- in-flight unit: none
- re-entry: durable-ack-baseline → deepen — baseline green proves driver+oracle; next rung crash-mid-flush is the real bug hunt (SIGKILL mid-flush), already ready. No L change (first-rung green, weak evidence).
- last-scanned-sha: 016b676ee125f02cb14054cce0cd5a78f3524ac5
- target-head-sha: 016b676ee125f02cb14054cce0cd5a78f3524ac5
- re-plan triggers: none
- publish-pending: []
- last episode summary: |
    Executor #1 (durable-ack-baseline) — the driver-bootstrap unit. Built the
    bespoke slatedb-driver (musl static-pie, default-features=false, first-try),
    vendored + build.sh. durable_ack.py baseline with the universal oracle plane.
    In-guest: baseline GREEN (exploration nd79nxsh, 3/3 succeeded) and
    ORACLE_SELFTEST RED (run 01KX5YB5…, appears in --violations) — step-5
    red-proof gate satisfied. test-reviewer: KEEP. Published official
    durable-ack-baseline (nd773r0v…, depth 10, green replay-confirmation).
    Playbook written (runs/executor-notes.md): verdict-reading (workloads ls is
    account-wide; failed+INVARIANT-FAIL still counts as violation), official-run
    flow, driver rebuild. Next dispatcher row: 3 (re-entry pending
    durable-ack-baseline) → producer inline re-entry decision.
