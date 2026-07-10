# Loop state
- rails: { loops: 100, workloads: 250 }   # defaults — safety rails, not targets
- counters: { episodes: 1, producer: 1, executor: 0, workloads: 0 }
- no-new-info: { streak: 0, K: 5 }
- in-flight unit: none
- re-entry: none
- last-scanned-sha: 016b676ee125f02cb14054cce0cd5a78f3524ac5
- target-head-sha: 016b676ee125f02cb14054cce0cd5a78f3524ac5
- re-plan triggers: none
- publish-pending: []
- last episode summary: |
    Producer #1 (cartographer fan-out + first batch). 5 foreground scouts (docs,
    tests, commits, runtime, api) — all source-cited, strongly convergent.
    Created 6 areas (durability, consistency, fencing, clone, gc, compaction),
    backlog seeded (10 active, top-score 400), mapping-breadth floor reconciled
    (all modules assigned or parked). Strategy-critic gated the batch (REDO →
    applied all 6 required changes): reframed durable-ack rung 3 from a
    guaranteed-green drop-PUT to the HEAD-contiguity frontier attack; parked
    writebatch-atomicity (certified below-floor, vacuous crash-mid-batch);
    counter-promoted durability-filter-remote into slot 2 (breaks all-durability
    anchoring); reframed the wal-replay backlog row to retry/double-apply +
    lowered score; added read-scan-mvcc-ttl missing seam; pinned the driver build
    to musl-static default-features=false with mandatory per-ack fsync ack-log.
    READY batch (6 explorations across 2 promises):
      durable-ack-{baseline,crash-mid-flush,wal-head-contiguity},
      durability-filter-remote-{baseline,crash-confirm,inflight-flush}.
    CRITICAL PATH: no SUT driver binary exists yet — the first executor episode
    (durable-ack-baseline) must build the bespoke slatedb-driver (see
    promises/durable-ack-survives-crash.md §Workload plan + map.md §SUT driver)
    before any run. Next dispatcher row: 5 (ready entries) → executor on
    durable-ack-baseline (oldest promise, baseline first).
