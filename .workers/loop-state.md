# Loop state
- rails: { loops: 100, workloads: 250 }   # defaults — safety rails, not targets
- counters: { episodes: 4, producer: 1, executor: 3, workloads: 3 }
- no-new-info: { streak: 0, K: 5 }
- in-flight unit: none
- re-entry: durable-ack-wal-head-contiguity → switch — GREEN; durable-ack ladder floor COMPLETE (3/3, silent-truncation hypothesis refuted with source guards). Return to dispatcher; ready buffer (durability-filter-remote, 3 rungs) precedes the backlog. No L decay (3 greens on distinct harnesses, not same-corridor supersets). Added corridor retrying-store-writer-open.
- last-scanned-sha: 016b676ee125f02cb14054cce0cd5a78f3524ac5
- target-head-sha: 016b676ee125f02cb14054cce0cd5a78f3524ac5
- re-plan triggers: none
- publish-pending: [durable-ack-crash-mid-flush, durable-ack-wal-head-contiguity]   # transient convex 503 OCC (fleet-wide load 2026-07-10); re-fire publish.py — baseline official live (nd773r0v)
- last episode summary: |
    Executor #3 (durable-ack-wal-head-contiguity) — GREEN, the genuine-bug
    candidate. Silent-truncation hypothesis REFUTED (source-verified): a
    false-negative HEAD on a to-be-replayed WAL SST → LOUD reopen failure, never
    silent loss. Guards: PutMode::Create fence-barrier walk self-corrects a
    false-low frontier (fence.rs:143-172); replay read of the lied-about id fails
    loudly. Control (truthful) reopen proves the acked tail durable. Driver:
    verify open returns Result (no panic) + VERIFY_OPEN_FAILED line. Surfaced
    follow-up corridor retrying-store-writer-open (does #1909 wrap writer-open?).
    durable-ack promise now fully covered to floor (3/3 green).

    RESUME POINTER (fresh session): dispatcher row 5 → executor on
    **durability-filter-remote-baseline** (status: ready, oldest ready promise).
    Reuses slatedb-driver — EXTEND it with `read --durability {memory,remote}`
    and `put --no-await-durable` ops, then the workload checks R_remote ⊆
    survivors after crash + the Memory/Remote discrimination (non-vacuity
    control). Then its crash-confirm + inflight-flush rungs, then the backlog
    (top: clone-consistency 400, compacted-gc-vs-reader 400 — both need
    Admin::create_clone / a GC loop wired into the driver = a bigger build).
    Also pending: re-fire publish.py for the 2 pending officials once convex
    load eases. Driver rebuild recipe + verdict-reading gotchas in
    runs/executor-notes.md. Nothing in flight; all state committed.
