# Loop state
- rails: { loops: 100, workloads: 250 }   # defaults — safety rails, not targets
- counters: { episodes: 7, producer: 1, executor: 6, workloads: 6 }
- no-new-info: { streak: 0, K: 5 }
- in-flight unit: none
- re-entry: durability-filter-remote-inflight-flush → switch — GREEN; durability-filter-remote ladder floor COMPLETE (3/3). Both batch-1 promises now fully covered. No ready entries remain (writebatch parked) → dispatcher routes to row 6 (producer episode: promote backlog top clone-consistency / compacted-gc). No L change (fault-boundary green, source-explained structural).
- last-scanned-sha: 016b676ee125f02cb14054cce0cd5a78f3524ac5
- target-head-sha: 016b676ee125f02cb14054cce0cd5a78f3524ac5
- re-plan triggers: none
- publish-pending: []   # all 3 durable-ack officials live (baseline nd7db1rk, crash-mid-flush nd72ajfy, wal-head-contiguity nd7epmpg)
- last episode summary: |
    Executor #6 (durability-filter-remote-inflight-flush) — GREEN. BlockWalPut
    wrapper holds the WAL SST PUT in-flight; Remote excludes the value until the
    PUT lands (during_block_hits=0, put_was_blocked=true in-guest). Source: the
    watermark advances strictly AFTER the PUT (wal_buffer.rs:322-337 → db.rs:2069
    → oracle.rs:60-105 → reader.rs:111-113), so this boundary is structurally
    green. **durability-filter-remote floor COMPLETE (3/3).** BOTH batch-1
    promises now covered to floor; 6 green officials live on the page.

    RESUME POINTER (fresh session): no ready entries left → dispatcher row 6 →
    **PRODUCER episode**: promote from backlog top. Top corridors:
    clone-consistency (400, [path: clone-external-sst]) and compacted-gc-vs-reader
    (400, [path: gc-vs-reader]) — BOTH need bigger driver extensions the current
    driver lacks: `Admin::create_clone`/`CloneBuilder` for clone, and a
    checkpoint + concurrent GC loop for gc-vs-reader. The producer should author
    those promises (baseline + adversarial + fault-boundary ladders) and their
    driver-op requirements, gate with strategy-critic, then executor builds the
    driver ops. Also un-attacked: fencing (split-brain, two-process), compaction
    (ownership race), ssi-write-skew, read-scan-mvcc-ttl, wal-fence-gc,
    retrying-store-writer-open. writebatch-atomicity is PARKED (certified
    below-floor). Full backlog in .workers/backlog.md (11 rows, threshold 20).
    Driver recipe + durprobe/remote-run/inflight-probe/block_put + verdict
    gotchas in runs/executor-notes.md. Nothing in flight; all state committed.
