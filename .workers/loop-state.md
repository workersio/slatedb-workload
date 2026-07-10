# Loop state
- rails: { loops: 100, workloads: 250 }   # defaults — safety rails, not targets
- counters: { episodes: 10, producer: 2, executor: 8, workloads: 8 }
- no-new-info: { streak: 0, K: 5 }
- in-flight unit: none
- re-entry: none
- last-scanned-sha: 016b676ee125f02cb14054cce0cd5a78f3524ac5
- target-head-sha: 016b676ee125f02cb14054cce0cd5a78f3524ac5
- re-plan triggers: executor-bounce-back — fencing-split-brain-overlap-writes BLOCKED (zombie window structurally unreachable, fence airtight; anti-vacuity VOID on-box+in-guest). PRODUCER TRIAGE: retire the rung as certified-unreachable OR fold into stale-epoch-flush (rung 3, the in-flight-buffered-flush variant). Cite fence.rs:145-171 + tablestore.rs:1125,1133.
- publish-pending: [durability-filter-remote-inflight-flush, fencing-split-brain-baseline]   # convex OCC under fleet load; DEFER publishing — do NOT call publish.py per-episode (it re-runs ALL done explorations each time = wasteful churn + repeated OCC). Publish ONCE at a genuine wrap-up when convex is calm, or after teaching publish.py to skip already-published-at-HEAD. 6 officials already live.
- last episode summary: |
    Executor #8 (fencing-split-brain-overlap-writes) — BLOCKED (anti-vacuity VOID,
    honest). The zombie/split-brain corridor is structurally UNREACHABLE: the
    fence is airtight — no durable victim write lands after the usurper's epoch
    bump (post_fence_suspects=0 across 49 on-box + 10 in-guest seeds). Source:
    fence.rs:145-171 advances the WAL barrier past incumbent ids → next victim
    flush Fenced (tablestore.rs:1125,1133 PutMode::Create). Oracle non-vacuous
    (selftest RED). This is a STRONGER guarantee than the promise asked — SlateDB
    prevents post-fence writes from being acked at all. Evidence:
    runs/fencing-split-brain-overlap-writes.md. Re-plan trigger set.

    RESUME POINTER (fresh session): dispatcher row 4 (re-plan trigger) → PRODUCER
    TRIAGE episode: decide overlap-writes — retire as certified-unreachable
    (like the clone pin + wal-head-contiguity refutations) OR fold into
    stale-epoch-flush (rung 3), which tests the ONE remaining reachable variant:
    a victim flush already IN FLIGHT (WAL PUT started) when the barrier lands —
    does it complete durably? Then clear the trigger; row 5 → executor on
    stale-epoch-flush + compacted-gc (2 rungs ready). Note: the fencing baseline
    green stands; only the overlap rung is unreachable-as-framed.

    Executor #7 (fencing-split-brain-baseline) — GREEN. Two-process fencing
    (fence-victim/fence-usurper on one LocalFileSystem root); superseded put →
    ErrorKind::Closed(CloseReason::Fenced) (error.rs:115,618; fence.rs:342-354).
    Victim fenced after usurper open; usurper durable; selftest red.
    **LEAD for the next rung:** in-guest the victim landed ONE ok await_durable
    write AFTER the usurper opened before being fenced — overlap-writes must
    check if that post-fence write is a durable zombie (potential split-brain
    RED). Evidence: runs/fencing-split-brain-baseline.md.

    RESUME POINTER (fresh session): dispatcher row 5 → executor on
    **fencing-split-brain-overlap-writes** (status: ready) — the adversarial rung
    with a CONCRETE hypothesis: A and B write overlapping keys across the B-open
    fence; reopen and assert the final durable state is a valid single-writer
    history — any surviving post-fence victim value = split-brain/lost-update RED.
    The baseline proved the post-open ok-write window exists (victim_ok_after_
    prelude=1); this rung falsifies whether it persists durably. Reuses
    fence-victim/fence-usurper; add the single-writer-history verify. Then
    stale-epoch-flush, then compacted-gc (2 rungs ready), then finish clone.
    Driver subcommands + gotchas in runs/executor-notes.md. Nothing in flight.

    Producer #2 (backlog promotion, strategy-critic-gated). Promoted 3 corridors;
    the critic (source-verified) REDO caught two structural traps and I applied
    all: (1) clone-consistency invariant-3 (referential FileNotFound) is PINNED —
    single-source clones write a lifetime:None parent checkpoint (clone.rs:197-212)
    that compacted GC excludes (compacted_gc.rs:224); reframed rung 3 to the
    clone-CREATION crash window (ephemeral 300s pin gap), demoted invariant-4
    (#1851, fixed) to a regression guard, kept clone PLANNED (lowest-value,
    deferred). (2) compacted-gc #319 is genuinely open but ONLY reachable via a
    long-lived scan ITERATOR held across compaction→reestablish→GC — needs an
    armed-fault witness or it's a vacuous green; certified 2 rungs (dropped the
    near-vacuous crash rung). (3) Counter-promoted writer-fencing-split-brain
    (cheapest reachable red, reuses the crash driver + a 2nd process) to the FRONT.
    READY buffer (5 explorations): fencing-split-brain-{baseline,overlap-writes,
    stale-epoch-flush} + compacted-gc-{baseline,reader-outlives-checkpoint}.
    Backlog now 8 active (top reader-checkpoint-reestablish 288, which OVERLAPS
    compacted-gc rung 2 — shared #1900 harness).

    RESUME POINTER (fresh session): dispatcher row 5 → executor on
    **fencing-split-brain-baseline** FIRST (cheapest reachable red). Driver ext:
    `fence-victim` (open, write, then attempt writes and report FENCE_OBSERVED
    result=ok|fenced) + `fence-usurper` (open same root, fences victim) — two
    processes on one LocalFileSystem root; VERIFY SlateDBError::Fenced vs error.rs
    + fence.rs. Then its 2 adversarial rungs, then compacted-gc (needs the
    long-iterator + compactor + GC(low min_age) harness + armed-fault witness).
    Then clone-consistency (finish the reframe → ready). Backlog corridors after:
    ssi-write-skew (267), compactor-ownership-race (256), snapshot-pin (256),
    wal-fence-gc (240), read-scan-mvcc-ttl (192). Driver recipe + all subcommands
    (run/verify/durprobe/remote-run/inflight-probe/block_put) in
    runs/executor-notes.md. Nothing in flight; all state committed.

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
