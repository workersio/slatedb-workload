---
key: gc
title: Garbage Collection
description: GC never deletes an object still referenced by the live manifest, a checkpoint, a clone, or an in-flight reader.
order: 50
---
# Garbage Collection

The GC (`garbage_collector.rs`, defaults interval 60s / min_age 300s,
`config.rs:55-56`) reclaims compacted SSTs and WAL objects no longer
referenced. Two documented danger windows:

- **Compacted GC vs live reader** — authors explicitly *disable* compacted GC
  in their flagship consistency test (`slatedb-dst/tests/bank.rs:194` "Disable
  `compacted` GC until #319 is done") and only have an isolated in-process unit
  repro (`compacted_gc.rs:599`). A reader/scan that outlives its checkpoint can
  read an SST that GC deleted → `FileNotFound` surfaced to a read (data-loss).
- **WAL fence GC (#352)** — `wal_gc.rs:141-146` "WAL fence GC is dry-run by
  default"; `config.rs:1391-1408` carries an explicit data-loss warning about
  the fence-position deletion race. `bank.rs:76-85` deliberately skips WAL
  bandwidth toxics because slow WAL replay makes reader checkpoints expire →
  "checkpoint missing" — a fragility they route around rather than test.

**Invariant to falsify:** GC never deletes an object still pinned by the live
manifest, a checkpoint, a clone, or an in-flight reader; no read ever surfaces
`FileNotFound`. Our axis: run GC *enabled* (the config the authors avoid) with a
real GC loop + slow readers + aggressive compaction under whole-process faults.
