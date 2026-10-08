# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this repo is

mini-lsm is a course ("LSM in a Week") for building an LSM-tree key-value storage engine in Rust. It runs for 3 weeks with 7 chapters each. The book lives in `mini-lsm-book/src/` (`week{W}-0{D}-*.md`), and each chapter specifies exactly what to implement in that step.

Cargo workspace members:
- `mini-lsm-starter/`: the student crate. **All implementation work goes here.** Its functions start as `unimplemented!()`.
- `mini-lsm/`: reference solution for weeks 1–2. Do not modify it.
- `mini-lsm-mvcc/`: reference solution for week 3 (MVCC). Do not modify it.
- `xtask/`: dev commands, run through the `cargo x` alias (see `.cargo/config.toml`).

The starter crate is fully implemented through week 3 day 7 (MVCC final state: timestamped keys, transactions, serializable validation, compaction filters), and all 20 days of tests are copied into `src/tests/`. Read the matching book chapter before changing a component. The reference crates can serve as an oracle when a test's expectations are unclear.

Starter-specific choices to keep consistent:
- Checksums use the `crc32fast` crate, which was added to the starter's `Cargo.toml`.
- Block entries use prefix compression and include the timestamp. SST block meta also stores `max_ts`. The open path recovers the commit timestamp from SSTs and WALs.
- `get_with_ts` does point lookups newest-first: memtables, then L0, then each level, using the bloom filter and key range checks. Scans go through `scan_with_ts` → `LsmIterator`, which picks the latest version `<= read_ts`.

## Commands

```bash
cargo x copy-test --week 1 --day 1   # copy that day's tests (+ harness.rs) into mini-lsm-starter/src/tests/
cargo x scheck                       # starter crate: fmt + check --all-targets + nextest + clippy
cargo nextest run -p mini-lsm-starter                       # starter tests only
cargo nextest run -p mini-lsm-starter week1_day2            # one day's tests (substring filter)
cargo nextest run -p mini-lsm-starter test_task1_memtable_get  # a single test
cargo clippy -p mini-lsm-starter --all-targets
cargo run --bin mini-lsm-cli         # starter CLI; *-ref / *-mvcc-ref binaries run the reference solutions
cargo run --bin compaction-simulator # exercise compaction controllers (week 2)
cargo x check                        # whole workspace (reference crates), for course developers
cargo x book                         # serve the book (needs mdbook)
```

- `cargo x copy-test` regenerates `mini-lsm-starter/src/tests.rs` from whatever files are in `src/tests/`. That is why the file loses its license header, which is expected. Don't hand-edit it. Copy tests for each day **cumulatively** so earlier days keep running as regression tests.
- Tests for weeks 1–2 come from `mini-lsm/src/tests`, and tests for week 3 come from `mini-lsm-mvcc/src/tests`.
- nextest config: a test is marked slow after 10s and killed after 30s (`.config/nextest.toml`). A hang usually means a deadlock or an iterator that never advances.
- Toolchain: stable, edition 2024.

## Architecture (starter crate)

**Keys.** `key.rs` defines `Key<T>` with the aliases `KeySlice`, `KeyVec`, and `KeyBytes`. In weeks 1–2 a key is only bytes (`TS_ENABLED = false`). Week 3 day 1 changes this to `Key<T>(T, u64)` with a timestamp, sorted by key ascending and then ts **descending** (`mini-lsm-mvcc/src/key.rs` is the target shape). Every encoder and decoder (block, SST, memtable, WAL) has to change at that point.

**Storage state and concurrency** (`lsm_storage.rs`):
- `LsmStorageState` is an immutable snapshot with these fields: the active `memtable`, `imm_memtables` (newest first), `l0_sstables` (newest first), `levels: Vec<(level_id, Vec<sst_id>)>`, and an `sstables` map from id to `Arc<SsTable>`.
- `LsmStorageInner.state` is a `RwLock<Arc<LsmStorageState>>`. Readers take the read lock, clone the `Arc`, and release the lock right away. Writers that change structure (freeze, flush, compaction) first take `state_lock: Mutex<()>`, then clone the state, modify the clone, and swap it in under a brief write lock. `force_freeze_memtable` receives the `state_lock` guard to prove the caller holds it.
- `MiniLsm` wraps `Arc<LsmStorageInner>` and owns the background flush and compaction threads (`compact.rs`: `spawn_compaction_thread`, `spawn_flush_thread`). `close()` must stop those threads and, from week 2 on, flush or sync.

**Read path.** Memtables and SSTs each produce a `StorageIterator` (`iterators.rs`, a non-standard `key/value/is_valid/next` trait). These are combined as follows:
- `MergeIterator` is a heap over iterators of the same type, where ties go to the lower index, i.e. the newer source.
- `TwoMergeIterator<A, B>` merges two iterators of different types, and A wins ties.
- `SstConcatIterator` handles sorted, non-overlapping levels.
- `LsmIterator` (`lsm_iterator.rs`) skips tombstones (empty values) and enforces the upper bound. `FusedIterator` stops calling the inner iterator once it has become invalid or returned an error.

**Write path.** A put goes to the memtable (`crossbeam_skiplist::SkipMap`, plus the WAL from week 2 day 6). Once the memtable passes `target_sst_size` it is frozen into `imm_memtables`. The flush thread turns the oldest immutable memtable into an L0 SST. A delete is a put with an empty value.

**On-disk formats.** These are defined by the book chapters, and the tests check behavior rather than bytes.
- Block (`block/`): entries, then u16 offsets, then a u16 count. Prefix key compression is added in 1.7.
- SST (`table/`): blocks, then block meta, then the meta offset, then a bloom filter (1.7).
- Block cache: `moka`.
- `manifest.rs` (2.5) and `wal.rs` (2.6) are append-only record logs, and checksums are added in 2.7.

**Compaction** (`compact.rs` + `compact/{simple_leveled,tiered,leveled}.rs`). Each controller is a pure planner: `generate_compaction_task(&state)` decides the work, and `apply_compaction_result(&state, task, output)` returns the new state plus the files to delete. Because the controllers are pure, `compaction-simulator` can drive them without real I/O. `LsmStorageInner` does the actual merge-and-write and installs the result.

**MVCC (week 3).** `mvcc.rs`, `mvcc/txn.rs`, and `mvcc/watermark.rs` cover these pieces:
- timestamp allocation
- `Transaction` (snapshot read with a local write set, and OCC with serializable validation in 3.5–3.6)
- a watermark for GC during compaction
- compaction filters (3.7)

## Environment notes

This checkout is on WSL with `core.filemode=true`, so `git status` shows hundreds of files that changed only in mode (644→755). Ignore those, or ask the user before running `git config core.filemode false`.
