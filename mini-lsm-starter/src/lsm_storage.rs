// Copyright (c) 2022-2025 Alex Chi Z
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;

use anyhow::{Context, Result};
use bytes::Bytes;
use parking_lot::{Mutex, MutexGuard, RwLock};

use crate::block::Block;
use crate::compact::{
    CompactionController, CompactionOptions, CompactionTask, LeveledCompactionController,
    LeveledCompactionOptions, SimpleLeveledCompactionController, SimpleLeveledCompactionOptions,
    TieredCompactionController,
};
use crate::iterators::StorageIterator;
use crate::iterators::concat_iterator::SstConcatIterator;
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::key::{KeySlice, TS_RANGE_BEGIN};
use crate::lsm_iterator::{FusedIterator, LsmIterator};
use crate::manifest::{Manifest, ManifestRecord};
use crate::mem_table::{
    MemTable, map_bound, map_lower_bound_all_versions, map_upper_bound_all_versions,
};
use crate::mvcc::LsmMvccInner;
use crate::mvcc::txn::{Transaction, TxnIterator};
use crate::table::{FileObject, SsTable, SsTableBuilder, SsTableIterator};

pub type BlockCache = moka::sync::Cache<(usize, usize), Arc<Block>>;

/// Represents the state of the storage engine.
#[derive(Clone)]
pub struct LsmStorageState {
    /// The current memtable.
    pub memtable: Arc<MemTable>,
    /// Immutable memtables, from latest to earliest.
    pub imm_memtables: Vec<Arc<MemTable>>,
    /// L0 SSTs, from latest to earliest.
    pub l0_sstables: Vec<usize>,
    /// SsTables sorted by key range; L1 - L_max for leveled compaction, or tiers for tiered
    /// compaction.
    pub levels: Vec<(usize, Vec<usize>)>,
    /// SST objects.
    pub sstables: HashMap<usize, Arc<SsTable>>,
}

pub enum WriteBatchRecord<T: AsRef<[u8]>> {
    Put(T, T),
    Del(T),
}

impl LsmStorageState {
    fn create(options: &LsmStorageOptions) -> Self {
        let levels = match &options.compaction_options {
            CompactionOptions::Leveled(LeveledCompactionOptions { max_levels, .. })
            | CompactionOptions::Simple(SimpleLeveledCompactionOptions { max_levels, .. }) => (1
                ..=*max_levels)
                .map(|level| (level, Vec::new()))
                .collect::<Vec<_>>(),
            CompactionOptions::Tiered(_) => Vec::new(),
            CompactionOptions::NoCompaction => vec![(1, Vec::new())],
        };
        Self {
            memtable: Arc::new(MemTable::create(0)),
            imm_memtables: Vec::new(),
            l0_sstables: Vec::new(),
            levels,
            sstables: Default::default(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct LsmStorageOptions {
    // Block size in bytes
    pub block_size: usize,
    // SST size in bytes, also the approximate memtable capacity limit
    pub target_sst_size: usize,
    // Maximum number of memtables in memory, flush to L0 when exceeding this limit
    pub num_memtable_limit: usize,
    pub compaction_options: CompactionOptions,
    pub enable_wal: bool,
    pub serializable: bool,
}

impl LsmStorageOptions {
    pub fn default_for_week1_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 50,
            serializable: false,
        }
    }

    pub fn default_for_week1_day6_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
        }
    }

    pub fn default_for_week2_test(compaction_options: CompactionOptions) -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 1 << 20, // 1MB
            compaction_options,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
        }
    }
}

#[derive(Clone, Debug)]
pub enum CompactionFilter {
    Prefix(Bytes),
}

/// The storage interface of the LSM tree.
pub(crate) struct LsmStorageInner {
    pub(crate) state: Arc<RwLock<Arc<LsmStorageState>>>,
    pub(crate) state_lock: Mutex<()>,
    path: PathBuf,
    pub(crate) block_cache: Arc<BlockCache>,
    next_sst_id: AtomicUsize,
    pub(crate) options: Arc<LsmStorageOptions>,
    pub(crate) compaction_controller: CompactionController,
    pub(crate) manifest: Option<Manifest>,
    pub(crate) mvcc: Option<LsmMvccInner>,
    pub(crate) compaction_filters: Arc<Mutex<Vec<CompactionFilter>>>,
}

/// A thin wrapper for `LsmStorageInner` and the user interface for MiniLSM.
pub struct MiniLsm {
    pub(crate) inner: Arc<LsmStorageInner>,
    /// Notifies the L0 flush thread to stop working. (In week 1 day 6)
    flush_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the flush thread. (In week 1 day 6)
    flush_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
    /// Notifies the compaction thread to stop working. (In week 2)
    compaction_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the compaction thread. (In week 2)
    compaction_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl Drop for MiniLsm {
    fn drop(&mut self) {
        self.compaction_notifier.send(()).ok();
        self.flush_notifier.send(()).ok();
    }
}

impl MiniLsm {
    pub fn close(&self) -> Result<()> {
        self.flush_notifier.send(()).ok();
        self.compaction_notifier.send(()).ok();

        let mut flush_thread = self.flush_thread.lock();
        if let Some(flush_thread) = flush_thread.take() {
            flush_thread
                .join()
                .map_err(|e| anyhow::anyhow!("{:?}", e))?;
        }
        let mut compaction_thread = self.compaction_thread.lock();
        if let Some(compaction_thread) = compaction_thread.take() {
            compaction_thread
                .join()
                .map_err(|e| anyhow::anyhow!("{:?}", e))?;
        }

        if self.inner.options.enable_wal {
            // With WAL, it is enough to persist the WAL of the memtables.
            self.inner.sync()?;
            self.inner.sync_dir()?;
            return Ok(());
        }

        // Without WAL, the in-memory data must be flushed to disk before closing.
        if !self.inner.state.read().memtable.is_empty() {
            self.inner
                .force_freeze_memtable(&self.inner.state_lock.lock())?;
        }
        while !self.inner.state.read().imm_memtables.is_empty() {
            self.inner.force_flush_next_imm_memtable()?;
        }
        self.inner.sync_dir()?;

        Ok(())
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Arc<Self>> {
        let inner = Arc::new(LsmStorageInner::open(path, options)?);
        let (tx1, rx) = crossbeam_channel::unbounded();
        let compaction_thread = inner.spawn_compaction_thread(rx)?;
        let (tx2, rx) = crossbeam_channel::unbounded();
        let flush_thread = inner.spawn_flush_thread(rx)?;
        Ok(Arc::new(Self {
            inner,
            flush_notifier: tx2,
            flush_thread: Mutex::new(flush_thread),
            compaction_notifier: tx1,
            compaction_thread: Mutex::new(compaction_thread),
        }))
    }

    pub fn new_txn(&self) -> Result<Arc<Transaction>> {
        self.inner.new_txn()
    }

    pub fn write_batch<T: AsRef<[u8]>>(&self, batch: &[WriteBatchRecord<T>]) -> Result<()> {
        self.inner.write_batch(batch)
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        self.inner.add_compaction_filter(compaction_filter)
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.inner.get(key)
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.inner.put(key, value)
    }

    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.inner.delete(key)
    }

    pub fn sync(&self) -> Result<()> {
        self.inner.sync()
    }

    pub fn scan(&self, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> Result<TxnIterator> {
        self.inner.scan(lower, upper)
    }

    /// Only call this in test cases due to race conditions
    pub fn force_flush(&self) -> Result<()> {
        if !self.inner.state.read().memtable.is_empty() {
            self.inner
                .force_freeze_memtable(&self.inner.state_lock.lock())?;
        }
        if !self.inner.state.read().imm_memtables.is_empty() {
            self.inner.force_flush_next_imm_memtable()?;
        }
        Ok(())
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        self.inner.force_full_compaction()
    }
}

impl LsmStorageInner {
    pub(crate) fn next_sst_id(&self) -> usize {
        self.next_sst_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    pub(crate) fn mvcc(&self) -> &LsmMvccInner {
        self.mvcc.as_ref().unwrap()
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub(crate) fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Self> {
        let path = path.as_ref();
        let mut state = LsmStorageState::create(&options);
        let block_cache = Arc::new(BlockCache::new(1 << 20)); // 4GB block cache

        let compaction_controller = match &options.compaction_options {
            CompactionOptions::Leveled(options) => {
                CompactionController::Leveled(LeveledCompactionController::new(options.clone()))
            }
            CompactionOptions::Tiered(options) => {
                CompactionController::Tiered(TieredCompactionController::new(options.clone()))
            }
            CompactionOptions::Simple(options) => CompactionController::Simple(
                SimpleLeveledCompactionController::new(options.clone()),
            ),
            CompactionOptions::NoCompaction => CompactionController::NoCompaction,
        };

        if !path.exists() {
            std::fs::create_dir_all(path).context("failed to create DB dir")?;
        }

        let manifest_path = path.join("MANIFEST");
        let mut next_sst_id = 1;
        // The largest commit timestamp found on disk; new writes must use larger timestamps.
        let mut last_commit_ts = 0;
        let manifest = if !manifest_path.exists() {
            if options.enable_wal {
                state.memtable = Arc::new(MemTable::create_with_wal(
                    state.memtable.id(),
                    Self::path_of_wal_static(path, state.memtable.id()),
                )?);
            }
            let manifest = Manifest::create(&manifest_path)?;
            manifest.add_record_when_init(ManifestRecord::NewMemtable(state.memtable.id()))?;
            manifest
        } else {
            let (manifest, records) = Manifest::recover(&manifest_path)?;

            // Replay the manifest to rebuild the LSM structure.
            let mut memtables = BTreeSet::new();
            for record in records {
                match record {
                    ManifestRecord::Flush(sst_id) => {
                        let res = memtables.remove(&sst_id);
                        assert!(res, "memtable not exist?");
                        if compaction_controller.flush_to_l0() {
                            state.l0_sstables.insert(0, sst_id);
                        } else {
                            state.levels.insert(0, (sst_id, vec![sst_id]));
                        }
                        next_sst_id = next_sst_id.max(sst_id);
                    }
                    ManifestRecord::NewMemtable(memtable_id) => {
                        next_sst_id = next_sst_id.max(memtable_id);
                        memtables.insert(memtable_id);
                    }
                    ManifestRecord::Compaction(task, output) => {
                        state = apply_compaction_result_for_recovery(
                            &compaction_controller,
                            &state,
                            &task,
                            &output,
                        );
                        next_sst_id =
                            next_sst_id.max(output.iter().max().copied().unwrap_or_default());
                    }
                }
            }

            // Open all SSTs referenced by the recovered structure.
            for table_id in state
                .l0_sstables
                .iter()
                .chain(state.levels.iter().flat_map(|(_, files)| files))
            {
                let table_id = *table_id;
                let sst = SsTable::open(
                    table_id,
                    Some(block_cache.clone()),
                    FileObject::open(&Self::path_of_sst_static(path, table_id))
                        .with_context(|| format!("failed to open SST: {}", table_id))?,
                )?;
                last_commit_ts = last_commit_ts.max(sst.max_ts());
                state.sstables.insert(table_id, Arc::new(sst));
            }

            // `apply_compaction_result` does not sort SSTs during recovery, so do it now.
            if let CompactionController::Leveled(_) = &compaction_controller {
                for (_, ssts) in &mut state.levels {
                    ssts.sort_by(|x, y| {
                        state.sstables[x]
                            .first_key()
                            .cmp(state.sstables[y].first_key())
                    });
                }
            }

            next_sst_id += 1;

            // Recover memtables from WALs; memtables that were not flushed become immutable.
            if options.enable_wal {
                for id in memtables.iter() {
                    let memtable =
                        MemTable::recover_from_wal(*id, Self::path_of_wal_static(path, *id))?;
                    if !memtable.is_empty() {
                        last_commit_ts = last_commit_ts.max(memtable.max_ts());
                        state.imm_memtables.insert(0, Arc::new(memtable));
                    }
                }
                state.memtable = Arc::new(MemTable::create_with_wal(
                    next_sst_id,
                    Self::path_of_wal_static(path, next_sst_id),
                )?);
            } else {
                state.memtable = Arc::new(MemTable::create(next_sst_id));
            }
            manifest.add_record_when_init(ManifestRecord::NewMemtable(state.memtable.id()))?;
            next_sst_id += 1;
            manifest
        };

        let storage = Self {
            state: Arc::new(RwLock::new(Arc::new(state))),
            state_lock: Mutex::new(()),
            path: path.to_path_buf(),
            block_cache,
            next_sst_id: AtomicUsize::new(next_sst_id),
            compaction_controller,
            manifest: Some(manifest),
            options: options.into(),
            mvcc: Some(LsmMvccInner::new(last_commit_ts)),
            compaction_filters: Arc::new(Mutex::new(Vec::new())),
        };
        storage.sync_dir()?;

        Ok(storage)
    }

    pub fn sync(&self) -> Result<()> {
        self.state.read().memtable.sync_wal()
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        let mut compaction_filters = self.compaction_filters.lock();
        compaction_filters.push(compaction_filter);
    }

    /// Get a key from the storage. In day 7, this can be further optimized by using a bloom filter.
    pub fn get(self: &Arc<Self>, key: &[u8]) -> Result<Option<Bytes>> {
        let txn = self.new_txn()?;
        txn.get(key)
    }

    /// Get the latest version of a key visible at `read_ts`.
    pub(crate) fn get_with_ts(&self, key: &[u8], read_ts: u64) -> Result<Option<Bytes>> {
        let snapshot = {
            let guard = self.state.read();
            Arc::clone(&guard)
        };

        // Search memtables from the newest to the oldest.
        if let Some(value) = snapshot.memtable.get_with_ts(key, read_ts) {
            return Ok(Some(value).filter(|v| !v.is_empty()));
        }
        for memtable in &snapshot.imm_memtables {
            if let Some(value) = memtable.get_with_ts(key, read_ts) {
                return Ok(Some(value).filter(|v| !v.is_empty()));
            }
        }

        // Then search SSTs, also from the newest to the oldest: L0 first, then each level. A newer
        // SST always holds newer versions of a key than an older SST.
        let candidates = snapshot.l0_sstables.iter().chain(
            snapshot
                .levels
                .iter()
                .flat_map(|(_, level_sst_ids)| level_sst_ids.iter()),
        );
        for table_id in candidates {
            let table = snapshot.sstables[table_id].clone();
            if !table_may_contain_key(&table, key) {
                continue;
            }
            let iter =
                SsTableIterator::create_and_seek_to_key(table, KeySlice::from_slice(key, read_ts))?;
            if iter.is_valid() && iter.key().key_ref() == key {
                return Ok(Some(Bytes::copy_from_slice(iter.value())).filter(|v| !v.is_empty()));
            }
        }

        Ok(None)
    }

    /// Write a batch of data into the storage. Implement in week 2 day 7.
    pub fn write_batch<T: AsRef<[u8]>>(
        self: &Arc<Self>,
        batch: &[WriteBatchRecord<T>],
    ) -> Result<()> {
        if !self.options.serializable {
            self.write_batch_inner(batch)?;
        } else {
            // Go through a transaction so that the write participates in conflict detection.
            let txn = self.new_txn()?;
            for record in batch {
                match record {
                    WriteBatchRecord::Del(key) => txn.delete(key.as_ref()),
                    WriteBatchRecord::Put(key, value) => txn.put(key.as_ref(), value.as_ref()),
                }
            }
            txn.commit()?;
        }
        Ok(())
    }

    /// Write a batch of data atomically with a new commit timestamp, returning the timestamp.
    pub fn write_batch_inner<T: AsRef<[u8]>>(&self, batch: &[WriteBatchRecord<T>]) -> Result<u64> {
        let _write_lock = self.mvcc().write_lock.lock();
        let ts = self.mvcc().latest_commit_ts() + 1;
        let data = batch
            .iter()
            .map(|record| match record {
                WriteBatchRecord::Put(key, value) => {
                    let (key, value) = (key.as_ref(), value.as_ref());
                    assert!(!key.is_empty(), "key cannot be empty");
                    assert!(!value.is_empty(), "value cannot be empty");
                    (KeySlice::from_slice(key, ts), value)
                }
                WriteBatchRecord::Del(key) => {
                    let key = key.as_ref();
                    assert!(!key.is_empty(), "key cannot be empty");
                    (KeySlice::from_slice(key, ts), &b""[..])
                }
            })
            .collect::<Vec<_>>();
        let size = {
            let guard = self.state.read();
            guard.memtable.put_batch(&data)?;
            guard.memtable.approximate_size()
        };
        self.try_freeze(size)?;
        self.mvcc().update_commit_ts(ts);
        Ok(ts)
    }

    /// Put a key-value pair into the storage by writing into the current memtable.
    pub fn put(self: &Arc<Self>, key: &[u8], value: &[u8]) -> Result<()> {
        self.write_batch(&[WriteBatchRecord::Put(key, value)])
    }

    /// Remove a key from the storage by writing an empty value.
    pub fn delete(self: &Arc<Self>, key: &[u8]) -> Result<()> {
        self.write_batch(&[WriteBatchRecord::Del(key)])
    }

    /// Freeze the current memtable if it has grown beyond the target SST size.
    fn try_freeze(&self, estimated_size: usize) -> Result<()> {
        if estimated_size >= self.options.target_sst_size {
            let state_lock = self.state_lock.lock();
            // Another thread may have frozen the memtable while we were waiting for the lock.
            if self.state.read().memtable.approximate_size() >= self.options.target_sst_size {
                self.force_freeze_memtable(&state_lock)?;
            }
        }
        Ok(())
    }

    pub(crate) fn path_of_sst_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.sst", id))
    }

    pub(crate) fn path_of_sst(&self, id: usize) -> PathBuf {
        Self::path_of_sst_static(&self.path, id)
    }

    pub(crate) fn path_of_wal_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.wal", id))
    }

    pub(crate) fn path_of_wal(&self, id: usize) -> PathBuf {
        Self::path_of_wal_static(&self.path, id)
    }

    pub(super) fn sync_dir(&self) -> Result<()> {
        std::fs::File::open(&self.path)?.sync_all()?;
        Ok(())
    }

    /// Force freeze the current memtable to an immutable memtable
    pub fn force_freeze_memtable(&self, state_lock_observer: &MutexGuard<'_, ()>) -> Result<()> {
        let memtable_id = self.next_sst_id();
        let memtable = if self.options.enable_wal {
            Arc::new(MemTable::create_with_wal(
                memtable_id,
                self.path_of_wal(memtable_id),
            )?)
        } else {
            Arc::new(MemTable::create(memtable_id))
        };

        self.freeze_memtable_with_memtable(memtable)?;

        if let Some(manifest) = &self.manifest {
            manifest.add_record(
                state_lock_observer,
                ManifestRecord::NewMemtable(memtable_id),
            )?;
        }
        self.sync_dir()?;

        Ok(())
    }

    fn freeze_memtable_with_memtable(&self, memtable: Arc<MemTable>) -> Result<()> {
        let old_memtable = {
            let mut guard = self.state.write();
            let mut snapshot = guard.as_ref().clone();
            let old_memtable = std::mem::replace(&mut snapshot.memtable, memtable);
            snapshot.imm_memtables.insert(0, old_memtable.clone());
            *guard = Arc::new(snapshot);
            old_memtable
        };
        // Make sure everything in the frozen memtable is persisted in its WAL.
        old_memtable.sync_wal()?;
        Ok(())
    }

    /// Force flush the earliest-created immutable memtable to disk
    pub fn force_flush_next_imm_memtable(&self) -> Result<()> {
        let state_lock = self.state_lock.lock();

        let memtable_to_flush = {
            let guard = self.state.read();
            guard
                .imm_memtables
                .last()
                .expect("no imm memtables!")
                .clone()
        };

        // Build the SST without holding the state read/write lock.
        let mut builder = SsTableBuilder::new(self.options.block_size);
        memtable_to_flush.flush(&mut builder)?;
        let sst_id = memtable_to_flush.id();
        let sst = Arc::new(builder.build(
            sst_id,
            Some(self.block_cache.clone()),
            self.path_of_sst(sst_id),
        )?);

        {
            let mut guard = self.state.write();
            let mut snapshot = guard.as_ref().clone();
            let mem = snapshot.imm_memtables.pop().unwrap();
            assert_eq!(mem.id(), sst_id);
            if self.compaction_controller.flush_to_l0() {
                // In leveled compaction or no compaction, simply flush to L0
                snapshot.l0_sstables.insert(0, sst_id);
            } else {
                // In tiered compaction, create a new tier
                snapshot.levels.insert(0, (sst_id, vec![sst_id]));
            }
            snapshot.sstables.insert(sst_id, sst);
            *guard = Arc::new(snapshot);
        }

        if self.options.enable_wal {
            std::fs::remove_file(self.path_of_wal(sst_id))?;
        }

        if let Some(manifest) = &self.manifest {
            manifest.add_record(&state_lock, ManifestRecord::Flush(sst_id))?;
        }

        self.sync_dir()?;

        Ok(())
    }

    pub fn new_txn(self: &Arc<Self>) -> Result<Arc<Transaction>> {
        Ok(self.mvcc().new_txn(self.clone(), self.options.serializable))
    }

    /// Create an iterator over a range of keys.
    pub fn scan(self: &Arc<Self>, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> Result<TxnIterator> {
        let txn = self.new_txn()?;
        txn.scan(lower, upper)
    }

    /// Create an iterator over a range of keys that sees the versions visible at `read_ts`.
    pub(crate) fn scan_with_ts(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
        read_ts: u64,
    ) -> Result<FusedIterator<LsmIterator>> {
        let snapshot = {
            let guard = self.state.read();
            Arc::clone(&guard)
        };

        let (key_lower, key_upper) = (
            map_lower_bound_all_versions(lower),
            map_upper_bound_all_versions(upper),
        );
        let mut memtable_iters = Vec::with_capacity(snapshot.imm_memtables.len() + 1);
        memtable_iters.push(Box::new(snapshot.memtable.scan(key_lower, key_upper)));
        for memtable in &snapshot.imm_memtables {
            memtable_iters.push(Box::new(memtable.scan(key_lower, key_upper)));
        }
        let memtable_iter = MergeIterator::create(memtable_iters);

        let mut l0_iters = Vec::with_capacity(snapshot.l0_sstables.len());
        for table_id in &snapshot.l0_sstables {
            let table = snapshot.sstables[table_id].clone();
            if range_overlap(
                lower,
                upper,
                table.first_key().key_ref(),
                table.last_key().key_ref(),
            ) {
                l0_iters.push(Box::new(seek_sst_to_lower_bound(table, lower)?));
            }
        }
        let l0_iter = MergeIterator::create(l0_iters);

        let mut level_iters = Vec::with_capacity(snapshot.levels.len());
        for (_, level_sst_ids) in &snapshot.levels {
            let level_ssts = level_sst_ids
                .iter()
                .map(|id| snapshot.sstables[id].clone())
                .filter(|table| {
                    range_overlap(
                        lower,
                        upper,
                        table.first_key().key_ref(),
                        table.last_key().key_ref(),
                    )
                })
                .collect::<Vec<_>>();
            let level_iter = match lower {
                Bound::Included(key) => SstConcatIterator::create_and_seek_to_key(
                    level_ssts,
                    KeySlice::from_slice(key, TS_RANGE_BEGIN),
                )?,
                Bound::Excluded(key) => {
                    let mut iter = SstConcatIterator::create_and_seek_to_key(
                        level_ssts,
                        KeySlice::from_slice(key, TS_RANGE_BEGIN),
                    )?;
                    while iter.is_valid() && iter.key().key_ref() == key {
                        iter.next()?;
                    }
                    iter
                }
                Bound::Unbounded => SstConcatIterator::create_and_seek_to_first(level_ssts)?,
            };
            level_iters.push(Box::new(level_iter));
        }
        let levels_iter = MergeIterator::create(level_iters);

        let iter = TwoMergeIterator::create(
            TwoMergeIterator::create(memtable_iter, l0_iter)?,
            levels_iter,
        )?;
        Ok(FusedIterator::new(LsmIterator::new(
            iter,
            map_bound(upper),
            read_ts,
        )?))
    }
}

/// Replay a compaction record from the manifest. Full compaction is not handled by the controller.
fn apply_compaction_result_for_recovery(
    compaction_controller: &CompactionController,
    snapshot: &LsmStorageState,
    task: &CompactionTask,
    output: &[usize],
) -> LsmStorageState {
    if let CompactionTask::ForceFullCompaction {
        l0_sstables,
        l1_sstables,
    } = task
    {
        let mut snapshot = snapshot.clone();
        let l0_compacted = l0_sstables.iter().collect::<HashSet<_>>();
        snapshot.l0_sstables.retain(|x| !l0_compacted.contains(x));
        assert_eq!(&snapshot.levels[0].1, l1_sstables);
        snapshot.levels[0].1 = output.to_vec();
        return snapshot;
    }
    compaction_controller
        .apply_compaction_result(snapshot, task, output, true)
        .0
}

/// Check whether the user-provided range overlaps the key range `[table_begin, table_end]` of a table.
fn range_overlap(
    user_begin: Bound<&[u8]>,
    user_end: Bound<&[u8]>,
    table_begin: &[u8],
    table_end: &[u8],
) -> bool {
    match user_end {
        Bound::Excluded(key) if key <= table_begin => return false,
        Bound::Included(key) if key < table_begin => return false,
        _ => {}
    }
    match user_begin {
        Bound::Excluded(key) if key >= table_end => return false,
        Bound::Included(key) if key > table_end => return false,
        _ => {}
    }
    true
}

/// Check the key range and the bloom filter of a table to see if it may contain `key`.
fn table_may_contain_key(table: &SsTable, key: &[u8]) -> bool {
    if key < table.first_key().key_ref() || key > table.last_key().key_ref() {
        return false;
    }
    if let Some(bloom) = &table.bloom
        && !bloom.may_contain(farmhash::fingerprint32(key))
    {
        return false;
    }
    true
}

/// Create an SST iterator positioned at the first key that satisfies the lower bound.
fn seek_sst_to_lower_bound(table: Arc<SsTable>, lower: Bound<&[u8]>) -> Result<SsTableIterator> {
    let iter = match lower {
        Bound::Included(key) => SsTableIterator::create_and_seek_to_key(
            table,
            KeySlice::from_slice(key, TS_RANGE_BEGIN),
        )?,
        Bound::Excluded(key) => {
            let mut iter = SsTableIterator::create_and_seek_to_key(
                table,
                KeySlice::from_slice(key, TS_RANGE_BEGIN),
            )?;
            while iter.is_valid() && iter.key().key_ref() == key {
                iter.next()?;
            }
            iter
        }
        Bound::Unbounded => SsTableIterator::create_and_seek_to_first(table)?,
    };
    Ok(iter)
}
