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

mod leveled;
mod simple_leveled;
mod tiered;

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
pub use leveled::{LeveledCompactionController, LeveledCompactionOptions, LeveledCompactionTask};
use serde::{Deserialize, Serialize};
pub use simple_leveled::{
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, SimpleLeveledCompactionTask,
};
pub use tiered::{TieredCompactionController, TieredCompactionOptions, TieredCompactionTask};

use crate::iterators::StorageIterator;
use crate::iterators::concat_iterator::SstConcatIterator;
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::key::KeySlice;
use crate::lsm_storage::{CompactionFilter, LsmStorageInner, LsmStorageState};
use crate::manifest::ManifestRecord;
use crate::table::{SsTable, SsTableBuilder, SsTableIterator};

#[derive(Debug, Serialize, Deserialize)]
pub enum CompactionTask {
    Leveled(LeveledCompactionTask),
    Tiered(TieredCompactionTask),
    Simple(SimpleLeveledCompactionTask),
    ForceFullCompaction {
        l0_sstables: Vec<usize>,
        l1_sstables: Vec<usize>,
    },
}

impl CompactionTask {
    fn compact_to_bottom_level(&self) -> bool {
        match self {
            CompactionTask::ForceFullCompaction { .. } => true,
            CompactionTask::Leveled(task) => task.is_lower_level_bottom_level,
            CompactionTask::Simple(task) => task.is_lower_level_bottom_level,
            CompactionTask::Tiered(task) => task.bottom_tier_included,
        }
    }
}

pub(crate) enum CompactionController {
    Leveled(LeveledCompactionController),
    Tiered(TieredCompactionController),
    Simple(SimpleLeveledCompactionController),
    NoCompaction,
}

impl CompactionController {
    pub fn generate_compaction_task(&self, snapshot: &LsmStorageState) -> Option<CompactionTask> {
        match self {
            CompactionController::Leveled(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Leveled),
            CompactionController::Simple(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Simple),
            CompactionController::Tiered(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Tiered),
            CompactionController::NoCompaction => unreachable!(),
        }
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &CompactionTask,
        output: &[usize],
        in_recovery: bool,
    ) -> (LsmStorageState, Vec<usize>) {
        match (self, task) {
            (CompactionController::Leveled(ctrl), CompactionTask::Leveled(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output, in_recovery)
            }
            (CompactionController::Simple(ctrl), CompactionTask::Simple(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            (CompactionController::Tiered(ctrl), CompactionTask::Tiered(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            _ => unreachable!(),
        }
    }
}

impl CompactionController {
    pub fn flush_to_l0(&self) -> bool {
        matches!(
            self,
            Self::Leveled(_) | Self::Simple(_) | Self::NoCompaction
        )
    }
}

#[derive(Debug, Clone)]
pub enum CompactionOptions {
    /// Leveled compaction with partial compaction + dynamic level support (= RocksDB's Leveled
    /// Compaction)
    Leveled(LeveledCompactionOptions),
    /// Tiered compaction (= RocksDB's universal compaction)
    Tiered(TieredCompactionOptions),
    /// Simple leveled compaction
    Simple(SimpleLeveledCompactionOptions),
    /// In no compaction mode (week 1), always flush to L0
    NoCompaction,
}

impl LsmStorageInner {
    /// Merge the entries from `iter` into a list of new SSTs, each about `target_sst_size` large.
    ///
    /// All versions newer than the watermark are kept. Among the versions at or below the
    /// watermark, only the latest one is visible to any reader, so older ones are dropped. That
    /// latest version is dropped as well if it is a tombstone at the bottom level, or if it matches
    /// a compaction filter.
    fn compact_generate_sst_from_iter(
        &self,
        mut iter: impl 'static + for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>,
        compact_to_bottom_level: bool,
    ) -> Result<Vec<Arc<SsTable>>> {
        let watermark = self.mvcc().watermark();
        let compaction_filters = self.compaction_filters.lock().clone();
        let mut builder: Option<SsTableBuilder> = None;
        let mut new_ssts = Vec::new();
        let mut last_key = Vec::<u8>::new();
        let mut seen_version_below_watermark = false;
        while iter.is_valid() {
            let same_as_last_key = iter.key().key_ref() == last_key;
            if !same_as_last_key {
                last_key.clear();
                last_key.extend(iter.key().key_ref());
                seen_version_below_watermark = false;
            }

            if iter.key().ts() <= watermark {
                if seen_version_below_watermark {
                    // Shadowed by a newer version that is also below the watermark.
                    iter.next()?;
                    continue;
                }
                seen_version_below_watermark = true;
                let is_removable_tombstone = compact_to_bottom_level && iter.value().is_empty();
                let is_filtered = compaction_filters.iter().any(|filter| match filter {
                    CompactionFilter::Prefix(prefix) => iter.key().key_ref().starts_with(prefix),
                });
                if is_removable_tombstone || is_filtered {
                    iter.next()?;
                    continue;
                }
            }

            // Only split SSTs at user-key boundaries so that all versions of a key stay together.
            if !same_as_last_key
                && let Some(builder_inner) = &builder
                && builder_inner.estimated_size() >= self.options.target_sst_size
            {
                let full_builder = builder.take().unwrap();
                new_ssts.push(self.build_compacted_sst(full_builder)?);
            }
            if builder.is_none() {
                builder = Some(SsTableBuilder::new(self.options.block_size));
            }
            builder.as_mut().unwrap().add(iter.key(), iter.value());
            iter.next()?;
        }
        if let Some(builder) = builder
            && !builder.is_empty()
        {
            new_ssts.push(self.build_compacted_sst(builder)?);
        }
        Ok(new_ssts)
    }

    fn build_compacted_sst(&self, builder: SsTableBuilder) -> Result<Arc<SsTable>> {
        let sst_id = self.next_sst_id();
        Ok(Arc::new(builder.build(
            sst_id,
            Some(self.block_cache.clone()),
            self.path_of_sst(sst_id),
        )?))
    }

    /// Compact the SSTs in an upper level (or L0 if `upper_level` is `None`) with a lower level.
    fn compact_two_levels(
        &self,
        snapshot: &LsmStorageState,
        upper_level: Option<usize>,
        upper_level_sst_ids: &[usize],
        lower_level_sst_ids: &[usize],
        compact_to_bottom_level: bool,
    ) -> Result<Vec<Arc<SsTable>>> {
        let lower_ssts = lower_level_sst_ids
            .iter()
            .map(|id| snapshot.sstables[id].clone())
            .collect::<Vec<_>>();
        let lower_iter = SstConcatIterator::create_and_seek_to_first(lower_ssts)?;
        match upper_level {
            Some(_) => {
                let upper_ssts = upper_level_sst_ids
                    .iter()
                    .map(|id| snapshot.sstables[id].clone())
                    .collect::<Vec<_>>();
                let upper_iter = SstConcatIterator::create_and_seek_to_first(upper_ssts)?;
                self.compact_generate_sst_from_iter(
                    TwoMergeIterator::create(upper_iter, lower_iter)?,
                    compact_to_bottom_level,
                )
            }
            None => {
                let mut upper_iters = Vec::with_capacity(upper_level_sst_ids.len());
                for id in upper_level_sst_ids {
                    upper_iters.push(Box::new(SsTableIterator::create_and_seek_to_first(
                        snapshot.sstables[id].clone(),
                    )?));
                }
                let upper_iter = MergeIterator::create(upper_iters);
                self.compact_generate_sst_from_iter(
                    TwoMergeIterator::create(upper_iter, lower_iter)?,
                    compact_to_bottom_level,
                )
            }
        }
    }

    fn compact(&self, task: &CompactionTask) -> Result<Vec<Arc<SsTable>>> {
        let snapshot = {
            let state = self.state.read();
            state.clone()
        };
        match task {
            CompactionTask::ForceFullCompaction {
                l0_sstables,
                l1_sstables,
            } => self.compact_two_levels(
                &snapshot,
                None,
                l0_sstables,
                l1_sstables,
                task.compact_to_bottom_level(),
            ),
            CompactionTask::Simple(SimpleLeveledCompactionTask {
                upper_level,
                upper_level_sst_ids,
                lower_level_sst_ids,
                ..
            })
            | CompactionTask::Leveled(LeveledCompactionTask {
                upper_level,
                upper_level_sst_ids,
                lower_level_sst_ids,
                ..
            }) => self.compact_two_levels(
                &snapshot,
                *upper_level,
                upper_level_sst_ids,
                lower_level_sst_ids,
                task.compact_to_bottom_level(),
            ),
            CompactionTask::Tiered(TieredCompactionTask { tiers, .. }) => {
                let mut iters = Vec::with_capacity(tiers.len());
                for (_, tier_sst_ids) in tiers {
                    let ssts = tier_sst_ids
                        .iter()
                        .map(|id| snapshot.sstables[id].clone())
                        .collect::<Vec<_>>();
                    iters.push(Box::new(SstConcatIterator::create_and_seek_to_first(ssts)?));
                }
                self.compact_generate_sst_from_iter(
                    MergeIterator::create(iters),
                    task.compact_to_bottom_level(),
                )
            }
        }
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        let snapshot = {
            let state = self.state.read();
            state.clone()
        };

        let l0_sstables = snapshot.l0_sstables.clone();
        let l1_sstables = snapshot.levels[0].1.clone();
        let compaction_task = CompactionTask::ForceFullCompaction {
            l0_sstables: l0_sstables.clone(),
            l1_sstables: l1_sstables.clone(),
        };

        let sstables = self.compact(&compaction_task)?;
        let ids = sstables.iter().map(|x| x.sst_id()).collect::<Vec<_>>();

        {
            let state_lock = self.state_lock.lock();
            let mut state = self.state.read().as_ref().clone();
            for sst in l0_sstables.iter().chain(l1_sstables.iter()) {
                let result = state.sstables.remove(sst);
                assert!(result.is_some());
            }
            for new_sst in sstables {
                let result = state.sstables.insert(new_sst.sst_id(), new_sst);
                assert!(result.is_none());
            }
            assert_eq!(l1_sstables, state.levels[0].1);
            state.levels[0].1 = ids.clone();
            // New L0 SSTs may have been flushed while compacting, so only remove the compacted ones.
            let mut l0_sstables_map = l0_sstables.iter().copied().collect::<HashSet<_>>();
            state.l0_sstables = state
                .l0_sstables
                .iter()
                .filter(|x| !l0_sstables_map.remove(x))
                .copied()
                .collect::<Vec<_>>();
            assert!(l0_sstables_map.is_empty());
            *self.state.write() = Arc::new(state);
            self.sync_dir()?;
            if let Some(manifest) = &self.manifest {
                manifest.add_record(
                    &state_lock,
                    ManifestRecord::Compaction(compaction_task, ids),
                )?;
            }
        }

        for sst in l0_sstables.iter().chain(l1_sstables.iter()) {
            std::fs::remove_file(self.path_of_sst(*sst))?;
        }
        self.sync_dir()?;

        Ok(())
    }

    fn trigger_compaction(&self) -> Result<()> {
        let snapshot = {
            let state = self.state.read();
            state.clone()
        };
        let Some(task) = self
            .compaction_controller
            .generate_compaction_task(&snapshot)
        else {
            return Ok(());
        };
        self.dump_structure();
        println!("running compaction task: {:?}", task);

        let sstables = self.compact(&task)?;
        let output = sstables.iter().map(|x| x.sst_id()).collect::<Vec<_>>();

        let ssts_to_remove = {
            let state_lock = self.state_lock.lock();
            let mut snapshot = self.state.read().as_ref().clone();
            for sst in sstables {
                let result = snapshot.sstables.insert(sst.sst_id(), sst);
                assert!(result.is_none());
            }
            let (mut snapshot, files_to_remove) = self
                .compaction_controller
                .apply_compaction_result(&snapshot, &task, &output, false);
            let mut ssts_to_remove = Vec::with_capacity(files_to_remove.len());
            for file_to_remove in &files_to_remove {
                let result = snapshot.sstables.remove(file_to_remove);
                assert!(result.is_some(), "cannot remove {}.sst", file_to_remove);
                ssts_to_remove.push(result.unwrap());
            }
            *self.state.write() = Arc::new(snapshot);
            self.sync_dir()?;
            if let Some(manifest) = &self.manifest {
                manifest.add_record(
                    &state_lock,
                    ManifestRecord::Compaction(task, output.clone()),
                )?;
            }
            ssts_to_remove
        };

        println!(
            "compaction finished: {} files removed, {} files added, output={:?}",
            ssts_to_remove.len(),
            output.len(),
            output
        );
        for sst in ssts_to_remove {
            std::fs::remove_file(self.path_of_sst(sst.sst_id()))?;
        }
        self.sync_dir()?;

        Ok(())
    }

    pub(crate) fn spawn_compaction_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        if let CompactionOptions::Leveled(_)
        | CompactionOptions::Simple(_)
        | CompactionOptions::Tiered(_) = self.options.compaction_options
        {
            let this = self.clone();
            let handle = std::thread::spawn(move || {
                let ticker = crossbeam_channel::tick(Duration::from_millis(50));
                loop {
                    crossbeam_channel::select! {
                        recv(ticker) -> _ => if let Err(e) = this.trigger_compaction() {
                            eprintln!("compaction failed: {}", e);
                        },
                        recv(rx) -> _ => return
                    }
                }
            });
            return Ok(Some(handle));
        }
        Ok(None)
    }

    fn trigger_flush(&self) -> Result<()> {
        let res = {
            let state = self.state.read();
            state.imm_memtables.len() >= self.options.num_memtable_limit
        };
        if res {
            self.force_flush_next_imm_memtable()?;
        }
        Ok(())
    }

    pub(crate) fn spawn_flush_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        let this = self.clone();
        let handle = std::thread::spawn(move || {
            let ticker = crossbeam_channel::tick(Duration::from_millis(50));
            loop {
                crossbeam_channel::select! {
                    recv(ticker) -> _ => if let Err(e) = this.trigger_flush() {
                        eprintln!("flush failed: {}", e);
                    },
                    recv(rx) -> _ => return
                }
            }
        });
        Ok(Some(handle))
    }
}
