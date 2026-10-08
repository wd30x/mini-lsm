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

use std::collections::HashSet;

use serde::{Deserialize, Serialize};

use crate::lsm_storage::LsmStorageState;

#[derive(Debug, Serialize, Deserialize)]
pub struct LeveledCompactionTask {
    // if upper_level is `None`, then it is L0 compaction
    pub upper_level: Option<usize>,
    pub upper_level_sst_ids: Vec<usize>,
    pub lower_level: usize,
    pub lower_level_sst_ids: Vec<usize>,
    pub is_lower_level_bottom_level: bool,
}

#[derive(Debug, Clone)]
pub struct LeveledCompactionOptions {
    pub level_size_multiplier: usize,
    pub level0_file_num_compaction_trigger: usize,
    pub max_levels: usize,
    pub base_level_size_mb: usize,
}

pub struct LeveledCompactionController {
    options: LeveledCompactionOptions,
}

impl LeveledCompactionController {
    pub fn new(options: LeveledCompactionOptions) -> Self {
        Self { options }
    }

    fn find_overlapping_ssts(
        &self,
        snapshot: &LsmStorageState,
        sst_ids: &[usize],
        in_level: usize,
    ) -> Vec<usize> {
        let begin_key = sst_ids
            .iter()
            .map(|id| snapshot.sstables[id].first_key())
            .min()
            .cloned()
            .unwrap();
        let end_key = sst_ids
            .iter()
            .map(|id| snapshot.sstables[id].last_key())
            .max()
            .cloned()
            .unwrap();
        snapshot.levels[in_level - 1]
            .1
            .iter()
            .copied()
            .filter(|sst_id| {
                let sst = &snapshot.sstables[sst_id];
                !(sst.last_key() < &begin_key || sst.first_key() > &end_key)
            })
            .collect()
    }

    pub fn generate_compaction_task(
        &self,
        snapshot: &LsmStorageState,
    ) -> Option<LeveledCompactionTask> {
        let max_levels = self.options.max_levels;

        // Compute target sizes of each level.
        let mut target_level_size = vec![0; max_levels];
        let real_level_size = snapshot
            .levels
            .iter()
            .map(|(_, files)| {
                files
                    .iter()
                    .map(|id| snapshot.sstables[id].table_size())
                    .sum::<u64>() as usize
            })
            .collect::<Vec<_>>();
        let base_level_size_bytes = self.options.base_level_size_mb * 1024 * 1024;

        target_level_size[max_levels - 1] =
            real_level_size[max_levels - 1].max(base_level_size_bytes);
        let mut base_level = max_levels;
        for i in (0..(max_levels - 1)).rev() {
            let next_level_size = target_level_size[i + 1];
            if next_level_size > base_level_size_bytes {
                target_level_size[i] = next_level_size / self.options.level_size_multiplier;
            }
            if target_level_size[i] > 0 {
                base_level = i + 1;
            }
        }

        // L0 compaction has the highest priority.
        if snapshot.l0_sstables.len() >= self.options.level0_file_num_compaction_trigger {
            println!("flush L0 SST to base level {}", base_level);
            return Some(LeveledCompactionTask {
                upper_level: None,
                upper_level_sst_ids: snapshot.l0_sstables.clone(),
                lower_level: base_level,
                lower_level_sst_ids: self.find_overlapping_ssts(
                    snapshot,
                    &snapshot.l0_sstables,
                    base_level,
                ),
                is_lower_level_bottom_level: base_level == max_levels,
            });
        }

        // Otherwise compact the level with the highest current/target size ratio.
        let mut priorities = Vec::with_capacity(max_levels);
        for level in 0..max_levels {
            let priority = real_level_size[level] as f64 / target_level_size[level] as f64;
            if priority > 1.0 {
                priorities.push((priority, level + 1));
            }
        }
        priorities.sort_by(|a, b| a.partial_cmp(b).unwrap().reverse());
        if let Some(&(_, level)) = priorities.first() {
            println!(
                "target level sizes: {:?}, real level sizes: {:?}, base_level: {}",
                target_level_size
                    .iter()
                    .map(|x| format!("{:.3}MB", *x as f64 / 1024.0 / 1024.0))
                    .collect::<Vec<_>>(),
                real_level_size
                    .iter()
                    .map(|x| format!("{:.3}MB", *x as f64 / 1024.0 / 1024.0))
                    .collect::<Vec<_>>(),
                base_level,
            );
            // Select the oldest SST in the upper level.
            let selected_sst = snapshot.levels[level - 1].1.iter().min().copied().unwrap();
            println!(
                "compaction triggered by priority: {level} out of {:?}, select {selected_sst} for compaction",
                priorities
            );
            return Some(LeveledCompactionTask {
                upper_level: Some(level),
                upper_level_sst_ids: vec![selected_sst],
                lower_level: level + 1,
                lower_level_sst_ids: self.find_overlapping_ssts(
                    snapshot,
                    &[selected_sst],
                    level + 1,
                ),
                is_lower_level_bottom_level: level + 1 == max_levels,
            });
        }
        None
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &LeveledCompactionTask,
        output: &[usize],
        in_recovery: bool,
    ) -> (LsmStorageState, Vec<usize>) {
        let mut snapshot = snapshot.clone();
        let mut files_to_remove = Vec::new();
        let mut upper_level_sst_ids_set = task
            .upper_level_sst_ids
            .iter()
            .copied()
            .collect::<HashSet<_>>();
        let mut lower_level_sst_ids_set = task
            .lower_level_sst_ids
            .iter()
            .copied()
            .collect::<HashSet<_>>();

        if let Some(upper_level) = task.upper_level {
            let new_upper_level_ssts = snapshot.levels[upper_level - 1]
                .1
                .iter()
                .copied()
                .filter(|x| !upper_level_sst_ids_set.remove(x))
                .collect::<Vec<_>>();
            assert!(upper_level_sst_ids_set.is_empty());
            snapshot.levels[upper_level - 1].1 = new_upper_level_ssts;
        } else {
            let new_l0_ssts = snapshot
                .l0_sstables
                .iter()
                .copied()
                .filter(|x| !upper_level_sst_ids_set.remove(x))
                .collect::<Vec<_>>();
            assert!(upper_level_sst_ids_set.is_empty());
            snapshot.l0_sstables = new_l0_ssts;
        }

        files_to_remove.extend(&task.upper_level_sst_ids);
        files_to_remove.extend(&task.lower_level_sst_ids);

        let mut new_lower_level_ssts = snapshot.levels[task.lower_level - 1]
            .1
            .iter()
            .copied()
            .filter(|x| !lower_level_sst_ids_set.remove(x))
            .collect::<Vec<_>>();
        assert!(lower_level_sst_ids_set.is_empty());
        new_lower_level_ssts.extend(output);
        // During recovery the SSTs are not loaded yet; the caller sorts the levels afterwards.
        if !in_recovery {
            new_lower_level_ssts.sort_by(|x, y| {
                snapshot.sstables[x]
                    .first_key()
                    .cmp(snapshot.sstables[y].first_key())
            });
        }
        snapshot.levels[task.lower_level - 1].1 = new_lower_level_ssts;
        (snapshot, files_to_remove)
    }
}
