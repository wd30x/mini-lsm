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

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::lsm_storage::LsmStorageState;

#[derive(Debug, Serialize, Deserialize)]
pub struct TieredCompactionTask {
    pub tiers: Vec<(usize, Vec<usize>)>,
    pub bottom_tier_included: bool,
}

#[derive(Debug, Clone)]
pub struct TieredCompactionOptions {
    pub num_tiers: usize,
    pub max_size_amplification_percent: usize,
    pub size_ratio: usize,
    pub min_merge_width: usize,
    pub max_merge_width: Option<usize>,
}

pub struct TieredCompactionController {
    options: TieredCompactionOptions,
}

impl TieredCompactionController {
    pub fn new(options: TieredCompactionOptions) -> Self {
        Self { options }
    }

    pub fn generate_compaction_task(
        &self,
        snapshot: &LsmStorageState,
    ) -> Option<TieredCompactionTask> {
        assert!(
            snapshot.l0_sstables.is_empty(),
            "should not add l0 ssts in tiered compaction"
        );
        if snapshot.levels.len() < self.options.num_tiers {
            return None;
        }

        // Triggered by space amplification ratio.
        let upper_levels_size = snapshot.levels[..snapshot.levels.len() - 1]
            .iter()
            .map(|(_, files)| files.len())
            .sum::<usize>();
        let last_level_size = snapshot.levels.last().unwrap().1.len();
        let space_amp_ratio = upper_levels_size as f64 / last_level_size as f64 * 100.0;
        if space_amp_ratio >= self.options.max_size_amplification_percent as f64 {
            println!(
                "compaction triggered by space amplification ratio: {}",
                space_amp_ratio
            );
            return Some(TieredCompactionTask {
                tiers: snapshot.levels.clone(),
                bottom_tier_included: true,
            });
        }

        // Triggered by size ratio.
        let size_ratio_trigger = (100.0 + self.options.size_ratio as f64) / 100.0;
        let mut previous_tiers_size = 0;
        for id in 0..(snapshot.levels.len() - 1) {
            previous_tiers_size += snapshot.levels[id].1.len();
            let this_tier_size = snapshot.levels[id + 1].1.len();
            let current_size_ratio = this_tier_size as f64 / previous_tiers_size as f64;
            if current_size_ratio > size_ratio_trigger && id + 1 >= self.options.min_merge_width {
                println!(
                    "compaction triggered by size ratio: {}",
                    current_size_ratio * 100.0
                );
                return Some(TieredCompactionTask {
                    tiers: snapshot.levels[..=id].to_vec(),
                    bottom_tier_included: id + 1 >= snapshot.levels.len(),
                });
            }
        }

        // Reduce sorted runs.
        let num_tiers_to_take = snapshot
            .levels
            .len()
            .min(self.options.max_merge_width.unwrap_or(usize::MAX));
        println!("compaction triggered by reducing sorted runs");
        Some(TieredCompactionTask {
            tiers: snapshot.levels[..num_tiers_to_take].to_vec(),
            bottom_tier_included: num_tiers_to_take >= snapshot.levels.len(),
        })
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &TieredCompactionTask,
        output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        assert!(
            snapshot.l0_sstables.is_empty(),
            "should not add l0 ssts in tiered compaction"
        );
        let mut snapshot = snapshot.clone();
        let mut tiers_to_remove = task
            .tiers
            .iter()
            .map(|(tier_id, files)| (*tier_id, files))
            .collect::<HashMap<_, _>>();
        let mut levels = Vec::with_capacity(snapshot.levels.len());
        let mut new_tier_added = false;
        let mut files_to_remove = Vec::new();
        for (tier_id, files) in &snapshot.levels {
            if let Some(compacted_files) = tiers_to_remove.remove(tier_id) {
                // The tier is part of the compaction task.
                assert_eq!(
                    compacted_files, files,
                    "file changed after issuing compaction task"
                );
                files_to_remove.extend(compacted_files.iter().copied());
            } else {
                // New tiers may have been flushed in front of the compacted ones.
                levels.push((*tier_id, files.clone()));
            }
            if tiers_to_remove.is_empty() && !new_tier_added {
                // Place the new tier where the compacted tiers were.
                new_tier_added = true;
                if !output.is_empty() {
                    levels.push((output[0], output.to_vec()));
                }
            }
        }
        assert!(tiers_to_remove.is_empty(), "some tiers not found??");
        snapshot.levels = levels;
        (snapshot, files_to_remove)
    }
}
