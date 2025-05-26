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
            "l0 should be unused in tiered compaction"
        );

        if snapshot.levels.len() < self.options.num_tiers {
            return None;
        }

        // Space amplification trigger.
        let levels_size: usize = snapshot.levels[..snapshot.levels.len() - 1]
            .iter()
            .map(|(_, sstables)| sstables.len())
            .sum();
        let last_level_size = snapshot.levels.last().expect("non-empty").1.len();
        let space_amplification = (levels_size as f64 / last_level_size as f64) * 100.0;
        if space_amplification >= self.options.max_size_amplification_percent as f64 {
            println!("compaction triggered by space amplification ratio: {space_amplification}");
            return Some(TieredCompactionTask {
                tiers: snapshot.levels.clone(),
                bottom_tier_included: true,
            });
        }

        // Size ratio trigger.
        let size_trigger = (100.0 + self.options.size_ratio as f64) / 100.0;
        for (idx, (_tier, sstables)) in snapshot
            .levels
            .iter()
            .enumerate()
            .skip(self.options.min_merge_width)
        {
            let prev_sizes: usize = snapshot.levels[..idx]
                .iter()
                .map(|(_, sstables)| sstables.len())
                .sum();
            let size = sstables.len();
            let size_ratio = size as f64 / prev_sizes as f64;
            if size_ratio > size_trigger {
                println!(
                    "compaction triggered by size ratio: {} > {}",
                    size_ratio * 100.0,
                    size_trigger * 100.0
                );
                return Some(TieredCompactionTask {
                    tiers: snapshot.levels[..idx].to_vec(),
                    bottom_tier_included: false,
                });
            }
        }

        // Reduce sorted runs.
        let num_tiers = std::cmp::min(
            snapshot.levels.len(),
            self.options.max_merge_width.unwrap_or(usize::MAX),
        );
        println!("compaction triggered by reducing sorted runs");
        Some(TieredCompactionTask {
            tiers: snapshot.levels[..num_tiers].to_vec(),
            bottom_tier_included: snapshot.levels.len() >= num_tiers,
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
            "l0 should be unused in tiered compaction"
        );

        let mut del = Vec::new();
        let mut new_state = snapshot.clone();
        let mut tiers_to_compact: HashMap<_, _> = task.tiers.iter().cloned().collect();
        let tiers: Vec<_> = new_state.levels.drain(..).collect();

        if output.is_empty() {
            return (new_state, del);
        }

        let mut applied = false;
        for (tier, sst_ids) in tiers {
            match tiers_to_compact.remove(&tier) {
                Some(del_sst_ids) => {
                    del.extend(del_sst_ids);
                }
                None => {
                    new_state.levels.push((tier, sst_ids));
                }
            }
            if !applied && tiers_to_compact.is_empty() {
                applied = true;
                new_state.levels.push((output[0], output.to_vec()));
            }
        }

        (new_state, del)
    }
}
