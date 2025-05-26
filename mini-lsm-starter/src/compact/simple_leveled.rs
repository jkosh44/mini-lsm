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

#[derive(Debug, Clone)]
pub struct SimpleLeveledCompactionOptions {
    pub size_ratio_percent: usize,
    pub level0_file_num_compaction_trigger: usize,
    pub max_levels: usize,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SimpleLeveledCompactionTask {
    // if upper_level is `None`, then it is L0 compaction
    pub upper_level: Option<usize>,
    pub upper_level_sst_ids: Vec<usize>,
    pub lower_level: usize,
    pub lower_level_sst_ids: Vec<usize>,
    pub is_lower_level_bottom_level: bool,
}

pub struct SimpleLeveledCompactionController {
    options: SimpleLeveledCompactionOptions,
}

impl SimpleLeveledCompactionController {
    pub fn new(options: SimpleLeveledCompactionOptions) -> Self {
        Self { options }
    }

    /// Generates a compaction task.
    ///
    /// Returns `None` if no compaction needs to be scheduled. The order of SSTs in the compaction task id vector matters.
    pub fn generate_compaction_task(
        &self,
        snapshot: &LsmStorageState,
    ) -> Option<SimpleLeveledCompactionTask> {
        if snapshot.l0_sstables.len() >= self.options.level0_file_num_compaction_trigger {
            println!(
                "compaction triggered at level 0 because L0 has {} SSTs >= {}",
                snapshot.l0_sstables.len(),
                self.options.level0_file_num_compaction_trigger
            );
            return Some(SimpleLeveledCompactionTask {
                upper_level: None,
                upper_level_sst_ids: snapshot.l0_sstables.clone(),
                lower_level: 1,
                lower_level_sst_ids: snapshot.levels[0].1.clone(),
                is_lower_level_bottom_level: false,
            });
        }

        let max_level = self.options.max_levels;
        for idx in 0..(max_level - 1) {
            let (upper_level, upper_level_sst_ids) = &snapshot.levels[idx];
            let (lower_level, lower_level_sst_ids) = &snapshot.levels[idx + 1];

            assert_eq!(*upper_level, idx + 1);
            assert_eq!(*lower_level, idx + 2);

            let size_ratio = lower_level_sst_ids
                .len()
                .checked_div(upper_level_sst_ids.len());
            if let Some(size_ratio) = size_ratio {
                if size_ratio * 100 < self.options.size_ratio_percent {
                    println!(
                        "compaction triggered at level {} and {} with size ratio {}",
                        upper_level, lower_level, size_ratio
                    );
                    return Some(SimpleLeveledCompactionTask {
                        upper_level: Some(*upper_level),
                        upper_level_sst_ids: upper_level_sst_ids.clone(),
                        lower_level: *lower_level,
                        lower_level_sst_ids: lower_level_sst_ids.clone(),
                        is_lower_level_bottom_level: *lower_level == max_level,
                    });
                }
            }
        }

        None
    }

    /// Apply the compaction result.
    ///
    /// The compactor will call this function with the compaction task and the list of SST ids generated. This function applies the
    /// result and generates a new LSM state. The functions should only change `l0_sstables` and `levels` without changing memtables
    /// and `sstables` hash map. Though there should only be one thread running compaction jobs, you should think about the case
    /// where an L0 SST gets flushed while the compactor generates new SSTs, and with that in mind, you should do some sanity checks
    /// in your implementation.
    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &SimpleLeveledCompactionTask,
        output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        let mut del = Vec::new();
        let mut new_state = snapshot.clone();

        /// Helper fns to access upper level.
        fn upper_level<'a>(
            state: &'a LsmStorageState,
            upper_level: &'a Option<usize>,
        ) -> &'a Vec<usize> {
            match upper_level {
                None => &state.l0_sstables,
                Some(upper_level) => &state.levels[upper_level - 1].1,
            }
        }
        fn upper_level_mut<'a>(
            state: &'a mut LsmStorageState,
            upper_level: &'a Option<usize>,
        ) -> &'a mut Vec<usize> {
            match upper_level {
                None => &mut state.l0_sstables,
                Some(upper_level) => &mut state.levels[upper_level - 1].1,
            }
        }

        // Remove upper level sst IDs.
        let del_upper_sst_ids: HashSet<_> = task.upper_level_sst_ids.iter().collect();
        upper_level_mut(&mut new_state, &task.upper_level).clear();
        for sst_id in upper_level(snapshot, &task.upper_level) {
            if del_upper_sst_ids.contains(sst_id) {
                del.push(*sst_id);
            } else {
                upper_level_mut(&mut new_state, &task.upper_level).push(*sst_id);
            }
        }

        // Remove lower level sst IDs.
        let del_lower_sst_ids: HashSet<_> = task.lower_level_sst_ids.iter().collect();
        new_state.levels[task.lower_level - 1].1.clear();
        for sst_id in &snapshot.levels[task.lower_level - 1].1 {
            if del_lower_sst_ids.contains(sst_id) {
                del.push(*sst_id);
            } else {
                new_state.levels[task.lower_level - 1].1.push(*sst_id);
            }
        }

        // Add new lower level sst IDs.
        new_state.levels[task.lower_level - 1]
            .1
            .extend_from_slice(output);

        (new_state, del)
    }
}
