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
use crate::lsm_storage::{LsmStorageInner, LsmStorageState};
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
    // TODO(jkosh44) Remove when used.
    #[expect(unused)]
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
    fn compact(&self, task: &CompactionTask) -> Result<Vec<Arc<SsTable>>> {
        let snapshot = {
            let state = self.state.read();
            state.clone()
        };
        match task {
            CompactionTask::Leveled(_) => {
                unimplemented!()
            }
            CompactionTask::Tiered(TieredCompactionTask {
                tiers,
                bottom_tier_included: _,
            }) => {
                let mut iters = Vec::with_capacity(tiers.len());
                for (_tier, sst_ids) in tiers {
                    let sstables = sst_ids
                        .into_iter()
                        .map(|id| Arc::clone(&snapshot.sstables[id]))
                        .collect();
                    let iter = SstConcatIterator::create_and_seek_to_first(sstables)?;
                    iters.push(Box::new(iter));
                }
                let iter = MergeIterator::create(iters);
                self.compact_inner(iter)
            }
            CompactionTask::Simple(SimpleLeveledCompactionTask {
                upper_level: Some(_),
                upper_level_sst_ids,
                lower_level_sst_ids,
                ..
            }) => {
                let sstables = upper_level_sst_ids
                    .iter()
                    .chain(lower_level_sst_ids.iter())
                    .map(|id| Arc::clone(&snapshot.sstables[id]))
                    .collect();
                let iter = SstConcatIterator::create_and_seek_to_first(sstables)?;
                self.compact_inner(iter)
            }
            CompactionTask::ForceFullCompaction {
                l0_sstables,
                l1_sstables,
            }
            | CompactionTask::Simple(SimpleLeveledCompactionTask {
                upper_level: None,
                upper_level_sst_ids: l0_sstables,
                lower_level_sst_ids: l1_sstables,
                ..
            }) => {
                let l0_sstables: Vec<_> = l0_sstables
                    .iter()
                    .map(|id| Arc::clone(&snapshot.sstables[id]))
                    .map(SsTableIterator::create_and_seek_to_first)
                    .collect::<Result<_, _>>()?;
                let l0_sstables = l0_sstables.into_iter().map(Box::new).collect();
                let l0_iter = MergeIterator::create(l0_sstables);

                let l1_sstables: Vec<_> = l1_sstables
                    .iter()
                    .map(|id| Arc::clone(&snapshot.sstables[id]))
                    .collect();
                let l1_iter = SstConcatIterator::create_and_seek_to_first(l1_sstables)?;

                let iter = TwoMergeIterator::create(l0_iter, l1_iter)?;
                self.compact_inner(iter)
            }
        }
    }

    fn compact_inner<T: for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>>(
        &self,
        mut iter: T,
    ) -> Result<Vec<Arc<SsTable>>> {
        let mut results = Vec::new();
        let mut builder = SsTableBuilder::new(self.options.block_size);
        while iter.is_valid() {
            if !iter.value().is_empty() {
                builder.add(iter.key(), iter.value());
                if builder.estimated_size() >= self.options.target_sst_size {
                    let sst = self.build_sst(builder)?;
                    results.push(sst);
                    builder = SsTableBuilder::new(self.options.block_size);
                }
            }
            iter.next()?;
        }
        if builder.estimated_size() > 0 {
            let sst = self.build_sst(builder)?;
            results.push(sst);
        }

        Ok(results)
    }

    fn build_sst(&self, builder: SsTableBuilder) -> Result<Arc<SsTable>> {
        let id = self.next_sst_id();
        let sst = builder.build(
            id,
            Some(Arc::clone(&self.block_cache)),
            self.path_of_sst(id),
        )?;
        Ok(Arc::new(sst))
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        let snapshot = {
            let state = self.state.read();
            state.clone()
        };
        let l0_sstables = snapshot.l0_sstables.clone();
        let l1_sstables = snapshot.levels[0].1.clone();
        let task = CompactionTask::ForceFullCompaction {
            l0_sstables: l0_sstables.clone(),
            l1_sstables: l1_sstables.clone(),
        };
        let new_ssts = self.compact(&task)?;
        {
            let removed_l0_sstables: HashSet<_> = l0_sstables.iter().cloned().collect();
            let _state_lock = self.state_lock.lock();
            let mut state = self.state.read().as_ref().clone();
            state
                .l0_sstables
                .retain(|id| !removed_l0_sstables.contains(id));
            let new_sst_ids = new_ssts.iter().map(|sst| sst.sst_id()).collect();
            state.levels[0] = (1, new_sst_ids);
            for removed_id in l0_sstables.iter().chain(l1_sstables.iter()) {
                state.sstables.remove(removed_id);
            }
            for new_sst in new_ssts {
                state.sstables.insert(new_sst.sst_id(), new_sst);
            }
            *self.state.write() = Arc::new(state);
        }
        for removed_id in l0_sstables.iter().chain(l1_sstables.iter()) {
            let path = self.path_of_sst(*removed_id);
            std::fs::remove_file(path)?;
        }
        Ok(())
    }

    fn trigger_compaction(&self) -> Result<()> {
        let snapshot = {
            let state = self.state.read();
            state.clone()
        };
        let task = self
            .compaction_controller
            .generate_compaction_task(&snapshot);
        let Some(task) = task else {
            return Ok(());
        };
        let ssts = self.compact(&task)?;
        let output: Vec<_> = ssts.iter().map(|sst| sst.sst_id()).collect();
        let mut removed_ssts = Vec::new();
        {
            let _state_lock = self.state_lock.lock();
            let mut snapshot = self.state.read().as_ref().clone();
            for sst in ssts {
                snapshot.sstables.insert(sst.sst_id(), sst);
            }
            let (mut new_state, del) = self
                .compaction_controller
                .apply_compaction_result(&snapshot, &task, &output, false);
            for del in del {
                removed_ssts.push(del);
                new_state.sstables.remove(&del);
            }
            *self.state.write() = Arc::new(new_state);
        }
        for removed_id in removed_ssts {
            let path = self.path_of_sst(removed_id);
            std::fs::remove_file(path)?;
        }

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
        if self.state.read().imm_memtables.len() + 1 >= self.options.num_memtable_limit {
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
