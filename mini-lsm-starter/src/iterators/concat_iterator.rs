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

use std::sync::Arc;

use anyhow::Result;

use super::StorageIterator;
use crate::{
    key::KeySlice,
    table::{SsTable, SsTableIterator},
};

/// Concat multiple iterators ordered in key order and their key ranges do not overlap. We do not want to create the
/// iterators when initializing this iterator to reduce the overhead of seeking.
pub struct SstConcatIterator {
    current: Option<SsTableIterator>,
    next_sst_idx: usize,
    sstables: Vec<Arc<SsTable>>,
}

impl SstConcatIterator {
    pub fn create_and_seek_to_first(sstables: Vec<Arc<SsTable>>) -> Result<Self> {
        let current = sstables
            .first()
            .map(|sstable| SsTableIterator::create_and_seek_to_first(Arc::clone(sstable)))
            .transpose()?;
        Ok(Self {
            current,
            next_sst_idx: 1,
            sstables,
        })
    }

    pub fn create_and_seek_to_key(sstables: Vec<Arc<SsTable>>, key: KeySlice) -> Result<Self> {
        let idx = sstables
            .binary_search_by_key(&key, |sstable| sstable.first_key().as_key_slice())
            .unwrap_or_else(|idx| idx.saturating_sub(1));
        let current = sstables
            .get(idx)
            .map(|sstable| SsTableIterator::create_and_seek_to_key(Arc::clone(sstable), key))
            .transpose()?;
        let next_sst_idx = idx + 1;

        Ok(Self {
            current,
            next_sst_idx,
            sstables,
        })
    }
}

impl StorageIterator for SstConcatIterator {
    type KeyType<'a> = KeySlice<'a>;

    fn key(&self) -> KeySlice {
        self.current.as_ref().unwrap().key()
    }

    fn value(&self) -> &[u8] {
        self.current.as_ref().unwrap().value()
    }

    fn is_valid(&self) -> bool {
        match &self.current {
            Some(iter) => iter.is_valid(),
            None => false,
        }
    }

    fn next(&mut self) -> Result<()> {
        assert!(self.is_valid());
        let current = self.current.as_mut().unwrap();
        current.next()?;
        if !current.is_valid() {
            if self.next_sst_idx < self.sstables.len() {
                let sst = Arc::clone(&self.sstables[self.next_sst_idx]);
                let iter = SsTableIterator::create_and_seek_to_first(sst)?;
                self.current = Some(iter);
                self.next_sst_idx += 1;
            } else {
                self.current = None;
            }
        }
        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        1
    }
}
