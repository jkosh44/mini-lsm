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

use anyhow::{anyhow, Result};
use bytes::Bytes;
use std::collections::Bound;

use crate::iterators::concat_iterator::SstConcatIterator;
use crate::{
    iterators::{
        StorageIterator, merge_iterator::MergeIterator, two_merge_iterator::TwoMergeIterator,
    },
    mem_table::MemTableIterator,
    table::SsTableIterator,
};

/// Represents the internal type for an LSM iterator. This type will be changed across the course for multiple times.
type LsmIteratorInner = TwoMergeIterator<
    TwoMergeIterator<MergeIterator<MemTableIterator>, MergeIterator<SsTableIterator>>,
    MergeIterator<SstConcatIterator>,
>;

pub struct LsmIterator {
    inner: LsmIteratorInner,
    upper: Bound<Bytes>,
    past_upper: bool,
}

impl LsmIterator {
    pub(crate) fn new(iter: LsmIteratorInner, upper: Bound<Bytes>) -> Result<Self> {
        Ok(Self {
            inner: iter,
            upper,
            past_upper: false,
        })
    }

    fn is_past_upper(&self) -> bool {
        if !self.is_valid() {
            true
        } else {
            match &self.upper {
                Bound::Unbounded => false,
                Bound::Included(upper) => self.key() > upper,
                Bound::Excluded(upper) => self.key() >= upper,
            }
        }
    }
}

impl StorageIterator for LsmIterator {
    type KeyType<'a> = &'a [u8];

    fn is_valid(&self) -> bool {
        !self.past_upper && self.inner.is_valid()
    }

    fn key(&self) -> &[u8] {
        self.inner.key().raw_ref()
    }

    fn value(&self) -> &[u8] {
        self.inner.value()
    }

    fn next(&mut self) -> Result<()> {
        self.inner.next()?;
        self.past_upper = self.is_past_upper();
        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        self.inner.num_active_iterators()
    }
}

enum NextState {
    Valid,
    Invalid,
    Errored(anyhow::Error),
}

impl NextState {
    fn is_valid(&self) -> bool {
        matches!(self, NextState::Valid)
    }

    fn error(&self) -> Option<&anyhow::Error> {
        match self {
            NextState::Errored(e) => Some(e),
            _ => None,
        }
    }
}

/// A wrapper around existing iterator, will prevent users from calling `next` when the iterator is
/// invalid. If an iterator is already invalid, `next` does not do anything. If `next` returns an error,
/// `is_valid` should return false, and `next` should always return an error.
pub struct FusedIterator<I: StorageIterator> {
    iter: I,
    state: NextState,
}

impl<I: StorageIterator> FusedIterator<I> {
    pub fn new(iter: I) -> Self {
        let mut iter = Self {
            iter,
            state: NextState::Valid,
        };
        // I hope this doesn't error...
        let _ = iter.skip_deleted_values();
        iter
    }

    fn skip_deleted_values(&mut self) -> Result<()> {
        while self.is_valid() && self.value().is_empty() {
            self.next_inner()?;
        }
        Ok(())
    }

    fn next_inner(&mut self) -> Result<()> {
        match self.iter.next() {
            Ok(()) => Ok(()),
            Err(e) => {
                self.state = NextState::Errored(e);
                Err(anyhow!("{}", self.state.error().unwrap()))
            }
        }
    }
}

impl<I: StorageIterator> StorageIterator for FusedIterator<I> {
    type KeyType<'a>
        = I::KeyType<'a>
    where
        Self: 'a;

    fn is_valid(&self) -> bool {
        self.state.is_valid() && self.iter.is_valid()
    }

    fn key(&self) -> Self::KeyType<'_> {
        self.iter.key()
    }

    fn value(&self) -> &[u8] {
        self.iter.value()
    }

    fn next(&mut self) -> Result<()> {
        match &self.state {
            NextState::Valid => {
                self.next_inner()?;
                self.skip_deleted_values()?;
                if !self.iter.is_valid() {
                    self.state = NextState::Invalid;
                }
                Ok(())
            }
            NextState::Invalid => Ok(()),
            NextState::Errored(e) => Err(anyhow!("{e}")),
        }
    }

    fn num_active_iterators(&self) -> usize {
        self.iter.num_active_iterators()
    }
}
