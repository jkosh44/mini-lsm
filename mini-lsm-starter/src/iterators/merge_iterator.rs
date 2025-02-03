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

use std::cmp::{self};
use std::collections::BinaryHeap;

use anyhow::Result;

use crate::key::KeySlice;

use super::StorageIterator;

struct HeapWrapper<I: StorageIterator>(pub usize, pub Box<I>);

impl<I: StorageIterator> PartialEq for HeapWrapper<I> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == cmp::Ordering::Equal
    }
}

impl<I: StorageIterator> Eq for HeapWrapper<I> {}

impl<I: StorageIterator> PartialOrd for HeapWrapper<I> {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<I: StorageIterator> Ord for HeapWrapper<I> {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.1
            .key()
            .cmp(&other.1.key())
            .then(self.0.cmp(&other.0))
            .reverse()
    }
}

/// Merge multiple iterators of the same type. If the same key occurs multiple times in some
/// iterators, prefer the one with smaller index.
pub struct MergeIterator<I: StorageIterator> {
    iters: BinaryHeap<HeapWrapper<I>>,
    current: Option<HeapWrapper<I>>,
}

impl<I: StorageIterator> MergeIterator<I> {
    pub fn create(iters: Vec<Box<I>>) -> Self {
        let mut iters: BinaryHeap<HeapWrapper<I>> = iters
            .into_iter()
            .filter(|iter| iter.is_valid())
            .enumerate()
            .map(|(idx, iter)| HeapWrapper(idx, iter))
            .collect();
        let current = iters.pop();
        Self { iters, current }
    }
}

impl<I: 'static + for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>> MergeIterator<I> {
    /// Advance all iters in `self.iters` s.t. they are all greater than `self.current`.
    fn advance_iters(&mut self) -> Result<()> {
        if !self.is_valid() {
            return Ok(());
        }

        while let Some(iter) = self.iters.peek() {
            if iter.1.key() > self.key() {
                return Ok(());
            }

            let mut iter = self.iters.pop().expect("peeked above");
            match self.advance_iter(&mut iter) {
                Ok(()) => {
                    if iter.1.is_valid() {
                        self.iters.push(iter)
                    }
                }
                e @ Err(_) => return e,
            }
        }

        if let Some(current) = &self.current {
            assert!(self
                .iters
                .iter()
                .all(|iter| iter.1.is_valid() && iter.1.key() > current.1.key()));
        }

        Ok(())
    }

    /// Advance `iter` s.t. it is less than `self.current`.
    fn advance_iter(&self, iter: &mut HeapWrapper<I>) -> Result<()> {
        assert!(iter.1.is_valid());
        while iter.1.is_valid() && iter.1.key() <= self.key() {
            iter.1.next()?;
        }
        assert!(!iter.1.is_valid() || iter.1.key() > self.key());
        Ok(())
    }

    fn invalidate(&mut self) {
        self.iters.clear();
        self.current.take();
        assert!(!self.is_valid());
    }
}

impl<I: 'static + for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>> StorageIterator
    for MergeIterator<I>
{
    type KeyType<'a> = KeySlice<'a>;

    fn key(&self) -> KeySlice {
        self.current.as_ref().expect("is valid").1.key()
    }

    fn value(&self) -> &[u8] {
        self.current.as_ref().expect("is valid").1.value()
    }

    fn is_valid(&self) -> bool {
        self.current.is_some()
    }

    fn next(&mut self) -> Result<()> {
        if !self.is_valid() {
            return Ok(());
        }
        self.advance_iters()?;

        let mut current = self.current.take().expect("is valid");
        if let Err(e) = current.1.next() {
            self.invalidate();
            return Err(e);
        }
        if current.1.is_valid() {
            self.iters.push(current);
        }
        self.current = self.iters.pop();
        if let Err(e) = self.advance_iters() {
            self.invalidate();
            return Err(e);
        }

        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        self.iters
            .iter()
            .map(|iter| iter.1.num_active_iterators())
            .sum::<usize>()
            + self
                .current
                .as_ref()
                .map(|iter| iter.1.num_active_iterators())
                .unwrap_or(0)
    }
}
