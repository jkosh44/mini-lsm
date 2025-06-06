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

use super::SsTable;
use crate::{block::BlockIterator, iterators::StorageIterator, key::KeySlice};

/// An iterator over the contents of an SSTable.
pub struct SsTableIterator {
    table: Arc<SsTable>,
    blk_iter: BlockIterator,
    blk_idx: usize,
}

impl SsTableIterator {
    /// Create a new iterator and seek to the first key-value pair in the first data block.
    pub fn create_and_seek_to_first(table: Arc<SsTable>) -> Result<Self> {
        let block = table.read_block_cached(0)?;
        let blk_iter = BlockIterator::create_and_seek_to_first(block);
        Ok(Self {
            table,
            blk_iter,
            blk_idx: 0,
        })
    }

    /// Seek to the first key-value pair in the first data block.
    pub fn seek_to_first(&mut self) -> Result<()> {
        self.blk_idx = 0;
        let block = self.table.read_block_cached(0)?;
        self.blk_iter = BlockIterator::create_and_seek_to_first(block);
        Ok(())
    }

    /// Create a new iterator and seek to the first key-value pair which >= `key`.
    pub fn create_and_seek_to_key(table: Arc<SsTable>, key: KeySlice) -> Result<Self> {
        let blk_idx = table.find_block_idx(key);
        let block = table.read_block_cached(blk_idx)?;
        let blk_iter = BlockIterator::create_and_seek_to_key(block, key);
        Ok(Self {
            table,
            blk_iter,
            blk_idx,
        })
    }

    /// Seek to the first key-value pair which >= `key`.
    /// Note: You probably want to review the handout for detailed explanation when implementing
    /// this function.
    pub fn seek_to_key(&mut self, key: KeySlice) -> Result<()> {
        // Find block.
        let mut idx = self
            .table
            .block_meta
            .binary_search_by_key(&key, |meta| meta.first_key.as_key_slice())
            .unwrap_or_else(|idx| idx);
        // Move to next block if this block's last key is less than our key.
        if idx < self.table.block_meta.len() - 1
            && self.table.block_meta[idx].last_key.as_key_slice() < key
        {
            idx += 1;
        }
        // Move to the previous block if it's last key is greater than our key.
        if idx > 0 && self.table.block_meta[idx - 1].last_key.as_key_slice() >= key {
            idx -= 1;
        }
        // Bound idx at last block.
        if idx >= self.table.block_meta.len() {
            idx = self.table.block_meta.len() - 1;
        }

        self.blk_idx = idx;
        let block = self.table.read_block_cached(idx)?;
        self.blk_iter = BlockIterator::create_and_seek_to_key(block, key);
        Ok(())
    }
}

impl StorageIterator for SsTableIterator {
    type KeyType<'a> = KeySlice<'a>;

    /// Return the `key` that's held by the underlying block iterator.
    fn key(&self) -> KeySlice {
        self.blk_iter.key()
    }

    /// Return the `value` that's held by the underlying block iterator.
    fn value(&self) -> &[u8] {
        self.blk_iter.value()
    }

    /// Return whether the current block iterator is valid or not.
    fn is_valid(&self) -> bool {
        assert!(
            self.blk_iter.is_valid() || self.blk_idx == self.table.num_of_blocks() - 1,
            "blk_idx={:?}, num_block={:?}",
            self.blk_idx,
            self.table.num_of_blocks()
        );
        self.blk_iter.is_valid()
    }

    /// Move to the next `key` in the block.
    /// Note: You may want to check if the current block iterator is valid after the move.
    fn next(&mut self) -> Result<()> {
        self.blk_iter.next();
        while !self.blk_iter.is_valid() && self.blk_idx < self.table.num_of_blocks() - 1 {
            self.blk_idx += 1;
            let block = self.table.read_block_cached(self.blk_idx)?;
            self.blk_iter = BlockIterator::create_and_seek_to_first(block);
        }

        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        if self.is_valid() { 1 } else { 0 }
    }
}
