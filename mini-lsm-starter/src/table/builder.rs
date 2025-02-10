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

use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use bytes::Bytes;

use super::{BlockMeta, FileObject, SsTable};
use crate::key::KeyBytes;
use crate::table::bloom::Bloom;
use crate::{block::BlockBuilder, key::KeySlice, lsm_storage::BlockCache};

/// Builds an SSTable from key-value pairs.
pub struct SsTableBuilder {
    builder: BlockBuilder,
    first_key: Vec<u8>,
    last_key: Vec<u8>,
    data: Vec<u8>,
    key_hashes: Vec<u32>,
    pub(crate) meta: Vec<BlockMeta>,
    block_size: usize,
}

impl SsTableBuilder {
    /// Create a builder based on target block size.
    pub fn new(block_size: usize) -> Self {
        let builder = BlockBuilder::new(block_size);
        Self {
            builder,
            first_key: Vec::new(),
            last_key: Vec::new(),
            data: Vec::new(),
            key_hashes: Vec::new(),
            meta: Vec::new(),
            block_size,
        }
    }

    /// Adds a key-value pair to SSTable.
    ///
    /// Note: You should split a new block when the current block is full.(`std::mem::replace` may
    /// be helpful here)
    pub fn add(&mut self, key: KeySlice, value: &[u8]) {
        if self.first_key.is_empty() {
            self.first_key = key.raw_ref().to_vec();
        }

        // TODO(jkosh44) This seems really wasteful.
        self.last_key = key.raw_ref().to_vec();
        let key_hash = farmhash::fingerprint32(key.raw_ref());
        self.key_hashes.push(key_hash);

        if !self.builder.add(key, value) {
            self.build_block();
        }
    }

    /// Get the estimated size of the SSTable.
    ///
    /// Since the data blocks contain much more data than meta blocks, just return the size of data
    /// blocks here.
    pub fn estimated_size(&self) -> usize {
        self.builder.size() + self.data.len()
    }

    /// Builds the SSTable and writes it to the given path. Use the `FileObject` structure to manipulate the disk objects.
    pub fn build(
        mut self,
        id: usize,
        block_cache: Option<Arc<BlockCache>>,
        path: impl AsRef<Path>,
    ) -> Result<SsTable> {
        self.build_block();
        let mut data = self.data;
        let block_meta_offset = data.len();
        BlockMeta::encode_block_meta(&self.meta, &mut data);
        data.extend((block_meta_offset as u32).to_le_bytes());
        let bloom_filter_offset = data.len();
        let bloom_filter = Bloom::new(&self.key_hashes, 0.01);
        bloom_filter.encode(&mut data);
        data.extend((bloom_filter_offset as u32).to_le_bytes());
        let file = FileObject::create(path.as_ref(), data)?;

        // TODO(jkosh44) Is this right?
        let (first_key, last_key) = match (self.meta.first(), self.meta.last()) {
            (Some(first), Some(last)) => (first.first_key.clone(), last.last_key.clone()),
            (None, None) => (KeyBytes::default(), KeyBytes::default()),
            _ => unreachable!(),
        };

        Ok(SsTable {
            file,
            block_meta: self.meta,
            block_meta_offset,
            id,
            block_cache,
            first_key,
            last_key,
            bloom: Some(bloom_filter),
            max_ts: 0,
        })
    }

    fn build_block(&mut self) {
        if !self.builder.is_empty() {
            let first_key: Bytes = std::mem::take(&mut self.first_key).into();
            let first_key = KeyBytes::from_bytes(first_key);
            let last_key: Bytes = std::mem::take(&mut self.last_key).into();
            let last_key = KeyBytes::from_bytes(last_key);
            let block_meta = BlockMeta {
                offset: self.data.len(),
                first_key,
                last_key,
            };
            self.meta.push(block_meta);

            let next_builder = BlockBuilder::new(self.block_size);
            let builder = std::mem::replace(&mut self.builder, next_builder);
            let block = builder.build();
            self.data.extend(block.encode());
        }
    }

    #[cfg(test)]
    pub(crate) fn build_for_test(self, path: impl AsRef<Path>) -> Result<SsTable> {
        self.build(0, None, path)
    }
}
