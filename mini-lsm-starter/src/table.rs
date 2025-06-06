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

pub(crate) mod bloom;
mod builder;
mod iterator;

use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use anyhow::{Result, anyhow};
pub use builder::SsTableBuilder;
use bytes::Buf;
pub use iterator::SsTableIterator;

use crate::block::Block;
use crate::key::{KeyBytes, KeySlice};
use crate::lsm_storage::BlockCache;

use self::bloom::Bloom;

const BLOCK_META_OFFSET_SIZE: usize = size_of::<u32>();

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockMeta {
    /// Offset of this data block.
    pub offset: usize,
    /// The first key of the data block.
    pub first_key: KeyBytes,
    /// The last key of the data block.
    pub last_key: KeyBytes,
}

impl BlockMeta {
    /// Encode block meta to a buffer.
    /// You may add extra fields to the buffer,
    /// in order to help keep track of `first_key` when decoding from the same buffer in the future.
    pub fn encode_block_meta(block_meta: &[BlockMeta], buf: &mut Vec<u8>) {
        for block_meta in block_meta {
            assert!(
                block_meta.offset <= u32::MAX as usize,
                "{} too big",
                block_meta.offset
            );
            let offset_bytes = (block_meta.offset as u32).to_le_bytes();
            assert!(block_meta.first_key.len() <= u16::MAX as usize);
            let first_key_len_bytes = (block_meta.first_key.len() as u16).to_le_bytes();
            let first_key_bytes = block_meta.first_key.raw_ref().iter().cloned();
            assert!(block_meta.last_key.len() <= u16::MAX as usize);
            let last_key_len_bytes = (block_meta.last_key.len() as u16).to_le_bytes();
            let last_key_bytes = block_meta.last_key.raw_ref().iter().cloned();
            let bytes = std::iter::empty()
                .chain(offset_bytes)
                .chain(first_key_len_bytes)
                .chain(first_key_bytes)
                .chain(last_key_len_bytes)
                .chain(last_key_bytes);
            buf.extend(bytes);
        }
    }

    /// Decode block meta from a buffer.
    pub fn decode_block_meta(mut buf: impl Buf) -> Vec<BlockMeta> {
        let mut block_metas = Vec::new();
        while buf.has_remaining() {
            let offset = buf.get_u32_le() as usize;
            let first_key_len = buf.get_u16_le() as usize;
            let first_key = buf.copy_to_bytes(first_key_len);
            let first_key = KeyBytes::from_bytes(first_key);
            let last_key_len = buf.get_u16_le() as usize;
            let last_key = buf.copy_to_bytes(last_key_len);
            let last_key = KeyBytes::from_bytes(last_key);
            let block_meta = BlockMeta {
                offset,
                first_key,
                last_key,
            };
            block_metas.push(block_meta);
        }
        block_metas
    }
}

/// A file object.
pub struct FileObject(Option<File>, u64);

impl FileObject {
    pub fn read(&self, offset: u64, len: u64) -> Result<Vec<u8>> {
        use std::os::unix::fs::FileExt;
        let mut data = vec![0; len as usize];
        self.0
            .as_ref()
            .unwrap()
            .read_exact_at(&mut data[..], offset)?;
        Ok(data)
    }

    pub fn size(&self) -> u64 {
        self.1
    }

    /// Create a new file object (day 2) and write the file to the disk (day 4).
    pub fn create(path: &Path, data: Vec<u8>) -> Result<Self> {
        std::fs::write(path, &data)?;
        File::open(path)?.sync_all()?;
        Ok(FileObject(
            Some(File::options().read(true).write(false).open(path)?),
            data.len() as u64,
        ))
    }

    pub fn open(path: &Path) -> Result<Self> {
        let file = File::options().read(true).write(false).open(path)?;
        let size = file.metadata()?.len();
        Ok(FileObject(Some(file), size))
    }
}

/// An SSTable.
pub struct SsTable {
    /// The actual storage unit of SsTable, the format is as above.
    pub(crate) file: FileObject,
    /// The meta blocks that hold info for data blocks.
    pub(crate) block_meta: Vec<BlockMeta>,
    /// The offset that indicates the start point of meta blocks in `file`.
    pub(crate) block_meta_offset: usize,
    id: usize,
    block_cache: Option<Arc<BlockCache>>,
    first_key: KeyBytes,
    last_key: KeyBytes,
    pub(crate) bloom: Option<Bloom>,
    /// The maximum timestamp stored in this SST, implemented in week 3.
    max_ts: u64,
}

impl SsTable {
    #[cfg(test)]
    pub(crate) fn open_for_test(file: FileObject) -> Result<Self> {
        Self::open(0, None, file)
    }

    /// Open SSTable from a file.
    pub fn open(id: usize, block_cache: Option<Arc<BlockCache>>, file: FileObject) -> Result<Self> {
        let data = file.read(0, file.size())?;

        if data.is_empty() {
            return Ok(Self {
                file,
                block_meta: Vec::new(),
                block_meta_offset: 0,
                id,
                block_cache,
                first_key: KeyBytes::default(),
                last_key: KeyBytes::default(),
                bloom: None,
                max_ts: 0,
            });
        }

        let offset = data.len() - BLOCK_META_OFFSET_SIZE;
        let bloom_filter_offset = &data[offset..];
        let data = &data[..offset];
        let bloom_filter_offset =
            u32::from_le_bytes(bloom_filter_offset.try_into().expect("correct size")) as usize;

        let bloom_filter_bytes = &data[bloom_filter_offset..];
        let data = &data[..bloom_filter_offset];
        let bloom_filter = Bloom::decode(bloom_filter_bytes)?;

        let offset = data.len() - BLOCK_META_OFFSET_SIZE;
        let block_meta_offset = &data[offset..];
        let data = &data[..offset];
        let block_meta_offset =
            u32::from_le_bytes(block_meta_offset.try_into().expect("correct size")) as usize;
        let block_meta_bytes = &data[block_meta_offset..];
        let block_meta = BlockMeta::decode_block_meta(block_meta_bytes);

        let first_key = block_meta.first().unwrap().first_key.clone();
        let last_key = block_meta.last().unwrap().last_key.clone();

        Ok(Self {
            file,
            block_meta,
            block_meta_offset,
            id,
            block_cache,
            first_key,
            last_key,
            bloom: Some(bloom_filter),
            max_ts: 0,
        })
    }

    /// Create a mock SST with only first key + last key metadata
    pub fn create_meta_only(
        id: usize,
        file_size: u64,
        first_key: KeyBytes,
        last_key: KeyBytes,
    ) -> Self {
        Self {
            file: FileObject(None, file_size),
            block_meta: vec![],
            block_meta_offset: 0,
            id,
            block_cache: None,
            first_key,
            last_key,
            bloom: None,
            max_ts: 0,
        }
    }

    /// Read a block from the disk.
    pub fn read_block(&self, block_idx: usize) -> Result<Arc<Block>> {
        let meta = self
            .block_meta
            .get(block_idx)
            .ok_or_else(|| anyhow!("invalid idx"))?;
        let offset = meta.offset;
        let len = match self.block_meta.get(block_idx + 1) {
            Some(meta) => meta.offset - offset,
            None => self.block_meta_offset - offset,
        };
        let data = self.file.read(offset as u64, len as u64)?;
        Ok(Arc::new(Block::decode(data.as_slice())))
    }

    /// Read a block from disk, with block cache. (Day 4)
    pub fn read_block_cached(&self, block_idx: usize) -> Result<Arc<Block>> {
        match &self.block_cache {
            Some(cache) => cache
                .try_get_with((self.id, block_idx), || self.read_block(block_idx))
                .map_err(|e| anyhow!("{}", e)),
            None => self.read_block(block_idx),
        }
    }

    /// Find the block that may contain `key`.
    /// Note: You may want to make use of the `first_key` stored in `BlockMeta`.
    /// You may also assume the key-value pairs stored in each consecutive block are sorted.
    pub fn find_block_idx(&self, key: KeySlice) -> usize {
        for (idx, block_meta) in self.block_meta.iter().enumerate() {
            if block_meta.first_key.as_key_slice() <= key
                && key <= block_meta.last_key.as_key_slice()
            {
                return idx;
            }
        }

        self.block_meta.len() - 1
    }

    /// Get number of data blocks.
    pub fn num_of_blocks(&self) -> usize {
        self.block_meta.len()
    }

    pub fn first_key(&self) -> &KeyBytes {
        &self.first_key
    }

    pub fn last_key(&self) -> &KeyBytes {
        &self.last_key
    }

    pub fn table_size(&self) -> u64 {
        self.file.1
    }

    pub fn sst_id(&self) -> usize {
        self.id
    }

    pub fn max_ts(&self) -> u64 {
        self.max_ts
    }
}
