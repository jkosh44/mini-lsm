use crate::key::{KeySlice, KeyVec};

use super::Block;

/// Builds a block.
pub struct BlockBuilder {
    /// Offsets of each key-value entries.
    offsets: Vec<u16>,
    /// All serialized key-value pairs in the block.
    data: Vec<u8>,
    /// The expected block size.
    block_size: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        Self {
            offsets: Vec::new(),
            data: Vec::new(),
            block_size,
            first_key: KeyVec::new(),
        }
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    #[must_use]
    pub fn add(&mut self, key: KeySlice, value: &[u8]) -> bool {
        let mut is_first_key = false;
        if self.is_empty() {
            is_first_key = true;
        }

        self.offsets.push(self.data.len() as u16);

        // Compute how much of the key overlaps with the first key.
        let key_overlap_len = key
            .raw_ref()
            .iter()
            .zip(self.first_key.raw_ref())
            .enumerate()
            .find_map(|(idx, (key, first_key))| if key != first_key { Some(idx) } else { None })
            .unwrap_or_else(|| std::cmp::min(key.len(), self.first_key.len()));
        let rest_key_len = key.len() - key_overlap_len;
        let rest_of_key = &key.raw_ref()[key_overlap_len..];
        let key_overlap_len = key_overlap_len as u16;
        let rest_key_len = rest_key_len as u16;

        let value_len = value.len() as u16;
        let bytes = std::iter::empty()
            .chain(key_overlap_len.to_le_bytes())
            .chain(rest_key_len.to_le_bytes())
            .chain(rest_of_key.iter().cloned())
            .chain(value_len.to_le_bytes())
            .chain(value.iter().cloned());
        self.data.extend(bytes);

        if is_first_key {
            assert_eq!(key.len(), rest_of_key.len());
            self.first_key = KeyVec::from_vec(rest_of_key.to_vec());
        }

        let size = self.size();
        is_first_key || size < self.block_size
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        assert_eq!(self.offsets.is_empty(), self.data.is_empty());
        assert_eq!(self.first_key.is_empty(), self.data.is_empty());
        self.data.is_empty()
    }

    /// Finalize the block.
    pub fn build(self) -> Block {
        Block {
            data: self.data,
            offsets: self.offsets,
        }
    }

    pub fn size(&self) -> usize {
        self.data.len()
    }
}
