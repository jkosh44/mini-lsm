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
        let mut first_key_short_circuit = false;
        if self.is_empty() {
            self.first_key = key.to_key_vec();
            first_key_short_circuit = true;
        }

        self.offsets.push(self.data.len() as u16);
        let key_len = key.len() as u16;
        let value_len = value.len() as u16;
        let bytes = std::iter::empty()
            .chain(key_len.to_le_bytes())
            .chain(key.raw_ref().iter().cloned())
            .chain(value_len.to_le_bytes())
            .chain(value.iter().cloned());
        self.data.extend(bytes);

        let size = self.size();
        first_key_short_circuit || size < self.block_size
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

    fn size(&self) -> usize {
        // data
        self.data.len() +
            // offsets
            (self.offsets.len() * size_of::<u16>()) +
            // num_elements
            size_of::<u16>()
    }
}
