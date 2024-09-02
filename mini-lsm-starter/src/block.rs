mod builder;
mod iterator;

pub use builder::BlockBuilder;
use bytes::Bytes;
pub use iterator::BlockIterator;

const NUM_ELEMENTS_SIZE: usize = size_of::<u16>();
const OFFSET_SIZE: usize = size_of::<u16>();

/// A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted key-value pairs.
pub struct Block {
    pub(crate) data: Vec<u8>,
    pub(crate) offsets: Vec<u16>,
}

impl Block {
    /// Encode the internal data to the data layout illustrated in the tutorial
    /// Note: You may want to recheck if any of the expected field is missing from your output
    pub fn encode(&self) -> Bytes {
        let num_elements = self.offsets.len() as u16;
        self.data
            .iter()
            .cloned()
            .chain(
                self.offsets
                    .iter()
                    .flat_map(|offset| offset.to_le_bytes().into_iter()),
            )
            .chain(num_elements.to_le_bytes())
            .collect()
    }

    /// Decode from the data layout, transform the input `data` to a single `Block`
    pub fn decode(data: &[u8]) -> Self {
        let n = data.len();
        let offset = n - NUM_ELEMENTS_SIZE;
        let (data, num_elements) = (&data[..offset], &data[offset..]);
        let n = data.len();
        let num_elements = u16::from_le_bytes(num_elements.try_into().expect("correct size"));
        let offset = n - (num_elements as usize * OFFSET_SIZE);
        let (data, offsets) = (&data[..offset], &data[offset..]);
        let offsets: Vec<_> = offsets
            .chunks(OFFSET_SIZE)
            .map(|offset| u16::from_le_bytes(offset.try_into().expect("correct size")))
            .collect();
        let data = data.to_vec();

        Self { data, offsets }
    }

    pub(crate) fn num_elements(&self) -> usize {
        self.offsets.len()
    }
}
