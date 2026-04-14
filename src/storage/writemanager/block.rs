use std::{
    borrow::Cow,
    io::{Cursor, Read, Seek, SeekFrom},
};

use tokio::io::AsyncReadExt;
use tokio::io::{AsyncRead, AsyncSeek};

use crate::{
    error::DBError,
    storage::{
        constant::SSTABLE_BLOCK_SIZE,
        record::{MemtableRecord, Record},
    },
};

pub enum BlockBuilderState {
    EnoughSpace,
    Full(Block, Vec<u8>),
}

/// A fixed-size block (4KB) containing sorted key-value records
/// Each block has a header with metadata and contains multiple records
#[derive(Debug, Clone)]
pub struct Block {
    /// Offset of this block in the SSTable file
    pub offset: u64,
    /// First key in this block (for sparse index)
    pub first_key: Vec<u8>,
    /// Last key in this block (for range checks)
    pub last_key: Vec<u8>,
    /// Number of records in this block
    pub record_count: u32,
    /// Actual size of data in this block (may be less than SSTABLE_BLOCK_SIZE)
    pub data_size: u32,

    /// Add this when decode the data / load the data
    pub data: Option<Vec<MemtableRecord>>,
}

/// Builder for creating fixed-size blocks
/// Tracks size and manages the 4KB limit
pub struct BlockBuilder {
    /// Current block data
    data: Vec<MemtableRecord>,
    data_size: usize,
    /// First key in current block (empty if no records yet)
    first_key: Option<Vec<u8>>,
    /// Last key added to current block
    last_key: Option<Vec<u8>>,
    /// Number of records in current block
    record_count: u32,
    /// Starting offset for current block
    block_offset: u64,
}

impl BlockBuilder {
    pub fn new(block_offset: u64) -> Self {
        BlockBuilder {
            data: Vec::new(),
            data_size: 0,
            first_key: None,
            last_key: None,
            record_count: 0,
            block_offset,
        }
    }

    /// Try to add a record to the current block
    /// The consumer need the offset of the block to know where to write the block in the SSTable file, so we need to pass the offset when create the BlockBuilder
    pub fn add_record(&mut self, key: &Vec<u8>, record: &MemtableRecord) -> BlockBuilderState {
        // Check if adding this record would exceed block size
        if self.data_size + record.record_length(key) > SSTABLE_BLOCK_SIZE {
            let records = std::mem::take(&mut self.data);

            let block = Block {
                offset: self.block_offset,
                first_key: self.first_key.clone().unwrap_or_default(),
                last_key: self.last_key.clone().unwrap_or_default(),
                record_count: self.record_count,
                data_size: self.data_size as u32,
                data: Some(records),
            };

            // finalize
            self.first_key = None;
            self.last_key = None;
            self.record_count = 0;
            self.data_size = 0;

            let mut block_data = Vec::new();
            block_data.extend_from_slice(&block.encode());

            self.block_offset = self
                .block_offset
                .checked_add(block_data.len() as u64)
                .expect("block offset overflow");

            return BlockBuilderState::Full(block, block_data);
        }

        // first_key need to be the smallest key in the block.
        if self.first_key.is_none() {
            self.first_key = Some(key.clone());
        }
        if key < self.first_key.as_ref().unwrap_or(&vec![]) {
            self.first_key = Some(key.clone());
        }

        // last_key need to be the largest key in the block.
        if key > self.last_key.as_ref().unwrap_or(&vec![]) {
            self.last_key = Some(key.clone());
        }

        // Add record to block
        self.data.push(MemtableRecord {
            value: record.value.clone(),
            record_type: record.record_type,
            key: key.clone(),
            lsn: record.lsn,
        });
        self.record_count += 1;
        self.data_size += record.record_length(key);

        BlockBuilderState::EnoughSpace
    }

    /// Finalize the current block and return its metadata and data
    pub fn build(self) -> Option<(Block, Vec<u8>)> {
        if self.data.is_empty() {
            return None;
        }

        let block = Block {
            offset: self.block_offset,
            first_key: self.first_key.unwrap(),
            last_key: self.last_key.unwrap(),
            record_count: self.record_count,
            data_size: self.data_size as u32,
            data: Some(self.data),
        };

        Some((block, vec![]))
    }

    /// Check if block is empty
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Get current size of block
    pub fn size(&self) -> usize {
        self.data.len()
    }
}

impl Block {
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::new();

        // Encode first key length and first key
        let first_key_len = self.first_key.len() as u32;
        buf.extend_from_slice(&first_key_len.to_be_bytes());
        buf.extend_from_slice(&self.first_key);

        // Encode last key length and last key
        let last_key_len = self.last_key.len() as u32;
        buf.extend_from_slice(&last_key_len.to_be_bytes());
        buf.extend_from_slice(&self.last_key);

        // Encode record count
        buf.extend_from_slice(&self.record_count.to_be_bytes());

        // Encode data size
        buf.extend_from_slice(&self.data_size.to_be_bytes());

        if let Some(records) = &self.data {
            for record in records {
                buf.extend_from_slice(&record.encode(&record.key));
            }
        }

        buf
    }

    pub fn decode<T: Read + Seek>(mut data: &mut T, offset: u64) -> Result<Self, std::io::Error> {
        // 1. Move the cursor (purely for state consistency, though we use slice offsets)
        data.seek(SeekFrom::Start(offset))?;

        // 2. Decode First Key
        let mut len_buf = [0u8; 4];
        data.read_exact(&mut len_buf)?;
        let first_key_len = u32::from_be_bytes(len_buf) as usize;

        let mut first_key = vec![0u8; first_key_len];
        data.read_exact(&mut first_key)?;

        // 3. Decode Last Key
        data.read_exact(&mut len_buf)?;
        let last_key_len = u32::from_be_bytes(len_buf) as usize;

        let mut last_key = vec![0u8; last_key_len];
        data.read_exact(&mut last_key)?;

        // 4. Decode record_count and data_size
        // Both are u32 as per the struct definition
        data.read_exact(&mut len_buf)?;
        let record_count = u32::from_be_bytes(len_buf);

        data.read_exact(&mut len_buf)?;
        let data_size = u32::from_be_bytes(len_buf);

        let mut records = Vec::with_capacity(record_count as usize);
        for _ in 0..record_count {
            let record = MemtableRecord::decode(&mut data)?;
            records.push(record);
        }

        Ok(Block {
            offset,
            first_key: first_key.to_vec(),
            last_key: last_key.to_vec(),
            record_count,
            data_size,
            data: Some(records),
        })
    }

    pub async fn async_decode<T: AsyncRead + AsyncSeek + Unpin>(
        mut data: &mut T,
    ) -> Result<Self, std::io::Error> {
        // 1. Decode First Key
        let mut len_buf = [0u8; 4];
        data.read_exact(&mut len_buf).await?;
        let first_key_len = u32::from_be_bytes(len_buf) as usize;
        log::debug!("Decoding block: first_key_len = {}", first_key_len);

        let mut first_key = vec![0u8; first_key_len];
        data.read_exact(&mut first_key).await?;
        log::debug!(
            "Decoding block: first_key = {:?}",
            String::from_utf8_lossy(&first_key)
        );

        // 2. Decode Last Key
        data.read_exact(&mut len_buf).await?;
        let last_key_len = u32::from_be_bytes(len_buf) as usize;

        let mut last_key = vec![0u8; last_key_len];
        data.read_exact(&mut last_key).await?;

        // 3. Decode record_count and data_size
        // Both are u32 as per the struct definition
        data.read_exact(&mut len_buf).await?;
        let record_count = u32::from_be_bytes(len_buf);

        data.read_exact(&mut len_buf).await?;
        let data_size = u32::from_be_bytes(len_buf);

        let mut records = Vec::with_capacity(record_count as usize);
        log::debug!(
            "Decoding block with record_count: {}, data_size: {}",
            record_count,
            data_size
        );
        for _ in 0..record_count {
            let record = MemtableRecord::async_decode(&mut data).await?;
            records.push(record);
        }

        Ok(Block {
            offset: 0, // async decode doesn't have offset context, set to 0 or handle differently if needed
            first_key: first_key.to_vec(),
            last_key: last_key.to_vec(),
            record_count,
            data_size,
            data: Some(records),
        })
    }
}

impl Iterator for Block {
    type Item = MemtableRecord;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(records) = &mut self.data {
            if !records.is_empty() {
                return Some(records.remove(0));
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::record::RecordType;

    #[test]
    fn test_block_builder_initial_state() {
        // Test that BlockBuilder starts in empty state
        let builder = BlockBuilder::new(0);

        assert!(builder.is_empty());
        assert_eq!(builder.size(), 0);
    }

    #[test]
    fn test_block_builder_add_record() {
        // Test adding a single record to the block builder
        let mut builder = BlockBuilder::new(0);

        let key1 = b"key1";
        let memtable_record = MemtableRecord::new(b"value1".to_vec(), RecordType::Put, 1000);

        let result = builder.add_record(&key1.to_vec(), &memtable_record);
        assert!(matches!(result, BlockBuilderState::EnoughSpace));

        assert!(!builder.is_empty());
        assert_eq!(
            builder.size(),
            memtable_record.record_length(&key1.to_vec())
        );
    }

    #[test]
    fn test_block_builder_add_multiple_records() {
        // Test adding multiple records to the block builder
        let mut builder = BlockBuilder::new(0);

        // Add first record
        let key1 = b"key1";
        let record1 = MemtableRecord::new(b"value1".to_vec(), RecordType::Put, 1000);
        let result = builder.add_record(&key1.to_vec(), &record1);
        assert!(matches!(result, BlockBuilderState::EnoughSpace));

        // Add second record
        let key2 = b"key2";
        let record2 = MemtableRecord::new(b"value2".to_vec(), RecordType::Put, 2000);
        let result = builder.add_record(&key2.to_vec(), &record2);
        assert!(matches!(result, BlockBuilderState::EnoughSpace));

        // Verify record count
        assert_eq!(
            builder.size(),
            record1.record_length(&key1.to_vec()) + record2.record_length(&key2.to_vec())
        );
    }

    #[test]
    fn test_block_builder_block_full() {
        // Test that adding a large record triggers BlockBuilderState::Full
        let mut builder = BlockBuilder::new(0);

        let key1 = b"key1";
        let memtable_record = MemtableRecord::new(b"value1".to_vec(), RecordType::Put, 1000);
        assert!(matches!(
            builder.add_record(&key1.to_vec(), &memtable_record),
            BlockBuilderState::EnoughSpace
        ));

        // Add a large record that exceeds block size
        let key2 = b"key2";
        let large_value = vec![0u8; SSTABLE_BLOCK_SIZE];
        let large_record = MemtableRecord::new(large_value, RecordType::Put, 2000);

        let result = builder.add_record(&key2.to_vec(), &large_record);
        assert!(matches!(result, BlockBuilderState::Full(_, _)));

        if let BlockBuilderState::Full(block, block_data) = result {
            assert_eq!(block.offset, 0);
            assert_eq!(block.first_key, key1.to_vec());
            assert_eq!(block.last_key, key1.to_vec());
            assert_eq!(block.record_count, 1);
            assert!(!block_data.is_empty());
            // block_data = header + payload
            assert_eq!(
                block_data.len(),
                block.encode().len() + block.data_size as usize
            );
        }
    }

    #[test]
    fn test_block_builder_build() {
        // Test building a block with records
        let mut builder = BlockBuilder::new(0);

        let key1 = b"key1";
        let memtable_record = MemtableRecord::new(b"value1".to_vec(), RecordType::Put, 1000);
        assert!(matches!(
            builder.add_record(&key1.to_vec(), &memtable_record),
            BlockBuilderState::EnoughSpace
        ));

        let result = builder.build();
        assert!(result.is_some());

        let (block, data) = result.unwrap();

        assert_eq!(block.offset, 0);
        assert_eq!(block.first_key, key1.to_vec());
        assert_eq!(block.last_key, key1.to_vec());
        assert_eq!(block.record_count, 1);
        assert!(!data.is_empty());
    }

    #[test]
    fn test_block_builder_build_empty() {
        // Test building an empty block returns None
        let builder = BlockBuilder::new(0);

        let result = builder.build();
        assert!(result.is_none());
    }

    #[test]
    fn test_block_builder_with_non_zero_offset() {
        // Test that BlockBuilder correctly uses the provided offset
        let test_offset = 8192u64;
        let builder = BlockBuilder::new(test_offset);

        assert!(builder.is_empty());
        assert_eq!(builder.size(), 0);
    }

    #[test]
    fn test_block_builder_first_last_key_tracking() {
        // Test that first_key and last_key are correctly tracked
        let mut builder = BlockBuilder::new(0);

        let key1 = b"aaa";
        let record1 = MemtableRecord::new(b"value1".to_vec(), RecordType::Put, 1000);
        assert!(matches!(
            builder.add_record(&key1.to_vec(), &record1),
            BlockBuilderState::EnoughSpace
        ));

        let key2 = b"mmm";
        let record2 = MemtableRecord::new(b"value2".to_vec(), RecordType::Put, 2000);
        assert!(matches!(
            builder.add_record(&key2.to_vec(), &record2),
            BlockBuilderState::EnoughSpace
        ));

        let key3 = b"zzz";
        let record3 = MemtableRecord::new(b"value3".to_vec(), RecordType::Put, 3000);
        assert!(matches!(
            builder.add_record(&key3.to_vec(), &record3),
            BlockBuilderState::EnoughSpace
        ));

        let (block, _) = builder.build().unwrap();

        assert_eq!(block.first_key, b"aaa".to_vec());
        assert_eq!(block.last_key, b"zzz".to_vec());
        assert_eq!(block.record_count, 3);
    }

    #[test]
    fn test_block_builder_with_delete_records() {
        // Test that Delete (tombstone) records work correctly
        let mut builder = BlockBuilder::new(0);

        let key1 = b"key1";
        let put_record = MemtableRecord::new(b"value1".to_vec(), RecordType::Put, 1000);
        assert!(matches!(
            builder.add_record(&key1.to_vec(), &put_record),
            BlockBuilderState::EnoughSpace
        ));

        let key2 = b"key2";
        let delete_record = MemtableRecord::new(Vec::new(), RecordType::Delete, 2000);
        assert!(matches!(
            builder.add_record(&key2.to_vec(), &delete_record),
            BlockBuilderState::EnoughSpace
        ));

        let (block, _) = builder.build().unwrap();

        assert_eq!(block.record_count, 2);
    }

    #[test]
    fn test_block_builder_size_calculation() {
        // Test that size() returns correct accumulated size
        let mut builder = BlockBuilder::new(0);

        let key = b"test_key";
        let value = b"test_value";
        let record = MemtableRecord::new(value.to_vec(), RecordType::Put, 1000);

        let record_len = record.record_length(&key.to_vec());
        assert!(matches!(
            builder.add_record(&key.to_vec(), &record),
            BlockBuilderState::EnoughSpace
        ));

        assert_eq!(builder.size(), record_len);
    }

    #[test]
    fn test_block_encode_header() {
        // Test Block::encode produces correct header
        let block = Block {
            offset: 0,
            first_key: b"first".to_vec(),
            last_key: b"last".to_vec(),
            record_count: 5,
            data_size: 100,
            data: None,
        };

        let encoded = block.encode();

        // Verify header structure:
        // first_key_len (4 bytes) + first_key + last_key_len (4 bytes) + last_key + record_count (4 bytes) + data_size (4 bytes)
        assert_eq!(encoded.len(), 4 + 5 + 4 + 4 + 4 + 4); // 25 bytes

        // Parse and verify
        let first_key_len =
            u32::from_be_bytes([encoded[0], encoded[1], encoded[2], encoded[3]]) as usize;
        assert_eq!(first_key_len, 5);

        let last_key_len =
            u32::from_be_bytes([encoded[9], encoded[10], encoded[11], encoded[12]]) as usize;
        assert_eq!(last_key_len, 4);

        let record_count = u32::from_be_bytes([encoded[17], encoded[18], encoded[19], encoded[20]]);
        assert_eq!(record_count, 5);

        let data_size = u32::from_be_bytes([encoded[21], encoded[22], encoded[23], encoded[24]]);
        assert_eq!(data_size, 100);
    }

    #[test]
    fn test_block_with_various_key_lengths() {
        // Test Block metadata with various key sizes
        let test_cases = vec![
            (b"k".to_vec(), b"key".to_vec(), 1),
            (b"short".to_vec(), b"key".to_vec(), 5),
            (vec![b'x'; 100], b"key".to_vec(), 100),
        ];

        for (first_key, last_key, key_len) in test_cases {
            let block = Block {
                offset: 0,
                first_key: first_key.clone(),
                last_key: last_key.clone(),
                record_count: 1,
                data_size: 50,
                data: None,
            };

            let encoded = block.encode();
            assert!(!encoded.is_empty());

            // Verify first_key is correctly encoded
            let encoded_first_key_len =
                u32::from_be_bytes([encoded[0], encoded[1], encoded[2], encoded[3]]) as usize;
            assert_eq!(encoded_first_key_len, key_len);
        }
    }
}
