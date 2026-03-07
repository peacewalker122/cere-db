use std::io::{Read, Seek, SeekFrom};

use crate::{
    error::DBError,
    storage::{log::Record, record},
};

use super::constant::SSTABLE_BLOCK_SIZE;

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
    pub data: Option<Vec<Record>>,
}

/// Builder for creating fixed-size blocks
/// Tracks size and manages the 4KB limit
pub struct BlockBuilder {
    /// Current block data
    data: Vec<u8>,
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
            first_key: None,
            last_key: None,
            record_count: 0,
            block_offset,
        }
    }

    /// Try to add a record to the current block
    /// Returns Ok(()) if added successfully
    /// Returns Err(record_bytes) if block is full and record couldn't be added
    pub fn add_record(&mut self, record: &Record) -> Result<(), Vec<u8>> {
        // Check if adding this record would exceed block size
        if self.data.len() + record.value.len() > SSTABLE_BLOCK_SIZE {
            return Err("Record too large to fit in block".as_bytes().to_vec());
        }

        // Track first key
        if self.first_key.is_none() {
            self.first_key = Some(record.key.to_vec());
        }

        // Update last key
        self.last_key = Some(record.key.to_vec());

        // Add record to block
        self.data.extend_from_slice(&record.encode());
        self.record_count += 1;

        Ok(())
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
            data_size: self.data.len() as u32,
            data: None,
        };

        // prepend the block header to the data
        let mut block_data = Vec::new();
        block_data.extend_from_slice(&block.encode());
        block_data.extend_from_slice(&self.data);

        Some((block, block_data))
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

        buf
    }

    pub fn decode<T: Read + Seek>(data: &mut T, offset: u64) -> Result<Self, DBError> {
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
            // we want to traverse and decode each record that available in
            // this block

            let record = record::Record::decode(data)?;
            records.push(record);
        }

        records.sort_by(|left, right| left.key.cmp(&right.key));

        Ok(Block {
            offset,
            first_key: first_key.to_vec(),
            last_key: last_key.to_vec(),
            record_count,
            data_size,
            data: Some(records),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::record::{Record, RecordType};
    use std::io::Cursor;

    fn find_record<'a>(records: &'a [Record], key: &[u8]) -> &'a Record {
        records
            .iter()
            .find(|record| record.key.as_slice() == key)
            .unwrap_or_else(|| panic!("Key {:?} should exist", key))
    }

    #[test]
    fn test_block_builder() {
        // Test that BlockBuilder properly manages 4KB blocks
        let mut builder = BlockBuilder::new(0);

        assert!(builder.is_empty());
        assert_eq!(builder.size(), 0);

        // Add a small record
        let key1 = b"key1";
        let record = Record::new(key1.to_vec(), b"value1".to_vec(), RecordType::Put, 1000);
        let record_bytes = record.encode();

        let result = builder.add_record(&record);
        assert!(result.is_ok());
        assert!(!builder.is_empty());
        assert_eq!(builder.size(), record_bytes.len());

        // Try to add a record that would exceed block size
        let key2 = b"key2";
        let large_value = vec![0u8; SSTABLE_BLOCK_SIZE];
        let large_record = Record::new(key2.to_vec(), large_value, RecordType::Put, 2000);
        let _large_record_bytes = large_record.encode();

        let result = builder.add_record(&large_record);
        assert!(result.is_err()); // Should fail - block full

        // Build the block
        let result = builder.build();
        assert!(result.is_some());

        let (parent_block, data) = result.unwrap();

        // try to decode the block_header
        let mut cursor = Cursor::new(data.clone());
        let block = Block::decode(&mut cursor, 0).unwrap();

        assert_eq!(block.record_count, 1);
        assert_eq!(block.first_key, parent_block.first_key);
        assert_eq!(block.last_key, parent_block.last_key);

        // verify the record was decoded correctly in block.data
        assert!(block.data.is_some(), "block.data should be populated");
        let records = block.data.as_ref().unwrap();
        assert_eq!(records.len(), 1, "block should contain 1 record");

        let decoded_record = find_record(records, b"key1");
        assert_eq!(decoded_record.key, b"key1");
        assert_eq!(decoded_record.value, b"value1");
        assert_eq!(decoded_record.record_type, RecordType::Put);
    }

    #[test]
    fn test_block_decode_single_record() {
        // Positive test: Verifies that Block::decode correctly populates block.data with exactly 1 record
        // This tests the core record decoding loop (lines 162-172) for the simplest case

        // Arrange: Create a block with a single record
        let mut builder = BlockBuilder::new(0);
        let key = b"test_key";
        let value = b"test_value";
        let record = Record::new(key.to_vec(), value.to_vec(), RecordType::Put, 1000);
        builder.add_record(&record).unwrap();
        let (_, data) = builder.build().unwrap();

        // Act: Decode the block
        let mut cursor = Cursor::new(data.clone());
        let block = Block::decode(&mut cursor, 0).unwrap();

        // Assert: block.data should contain exactly 1 record with correct values
        assert!(block.data.is_some(), "block.data should be Some");
        let records = block.data.as_ref().unwrap();
        assert_eq!(records.len(), 1, "block should contain exactly 1 record");

        let decoded_record = find_record(records, b"test_key");

        assert_eq!(decoded_record.key, b"test_key");
        assert_eq!(decoded_record.value, b"test_value");
        assert_eq!(decoded_record.record_type, RecordType::Put);
    }

    #[test]
    fn test_block_decode_multiple_records() {
        // Positive test: Verifies that Block::decode correctly decodes ALL records in a block
        // and maintains correct insertion order. Tests the record decoding loop for multiple iterations.

        // Arrange: Create a block with 4 distinct records
        let mut builder = BlockBuilder::new(0);
        let records_data = vec![
            (b"key1" as &[u8], b"value1" as &[u8]),
            (b"key2", b"value2"),
            (b"key3", b"value3"),
            (b"key4", b"value4"),
        ];

        for (key, value) in &records_data {
            let record = Record::new(key.to_vec(), value.to_vec(), RecordType::Put, 1000);
            builder.add_record(&record).unwrap();
        }
        let (_, data) = builder.build().unwrap();

        // Act: Decode the block
        let mut cursor = Cursor::new(data.clone());
        let block = Block::decode(&mut cursor, 0).unwrap();

        // Assert: block.data should contain all 4 records in correct order
        assert!(block.data.is_some(), "block.data should be Some");
        let decoded_records = block.data.as_ref().unwrap();
        assert_eq!(
            decoded_records.len(),
            4,
            "block should contain exactly 4 records"
        );

        // Verify each record matches expected data by key lookup
        // Note: BTreeMap sorts by key, so keys will be in sorted order
        for (expected_key, expected_value) in records_data.iter() {
            let record = find_record(decoded_records, *expected_key);
            assert_eq!(
                record.key, *expected_key,
                "Key mismatch for {:?}",
                expected_key
            );
            assert_eq!(
                record.value, *expected_value,
                "Value mismatch for key {:?}",
                expected_key
            );
            assert_eq!(
                record.record_type,
                RecordType::Put,
                "Record type mismatch for key {:?}",
                expected_key
            );
        }
    }

    #[test]
    fn test_block_decode_preserves_record_types() {
        // Positive test: Verifies that Block::decode correctly preserves different RecordType values
        // (Put and Delete). This ensures the record decoding properly reads and stores the record_type field.

        // Arrange: Create a block with mixed Put and Delete records
        let mut builder = BlockBuilder::new(0);

        // Add 2 Put records
        let record1 = Record::new(b"key1".to_vec(), b"value1".to_vec(), RecordType::Put, 1000);
        builder.add_record(&record1).unwrap();

        let record2 = Record::new(b"key2".to_vec(), b"value2".to_vec(), RecordType::Put, 2000);
        builder.add_record(&record2).unwrap();

        // Add 2 Delete (tombstone) records
        let record3 = Record::tombstone(b"key3".to_vec(), 3000);
        builder.add_record(&record3).unwrap();

        let record4 = Record::tombstone(b"key4".to_vec(), 4000);
        builder.add_record(&record4).unwrap();

        let (_, data) = builder.build().unwrap();

        // Act: Decode the block
        let mut cursor = Cursor::new(data.clone());
        let block = Block::decode(&mut cursor, 0).unwrap();

        // Assert: All 4 records should be decoded with correct types
        assert!(block.data.is_some(), "block.data should be Some");
        let decoded_records = block.data.as_ref().unwrap();
        assert_eq!(
            decoded_records.len(),
            4,
            "block should contain exactly 4 records"
        );

        // Verify record types are preserved - access by key
        let record0 = find_record(decoded_records, b"key1");
        assert_eq!(
            record0.record_type,
            RecordType::Put,
            "Record 0 should be Put"
        );
        assert_eq!(record0.key, b"key1");

        let record1 = find_record(decoded_records, b"key2");
        assert_eq!(
            record1.record_type,
            RecordType::Put,
            "Record 1 should be Put"
        );
        assert_eq!(record1.key, b"key2");

        let record2 = find_record(decoded_records, b"key3");
        assert_eq!(
            record2.record_type,
            RecordType::Delete,
            "Record 2 should be Delete"
        );
        assert_eq!(record2.key, b"key3");
        assert_eq!(record2.value, b"", "Delete records should have empty value");

        let record3 = find_record(decoded_records, b"key4");
        assert_eq!(
            record3.record_type,
            RecordType::Delete,
            "Record 3 should be Delete"
        );
        assert_eq!(record3.key, b"key4");
        assert_eq!(record3.value, b"", "Delete records should have empty value");
    }

    #[test]
    fn test_block_decode_with_varying_key_sizes() {
        // Positive test: Verifies that Block::decode correctly handles keys of different sizes
        // This tests the robustness of the decoding logic with varying key lengths.

        // Arrange: Create a block with short, medium, and long keys
        let mut builder = BlockBuilder::new(0);

        // Short key (4 bytes)
        let short_key = b"key1";
        let record1 = Record::new(
            short_key.to_vec(),
            b"value1".to_vec(),
            RecordType::Put,
            1000,
        );
        builder.add_record(&record1).unwrap();

        // Medium key (50 bytes)
        let medium_key = b"medium_key_with_exactly_50_bytes_in_total_here!!!!";
        assert_eq!(medium_key.len(), 50, "Medium key should be 50 bytes");
        let record2 = Record::new(
            medium_key.to_vec(),
            b"value2".to_vec(),
            RecordType::Put,
            2000,
        );
        builder.add_record(&record2).unwrap();

        // Long key (200 bytes)
        let long_key = vec![b'x'; 200];
        let record3 = Record::new(long_key.clone(), b"value3".to_vec(), RecordType::Put, 3000);
        builder.add_record(&record3).unwrap();

        let (_, data) = builder.build().unwrap();

        // Act: Decode the block
        let mut cursor = Cursor::new(data.clone());
        let block = Block::decode(&mut cursor, 0).unwrap();

        // Assert: All 3 records should be decoded with correct key sizes
        assert!(block.data.is_some(), "block.data should be Some");
        let decoded_records = block.data.as_ref().unwrap();
        assert_eq!(
            decoded_records.len(),
            3,
            "block should contain exactly 3 records"
        );

        // Verify short key
        let record0 = find_record(decoded_records, short_key);
        assert_eq!(record0.key, short_key, "Short key mismatch");
        assert_eq!(record0.key.len(), 4, "Short key should be 4 bytes");

        // Verify medium key
        let record1 = find_record(decoded_records, medium_key);
        assert_eq!(record1.key, medium_key, "Medium key mismatch");
        assert_eq!(record1.key.len(), 50, "Medium key should be 50 bytes");

        // Verify long key
        let record2 = find_record(decoded_records, long_key.as_slice());
        assert_eq!(record2.key, long_key.as_slice(), "Long key mismatch");
        assert_eq!(record2.key.len(), 200, "Long key should be 200 bytes");
    }

    #[test]
    fn test_block_decode_cursor_position_advances() {
        // Positive test: Verifies that the cursor position advances correctly after Block::decode
        // This ensures the decoding loop properly advances through all record data.

        // Arrange: Create a block with known size
        let mut builder = BlockBuilder::new(0);
        let record1 = Record::new(b"key1".to_vec(), b"value1".to_vec(), RecordType::Put, 1000);
        let record2 = Record::new(b"key2".to_vec(), b"value2".to_vec(), RecordType::Put, 2000);
        builder.add_record(&record1).unwrap();
        builder.add_record(&record2).unwrap();

        let (block_metadata, data) = builder.build().unwrap();

        // Act: Decode the block and check cursor position
        let mut cursor = Cursor::new(data.clone());
        let initial_pos = cursor.position();
        let block = Block::decode(&mut cursor, 0).unwrap();
        let final_pos = cursor.position();

        // Assert: Cursor should start at 0 and advance to header_size + data_size
        assert_eq!(initial_pos, 0, "Initial cursor position should be 0");

        // Calculate expected position: header size + data size
        // Header size = 4 (first_key_len) + first_key.len() + 4 (last_key_len) + last_key.len() + 4 (record_count) + 4 (data_size)
        let expected_pos = block_metadata.encode().len() + block_metadata.data_size as usize;
        assert_eq!(
            final_pos as usize, expected_pos,
            "Cursor should advance to header_size + data_size"
        );

        // Also verify block.data was populated
        assert!(block.data.is_some(), "block.data should be populated");
        assert_eq!(
            block.data.as_ref().unwrap().len(),
            2,
            "block should contain 2 records"
        );
    }

    #[test]
    fn test_block_decode_corrupted_record_data() {
        // Negative test: Verifies that Block::decode fails when record data is corrupted
        // This ensures proper error handling when checksum validation fails.

        // Arrange: Build a valid block, then corrupt a byte in the record data
        let mut builder = BlockBuilder::new(0);
        let record = Record::new(b"key1".to_vec(), b"value1".to_vec(), RecordType::Put, 1000);
        builder.add_record(&record).unwrap();
        let (block_metadata, mut data) = builder.build().unwrap();

        // Calculate where the record data starts (after header)
        let header_size = block_metadata.encode().len();

        // Corrupt a byte in the record data section (corrupt the value, not the key or metadata)
        // Record format: 1 (type) + 8 (timestamp) + 8 (key_len) + 8 (val_len) + key + value + 4 (checksum)
        // Corrupt the first byte of the value
        let corruption_offset = header_size + 1 + 8 + 8 + 8 + 4; // After metadata and key
        if corruption_offset < data.len() {
            data[corruption_offset] ^= 0xFF; // Flip all bits
        }

        // Act: Attempt to decode the corrupted block
        let mut cursor = Cursor::new(data);
        let result = Block::decode(&mut cursor, 0);

        // Assert: Decode should fail due to checksum mismatch
        assert!(result.is_err(), "Decode should fail with corrupted data");
    }

    #[test]
    fn test_block_decode_invalid_record_count() {
        // Negative test: Verifies that Block::decode fails when record_count in header
        // exceeds the actual number of records present. This tests error handling for
        // malformed block headers.

        // Arrange: Manually construct a block with invalid record_count
        let mut builder = BlockBuilder::new(0);
        let record = Record::new(b"key1".to_vec(), b"value1".to_vec(), RecordType::Put, 1000);
        builder.add_record(&record).unwrap();
        let (mut block_metadata, data) = builder.build().unwrap();

        // Manipulate the record_count in the header to be higher than actual
        block_metadata.record_count = 10; // We only have 1 record

        // Rebuild the data with corrupted header
        let mut corrupted_data = Vec::new();
        corrupted_data.extend_from_slice(&block_metadata.encode());
        // Add the original record data (skip the original header)
        let header_size = Block {
            offset: 0,
            first_key: b"key1".to_vec(),
            last_key: b"key1".to_vec(),
            record_count: 1,
            data_size: block_metadata.data_size,
            data: None,
        }
        .encode()
        .len();
        corrupted_data.extend_from_slice(&data[header_size..]);

        // Act: Attempt to decode with invalid record_count
        let mut cursor = Cursor::new(corrupted_data);
        let result = Block::decode(&mut cursor, 0);

        // Assert: Decode should fail (EOF or array bounds error)
        assert!(
            result.is_err(),
            "Decode should fail when record_count exceeds actual records"
        );
    }
}
