use std::io::{Read, Seek, SeekFrom};
use std::time::{SystemTime, UNIX_EPOCH};

use tokio::io::{AsyncRead, AsyncSeek};

/// Get current Unix timestamp in milliseconds
pub fn current_timestamp_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_millis() as u64
}

#[repr(u8)]
#[derive(Debug, PartialEq, Clone, Copy)]
pub enum RecordType {
    Put = 1,
    Delete = 2,
}

/// Represents a key-value record with metadata
#[derive(Debug, Clone)]
pub struct Record {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub record_type: RecordType,
    pub timestamp: u64, // Unix timestamp in milliseconds
    pub offset: u64,
}

impl Record {
    pub fn new(key: Vec<u8>, value: Vec<u8>, record_type: RecordType, timestamp: u64) -> Self {
        Self {
            key,
            value,
            record_type,
            timestamp,
            offset: 0,
        }
    }

    /// Create a tombstone record (delete marker)
    pub fn tombstone(key: Vec<u8>, timestamp: u64) -> Self {
        Self {
            key,
            value: Vec::new(),
            record_type: RecordType::Delete,
            timestamp,
            offset: 0,
        }
    }

    /// Encode the record to bytes
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(1 + 8 + 8 + 8 + self.key.len() + self.value.len() + 4);

        let checksum = crc32fast::hash(&self.value);

        buf.extend_from_slice(&(self.record_type as u8).to_be_bytes());
        buf.extend_from_slice(&self.timestamp.to_be_bytes());
        buf.extend_from_slice(&(self.key.len() as u64).to_be_bytes());
        buf.extend_from_slice(&(self.value.len() as u64).to_be_bytes());
        buf.extend_from_slice(&self.key);
        buf.extend_from_slice(&self.value);
        buf.extend_from_slice(&checksum.to_be_bytes());

        buf
    }

    /// Decode a record from a reader at a specific offset
    pub fn decode<T: Read + Seek>(reader: &mut T) -> Result<Record, std::io::Error> {
        let mut record_type_buf = [0u8; 1];
        reader.read_exact(&mut record_type_buf)?;
        let record_type_byte = record_type_buf[0];

        let mut u64_buf = [0u8; 8];
        reader.read_exact(&mut u64_buf)?;
        let timestamp = u64::from_be_bytes(u64_buf);

        reader.read_exact(&mut u64_buf)?;
        let key_len = u64::from_be_bytes(u64_buf) as usize;

        reader.read_exact(&mut u64_buf)?;
        let value_len = u64::from_be_bytes(u64_buf) as usize;

        let mut key_buf = vec![0u8; key_len];
        reader.read_exact(&mut key_buf)?;

        let mut value_buf = vec![0u8; value_len];
        reader.read_exact(&mut value_buf)?;

        let mut checksum_buf = [0u8; 4];
        reader.read_exact(&mut checksum_buf)?;
        let checksum = u32::from_be_bytes(checksum_buf);

        if crc32fast::hash(&value_buf) != checksum {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Checksum mismatch",
            ));
        }

        let record_type = match record_type_byte {
            1 => RecordType::Put,
            2 => RecordType::Delete,
            _ => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Invalid record type",
                ));
            }
        };

        let next_offset = (1 + 8 + 8 + 8 + key_len + value_len + 4) as u64;

        Ok(Self {
            key: key_buf, // allocate new Vec for key
            value: value_buf,
            record_type,
            timestamp,
            offset: next_offset,
        })
    }
}

// Trait implementations for Record ordering and equality
// Note: Ord/PartialOrd compare by key first, then timestamp (descending - newer first)
// This is the typical LSM-tree pattern where we want the latest version of a key
// PartialEq/Eq compare all fields for strict equality

impl PartialEq for Record {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
            && self.value == other.value
            && self.record_type == other.record_type
            && self.timestamp == other.timestamp
    }
}

impl Eq for Record {}

impl PartialOrd for Record {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Record {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // compare the key first.

        let res = self.key.cmp(&other.key);
        if res != std::cmp::Ordering::Equal {
            return res;
        }

        // If keys are equal, compare timestamps in descending order (newer first)
        other.timestamp.cmp(&self.timestamp)
    }
}

// ============================================================================
// Backward Compatibility Layer for WAL operations
// ============================================================================

/// Result structure for decode_record function (backward compatibility)
/// This represents a decoded WAL record with LSN
#[derive(Debug)]
pub struct DecodeRecordResult {
    pub key: Vec<u8>,
    pub val: Vec<u8>,
    pub record_type: RecordType,
    pub lsn: u64,
    pub offset: u64,
}

/// Decode a WAL record from a reader at a specific offset (backward compatibility)
/// This function reads WAL format records which include LSN
pub fn decode_record<R: Read + Seek>(
    mut reader: R,
    offset: u64,
) -> Result<DecodeRecordResult, std::io::Error> {
    reader.seek(SeekFrom::Start(offset))?;

    // Read record type
    let mut record_type_buf = [0u8; 1];
    reader.read_exact(&mut record_type_buf)?;
    let record_type = match record_type_buf[0] {
        1 => RecordType::Put,
        2 => RecordType::Delete,
        _ => {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Invalid record type",
            ));
        }
    };

    // Read LSN
    let mut lsn_buf = [0u8; 8];
    reader.read_exact(&mut lsn_buf)?;
    let lsn = u64::from_be_bytes(lsn_buf);

    let mut timestamp_buf = [0u8; 8];
    reader.read_exact(&mut timestamp_buf)?;

    // Read key length
    let mut len_buf = [0u8; 8];
    reader.read_exact(&mut len_buf)?;
    let key_len = u64::from_be_bytes(len_buf) as usize;

    // Read value length
    reader.read_exact(&mut len_buf)?;
    let value_len = u64::from_be_bytes(len_buf) as usize;

    // Read key
    let mut key = vec![0u8; key_len];
    reader.read_exact(&mut key)?;

    // Read value
    let mut value = vec![0u8; value_len];
    reader.read_exact(&mut value)?;

    // Read and verify checksum
    let mut checksum_buf = [0u8; 4];
    reader.read_exact(&mut checksum_buf)?;
    let checksum = u32::from_be_bytes(checksum_buf);

    if crc32fast::hash(&value) != checksum {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "Checksum mismatch",
        ));
    }

    // Calculate next offset
    let next_offset = offset + 1 + 8 + 8 + 8 + key_len as u64 + value_len as u64 + 4;

    Ok(DecodeRecordResult {
        key,
        val: value,
        record_type,
        lsn,
        offset: next_offset,
    })
}

#[derive(Debug, Clone)]
pub struct MemtableRecord {
    pub record_type: RecordType,
    pub lsn: u64,
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

impl MemtableRecord {
    pub fn new(value: Vec<u8>, record_type: RecordType, lsn: u64) -> Self {
        Self {
            key: vec![],
            value,
            record_type,
            lsn,
        }
    }

    // only use this when storing the record in sstable.
    pub fn with_key(mut self, key: Vec<u8>) -> Self {
        self.key = key;
        self
    }

    pub fn encode(&self, key: &Vec<u8>) -> Vec<u8> {
        let mut encoded = Vec::new();
        encoded.extend_from_slice(&(key.len() as u32).to_le_bytes());
        encoded.extend_from_slice(&key);
        encoded.extend_from_slice(&(self.value.len() as u32).to_le_bytes());
        encoded.extend_from_slice(&self.value);
        encoded.push(self.record_type as u8);
        encoded.extend_from_slice(&self.lsn.to_le_bytes());

        encoded
    }

    pub fn record_length(&self, key: &Vec<u8>) -> usize {
        4 + key.len() + 4 + self.value.len() + 1 + 8
    }

    pub fn decode<T: Read + Seek>(encoded: T) -> Result<Self, std::io::Error> {
        let mut reader = encoded;

        let mut key_len_buf = [0u8; 4];
        reader.read_exact(&mut key_len_buf)?;
        let key_len = u32::from_le_bytes(key_len_buf) as usize;

        let mut key_buf = vec![0u8; key_len];
        reader.read_exact(&mut key_buf)?;

        let mut value_len_buf = [0u8; 4];
        reader.read_exact(&mut value_len_buf)?;
        let value_len = u32::from_le_bytes(value_len_buf) as usize;

        let mut value_buf = vec![0u8; value_len];
        reader.read_exact(&mut value_buf)?;

        let mut record_type_buf = [0u8; 1];
        reader.read_exact(&mut record_type_buf)?;
        let record_type = match record_type_buf[0] {
            1 => RecordType::Put,
            2 => RecordType::Delete,
            _ => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Invalid record type",
                ));
            }
        };

        let mut lsn_buf = [0u8; 8];
        reader.read_exact(&mut lsn_buf)?;
        let lsn = u64::from_le_bytes(lsn_buf);

        Ok(Self {
            key: key_buf,
            value: value_buf,
            record_type,
            lsn,
        })
    }

    pub async fn async_decode<T: AsyncRead + AsyncSeek + Unpin>(
        mut reader: T,
    ) -> Result<Self, std::io::Error> {
        use tokio::io::AsyncReadExt;

        let mut key_len_buf = [0u8; 4];
        reader.read_exact(&mut key_len_buf).await?;
        let key_len = u32::from_le_bytes(key_len_buf) as usize;

        let mut key_buf = vec![0u8; key_len];
        reader.read_exact(&mut key_buf).await?;

        let mut value_len_buf = [0u8; 4];
        reader.read_exact(&mut value_len_buf).await?;
        let value_len = u32::from_le_bytes(value_len_buf) as usize;

        let mut value_buf = vec![0u8; value_len];
        reader.read_exact(&mut value_buf).await?;

        let mut record_type_buf = [0u8; 1];
        reader.read_exact(&mut record_type_buf).await?;
        let record_type = match record_type_buf[0] {
            1 => RecordType::Put,
            2 => RecordType::Delete,
            _ => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Invalid record type",
                ));
            }
        };

        let mut lsn_buf = [0u8; 8];
        reader.read_exact(&mut lsn_buf).await?;
        let lsn = u64::from_le_bytes(lsn_buf);

        Ok(Self {
            key: key_buf,
            value: value_buf,
            record_type,
            lsn,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_record_tombstone() {
        // Positive test: Verifies that tombstone (delete) records can be created and decoded correctly
        let key = b"deleted_key".to_vec();
        let timestamp = 1234567890123_u64;
        let record = Record::tombstone(key, timestamp);
        let encoded = record.encode();

        let decoded = Record::decode(&mut Cursor::new(encoded)).unwrap();

        assert_eq!(decoded.key, b"deleted_key");
        assert_eq!(decoded.value, b"");
        assert_eq!(decoded.record_type, RecordType::Delete);
    }

    #[test]
    fn test_record_decode_calculates_offset() {
        // Positive test: Verifies that the next record offset is calculated correctly after decoding
        let key = b"test".to_vec();
        let value = b"data".to_vec();
        let timestamp = 1234567890123_u64;
        let record = Record::new(key, value, RecordType::Put, timestamp);
        let encoded = record.encode();

        let decoded = Record::decode(&mut Cursor::new(encoded)).unwrap();

        // Offset should be: 1 (type) + 8 (timestamp) + 8 (key_len) + 8 (val_len) + 4 (key) + 4 (val) + 4 (checksum) = 37
        assert_eq!(decoded.offset, 37);
    }

    #[test]
    fn test_record_checksum_validation() {
        // Negative test: Verifies that corrupted data is detected via checksum validation
        let key = b"key".to_vec();
        let value = b"value".to_vec();
        let timestamp = 1234567890123_u64;
        let record = Record::new(key, value, RecordType::Put, timestamp);
        let mut encoded = record.encode();

        // Corrupt the value section (not the metadata)
        // Format: 1 (type) + 8 (timestamp) + 8 (key_len) + 8 (val_len) + 3 (key) + 5 (value) + 4 (checksum)
        // Value starts at offset 28 (1 + 8 + 8 + 8 + 3), so corrupt the first byte of value
        let value_offset = 1 + 8 + 8 + 8 + 3; // After metadata and key
        encoded[value_offset] ^= 0xFF;

        let result = Record::decode(&mut Cursor::new(encoded));
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), std::io::ErrorKind::InvalidData);
    }

    #[test]
    fn test_record_timestamp_encoding() {
        // Positive test: Verifies that timestamp is preserved through encode/decode cycle
        let key = b"timestamped_key".to_vec();
        let value = b"timestamped_value".to_vec();
        let timestamp = 1234567890123_u64;
        let record = Record::new(key, value, RecordType::Put, timestamp);
        let encoded = record.encode();

        let decoded = Record::decode(&mut Cursor::new(encoded)).unwrap();

        assert_eq!(decoded.timestamp, timestamp);
        assert_eq!(decoded.key, b"timestamped_key");
        assert_eq!(decoded.value, b"timestamped_value");
    }

    #[test]
    fn test_record_timestamp_extreme_values() {
        // Positive test: Verifies that extreme timestamp values (0 and u64::MAX) are handled correctly
        let key = b"extreme_key".to_vec();
        let value = b"extreme_value".to_vec();

        // Test with timestamp = 0
        let record_min = Record::new(key.clone(), value.clone(), RecordType::Put, 0);
        let encoded_min = record_min.encode();
        let decoded_min = Record::decode(&mut Cursor::new(encoded_min)).unwrap();
        assert_eq!(decoded_min.timestamp, 0);

        // Test with timestamp = u64::MAX
        let record_max = Record::new(key, value, RecordType::Put, u64::MAX);
        let encoded_max = record_max.encode();
        let decoded_max = Record::decode(&mut Cursor::new(encoded_max)).unwrap();
        assert_eq!(decoded_max.timestamp, u64::MAX);
    }

    #[test]
    fn test_record_ordering_by_key() {
        // Positive test: Verifies that records with different keys are ordered correctly (lexicographically)
        let timestamp = 1234567890123_u64;
        let record_a = Record::new(
            b"apple".to_vec(),
            b"value1".to_vec(),
            RecordType::Put,
            timestamp,
        );
        let record_b = Record::new(
            b"banana".to_vec(),
            b"value2".to_vec(),
            RecordType::Put,
            timestamp,
        );
        let record_c = Record::new(
            b"cherry".to_vec(),
            b"value3".to_vec(),
            RecordType::Put,
            timestamp,
        );

        assert!(record_a < record_b);
        assert!(record_b < record_c);
        assert!(record_a < record_c);
    }

    #[test]
    fn test_record_ordering_by_timestamp() {
        // Positive test: Verifies that records with same key are ordered by timestamp descending (newer first)
        let key = b"same_key".to_vec();
        let record_old = Record::new(key.clone(), b"old_value".to_vec(), RecordType::Put, 1000);
        let record_mid = Record::new(key.clone(), b"mid_value".to_vec(), RecordType::Put, 2000);
        let record_new = Record::new(key, b"new_value".to_vec(), RecordType::Put, 3000);

        // Newer timestamps should come BEFORE older ones (descending order)
        assert!(record_new < record_mid);
        assert!(record_mid < record_old);
        assert!(record_new < record_old);
    }

    #[test]
    fn test_record_equality_same_timestamp() {
        // Positive test: Verifies that identical records (including timestamp) are considered equal
        let key = b"eq_key".to_vec();
        let value = b"eq_value".to_vec();
        let timestamp = 1234567890123_u64;

        let record1 = Record::new(key.clone(), value.clone(), RecordType::Put, timestamp);
        let record2 = Record::new(key, value, RecordType::Put, timestamp);

        assert_eq!(record1, record2);
    }

    #[test]
    fn test_record_equality_different_timestamp() {
        // Negative test: Verifies that records differing only in timestamp are NOT considered equal
        let key = b"eq_key".to_vec();
        let value = b"eq_value".to_vec();

        let record1 = Record::new(key.clone(), value.clone(), RecordType::Put, 1000);
        let record2 = Record::new(key, value, RecordType::Put, 2000);

        assert_ne!(record1, record2);
    }
}
