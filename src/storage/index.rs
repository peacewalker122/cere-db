use std::collections::BTreeMap;
use std::io::{BufReader, Read, Seek, SeekFrom};

use crate::{error::DBError, storage::bloom::BloomFilterWrapper};

use super::footer::SSTableFooter;

/// Index Entry: maps a key to its offset in the data block
#[derive(Debug, Clone)]
pub struct IndexEntry {
    pub key: Vec<u8>,
    pub offset: u64,
}

impl IndexEntry {
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(8 + 8 + self.key.len());
        buf.extend_from_slice(&(self.key.len() as u64).to_be_bytes());
        buf.extend_from_slice(&self.key);
        buf.extend_from_slice(&self.offset.to_be_bytes());
        buf
    }

    pub fn decode<R: Read>(mut reader: R) -> Result<Self, std::io::Error> {
        let mut len_buf = [0u8; 8];
        reader.read_exact(&mut len_buf)?;
        let key_len = u64::from_be_bytes(len_buf) as usize;

        let mut key = vec![0u8; key_len];
        reader.read_exact(&mut key)?;

        let mut offset_buf = [0u8; 8];
        reader.read_exact(&mut offset_buf)?;
        let offset = u64::from_be_bytes(offset_buf);

        Ok(IndexEntry { key, offset })
    }
}

/// Sparse Index Entry: maps the first key of a block to the block's offset
/// This is more efficient for high-cardinality keys (like UUIDs)
#[derive(Debug, Clone)]
pub struct SparseIndexEntry {
    /// First key in the block
    pub first_key: Vec<u8>,
    /// Offset of the block in the file
    pub block_offset: u64,
    /// Last key in the block (for range checking)
    pub last_key: Vec<u8>,
    /// Number of records in this block
    pub record_count: u32,
}

impl SparseIndexEntry {
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::new();

        // Encode first_key
        buf.extend_from_slice(&(self.first_key.len() as u64).to_be_bytes());
        buf.extend_from_slice(&self.first_key);

        // Encode block_offset
        buf.extend_from_slice(&self.block_offset.to_be_bytes());

        // Encode last_key
        buf.extend_from_slice(&(self.last_key.len() as u64).to_be_bytes());
        buf.extend_from_slice(&self.last_key);

        // Encode record_count
        buf.extend_from_slice(&self.record_count.to_be_bytes());

        buf
    }

    pub fn decode<R: Read>(mut reader: R) -> Result<Self, std::io::Error> {
        let mut len_buf = [0u8; 8];

        // Decode first_key
        reader.read_exact(&mut len_buf)?;
        let first_key_len = u64::from_be_bytes(len_buf) as usize;
        let mut first_key = vec![0u8; first_key_len];
        reader.read_exact(&mut first_key)?;

        // Decode block_offset
        reader.read_exact(&mut len_buf)?;
        let block_offset = u64::from_be_bytes(len_buf);

        // Decode last_key
        reader.read_exact(&mut len_buf)?;
        let last_key_len = u64::from_be_bytes(len_buf) as usize;
        let mut last_key = vec![0u8; last_key_len];
        reader.read_exact(&mut last_key)?;

        // Decode record_count
        let mut count_buf = [0u8; 4];
        reader.read_exact(&mut count_buf)?;
        let record_count = u32::from_be_bytes(count_buf);

        Ok(SparseIndexEntry {
            first_key,
            block_offset,
            last_key,
            record_count,
        })
    }

    pub async fn async_decode<R: tokio::io::AsyncRead + Unpin>(
        mut reader: R,
    ) -> Result<Self, std::io::Error> {
        let mut len_buf = [0u8; 8];

        // Decode first_key
        tokio::io::AsyncReadExt::read_exact(&mut reader, &mut len_buf).await?;
        let first_key_len = u64::from_be_bytes(len_buf) as usize;
        let mut first_key = vec![0u8; first_key_len];
        tokio::io::AsyncReadExt::read_exact(&mut reader, &mut first_key).await?;

        // Decode block_offset
        tokio::io::AsyncReadExt::read_exact(&mut reader, &mut len_buf).await?;
        let block_offset = u64::from_be_bytes(len_buf);

        // Decode last_key
        tokio::io::AsyncReadExt::read_exact(&mut reader, &mut len_buf).await?;
        let last_key_len = u64::from_be_bytes(len_buf) as usize;
        let mut last_key = vec![0u8; last_key_len];
        tokio::io::AsyncReadExt::read_exact(&mut reader, &mut last_key).await?;

        // Decode record_count
        let mut count_buf = [0u8; 4];
        tokio::io::AsyncReadExt::read_exact(&mut reader, &mut count_buf).await?;
        let record_count = u32::from_be_bytes(count_buf);

        Ok(SparseIndexEntry {
            first_key,
            block_offset,
            last_key,
            record_count,
        })
    }
}

/// Helper to verify index checksum
pub fn verify_index_checksum(data: &[u8], expected: u32) -> Result<(), std::io::Error> {
    let calculated = crc32fast::hash(data);
    if calculated != expected {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!(
                "Index block checksum mismatch: expected 0x{:X}, got 0x{:X}",
                expected, calculated
            ),
        ));
    }
    Ok(())
}

/// Search for a key in the SSTable using sparse index and linear block scan
/// This is optimized for high-cardinality keys (like UUIDs)
pub fn search_sstable_sparse<R>(
    mut reader: R,
    key: &[u8],
    sparse_index: &[SparseIndexEntry],
) -> Result<Option<Vec<u8>>, DBError>
where
    R: Read + Seek,
{
    use crate::storage::block::Block;
    use crate::storage::record::RecordType;

    // Find the block that might contain the key using binary search
    // We need to find the block where: first_key <= key <= last_key
    let mut target_block: Option<&SparseIndexEntry> = None;

    // Binary search for the correct block
    let mut left = 0;
    let mut right = sparse_index.len();

    while left < right {
        let mid = left + (right - left) / 2;
        let entry = &sparse_index[mid];

        if key < entry.first_key.as_slice() {
            // Key is before this block
            right = mid;
        } else if key > entry.last_key.as_slice() {
            // Key is after this block
            left = mid + 1;
        } else {
            // Key is within this block's range (first_key <= key <= last_key)
            target_block = Some(entry);
            break;
        }
    }

    // If no block contains this key range, key doesn't exist
    let block = match target_block {
        Some(b) => b,
        None => return Ok(None),
    };

    log::trace!(
        "Scanning block at offset {} for key {:?}",
        block.block_offset,
        String::from_utf8_lossy(key)
    );

    // Seek to the block and scan linearly
    reader.seek(SeekFrom::Start(block.block_offset))?;

    // Decode the block
    let block = Block::decode(&mut reader, block.block_offset)?;

    if let Some(records) = block.data.as_ref() {
        if let Ok(index) = records.binary_search_by(|record| record.key.as_slice().cmp(key)) {
            let record = &records[index];
            match record.record_type {
                RecordType::Put => return Ok(Some(record.value.clone())),
                RecordType::Delete => return Ok(None), // Tombstone
            }
        }
    }

    Ok(None)
}

/// Search for a key in the SSTable using the index
pub fn search_sstable<R: Read + Seek>(
    mut reader: R,
    key: &[u8],
    index: &BTreeMap<Vec<u8>, u64>,
) -> Result<Option<Vec<u8>>, std::io::Error> {
    use super::footer::decode_record_from_file;

    // Look up key in index
    if let Some(&offset) = index.get(key) {
        // Read the record at the offset
        let buf = BufReader::new(&mut reader);

        let (record_key, record_value, record_type, _next_offset) =
            decode_record_from_file(buf.buffer(), offset as usize)?;

        // Verify key matches
        if record_key == key {
            match record_type {
                crate::storage::record::RecordType::Put => Ok(Some(record_value.to_vec())),
                crate::storage::record::RecordType::Delete => Ok(None), // Tombstone
            }
        } else {
            Ok(None)
        }
    } else {
        Ok(None)
    }
}

/// Search for a key in the SSTable using bloom filter and index
/// This is more efficient as it checks the bloom filter first
pub fn search_sstable_with_bloom<R: Read + Seek>(
    mut reader: R,
    key: &[u8],
    bloom: &BloomFilterWrapper,
    index: &BTreeMap<Vec<u8>, u64>,
) -> Result<Option<Vec<u8>>, std::io::Error> {
    // Check bloom filter first - if it returns false, key definitely doesn't exist
    if !bloom.contains(key) {
        return Ok(None);
    }

    // Bloom filter says key might exist, check the index
    search_sstable(&mut reader, key, index)
}
