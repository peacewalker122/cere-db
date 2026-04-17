use std::collections::BTreeMap;
use std::io::{Read, Seek, SeekFrom};
use tokio::io::{AsyncRead, AsyncSeek, AsyncSeekExt};

use crate::{
    error::DBError,
    storage::{bloom::BloomFilterWrapper, record::RecordType},
};

/// SSTable Footer Structure:
/// - data_block_start: u64 (8 bytes) - where data blocks start
/// - data_block_end: u64 (8 bytes) - where data blocks end
/// - index_block_start: u64 (8 bytes) - where index block starts
/// - index_block_end: u64 (8 bytes) - where index block ends
/// - index_checksum: u32 (4 bytes) - CRC32 of index block
/// - bloom_block_start: u64 (8 bytes) - where bloom filter starts
/// - bloom_block_end: u64 (8 bytes) - where bloom filter ends
/// - bloom_checksum: u32 (4 bytes) - CRC32 of bloom filter
/// - magic_number: u32 (4 bytes) - validation marker (0xDB055555)
/// - footer_checksum: u32 (4 bytes) - CRC32 of footer data (excluding this field)
/// Total: 64 bytes
pub const FOOTER_SIZE: u64 = 64;
pub const MAGIC_NUMBER: u32 = 0xDB055555;

#[derive(Debug, Clone)]
pub struct SSTableFooter {
    pub data_block_start: u64,
    pub data_block_end: u64,
    pub index_block_start: u64,
    pub index_block_end: u64,
    pub index_checksum: u32,
    pub bloom_block_start: u64,
    pub bloom_block_end: u64,
    pub bloom_checksum: u32,
}

impl SSTableFooter {
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(FOOTER_SIZE as usize);
        buf.extend_from_slice(&self.data_block_start.to_be_bytes());
        buf.extend_from_slice(&self.data_block_end.to_be_bytes());
        buf.extend_from_slice(&self.index_block_start.to_be_bytes());
        buf.extend_from_slice(&self.index_block_end.to_be_bytes());
        buf.extend_from_slice(&self.index_checksum.to_be_bytes());
        buf.extend_from_slice(&self.bloom_block_start.to_be_bytes());
        buf.extend_from_slice(&self.bloom_block_end.to_be_bytes());
        buf.extend_from_slice(&self.bloom_checksum.to_be_bytes());
        buf.extend_from_slice(&MAGIC_NUMBER.to_be_bytes());

        // Calculate checksum of all footer data
        let footer_checksum = crc32fast::hash(&buf);
        buf.extend_from_slice(&footer_checksum.to_be_bytes());

        buf
    }

    pub fn decode<R: Read + Seek>(mut reader: R) -> Result<Self, std::io::Error> {
        log::debug!("Decoding SSTable footer...");
        let mut buf = [0u8; 8];
        let mut footer_data = Vec::with_capacity(60); // All data except final checksum

        reader.read_exact(&mut buf)?;
        footer_data.extend_from_slice(&buf);
        let data_block_start = u64::from_be_bytes(buf);

        reader.read_exact(&mut buf)?;
        footer_data.extend_from_slice(&buf);
        let data_block_end = u64::from_be_bytes(buf);

        reader.read_exact(&mut buf)?;
        footer_data.extend_from_slice(&buf);
        let index_block_start = u64::from_be_bytes(buf);

        reader.read_exact(&mut buf)?;
        footer_data.extend_from_slice(&buf);
        let index_block_end = u64::from_be_bytes(buf);

        let mut checksum_buf = [0u8; 4];
        reader.read_exact(&mut checksum_buf)?;
        footer_data.extend_from_slice(&checksum_buf);
        let index_checksum = u32::from_be_bytes(checksum_buf);

        reader.read_exact(&mut buf)?;
        footer_data.extend_from_slice(&buf);
        let bloom_block_start = u64::from_be_bytes(buf);

        reader.read_exact(&mut buf)?;
        footer_data.extend_from_slice(&buf);
        let bloom_block_end = u64::from_be_bytes(buf);

        reader.read_exact(&mut checksum_buf)?;
        footer_data.extend_from_slice(&checksum_buf);
        let bloom_checksum = u32::from_be_bytes(checksum_buf);

        let mut magic_buf = [0u8; 4];
        reader.read_exact(&mut magic_buf)?;
        footer_data.extend_from_slice(&magic_buf);
        let magic = u32::from_be_bytes(magic_buf);

        if magic != MAGIC_NUMBER {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "Invalid magic number: expected 0x{:X}, got 0x{:X}",
                    MAGIC_NUMBER, magic
                ),
            ));
        }

        // Verify footer checksum
        reader.read_exact(&mut checksum_buf)?;
        let stored_checksum = u32::from_be_bytes(checksum_buf);
        let calculated_checksum = crc32fast::hash(&footer_data);

        if stored_checksum != calculated_checksum {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "Footer checksum mismatch: expected 0x{:X}, got 0x{:X}",
                    calculated_checksum, stored_checksum
                ),
            ));
        }

        Ok(SSTableFooter {
            data_block_start,
            data_block_end,
            index_block_start,
            index_block_end,
            index_checksum,
            bloom_block_start,
            bloom_block_end,
            bloom_checksum,
        })
    }

    pub async fn async_decode<R: AsyncRead + AsyncSeek + Unpin>(
        mut reader: &mut R,
    ) -> Result<Self, std::io::Error> {
        reader.seek(SeekFrom::End(-(FOOTER_SIZE as i64))).await?;
        let mut buf = vec![0u8; FOOTER_SIZE as usize];
        tokio::io::AsyncReadExt::read_exact(&mut reader, &mut buf).await?;

        let mut cursor = std::io::Cursor::new(buf);
        Self::decode(&mut cursor)
    }
}

/// Helper to verify bloom filter checksum
pub fn verify_bloom_checksum(data: &[u8], expected: u32) -> Result<(), std::io::Error> {
    let calculated = crc32fast::hash(data);
    if calculated != expected {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!(
                "Bloom filter checksum mismatch: expected 0x{:X}, got 0x{:X}",
                expected, calculated
            ),
        ));
    }
    Ok(())
}

/// Helper function to decode a record from a File/generic reader at an offset
/// This reads the data into a buffer then decodes using Record::decode
pub fn decode_record_from_file(
    reader: &[u8],
    mut offset: usize,
) -> Result<(Vec<u8>, Vec<u8>, RecordType, usize), std::io::Error> {
    // Read record type

    let record_type = match reader[offset as usize] {
        1 => RecordType::Put,
        2 => RecordType::Delete,
        _ => {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Invalid record type",
            ));
        }
    };
    offset += 1;

    // Read key length
    let mut len_buf: [u8; 8] = reader[offset..offset + 8].try_into().map_err(|e| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("Failed to read key length: {:?}", e),
        )
    })?;
    let key_len = u64::from_be_bytes(len_buf) as usize;
    offset += 8;

    // Read value length
    len_buf = reader[offset..offset + 8].try_into().map_err(|e| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("Failed to read value length: {:?}", e),
        )
    })?;
    let value_len = u64::from_be_bytes(len_buf) as usize;
    offset += 8;

    // Read key
    let key = &reader[offset..offset + key_len];
    offset += key_len;

    // Read value
    let value = &reader[offset..offset + value_len];
    offset += value_len;

    // Read and verify checksum
    let checksum_buf: [u8; 4] = reader[offset..offset + 4].try_into().or_else(|e| {
        Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("Failed to read checksum: {:?}", e),
        ))
    })?;
    let checksum = u32::from_be_bytes(checksum_buf);

    if crc32fast::hash(&value) != checksum {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "Checksum mismatch",
        ));
    }

    // Calculate next offset
    let next_offset = offset + 1 + 8 + 8 + key_len + value_len + 4;

    Ok((key.to_vec(), value.to_vec(), record_type, next_offset))
}

/// Read the footer from an SSTable file
pub fn read_sstable_footer<R: Read + Seek>(mut reader: R) -> Result<SSTableFooter, std::io::Error> {
    // Seek to footer location (last FOOTER_SIZE bytes)
    reader.seek(SeekFrom::End(-(FOOTER_SIZE as i64)))?;
    SSTableFooter::decode(reader)
}

/// Read the sparse index block from an SSTable file
pub fn read_sstable_sparse_index<R: Read + Seek>(
    mut reader: R,
    footer: &SSTableFooter,
) -> Result<Vec<crate::storage::index::SparseIndexEntry>, std::io::Error> {
    use crate::storage::index::{SparseIndexEntry, verify_index_checksum};

    // Calculate index block size
    let index_size = footer.index_block_end - footer.index_block_start;

    // Seek to index block start and read entire block
    reader.seek(SeekFrom::Start(footer.index_block_start))?;
    let mut index_data = vec![0u8; index_size as usize];
    reader.read_exact(&mut index_data)?;

    // Verify index checksum
    verify_index_checksum(&index_data, footer.index_checksum)?;

    // Parse sparse index entries
    let mut cursor = std::io::Cursor::new(&index_data);

    // Read number of entries
    let mut count_buf = [0u8; 8];
    std::io::Read::read_exact(&mut cursor, &mut count_buf)?;
    let entry_count = u64::from_be_bytes(count_buf);

    // Read all sparse index entries
    let mut sparse_index = Vec::new();
    for _ in 0..entry_count {
        let entry = SparseIndexEntry::decode(&mut cursor)?;
        sparse_index.push(entry);
    }

    Ok(sparse_index)
}

/// Read the index block from an SSTable file (legacy function for compatibility)
pub fn read_sstable_index<R: Read + Seek>(
    mut reader: R,
    footer: &SSTableFooter,
) -> Result<BTreeMap<Vec<u8>, u64>, std::io::Error> {
    use crate::storage::index::{IndexEntry, verify_index_checksum};

    // Calculate index block size
    let index_size = footer.index_block_end - footer.index_block_start;

    // Seek to index block start and read entire block
    reader.seek(SeekFrom::Start(footer.index_block_start))?;
    let mut index_data = vec![0u8; index_size as usize];
    reader.read_exact(&mut index_data)?;

    // Verify index checksum
    verify_index_checksum(&index_data, footer.index_checksum)?;

    // Parse index entries
    let mut cursor = std::io::Cursor::new(&index_data);

    // Read number of entries
    let mut count_buf = [0u8; 8];
    std::io::Read::read_exact(&mut cursor, &mut count_buf)?;
    let entry_count = u64::from_be_bytes(count_buf);

    // Read all index entries
    let mut index = BTreeMap::new();
    for _ in 0..entry_count {
        let entry = IndexEntry::decode(&mut cursor)?;
        index.insert(entry.key, entry.offset);
    }

    Ok(index)
}

/// Read the bloom filter from an SSTable file
pub fn read_sstable_bloom<R: Read + Seek>(
    mut reader: R,
    footer: &SSTableFooter,
) -> Result<BloomFilterWrapper, std::io::Error> {
    // Calculate bloom filter block size
    let bloom_size = footer.bloom_block_end - footer.bloom_block_start;

    // Seek to bloom filter block start and read entire block
    reader.seek(SeekFrom::Start(footer.bloom_block_start))?;
    let mut bloom_data = vec![0u8; bloom_size as usize];
    reader.read_exact(&mut bloom_data)?;

    // Verify bloom filter checksum
    verify_bloom_checksum(&bloom_data, footer.bloom_checksum)?;

    // Decode bloom filter
    let cursor = std::io::Cursor::new(&bloom_data);
    BloomFilterWrapper::decode(cursor)
}

