use crossbeam_skiplist::SkipMap;
use std::{
    cmp::{Ordering, Reverse},
    collections::{BTreeMap, BinaryHeap},
    fs::OpenOptions,
    io::{BufReader, Read, Seek, SeekFrom, Write},
};
use tokio::io::{AsyncRead, AsyncSeek, AsyncSeekExt};

use crate::{
    error::DBError,
    storage::log::{Block, Record},
};

use super::{block::BlockBuilder, bloom::BloomFilter, record::RecordType};

// The structure itself is
// 1. Block
// 2. Index, sparse index
// 3. Bloom filter
// 4. Footer
#[derive(Debug)]
pub struct SSTable {
    pub block: Vec<Block>,
    pub index: Vec<SparseIndexEntry>,
    pub bloom: BloomFilter,
    pub footer: SSTableFooter,
}

impl SSTable {
    pub fn decode<R: Read + Seek>(mut cursor: R) -> Result<Self, DBError> {
        // Read footer
        cursor.seek(SeekFrom::End(-(FOOTER_SIZE as i64)))?;

        let footer = SSTableFooter::decode(&mut cursor)?;
        log::debug!(
            "Decoded SSTable footer: data=[{}-{}], sparse_index=[{}-{}], bloom=[{}-{}], index_crc=0x{:X}, bloom_crc=0x{:X}",
            footer.data_block_start,
            footer.data_block_end,
            footer.index_block_start,
            footer.index_block_end,
            footer.bloom_block_start,
            footer.bloom_block_end,
            footer.index_checksum,
            footer.bloom_checksum
        );

        // Read index block
        cursor.seek(SeekFrom::Start(footer.index_block_start))?;
        let mut index_data =
            vec![0u8; (footer.index_block_end - footer.index_block_start) as usize];
        cursor.read_exact(&mut index_data)?;

        // Verify index checksum
        verify_index_checksum(&index_data, footer.index_checksum)?;

        // Parse index entries
        let mut index_cursor = std::io::Cursor::new(&index_data);
        let mut index = Vec::new();

        // Read number of entries
        let mut count_buf = [0u8; 8];
        std::io::Read::read_exact(&mut index_cursor, &mut count_buf)?;
        let entry_count = u64::from_be_bytes(count_buf);

        let mut block_buf = vec![0u8; (footer.data_block_end - footer.data_block_start) as usize];
        cursor.seek(SeekFrom::Start(footer.data_block_start))?;
        cursor.read_exact(&mut block_buf)?;

        // this block_cursor ownership will later be passed to Block::decode
        let mut block_cursor = std::io::Cursor::new(block_buf);

        // Read all index entries and decode blocks
        let mut blocks = Vec::new();
        for _ in 0..entry_count {
            let entry = SparseIndexEntry::decode(&mut index_cursor)?;
            // Adjust offset to be relative to the start of the data block section
            let relative_offset = entry.block_offset - footer.data_block_start;
            index.push(entry);

            // Decode the block from the static data
            let block = Block::decode(&mut block_cursor, relative_offset)?;
            blocks.push(block);
        }

        // Read bloom filter block
        cursor.seek(SeekFrom::Start(footer.bloom_block_start))?;
        let mut bloom_data =
            vec![0u8; (footer.bloom_block_end - footer.bloom_block_start) as usize];
        cursor.read_exact(&mut bloom_data)?;

        // Verify bloom filter checksum
        verify_bloom_checksum(&bloom_data, footer.bloom_checksum)?;

        // Decode bloom filter
        let bloom_cursor = std::io::Cursor::new(&bloom_data);
        let bloom = BloomFilter::decode(bloom_cursor)?;

        Ok(SSTable {
            block: blocks,
            index,
            bloom,
            footer,
        })
    }

    pub async fn decode_async<R>(mut reader: R) -> Result<Self, DBError>
    where
        R: AsyncRead + AsyncSeek + Unpin,
    {
        reader.seek(SeekFrom::End(-(FOOTER_SIZE as i64))).await?;
        let mut footer_bytes = vec![0u8; FOOTER_SIZE as usize];
        tokio::io::AsyncReadExt::read_exact(&mut reader, &mut footer_bytes).await?;
        let footer = SSTableFooter::decode(std::io::Cursor::new(&footer_bytes))?;
        log::debug!(
            "Decoded SSTable footer: data=[{}-{}], sparse_index=[{}-{}], bloom=[{}-{}], index_crc=0x{:X}, bloom_crc=0x{:X}",
            footer.data_block_start,
            footer.data_block_end,
            footer.index_block_start,
            footer.index_block_end,
            footer.bloom_block_start,
            footer.bloom_block_end,
            footer.index_checksum,
            footer.bloom_checksum
        );

        reader
            .seek(SeekFrom::Start(footer.index_block_start))
            .await?;
        let mut index_data =
            vec![0u8; (footer.index_block_end - footer.index_block_start) as usize];
        tokio::io::AsyncReadExt::read_exact(&mut reader, &mut index_data).await?;
        verify_index_checksum(&index_data, footer.index_checksum)?;

        let mut index_cursor = std::io::Cursor::new(&index_data);
        let mut index = Vec::new();

        let mut count_buf = [0u8; 8];
        std::io::Read::read_exact(&mut index_cursor, &mut count_buf)?;
        let entry_count = u64::from_be_bytes(count_buf);

        reader
            .seek(SeekFrom::Start(footer.data_block_start))
            .await?;
        let mut block_buf = vec![0u8; (footer.data_block_end - footer.data_block_start) as usize];
        tokio::io::AsyncReadExt::read_exact(&mut reader, &mut block_buf).await?;
        let mut block_cursor = std::io::Cursor::new(block_buf);

        let mut blocks = Vec::new();
        for _ in 0..entry_count {
            let entry = SparseIndexEntry::decode(&mut index_cursor)?;
            let relative_offset = entry.block_offset - footer.data_block_start;
            index.push(entry);

            let block = Block::decode(&mut block_cursor, relative_offset)?;
            blocks.push(block);
        }

        reader
            .seek(SeekFrom::Start(footer.bloom_block_start))
            .await?;
        let mut bloom_data =
            vec![0u8; (footer.bloom_block_end - footer.bloom_block_start) as usize];
        tokio::io::AsyncReadExt::read_exact(&mut reader, &mut bloom_data).await?;
        verify_bloom_checksum(&bloom_data, footer.bloom_checksum)?;

        let bloom_cursor = std::io::Cursor::new(&bloom_data);
        let bloom = BloomFilter::decode(bloom_cursor)?;

        Ok(SSTable {
            block: blocks,
            index,
            bloom,
            footer,
        })
    }

    pub async fn build_from_records(records: Vec<Record>, _level: u32) -> Result<SSTable, DBError> {
        // this function will build an SSTable from the list of records, it will build the index,
        // bloom filter and footer, but it will not write to disk, the caller can use the build SSTable and write it to disk using the flush_memtable function

        // Build sparse index and bloom filter as we write data blocks
        let mut sparse_index: Vec<SparseIndexEntry> = Vec::new();
        let mut blocks: Vec<Block> = Vec::new();
        let mut block_data_sizes: Vec<u64> = Vec::new();

        // Create Bloom filter with appropriate capacity
        let mut bloom_filter = BloomFilter::with_rate(records.len(), 0.01);

        log::debug!("Writing {} records to 4KB blocks...", records.len());

        let data_block_start = 0u64;
        let mut current_offset = data_block_start;
        let mut block_builder = BlockBuilder::new(current_offset);

        // Write all merged records to blocks
        for record in records.iter() {
            // Insert key into Bloom filter
            bloom_filter.insert(record.key.to_owned());

            // Try to add record to current block
            match block_builder.add_record(record) {
                Ok(()) => {
                    // Record added successfully
                }
                Err(_record) => {
                    // Block is full, finalize it and create a new one
                    if let Some((block_meta, block_data)) = block_builder.build() {
                        log::trace!(
                            "Block filled: offset={}, size={} bytes, records={}, first_key={:?}, last_key={:?}",
                            block_meta.offset,
                            block_meta.data_size,
                            block_meta.record_count,
                            String::from_utf8_lossy(&block_meta.first_key),
                            String::from_utf8_lossy(&block_meta.last_key)
                        );

                        // Add to sparse index
                        sparse_index.push(SparseIndexEntry {
                            first_key: block_meta.first_key.clone(),
                            block_offset: block_meta.offset,
                            last_key: block_meta.last_key.clone(),
                            record_count: block_meta.record_count,
                        });

                        // Store block data
                        let block_total_size = block_data.len() as u64;
                        blocks.push(block_meta);
                        block_data_sizes.push(block_total_size);
                        current_offset += block_total_size;
                    }

                    // Create new block and add the record that didn't fit
                    block_builder = BlockBuilder::new(current_offset);
                    block_builder
                        .add_record(record)
                        .expect("Fresh block should have space for record");
                }
            }
        }

        // Finalize the last block if it has data
        if !block_builder.is_empty()
            && let Some((block_meta, block_data)) = block_builder.build()
        {
            log::trace!(
                "Final block: offset={}, size={} bytes, records={}, first_key={:?}, last_key={:?}",
                block_meta.offset,
                block_meta.data_size,
                block_meta.record_count,
                String::from_utf8_lossy(&block_meta.first_key),
                String::from_utf8_lossy(&block_meta.last_key)
            );

            sparse_index.push(SparseIndexEntry {
                first_key: block_meta.first_key.clone(),
                block_offset: block_meta.offset,
                last_key: block_meta.last_key.clone(),
                record_count: block_meta.record_count,
            });

            let block_total_size = block_data.len() as u64;
            blocks.push(block_meta);
            block_data_sizes.push(block_total_size);
        }

        log::info!(
            "Created {} blocks from {} entries",
            blocks.len(),
            records.len()
        );

        let data_block_end = data_block_start + block_data_sizes.iter().sum::<u64>();

        // Build sparse index block
        let mut index_blocks: Vec<u8> = Vec::new();

        // Write number of sparse index entries
        index_blocks.extend_from_slice(&(sparse_index.len() as u64).to_be_bytes());

        // Write each sparse index entry
        for entry in sparse_index.iter() {
            index_blocks.append(&mut entry.encode());
        }

        // Calculate index block checksum
        let index_checksum = crc32fast::hash(&index_blocks);

        // Write Bloom filter block
        let bloom_data = bloom_filter.encode();

        // Calculate bloom filter checksum
        let bloom_checksum = crc32fast::hash(&bloom_data);

        let index_block_start = data_block_end;
        let index_block_end = index_block_start + index_blocks.len() as u64;
        let bloom_block_start = index_block_end;
        let bloom_block_end = bloom_block_start + bloom_data.len() as u64;

        // Write footer
        let footer = SSTableFooter {
            data_block_start,
            data_block_end,
            index_block_start,
            index_block_end,
            index_checksum,
            bloom_block_start,
            bloom_block_end,
            bloom_checksum,
        };

        Ok(SSTable {
            block: blocks,
            index: sparse_index,
            bloom: bloom_filter,
            footer,
        })
    }
}

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
const FOOTER_SIZE: u64 = 64;
const MAGIC_NUMBER: u32 = 0xDB055555;

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
}

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
}

/// Helper to verify index checksum
fn verify_index_checksum(data: &[u8], expected: u32) -> Result<(), std::io::Error> {
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

/// Helper to verify bloom filter checksum
fn verify_bloom_checksum(data: &[u8], expected: u32) -> Result<(), std::io::Error> {
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

const DATA_DIR: &str = "data";
const ARCHIVE_DIR: &str = "archive";

fn level_dir(level: u32) -> std::path::PathBuf {
    std::path::Path::new(DATA_DIR).join(format!("level-{level}"))
}

fn sstable_path(level: u32, file_id: u64) -> std::path::PathBuf {
    level_dir(level).join(format!("{file_id}.db"))
}

fn archive_wal_path(file_path: &str) -> std::path::PathBuf {
    let wal_name = std::path::Path::new(file_path)
        .file_name()
        .unwrap_or_default();
    std::path::Path::new(ARCHIVE_DIR).join(wal_name)
}

pub fn flush_memtable(
    memtable: SkipMap<Vec<u8>, (RecordType, Vec<u8>)>,
    level: u32,
    file_id: u64,
    file_path: &str,
) -> Result<String, std::io::Error> {
    let sstable_path = sstable_path(level, file_id);
    let sstable_filename = sstable_path.to_string_lossy().to_string();

    log::info!(
        "Starting memtable flush to SSTable '{}' with 4KB blocks, entries: {}",
        sstable_filename,
        memtable.len()
    );

    log::debug!("No merge needed, writing memtable only");
    let records_to_write: Vec<Record> = memtable
        .iter()
        .map(|entry| {
            Record::new(
                entry.key().to_owned(),
                entry.value().1.to_owned(),
                entry.value().0,
                crate::storage::record::current_timestamp_millis(),
            )
        })
        .collect();

    // Now write merged/new data to file (truncate and rewrite)
    let mut file = OpenOptions::new()
        .write(true)
        .truncate(true) // Overwrite existing content
        .create(true)
        .open(&sstable_path)?;

    let data_block_start = 0u64; // Starting from beginning of file

    // Build sparse index and bloom filter as we write data blocks
    let mut sparse_index: Vec<SparseIndexEntry> = Vec::new();
    let mut blocks: Vec<Vec<u8>> = Vec::new();

    // Create Bloom filter with appropriate capacity
    let mut bloom_filter = BloomFilter::with_rate(records_to_write.len(), 0.01);

    // Create first block builder
    let mut current_offset = data_block_start;
    let mut block_builder = BlockBuilder::new(current_offset);

    log::debug!(
        "Writing {} records to 4KB blocks...",
        records_to_write.len()
    );

    // Write all merged records to blocks
    for record in records_to_write.iter() {
        // Insert key into Bloom filter
        bloom_filter.insert(record.key.to_owned());

        // Try to add record to current block
        match block_builder.add_record(record) {
            Ok(()) => {
                // Record added successfully
            }
            Err(_record) => {
                // Block is full, finalize it and create a new one
                if let Some((block_meta, block_data)) = block_builder.build() {
                    log::trace!(
                        "Block filled: offset={}, size={} bytes, records={}, first_key={:?}, last_key={:?}",
                        block_meta.offset,
                        block_meta.data_size,
                        block_meta.record_count,
                        String::from_utf8_lossy(&block_meta.first_key),
                        String::from_utf8_lossy(&block_meta.last_key)
                    );

                    // Add to sparse index
                    sparse_index.push(SparseIndexEntry {
                        first_key: block_meta.first_key,
                        block_offset: block_meta.offset,
                        last_key: block_meta.last_key,
                        record_count: block_meta.record_count,
                    });

                    // Store block data
                    let block_total_size = block_data.len() as u64;
                    blocks.push(block_data);
                    current_offset += block_total_size;
                }

                // Create new block and add the record that didn't fit
                block_builder = BlockBuilder::new(current_offset);
                block_builder
                    .add_record(record)
                    .expect("Fresh block should have space for record");
            }
        }
    }

    // Finalize the last block if it has data
    if !block_builder.is_empty()
        && let Some((block_meta, block_data)) = block_builder.build()
    {
        log::trace!(
            "Final block: offset={}, size={} bytes, records={}, first_key={:?}, last_key={:?}",
            block_meta.offset,
            block_meta.data_size,
            block_meta.record_count,
            String::from_utf8_lossy(&block_meta.first_key),
            String::from_utf8_lossy(&block_meta.last_key)
        );

        sparse_index.push(SparseIndexEntry {
            first_key: block_meta.first_key,
            block_offset: block_meta.offset,
            last_key: block_meta.last_key,
            record_count: block_meta.record_count,
        });

        blocks.push(block_data);
    }

    log::info!(
        "Created {} blocks from {} entries",
        blocks.len(),
        memtable.len()
    );

    // Write all blocks to file
    for block_data in &blocks {
        file.write_all(block_data)?;
    }
    let data_block_end = file.metadata()?.len();

    // Build and write sparse index block
    let index_block_start = data_block_end;
    let mut index_blocks: Vec<u8> = Vec::new();

    // Write number of sparse index entries
    index_blocks.extend_from_slice(&(sparse_index.len() as u64).to_be_bytes());

    // Write each sparse index entry
    for entry in sparse_index.iter() {
        index_blocks.append(&mut entry.encode());
    }

    file.write_all(&index_blocks)?;
    let index_block_end = file.metadata()?.len();

    // Calculate index block checksum
    let index_checksum = crc32fast::hash(&index_blocks);

    // Write Bloom filter block
    let bloom_block_start = index_block_end;
    let bloom_data = bloom_filter.encode();
    file.write_all(&bloom_data)?;
    let bloom_block_end = file.metadata()?.len();

    // Calculate bloom filter checksum
    let bloom_checksum = crc32fast::hash(&bloom_data);

    // Write footer
    let footer = SSTableFooter {
        data_block_start,
        data_block_end,
        index_block_start,
        index_block_end,
        index_checksum,
        bloom_block_start,
        bloom_block_end,
        bloom_checksum,
    };

    file.write_all(&footer.encode())?;
    file.sync_data()?;

    log::info!(
        "Flushed SSTable '{}': {} blocks, data=[{}-{}], sparse_index=[{}-{}], bloom=[{}-{}], index_crc=0x{:X}, bloom_crc=0x{:X}",
        sstable_filename,
        blocks.len(),
        data_block_start,
        data_block_end,
        index_block_start,
        index_block_end,
        bloom_block_start,
        bloom_block_end,
        index_checksum,
        bloom_checksum
    );

    std::fs::create_dir_all(ARCHIVE_DIR)?;
    let archived_wal = archive_wal_path(file_path);
    if let Err(err) = std::fs::rename(file_path, archived_wal) {
        log::warn!("Failed to archive WAL '{}': {}", file_path, err);
    }

    Ok(sstable_filename)
}

pub trait SortedRecordSource {
    fn next_record(&mut self) -> Result<Option<Record>, DBError>;
}

type MemtableIter<'a> = crossbeam_skiplist::map::Iter<'a, Vec<u8>, (RecordType, Vec<u8>)>;

struct MemtableSource<'a> {
    iter: MemtableIter<'a>,
}

impl<'a> MemtableSource<'a> {
    fn new(memtable: &'a SkipMap<Vec<u8>, (RecordType, Vec<u8>)>) -> Self {
        Self {
            iter: memtable.iter(),
        }
    }
}

impl<'a> SortedRecordSource for MemtableSource<'a> {
    fn next_record(&mut self) -> Result<Option<Record>, DBError> {
        let entry = match self.iter.next() {
            Some(entry) => entry,
            None => return Ok(None),
        };

        let (record_type, value) = entry.value();
        Ok(Some(Record::new(
            entry.key().to_owned(),
            value.to_owned(),
            *record_type,
            crate::storage::record::current_timestamp_millis(),
        )))
    }
}

pub struct SSTableSource {
    blocks: Vec<Block>,
    current_block_idx: usize,
    current_block_record_idx: usize,
}

impl SSTableSource {
    pub fn new(blocks: Vec<Block>) -> Self {
        let mut source = Self {
            blocks,
            current_block_idx: 0,
            current_block_record_idx: 0,
        };
        source.load_next_block();
        source
    }

    fn load_next_block(&mut self) {
        if self.current_block_idx >= self.blocks.len() {
            return;
        }

        let _block = &self.blocks[self.current_block_idx];
        self.current_block_record_idx = 0;
        self.current_block_idx += 1;
    }
}

#[cfg(test)]
fn find_record<'a>(records: &'a [Record], key: &[u8]) -> Option<&'a Record> {
    records
        .binary_search_by(|record| record.key.as_slice().cmp(key))
        .ok()
        .map(|index| &records[index])
}

impl SortedRecordSource for SSTableSource {
    fn next_record(&mut self) -> Result<Option<Record>, DBError> {
        loop {
            if self.current_block_idx == 0 || self.current_block_idx > self.blocks.len() {
                return Ok(None);
            }

            let block_idx = self.current_block_idx - 1;
            let should_advance = match self.blocks[block_idx].data.as_ref() {
                Some(records) if self.current_block_record_idx < records.len() => {
                    let rec = records[self.current_block_record_idx].clone();
                    self.current_block_record_idx += 1;
                    return Ok(Some(rec));
                }
                _ => true,
            };

            if should_advance {
                if self.current_block_idx >= self.blocks.len() {
                    return Ok(None);
                }
                self.load_next_block();
            }
        }
    }
}

#[derive(Debug)]
struct HeapItem {
    record: Record,
    source_id: usize,
}

impl PartialEq for HeapItem {
    fn eq(&self, other: &Self) -> bool {
        self.record == other.record && self.source_id == other.source_id
    }
}

impl Eq for HeapItem {}

impl PartialOrd for HeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HeapItem {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.record.cmp(&other.record) {
            Ordering::Equal => self.source_id.cmp(&other.source_id),
            ordering => ordering,
        }
    }
}

fn build_merge_sources<'a>(
    memtable: &'a SkipMap<Vec<u8>, (RecordType, Vec<u8>)>,
    sstables: &'a [Block],
) -> Vec<Box<dyn SortedRecordSource + 'a>> {
    let mut sources: Vec<Box<dyn SortedRecordSource + 'a>> = Vec::new();
    sources.push(Box::new(MemtableSource::new(memtable)));
    sources.push(Box::new(SSTableSource::new(sstables.to_vec())));
    sources
}

pub fn merge_record_sources<'a>(
    mut sources: Vec<Box<dyn SortedRecordSource + 'a>>,
) -> Result<Vec<Record>, DBError> {
    let mut minheap = BinaryHeap::with_capacity(sources.len());

    for (source_id, source) in sources.iter_mut().enumerate() {
        if let Some(record) = source.next_record()? {
            minheap.push(Reverse(HeapItem { record, source_id }));
        }
    }

    let mut merged_records = Vec::new();
    while let Some(Reverse(item)) = minheap.pop() {
        let mut current = item.record;
        let key = current.key.clone();

        if let Some(next) = sources[item.source_id].next_record()? {
            minheap.push(Reverse(HeapItem {
                record: next,
                source_id: item.source_id,
            }));
        }

        while let Some(Reverse(peek)) = minheap.peek() {
            if peek.record.key != key {
                break;
            }

            let Reverse(duplicate) = minheap.pop().expect("peeked entry exists");
            if duplicate.record.timestamp > current.timestamp {
                current = duplicate.record;
            }

            if let Some(next) = sources[duplicate.source_id].next_record()? {
                minheap.push(Reverse(HeapItem {
                    record: next,
                    source_id: duplicate.source_id,
                }));
            }
        }

        merged_records.push(current);
    }

    Ok(merged_records)
}

// k-way streaming merge: O(K) heap where K = number of sources
pub fn merge_sstables(
    memtable: &SkipMap<Vec<u8>, (RecordType, Vec<u8>)>,
    sstables: &Vec<Block>,
) -> Result<Vec<Record>, DBError> {
    let sources = build_merge_sources(memtable, sstables);
    merge_record_sources(sources)
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
) -> Result<Vec<SparseIndexEntry>, std::io::Error> {
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

    // Read all index entries
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
) -> Result<BloomFilter, std::io::Error> {
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
    BloomFilter::decode(cursor)
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
    // Find the block that might contain the key using binary search
    // We need to find the block where: first_key <= key <= last_key
    let mut target_block: Option<&SparseIndexEntry> = None;

    // Binary search for the correct block
    let mut left = 0;
    let mut right = sparse_index.len();

    while left < right {
        let mid = left + (right - left) / 2;
        let entry = &sparse_index[mid];

        // println!(
        //     "Comparing key {:?} with block range {:?} - {:?}",
        //     String::from_utf8_lossy(key),
        //     String::from_utf8_lossy(&entry.first_key),
        //     String::from_utf8_lossy(&entry.last_key),
        // );

        if key < entry.first_key.as_slice() {
            // Key is before this block
            right = mid;
        } else if key > entry.last_key.as_slice() {
            // Key is after this block
            left = mid + 1;
        } else {
            // Key is within this block's range (first_key <= key <= last_key)
            target_block = Some(entry);
            // println!("Target block found at offset {}", entry.block_offset);
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

    // Start decoding from the beginning of buf (skip the block header first)
    // The block header contains: first_key_len + first_key + last_key_len + last_key + record_count + data_size
    // We need to skip this header to get to the actual record data

    // Decode the block header to know where records start

    let block = Block::decode(&mut reader, block.block_offset)?;
    // get the data block. Traverse through the key

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
    // Look up key in index
    if let Some(&offset) = index.get(key) {
        // Read the record at the offset

        let buf = BufReader::new(&mut reader);

        let (record_key, record_value, record_type, _next_offset) =
            decode_record_from_file(buf.buffer(), offset as usize)?;

        // Verify key matches
        if record_key == key {
            match record_type {
                RecordType::Put => Ok(Some(record_value.to_vec())),
                RecordType::Delete => Ok(None), // Tombstone
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
    bloom: &BloomFilter,
    index: &BTreeMap<Vec<u8>, u64>,
) -> Result<Option<Vec<u8>>, std::io::Error> {
    // Check bloom filter first - if it returns false, key definitely doesn't exist
    if !bloom.contains(key) {
        return Ok(None);
    }

    // Bloom filter says key might exist, check the index
    search_sstable(&mut reader, key, index)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;
    use std::io::Cursor;
    use std::{
        path::Path,
        sync::{Mutex, OnceLock},
        time::UNIX_EPOCH,
    };

    fn with_temp_dir<T>(test_name: &str, test: impl FnOnce() -> T) -> T {
        static TEST_MUTEX: OnceLock<Mutex<()>> = OnceLock::new();
        let _guard = TEST_MUTEX.get_or_init(|| Mutex::new(())).lock().unwrap();

        let unique_id = std::time::SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let temp_root =
            std::env::temp_dir().join(format!("wasm-kv-sstable-{test_name}-{unique_id}"));

        std::fs::create_dir_all(&temp_root).unwrap();
        let original_dir = std::env::current_dir().unwrap();
        std::env::set_current_dir(&temp_root).unwrap();

        let result = test();

        std::env::set_current_dir(original_dir).unwrap();
        std::fs::remove_dir_all(&temp_root).ok();

        result
    }

    fn flush_for_test(memtable: SkipMap<Vec<u8>, (RecordType, Vec<u8>)>, file_id: u64) -> String {
        let wal_path = format!("test_wal_{file_id}.log");

        std::fs::create_dir_all("archive").ok();
        std::fs::create_dir_all("data/level-0").ok();
        std::fs::write(&wal_path, b"").ok();

        flush_memtable(memtable, 0, file_id, &wal_path).unwrap()
    }

    fn run_async_test<T>(test: impl std::future::Future<Output = T>) -> T {
        tokio::runtime::Runtime::new()
            .expect("runtime should build")
            .block_on(test)
    }

    #[test]
    fn test_flush_memtable_writes_level0_path() {
        // Arrange: set up memtable and directories for level-0 output.
        with_temp_dir("flush-level0-path", || {
            let memtable = SkipMap::new();
            memtable.insert(b"key".to_vec(), (RecordType::Put, b"value".to_vec()));
            let file_id = 42;
            let wal_path = format!("wal_{file_id}.log");
            std::fs::create_dir_all("data/level-0").unwrap();
            std::fs::write(&wal_path, b"wal").unwrap();

            // Act: flush memtable to SSTable.
            let filename = flush_memtable(memtable, 0, file_id, &wal_path).unwrap();

            // Assert: SSTable flush writes to data/level-0/{id}.db per objective.
            assert_eq!(filename, "data/level-0/42.db");
            assert!(Path::new(&filename).exists(), "expected SSTable to exist");
        });
    }

    #[test]
    fn test_flush_memtable_errors_without_level_dir() {
        // Arrange: set up memtable without creating level-0 directory.
        with_temp_dir("flush-missing-level0", || {
            let memtable = SkipMap::new();
            memtable.insert(b"key".to_vec(), (RecordType::Put, b"value".to_vec()));
            let file_id = 77;
            let wal_path = format!("wal_{file_id}.log");
            std::fs::write(&wal_path, b"wal").unwrap();

            // Act: attempt to flush without required directory.
            let result = flush_memtable(memtable, 0, file_id, &wal_path);

            // Assert: missing level-0 directory is handled as an error (negative case).
            assert!(
                result.is_err(),
                "expected error when level directory is missing"
            );
        });
    }

    #[test]
    fn test_flush_archives_wal_with_matching_id() {
        // Arrange: WAL file named with the same ID used for SSTable.
        with_temp_dir("flush-archives-wal", || {
            let memtable = SkipMap::new();
            memtable.insert(b"key".to_vec(), (RecordType::Put, b"value".to_vec()));
            let file_id = 9001;
            let wal_path = format!("wal_{file_id}.log");
            std::fs::create_dir_all("data/level-0").unwrap();
            std::fs::write(&wal_path, b"wal").unwrap();

            // Act: flush memtable, which archives the WAL.
            let filename = flush_memtable(memtable, 0, file_id, &wal_path).unwrap();

            // Assert: WAL file naming uses same ID and is archived after flush.
            assert_eq!(filename, "data/level-0/9001.db");
            assert!(Path::new("archive/wal_9001.log").exists());
            assert!(
                !Path::new(&wal_path).exists(),
                "expected WAL to be archived"
            );
        });
    }

    #[test]
    fn test_flush_skips_archive_when_wal_missing() {
        // Arrange: WAL path does not exist to simulate improper input.
        with_temp_dir("flush-missing-wal", || {
            let memtable = SkipMap::new();
            memtable.insert(b"key".to_vec(), (RecordType::Put, b"value".to_vec()));
            let file_id = 811;
            let wal_path = format!("wal_{file_id}.log");
            std::fs::create_dir_all("data/level-0").unwrap();

            // Act: flush without a WAL file on disk.
            let result = flush_memtable(memtable, 0, file_id, &wal_path).unwrap();

            // Assert: flush succeeds but WAL archive is not created (negative case).
            assert_eq!(result, "data/level-0/811.db");
            assert!(!Path::new("archive/wal_811.log").exists());
        });
    }

    #[test]
    fn test_sstable_footer() {
        let footer = SSTableFooter {
            data_block_start: 0,
            data_block_end: 1000,
            index_block_start: 1000,
            index_block_end: 1500,
            index_checksum: 0x12345678,
            bloom_block_start: 1500,
            bloom_block_end: 1600,
            bloom_checksum: 0x87654321,
        };

        let encoded = footer.encode();
        assert_eq!(encoded.len(), FOOTER_SIZE as usize);

        let decoded = SSTableFooter::decode(Cursor::new(&encoded)).unwrap();
        assert_eq!(decoded.data_block_start, footer.data_block_start);
        assert_eq!(decoded.data_block_end, footer.data_block_end);
        assert_eq!(decoded.index_block_start, footer.index_block_start);
        assert_eq!(decoded.index_block_end, footer.index_block_end);
        assert_eq!(decoded.index_checksum, footer.index_checksum);
        assert_eq!(decoded.bloom_block_start, footer.bloom_block_start);
        assert_eq!(decoded.bloom_block_end, footer.bloom_block_end);
        assert_eq!(decoded.bloom_checksum, footer.bloom_checksum);
    }

    #[test]
    fn test_index_entry() {
        let entry = IndexEntry {
            key: b"test_key".to_vec(),
            offset: 12345,
        };

        let encoded = entry.encode();
        let decoded = IndexEntry::decode(Cursor::new(&encoded)).unwrap();

        assert_eq!(decoded.key, entry.key);
        assert_eq!(decoded.offset, entry.offset);
    }

    #[test]
    fn test_sstable_index() {
        let mut index = BTreeMap::new();
        index.insert(b"key1".to_vec(), 100u64);
        index.insert(b"key2".to_vec(), 200u64);
        index.insert(b"key3".to_vec(), 300u64);

        // Encode index block
        let mut index_block = Vec::new();
        index_block.extend_from_slice(&(index.len() as u64).to_be_bytes());

        for (key, offset) in index.iter() {
            let entry = IndexEntry {
                key: key.clone(),
                offset: *offset,
            };
            index_block.append(&mut entry.encode());
        }

        // Calculate checksum
        let index_checksum = crc32fast::hash(&index_block);

        // Create mock footer
        let footer = SSTableFooter {
            data_block_start: 0,
            data_block_end: 1000,
            index_block_start: 0, // Index starts at beginning for this test
            index_block_end: index_block.len() as u64,
            index_checksum,
            bloom_block_start: 0,
            bloom_block_end: 0,
            bloom_checksum: 0,
        };

        // Decode index
        let decoded_index = read_sstable_index(Cursor::new(&index_block), &footer).unwrap();

        assert_eq!(decoded_index.len(), 3);
        assert_eq!(decoded_index.get(b"key1".as_ref()), Some(&100));
        assert_eq!(decoded_index.get(b"key2".as_ref()), Some(&200));
        assert_eq!(decoded_index.get(b"key3".as_ref()), Some(&300));
    }

    #[test]
    fn test_checksum_validation() {
        // Test footer checksum validation
        let footer = SSTableFooter {
            data_block_start: 0,
            data_block_end: 1000,
            index_block_start: 1000,
            index_block_end: 1500,
            index_checksum: 0x12345678,
            bloom_block_start: 1500,
            bloom_block_end: 1600,
            bloom_checksum: 0x87654321,
        };

        let mut encoded = footer.encode();

        // Corrupt the footer checksum (last 4 bytes)
        let len = encoded.len();
        encoded[len - 1] ^= 0xFF; // Flip bits in last byte

        let result = SSTableFooter::decode(Cursor::new(&encoded));
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Footer checksum mismatch")
        );

        // Test index checksum validation
        let mut index = BTreeMap::new();
        index.insert(b"key1".to_vec(), 100u64);

        let mut index_block = Vec::new();
        index_block.extend_from_slice(&(index.len() as u64).to_be_bytes());

        let entry = IndexEntry {
            key: b"key1".to_vec(),
            offset: 100,
        };
        index_block.append(&mut entry.encode());

        // Correct checksum
        let correct_checksum = crc32fast::hash(&index_block);

        // Wrong checksum
        let wrong_checksum = correct_checksum ^ 0xFFFF;

        let footer = SSTableFooter {
            data_block_start: 0,
            data_block_end: 1000,
            index_block_start: 0,
            index_block_end: index_block.len() as u64,
            index_checksum: wrong_checksum,
            bloom_block_start: 0,
            bloom_block_end: 0,
            bloom_checksum: 0,
        };

        let result = read_sstable_index(Cursor::new(&index_block), &footer);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Index block checksum mismatch")
        );
    }

    #[test]
    fn test_sstable_decode_valid() {
        // Test decoding a valid SSTable with multiple entries
        // Create a memtable with test data
        let memtable = SkipMap::new();
        memtable.insert(b"apple".to_vec(), (RecordType::Put, b"red".to_vec()));
        memtable.insert(b"banana".to_vec(), (RecordType::Put, b"yellow".to_vec()));
        memtable.insert(b"cherry".to_vec(), (RecordType::Put, b"red".to_vec()));

        // Use flush_memtable to create SSTable with unique filename
        let file_id = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;
        let filename = flush_for_test(memtable, file_id);

        // Open the file for decoding
        let file = File::open(&filename).unwrap();

        // Decode SSTable
        let decoded = SSTable::decode(&file).unwrap();

        // Clean up
        std::fs::remove_file(&filename).unwrap();

        // Verify decoded structure
        assert_eq!(decoded.block.len(), 1, "Should have exactly one block");
        assert_eq!(
            decoded.index.len(),
            1,
            "Should have exactly one index entry"
        );

        // Verify block metadata
        let block = &decoded.block[0];
        assert_eq!(block.first_key, b"apple", "First key should be 'apple'");
        assert_eq!(
            block.last_key, b"cherry",
            "Last key should be 'cherry' (sorted order)"
        );
        assert_eq!(block.record_count, 3, "Block should contain 3 records");
        assert!(block.data_size > 0, "Block should have data");

        // Verify index entry points to the block
        let index_entry = &decoded.index[0];
        assert_eq!(
            index_entry.first_key, b"apple",
            "Index first_key should match block"
        );
        assert_eq!(
            index_entry.last_key, b"cherry",
            "Index last_key should match block"
        );
        assert_eq!(
            index_entry.block_offset, block.offset,
            "Index should point to block offset"
        );
        assert_eq!(
            index_entry.record_count, 3,
            "Index record_count should match block"
        );

        // Verify bloom filter contains our keys
        assert!(decoded.bloom.contains(b"apple"));
        assert!(decoded.bloom.contains(b"banana"));
        assert!(decoded.bloom.contains(b"cherry"));
    }

    #[test]
    fn test_sstable_decode_single_block() {
        // Test decoding an SSTable with a single entry
        let memtable = SkipMap::new();
        memtable.insert(b"key".to_vec(), (RecordType::Put, b"value".to_vec()));

        // Use flush_memtable to create SSTable with unique filename
        let file_id = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;
        let filename = flush_for_test(memtable, file_id);

        // Open the file for decoding
        let file = File::open(&filename).unwrap();

        // Decode SSTable
        let decoded = SSTable::decode(&file).unwrap();

        // Clean up
        std::fs::remove_file(&filename).unwrap();

        // Verify decoded structure
        assert_eq!(decoded.block.len(), 1, "Should have exactly one block");
        assert_eq!(
            decoded.index.len(),
            1,
            "Should have exactly one index entry"
        );

        // Verify block metadata
        let block = &decoded.block[0];
        assert_eq!(block.first_key, b"key", "First key should be 'key'");
        assert_eq!(block.last_key, b"key", "Last key should be 'key'");
        assert_eq!(block.record_count, 1, "Block should contain 1 record");
        assert!(block.data_size > 0, "Block should have data");

        // Verify index entry points to the block
        let index_entry = &decoded.index[0];
        assert_eq!(
            index_entry.first_key, b"key",
            "Index first_key should match block"
        );
        assert_eq!(
            index_entry.last_key, b"key",
            "Index last_key should match block"
        );
        assert_eq!(
            index_entry.block_offset, block.offset,
            "Index should point to block offset"
        );
        assert_eq!(
            index_entry.record_count, 1,
            "Index record_count should match block"
        );

        // Verify bloom filter
        assert!(decoded.bloom.contains(b"key"));
    }

    #[test]
    fn test_sstable_decode_empty_sstable() {
        // Test decoding an SSTable with no entries (valid structure but empty)
        let memtable = SkipMap::new();
        // Don't insert anything - empty memtable

        // Use flush_memtable to create SSTable with unique filename
        let file_id = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;
        let filename = flush_for_test(memtable, file_id);

        // Open the file for decoding
        let file = File::open(&filename).unwrap();

        // Decode SSTable
        let decoded = SSTable::decode(&file).unwrap();

        // Clean up
        std::fs::remove_file(&filename).unwrap();

        // Verify empty structure
        assert_eq!(decoded.block.len(), 0, "Should have no blocks");
        assert_eq!(decoded.index.len(), 0, "Should have no index entries");
    }

    #[test]
    fn test_sstable_decode_corrupted_index_checksum() {
        // Test that corrupted index checksum causes decode to fail
        let memtable = SkipMap::new();
        memtable.insert(b"key".to_vec(), (RecordType::Put, b"value".to_vec()));

        // Use flush_memtable to create valid SSTable with unique filename
        let file_id = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;
        let filename = flush_for_test(memtable, file_id);

        // Read the file data to corrupt it
        let mut sstable_data = std::fs::read(&filename).unwrap();

        // Read footer to find index checksum location
        let footer_offset = sstable_data.len() - FOOTER_SIZE as usize;
        let footer_data = &sstable_data[footer_offset..];
        let mut cursor = std::io::Cursor::new(footer_data);
        let _footer = SSTableFooter::decode(&mut cursor).unwrap();

        // Corrupt the index checksum in the footer (offset 32 in footer: 4 u64s = 32 bytes)
        let checksum_offset = footer_offset + 32;
        sstable_data[checksum_offset] ^= 0xFF; // Flip bits

        // Write corrupted data back
        std::fs::write(&filename, &sstable_data).unwrap();

        // Open the corrupted file for decoding
        let file = File::open(&filename).unwrap();

        // Decode should fail with IO error (checksum validation happens in verify_index_checksum)
        let result = SSTable::decode(&file);

        // Clean up
        std::fs::remove_file(&filename).unwrap();
        assert!(result.is_err());
        match result.unwrap_err() {
            DBError::IO(_) => {} // Expected: IO error from checksum mismatch
            other => panic!("Expected IO error, got: {:?}", other),
        }
    }

    #[test]
    fn test_sstable_decode_corrupted_bloom_checksum() {
        // Test that corrupted bloom filter checksum causes decode to fail
        let memtable = SkipMap::new();
        memtable.insert(b"key".to_vec(), (RecordType::Put, b"value".to_vec()));

        // Use flush_memtable to create valid SSTable with unique filename
        let file_id = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;
        let filename = flush_for_test(memtable, file_id);

        // Read the file data to corrupt it
        let mut sstable_data = std::fs::read(&filename).unwrap();

        // Corrupt the bloom checksum in the footer (offset 48 in footer: 6 u64s = 48 bytes)
        let footer_offset = sstable_data.len() - FOOTER_SIZE as usize;
        let checksum_offset = footer_offset + 48;
        sstable_data[checksum_offset] ^= 0xFF; // Flip bits

        // Write corrupted data back
        std::fs::write(&filename, &sstable_data).unwrap();

        // Open the corrupted file for decoding
        let file = File::open(&filename).unwrap();

        // Decode should fail with IO error (checksum validation happens in verify_bloom_checksum)
        let result = SSTable::decode(&file);

        // Clean up
        std::fs::remove_file(&filename).unwrap();
        assert!(result.is_err());
        match result.unwrap_err() {
            DBError::IO(_) => {} // Expected: IO error from checksum mismatch
            other => panic!("Expected IO error, got: {:?}", other),
        }
    }

    #[test]
    fn test_sstable_decode_corrupted_footer_checksum() {
        // Test that corrupted footer checksum causes decode to fail
        let memtable = SkipMap::new();
        memtable.insert(b"key".to_vec(), (RecordType::Put, b"value".to_vec()));

        // Use flush_memtable to create valid SSTable with unique filename
        let file_id = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;
        let filename = flush_for_test(memtable, file_id);

        // Read the file data to corrupt it
        let mut sstable_data = std::fs::read(&filename).unwrap();

        // Corrupt the footer checksum (last 4 bytes of the file)
        let len = sstable_data.len();
        sstable_data[len - 1] ^= 0xFF;

        // Write corrupted data back
        std::fs::write(&filename, &sstable_data).unwrap();

        // Open the corrupted file for decoding
        let file = File::open(&filename).unwrap();

        // Decode should fail with IO error (checksum validation happens in SSTableFooter::decode)
        let result = SSTable::decode(&file);

        // Clean up
        std::fs::remove_file(&filename).unwrap();
        assert!(result.is_err());
        match result.unwrap_err() {
            DBError::IO(_) => {} // Expected: IO error from checksum mismatch
            other => panic!("Expected IO error, got: {:?}", other),
        }
    }

    #[test]
    fn test_sstable_decode_invalid_magic_number() {
        // Test that invalid magic number causes decode to fail
        let memtable = SkipMap::new();
        memtable.insert(b"key".to_vec(), (RecordType::Put, b"value".to_vec()));

        // Use flush_memtable to create valid SSTable with unique filename
        let file_id = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;
        let filename = flush_for_test(memtable, file_id);

        // Read the file data to corrupt it
        let mut sstable_data = std::fs::read(&filename).unwrap();

        // Corrupt the magic number in footer (offset 52 in footer: 6 u64s + 2 u32s = 56 bytes, magic at 52)
        let footer_offset = sstable_data.len() - FOOTER_SIZE as usize;
        let magic_offset = footer_offset + 52;
        sstable_data[magic_offset] ^= 0xFF; // Flip bits

        // Write corrupted data back
        std::fs::write(&filename, &sstable_data).unwrap();

        // Open the corrupted file for decoding
        let file = File::open(&filename).unwrap();

        // Decode should fail with IO error (magic number validation happens in SSTableFooter::decode)
        let result = SSTable::decode(&file);

        // Clean up
        std::fs::remove_file(&filename).unwrap();
        assert!(result.is_err());
        match result.unwrap_err() {
            DBError::IO(_) => {} // Expected: IO error from invalid magic number
            other => panic!("Expected IO error, got: {:?}", other),
        }
    }

    #[test]
    fn test_sstable_decode_verifies_bloom_contains_keys() {
        // Test that decoded bloom filter contains the expected keys
        let memtable = SkipMap::new();
        memtable.insert(b"alpha".to_vec(), (RecordType::Put, b"1".to_vec()));
        memtable.insert(b"beta".to_vec(), (RecordType::Put, b"2".to_vec()));
        memtable.insert(b"gamma".to_vec(), (RecordType::Put, b"3".to_vec()));

        // Use flush_memtable to create SSTable with unique filename
        let file_id = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;
        let filename = flush_for_test(memtable, file_id);

        // Open the file for decoding
        let file = File::open(&filename).unwrap();

        // Decode SSTable
        let decoded = SSTable::decode(&file).unwrap();

        // Clean up
        std::fs::remove_file(&filename).unwrap();

        // Verify block metadata
        assert_eq!(decoded.block.len(), 1, "Should have exactly one block");
        let block = &decoded.block[0];
        assert_eq!(
            block.first_key, b"alpha",
            "First key should be 'alpha' (sorted order)"
        );
        assert_eq!(
            block.last_key, b"gamma",
            "Last key should be 'gamma' (sorted order)"
        );
        assert_eq!(block.record_count, 3, "Block should contain 3 records");

        // Verify index entry
        assert_eq!(
            decoded.index.len(),
            1,
            "Should have exactly one index entry"
        );
        let index_entry = &decoded.index[0];
        assert_eq!(
            index_entry.first_key, b"alpha",
            "Index first_key should be 'alpha'"
        );
        assert_eq!(
            index_entry.last_key, b"gamma",
            "Index last_key should be 'gamma'"
        );
        assert_eq!(index_entry.record_count, 3, "Index should show 3 records");

        // Verify bloom filter contains expected keys
        assert!(
            decoded.bloom.contains(b"alpha"),
            "Bloom filter should contain 'alpha'"
        );
        assert!(
            decoded.bloom.contains(b"beta"),
            "Bloom filter should contain 'beta'"
        );
        assert!(
            decoded.bloom.contains(b"gamma"),
            "Bloom filter should contain 'gamma'"
        );
        assert!(
            !decoded.bloom.contains(b"nonexistent"),
            "Bloom filter should not contain 'nonexistent'"
        );
    }

    #[test]
    fn test_sstable_decode_block_data_single_record() {
        // Positive test: Verifies that SSTable::decode populates block.data with exactly 1 record
        // This tests the end-to-end integration: flush_memtable → SSTable::decode → Block::decode → block.data

        // Arrange: Create memtable with 1 record
        let memtable = SkipMap::new();
        memtable.insert(
            b"testkey".to_vec(),
            (RecordType::Put, b"testvalue".to_vec()),
        );

        // Act: Flush and decode with unique filename
        let file_id = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;
        let filename = flush_for_test(memtable, file_id);

        // Open the file for decoding
        let file = File::open(&filename).unwrap();

        let decoded = SSTable::decode(&file).unwrap();

        // Clean up
        std::fs::remove_file(&filename).unwrap();

        // Assert: Verify block.data field is populated with 1 record
        assert_eq!(decoded.block.len(), 1, "Should have exactly one block");
        let block = &decoded.block[0];

        assert!(
            block.data.is_some(),
            "block.data should be populated after decode"
        );

        let records = block.data.as_ref().unwrap();
        assert_eq!(
            records.len(),
            1,
            "block.data should contain exactly 1 record"
        );

        // Verify the record's key, value, and type match expected
        let record = find_record(records, b"testkey").expect("Key 'testkey' should exist");
        assert_eq!(record.key, b"testkey", "Record key should match");
        assert_eq!(record.value, b"testvalue", "Record value should match");
        assert_eq!(
            record.record_type,
            RecordType::Put,
            "Record type should be Put"
        );
    }

    #[test]
    fn test_sstable_decode_block_data_multiple_records() {
        // Positive test: Verifies that SSTable::decode populates block.data with all records in sorted order
        // SkipMap automatically sorts keys, so we expect: bird, cat, dog, fish

        // Arrange: Create memtable with 4 records (inserted out of order)
        let memtable = SkipMap::new();
        memtable.insert(b"dog".to_vec(), (RecordType::Put, b"woof".to_vec()));
        memtable.insert(b"cat".to_vec(), (RecordType::Put, b"meow".to_vec()));
        memtable.insert(b"bird".to_vec(), (RecordType::Put, b"tweet".to_vec()));
        memtable.insert(b"fish".to_vec(), (RecordType::Put, b"blub".to_vec()));

        // Act: Flush and decode with unique filename
        let file_id = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;
        let filename = flush_for_test(memtable, file_id);

        // Open the file for decoding
        let file = File::open(&filename).unwrap();

        let decoded = SSTable::decode(&file).unwrap();

        // Clean up
        std::fs::remove_file(&filename).unwrap();

        // Assert: Verify block.data contains all 4 records in sorted order
        assert_eq!(decoded.block.len(), 1, "Should have exactly one block");
        let block = &decoded.block[0];

        assert!(
            block.data.is_some(),
            "block.data should be populated after decode"
        );

        let records = block.data.as_ref().unwrap();
        assert_eq!(
            records.len(),
            4,
            "block.data should contain exactly 4 records"
        );

        // Verify records are in sorted order (SkipMap sorts keys)
        // Access each record by key using BTreeMap.get()
        let record_bird = find_record(records, b"bird").expect("Key 'bird' should exist");
        assert_eq!(record_bird.key, b"bird", "First record should be 'bird'");
        assert_eq!(
            record_bird.value, b"tweet",
            "First record value should match"
        );
        assert_eq!(record_bird.record_type, RecordType::Put);

        let record_cat = find_record(records, b"cat").expect("Key 'cat' should exist");
        assert_eq!(record_cat.key, b"cat", "Second record should be 'cat'");
        assert_eq!(
            record_cat.value, b"meow",
            "Second record value should match"
        );
        assert_eq!(record_cat.record_type, RecordType::Put);

        let record_dog = find_record(records, b"dog").expect("Key 'dog' should exist");
        assert_eq!(record_dog.key, b"dog", "Third record should be 'dog'");
        assert_eq!(record_dog.value, b"woof", "Third record value should match");
        assert_eq!(record_dog.record_type, RecordType::Put);

        let record_fish = find_record(records, b"fish").expect("Key 'fish' should exist");
        assert_eq!(record_fish.key, b"fish", "Fourth record should be 'fish'");
        assert_eq!(
            record_fish.value, b"blub",
            "Fourth record value should match"
        );
        assert_eq!(record_fish.record_type, RecordType::Put);
    }

    #[test]
    fn test_sstable_decode_block_data_preserves_tombstones() {
        // Positive test: Verifies that SSTable::decode preserves RecordType::Delete (tombstones) in block.data
        // This ensures the decoding correctly maintains record type information

        // Arrange: Create memtable with mixed Put and Delete records
        let memtable = SkipMap::new();
        memtable.insert(b"active".to_vec(), (RecordType::Put, b"alive".to_vec()));
        memtable.insert(b"deleted".to_vec(), (RecordType::Delete, b"".to_vec()));
        memtable.insert(
            b"updated".to_vec(),
            (RecordType::Put, b"new_value".to_vec()),
        );

        // Act: Flush and decode with unique filename
        let file_id = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;
        let filename = flush_for_test(memtable, file_id);

        // Open the file for decoding
        let file = File::open(&filename).unwrap();

        let decoded = SSTable::decode(&file).unwrap();

        // Clean up
        std::fs::remove_file(&filename).unwrap();

        // Assert: Verify block.data contains all 3 records with correct types
        assert_eq!(decoded.block.len(), 1, "Should have exactly one block");
        let block = &decoded.block[0];

        assert!(
            block.data.is_some(),
            "block.data should be populated after decode"
        );

        let records = block.data.as_ref().unwrap();
        assert_eq!(
            records.len(),
            3,
            "block.data should contain exactly 3 records"
        );

        // SkipMap sorts keys: active, deleted, updated
        // Access each record by key using BTreeMap.get()
        let record_active = find_record(records, b"active").expect("Key 'active' should exist");
        assert_eq!(
            record_active.key, b"active",
            "First record should be 'active'"
        );
        assert_eq!(record_active.value, b"alive");
        assert_eq!(
            record_active.record_type,
            RecordType::Put,
            "First record should be Put"
        );

        let record_deleted = find_record(records, b"deleted").expect("Key 'deleted' should exist");
        assert_eq!(
            record_deleted.key, b"deleted",
            "Second record should be 'deleted'"
        );
        assert_eq!(
            record_deleted.value, b"",
            "Delete record should have empty value"
        );
        assert_eq!(
            record_deleted.record_type,
            RecordType::Delete,
            "Second record should be Delete (tombstone)"
        );

        let record_updated = find_record(records, b"updated").expect("Key 'updated' should exist");
        assert_eq!(
            record_updated.key, b"updated",
            "Third record should be 'updated'"
        );
        assert_eq!(record_updated.value, b"new_value");
        assert_eq!(
            record_updated.record_type,
            RecordType::Put,
            "Third record should be Put"
        );
    }

    #[test]
    fn test_sstable_decode_block_data_multiple_blocks() {
        // Positive test: Verifies that SSTable::decode populates block.data for ALL blocks
        // when the SSTable spans multiple 4KB blocks. This ensures the decoding loop
        // in SSTable::decode properly handles each block independently.

        // Arrange: Create memtable with 50 small records
        // This should create at least 1 block, and we verify block.data is populated for all blocks
        let memtable = SkipMap::new();
        for i in 0..50 {
            let key = format!("testkey{:03}", i);
            let value = format!("testvalue{:03}", i);
            memtable.insert(
                key.as_bytes().to_vec(),
                (RecordType::Put, value.as_bytes().to_vec()),
            );
        }

        // Act: Flush and decode with unique filename
        let file_id = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;
        let filename = flush_for_test(memtable, file_id);

        // Open the file for decoding
        let file = File::open(&filename).unwrap();

        let decoded = SSTable::decode(&file).unwrap();

        // Clean up
        std::fs::remove_file(&filename).unwrap();

        // Assert: Verify block.data is populated for ALL blocks (whether 1 or more)
        assert!(decoded.block.len() >= 1, "Should have at least 1 block");

        // Verify each block has block.data populated
        let mut total_records = 0;
        for (i, block) in decoded.block.iter().enumerate() {
            assert!(
                block.data.is_some(),
                "Block {} should have data populated",
                i
            );

            let records = block.data.as_ref().unwrap();
            assert_eq!(
                records.len() as u32,
                block.record_count,
                "Block {} data length should match record_count",
                i
            );

            // Verify each record in this block is valid
            for record in records.iter() {
                assert!(
                    !record.key.is_empty(),
                    "Block {} record should have non-empty key",
                    i
                );
                assert!(
                    !record.value.is_empty(),
                    "Block {} record should have non-empty value",
                    i
                );
                assert_eq!(
                    record.record_type,
                    RecordType::Put,
                    "Block {} record should be Put type",
                    i
                );
            }

            total_records += records.len();
        }

        // Verify we decoded all 50 records across all blocks
        assert_eq!(
            total_records, 50,
            "Should have decoded all 50 records across all blocks"
        );
    }

    #[test]
    fn test_async_sstable_decode_valid() {
        run_async_test(async {
            let memtable = SkipMap::new();
            memtable.insert(b"apple".to_vec(), (RecordType::Put, b"red".to_vec()));
            memtable.insert(b"banana".to_vec(), (RecordType::Put, b"yellow".to_vec()));
            memtable.insert(b"cherry".to_vec(), (RecordType::Put, b"red".to_vec()));

            let file_id = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos() as u64;
            let filename = flush_for_test(memtable, file_id);

            let file = tokio::fs::File::open(&filename).await.unwrap();
            let decoded = SSTable::decode_async(file).await.unwrap();

            std::fs::remove_file(&filename).unwrap();

            assert_eq!(decoded.block.len(), 1, "Should have exactly one block");
            assert_eq!(
                decoded.index.len(),
                1,
                "Should have exactly one index entry"
            );

            let block = &decoded.block[0];
            assert_eq!(block.first_key, b"apple", "First key should be 'apple'");
            assert_eq!(
                block.last_key, b"cherry",
                "Last key should be 'cherry' (sorted order)"
            );
            assert_eq!(block.record_count, 3, "Block should contain 3 records");
            assert!(block.data_size > 0, "Block should have data");

            let index_entry = &decoded.index[0];
            assert_eq!(index_entry.first_key, b"apple");
            assert_eq!(index_entry.last_key, b"cherry");
            assert_eq!(index_entry.block_offset, block.offset);
            assert_eq!(index_entry.record_count, 3);

            assert!(decoded.bloom.contains(b"apple"));
            assert!(decoded.bloom.contains(b"banana"));
            assert!(decoded.bloom.contains(b"cherry"));
        });
    }

    #[test]
    fn test_async_sstable_decode_single_block() {
        run_async_test(async {
            let memtable = SkipMap::new();
            memtable.insert(b"key".to_vec(), (RecordType::Put, b"value".to_vec()));

            let file_id = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos() as u64;
            let filename = flush_for_test(memtable, file_id);

            let file = tokio::fs::File::open(&filename).await.unwrap();
            let decoded = SSTable::decode_async(file).await.unwrap();

            std::fs::remove_file(&filename).unwrap();

            assert_eq!(decoded.block.len(), 1, "Should have exactly one block");
            assert_eq!(
                decoded.index.len(),
                1,
                "Should have exactly one index entry"
            );

            let block = &decoded.block[0];
            assert_eq!(block.first_key, b"key", "First key should be 'key'");
            assert_eq!(block.last_key, b"key", "Last key should be 'key'");
            assert_eq!(block.record_count, 1, "Block should contain 1 record");
            assert!(block.data_size > 0, "Block should have data");

            let index_entry = &decoded.index[0];
            assert_eq!(index_entry.first_key, b"key");
            assert_eq!(index_entry.last_key, b"key");
            assert_eq!(index_entry.block_offset, block.offset);
            assert_eq!(index_entry.record_count, 1);

            assert!(decoded.bloom.contains(b"key"));
        });
    }

    #[test]
    fn test_async_sstable_decode_empty_sstable() {
        run_async_test(async {
            let memtable = SkipMap::new();

            let file_id = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos() as u64;
            let filename = flush_for_test(memtable, file_id);

            let file = tokio::fs::File::open(&filename).await.unwrap();
            let decoded = SSTable::decode_async(file).await.unwrap();

            std::fs::remove_file(&filename).unwrap();

            assert_eq!(decoded.block.len(), 0, "Should have no blocks");
            assert_eq!(decoded.index.len(), 0, "Should have no index entries");
        });
    }

    #[test]
    fn test_async_sstable_decode_corrupted_index_checksum() {
        run_async_test(async {
            let memtable = SkipMap::new();
            memtable.insert(b"key".to_vec(), (RecordType::Put, b"value".to_vec()));

            let file_id = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos() as u64;
            let filename = flush_for_test(memtable, file_id);

            let mut sstable_data = std::fs::read(&filename).unwrap();

            let footer_offset = sstable_data.len() - FOOTER_SIZE as usize;
            let checksum_offset = footer_offset + 32;
            sstable_data[checksum_offset] ^= 0xFF;

            std::fs::write(&filename, &sstable_data).unwrap();

            let file = tokio::fs::File::open(&filename).await.unwrap();
            let result = SSTable::decode_async(file).await;

            std::fs::remove_file(&filename).unwrap();
            assert!(result.is_err());
            match result.unwrap_err() {
                DBError::IO(_) => {}
                other => panic!("Expected IO error, got: {:?}", other),
            }
        });
    }

    #[test]
    fn test_async_sstable_decode_corrupted_bloom_checksum() {
        run_async_test(async {
            let memtable = SkipMap::new();
            memtable.insert(b"key".to_vec(), (RecordType::Put, b"value".to_vec()));

            let file_id = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos() as u64;
            let filename = flush_for_test(memtable, file_id);

            let mut sstable_data = std::fs::read(&filename).unwrap();

            let footer_offset = sstable_data.len() - FOOTER_SIZE as usize;
            let checksum_offset = footer_offset + 48;
            sstable_data[checksum_offset] ^= 0xFF;

            std::fs::write(&filename, &sstable_data).unwrap();

            let file = tokio::fs::File::open(&filename).await.unwrap();
            let result = SSTable::decode_async(file).await;

            std::fs::remove_file(&filename).unwrap();
            assert!(result.is_err());
            match result.unwrap_err() {
                DBError::IO(_) => {}
                other => panic!("Expected IO error, got: {:?}", other),
            }
        });
    }

    #[test]
    fn test_async_sstable_decode_corrupted_footer_checksum() {
        run_async_test(async {
            let memtable = SkipMap::new();
            memtable.insert(b"key".to_vec(), (RecordType::Put, b"value".to_vec()));

            let file_id = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos() as u64;
            let filename = flush_for_test(memtable, file_id);

            let mut sstable_data = std::fs::read(&filename).unwrap();

            let len = sstable_data.len();
            sstable_data[len - 1] ^= 0xFF;

            std::fs::write(&filename, &sstable_data).unwrap();

            let file = tokio::fs::File::open(&filename).await.unwrap();
            let result = SSTable::decode_async(file).await;

            std::fs::remove_file(&filename).unwrap();
            assert!(result.is_err());
            match result.unwrap_err() {
                DBError::IO(_) => {}
                other => panic!("Expected IO error, got: {:?}", other),
            }
        });
    }

    #[test]
    fn test_async_sstable_decode_invalid_magic_number() {
        run_async_test(async {
            let memtable = SkipMap::new();
            memtable.insert(b"key".to_vec(), (RecordType::Put, b"value".to_vec()));

            let file_id = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos() as u64;
            let filename = flush_for_test(memtable, file_id);

            let mut sstable_data = std::fs::read(&filename).unwrap();

            let footer_offset = sstable_data.len() - FOOTER_SIZE as usize;
            let magic_offset = footer_offset + 52;
            sstable_data[magic_offset] ^= 0xFF;

            std::fs::write(&filename, &sstable_data).unwrap();

            let file = tokio::fs::File::open(&filename).await.unwrap();
            let result = SSTable::decode_async(file).await;

            std::fs::remove_file(&filename).unwrap();
            assert!(result.is_err());
            match result.unwrap_err() {
                DBError::IO(_) => {}
                other => panic!("Expected IO error, got: {:?}", other),
            }
        });
    }

    #[test]
    fn test_async_sstable_decode_verifies_bloom_contains_keys() {
        run_async_test(async {
            let memtable = SkipMap::new();
            memtable.insert(b"alpha".to_vec(), (RecordType::Put, b"1".to_vec()));
            memtable.insert(b"beta".to_vec(), (RecordType::Put, b"2".to_vec()));
            memtable.insert(b"gamma".to_vec(), (RecordType::Put, b"3".to_vec()));

            let file_id = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos() as u64;
            let filename = flush_for_test(memtable, file_id);

            let file = tokio::fs::File::open(&filename).await.unwrap();
            let decoded = SSTable::decode_async(file).await.unwrap();

            std::fs::remove_file(&filename).unwrap();

            assert_eq!(decoded.block.len(), 1, "Should have exactly one block");
            let block = &decoded.block[0];
            assert_eq!(block.first_key, b"alpha");
            assert_eq!(block.last_key, b"gamma");
            assert_eq!(block.record_count, 3);

            assert_eq!(
                decoded.index.len(),
                1,
                "Should have exactly one index entry"
            );
            let index_entry = &decoded.index[0];
            assert_eq!(index_entry.first_key, b"alpha");
            assert_eq!(index_entry.last_key, b"gamma");
            assert_eq!(index_entry.record_count, 3);

            assert!(decoded.bloom.contains(b"alpha"));
            assert!(decoded.bloom.contains(b"beta"));
            assert!(decoded.bloom.contains(b"gamma"));
            assert!(!decoded.bloom.contains(b"nonexistent"));
        });
    }

    #[test]
    fn test_async_sstable_decode_block_data_single_record() {
        run_async_test(async {
            let memtable = SkipMap::new();
            memtable.insert(
                b"testkey".to_vec(),
                (RecordType::Put, b"testvalue".to_vec()),
            );

            let file_id = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos() as u64;
            let filename = flush_for_test(memtable, file_id);

            let file = tokio::fs::File::open(&filename).await.unwrap();
            let decoded = SSTable::decode_async(file).await.unwrap();

            std::fs::remove_file(&filename).unwrap();

            assert_eq!(decoded.block.len(), 1, "Should have exactly one block");
            let block = &decoded.block[0];

            assert!(block.data.is_some(), "block.data should be populated");

            let records = block.data.as_ref().unwrap();
            assert_eq!(records.len(), 1, "block.data should contain 1 record");

            let record = find_record(records, b"testkey").expect("Key 'testkey' should exist");
            assert_eq!(record.key, b"testkey");
            assert_eq!(record.value, b"testvalue");
            assert_eq!(record.record_type, RecordType::Put);
        });
    }

    #[test]
    fn test_async_sstable_decode_block_data_multiple_records() {
        run_async_test(async {
            let memtable = SkipMap::new();
            memtable.insert(b"dog".to_vec(), (RecordType::Put, b"woof".to_vec()));
            memtable.insert(b"cat".to_vec(), (RecordType::Put, b"meow".to_vec()));
            memtable.insert(b"bird".to_vec(), (RecordType::Put, b"tweet".to_vec()));
            memtable.insert(b"fish".to_vec(), (RecordType::Put, b"blub".to_vec()));

            let file_id = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos() as u64;
            let filename = flush_for_test(memtable, file_id);

            let file = tokio::fs::File::open(&filename).await.unwrap();
            let decoded = SSTable::decode_async(file).await.unwrap();

            std::fs::remove_file(&filename).unwrap();

            assert_eq!(decoded.block.len(), 1, "Should have exactly one block");
            let block = &decoded.block[0];

            assert!(block.data.is_some(), "block.data should be populated");

            let records = block.data.as_ref().unwrap();
            assert_eq!(records.len(), 4, "block.data should contain 4 records");

            let record_bird = find_record(records, b"bird").unwrap();
            assert_eq!(record_bird.key, b"bird");
            assert_eq!(record_bird.value, b"tweet");
            assert_eq!(record_bird.record_type, RecordType::Put);

            let record_cat = find_record(records, b"cat").unwrap();
            assert_eq!(record_cat.key, b"cat");
            assert_eq!(record_cat.value, b"meow");
            assert_eq!(record_cat.record_type, RecordType::Put);

            let record_dog = find_record(records, b"dog").unwrap();
            assert_eq!(record_dog.key, b"dog");
            assert_eq!(record_dog.value, b"woof");
            assert_eq!(record_dog.record_type, RecordType::Put);

            let record_fish = find_record(records, b"fish").unwrap();
            assert_eq!(record_fish.key, b"fish");
            assert_eq!(record_fish.value, b"blub");
            assert_eq!(record_fish.record_type, RecordType::Put);
        });
    }

    #[test]
    fn test_async_sstable_decode_block_data_preserves_tombstones() {
        run_async_test(async {
            let memtable = SkipMap::new();
            memtable.insert(b"active".to_vec(), (RecordType::Put, b"alive".to_vec()));
            memtable.insert(b"deleted".to_vec(), (RecordType::Delete, b"".to_vec()));
            memtable.insert(
                b"updated".to_vec(),
                (RecordType::Put, b"new_value".to_vec()),
            );

            let file_id = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos() as u64;
            let filename = flush_for_test(memtable, file_id);

            let file = tokio::fs::File::open(&filename).await.unwrap();
            let decoded = SSTable::decode_async(file).await.unwrap();

            std::fs::remove_file(&filename).unwrap();

            assert_eq!(decoded.block.len(), 1, "Should have exactly one block");
            let block = &decoded.block[0];

            assert!(block.data.is_some(), "block.data should be populated");

            let records = block.data.as_ref().unwrap();
            assert_eq!(records.len(), 3, "block.data should contain 3 records");

            let record_active = find_record(records, b"active").unwrap();
            assert_eq!(record_active.key, b"active");
            assert_eq!(record_active.value, b"alive");
            assert_eq!(record_active.record_type, RecordType::Put);

            let record_deleted = find_record(records, b"deleted").unwrap();
            assert_eq!(record_deleted.key, b"deleted");
            assert_eq!(record_deleted.value, b"");
            assert_eq!(record_deleted.record_type, RecordType::Delete);

            let record_updated = find_record(records, b"updated").unwrap();
            assert_eq!(record_updated.key, b"updated");
            assert_eq!(record_updated.value, b"new_value");
            assert_eq!(record_updated.record_type, RecordType::Put);
        });
    }

    #[test]
    fn test_async_sstable_decode_block_data_multiple_blocks() {
        run_async_test(async {
            let memtable = SkipMap::new();
            for i in 0..50 {
                let key = format!("testkey{:03}", i);
                let value = format!("testvalue{:03}", i);
                memtable.insert(
                    key.as_bytes().to_vec(),
                    (RecordType::Put, value.as_bytes().to_vec()),
                );
            }

            let file_id = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos() as u64;
            let filename = flush_for_test(memtable, file_id);

            let file = tokio::fs::File::open(&filename).await.unwrap();
            let decoded = SSTable::decode_async(file).await.unwrap();

            std::fs::remove_file(&filename).unwrap();

            assert!(decoded.block.len() >= 1, "Should have at least 1 block");

            let mut total_records = 0;
            for (i, block) in decoded.block.iter().enumerate() {
                assert!(block.data.is_some(), "Block {} should have data", i);

                let records = block.data.as_ref().unwrap();
                assert_eq!(
                    records.len() as u32,
                    block.record_count,
                    "Block {} data length should match record_count",
                    i
                );

                for record in records.iter() {
                    assert!(!record.key.is_empty(), "Block {} record should have key", i);
                    assert!(
                        !record.value.is_empty(),
                        "Block {} record should have value",
                        i
                    );
                    assert_eq!(
                        record.record_type,
                        RecordType::Put,
                        "Block {} record should be Put",
                        i
                    );
                }

                total_records += records.len();
            }

            assert_eq!(
                total_records, 50,
                "Should have decoded all 50 records across all blocks"
            );
        });
    }

    // ============================================================================
    // merge_sstables Tests
    // ============================================================================

    #[test]
    fn test_merge_sstables_empty() {
        // Test merging empty memtable with empty SSTables
        let memtable: SkipMap<Vec<u8>, (RecordType, Vec<u8>)> = SkipMap::new();
        let sstables: Vec<Block> = vec![];

        let result = merge_sstables(&memtable, &sstables).unwrap();

        assert_eq!(
            result.len(),
            0,
            "Merging empty inputs should return empty result"
        );
    }

    #[test]
    fn test_merge_sstables_memtable_only() {
        // Test merging memtable with no SSTables - should return sorted memtable records
        let memtable: SkipMap<Vec<u8>, (RecordType, Vec<u8>)> = SkipMap::new();
        memtable.insert(b"dog".to_vec(), (RecordType::Put, b"woof".to_vec()));
        memtable.insert(b"cat".to_vec(), (RecordType::Put, b"meow".to_vec()));
        memtable.insert(b"bird".to_vec(), (RecordType::Put, b"tweet".to_vec()));

        let sstables: Vec<Block> = vec![];

        let result = merge_sstables(&memtable, &sstables).unwrap();

        assert_eq!(result.len(), 3, "Should have 3 records from memtable");

        // Verify sorted order (bird, cat, dog)
        assert_eq!(result[0].key, b"bird", "First record should be 'bird'");
        assert_eq!(result[0].value, b"tweet");
        assert_eq!(result[0].record_type, RecordType::Put);

        assert_eq!(result[1].key, b"cat", "Second record should be 'cat'");
        assert_eq!(result[1].value, b"meow");

        assert_eq!(result[2].key, b"dog", "Third record should be 'dog'");
        assert_eq!(result[2].value, b"woof");
    }

    #[test]
    fn test_merge_sstables_sstable_only() {
        // Test merging empty memtable with SSTable data - should return sorted SSTable records
        let memtable: SkipMap<Vec<u8>, (RecordType, Vec<u8>)> = SkipMap::new();

        // Create a block with data
        let block_data = vec![
            Record::new(b"apple".to_vec(), b"red".to_vec(), RecordType::Put, 1000),
            Record::new(
                b"banana".to_vec(),
                b"yellow".to_vec(),
                RecordType::Put,
                1000,
            ),
        ];

        let block = Block {
            offset: 0,
            first_key: b"apple".to_vec(),
            last_key: b"banana".to_vec(),
            record_count: 2,
            data_size: 100,
            data: Some(block_data),
        };

        let sstables = vec![block];

        let result = merge_sstables(&memtable, &sstables).unwrap();

        assert_eq!(result.len(), 2, "Should have 2 records from SSTable");

        // Verify sorted order (apple, banana)
        assert_eq!(result[0].key, b"apple", "First record should be 'apple'");
        assert_eq!(result[0].value, b"red");
        assert_eq!(result[0].record_type, RecordType::Put);

        assert_eq!(result[1].key, b"banana", "Second record should be 'banana'");
        assert_eq!(result[1].value, b"yellow");
    }

    #[test]
    fn test_merge_sstables_memtable_and_sstable() {
        // Test merging memtable and SSTable with non-overlapping keys
        let memtable: SkipMap<Vec<u8>, (RecordType, Vec<u8>)> = SkipMap::new();
        memtable.insert(b"dog".to_vec(), (RecordType::Put, b"woof".to_vec()));
        memtable.insert(b"cat".to_vec(), (RecordType::Put, b"meow".to_vec()));

        // Create SSTable block
        let block_data = vec![
            Record::new(b"apple".to_vec(), b"red".to_vec(), RecordType::Put, 1000),
            Record::new(
                b"banana".to_vec(),
                b"yellow".to_vec(),
                RecordType::Put,
                1000,
            ),
        ];

        let block = Block {
            offset: 0,
            first_key: b"apple".to_vec(),
            last_key: b"banana".to_vec(),
            record_count: 2,
            data_size: 100,
            data: Some(block_data),
        };

        let sstables = vec![block];

        let result = merge_sstables(&memtable, &sstables).unwrap();

        assert_eq!(result.len(), 4, "Should have 4 merged records");

        // Verify sorted order: apple, banana, cat, dog
        assert_eq!(result[0].key, b"apple");
        assert_eq!(result[1].key, b"banana");
        assert_eq!(result[2].key, b"cat");
        assert_eq!(result[3].key, b"dog");
    }

    #[test]
    fn test_merge_sstables_duplicate_keys_memtable_wins() {
        // Test merging records with duplicate keys from memtable and SSTable
        // Current implementation returns all records sorted (no deduplication yet)
        let memtable: SkipMap<Vec<u8>, (RecordType, Vec<u8>)> = SkipMap::new();
        memtable.insert(b"key1".to_vec(), (RecordType::Put, b"new_value".to_vec()));

        // Create SSTable block with same key
        let block_data = vec![Record::new(
            b"key1".to_vec(),
            b"old_value".to_vec(),
            RecordType::Put,
            1000,
        )];

        let block = Block {
            offset: 0,
            first_key: b"key1".to_vec(),
            last_key: b"key1".to_vec(),
            record_count: 1,
            data_size: 50,
            data: Some(block_data),
        };

        let sstables = vec![block];

        let result = merge_sstables(&memtable, &sstables).unwrap();

        // Current implementation returns both records sorted by key, then by timestamp (desc)
        assert_eq!(result.len(), 1, "Should have 1 records");

        // Both records should have key "key1", sorted by timestamp (newer first)
        assert_eq!(result[0].key, b"key1");

        assert_eq!(
            result[0].value, b"new_value",
            "First should be memtable record"
        );

        assert!(
            result[0].timestamp != 0,
            "First should have memtable timestamp (0)"
        );
    }

    #[test]
    fn test_merge_sstables_duplicate_keys_sstable_wins() {
        // Test merging records with duplicate keys from multiple SSTables
        // Current implementation returns all records sorted (no deduplication yet)
        let memtable: SkipMap<Vec<u8>, (RecordType, Vec<u8>)> = SkipMap::new();

        // Create two SSTable blocks with same key but different timestamps
        let block_data1 = vec![Record::new(
            b"key1".to_vec(),
            b"old_value".to_vec(),
            RecordType::Put,
            1000,
        )];

        let block1 = Block {
            offset: 0,
            first_key: b"key1".to_vec(),
            last_key: b"key1".to_vec(),
            record_count: 1,
            data_size: 50,
            data: Some(block_data1),
        };

        let block_data2 = vec![Record::new(
            b"key1".to_vec(),
            b"new_value".to_vec(),
            RecordType::Put,
            2000,
        )];

        let block2 = Block {
            offset: 100,
            first_key: b"key1".to_vec(),
            last_key: b"key1".to_vec(),
            record_count: 1,
            data_size: 50,
            data: Some(block_data2),
        };

        let sstables = vec![block1, block2];

        let result = merge_sstables(&memtable, &sstables).unwrap();

        // Current implementation returns both records sorted by key, then by timestamp (desc)
        assert_eq!(
            result.len(),
            1,
            "Should have 1 records (no deduplication yet)"
        );

        // Both records should have key "key1", sorted by timestamp (newer first)
        assert_eq!(result[0].key, b"key1");

        // Verify newer record comes first due to Record::Ord impl (descending timestamp)
        assert_eq!(
            result[0].timestamp, 2000,
            "First should have newer timestamp"
        );
        assert_eq!(
            result[0].value, b"new_value",
            "First should be newer record"
        );
    }

    #[test]
    fn test_merge_sstables_multiple_sstables() {
        // Test merging multiple SSTables with non-overlapping keys
        let memtable: SkipMap<Vec<u8>, (RecordType, Vec<u8>)> = SkipMap::new();
        memtable.insert(b"zebra".to_vec(), (RecordType::Put, b"stripes".to_vec()));

        // SSTable 1
        let block_data1 = vec![Record::new(
            b"apple".to_vec(),
            b"red".to_vec(),
            RecordType::Put,
            1000,
        )];

        let block1 = Block {
            offset: 0,
            first_key: b"apple".to_vec(),
            last_key: b"apple".to_vec(),
            record_count: 1,
            data_size: 50,
            data: Some(block_data1),
        };

        // SSTable 2
        let block_data2 = vec![Record::new(
            b"mango".to_vec(),
            b"orange".to_vec(),
            RecordType::Put,
            1000,
        )];

        let block2 = Block {
            offset: 100,
            first_key: b"mango".to_vec(),
            last_key: b"mango".to_vec(),
            record_count: 1,
            data_size: 50,
            data: Some(block_data2),
        };

        let sstables = vec![block1, block2];

        let result = merge_sstables(&memtable, &sstables).unwrap();

        assert_eq!(result.len(), 3, "Should have 3 records from all sources");

        // Verify sorted order: apple, mango, zebra
        assert_eq!(result[0].key, b"apple");
        assert_eq!(result[1].key, b"mango");
        assert_eq!(result[2].key, b"zebra");
    }

    #[test]
    fn test_merge_sstables_preserves_record_types() {
        // Test that both Put and Delete record types are preserved
        let memtable: SkipMap<Vec<u8>, (RecordType, Vec<u8>)> = SkipMap::new();
        memtable.insert(b"deleted_key".to_vec(), (RecordType::Delete, b"".to_vec()));
        memtable.insert(b"active_key".to_vec(), (RecordType::Put, b"value".to_vec()));

        // Create SSTable block with mixed types
        let block_data = vec![
            Record::new(
                b"another_active".to_vec(),
                b"data".to_vec(),
                RecordType::Put,
                1000,
            ),
            Record::new(
                b"another_deleted".to_vec(),
                b"".to_vec(),
                RecordType::Delete,
                1000,
            ),
        ];

        let block = Block {
            offset: 0,
            first_key: b"another_active".to_vec(),
            last_key: b"another_deleted".to_vec(),
            record_count: 2,
            data_size: 100,
            data: Some(block_data),
        };

        let sstables = vec![block];

        let result = merge_sstables(&memtable, &sstables).unwrap();

        assert_eq!(result.len(), 4, "Should have 4 records");

        // Verify record types are preserved
        // Sorted order: active_key, another_active, another_deleted, deleted_key
        assert_eq!(result[0].key, b"active_key");
        assert_eq!(result[0].record_type, RecordType::Put);

        assert_eq!(result[1].key, b"another_active");
        assert_eq!(result[1].record_type, RecordType::Put);

        assert_eq!(result[2].key, b"another_deleted");
        assert_eq!(result[2].record_type, RecordType::Delete);

        assert_eq!(result[3].key, b"deleted_key");
        assert_eq!(result[3].record_type, RecordType::Delete);
    }

    #[test]
    fn test_merge_sstables_sorted_output() {
        // Test that output is correctly sorted by key regardless of input order
        let memtable: SkipMap<Vec<u8>, (RecordType, Vec<u8>)> = SkipMap::new();
        // Insert in random order
        memtable.insert(b"z_last".to_vec(), (RecordType::Put, b"zzz".to_vec()));
        memtable.insert(b"a_first".to_vec(), (RecordType::Put, b"aaa".to_vec()));
        memtable.insert(b"m_middle".to_vec(), (RecordType::Put, b"mmm".to_vec()));

        // Create SSTable block with random order keys
        let block_data = vec![
            Record::new(b"b_two".to_vec(), b"bbb".to_vec(), RecordType::Put, 1000),
            Record::new(b"d_four".to_vec(), b"ddd".to_vec(), RecordType::Put, 1000),
        ];

        let block = Block {
            offset: 0,
            first_key: b"b_two".to_vec(),
            last_key: b"d_four".to_vec(),
            record_count: 2,
            data_size: 100,
            data: Some(block_data),
        };

        let sstables = vec![block];

        let result = merge_sstables(&memtable, &sstables).unwrap();

        assert_eq!(result.len(), 5, "Should have 5 records");

        // Verify strictly ascending order
        assert_eq!(result[0].key, b"a_first");
        assert_eq!(result[1].key, b"b_two");
        assert_eq!(result[2].key, b"d_four");
        assert_eq!(result[3].key, b"m_middle");
        assert_eq!(result[4].key, b"z_last");

        // Verify each subsequent key is greater than the previous
        for i in 1..result.len() {
            assert!(
                result[i].key > result[i - 1].key,
                "Records should be in ascending order by key"
            );
        }
    }

    #[test]
    fn test_merge_sstables_empty_block_data() {
        // Test that blocks with None data field are handled correctly
        let memtable: SkipMap<Vec<u8>, (RecordType, Vec<u8>)> = SkipMap::new();
        memtable.insert(b"key1".to_vec(), (RecordType::Put, b"value1".to_vec()));

        // Create block with None data
        let block = Block {
            offset: 0,
            first_key: b"key2".to_vec(),
            last_key: b"key2".to_vec(),
            record_count: 0,
            data_size: 0,
            data: None, // No data
        };

        let sstables = vec![block];

        let result = merge_sstables(&memtable, &sstables).unwrap();

        // Should only have memtable record since block has no data
        assert_eq!(result.len(), 1, "Should have 1 record from memtable");
        assert_eq!(result[0].key, b"key1");
    }

    #[test]
    fn test_flush_memtable_merge_with_existing_sstable() {
        use std::sync::atomic::{AtomicU64, Ordering};

        // Test that flush_memtable overwrites when called twice
        static FILE_COUNTER: AtomicU64 = AtomicU64::new(0);
        let file_id = FILE_COUNTER.fetch_add(1, Ordering::SeqCst);

        // Step 1: Create initial SSTable with 3 records
        let memtable1 = SkipMap::new();
        memtable1.insert(b"apple".to_vec(), (RecordType::Put, b"red".to_vec()));
        memtable1.insert(b"banana".to_vec(), (RecordType::Put, b"yellow".to_vec()));
        memtable1.insert(b"cherry".to_vec(), (RecordType::Put, b"dark_red".to_vec()));

        let filename = flush_for_test(memtable1, file_id);

        // Verify initial SSTable was created
        assert!(
            std::path::Path::new(&filename).exists(),
            "Initial SSTable should be created"
        );

        // Step 2: Create second memtable with overlapping and new keys
        let memtable2 = SkipMap::new();
        // Update existing key (should win due to newer timestamp)
        memtable2.insert(b"banana".to_vec(), (RecordType::Put, b"green".to_vec()));
        // Add new keys
        memtable2.insert(b"date".to_vec(), (RecordType::Put, b"brown".to_vec()));
        memtable2.insert(
            b"elderberry".to_vec(),
            (RecordType::Put, b"purple".to_vec()),
        );
        // Delete an existing key
        memtable2.insert(b"cherry".to_vec(), (RecordType::Delete, b"".to_vec()));

        // Step 3: Flush second memtable using the same file_id (overwrites)
        let filename = flush_for_test(memtable2, file_id);

        // Step 4: Decode the merged SSTable and verify results
        let file = File::open(&filename).unwrap();
        let sstable = SSTable::decode(&file).unwrap();

        // Collect all records from all blocks
        let mut all_records = Vec::new();
        for block in &sstable.block {
            if let Some(ref data) = block.data {
                for record in data.iter() {
                    all_records.push(record);
                }
            }
        }

        // Should have 4 keys from the second flush
        assert_eq!(
            all_records.len(),
            4,
            "Should have 4 records after overwrite"
        );

        // Verify keys are sorted
        let keys: Vec<&[u8]> = all_records.iter().map(|r| r.key.as_slice()).collect();
        assert_eq!(keys[0], b"banana");
        assert_eq!(keys[1], b"cherry");
        assert_eq!(keys[2], b"date");
        assert_eq!(keys[3], b"elderberry");

        // Verify values and record types
        assert_eq!(all_records[0].value, b"green"); // banana updated to green
        assert_eq!(all_records[0].record_type, RecordType::Put);

        assert_eq!(all_records[1].record_type, RecordType::Delete); // cherry deleted
        assert_eq!(all_records[1].value, b""); // Delete records have empty value

        assert_eq!(all_records[2].value, b"brown"); // date is new
        assert_eq!(all_records[2].record_type, RecordType::Put);

        assert_eq!(all_records[3].value, b"purple"); // elderberry is new
        assert_eq!(all_records[3].record_type, RecordType::Put);

        // Cleanup
        std::fs::remove_file(&filename).ok();
    }

    #[test]
    fn test_flush_memtable_no_merge_on_new_file() {
        use std::sync::atomic::{AtomicU64, Ordering};

        // Test that flush_memtable works correctly with a new file (no merge needed)
        static FILE_COUNTER: AtomicU64 = AtomicU64::new(1000);
        let file_id = FILE_COUNTER.fetch_add(1, Ordering::SeqCst);

        // Create memtable
        let memtable = SkipMap::new();
        memtable.insert(b"key1".to_vec(), (RecordType::Put, b"value1".to_vec()));
        memtable.insert(b"key2".to_vec(), (RecordType::Put, b"value2".to_vec()));

        // Flush to new file
        let filename = flush_for_test(memtable, file_id);

        // Verify file was created
        assert!(
            std::path::Path::new(&filename).exists(),
            "SSTable file should be created"
        );

        // Decode and verify
        let file = File::open(&filename).unwrap();
        let sstable = SSTable::decode(&file).unwrap();

        let mut all_records = Vec::new();
        for block in &sstable.block {
            if let Some(ref data) = block.data {
                for record in data.iter() {
                    all_records.push(record);
                }
            }
        }

        assert_eq!(all_records.len(), 2, "Should have 2 records");
        assert_eq!(all_records[0].key, b"key1");
        assert_eq!(all_records[0].value, b"value1");
        assert_eq!(all_records[1].key, b"key2");
        assert_eq!(all_records[1].value, b"value2");

        // Cleanup
        std::fs::remove_file(&filename).ok();
    }

    #[test]
    fn test_flush_memtable_merge_handles_empty_existing_file() {
        use std::sync::atomic::{AtomicU64, Ordering};

        // Test that flush_memtable handles an empty/incomplete existing file correctly
        static FILE_COUNTER: AtomicU64 = AtomicU64::new(2000);
        let file_id = FILE_COUNTER.fetch_add(1, Ordering::SeqCst);
        let memtable = SkipMap::new();
        memtable.insert(b"key1".to_vec(), (RecordType::Put, b"value1".to_vec()));
        let filename = flush_for_test(memtable, file_id);

        // Decode and verify
        let file = File::open(&filename).unwrap();
        let sstable = SSTable::decode(&file).unwrap();

        let mut all_records = Vec::new();
        for block in &sstable.block {
            if let Some(ref data) = block.data {
                for record in data.iter() {
                    all_records.push(record);
                }
            }
        }

        assert_eq!(all_records.len(), 1, "Should have 1 record");
        assert_eq!(all_records[0].key, b"key1");
        assert_eq!(all_records[0].value, b"value1");

        // Cleanup
        std::fs::remove_file(&filename).ok();
    }
}
