use crossbeam_skiplist::SkipMap;
use std::{fs::OpenOptions, io::Write};

use crate::storage::{block::BlockBuilder, bloom::BloomFilter, record::Record, record::RecordType};

use super::sstable::{SSTableFooter, SparseIndexEntry};

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

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;
    use std::{
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
        let temp_root = std::env::temp_dir().join(format!("wasm-kv-flush-{test_name}-{unique_id}"));

        std::fs::create_dir_all(&temp_root).unwrap();
        let original_dir = std::env::current_dir().unwrap();
        std::env::set_current_dir(&temp_root).unwrap();

        let result = test();

        std::env::set_current_dir(original_dir).unwrap();
        std::fs::remove_dir_all(&temp_root).ok();

        result
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
}
