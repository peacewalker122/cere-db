use std::{
    io::Write,
    sync::{Arc, RwLock},
};

use rayon::prelude::*;
use tokio::sync::mpsc;

use crate::storage::{
    self,
    block::Block,
    bloom::BloomFilter,
    flush::flush_memtable,
    manifest,
    record::Record,
    signal::{CompactionSignal, FlushSignal},
    sstable::{SSTable, SSTableSource, SortedRecordSource, merge_record_sources},
};

pub fn flush_watcher(
    flush_receiver: &crossbeam_channel::Receiver<FlushSignal>,
    levelstore: Arc<RwLock<Vec<Vec<u8>>>>,
) {
    // Non-blocking check for flush signal
    if let Ok(signal) = flush_receiver.recv() {
        log::info!("Received flush signal, flushing memtable to SSTable");
        match flush_memtable(signal.value, 0, signal.file_id, &signal.wal_path) {
            Ok(sstable_filename) => {
                manifest::add_file(0, &sstable_filename)
                    .expect("Failed to update manifest after flushing SSTable");

                if let Ok(mut store) = levelstore.write() {
                    store.push(sstable_filename.as_bytes().to_vec());
                } else {
                    log::warn!("Failed to update levelstore after flush");
                }
            }
            Err(e) => panic!("Failed to flush memtable to SSTable: {}", e),
        }

        log::info!("Memtable flushed successfully");
    }
}

// This function would trigger compaction processes based on certain conditions, such as the number of SSTables or their sizes.
// Current implementation it check the data/ directory and check each level for the number of
// files if the count exceeds a certain threshold, it triggers a compaction process to merge
// those files into a single SSTable and move it to the next level. This helps to optimize read performance and manage storage space effectively.
pub async fn compaction_watcher(receiver: &mut mpsc::Receiver<CompactionSignal>) {
    while let Some(recv) = receiver.recv().await {
        let compaction_result =
            tokio::task::spawn_blocking(move || process_compaction_signal(recv))
                .await
                .expect("Compaction worker task panicked");

        compaction_result.expect("Compaction process failed");
    }
}

fn process_compaction_signal(recv: CompactionSignal) -> Result<(), String> {
    // TODO: implement page / buffer management for this process
    // for example if the user trying to read from particular level that were being
    // compacted. We still can allowed the read request to it but there's need to handle
    // edge case like deleting the sstable file that on the other process were trying to
    // read from it. So we need to make sure that the file is not deleted until the compaction process is done and the new sstable file is created and added to the manifest. We can use reference counting or some kind of locking mechanism to ensure that the file is not deleted while it's still being read or compacted.

    log::info!(
        "Received compaction signal for level {}, files: {:?}",
        recv.compaction_level,
        recv.files_to_compact
    );

    // for now just compact the files and add the new sstable file to the manifest
    let mut indexed_sources: Vec<(usize, Vec<Block>)> = recv
        .files_to_compact
        .iter()
        .enumerate()
        .collect::<Vec<_>>()
        .par_iter()
        .map(|(source_idx, filename)| {
            let file_source = std::fs::File::open(filename).map_err(|error| {
                format!(
                    "Failed to open SSTable for compaction '{}': {error}",
                    filename.display()
                )
            })?;

            let sstable = SSTable::decode(file_source).map_err(|error| {
                format!(
                    "Failed to decode SSTable for compaction '{}': {error}",
                    filename.display()
                )
            })?;

            Ok::<(usize, Vec<Block>), String>((*source_idx, sstable.block))
        })
        .collect::<Result<Vec<_>, _>>()?;

    indexed_sources.sort_unstable_by_key(|(source_idx, _)| *source_idx);
    let sources: Vec<Vec<Block>> = indexed_sources
        .into_iter()
        .map(|(_, blocks)| blocks)
        .collect();

    log::info!(
        "Parsed {} files for compaction, preparing to merge records",
        recv.files_to_compact.len()
    );

    let merge_source: Vec<Box<dyn SortedRecordSource>> = sources
        .into_iter()
        .map(|blocks| Box::new(SSTableSource::new(blocks)) as Box<dyn SortedRecordSource>)
        .collect();

    let merged_records = merge_record_sources(merge_source)
        .map_err(|error| format!("Failed to merge records during compaction: {error}"))?;

    log::info!(
        "Merged {} records from {} files for compaction",
        merged_records.len(),
        recv.files_to_compact.len()
    );

    let next_level = recv.compaction_level + 1;
    let sstable_filename =
        write_compacted_sstable_from_records(&merged_records, next_level, recv.file_id)
            .map_err(|error| format!("Failed to write compacted SSTable: {error}"))?;

    manifest::add_file(next_level, &sstable_filename)
        .map_err(|error| format!("Failed to update manifest after compaction: {error}"))?;

    for filename in &recv.files_to_compact {
        if let Err(e) = std::fs::remove_file(filename) {
            log::warn!(
                "Failed to delete old SSTable file after compaction: {:?}",
                e
            );
        }
    }

    log::info!(
        "Compaction completed successfully, new SSTable created: {}",
        sstable_filename
    );

    Ok(())
}

fn write_compacted_sstable_from_records(
    records: &[Record],
    level: u32,
    file_id: u64,
) -> Result<String, std::io::Error> {
    let level_dir = std::path::Path::new("data").join(format!("level-{level}"));
    std::fs::create_dir_all(&level_dir)?;
    let sstable_path = level_dir.join(format!("{file_id}.db"));
    let sstable_filename = sstable_path.to_string_lossy().to_string();

    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .truncate(true)
        .create(true)
        .open(&sstable_path)?;

    let data_block_start = 0u64;
    let mut sparse_index: Vec<crate::storage::sstable::SparseIndexEntry> = Vec::new();
    let mut blocks: Vec<Vec<u8>> = Vec::new();
    let mut bloom_filter = BloomFilter::with_rate(records.len(), 0.01);

    let mut current_offset = data_block_start;
    let mut block_builder = crate::storage::block::BlockBuilder::new(current_offset);

    for record in records.iter() {
        bloom_filter.insert(record.key.clone());

        match block_builder.add_record(record) {
            Ok(()) => {}
            Err(_record) => {
                if let Some((block_meta, block_data)) = block_builder.build() {
                    sparse_index.push(crate::storage::sstable::SparseIndexEntry {
                        first_key: block_meta.first_key,
                        block_offset: block_meta.offset,
                        last_key: block_meta.last_key,
                        record_count: block_meta.record_count,
                    });

                    let block_total_size = block_data.len() as u64;
                    blocks.push(block_data);
                    current_offset += block_total_size;
                }

                block_builder = crate::storage::block::BlockBuilder::new(current_offset);
                block_builder
                    .add_record(record)
                    .expect("Fresh block should have space for record");
            }
        }
    }

    if !block_builder.is_empty()
        && let Some((block_meta, block_data)) = block_builder.build()
    {
        sparse_index.push(crate::storage::sstable::SparseIndexEntry {
            first_key: block_meta.first_key,
            block_offset: block_meta.offset,
            last_key: block_meta.last_key,
            record_count: block_meta.record_count,
        });

        blocks.push(block_data);
    }

    for block_data in &blocks {
        file.write_all(block_data)?;
    }
    let data_block_end = file.metadata()?.len();

    let index_block_start = data_block_end;
    let mut index_blocks: Vec<u8> = Vec::new();
    index_blocks.extend_from_slice(&(sparse_index.len() as u64).to_be_bytes());
    for entry in sparse_index.iter() {
        index_blocks.append(&mut entry.encode());
    }
    file.write_all(&index_blocks)?;
    let index_block_end = file.metadata()?.len();
    let index_checksum = crc32fast::hash(&index_blocks);

    let bloom_block_start = index_block_end;
    let bloom_data = bloom_filter.encode();
    file.write_all(&bloom_data)?;
    let bloom_block_end = file.metadata()?.len();
    let bloom_checksum = crc32fast::hash(&bloom_data);

    let footer = crate::storage::sstable::SSTableFooter {
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

    Ok(sstable_filename)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{
        manifest,
        record::{Record, RecordType},
        sstable::{self, SSTable},
    };
    use crossbeam_skiplist::SkipMap;
    use std::{
        path::{Path, PathBuf},
        sync::{Mutex, OnceLock},
    };

    fn with_temp_dir<T>(test_name: &str, test: impl FnOnce() -> T) -> T {
        static TEST_MUTEX: OnceLock<Mutex<()>> = OnceLock::new();
        let _guard = TEST_MUTEX.get_or_init(|| Mutex::new(())).lock().unwrap();

        let unique_id = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let temp_root =
            std::env::temp_dir().join(format!("wasm-kv-watcher-{test_name}-{unique_id}"));

        std::fs::create_dir_all(&temp_root).unwrap();
        let original_dir = std::env::current_dir().unwrap();
        std::env::set_current_dir(&temp_root).unwrap();

        let result = test();

        std::env::set_current_dir(original_dir).unwrap();
        std::fs::remove_dir_all(&temp_root).ok();

        result
    }

    fn build_source_sstable(level: u32, file_id: u64, entries: &[(&[u8], &[u8], u64)]) -> PathBuf {
        let memtable = SkipMap::new();
        for (key, value, _timestamp) in entries {
            memtable.insert(key.to_vec(), (RecordType::Put, value.to_vec()));
        }

        let wal_path = format!("wal_{file_id}.log");
        std::fs::write(&wal_path, b"wal").unwrap();

        let path = sstable::flush_memtable(memtable, level, file_id, &wal_path).unwrap();
        PathBuf::from(path)
    }

    fn read_records(path: &Path) -> Vec<Record> {
        let file = std::fs::File::open(path).unwrap();
        let sstable = SSTable::decode(file).unwrap();
        sstable
            .block
            .into_iter()
            .flat_map(|block| block.data.unwrap_or_default().into_iter())
            .collect()
    }

    #[test]
    fn test_compaction_creates_next_level_and_removes_sources() {
        with_temp_dir("compaction-next-level-removes-sources", || {
            std::fs::create_dir_all("data/level-0").unwrap();
            std::fs::create_dir_all("data/level-2").unwrap();

            let src1 = build_source_sstable(0, 100, &[(b"k-a", b"v-a", 1)]);
            let src2 = build_source_sstable(0, 101, &[(b"k-b", b"v-b", 2)]);

            process_compaction_signal(CompactionSignal {
                files_to_compact: vec![src1.clone(), src2.clone()],
                compaction_level: 1,
                file_id: 202,
            })
            .expect("compaction should succeed");

            let compacted = Path::new("data/level-2/202.db");
            assert!(compacted.exists(), "expected compacted SSTable at level 2");
            assert!(!src1.exists(), "expected source file 1 to be deleted");
            assert!(!src2.exists(), "expected source file 2 to be deleted");
        });
    }

    #[test]
    fn test_compaction_merges_all_input_records_into_output() {
        with_temp_dir("compaction-merge-all-records", || {
            std::fs::create_dir_all("data/level-0").unwrap();
            std::fs::create_dir_all("data/level-2").unwrap();

            let src1 = build_source_sstable(
                0,
                110,
                &[(b"alpha", b"value-alpha", 1), (b"beta", b"value-beta", 2)],
            );
            let src2 = build_source_sstable(
                0,
                111,
                &[(b"gamma", b"value-gamma", 3), (b"delta", b"value-delta", 4)],
            );

            process_compaction_signal(CompactionSignal {
                files_to_compact: vec![src1, src2],
                compaction_level: 1,
                file_id: 303,
            })
            .expect("compaction should succeed");

            let records = read_records(Path::new("data/level-2/303.db"));
            assert_eq!(records.len(), 4, "expected all records to be merged");

            let keys: std::collections::HashSet<Vec<u8>> =
                records.into_iter().map(|record| record.key).collect();

            assert!(keys.contains(b"alpha" as &[u8]));
            assert!(keys.contains(b"beta" as &[u8]));
            assert!(keys.contains(b"gamma" as &[u8]));
            assert!(keys.contains(b"delta" as &[u8]));
        });
    }

    #[test]
    fn test_compaction_overlapping_keys_selects_latest_record() {
        with_temp_dir("compaction-overlap-selects-latest", || {
            std::fs::create_dir_all("data/level-2").unwrap();

            let old = Record::new(
                b"same-key".to_vec(),
                b"old-value".to_vec(),
                RecordType::Put,
                1,
            );
            let new = Record::new(
                b"same-key".to_vec(),
                b"new-value".to_vec(),
                RecordType::Put,
                2,
            );
            let merged = vec![old, new];

            let out = write_compacted_sstable_from_records(&merged, 2, 404).unwrap();
            manifest::add_file(2, &out).unwrap();

            let file = std::fs::File::open(&out).unwrap();
            let sstable = SSTable::decode(file).unwrap();

            let merge_source: Vec<Box<dyn SortedRecordSource>> =
                vec![Box::new(SSTableSource::new(sstable.block))];
            let deduped = merge_record_sources(merge_source).unwrap();

            assert_eq!(deduped.len(), 1, "expected dedupe to keep one record");
            assert_eq!(deduped[0].key.as_slice(), b"same-key");
            assert_eq!(deduped[0].value.as_slice(), b"new-value");
            assert_eq!(deduped[0].timestamp, 2);
        });
    }
}
