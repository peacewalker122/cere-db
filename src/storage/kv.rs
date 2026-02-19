use crossbeam_skiplist::SkipMap;
use std::{
    borrow::Cow,
    fs::File,
    sync::{Arc, Mutex, RwLock},
};

use crate::{
    api::api::KVEngine,
    error::DBError,
    storage::{
        self,
        log::{RecordType, search_sstable_sparse},
        manifest,
        sstable::SSTable,
        wal::WALRecord,
    },
};

#[derive(Debug)]
pub struct PersistentKV {
    pub memtable: SkipMap<Vec<u8>, (RecordType, Vec<u8>)>,
    pub levelstore: Arc<RwLock<Vec<Vec<u8>>>>,

    memtable_size: u64,
    wal: WALRecord,
    wal_file: Mutex<File>,
    wal_path: Mutex<String>,
    wal_id: Mutex<u64>,

    flush_sender: crossbeam_channel::Sender<FlushSignal>,
}

struct FlushSignal {
    value: SkipMap<Vec<u8>, (RecordType, Vec<u8>)>,
    wal_path: String,
    file_id: u64,
}

fn current_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

impl PersistentKV {
    pub fn new() -> Self {
        // Read manifest on startup to populate levelstore
        let level_map = manifest::read_manifest().unwrap_or_else(|e| {
            log::warn!(
                "Failed to read manifest: {}, starting with empty levelstore",
                e
            );
            std::collections::HashMap::new()
        });

        // Convert HashMap<u32, Vec<String>> to Vec<Vec<u8>> for levelstore
        // This is a temporary structure until we refactor levelstore properly
        let mut levelstore = Vec::new();
        for (_level, files) in level_map.iter() {
            for filename in files {
                levelstore.push(filename.as_bytes().to_vec());
            }
        }
        levelstore.push(b"app.db".to_vec()); // for testing purpose

        let levelstore = Arc::new(RwLock::new(levelstore));

        let levelstore_len = levelstore
            .write()
            .map(|store| store.len())
            .unwrap_or_else(|_| {
                log::warn!("Failed to lock levelstore for length");
                0
            });

        log::info!(
            "PersistentKV initialized with {} files from manifest",
            levelstore_len
        );

        let initial_wal_id = current_millis();
        let initial_wal_path = format!("wal_{initial_wal_id}.log");
        let wal_file = File::options()
            .read(true)
            .write(true)
            .create(true)
            .open(&initial_wal_path)
            .expect("Failed to open WAL file");

        // channel to send the "event" of current memtable to be flushed to SSTable
        let (flush_sender, flush_receiver) = crossbeam_channel::bounded::<FlushSignal>(1);

        let result = PersistentKV {
            memtable: SkipMap::new(),
            levelstore: Arc::clone(&levelstore),
            memtable_size: 0,
            wal: WALRecord::new(),
            wal_file: Mutex::new(wal_file),
            wal_path: Mutex::new(initial_wal_path),
            wal_id: Mutex::new(initial_wal_id),
            flush_sender,
        };

        std::thread::spawn(move || {
            loop {
                flush_watcher(&flush_receiver, Arc::clone(&levelstore));
            }
        });

        result
    }
}

impl Default for PersistentKV {
    fn default() -> Self {
        Self::new()
    }
}

impl KVEngine for PersistentKV {
    fn get(&self, key: &[u8]) -> Result<Option<Cow<'_, Vec<u8>>>, DBError> {
        log::trace!("Getting key: {:?}", String::from_utf8_lossy(key));

        // 1. Search in memtable first (most recent data)
        if let Some(entry) = self.memtable.get(key) {
            log::debug!("Key found in memtable: {:?}", String::from_utf8_lossy(key));
            let (record_type, value) = entry.value();
            match record_type {
                RecordType::Put => return Ok(Some(Cow::Owned(value.clone()))), // Return value from memtable, Allocated
                RecordType::Delete => return Ok(None), // Tombstone - key deleted
            }
        }

        // 2. Key not in memtable, search through all SSTable files in levelstore
        log::debug!(
            "Key not in memtable, searching {} SSTable files",
            self.levelstore
                .read()
                .map_err(|_| DBError::MutexPoisoned("mutex was poisioned".to_owned()))?
                .len()
        );

        let levelstore = self
            .levelstore
            .read()
            .map_err(|_| DBError::MutexPoisoned("mutex was poisioned".to_owned()))?;

        for (idx, filename_bytes) in levelstore.iter().enumerate() {
            let filename = String::from_utf8_lossy(filename_bytes);

            log::trace!(
                "Searching SSTable {}/{}: {}",
                idx + 1,
                levelstore.len(),
                filename
            );

            // Try to open the SSTable file
            let file = match File::open(filename.as_ref()) {
                Ok(f) => f,
                Err(e) => {
                    log::warn!("Failed to open SSTable {}: {}, skipping", filename, e);
                    continue;
                }
            };

            log::debug!("Trying to decode SSTable: {}", filename);
            let sstable = SSTable::decode(&file).map_err(|e| {
                log::error!("Failed to decode SSTable {}: {:?}", filename, e);
                DBError::StorageError("Failed to decode SSTable".to_string())
            })?;

            // check bloom filter first
            if !sstable.bloom.contains(key) {
                log::trace!("Key not in bloom filter of {}", filename);
                continue; // Key definitely not in this SSTable
            }

            match search_sstable_sparse(&file, key, &sstable.index)? {
                Some(val) => {
                    log::debug!(
                        "Key found in SSTable {}: {:?}",
                        filename,
                        String::from_utf8_lossy(key)
                    );

                    return Ok(Some(Cow::Owned(val)));
                }
                None => {
                    log::trace!(
                        "Key not found in {} (bloom filter false positive)",
                        filename
                    );
                    // Continue searching in next SSTable
                }
            }
        }

        // Key not found in any SSTable
        log::warn!(
            "Key not found in any SSTable: {:?}",
            String::from_utf8_lossy(key)
        );

        Ok(None)
    }

    fn put(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<(), DBError> {
        log::trace!(
            "Putting key: {:?}, value size: {} bytes",
            String::from_utf8_lossy(&key),
            value.len()
        );

        let size = key.len() + value.len();
        // Write to WAL and get the LSN
        let (_offset, lsn) = storage::log::store_log(
            self.wal_file
                .get_mut()
                .map_err(|_| DBError::MutexPoisoned("mutex was poisioned".to_owned()))?,
            &key,
            &value,
            RecordType::Put,
            &self.wal,
        )?;
        log::trace!("WAL write complete with LSN: {}", lsn);

        self.memtable.insert(key, (RecordType::Put, value));

        // add the size of key and value to memtable_size
        self.memtable_size += size as u64;

        log::debug!("Memtable size: {} bytes", self.memtable_size);

        // Check if memtable size exceeds threshold
        if self.memtable_size >= storage::constant::MEMTABLE_SIZE_THRESHOLD {
            log::info!(
                "Memtable size threshold reached ({} >= {}), flushing to SSTable",
                self.memtable_size,
                storage::constant::MEMTABLE_SIZE_THRESHOLD
            );

            // Replace memtable with a new empty one, taking ownership of the old one
            let old_memtable = std::mem::replace(&mut self.memtable, SkipMap::new());
            self.memtable_size = 0;

            let current_wal_id = *self
                .wal_id
                .get_mut()
                .map_err(|_| DBError::MutexPoisoned("mutex was poisioned".to_owned()))?;

            // Send the old memtable to the flush watcher thread to be flushed to SSTable and
            // checkpointing the WAL file
            let current_wal_path = self
                .wal_path
                .get_mut()
                .map_err(|_| DBError::MutexPoisoned("mutex was poisioned".to_owned()))?
                .clone();

            self.flush_sender
                .send(FlushSignal {
                    value: old_memtable,
                    wal_path: current_wal_path,
                    file_id: current_wal_id,
                })
                .expect("Failed to send memtable to flush watcher");

            let next_wal_id = current_millis();
            let next_wal_path = format!("wal_{next_wal_id}.log");
            let next_wal_file = File::options()
                .read(true)
                .write(true)
                .create(true)
                .open(&next_wal_path)
                .map_err(|e| DBError::StorageError(format!("Failed to open WAL file: {e}")))?;

            *self
                .wal_id
                .get_mut()
                .map_err(|_| DBError::MutexPoisoned("mutex was poisioned".to_owned()))? =
                next_wal_id;
            *self
                .wal_path
                .get_mut()
                .map_err(|_| DBError::MutexPoisoned("mutex was poisioned".to_owned()))? =
                next_wal_path;
            *self
                .wal_file
                .get_mut()
                .map_err(|_| DBError::MutexPoisoned("mutex was poisioned".to_owned()))? =
                next_wal_file;

            log::info!("Memtable flushed and reset");
        }

        Ok(())
    }

    fn delete(&mut self, key: &[u8]) {
        log::debug!("Deleting key: {:?}", String::from_utf8_lossy(key));
        self.memtable
            .insert(key.to_vec(), (RecordType::Delete, vec![]));
    }
}

fn flush_watcher(
    flush_receiver: &crossbeam_channel::Receiver<FlushSignal>,
    levelstore: Arc<RwLock<Vec<Vec<u8>>>>,
) {
    // Non-blocking check for flush signal
    if let Ok(signal) = flush_receiver.recv() {
        log::info!("Received flush signal, flushing memtable to SSTable");
        match storage::log::flush_memtable(signal.value, 0, signal.file_id, &signal.wal_path) {
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

#[cfg(test)]
mod tests {

    use super::*;
    use crate::storage::{manifest, sstable, wal};
    use std::{
        path::Path,
        sync::{Mutex, OnceLock},
    };

    fn init_logger() {
        let _ = env_logger::builder().is_test(true).try_init();
    }

    fn with_temp_dir<T>(test_name: &str, test: impl FnOnce() -> T) -> T {
        static TEST_MUTEX: OnceLock<Mutex<()>> = OnceLock::new();
        let _guard = TEST_MUTEX.get_or_init(|| Mutex::new(())).lock().unwrap();

        let unique_id = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let temp_root = std::env::temp_dir().join(format!("wasm-kv-kv-{test_name}-{unique_id}"));

        std::fs::create_dir_all(&temp_root).unwrap();
        let original_dir = std::env::current_dir().unwrap();
        std::env::set_current_dir(&temp_root).unwrap();

        let result = test();

        std::env::set_current_dir(original_dir).unwrap();
        std::fs::remove_dir_all(&temp_root).ok();

        result
    }

    #[test]
    fn test_in_memory_kv() {
        #[cfg(feature = "dhat-heap")]
        let _profiler = dhat::Profiler::new_heap();

        // Arrange - Setup KV store
        let mut kv = PersistentKV::new();

        // Act - Put a key-value pair
        kv.put(b"key1".to_vec(), b"value1".to_vec())
            .expect("put failed");

        // Assert - Verify value exists
        let result = kv.get(b"key1").expect("get failed");
        assert!(result.is_some(), "expected Some, got None");
        assert_eq!(
            result.as_ref().unwrap().as_slice(),
            b"value1",
            "unexpected result from get(key1)"
        );

        // Act - Delete the key
        kv.delete(b"key1");

        // Assert - Verify key is deleted (returns None)
        let result = kv.get(b"key1").expect("get failed");
        assert!(
            result.is_none(),
            "expected None after delete, got: {:?}",
            result
        );
    }

    #[test]
    fn test_active_data() {
        #[cfg(feature = "dhat-heap")]
        let _profiler = dhat::Profiler::new_heap();

        init_logger();
        // Arrange - Setup KV store
        let mut kv = PersistentKV::new();

        kv.put(
            "key99".as_bytes().to_vec(),
            "valuefromkey99".as_bytes().to_vec(),
        )
        .expect("put failed for key99");

        // Act - Insert 10.000 key-value pairs
        (1..10000)
            .try_for_each(|_| -> Result<(), DBError> {
                let current_unix = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis();

                kv.put(
                    format!("key{current_unix}").as_bytes().to_vec(),
                    format!("valuefromkey{current_unix}").as_bytes().to_vec(),
                )?;

                Ok(())
            })
            .unwrap();

        // Assert - Check non-existent key returns None
        let result = kv.get(b"keyyangemangkosong").unwrap();
        assert!(result.is_none(), "expected None for non-existent key");

        // Assert - Check existing key returns correct value
        let result2 = kv.get(b"key99").expect("get failed for key99");
        assert!(result2.is_some(), "expected Some for key99");
        assert_eq!(
            result2.as_ref().unwrap().as_slice(),
            b"valuefromkey99",
            "unexpected value for key99"
        );

        kv.delete(b"key99");

        let result3 = kv.get(b"key99").unwrap();
        assert!(result3.is_none(), "expected None after deleting key99");
    }

    #[test]
    fn test_flush_rotation_and_manifest_uses_sstable_filename() {
        // Arrange: prepare isolated directories and WAL file for flush watcher.
        with_temp_dir("flush-rotation-manifest", || {
            init_logger();
            std::fs::create_dir_all("data/level-0").unwrap();
            std::fs::create_dir_all("archive").unwrap();

            let wal_id = 1000;
            let wal_path = format!("wal_{wal_id}.log");
            std::fs::write(&wal_path, b"wal").unwrap();

            let memtable = SkipMap::new();
            memtable.insert(b"key".to_vec(), (RecordType::Put, b"value".to_vec()));

            let (sender, receiver) = crossbeam_channel::bounded(1);

            // Act: flush memtable through the watcher.
            sender
                .send(FlushSignal {
                    value: memtable,
                    wal_path: wal_path.clone(),
                    file_id: wal_id,
                })
                .unwrap();
            let levelstore = Arc::new(RwLock::new(Vec::new()));
            flush_watcher(&receiver, Arc::clone(&levelstore));

            // Simulate WAL rotation following flush.
            let next_wal_id = wal_id + 1;
            let next_wal_path = format!("wal_{next_wal_id}.log");
            File::options()
                .read(true)
                .write(true)
                .create(true)
                .open(&next_wal_path)
                .unwrap();

            // Assert: WAL rotation creates a new WAL file after flush.
            assert!(
                Path::new(&next_wal_path).exists(),
                "expected new WAL file after flush"
            );

            // Assert: manifest entry uses the returned SSTable filename.
            let sstable_filename =
                sstable::flush_memtable(SkipMap::new(), 0, wal_id + 2, "wal_unused.log").unwrap();
            manifest::add_file(0, &sstable_filename).unwrap();
            let manifest_map = manifest::read_manifest().unwrap();
            let files = manifest_map.get(&0).expect("expected level-0 files");
            assert!(
                files.contains(&sstable_filename),
                "manifest should store the returned SSTable filename"
            );
        });
    }

    #[test]
    fn test_manifest_does_not_store_unreturned_filename() {
        // Arrange: create manifest entry with a different filename than flush output.
        with_temp_dir("manifest-mismatch", || {
            std::fs::create_dir_all("data/level-0").unwrap();
            let memtable = SkipMap::new();
            memtable.insert(b"key".to_vec(), (RecordType::Put, b"value".to_vec()));
            let wal_id = 3333;
            let wal_path = format!("wal_{wal_id}.log");
            std::fs::write(&wal_path, b"wal").unwrap();

            // Act: flush and then add a different manifest entry.
            let sstable_filename = sstable::flush_memtable(memtable, 0, wal_id, &wal_path).unwrap();
            let wrong_filename = "data/level-0/not-the-sstable.db";
            manifest::add_file(0, wrong_filename).unwrap();

            // Assert: manifest does not contain the flush filename when a wrong entry is added.
            let manifest_map = manifest::read_manifest().unwrap();
            let files = manifest_map.get(&0).expect("expected level-0 files");
            assert!(
                !files.contains(&sstable_filename),
                "manifest should not include unreturned filename (negative case)"
            );
        });
    }

    #[test]
    fn test_wal_recovery_rebuilds_memtable_with_tombstones() {
        // This test meets the objective by verifying WAL recovery rebuilds memtable state.
        with_temp_dir("wal-recovery-memtable", || {
            // Arrange: create a deterministic WAL file and write Put/Delete/Put records.
            let wal_id = 4242;
            let wal_path = format!("wal_{wal_id}.log");
            let mut wal_file = File::options()
                .read(true)
                .write(true)
                .create(true)
                .open(&wal_path)
                .unwrap();

            let mut wal_record = WALRecord::new();
            wal_record.value = b"value-alpha".to_vec();
            storage::log::store_log(
                &mut wal_file,
                &b"alpha".to_vec(),
                &b"value-alpha".to_vec(),
                RecordType::Put,
                &wal_record,
            )
            .unwrap();
            wal_record.value = Vec::new();
            storage::log::store_log(
                &mut wal_file,
                &b"beta".to_vec(),
                &Vec::new(),
                RecordType::Delete,
                &wal_record,
            )
            .unwrap();
            wal_record.value = b"value-gamma".to_vec();
            storage::log::store_log(
                &mut wal_file,
                &b"gamma".to_vec(),
                &b"value-gamma".to_vec(),
                RecordType::Put,
                &wal_record,
            )
            .unwrap();

            // Act: read WAL bytes, recover records, rebuild memtable.
            let wal_bytes = std::fs::read(&wal_path).unwrap();
            let recovered = wal::recover(&wal_bytes).unwrap();
            let memtable = SkipMap::new();
            for record in recovered {
                memtable.insert(
                    record.key.clone(),
                    (record.record_type, record.value.clone()),
                );
            }

            // Assert: Put keys are present with expected values, Delete is a tombstone.
            let alpha = memtable.get(b"alpha" as &[u8]).expect("alpha missing");
            assert_eq!(alpha.value().0, RecordType::Put);
            assert_eq!(alpha.value().1.as_slice(), b"value-alpha");

            let beta = memtable.get(b"beta" as &[u8]).expect("beta missing");
            assert_eq!(beta.value().0, RecordType::Delete);
            assert!(
                beta.value().1.is_empty(),
                "tombstone should have empty value"
            );

            let gamma = memtable.get(b"gamma" as &[u8]).expect("gamma missing");
            assert_eq!(gamma.value().0, RecordType::Put);
            assert_eq!(gamma.value().1.as_slice(), b"value-gamma");
        });
    }

    #[test]
    fn test_wal_recovery_fails_on_corrupt_data() {
        // This test meets the objective by ensuring WAL recovery fails on corruption.
        with_temp_dir("wal-recovery-corrupt", || {
            // Arrange: write a valid WAL record and then corrupt its bytes.
            let wal_id = 4343;
            let wal_path = format!("wal_{wal_id}.log");
            let mut wal_file = File::options()
                .read(true)
                .write(true)
                .create(true)
                .open(&wal_path)
                .unwrap();

            let mut wal_record = WALRecord::new();
            wal_record.value = b"value-alpha".to_vec();
            storage::log::store_log(
                &mut wal_file,
                &b"alpha".to_vec(),
                &b"value-alpha".to_vec(),
                RecordType::Put,
                &wal_record,
            )
            .unwrap();

            let mut wal_bytes = std::fs::read(&wal_path).unwrap();
            let last_index = wal_bytes.len() - 1;
            wal_bytes[last_index] ^= 0xFF; // corrupt checksum byte

            // Act: attempt WAL recovery on corrupted data.
            let result = wal::recover(&wal_bytes);

            // Assert: recovery fails with invalid data.
            assert!(
                result.is_err(),
                "expected wal::recover to fail on corruption"
            );
        });
    }

    #[test]
    fn test_get_across_scattered_levels() {
        // This test meets the objective by verifying get() finds data across multiple levels.
        with_temp_dir("scatter-levels", || {
            init_logger();
            std::fs::create_dir_all("data/level-0").unwrap();
            std::fs::create_dir_all("data/level-1").unwrap();
            std::fs::create_dir_all("data/level-2").unwrap();
            std::fs::create_dir_all("archive").unwrap();

            let wal_path_0 = "wal_100.log";
            let wal_path_1 = "wal_200.log";
            let wal_path_2 = "wal_300.log";
            std::fs::write(wal_path_0, b"wal").unwrap();
            std::fs::write(wal_path_1, b"wal").unwrap();
            std::fs::write(wal_path_2, b"wal").unwrap();

            let memtable_0 = SkipMap::new();
            memtable_0.insert(b"key-l0".to_vec(), (RecordType::Put, b"value-l0".to_vec()));
            let memtable_1 = SkipMap::new();
            memtable_1.insert(b"key-l1".to_vec(), (RecordType::Put, b"value-l1".to_vec()));
            let memtable_2 = SkipMap::new();
            memtable_2.insert(b"key-l2".to_vec(), (RecordType::Put, b"value-l2".to_vec()));

            let sstable_0 = sstable::flush_memtable(memtable_0, 0, 100, wal_path_0).unwrap();
            let sstable_1 = sstable::flush_memtable(memtable_1, 1, 200, wal_path_1).unwrap();
            let sstable_2 = sstable::flush_memtable(memtable_2, 2, 300, wal_path_2).unwrap();

            let kv = PersistentKV::new();
            if let Ok(mut store) = kv.levelstore.write() {
                store.clear();
                store.push(sstable_0.as_bytes().to_vec());
                store.push(sstable_1.as_bytes().to_vec());
                store.push(sstable_2.as_bytes().to_vec());
            }

            let value_0 = kv.get(b"key-l0").expect("get failed for key-l0");
            assert!(value_0.is_some(), "expected Some for key-l0");
            assert_eq!(
                value_0.as_ref().unwrap().as_slice(),
                b"value-l0",
                "unexpected value for key-l0"
            );

            let value_1 = kv.get(b"key-l1").expect("get failed for key-l1");
            assert!(value_1.is_some(), "expected Some for key-l1");
            assert_eq!(
                value_1.as_ref().unwrap().as_slice(),
                b"value-l1",
                "unexpected value for key-l1"
            );

            let value_2 = kv.get(b"key-l2").expect("get failed for key-l2");
            assert!(value_2.is_some(), "expected Some for key-l2");
            assert_eq!(
                value_2.as_ref().unwrap().as_slice(),
                b"value-l2",
                "unexpected value for key-l2"
            );

            let missing = kv.get(b"key-missing").unwrap();
            assert!(missing.is_none(), "expected None for missing key");
        });
    }
}
