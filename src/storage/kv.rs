use crossbeam_skiplist::SkipMap;
use std::{borrow::Cow, fs::File, sync::Mutex};

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
    pub levelstore: Vec<Vec<u8>>,

    memtable_size: u64,
    wal: WALRecord,
    wal_file: Mutex<File>,

    flush_sender: crossbeam_channel::Sender<FlushSignal>,
}

struct FlushSignal {
    value: SkipMap<Vec<u8>, (RecordType, Vec<u8>)>,
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

        log::info!(
            "PersistentKV initialized with {} files from manifest",
            levelstore.len()
        );

        let wal_file = File::options()
            .read(true)
            .write(true)
            .create(true)
            .open("app.log")
            .expect("Failed to open WAL file");

        // channel to send the "event" of current memtable to be flushed to SSTable
        let (flush_sender, flush_receiver) = crossbeam_channel::bounded::<FlushSignal>(1);

        let result = PersistentKV {
            memtable: SkipMap::new(),
            levelstore,
            memtable_size: 0,
            wal: WALRecord::new(),
            wal_file: Mutex::new(wal_file),
            flush_sender,
        };

        std::thread::spawn(move || {
            loop {
                flush_watcher(&flush_receiver);
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
            self.levelstore.len()
        );

        for (idx, filename_bytes) in self.levelstore.iter().enumerate() {
            let filename = String::from_utf8_lossy(filename_bytes);

            log::trace!(
                "Searching SSTable {}/{}: {}",
                idx + 1,
                self.levelstore.len(),
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

            let sstable = SSTable::decode(&file)?;

            // check bloom filter first
            if !sstable.bloom.contains(key) {
                log::trace!("Key not in bloom filter of {}", filename);
                return Ok(None);
                // continue; // Key definitely not in this SSTable
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
        log::debug!(
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

            // Generate unique file ID using timestamp (nanoseconds since UNIX_EPOCH)
            let file_id = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64;

            // Replace memtable with a new empty one, taking ownership of the old one
            let old_memtable = std::mem::replace(&mut self.memtable, SkipMap::new());
            self.memtable_size = 0;

            // Send the old memtable to the flush watcher thread to be flushed to SSTable and
            // checkpointing the WAL file
            self.flush_sender
                .send(FlushSignal {
                    value: old_memtable,
                })
                .expect("Failed to send memtable to flush watcher");

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

fn flush_watcher(flush_receiver: &crossbeam_channel::Receiver<FlushSignal>) {
    // Non-blocking check for flush signal
    if let Ok(signal) = flush_receiver.recv() {
        match storage::log::flush_memtable(signal.value, "app.db", 0, "app.log") {
            Ok(_) => {
                manifest::add_file(0, "app.db")
                    .expect("Failed to update manifest after flushing SSTable");
            }
            Err(e) => panic!("Failed to flush memtable to SSTable: {}", e),
        }

        log::info!("Memtable flushed successfully");
    }
}

#[cfg(test)]
mod tests {

    use super::*;

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

        // Arrange - Setup KV store
        let mut kv = PersistentKV::new();

        kv.put(
            "key99".as_bytes().to_vec(),
            "valuefromkey99".as_bytes().to_vec(),
        )
        .expect("put failed for key99");

        // Act - Insert 10.000 key-value pairs
        (1..5000)
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
        let result2 = kv.get(b"key99").unwrap();
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
}
