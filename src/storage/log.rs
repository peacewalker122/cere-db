pub use super::block::{Block, BlockBuilder};
pub use super::constant::{WAL_HEADER_SIZE, WAL_MAGIC, WAL_VERSION};
pub use super::record::{
    // Backward compatibility exports (deprecated)
    DecodeRecordResult,
    Record,
    RecordType,
    decode_record,
};
pub use super::sstable::{
    IndexEntry, SSTableFooter, SparseIndexEntry, flush_memtable, read_sstable_bloom,
    read_sstable_footer, read_sstable_index, read_sstable_sparse_index, search_sstable,
    search_sstable_sparse, search_sstable_with_bloom,
};
use crate::storage::wal::{WALHeader, WALRecord};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};

/// Write WAL header to a file
fn write_wal_header<W: Write>(mut writer: W) -> Result<(), std::io::Error> {
    let header = WALHeader::new();
    writer.write_all(&header.encode())?;
    writer.flush()?;
    Ok(())
}

/// Read WAL header from a file
pub fn read_wal_header<R: Read>(mut reader: R) -> Result<WALHeader, std::io::Error> {
    let mut buf = vec![0u8; WAL_HEADER_SIZE as usize];
    reader.read_exact(&mut buf)?;
    WALHeader::decode(&buf)
}

/// Store a log entry to the Write-Ahead Log (WAL)
/// Returns the offset where the record was written and its LSN
pub fn store_log(
    mut file: &mut File,
    key: &Vec<u8>,
    value: &Vec<u8>,
    record_type: RecordType,
    wal: &WALRecord,
) -> Result<(u64, u64), std::io::Error> {
    // If file is new (empty), write header
    if file.metadata()?.len() == 0 {
        write_wal_header(&mut file)?;
    }

    // Get current position for offset (should be at end after calculate_next_lsn)
    let offset = file.seek(SeekFrom::End(0))?;
    let lsn = wal.get_lsn();

    // Write and sync to ensure durability
    wal.encode(&mut file, record_type, key, value)?;
    file.sync_data()?;

    Ok((offset, lsn))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::wal::WAL;
    use std::fs::OpenOptions;
    use std::{sync::Arc, thread};

    #[test]
    fn test_store_and_decode_log() {
        let test_file_path = std::env::temp_dir().join("wasm-kv-test_store_and_decode_log.log");
        let test_file = test_file_path.to_str().unwrap();

        // Clean up any existing test file
        let _ = std::fs::remove_file(test_file);

        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(test_file)
            .unwrap();

        // Store a record
        let key = b"test_key".to_vec();
        let value = b"test_value".to_vec();
        let mut wal = WALRecord::new();
        wal.key = key.clone();
        wal.value = value.clone();
        wal.record_type = RecordType::Put;
        let (offset, lsn) = store_log(&mut file, &key, &value, RecordType::Put, &mut wal).unwrap();
        assert_eq!(offset, WAL_HEADER_SIZE); // First record is right after header
        assert_eq!(lsn, 1); // Current implementation returns constant LSN

        // Read it back
        let file = std::fs::File::open(test_file).unwrap();
        let result = WAL::decode(file).unwrap();

        for record in result.records {
            if record.key == b"test_key" {
                assert_eq!(record.key, b"test_key");
                assert_eq!(record.value, b"test_value");
                assert_eq!(record.record_type, RecordType::Put);
                assert_eq!(record.lsn_val, 1);
            }
        }

        // Clean up
        std::fs::remove_file(test_file).unwrap();
    }

    #[test]
    fn test_store_tombstone() {
        let test_file_path = std::env::temp_dir().join("wasm-kv-test_store_tombstone.log");
        let test_file = test_file_path.to_str().unwrap();

        // Clean up any existing test file
        let _ = std::fs::remove_file(test_file);

        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(test_file)
            .unwrap();

        // Store a tombstone
        let key = b"deleted_key".to_vec();
        let value = Vec::new();
        let mut wal = WALRecord::new();
        wal.key = key.clone();
        wal.value = value.clone();
        wal.record_type = RecordType::Delete;
        let (offset, lsn) =
            store_log(&mut file, &key, &value, RecordType::Delete, &mut wal).unwrap();
        assert_eq!(offset, WAL_HEADER_SIZE);
        assert_eq!(lsn, 1);

        // Read it back
        let file = std::fs::File::open(test_file).unwrap();
        let result = WAL::decode(file).unwrap(); // this is doesn't
        // directly read the wal, it read the header first, hence we need an approach that // can skip the header

        for record in result.records {
            if record.key == b"deleted_key" {
                assert_eq!(record.record_type, RecordType::Delete);
                assert_eq!(record.value.len(), 0);
                assert_eq!(record.lsn_val, 1);
            }
        }

        // Clean up
        std::fs::remove_file(test_file).unwrap();
    }

    #[test]
    fn test_wal_header() {
        let test_file_path = std::env::temp_dir().join("wasm-kv-test_wal_header.log");
        let test_file = test_file_path.to_str().unwrap();

        // Clean up any existing test file
        let _ = std::fs::remove_file(test_file);

        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(test_file)
            .unwrap();

        // Create a new WAL file (header will be written automatically)
        let key = b"key1".to_vec();
        let value = b"value1".to_vec();
        let mut wal = WALRecord::new();
        wal.key = key.clone();
        wal.value = value.clone();
        wal.record_type = RecordType::Put;
        store_log(&mut file, &key, &value, RecordType::Put, &mut wal).unwrap();

        // Read and validate header
        let file = std::fs::File::open(test_file).unwrap();
        let header = read_wal_header(file).unwrap();

        assert_eq!(&header.magic, WAL_MAGIC);
        assert_eq!(header.version, WAL_VERSION);
        assert_eq!(header.last_checkpoint, 0);

        // Clean up
        std::fs::remove_file(test_file).unwrap();
    }

    #[test]
    fn test_lsn_increment() {
        let test_file = std::env::temp_dir().join("wasm-kv-test_wal_lsn.log");
        let test_file = test_file.to_str().unwrap();

        // Clean up any existing test file
        let _ = std::fs::remove_file(test_file);

        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(test_file)
            .unwrap();

        // Store multiple records
        let key1 = b"key1".to_vec();
        let value1 = b"value1".to_vec();
        let mut wal1 = WALRecord::new();
        wal1.key = key1.clone();
        wal1.value = value1.clone();
        wal1.record_type = RecordType::Put;
        let (_offset1, lsn1) =
            store_log(&mut file, &key1, &value1, RecordType::Put, &mut wal1).unwrap();

        let key2 = b"key2".to_vec();
        let value2 = b"value2".to_vec();
        let (_offset2, lsn2) =
            store_log(&mut file, &key2, &value2, RecordType::Put, &mut wal1).unwrap();

        let key3 = b"key3".to_vec();
        let value3 = b"value3".to_vec();
        let (_offset3, lsn3) =
            store_log(&mut file, &key3, &value3, RecordType::Put, &mut wal1).unwrap();

        // LSN should increment
        assert_eq!(lsn1, 1);
        assert_eq!(lsn2, 2);
        assert_eq!(lsn3, 3);

        // Clean up
        std::fs::remove_file(test_file).unwrap();
    }

    #[test]
    fn test_lsn_increment_concurrent() {
        let test_file_path = std::env::temp_dir().join("wasm-kv-test_wal_lsn_concurrent.log");
        let test_file = test_file_path.to_str().unwrap();
        let _ = std::fs::remove_file(test_file);

        const THREADS: usize = 8;

        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(test_file)
            .unwrap();

        let mut handles = Vec::new();
        let wal = Arc::new(WALRecord::new());

        let (tx, rx) = std::sync::mpsc::channel::<(Vec<u8>, Vec<u8>, Arc<WALRecord>)>();

        handles.push(thread::spawn(move || {
            let (key, value, wal) = rx.recv().unwrap();

            let (_offset, lsn) = store_log(&mut file, &key, &value, RecordType::Put, &wal)
                .expect("store_log failed");

            lsn
        }));

        for i in 0..THREADS {
            let tx = tx.clone();
            let key = format!("key{}", i).into_bytes();
            let value = format!("value{}", i).into_bytes();
            let wal = wal.clone();

            tx.send((key, value, wal)).unwrap();
        }

        // Collect LSNs
        let mut lsns: Vec<u64> = handles.into_iter().map(|h| h.join().unwrap()).collect();
        println!("LSNs: {:?}", lsns);

        // Sort so we can reason about ordering
        lsns.sort_unstable();

        // Expect a perfect sequence: 1..=THREADS
        for (i, lsn) in lsns.iter().enumerate() {
            assert_eq!(*lsn, (i + 1) as u64);
        }

        std::fs::remove_file(test_file).unwrap();
    }
}
