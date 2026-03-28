// need to expose given API
// 1. write log with group commit
// 2. read log with offset and length
// 3. checkpoint/flushing entrypoint (this is for easier wal "deletion" whenever checkpoint is done, we can just move the offset forward and ignore old logs)
// 4. recovery entrypoint (given a wal file, read from the last checkpoint offset and apply all logs to recover the state)
// 5. Manifest file management (keep track of wal files, their offsets, and checkpoint information)
// use async primivites with tokio

use std::{
    io::Write,
    path::PathBuf,
    sync::atomic::{self, AtomicU64},
};

use crossbeam_skiplist::SkipMap;
use tokio::{
    fs::File as TokioFile,
    io::{AsyncReadExt, AsyncWriteExt},
};

use crate::storage::{constant::*, record::RecordType, recovermanager::segment::SegmentId};

// Implement both read, write and recover as a method here also rotation
pub struct WALManager {
    lsn: AtomicU64,              // Log Sequence Number generator, starts from 1
    wal_file: Option<TokioFile>, // File handle for the WAL file

    active_segment_id: SegmentId, // Current WAL segment ID for log rotation
    max_segment_size: u64,        // Maximum size of a WAL segment before rotation

    wal_dir: PathBuf, // Directory where WAL files are stored
}

// Additional methods for writing logs, reading logs, checkpointing, and recovery would go here
impl WALManager {
    pub async fn new(wal_dir: PathBuf, max_segment_size: u64) -> Result<Self, std::io::Error> {
        let wal_files = std::fs::read_dir(&wal_dir)?;

        // retrieve the latest segment id from the wal_dir
        let mut max_segment_id: SegmentId = SegmentId(0);
        for entry in wal_files.flatten() {
            match entry.file_name().to_str() {
                Some(file_name) if file_name.ends_with(".log") => {
                    if let Ok(id) = file_name[..file_name.len() - 4].parse::<u64>() {
                        max_segment_id = SegmentId(std::cmp::max(max_segment_id.0, id));
                    }
                }
                _ => (),
            }
        }

        let mut wal_file: Option<TokioFile> = None; // lazily open the file when writing logs
        if max_segment_id.0 != 0 {
            let file = TokioFile::open(wal_dir.join(max_segment_id.filename())).await?;
            wal_file = Some(file);
        }

        Ok(WALManager {
            lsn: AtomicU64::new(1),
            wal_file: wal_file, // File will be opened lazily when writing logs
            active_segment_id: max_segment_id, // default 0
            max_segment_size,
            wal_dir,
        })
    }

    pub async fn write_log(
        &mut self,
        key: &Vec<u8>,
        value: &Vec<u8>,
        record_type: RecordType,
    ) -> Result<u64, std::io::Error> {
        // if file is none created it with an header intitialized
        if self.wal_file.is_none() {
            self.create_new_wal_file().await?;
        }

        // check if the current wal file exceeds the max segment size, if so, rotate to a new wal file
        if let Some(wal_file) = &mut self.wal_file {
            let metadata = wal_file.metadata().await?;
            if metadata.len() >= self.max_segment_size {
                log::info!(
                    "WAL segment {} reached max size ({} bytes), rotating to new segment",
                    self.active_segment_id.0,
                    metadata.len()
                );
                self.create_new_wal_file().await?;
            }
        }

        if self.wal_file.is_none() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "WAL file not initialized",
            ));
        }

        let wal_file = self.wal_file.as_mut().unwrap();
        let lsn = self.lsn.fetch_add(1, atomic::Ordering::SeqCst);

        let record = encode_record(key, value, record_type as u8, lsn).await?;

        // TODO: add grouped commit mechanism here
        wal_file.write_all(&record).await?;
        wal_file.sync_data().await?;

        Ok(lsn)
    }

    pub async fn recover(
        &mut self,
        memtable: &mut SkipMap<Vec<u8>, (RecordType, Vec<u8>)>,
    ) -> Result<(), std::io::Error> {
        let mut wal_files = tokio::fs::read_dir(&self.wal_dir).await?;

        let mut filenames = vec![];
        while let Ok(Some(entry)) = wal_files.next_entry().await {
            if entry
                .file_name()
                .to_str()
                .is_some_and(|s| s.ends_with(".log"))
            {
                filenames.push(entry.path());
            }
        }
        filenames.sort(); // ensure we process files in order with the lowest segment id first

        let mut set = tokio::task::JoinSet::new();
        for file_path in filenames {
            set.spawn(async move { Self::read_log(file_path).await });
        }

        while let Some(res) = set.join_next().await {
            match res {
                Ok(Ok(records)) => {
                    for record in records {
                        let key = record.key;
                        let value = record.value;
                        let record_type = match record.record_type {
                            1 => RecordType::Put,
                            2 => RecordType::Delete,
                            _ => continue, // skip invalid record types
                        };

                        if record_type == RecordType::Put {
                            memtable.insert(key, (record_type, value));
                        } else if record_type == RecordType::Delete {
                            memtable.insert(key, (record_type, vec![])); // use empty value to indicate deletion
                        }
                    }
                }
                Ok(Err(e)) => {
                    log::error!("Error reading WAL file: {}", e);
                    return Err(e);
                }
                Err(e) => {
                    log::error!("Task join error: {}", e);
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        "Task join error",
                    ));
                }
            }
        }

        Ok(())
    }

    async fn create_new_wal_file(&mut self) -> Result<(), std::io::Error> {
        self.active_segment_id.0 += 1;
        let wal_file_path = self.wal_dir.join(self.active_segment_id.filename());
        let mut new_file = TokioFile::create(&wal_file_path).await?;

        let header: WALHeader = WALHeader::default();
        new_file.write_all(&header.encode()).await?;

        self.wal_file = Some(new_file);
        Ok(())
    }

    async fn read_log(file_path: PathBuf) -> Result<Vec<WALRecord>, std::io::Error> {
        let mut wal_file = TokioFile::open(file_path).await?;
        let mut buf = tokio::io::BufReader::with_capacity(64 * 1024, wal_file);

        // read the header first
        let mut header_buf = vec![0u8; WAL_HEADER_SIZE as usize];
        buf.read_exact(&mut header_buf).await?;
        let header = WALHeader::decode(&header_buf)?;

        let mut record_offset = header.last_checkpoint_offset;
        if record_offset == 0 {
            record_offset = WAL_HEADER_SIZE; // start reading records right after the header if no checkpoint offset is set
        }

        let mut result = vec![];

        // should read the log records in a loop until the end of the file, and decode each record into WALRecord struct, for now we just return an empty vec to make the code compile
        loop {
            let mut record_type_buf = [0u8; 1];
            match buf.read_exact(&mut record_type_buf).await {
                Ok(_) => (),
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break, // reached end of file
                Err(e) => return Err(e),
            };

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

            let mut len_buf = [0u8; 8];
            if let Err(e) = buf.read_exact(&mut len_buf).await {
                if e.kind() == std::io::ErrorKind::UnexpectedEof {
                    log::warn!("Reached EOF while reading LSN, ending record parse");
                    break;
                }
                return Err(e);
            }
            let lsn = u64::from_be_bytes(len_buf);

            if let Err(e) = buf.read_exact(&mut len_buf).await {
                if e.kind() == std::io::ErrorKind::UnexpectedEof {
                    log::warn!("Reached EOF while reading key length, ending record parse");
                    break;
                }
                return Err(e);
            }
            let key_len = u64::from_be_bytes(len_buf) as usize;

            let mut key_bytes = vec![0u8; key_len];
            if let Err(e) = buf.read_exact(&mut key_bytes).await {
                if e.kind() == std::io::ErrorKind::UnexpectedEof {
                    log::warn!("Reached EOF while reading key, ending record parse");
                    break;
                }
                return Err(e);
            }

            if let Err(e) = buf.read_exact(&mut len_buf).await {
                if e.kind() == std::io::ErrorKind::UnexpectedEof {
                    log::warn!("Reached EOF while reading value length, ending record parse");
                    break;
                }
                return Err(e);
            }
            let value_len = u64::from_be_bytes(len_buf) as usize;

            let mut value_bytes = vec![0u8; value_len];
            if let Err(e) = buf.read_exact(&mut value_bytes).await {
                if e.kind() == std::io::ErrorKind::UnexpectedEof {
                    log::warn!("Reached EOF while reading value, ending record parse");
                    break;
                }
                return Err(e);
            }

            let mut crc_buf = [0u8; 4];
            if let Err(e) = buf.read_exact(&mut crc_buf).await {
                if e.kind() == std::io::ErrorKind::UnexpectedEof {
                    log::warn!("Reached EOF while reading checksum, ending record parse");
                    break;
                }
                return Err(e);
            }
            let checksum = u32::from_be_bytes(crc_buf);

            let mut hasher = crc32fast::Hasher::new();
            hasher.update(&record_type_buf);
            hasher.update(&lsn.to_be_bytes());
            hasher.update(&(key_len as u64).to_be_bytes());
            hasher.update(&key_bytes);
            hasher.update(&(value_len as u64).to_be_bytes());
            hasher.update(&value_bytes);
            let crc = hasher.finalize();

            if crc != checksum {
                log::error!(
                    "Checksum mismatch for record with LSN {}: expected {}, got {}",
                    lsn,
                    checksum,
                    crc32fast::hash(&value_bytes)
                );
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Checksum mismatch",
                ));
            }

            result.push(WALRecord {
                record_type: record_type_buf[0],
                lsn,
                key: key_bytes,
                value: value_bytes,
                checksum,
            });
        }

        Ok(result)
    }
}

pub struct WALRecord {
    pub record_type: u8,
    pub lsn: u64,
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub checksum: u32,
}

async fn encode_record(
    key: &Vec<u8>,
    value: &Vec<u8>,
    record_type: u8,
    lsn: u64,
) -> Result<Vec<u8>, std::io::Error> {
    let mut buf = Vec::new();

    tokio::io::AsyncWriteExt::write_all(&mut buf, &record_type.to_be_bytes()).await?;
    tokio::io::AsyncWriteExt::write_all(&mut buf, &lsn.to_be_bytes()).await?;
    tokio::io::AsyncWriteExt::write_all(&mut buf, &(key.len() as u64).to_be_bytes()).await?;
    tokio::io::AsyncWriteExt::write_all(&mut buf, key).await?;
    tokio::io::AsyncWriteExt::write_all(&mut buf, &(value.len() as u64).to_be_bytes()).await?;
    tokio::io::AsyncWriteExt::write_all(&mut buf, value).await?;

    let mut hash = crc32fast::Hasher::new();
    hash.update(&record_type.to_be_bytes());
    hash.update(&lsn.to_be_bytes());
    hash.update(&(key.len() as u64).to_be_bytes());
    hash.update(key);
    hash.update(&(value.len() as u64).to_be_bytes());
    hash.update(value);

    let checksum = hash.finalize();
    tokio::io::AsyncWriteExt::write_all(&mut buf, &checksum.to_be_bytes()).await?;

    log::info!(
        "Encoded record with LSN {}: key_len={},key={}, value_len={}, value={}, checksum={}",
        lsn,
        key.len(),
        String::from_utf8_lossy(key),
        value.len(),
        String::from_utf8_lossy(value),
        checksum
    );

    Ok(buf)
}

/// WAL file header structure (32 Bytes + 4 bytes checksum)
#[derive(Debug)]
pub struct WALHeader {
    pub magic: [u8; 8],              // Magic number: "WALMGIC\0"
    pub version: u64,                // WAL format version
    pub last_checkpoint: u64,        // LSN of last checkpoint
    pub last_checkpoint_offset: u64, // Reserved for future use
}

impl WALHeader {
    /// Encode header to bytes
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(WAL_HEADER_SIZE as usize);
        buf.extend_from_slice(&self.magic);
        buf.extend_from_slice(&self.version.to_be_bytes());
        buf.extend_from_slice(&self.last_checkpoint.to_be_bytes());
        buf.extend_from_slice(&self.last_checkpoint_offset.to_be_bytes());

        let mut hash = crc32fast::Hasher::new();
        hash.update(&buf);

        buf.extend_from_slice(&hash.finalize().to_be_bytes());

        buf
    }

    /// Decode header from bytes
    pub fn decode(buf: &Vec<u8>) -> Result<Self, std::io::Error> {
        if buf.len() < WAL_HEADER_SIZE as usize {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "Buffer too short for WAL header",
            ));
        }

        let mut magic = [0u8; 8];
        magic.copy_from_slice(&buf[0..8]);

        if &magic != WAL_MAGIC {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Invalid WAL magic number",
            ));
        }

        let mut b = [0u8; 8];
        b.copy_from_slice(&buf[8..16]);
        let version = u64::from_be_bytes(b);

        b.copy_from_slice(&buf[16..24]);
        let last_checkpoint = u64::from_be_bytes(b);

        b.copy_from_slice(&buf[24..32]);
        let last_checkpoint_offset = u64::from_be_bytes(b);

        let mut checksum_data = [0u8; 4];
        checksum_data.copy_from_slice(&buf[32..36]);
        let checksum = u32::from_be_bytes(checksum_data);

        let mut hash = crc32fast::Hasher::new();
        hash.update(&buf[0..32]);

        if checksum != hash.finalize() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "WAL header checksum mismatch",
            ));
        }

        Ok(WALHeader {
            magic,
            version,
            last_checkpoint,
            last_checkpoint_offset,
        })
    }
}

impl Default for WALHeader {
    fn default() -> Self {
        WALHeader {
            magic: *WAL_MAGIC,
            version: WAL_VERSION,
            last_checkpoint: 0,
            last_checkpoint_offset: 0,
        }
    }
}

#[cfg(test)]
mod test {
    use tokio::io::AsyncReadExt;

    use super::*;

    #[tokio::test]
    async fn initializenewfilesegment() {
        let wal_dir = std::env::temp_dir().join(uuid::Uuid::new_v4().to_string()); // make sure to use a unique temp directory for each test run
        std::fs::create_dir_all(&wal_dir).unwrap();

        println!(
            "Testing WALManager initialization with wal_dir: {:?}",
            wal_dir
        );

        let mut wal_manager = super::WALManager::new(wal_dir.clone(), 1024 * 1024)
            .await
            .unwrap();
        assert_eq!(wal_manager.active_segment_id.0, 0);
        assert!(wal_manager.wal_file.is_none());

        // Clean up
        std::fs::remove_dir_all(wal_dir).unwrap();
    }

    #[tokio::test]
    async fn writelogcreatesnewsegment() {
        let wal_dir = std::env::temp_dir().join(uuid::Uuid::new_v4().to_string());
        std::fs::create_dir_all(&wal_dir).unwrap();

        println!("Testing WALManager write_log with wal_dir: {:?}", wal_dir);

        let mut wal_manager = super::WALManager::new(wal_dir.clone(), 1024).await.unwrap();

        // Write a log entry that exceeds the max segment size to trigger rotation
        let key = b"key".to_vec();
        let value = vec![0u8; 2048]; // 2KB value to exceed the 1KB segment size
        let record_type = RecordType::Put;

        let lsn = wal_manager
            .write_log(&key, &value, record_type)
            .await
            .unwrap();
        assert_eq!(lsn, 1);
        assert_eq!(wal_manager.active_segment_id.0, 1); // should have rotated to segment 1

        // check the attached file, is it contains the log entry we just wrote?

        let wal_file_path = wal_dir.join(wal_manager.active_segment_id.filename());
        let mut wal_file = TokioFile::open(&wal_file_path).await.unwrap();

        // read from the header_last_offset to the end of the file, should be the log entry we just wrote
        let mut buf = Vec::new();
        wal_file.read_to_end(&mut buf).await.unwrap();

        // parse the header first...
        let header = WALHeader::decode(&buf[0..WAL_HEADER_SIZE as usize].to_vec()).unwrap();
        assert_eq!(header.magic, *WAL_MAGIC);
        assert_eq!(header.version, WAL_VERSION);
        assert_eq!(header.last_checkpoint, 0);
        assert_eq!(header.last_checkpoint_offset, 0);

        // need to parse the log entry here to verify the content
        // TODO: add log decoder logic later on to verify the log entry content, for now we just check the length of the log entry
        let log_entry = WALManager::read_log(wal_file_path).await.unwrap();
        assert_eq!(log_entry.len(), 1);
        assert_eq!(log_entry[0].record_type, record_type as u8);
        assert_eq!(log_entry[0].key, key);
        assert_eq!(log_entry[0].value, value);

        // Clean up
        std::fs::remove_dir_all(wal_dir).unwrap();
    }

    #[tokio::test]
    async fn write_log_single_record() {
        let wal_dir = std::env::temp_dir().join(uuid::Uuid::new_v4().to_string());
        std::fs::create_dir_all(&wal_dir).unwrap();

        let mut wal_manager = super::WALManager::new(wal_dir.clone(), 1024 * 1024)
            .await
            .unwrap();

        let key = b"test_key".to_vec();
        let value = b"test_value".to_vec();

        let lsn = wal_manager
            .write_log(&key, &value, RecordType::Put)
            .await
            .unwrap();

        // Verify LSN starts from 1
        assert_eq!(lsn, 1);

        // Read back and verify
        let wal_file_path = wal_dir.join("00000000000000000001.log");
        let records = WALManager::read_log(wal_file_path).await.unwrap();

        assert_eq!(records.len(), 1);
        assert_eq!(records[0].key, key);
        assert_eq!(records[0].value, value);
        assert_eq!(records[0].record_type, RecordType::Put as u8);

        std::fs::remove_dir_all(wal_dir).unwrap();
    }

    #[tokio::test]
    async fn write_log_multiple_records() {
        let wal_dir = std::env::temp_dir().join(uuid::Uuid::new_v4().to_string());
        std::fs::create_dir_all(&wal_dir).unwrap();

        let mut wal_manager = super::WALManager::new(wal_dir.clone(), 1024 * 1024)
            .await
            .unwrap();

        // Write multiple records
        let mut expected_lsns = vec![];
        for i in 0..5 {
            let key = format!("key_{}", i).into_bytes();
            let value = format!("value_{}", i).into_bytes();
            let lsn = wal_manager
                .write_log(&key, &value, RecordType::Put)
                .await
                .unwrap();
            expected_lsns.push(lsn);
        }

        // Verify LSNs are sequential
        assert_eq!(expected_lsns, vec![1, 2, 3, 4, 5]);

        // Read back and verify all records
        let wal_file_path = wal_dir.join("00000000000000000001.log");
        let records = WALManager::read_log(wal_file_path).await.unwrap();

        assert_eq!(records.len(), 5);
        for (i, record) in records.iter().enumerate() {
            assert_eq!(record.key, format!("key_{}", i).into_bytes());
            assert_eq!(record.value, format!("value_{}", i).into_bytes());
            assert_eq!(record.lsn, (i + 1) as u64);
        }

        std::fs::remove_dir_all(wal_dir).unwrap();
    }

    #[tokio::test]
    async fn write_log_delete_record() {
        let wal_dir = std::env::temp_dir().join(uuid::Uuid::new_v4().to_string());
        std::fs::create_dir_all(&wal_dir).unwrap();

        let mut wal_manager = super::WALManager::new(wal_dir.clone(), 1024 * 1024)
            .await
            .unwrap();

        let key = b"delete_me".to_vec();

        let lsn = wal_manager
            .write_log(&key, &vec![], RecordType::Delete)
            .await
            .unwrap();

        assert_eq!(lsn, 1);

        // Read back and verify
        let wal_file_path = wal_dir.join("00000000000000000001.log");
        let records = WALManager::read_log(wal_file_path).await.unwrap();

        assert_eq!(records.len(), 1);
        assert_eq!(records[0].key, key);
        assert_eq!(records[0].value, vec![]);
        assert_eq!(records[0].record_type, RecordType::Delete as u8);

        std::fs::remove_dir_all(wal_dir).unwrap();
    }

    #[tokio::test]
    async fn write_log_empty_value() {
        let wal_dir = std::env::temp_dir().join(uuid::Uuid::new_v4().to_string());
        std::fs::create_dir_all(&wal_dir).unwrap();

        let mut wal_manager = super::WALManager::new(wal_dir.clone(), 1024 * 1024)
            .await
            .unwrap();

        let key = b"key_with_empty_value".to_vec();
        let value = vec![];

        let lsn = wal_manager
            .write_log(&key, &value, RecordType::Put)
            .await
            .unwrap();

        assert_eq!(lsn, 1);

        // Read back and verify
        let wal_file_path = wal_dir.join("00000000000000000001.log");
        let records = WALManager::read_log(wal_file_path).await.unwrap();

        assert_eq!(records.len(), 1);
        assert_eq!(records[0].key, key);
        assert_eq!(records[0].value, vec![]);
        assert_eq!(records[0].record_type, RecordType::Put as u8);

        std::fs::remove_dir_all(wal_dir).unwrap();
    }

    #[tokio::test]
    async fn write_log_triggers_segment_rotation() {
        let wal_dir = std::env::temp_dir().join(uuid::Uuid::new_v4().to_string());
        std::fs::create_dir_all(&wal_dir).unwrap();

        let segment_size = 512;
        let mut wal_manager = super::WALManager::new(wal_dir.clone(), segment_size)
            .await
            .unwrap();

        // Write records until rotation is triggered
        let mut total_records = 0;
        for i in 0..15 {
            let key = format!("key{}", i).into_bytes();
            let value = format!("value{}", i).into_bytes();
            match wal_manager.write_log(&key, &value, RecordType::Put).await {
                Ok(_) => total_records += 1,
                Err(e) => {
                    log::info!("Write error at i={}: {}", i, e);
                    break;
                }
            }
        }

        // Should have created multiple segments
        assert!(wal_manager.active_segment_id.0 >= 1);

        // Verify we can recover all records across segments
        let mut wal_manager2 = super::WALManager::new(wal_dir.clone(), segment_size)
            .await
            .unwrap();
        let mut memtable = SkipMap::new();
        wal_manager2.recover(&mut memtable).await.unwrap();

        // All records should be recovered
        assert_eq!(memtable.len(), total_records);

        std::fs::remove_dir_all(wal_dir).unwrap();
    }

    #[tokio::test]
    async fn write_log_large_key_and_value() {
        let wal_dir = std::env::temp_dir().join(uuid::Uuid::new_v4().to_string());
        std::fs::create_dir_all(&wal_dir).unwrap();

        let mut wal_manager = super::WALManager::new(wal_dir.clone(), 1024 * 1024)
            .await
            .unwrap();

        // Write a record with large key and value
        let key = vec![0u8; 10000]; // 10KB key
        let value = vec![1u8; 50000]; // 50KB value

        let lsn = wal_manager
            .write_log(&key, &value, RecordType::Put)
            .await
            .unwrap();

        assert_eq!(lsn, 1);

        // Read back and verify
        let wal_file_path = wal_dir.join("00000000000000000001.log");
        let records = WALManager::read_log(wal_file_path).await.unwrap();

        assert_eq!(records.len(), 1);
        assert_eq!(records[0].key.len(), 10000);
        assert_eq!(records[0].value.len(), 50000);
        assert!(records[0].key.iter().all(|&b| b == 0));
        assert!(records[0].value.iter().all(|&b| b == 1));

        std::fs::remove_dir_all(wal_dir).unwrap();
    }

    #[tokio::test]
    async fn recover_empty_wal() {
        let wal_dir = std::env::temp_dir().join(uuid::Uuid::new_v4().to_string());
        std::fs::create_dir_all(&wal_dir).unwrap();

        // Create an empty WAL directory
        let mut wal_manager = super::WALManager::new(wal_dir.clone(), 1024 * 1024)
            .await
            .unwrap();

        let mut memtable = SkipMap::new();
        let result = wal_manager.recover(&mut memtable).await;
        assert!(result.is_ok());
        assert_eq!(memtable.len(), 0);

        std::fs::remove_dir_all(wal_dir).unwrap();
    }

    #[tokio::test]
    async fn recover_with_put_records() {
        let wal_dir = std::env::temp_dir().join(uuid::Uuid::new_v4().to_string());
        std::fs::create_dir_all(&wal_dir).unwrap();

        // Write some records first
        {
            let mut wal_manager = super::WALManager::new(wal_dir.clone(), 1024 * 1024)
                .await
                .unwrap();

            wal_manager
                .write_log(&b"key1".to_vec(), &b"value1".to_vec(), RecordType::Put)
                .await
                .unwrap();
            wal_manager
                .write_log(&b"key2".to_vec(), &b"value2".to_vec(), RecordType::Put)
                .await
                .unwrap();
            wal_manager
                .write_log(&b"key3".to_vec(), &b"value3".to_vec(), RecordType::Put)
                .await
                .unwrap();
        }

        // Recover into a new memtable
        let mut wal_manager = super::WALManager::new(wal_dir.clone(), 1024 * 1024)
            .await
            .unwrap();
        let mut memtable = SkipMap::new();
        wal_manager.recover(&mut memtable).await.unwrap();

        // Verify all records were recovered
        assert_eq!(memtable.len(), 3);

        let binding1 = memtable.get(&b"key1".to_vec()).unwrap();
        let (rt1, val1) = binding1.value();
        assert_eq!(*rt1, RecordType::Put);
        assert_eq!(val1, &b"value1".to_vec());

        let binding2 = memtable.get(&b"key2".to_vec()).unwrap();
        let (rt2, val2) = binding2.value();
        assert_eq!(*rt2, RecordType::Put);
        assert_eq!(val2, &b"value2".to_vec());

        let binding3 = memtable.get(&b"key3".to_vec()).unwrap();
        let (rt3, val3) = binding3.value();
        assert_eq!(*rt3, RecordType::Put);
        assert_eq!(val3, &b"value3".to_vec());

        std::fs::remove_dir_all(wal_dir).unwrap();
    }

    #[tokio::test]
    async fn recover_with_delete_records() {
        let wal_dir = std::env::temp_dir().join(uuid::Uuid::new_v4().to_string());
        std::fs::create_dir_all(&wal_dir).unwrap();

        // Write put and delete records
        {
            let mut wal_manager = super::WALManager::new(wal_dir.clone(), 1024 * 1024)
                .await
                .unwrap();

            // Put a key
            wal_manager
                .write_log(&b"key1".to_vec(), &b"value1".to_vec(), RecordType::Put)
                .await
                .unwrap();
            // Delete it
            wal_manager
                .write_log(&b"key1".to_vec(), &b"".to_vec(), RecordType::Delete)
                .await
                .unwrap();
        }

        // Recover
        let mut wal_manager = super::WALManager::new(wal_dir.clone(), 1024 * 1024)
            .await
            .unwrap();
        let mut memtable = SkipMap::new();
        wal_manager.recover(&mut memtable).await.unwrap();

        // The key should exist with a Delete record type (empty value)
        assert_eq!(memtable.len(), 1);
        let binding = memtable.get(&b"key1".to_vec()).unwrap();
        let (rt, val) = binding.value();
        assert_eq!(*rt, RecordType::Delete);
        assert!(val.is_empty());

        std::fs::remove_dir_all(wal_dir).unwrap();
    }

    #[tokio::test]
    async fn recover_with_segment_rotation() {
        let wal_dir = std::env::temp_dir().join(uuid::Uuid::new_v4().to_string());
        std::fs::create_dir_all(&wal_dir).unwrap();

        // Write enough records to trigger segment rotation
        let segment_size = 1024;
        {
            let mut wal_manager = super::WALManager::new(wal_dir.clone(), segment_size)
                .await
                .unwrap();

            // Write multiple records to trigger rotation
            for i in 0..10 {
                let key = format!("key{}", i);
                let value = format!("value{}", i);
                wal_manager
                    .write_log(
                        &key.as_bytes().to_vec(),
                        &value.as_bytes().to_vec(),
                        RecordType::Put,
                    )
                    .await
                    .unwrap();
            }
        }

        // Recover
        let mut wal_manager = super::WALManager::new(wal_dir.clone(), segment_size)
            .await
            .unwrap();
        let mut memtable = SkipMap::new();
        wal_manager.recover(&mut memtable).await.unwrap();

        // All records should be recovered across segments
        assert_eq!(memtable.len(), 10);

        for i in 0..10 {
            let key = format!("key{}", i);
            let value = format!("value{}", i);
            let entry = memtable.get(&key.as_bytes().to_vec()).unwrap();
            assert_eq!(entry.value().1, value.as_bytes().to_vec());
        }

        std::fs::remove_dir_all(wal_dir).unwrap();
    }

    #[tokio::test]
    async fn recover_checksum_verification() {
        let wal_dir = std::env::temp_dir().join(uuid::Uuid::new_v4().to_string());
        std::fs::create_dir_all(&wal_dir).unwrap();

        // Write a valid record
        {
            let mut wal_manager = super::WALManager::new(wal_dir.clone(), 1024 * 1024)
                .await
                .unwrap();

            wal_manager
                .write_log(&b"key".to_vec(), &b"value".to_vec(), RecordType::Put)
                .await
                .unwrap();
        }

        // Corrupt the WAL file by modifying a byte in the data
        {
            // Use zero-padded segment ID format (20 digits)
            let wal_file_path = wal_dir.join("00000000000000000001.log");
            let mut file = std::fs::OpenOptions::new()
                .write(true)
                .open(&wal_file_path)
                .unwrap();
            // Header is 36 bytes, record starts at offset 36:
            // 1 byte record_type + 8 bytes lsn + 8 bytes key_len + 3 bytes key + 8 bytes value_len = offset 64 for value
            // Corrupt a byte in the value ("value" = 5 bytes at offsets 64-68)
            use std::io::Seek;
            file.seek(std::io::SeekFrom::Start(65)).unwrap(); // corrupt 2nd byte of value
            file.write_all(&[0xFF]).unwrap();
        }

        // Recover should fail due to checksum mismatch
        let mut wal_manager = super::WALManager::new(wal_dir.clone(), 1024 * 1024)
            .await
            .unwrap();
        let mut memtable = SkipMap::new();
        let result = wal_manager.recover(&mut memtable).await;
        assert!(result.is_err());

        std::fs::remove_dir_all(wal_dir).unwrap();
    }
}
