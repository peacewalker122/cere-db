// need to expose given API
// 1. write log with group commit
// 2. read log with offset and length (canceled, use recover)
// 3. checkpoint/flushing entrypoint (this is for easier wal "deletion" whenever checkpoint is done, we can just move the offset forward and ignore old logs)
// 4. recovery entrypoint (given a wal file, read from the last checkpoint offset and apply all logs to recover the state)
// 5. Manifest file management (keep track of wal files, their offsets, and checkpoint information)
// use async primivites with tokio

use std::{
    collections::VecDeque,
    io::Write,
    path::PathBuf,
    sync::atomic::{self, AtomicU64},
};

use crossbeam_skiplist::SkipMap;
use tokio::{
    fs::{File as TokioFile, OpenOptions},
    io::{AsyncReadExt, AsyncWriteExt},
};

use crate::storage::{constant::*, record::RecordType, recovermanager::segment::SegmentId};

use super::log_store::{LogCommand, LogPosition, LogStore};

// Implement both read, write and recover as a method here also rotation
pub struct WALManager {
    active_wal: tokio::sync::RwLock<TokioFile>, // File handle for the WAL file (active)
    sealed_wal: tokio::sync::Mutex<Vec<u64>>, // File handles for sealed WAL files that are being flushed
    reserved_wal: tokio::sync::Mutex<VecDeque<u64>>, // File handles for reserved WAL files that are waiting to be flushed after checkpoint is done

    active_segment_id: SegmentId, // Current WAL segment ID for log rotation
    max_segment_size: u64,        // Maximum size of a WAL segment before rotation

    wal_dir: PathBuf, // Directory where WAL files are stored
}

pub type FileWalStore = WALManager;

// Additional methods for writing logs, reading logs, checkpointing, and recovery would go here
impl WALManager {
    pub async fn new(wal_dir: PathBuf, max_segment_size: u64) -> Result<Self, std::io::Error> {
        let wal_files = std::fs::read_dir(&wal_dir)?;

        // retrieve the latest segment id from the wal_dir
        let mut max_segment_id: SegmentId = SegmentId(AtomicU64::new(0));
        for entry in wal_files.flatten() {
            match entry.file_name().to_str() {
                Some(file_name) if file_name.ends_with(".log") => {
                    if let Ok(id) = file_name[..file_name.len() - 4].parse::<u64>() {
                        let max = std::cmp::max(
                            max_segment_id.0.load(std::sync::atomic::Ordering::SeqCst),
                            id,
                        );
                        max_segment_id = SegmentId(AtomicU64::new(max));
                    }
                }
                _ => (),
            }
        }
        let mut wal_file: TokioFile;
        let max_segment_id_value = max_segment_id.0.load(std::sync::atomic::Ordering::SeqCst);

        if max_segment_id_value > 0 {
            // Open existing segment with highest ID
            let file = OpenOptions::new()
                .append(true)
                .create(true)
                .open(wal_dir.join(max_segment_id.filename()))
                .await?;
            wal_file = file;
        } else {
            // Check if segment 0 already exists (recovery scenario)
            let seg0_path = wal_dir.join(max_segment_id.filename());
            if seg0_path.exists() {
                // Open existing segment 0
                let file = OpenOptions::new()
                    .append(true)
                    .create(true)
                    .open(&seg0_path)
                    .await?;
                wal_file = file;
            } else {
                // Create new segment 0
                wal_file = OpenOptions::new()
                    .append(true)
                    .create(true)
                    .open(&seg0_path)
                    .await?;
                let header: WALHeader = WALHeader::default();
                wal_file.write_all(&header.encode()).await?;
            }
        }

        log::debug!(
            "Initialized WALManager with active segment ID {}, max segment size {}, wal directory {:?}",
            max_segment_id_value,
            max_segment_size,
            wal_dir
        );

        Ok(WALManager {
            active_wal: tokio::sync::RwLock::new(wal_file), // File will be opened lazily when writing logs
            sealed_wal: tokio::sync::Mutex::new(vec![]),    // No sealed WAL files at initialization
            reserved_wal: tokio::sync::Mutex::new(VecDeque::new()), // No reserved WAL files at initialization
            active_segment_id: max_segment_id,                      // default 0
            max_segment_size,
            wal_dir,
        })
    }

    pub async fn write_log(
        &self,
        key: &Vec<u8>,
        value: &Vec<u8>,
        lsn: u64,
        record_type: RecordType,
    ) -> Result<u64, std::io::Error> {
        let mut wal_file = self.active_wal.write().await;

        // Check if current segment exceeds max size before writing
        let metadata = wal_file.metadata().await?;
        if metadata.len() >= self.max_segment_size {
            log::info!(
                "WAL segment {} reached max size ({} bytes), rotating to new segment",
                self.active_segment_id.0.load(atomic::Ordering::SeqCst),
                metadata.len()
            );
            drop(wal_file); // Release the lock before creating new file
            self.create_new_wal_file().await?;
            wal_file = self.active_wal.write().await;
        }

        let record = encode_record(key, value, record_type as u8, lsn).await?;

        log::info!(
            "Writing log with LSN {}: key_len={}, key={}, value_len={}, value={}, record_type={:?}, wal_file={:?}",
            lsn,
            key.len(),
            String::from_utf8_lossy(key),
            value.len(),
            String::from_utf8_lossy(value),
            record_type,
            wal_file
        );

        // TODO: add grouped commit mechanism here
        wal_file.write_all(&record).await?;
        wal_file.sync_data().await?;

        Ok(lsn)
    }

    pub async fn recover(
        &self,
        memtable: &mut SkipMap<Vec<u8>, (RecordType, Vec<u8>)>,
    ) -> Result<(), std::io::Error> {
        let records = self.recover_records().await?;

        for record in records {
            let key = record.key;
            let value = record.value;
            let record_type = match record.record_type {
                1 => RecordType::Put,
                2 => RecordType::Delete,
                _ => continue,
            };

            if record_type == RecordType::Put {
                memtable.insert(key, (record_type, value));
            } else {
                memtable.insert(key, (record_type, vec![]));
            }
        }

        Ok(())
    }

    pub async fn recover_records(&self) -> Result<Vec<WALRecord>, std::io::Error> {
        log::debug!("Starting WAL recovery from directory: {:?}", self.wal_dir);

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
        filenames.sort();

        let mut all_records = Vec::new();
        for file_path in filenames {
            let records = Self::read_log(file_path).await?;
            all_records.extend(records);
        }

        for record in &all_records {
            log::info!(
                "Recovering WAL record: key={:?}, value_len={}, type={:?}, lsn={}",
                String::from_utf8_lossy(&record.key),
                String::from_utf8_lossy(&record.value),
                record.record_type,
                record.lsn
            );
        }

        Ok(all_records)
    }

    async fn create_new_wal_file(&self) -> Result<(), std::io::Error> {
        self.active_segment_id
            .0
            .fetch_add(1, atomic::Ordering::SeqCst);
        let wal_file_path = self.wal_dir.join(self.active_segment_id.filename());
        let mut new_file = OpenOptions::new()
            .append(true)
            .create(true)
            .open(&wal_file_path)
            .await?;

        let header: WALHeader = WALHeader::default();
        new_file.write_all(&header.encode()).await?;

        let mut active = self.active_wal.write().await;
        *active = new_file;
        Ok(())
    }

    pub async fn rotate_wal_file(&self) -> Result<u64, std::io::Error> {
        // this will change the state of active wal into locked state and move the current active
        // wal file to the reserved / new wal file, and create a new wal file for the next write, the old wal file will be flushed to disk and can be deleted after the checkpoint is done
        let prev_segment_id = self.active_segment_id.0.load(atomic::Ordering::SeqCst);

        // check if there's a file on the reserve
        // if exist, use it and change the filename to the new segment id use the lowest segmentid first , if not, create a new file with the new segment id
        if let Some(reserved_segment_id) = self.reserved_wal.lock().await.pop_front() {
            // seal the current active wal file and move it to the sealed_wal list, the file will be flushed to disk and can be deleted after the checkpoint is done
            let mut sealed = self.sealed_wal.lock().await;
            sealed.push(prev_segment_id);

            let _ = self
                .active_segment_id
                .0
                .fetch_add(1, atomic::Ordering::SeqCst);
            let new_path = self.wal_dir.join(self.active_segment_id.filename());

            //  remove the wal file with just only the header, and move it to the new path
            tokio::fs::remove_file(self.wal_dir.join(format!("{:020}.log", prev_segment_id)))
                .await?;

            let new_file = OpenOptions::new()
                .append(true)
                .create(true)
                .open(&new_path)
                .await?;
            let mut active = self.active_wal.write().await;
            *active = new_file;
        } else {
            //  remove the wal file with just only the header, and move it to the new path
            tokio::fs::remove_file(self.wal_dir.join(format!("{:020}.log", prev_segment_id)))
                .await?;

            self.create_new_wal_file().await?;

            // still need to seal the current active wal file and move it to the sealed_wal list, the file will be flushed to disk and can be deleted after the checkpoint is done
            let mut sealed = self.sealed_wal.lock().await;
            sealed.push(prev_segment_id);

            // return the new segment id after rotation, the caller can use this to update the manifest file with the new active wal segment id
            return Ok(self.active_segment_id.0.load(atomic::Ordering::SeqCst));
        }

        Ok(prev_segment_id)
    }

    // this will change the state of the wal file from sealed to reserved, and move the file handle to the reserved_wal queue, the file will be flushed to disk and can be deleted after the checkpoint is done
    pub async fn change_to_reserve(&self, locked_wal: u64) -> Result<SegmentId, std::io::Error> {
        let mut sealed = self.sealed_wal.lock().await;

        if let Some(pos) = sealed.iter().position(|&id| id == locked_wal) {
            let segment_id = sealed.remove(pos);
            self.reserved_wal.lock().await.push_back(segment_id);
            Ok(SegmentId(AtomicU64::new(segment_id)))
        } else {
            Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "Locked WAL segment not found in sealed WAL list",
            ))
        }
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

#[async_trait::async_trait]
impl LogStore for WALManager {
    async fn append(&self, cmd: LogCommand) -> Result<LogPosition, std::io::Error> {
        let lsn = self
            .write_log(&cmd.key, &cmd.value, cmd.lsn, cmd.record_type)
            .await?;
        Ok(LogPosition { lsn })
    }

    async fn recover_commands(&self) -> Result<Vec<LogCommand>, std::io::Error> {
        let records = self.recover_records().await?;
        let mut out = Vec::with_capacity(records.len());

        for record in records {
            let record_type = match record.record_type {
                1 => RecordType::Put,
                2 => RecordType::Delete,
                _ => continue,
            };

            out.push(LogCommand {
                record_type,
                key: record.key,
                value: record.value,
                lsn: record.lsn,
            });
        }

        Ok(out)
    }

    async fn rotate(&self) -> Result<u64, std::io::Error> {
        self.rotate_wal_file().await
    }

    async fn mark_reserved(&self, segment_id: u64) -> Result<(), std::io::Error> {
        let _ = self.change_to_reserve(segment_id).await?;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use tokio::io::AsyncReadExt;

    use super::*;

    #[tokio::test]
    async fn initialize_new_file_segment() {
        let wal_dir = std::env::temp_dir().join(uuid::Uuid::new_v4().to_string()); // make sure to use a unique temp directory for each test run
        std::fs::create_dir_all(&wal_dir).unwrap();

        let mut wal_manager = super::WALManager::new(wal_dir.clone(), 1024 * 1024)
            .await
            .unwrap();

        assert_eq!(
            wal_manager
                .active_segment_id
                .0
                .load(std::sync::atomic::Ordering::SeqCst),
            0
        );
        assert_eq!(
            wal_manager
                .active_wal
                .read()
                .await
                .metadata()
                .await
                .unwrap()
                .len(),
            WAL_HEADER_SIZE as u64
        );

        // Clean up
        std::fs::remove_dir_all(wal_dir).unwrap();
    }

    #[tokio::test]
    async fn write_log_creates_new_segment() {
        let wal_dir = std::env::temp_dir().join(uuid::Uuid::new_v4().to_string());
        std::fs::create_dir_all(&wal_dir).unwrap();

        println!("Testing WALManager write_log with wal_dir: {:?}", wal_dir);

        let mut wal_manager = super::WALManager::new(wal_dir.clone(), 1024 * 1024)
            .await
            .unwrap();

        // Write a log entry (no rotation triggered with large segment size)
        let key = b"key".to_vec();
        let value = vec![0u8; 2048];
        let record_type = RecordType::Put;

        let lsn = wal_manager
            .write_log(&key, &value, 1, record_type)
            .await
            .unwrap();
        assert_eq!(lsn, 1);

        // Segment ID should remain at 0 (no rotation with 1MB segment size)
        assert_eq!(
            wal_manager
                .active_segment_id
                .0
                .load(std::sync::atomic::Ordering::SeqCst),
            0
        );

        // check the attached file, is it contains the log entry we just wrote?
        let wal_file_path = wal_dir.join(wal_manager.active_segment_id.filename());
        let mut wal_file = TokioFile::open(&wal_file_path).await.unwrap();

        // read from the header_last_offset to the end of the file
        let mut buf = Vec::new();
        wal_file.read_to_end(&mut buf).await.unwrap();

        // parse the header first...
        let header = WALHeader::decode(&buf[0..WAL_HEADER_SIZE as usize].to_vec()).unwrap();
        assert_eq!(header.magic, *WAL_MAGIC);
        assert_eq!(header.version, WAL_VERSION);
        assert_eq!(header.last_checkpoint, 0);
        assert_eq!(header.last_checkpoint_offset, 0);

        // verify the log entry content
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
            .write_log(&key, &value, 1, RecordType::Put)
            .await
            .unwrap();

        // Verify LSN starts from 1
        assert_eq!(lsn, 1);

        // Read back and verify
        let wal_file_path = wal_dir.join("00000000000000000000.log");
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
                .write_log(&key, &value, 1, RecordType::Put)
                .await
                .unwrap();
            expected_lsns.push(lsn);
        }

        // Read back and verify all records
        let wal_file_path = wal_dir.join("00000000000000000000.log");
        let records = WALManager::read_log(wal_file_path).await.unwrap();

        assert_eq!(records.len(), 5);
        for (i, record) in records.iter().enumerate() {
            assert_eq!(record.key, format!("key_{}", i).into_bytes());
            assert_eq!(record.value, format!("value_{}", i).into_bytes());
            assert_eq!(record.lsn, (1) as u64);
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
            .write_log(&key, &vec![], 1, RecordType::Delete)
            .await
            .unwrap();

        assert_eq!(lsn, 1);

        // Read back and verify
        let wal_file_path = wal_dir.join("00000000000000000000.log");
        let records = WALManager::read_log(wal_file_path).await.unwrap();

        assert_eq!(records.len(), 1);
        assert_eq!(records[0].key, key);
        assert_eq!(records[0].value, vec![] as Vec<u8>);
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
            .write_log(&key, &value, 1, RecordType::Put)
            .await
            .unwrap();

        assert_eq!(lsn, 1);

        // Read back and verify
        let wal_file_path = wal_dir.join("00000000000000000000.log");
        let records = WALManager::read_log(wal_file_path).await.unwrap();

        assert_eq!(records.len(), 1);
        assert_eq!(records[0].key, key);
        assert_eq!(records[0].value, vec![] as Vec<u8>);
        assert_eq!(records[0].record_type, RecordType::Put as u8);

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
            .write_log(&key, &value, 1, RecordType::Put)
            .await
            .unwrap();

        assert_eq!(lsn, 1);

        // Read back and verify
        let wal_file_path = wal_dir.join("00000000000000000000.log");
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
                .write_log(&b"key1".to_vec(), &b"value1".to_vec(), 1, RecordType::Put)
                .await
                .unwrap();
            wal_manager
                .write_log(&b"key2".to_vec(), &b"value2".to_vec(), 1, RecordType::Put)
                .await
                .unwrap();
            wal_manager
                .write_log(&b"key3".to_vec(), &b"value3".to_vec(), 1, RecordType::Put)
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
                .write_log(&b"key1".to_vec(), &b"value1".to_vec(), 1, RecordType::Put)
                .await
                .unwrap();
            // Delete it
            wal_manager
                .write_log(&b"key1".to_vec(), &b"".to_vec(), 1, RecordType::Delete)
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
    async fn recover_checksum_verification() {
        let wal_dir = std::env::temp_dir().join(uuid::Uuid::new_v4().to_string());
        std::fs::create_dir_all(&wal_dir).unwrap();

        // Write a valid record
        {
            let mut wal_manager = super::WALManager::new(wal_dir.clone(), 1024 * 1024)
                .await
                .unwrap();

            wal_manager
                .write_log(&b"key".to_vec(), &b"value".to_vec(), 1, RecordType::Put)
                .await
                .unwrap();
        }

        // Corrupt the WAL file by modifying a byte in the data
        {
            // Use zero-padded segment ID format (20 digits)
            let wal_file_path = wal_dir.join("00000000000000000000.log");
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
