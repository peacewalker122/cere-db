use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
    sync::atomic::AtomicU64,
};

use crossbeam_skiplist::SkipMap;
use tokio::{
    fs::{File, OpenOptions},
    io::AsyncWriteExt,
    sync::Mutex,
};

use super::{
    log_store::{LogCommand, LogPosition, LogStore},
    segment::SegmentId,
};
use crate::{
    error::DBError,
    storage::{constant::*, record::RecordType},
};

const LEGACY_WAL_VERSION: u64 = 1;
const MAX_FRAME_SIZE: usize = 64 * 1024 * 1024;
const FRAME_HEADER_SIZE: usize = 20;

pub struct WALManager {
    state: Mutex<WriterState>,
    max_segment_size: u64,
    wal_dir: PathBuf,
}

struct WriterState {
    file: File,
    segment_id: u64,
    size: u64,
    next_lsn: u64,
    poisoned: bool,
    sealed: BTreeSet<u64>,
}

pub type FileWalStore = WALManager;

#[derive(Debug, Clone)]
pub struct WALRecord {
    pub record_type: u8,
    pub lsn: u64,
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub checksum: u32,
}

#[derive(Debug)]
enum TransactionRecord {
    Update(LogCommand),
    Commit,
}

#[derive(Debug)]
struct Frame {
    lsn: u64,
    transaction_id: u64,
    previous_lsn: u64,
    record: TransactionRecord,
}

#[derive(Default)]
struct Analysis {
    latest_lsn: u64,
    previous_lsns: BTreeMap<u64, u64>,
    committed_transactions: BTreeSet<u64>,
    updates: Vec<LoggedUpdate>,
    legacy: Vec<WALRecord>,
}

struct LoggedUpdate {
    transaction_id: u64,
    command: LogCommand,
    checksum: u32,
}

impl Analysis {
    fn accept(&mut self, frame: Frame, checksum: u32) -> Result<(), DBError> {
        let previous_lsn = self
            .previous_lsns
            .get(&frame.transaction_id)
            .copied()
            .unwrap_or(0);
        if frame.lsn != self.latest_lsn + 1
            || frame.transaction_id == 0
            || self.committed_transactions.contains(&frame.transaction_id)
            || frame.previous_lsn != previous_lsn
        {
            return Err(DBError::Corrupted("Invalid WAL transaction chain".into()));
        }
        match frame.record {
            TransactionRecord::Update(command) => {
                self.updates.push(LoggedUpdate {
                    transaction_id: frame.transaction_id,
                    command,
                    checksum,
                });
            }
            TransactionRecord::Commit => {
                if previous_lsn == 0 {
                    return Err(DBError::Corrupted("Commit without update".into()));
                }
                self.committed_transactions.insert(frame.transaction_id);
            }
        }
        self.latest_lsn = frame.lsn;
        self.previous_lsns.insert(frame.transaction_id, frame.lsn);
        Ok(())
    }

    fn redo(self) -> Vec<WALRecord> {
        let mut records = self.legacy;
        records.extend(
            self.updates
                .into_iter()
                .filter(|update| self.committed_transactions.contains(&update.transaction_id))
                .map(|update| WALRecord {
                    record_type: update.command.record_type as u8,
                    lsn: update.command.lsn,
                    key: update.command.key,
                    value: update.command.value,
                    checksum: update.checksum,
                }),
        );
        records
    }
}

impl WALManager {
    pub async fn new(wal_dir: PathBuf, max_segment_size: u64) -> Result<Self, DBError> {
        tokio::fs::create_dir_all(&wal_dir).await?;
        let paths = segment_paths(&wal_dir).await?;
        let mut analysis = Analysis::default();
        let mut latest_version = WAL_VERSION;
        for (index, (_, path)) in paths.iter().enumerate() {
            latest_version = scan_segment(path, index + 1 == paths.len(), &mut analysis).await?;
        }
        let mut segment_id = paths.last().map_or(0, |(id, _)| *id);
        let mut sealed: BTreeSet<u64> = paths.iter().map(|(id, _)| *id).collect();
        if latest_version == LEGACY_WAL_VERSION && !paths.is_empty() {
            segment_id = segment_id
                .checked_add(1)
                .ok_or_else(|| DBError::StorageError("WAL segment ID exhausted".into()))?;
        }
        let path = segment_path(&wal_dir, segment_id);
        let file = if paths.is_empty() || latest_version == LEGACY_WAL_VERSION {
            create_segment(&wal_dir, segment_id).await?
        } else {
            OpenOptions::new().append(true).open(path).await?
        };
        sealed.remove(&segment_id);
        let size = file.metadata().await?.len();
        Ok(Self {
            state: Mutex::new(WriterState {
                file,
                segment_id,
                size,
                next_lsn: analysis.latest_lsn + 1,
                poisoned: false,
                sealed,
            }),
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
    ) -> Result<u64, DBError> {
        self.write_transaction(vec![LogCommand::new(
            record_type,
            key.clone(),
            value.clone(),
            lsn,
        )])
        .await?;
        Ok(lsn)
    }

    pub async fn write_transaction(&self, commands: Vec<LogCommand>) -> Result<(), DBError> {
        if commands.is_empty() {
            return Err(DBError::StorageError(
                "WAL transaction must contain an update".into(),
            ));
        }
        let mut state = self.state.lock().await;
        ensure_healthy(&state)?;
        let transaction_id = state.next_lsn;
        let mut bytes = Vec::new();
        let mut previous_lsn = 0;
        let mut next_lsn = state.next_lsn;
        for command in commands {
            if command.record_type == RecordType::Delete && !command.value.is_empty() {
                return Err(DBError::StorageError("Delete contains a value".into()));
            }
            bytes.extend(encode_frame(&Frame {
                lsn: next_lsn,
                transaction_id,
                previous_lsn,
                record: TransactionRecord::Update(command),
            })?);
            previous_lsn = next_lsn;
            next_lsn = next_lsn
                .checked_add(1)
                .ok_or_else(|| DBError::StorageError("WAL LSN exhausted".into()))?;
        }
        bytes.extend(encode_frame(&Frame {
            lsn: next_lsn,
            transaction_id,
            previous_lsn,
            record: TransactionRecord::Commit,
        })?);
        let following_lsn = next_lsn
            .checked_add(1)
            .ok_or_else(|| DBError::StorageError("WAL LSN exhausted".into()))?;
        state.poisoned = true;
        if state.size > WAL_HEADER_SIZE
            && state.size.saturating_add(bytes.len() as u64) > self.max_segment_size
        {
            self.rotate_locked(&mut state).await?;
        }
        state.file.write_all(&bytes).await?;
        state.file.sync_data().await?;
        state.size += bytes.len() as u64;
        state.next_lsn = following_lsn;
        state.poisoned = false;
        Ok(())
    }

    pub async fn recover(
        &self,
        memtable: &mut SkipMap<Vec<u8>, (RecordType, Vec<u8>)>,
    ) -> Result<(), DBError> {
        for record in self.recover_records().await? {
            let record_type = decode_record_type(record.record_type)?;
            memtable.insert(record.key, (record_type, record.value));
        }
        Ok(())
    }

    pub async fn recover_records(&self) -> Result<Vec<WALRecord>, DBError> {
        let state = self.state.lock().await;
        ensure_healthy(&state)?;
        let mut analysis = Analysis::default();
        for (_, path) in segment_paths(&self.wal_dir).await? {
            scan_segment(&path, false, &mut analysis).await?;
        }
        Ok(analysis.redo())
    }

    async fn rotate_locked(&self, state: &mut WriterState) -> Result<(), DBError> {
        state.file.sync_all().await?;
        let next_id = state
            .segment_id
            .checked_add(1)
            .ok_or_else(|| DBError::StorageError("WAL segment ID exhausted".into()))?;
        let file = create_segment(&self.wal_dir, next_id).await?;
        state.sealed.insert(state.segment_id);
        state.file = file;
        state.segment_id = next_id;
        state.size = WAL_HEADER_SIZE;
        Ok(())
    }

    pub async fn rotate_wal_file(&self) -> Result<u64, DBError> {
        let mut state = self.state.lock().await;
        ensure_healthy(&state)?;
        state.poisoned = true;
        self.rotate_locked(&mut state).await?;
        state.poisoned = false;
        Ok(state.segment_id)
    }

    pub async fn change_to_reserve(&self, locked_wal: u64) -> Result<SegmentId, DBError> {
        let mut state = self.state.lock().await;
        ensure_healthy(&state)?;
        if !state.sealed.remove(&locked_wal) {
            return Err(DBError::StorageError("WAL segment is not sealed".into()));
        }
        Ok(SegmentId(AtomicU64::new(locked_wal)))
    }

    #[cfg(test)]
    async fn read_committed_file(path: PathBuf) -> Result<Vec<WALRecord>, DBError> {
        let mut analysis = Analysis::default();
        scan_segment(&path, false, &mut analysis).await?;
        Ok(analysis.redo())
    }
}

fn ensure_healthy(state: &WriterState) -> Result<(), DBError> {
    if state.poisoned {
        return Err(DBError::StorageError(
            "WAL write interrupted or failed; reopen before reuse".into(),
        ));
    }
    Ok(())
}

fn segment_path(directory: &Path, id: u64) -> PathBuf {
    directory.join(format!("{id:020}.log"))
}

async fn segment_paths(directory: &Path) -> Result<Vec<(u64, PathBuf)>, DBError> {
    let mut entries = tokio::fs::read_dir(directory).await?;
    let mut paths = Vec::new();
    while let Some(entry) = entries.next_entry().await? {
        if let Some(id) = entry
            .file_name()
            .to_str()
            .and_then(|name| name.strip_suffix(".log"))
            .and_then(|id| id.parse::<u64>().ok())
        {
            if !entry.file_type().await?.is_file() {
                return Err(DBError::Corrupted(
                    "WAL segment is not a regular file".into(),
                ));
            }
            paths.push((id, entry.path()));
        }
    }
    paths.sort_by_key(|(id, _)| *id);
    if paths
        .windows(2)
        .any(|pair| pair[0].0 == pair[1].0 || pair[0].0.checked_add(1) != Some(pair[1].0))
    {
        return Err(DBError::Corrupted(
            "Duplicate or missing WAL segment".into(),
        ));
    }
    Ok(paths)
}

async fn create_segment(directory: &Path, id: u64) -> Result<File, DBError> {
    let mut file = OpenOptions::new()
        .create_new(true)
        .append(true)
        .open(segment_path(directory, id))
        .await?;
    file.write_all(&WALHeader::default().encode()).await?;
    file.sync_all().await?;
    File::open(directory).await?.sync_all().await?;
    Ok(file)
}

fn encode_frame(frame: &Frame) -> Result<Vec<u8>, DBError> {
    if let TransactionRecord::Update(command) = &frame.record {
        let size = 42usize
            .checked_add(command.key.len())
            .and_then(|size| size.checked_add(command.value.len()));
        if size.is_none_or(|size| size > MAX_FRAME_SIZE) {
            return Err(DBError::StorageError("WAL frame exceeds 64 MiB".into()));
        }
    }
    let mut body = Vec::new();
    body.push(match frame.record {
        TransactionRecord::Update(_) => 1,
        TransactionRecord::Commit => 2,
    });
    body.extend_from_slice(&frame.transaction_id.to_be_bytes());
    body.extend_from_slice(&frame.previous_lsn.to_be_bytes());
    if let TransactionRecord::Update(command) = &frame.record {
        body.push(command.record_type as u8);
        body.extend_from_slice(&command.lsn.to_be_bytes());
        body.extend_from_slice(&(command.key.len() as u64).to_be_bytes());
        body.extend_from_slice(&(command.value.len() as u64).to_be_bytes());
        body.extend_from_slice(&command.key);
        body.extend_from_slice(&command.value);
    }
    if body.len() > MAX_FRAME_SIZE {
        return Err(DBError::StorageError("WAL frame exceeds 64 MiB".into()));
    }
    let mut bytes = Vec::with_capacity(FRAME_HEADER_SIZE + body.len());
    bytes.extend_from_slice(&frame.lsn.to_be_bytes());
    bytes.extend_from_slice(&(body.len() as u32).to_be_bytes());
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(&bytes);
    hasher.update(&body);
    bytes.extend_from_slice(&hasher.finalize().to_be_bytes());
    bytes.extend_from_slice(&crc32fast::hash(&bytes).to_be_bytes());
    bytes.extend_from_slice(&body);
    Ok(bytes)
}

struct Decoder<'a> {
    remaining: &'a [u8],
}

impl<'a> Decoder<'a> {
    fn take(&mut self, length: usize) -> Result<&'a [u8], DBError> {
        if length > self.remaining.len() {
            return Err(std::io::Error::from(std::io::ErrorKind::UnexpectedEof).into());
        }
        let (value, remaining) = self.remaining.split_at(length);
        self.remaining = remaining;
        Ok(value)
    }

    fn number(&mut self) -> Result<u64, DBError> {
        Ok(u64::from_be_bytes(self.take(8)?.try_into()?))
    }

    fn payload(&mut self, length: u64) -> Result<Vec<u8>, DBError> {
        let length = usize::try_from(length)
            .map_err(|_| DBError::Corrupted("WAL length overflow".into()))?;
        Ok(self.take(length)?.to_vec())
    }
}

fn decode_record_type(value: u8) -> Result<RecordType, DBError> {
    match value {
        1 => Ok(RecordType::Put),
        2 => Ok(RecordType::Delete),
        _ => Err(DBError::Corrupted("Invalid WAL operation".into())),
    }
}

fn decode_frame(lsn: u64, body: &[u8]) -> Result<Frame, DBError> {
    let mut decoder = Decoder { remaining: body };
    let kind = decoder.take(1)?[0];
    let transaction_id = decoder.number()?;
    let previous_lsn = decoder.number()?;
    let record = match kind {
        1 => {
            let record_type = decode_record_type(decoder.take(1)?[0])?;
            let command_lsn = decoder.number()?;
            let key_length = decoder.number()?;
            let value_length = decoder.number()?;
            let key = decoder.payload(key_length)?;
            let value = decoder.payload(value_length)?;
            if record_type == RecordType::Delete && !value.is_empty() {
                return Err(DBError::Corrupted("Delete contains a value".into()));
            }
            TransactionRecord::Update(LogCommand::new(record_type, key, value, command_lsn))
        }
        2 => TransactionRecord::Commit,
        _ => return Err(DBError::Corrupted("Invalid WAL transaction record".into())),
    };
    if !decoder.remaining.is_empty() {
        return Err(DBError::Corrupted("Trailing WAL record bytes".into()));
    }
    Ok(Frame {
        lsn,
        transaction_id,
        previous_lsn,
        record,
    })
}

async fn scan_segment(
    path: &Path,
    repair_tail: bool,
    analysis: &mut Analysis,
) -> Result<u64, DBError> {
    let mut bytes = tokio::fs::read(path).await?;
    if bytes.len() < WAL_HEADER_SIZE as usize && repair_tail {
        let header = [WAL_VERSION, LEGACY_WAL_VERSION]
            .into_iter()
            .filter(|version| *version != LEGACY_WAL_VERSION || analysis.latest_lsn == 0)
            .map(|version| {
                WALHeader {
                    version,
                    ..WALHeader::default()
                }
                .encode()
            })
            .find(|header| header.starts_with(&bytes))
            .ok_or_else(|| DBError::Corrupted("Invalid incomplete WAL header".into()))?;
        let mut file = OpenOptions::new().append(true).open(path).await?;
        file.write_all(&header[bytes.len()..]).await?;
        file.sync_all().await?;
        File::open(
            path.parent().ok_or_else(|| {
                DBError::StorageError("WAL segment has no parent directory".into())
            })?,
        )
        .await?
        .sync_all()
        .await?;
        bytes = header;
    }
    let header = WALHeader::decode(&bytes)?;
    if header.version == LEGACY_WAL_VERSION && analysis.latest_lsn != 0 {
        return Err(DBError::Corrupted(
            "Legacy WAL after transactional segment".into(),
        ));
    }
    if header.last_checkpoint != 0 || header.last_checkpoint_offset != 0 {
        return Err(DBError::Corrupted(
            "Unsupported WAL checkpoint metadata".into(),
        ));
    }
    let mut offset = WAL_HEADER_SIZE as usize;
    while offset < bytes.len() {
        let start = offset;
        if header.version == LEGACY_WAL_VERSION {
            match decode_legacy(&bytes[offset..]) {
                Ok((record, length)) => {
                    analysis.legacy.push(record);
                    offset += length;
                }
                Err(DBError::IO(error)) if error.kind() == std::io::ErrorKind::UnexpectedEof => {
                    break;
                }
                Err(error) => return Err(error),
            }
        } else {
            if bytes.len() - offset < FRAME_HEADER_SIZE {
                break;
            }
            let header_checksum = u32::from_be_bytes(bytes[offset + 16..offset + 20].try_into()?);
            if header_checksum != crc32fast::hash(&bytes[offset..offset + 16]) {
                return Err(DBError::Corrupted(
                    "WAL frame header checksum mismatch".into(),
                ));
            }
            let lsn = u64::from_be_bytes(bytes[offset..offset + 8].try_into()?);
            let length = u32::from_be_bytes(bytes[offset + 8..offset + 12].try_into()?) as usize;
            if !(17..=MAX_FRAME_SIZE).contains(&length) {
                return Err(DBError::Corrupted("Invalid WAL frame size".into()));
            }
            if bytes.len() - offset - FRAME_HEADER_SIZE < length {
                break;
            }
            let checksum = u32::from_be_bytes(bytes[offset + 12..offset + 16].try_into()?);
            let end = offset + FRAME_HEADER_SIZE + length;
            let body = &bytes[offset + FRAME_HEADER_SIZE..end];
            let mut hasher = crc32fast::Hasher::new();
            hasher.update(&bytes[offset..offset + 12]);
            hasher.update(body);
            if checksum != hasher.finalize() {
                return Err(DBError::Corrupted("WAL frame checksum mismatch".into()));
            }
            let frame = decode_frame(lsn, body)
                .map_err(|error| DBError::Corrupted(format!("Invalid WAL frame: {error}")))?;
            analysis.accept(frame, checksum)?;
            offset = end;
        }
        if offset == start {
            break;
        }
    }
    if offset != bytes.len() {
        if !repair_tail {
            return Err(DBError::Corrupted(
                "Incomplete WAL frame in sealed segment".into(),
            ));
        }
        let file = OpenOptions::new().write(true).open(path).await?;
        file.set_len(offset as u64).await?;
        file.sync_all().await?;
    }
    Ok(header.version)
}

fn decode_legacy(bytes: &[u8]) -> Result<(WALRecord, usize), DBError> {
    let mut decoder = Decoder { remaining: bytes };
    let record_type = decoder.take(1)?[0];
    decode_record_type(record_type)?;
    let lsn = decoder.number()?;
    let key_length = decoder.number()?;
    let key = decoder.payload(key_length)?;
    let value_length = decoder.number()?;
    let value = decoder.payload(value_length)?;
    let payload_length = bytes.len() - decoder.remaining.len();
    let checksum = u32::from_be_bytes(decoder.take(4)?.try_into()?);
    if crc32fast::hash(&bytes[..payload_length]) != checksum {
        return Err(DBError::Corrupted("Legacy WAL checksum mismatch".into()));
    }
    Ok((
        WALRecord {
            record_type,
            lsn,
            key,
            value,
            checksum,
        },
        bytes.len() - decoder.remaining.len(),
    ))
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
    pub fn decode(buf: &[u8]) -> Result<Self, DBError> {
        if buf.len() < WAL_HEADER_SIZE as usize {
            return Err(DBError::Corrupted(
                "Buffer too short for WAL header".to_string(),
            ));
        }

        let mut magic = [0u8; 8];
        magic.copy_from_slice(&buf[0..8]);

        if &magic != WAL_MAGIC {
            return Err(DBError::Corrupted("Invalid WAL magic number".to_string()));
        }

        let mut b = [0u8; 8];
        b.copy_from_slice(&buf[8..16]);
        let version = u64::from_be_bytes(b);
        if version != LEGACY_WAL_VERSION && version != WAL_VERSION {
            return Err(DBError::Corrupted("Unsupported WAL version".into()));
        }

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
            return Err(DBError::Corrupted(
                "WAL header checksum mismatch".to_string(),
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
    async fn append(&self, command: LogCommand) -> Result<LogPosition, DBError> {
        let lsn = command.lsn;
        self.write_transaction(vec![command]).await?;
        Ok(LogPosition { lsn })
    }

    async fn recover_commands(&self) -> Result<Vec<LogCommand>, DBError> {
        self.recover_records()
            .await?
            .into_iter()
            .map(|record| {
                Ok(LogCommand::new(
                    decode_record_type(record.record_type)?,
                    record.key,
                    record.value,
                    record.lsn,
                ))
            })
            .collect()
    }

    async fn rotate(&self) -> Result<u64, DBError> {
        self.rotate_wal_file().await
    }

    async fn mark_reserved(&self, segment_id: u64) -> Result<(), DBError> {
        self.change_to_reserve(segment_id).await?;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use std::io::Write;
    use tokio::io::AsyncReadExt;

    use super::*;

    #[tokio::test]
    async fn initialize_new_file_segment() {
        let wal_dir = std::env::temp_dir().join(uuid::Uuid::new_v4().to_string()); // make sure to use a unique temp directory for each test run
        std::fs::create_dir_all(&wal_dir).unwrap();

        let wal_manager = super::WALManager::new(wal_dir.clone(), 1024 * 1024)
            .await
            .unwrap();

        assert_eq!(wal_manager.state.lock().await.segment_id, 0);
        assert_eq!(
            wal_manager
                .state
                .lock()
                .await
                .file
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

        let wal_manager = super::WALManager::new(wal_dir.clone(), 1024 * 1024)
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
        assert_eq!(wal_manager.state.lock().await.segment_id, 0);

        // check the attached file, is it contains the log entry we just wrote?
        let wal_file_path = wal_dir.join(format!(
            "{:020}.log",
            wal_manager.state.lock().await.segment_id
        ));
        let mut wal_file = File::open(&wal_file_path).await.unwrap();

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
        let log_entry = WALManager::read_committed_file(wal_file_path)
            .await
            .unwrap();
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

        let wal_manager = super::WALManager::new(wal_dir.clone(), 1024 * 1024)
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
        let records = WALManager::read_committed_file(wal_file_path)
            .await
            .unwrap();

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

        let wal_manager = super::WALManager::new(wal_dir.clone(), 1024 * 1024)
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
        let records = WALManager::read_committed_file(wal_file_path)
            .await
            .unwrap();

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

        let wal_manager = super::WALManager::new(wal_dir.clone(), 1024 * 1024)
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
        let records = WALManager::read_committed_file(wal_file_path)
            .await
            .unwrap();

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

        let wal_manager = super::WALManager::new(wal_dir.clone(), 1024 * 1024)
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
        let records = WALManager::read_committed_file(wal_file_path)
            .await
            .unwrap();

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

        let wal_manager = super::WALManager::new(wal_dir.clone(), 1024 * 1024)
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
        let records = WALManager::read_committed_file(wal_file_path)
            .await
            .unwrap();

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
        let wal_manager = super::WALManager::new(wal_dir.clone(), 1024 * 1024)
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
            let wal_manager = super::WALManager::new(wal_dir.clone(), 1024 * 1024)
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
        let wal_manager = super::WALManager::new(wal_dir.clone(), 1024 * 1024)
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
            let wal_manager = super::WALManager::new(wal_dir.clone(), 1024 * 1024)
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
        let wal_manager = super::WALManager::new(wal_dir.clone(), 1024 * 1024)
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
            let wal_manager = super::WALManager::new(wal_dir.clone(), 1024 * 1024)
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
            use std::io::Seek;
            file.seek(std::io::SeekFrom::Start(65)).unwrap();
            file.write_all(&[0xFF]).unwrap();
        }

        // Recover should fail due to checksum mismatch
        let result = super::WALManager::new(wal_dir.clone(), 1024 * 1024).await;
        assert!(matches!(result, Err(DBError::Corrupted(_))));

        std::fs::remove_dir_all(wal_dir).unwrap();
    }
}

#[cfg(test)]
#[path = "wal_tests.rs"]
mod crash_tests;
