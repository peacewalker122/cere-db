use std::{
    borrow::Cow,
    collections::BTreeMap,
    ffi::{OsStr, OsString},
    ops::RangeBounds,
    path::{Component, Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};

use crossbeam_skiplist::SkipMap;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::{
    api::api::{AsyncKVEngine, BatchOperation},
    error::DBError,
    storage::{
        config::StorageConfig,
        durable_dir::{create_dir_all_durable, sync_parent},
        kv2::{KV2, ReferenceIndexOptions},
        manifest_codec::ManifestManager,
        record::{MemtableRecord, RecordType},
        recovermanager::{
            log_store::LogCommand,
            wal::{RecoveredWalUpdate, WAL_VALUE_REF_ENCODED_SIZE, WALManager, WalValueRef},
        },
    },
};

const ROOT_MARKER_FILE: &str = "EXPERIMENTAL-WAL-VALUES";
const CURRENT_FILE: &str = "CURRENT";
const CURRENT_TEMP_FILE: &str = "CURRENT.tmp";
const ROOT_MARKER_TEMP_FILE: &str = "EXPERIMENTAL-WAL-VALUES.tmp";
const NEXT_GENERATION_FILE: &str = "NEXT-GENERATION";
const NEXT_GENERATION_TEMP_FILE: &str = "NEXT-GENERATION.tmp";
const GENERATION_SEAL_FILE: &str = "SEAL";
const GENERATION_SEAL_TEMP_FILE: &str = "SEAL.tmp";
const LOCK_FILE: &str = "LOCK";
const WAL_DIR: &str = "wal";
const INDEX_DIR: &str = "index";
const MANIFEST_FILE: &str = "MANIFEST";
const GENERATIONS_DIR: &str = "generations";
const GENERATION_NAME_DIGITS: usize = 20;
const INITIAL_GENERATION: u64 = 1;
const NEXT_GENERATION_VERSION: u8 = 1;
const NEXT_GENERATION_ENCODED_SIZE: usize = 1 + 8 + 4;
const GENERATION_SEAL_VERSION: u8 = 2;
const GENERATION_SEAL_HEADER_SIZE: usize = 1 + 8 * 4 + 4;
const GENERATION_SEAL_ENTRY_SIZE: usize = 8 + 8 + 4;
const MAX_COPY_TRANSACTION_RECORDS: usize = 256;
const MAX_COPY_TRANSACTION_BYTES: usize = 8 * 1024 * 1024;
const COPY_TRANSACTION_OVERHEAD_BYTES: usize = 37;
const COPY_UPDATE_FRAME_OVERHEAD_BYTES: usize = 62;
const ROOT_MARKER_MAGIC: &[u8; 16] = b"CEREDB-EXP-WAL\0\0";
const ROOT_MARKER_VERSION: u64 = 1;
const ROOT_MARKER_ENCODED_SIZE: usize = 16 + 8 + 4;
const CURRENT_VERSION: u8 = 1;
const CURRENT_ENCODED_SIZE: usize = 1 + 8 + 8 + 4;
const REFERENCE_VALUE_VERSION: u8 = 1;
pub(crate) const REFERENCE_VALUE_ENCODED_SIZE: usize = 1 + 8 + WAL_VALUE_REF_ENCODED_SIZE + 4;

pub(crate) struct RootLock {
    _file: std::fs::File,
}

impl RootLock {
    fn acquire(root: &Path) -> Result<Self, DBError> {
        let file = std::fs::OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(root.join(LOCK_FILE))?;
        match file.try_lock() {
            Ok(()) => Ok(Self { _file: file }),
            Err(std::fs::TryLockError::WouldBlock) => Err(DBError::StorageError(
                "experimental storage root is locked by another engine".into(),
            )),
            Err(std::fs::TryLockError::Error(error)) => Err(DBError::IO(error)),
        }
    }
}

const EXPERIMENTAL_BOOTSTRAP_ARTIFACTS: [&str; 10] = [
    ROOT_MARKER_FILE,
    ROOT_MARKER_TEMP_FILE,
    CURRENT_FILE,
    CURRENT_TEMP_FILE,
    NEXT_GENERATION_FILE,
    NEXT_GENERATION_TEMP_FILE,
    GENERATION_SEAL_FILE,
    GENERATION_SEAL_TEMP_FILE,
    LOCK_FILE,
    GENERATIONS_DIR,
];

const ROOT_FILE_ARTIFACTS: [&str; 10] = [
    ROOT_MARKER_FILE,
    ROOT_MARKER_TEMP_FILE,
    CURRENT_FILE,
    CURRENT_TEMP_FILE,
    NEXT_GENERATION_FILE,
    NEXT_GENERATION_TEMP_FILE,
    GENERATION_SEAL_FILE,
    GENERATION_SEAL_TEMP_FILE,
    LOCK_FILE,
    MANIFEST_FILE,
];

const ROOT_DIRECTORY_ARTIFACTS: [&str; 3] = [GENERATIONS_DIR, WAL_DIR, "sstable"];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageKind {
    Experimental,
    Kv2,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RootEntryKind {
    File,
    Directory,
    Other,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ExpectedEntryKind {
    File,
    Directory,
}

pub(crate) fn ensure_not_experimental_storage(path: &Path) -> Result<(), DBError> {
    if let Some(artifact) = EXPERIMENTAL_BOOTSTRAP_ARTIFACTS
        .into_iter()
        .find(|artifact| path.join(artifact).exists())
    {
        return Err(DBError::StorageError(format!(
            "{} is reserved by an experimental WAL value store ({artifact})",
            path.display()
        )));
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ReferenceValue {
    pub(crate) generation: u64,
    pub(crate) reference: WalValueRef,
}

impl ReferenceValue {
    pub(crate) fn new(generation: u64, reference: WalValueRef) -> Self {
        Self {
            generation,
            reference,
        }
    }

    pub(crate) fn encode(&self) -> [u8; REFERENCE_VALUE_ENCODED_SIZE] {
        let mut bytes = [0u8; REFERENCE_VALUE_ENCODED_SIZE];
        bytes[0] = REFERENCE_VALUE_VERSION;
        bytes[1..9].copy_from_slice(&self.generation.to_be_bytes());
        bytes[9..9 + WAL_VALUE_REF_ENCODED_SIZE].copy_from_slice(&self.reference.encode());
        let checksum = crc32fast::hash(&bytes[..REFERENCE_VALUE_ENCODED_SIZE - 4]);
        bytes[REFERENCE_VALUE_ENCODED_SIZE - 4..].copy_from_slice(&checksum.to_be_bytes());
        bytes
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self, DBError> {
        if bytes.len() != REFERENCE_VALUE_ENCODED_SIZE {
            return Err(DBError::Corrupted("Invalid reference value length".into()));
        }
        if bytes[0] != REFERENCE_VALUE_VERSION {
            return Err(DBError::Corrupted(
                "Unsupported reference value version".into(),
            ));
        }
        let checksum = u32::from_be_bytes(
            bytes[REFERENCE_VALUE_ENCODED_SIZE - 4..]
                .try_into()
                .map_err(|_| DBError::Corrupted("Invalid reference value checksum".into()))?,
        );
        if crc32fast::hash(&bytes[..REFERENCE_VALUE_ENCODED_SIZE - 4]) != checksum {
            return Err(DBError::Corrupted(
                "Reference value checksum mismatch".into(),
            ));
        }
        let generation = u64::from_be_bytes(
            bytes[1..9]
                .try_into()
                .map_err(|_| DBError::Corrupted("Invalid reference value generation".into()))?,
        );
        let reference = WalValueRef::decode(&bytes[9..9 + WAL_VALUE_REF_ENCODED_SIZE])?;
        let value = Self {
            generation,
            reference,
        };
        value.validate()?;
        Ok(value)
    }

    fn validate(&self) -> Result<(), DBError> {
        if self.generation == 0 {
            return Err(DBError::Corrupted(
                "Invalid reference value generation".into(),
            ));
        }
        if self.reference.record_type != RecordType::Put {
            return Err(DBError::Corrupted(
                "Reference value does not reference a put".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CurrentState {
    pub(crate) generation: u64,
    pub(crate) application_sequence_floor: u64,
}

impl CurrentState {
    fn encode(&self) -> [u8; CURRENT_ENCODED_SIZE] {
        let mut bytes = [0u8; CURRENT_ENCODED_SIZE];
        bytes[0] = CURRENT_VERSION;
        bytes[1..9].copy_from_slice(&self.generation.to_be_bytes());
        bytes[9..17].copy_from_slice(&self.application_sequence_floor.to_be_bytes());
        let checksum = crc32fast::hash(&bytes[..CURRENT_ENCODED_SIZE - 4]);
        bytes[CURRENT_ENCODED_SIZE - 4..].copy_from_slice(&checksum.to_be_bytes());
        bytes
    }

    fn decode(bytes: &[u8]) -> Result<Self, DBError> {
        if bytes.len() != CURRENT_ENCODED_SIZE {
            return Err(DBError::Corrupted("Invalid CURRENT length".into()));
        }
        if bytes[0] != CURRENT_VERSION {
            return Err(DBError::Corrupted("Unsupported CURRENT version".into()));
        }
        let checksum = u32::from_be_bytes(
            bytes[CURRENT_ENCODED_SIZE - 4..]
                .try_into()
                .map_err(|_| DBError::Corrupted("Invalid CURRENT checksum".into()))?,
        );
        if crc32fast::hash(&bytes[..CURRENT_ENCODED_SIZE - 4]) != checksum {
            return Err(DBError::Corrupted("CURRENT checksum mismatch".into()));
        }
        let generation = u64::from_be_bytes(
            bytes[1..9]
                .try_into()
                .map_err(|_| DBError::Corrupted("Invalid CURRENT generation".into()))?,
        );
        if generation == 0 {
            return Err(DBError::Corrupted("Invalid CURRENT generation".into()));
        }
        Ok(Self {
            generation,
            application_sequence_floor: u64::from_be_bytes(
                bytes[9..17]
                    .try_into()
                    .map_err(|_| DBError::Corrupted("Invalid CURRENT sequence".into()))?,
            ),
        })
    }
}

struct RootMarker;

impl RootMarker {
    fn encode() -> [u8; ROOT_MARKER_ENCODED_SIZE] {
        let mut bytes = [0u8; ROOT_MARKER_ENCODED_SIZE];
        bytes[..16].copy_from_slice(ROOT_MARKER_MAGIC);
        bytes[16..24].copy_from_slice(&ROOT_MARKER_VERSION.to_be_bytes());
        let checksum = crc32fast::hash(&bytes[..ROOT_MARKER_ENCODED_SIZE - 4]);
        bytes[ROOT_MARKER_ENCODED_SIZE - 4..].copy_from_slice(&checksum.to_be_bytes());
        bytes
    }

    fn decode(bytes: &[u8]) -> Result<(), DBError> {
        if bytes.len() != ROOT_MARKER_ENCODED_SIZE {
            return Err(DBError::Corrupted(
                "Invalid experimental root marker length".into(),
            ));
        }
        if &bytes[..16] != ROOT_MARKER_MAGIC {
            return Err(DBError::Corrupted(
                "Invalid experimental root marker magic".into(),
            ));
        }
        let version = u64::from_be_bytes(
            bytes[16..24]
                .try_into()
                .map_err(|_| DBError::Corrupted("Invalid root marker version".into()))?,
        );
        if version != ROOT_MARKER_VERSION {
            return Err(DBError::Corrupted(
                "Unsupported experimental root marker version".into(),
            ));
        }
        let checksum = u32::from_be_bytes(
            bytes[ROOT_MARKER_ENCODED_SIZE - 4..]
                .try_into()
                .map_err(|_| DBError::Corrupted("Invalid root marker checksum".into()))?,
        );
        if crc32fast::hash(&bytes[..ROOT_MARKER_ENCODED_SIZE - 4]) != checksum {
            return Err(DBError::Corrupted("Root marker checksum mismatch".into()));
        }
        Ok(())
    }
}

enum RootFormat {
    Fresh,
    IncompleteBootstrap(CurrentState),
    Experimental(CurrentState),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct GenerationReservation {
    next_generation: u64,
}

impl GenerationReservation {
    fn encode(&self) -> [u8; NEXT_GENERATION_ENCODED_SIZE] {
        let mut bytes = [0u8; NEXT_GENERATION_ENCODED_SIZE];
        bytes[0] = NEXT_GENERATION_VERSION;
        bytes[1..9].copy_from_slice(&self.next_generation.to_be_bytes());
        let checksum = crc32fast::hash(&bytes[..NEXT_GENERATION_ENCODED_SIZE - 4]);
        bytes[NEXT_GENERATION_ENCODED_SIZE - 4..].copy_from_slice(&checksum.to_be_bytes());
        bytes
    }

    fn decode(bytes: &[u8]) -> Result<Self, DBError> {
        if bytes.len() != NEXT_GENERATION_ENCODED_SIZE {
            return Err(DBError::Corrupted(
                "Invalid generation reservation length".into(),
            ));
        }
        if bytes[0] != NEXT_GENERATION_VERSION {
            return Err(DBError::Corrupted(
                "Unsupported generation reservation version".into(),
            ));
        }
        let checksum = u32::from_be_bytes(
            bytes[NEXT_GENERATION_ENCODED_SIZE - 4..]
                .try_into()
                .map_err(|_| DBError::Corrupted("Invalid reservation checksum".into()))?,
        );
        if crc32fast::hash(&bytes[..NEXT_GENERATION_ENCODED_SIZE - 4]) != checksum {
            return Err(DBError::Corrupted(
                "Generation reservation checksum mismatch".into(),
            ));
        }
        let next_generation = u64::from_be_bytes(
            bytes[1..9]
                .try_into()
                .map_err(|_| DBError::Corrupted("Invalid reserved generation".into()))?,
        );
        if next_generation == 0 {
            return Err(DBError::Corrupted("Invalid reserved generation".into()));
        }
        Ok(Self { next_generation })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct WalSegmentFingerprint {
    segment_id: u64,
    length: u64,
    checksum: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct GenerationSeal {
    generation: u64,
    application_sequence_floor: u64,
    max_live_application_sequence: u64,
    next_physical_lsn: u64,
    wal_segments: Vec<WalSegmentFingerprint>,
}

impl GenerationSeal {
    fn encode(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(
            GENERATION_SEAL_HEADER_SIZE + self.wal_segments.len() * GENERATION_SEAL_ENTRY_SIZE + 4,
        );
        bytes.push(GENERATION_SEAL_VERSION);
        bytes.extend_from_slice(&self.generation.to_be_bytes());
        bytes.extend_from_slice(&self.application_sequence_floor.to_be_bytes());
        bytes.extend_from_slice(&self.max_live_application_sequence.to_be_bytes());
        bytes.extend_from_slice(&self.next_physical_lsn.to_be_bytes());
        bytes.extend_from_slice(&(self.wal_segments.len() as u32).to_be_bytes());
        for segment in &self.wal_segments {
            bytes.extend_from_slice(&segment.segment_id.to_be_bytes());
            bytes.extend_from_slice(&segment.length.to_be_bytes());
            bytes.extend_from_slice(&segment.checksum.to_be_bytes());
        }
        let checksum = crc32fast::hash(&bytes);
        bytes.extend_from_slice(&checksum.to_be_bytes());
        bytes
    }

    fn decode(bytes: &[u8]) -> Result<Self, DBError> {
        if bytes.len() < GENERATION_SEAL_HEADER_SIZE + 4 {
            return Err(DBError::Corrupted("Invalid generation seal length".into()));
        }
        if bytes[0] != GENERATION_SEAL_VERSION {
            return Err(DBError::Corrupted(
                "Unsupported generation seal version".into(),
            ));
        }
        let segment_count = u32::from_be_bytes(
            bytes[33..37]
                .try_into()
                .map_err(|_| DBError::Corrupted("Invalid generation seal segment count".into()))?,
        ) as usize;
        let expected_length = GENERATION_SEAL_HEADER_SIZE
            .checked_add(segment_count * GENERATION_SEAL_ENTRY_SIZE)
            .and_then(|length| length.checked_add(4))
            .ok_or_else(|| DBError::Corrupted("Generation seal length overflow".into()))?;
        if bytes.len() != expected_length {
            return Err(DBError::Corrupted("Invalid generation seal length".into()));
        }
        let checksum = u32::from_be_bytes(
            bytes[expected_length - 4..]
                .try_into()
                .map_err(|_| DBError::Corrupted("Invalid generation seal checksum".into()))?,
        );
        if crc32fast::hash(&bytes[..expected_length - 4]) != checksum {
            return Err(DBError::Corrupted(
                "Generation seal checksum mismatch".into(),
            ));
        }
        let number = |offset: usize| -> u64 {
            u64::from_be_bytes(
                bytes[offset..offset + 8]
                    .try_into()
                    .expect("slice has 8 bytes"),
            )
        };
        let generation = number(1);
        if generation == 0 {
            return Err(DBError::Corrupted(
                "Invalid generation seal generation".into(),
            ));
        }
        let mut wal_segments = Vec::with_capacity(segment_count);
        for index in 0..segment_count {
            let offset = GENERATION_SEAL_HEADER_SIZE + index * GENERATION_SEAL_ENTRY_SIZE;
            let segment = WalSegmentFingerprint {
                segment_id: number(offset),
                length: number(offset + 8),
                checksum: u32::from_be_bytes(
                    bytes[offset + 16..offset + 20]
                        .try_into()
                        .expect("slice has 4 bytes"),
                ),
            };
            if wal_segments
                .last()
                .is_some_and(|previous: &WalSegmentFingerprint| {
                    previous.segment_id >= segment.segment_id
                })
            {
                return Err(DBError::Corrupted(
                    "Generation seal WAL segments are not ordered".into(),
                ));
            }
            wal_segments.push(segment);
        }
        Ok(Self {
            generation,
            application_sequence_floor: number(9),
            max_live_application_sequence: number(17),
            next_physical_lsn: number(25),
            wal_segments,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OperationalState {
    Active,
    Poisoned,
    Closed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CleanStats {
    pub old_generation: u64,
    pub new_generation: u64,
    pub live_records: u64,
    pub live_logical_bytes: u64,
    pub copied_payload_bytes: u64,
    pub copied_transactions: u64,
    pub index_sstables_written: u64,
    pub total_root_bytes_before: u64,
    pub observed_peak_bytes: u64,
    pub total_root_bytes_after: u64,
    pub deferred_cleanup_bytes: u64,
    pub write_pause: Duration,
    pub elapsed: Duration,
}

pub(crate) struct GenerationState {
    pub(crate) generation: u64,
    wal: Arc<WALManager>,
}

pub struct ExperimentalEngine {
    inner: KV2,
    generation: Arc<GenerationState>,
    root: PathBuf,
    state: OperationalState,
    _root_lock: Arc<RootLock>,
}

impl ExperimentalEngine {
    pub async fn open(root: impl AsRef<Path>) -> Result<Self, DBError> {
        Self::open_with_config(root, StorageConfig::default()).await
    }

    pub async fn open_with_config(
        root: impl AsRef<Path>,
        config: StorageConfig,
    ) -> Result<Self, DBError> {
        config.validate()?;
        let root = root.as_ref().to_path_buf();
        let root_format = preflight_root(&root).await?;
        create_dir_all_durable(&root).await?;
        let root_lock = Arc::new(RootLock::acquire(&root)?);
        let current = prepare_root(&root, root_format).await?;

        let generation = current.generation;
        let generation_dir = generation_path(&root, generation);
        let wal_dir = generation_dir.join(WAL_DIR);
        let index_dir = generation_dir.join(INDEX_DIR);
        let wal_manager =
            Arc::new(WALManager::new(wal_dir.clone(), config.wal_segment_size).await?);
        let recovered = wal_manager.recover_records_with_refs().await?;
        let (recovered_memtable, recovered_memtable_size, recovered_sequence) =
            build_reference_memtable(generation, recovered)?;
        let (_, next_physical_lsn) = wal_manager.recovery_position().await?;
        let seal = read_generation_seal(&generation_dir).await?;
        validate_recovery_against_current(
            &wal_dir,
            current,
            seal.as_ref(),
            recovered_sequence,
            next_physical_lsn,
        )
        .await?;
        let sequence_floor = recovered_sequence.max(current.application_sequence_floor);
        if sequence_floor > current.application_sequence_floor {
            write_current(
                &root,
                CurrentState {
                    generation,
                    application_sequence_floor: sequence_floor,
                },
            )
            .await?;
        }

        let inner = KV2::open_reference_index(
            ReferenceIndexOptions {
                index_dir,
                wal_dir,
                wal_manager: Arc::clone(&wal_manager),
                recovered_memtable,
                recovered_memtable_size,
                recovered_sequence: sequence_floor,
                generation,
                root_lock: Arc::clone(&root_lock),
            },
            config,
        )
        .await?;

        cleanup_abandoned_generations(&root, generation).await?;

        Ok(Self {
            inner,
            generation: Arc::new(GenerationState {
                generation,
                wal: wal_manager,
            }),
            root,
            state: OperationalState::Active,
            _root_lock: root_lock,
        })
    }

    pub async fn shutdown(&mut self) -> Result<(), DBError> {
        self.ensure_operational("shutdown")?;
        self.inner.shutdown().await?;
        let current = read_current(&self.root).await?;
        if current.generation != self.generation.generation {
            return Err(DBError::Corrupted(
                "CURRENT generation does not match the open engine".into(),
            ));
        }
        let sequence = self
            .inner
            .current_sequence_number()
            .max(current.application_sequence_floor);
        if sequence > current.application_sequence_floor {
            write_current(
                &self.root,
                CurrentState {
                    generation: current.generation,
                    application_sequence_floor: sequence,
                },
            )
            .await?;
        }
        self.state = OperationalState::Closed;
        Ok(())
    }

    pub async fn clean(&mut self) -> Result<CleanStats, DBError> {
        let started = Instant::now();
        self.ensure_operational("clean")?;
        self.inner.await_pending_writes().await?;

        let old_generation = self.generation.generation;
        let highwater = self.inner.current_sequence_number();
        let config = (*self.inner.config).clone();
        let total_root_bytes_before = directory_bytes(&self.root).await?;
        let live = self.inner.scan_records(..).await?;

        let new_generation = reserve_next_generation(&self.root).await?;
        bootstrap_generation(&self.root, new_generation).await?;
        let new_dir = generation_path(&self.root, new_generation);
        let new_wal =
            Arc::new(WALManager::new(new_dir.join(WAL_DIR), config.wal_segment_size).await?);

        let old_lease = Arc::clone(&self.generation);
        let copied = copy_live_records(&old_lease, &new_wal, new_generation, live).await?;
        if copied.max_live_sequence > highwater {
            return Err(DBError::Corrupted(
                "live index records exceed the current application sequence".into(),
            ));
        }

        let mut new_inner = KV2::open_reference_index(
            ReferenceIndexOptions {
                index_dir: new_dir.join(INDEX_DIR),
                wal_dir: new_dir.join(WAL_DIR),
                wal_manager: Arc::clone(&new_wal),
                recovered_memtable: copied.memtable,
                recovered_memtable_size: copied.memtable_size,
                recovered_sequence: highwater,
                generation: new_generation,
                root_lock: Arc::clone(&self._root_lock),
            },
            config,
        )
        .await?;

        let index_sstables_written = if copied.live_records > 0 {
            new_inner.flush_reference_memtable().await?;
            1
        } else {
            0
        };
        validate_reference_index(&new_inner, &new_wal, new_generation).await?;

        let wal_segments = seal_wal_segments(&new_dir.join(WAL_DIR)).await?;
        let (_, next_physical_lsn) = new_wal.recovery_position().await?;
        write_generation_seal(
            &new_dir,
            &GenerationSeal {
                generation: new_generation,
                application_sequence_floor: highwater,
                max_live_application_sequence: copied.max_live_sequence,
                next_physical_lsn,
                wal_segments,
            },
        )
        .await?;
        #[cfg(test)]
        clean_fault_point(CleanFaultPoint::AfterNewGenerationDurable).await?;

        let observed_peak_bytes = directory_bytes(&self.root).await?;
        let published = CurrentState {
            generation: new_generation,
            application_sequence_floor: highwater,
        };
        #[cfg(test)]
        clean_fault_point(CleanFaultPoint::BeforeCurrentTempWrite).await?;
        write_current_temp(&self.root, published).await?;
        self.state = OperationalState::Poisoned;
        #[cfg(test)]
        clean_fault_point(CleanFaultPoint::BeforeCurrentRename).await?;
        commit_current_temp(&self.root).await?;
        #[cfg(test)]
        clean_fault_point(CleanFaultPoint::AfterCurrentDurableBeforeAdopt).await?;

        let old_inner = std::mem::replace(&mut self.inner, new_inner);
        let old_generation_state = std::mem::replace(
            &mut self.generation,
            Arc::new(GenerationState {
                generation: new_generation,
                wal: Arc::clone(&new_wal),
            }),
        );
        self.state = OperationalState::Active;
        let write_pause = started.elapsed();
        drop(old_generation_state);

        let deferred_cleanup_bytes =
            reclaim_after_switch(&self.root, old_inner, new_generation).await;
        let total_root_bytes_after = directory_bytes(&self.root).await?;

        Ok(CleanStats {
            old_generation,
            new_generation,
            live_records: copied.live_records,
            live_logical_bytes: copied.live_logical_bytes,
            copied_payload_bytes: copied.copied_payload_bytes,
            copied_transactions: copied.copied_transactions,
            index_sstables_written,
            total_root_bytes_before,
            observed_peak_bytes,
            total_root_bytes_after,
            deferred_cleanup_bytes,
            write_pause,
            elapsed: started.elapsed(),
        })
    }

    fn ensure_operational(&self, operation: &str) -> Result<(), DBError> {
        match self.state {
            OperationalState::Active => Ok(()),
            OperationalState::Poisoned => Err(DBError::StorageError(format!(
                "experimental engine stopped after an interrupted generation switch; reopen before {operation}"
            ))),
            OperationalState::Closed => Err(DBError::StorageError(format!(
                "experimental engine is closed; reopen before {operation}"
            ))),
        }
    }

    async fn resolve_record(
        lease: &GenerationState,
        key: &[u8],
        record: &MemtableRecord,
    ) -> Result<Vec<u8>, DBError> {
        if record.record_type != RecordType::Put {
            return Err(DBError::Corrupted(
                "WAL reference index resolved a non-put record".into(),
            ));
        }
        let value = ReferenceValue::decode(&record.value)?;
        if value.generation != lease.generation {
            return Err(DBError::Corrupted(
                "WAL value reference generation mismatch".into(),
            ));
        }
        if value.reference.application_sequence != record.lsn {
            return Err(DBError::Corrupted(
                "WAL value reference sequence mismatch".into(),
            ));
        }
        lease
            .wal
            .read_value(&value.reference, key, record.lsn)
            .await
    }
}

impl AsyncKVEngine for ExperimentalEngine {
    async fn get(&self, key: &[u8]) -> Result<Option<Cow<'_, Vec<u8>>>, DBError> {
        self.ensure_operational("read")?;
        let lease = Arc::clone(&self.generation);
        let Some(record) = self.inner.get_record(key).await? else {
            return Ok(None);
        };
        if record.record_type == RecordType::Delete {
            return Ok(None);
        }
        Ok(Some(Cow::Owned(
            Self::resolve_record(&lease, key, &record).await?,
        )))
    }

    async fn put(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<(), DBError> {
        self.ensure_operational("write")?;
        self.inner.put(key, value).await
    }

    async fn delete(&mut self, key: Vec<u8>) -> Result<(), DBError> {
        self.ensure_operational("write")?;
        self.inner.delete(key).await
    }

    async fn write_batch(&mut self, operations: Vec<BatchOperation>) -> Result<(), DBError> {
        self.ensure_operational("write")?;
        self.inner.write_batch(operations).await
    }

    async fn await_pending_writes(&self) -> Result<(), DBError> {
        self.ensure_operational("wait for pending writes")?;
        self.inner.await_pending_writes().await
    }

    async fn scan(
        &self,
        range: impl RangeBounds<Vec<u8>> + Send,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, DBError> {
        self.ensure_operational("scan")?;
        let lease = Arc::clone(&self.generation);
        let records = self.inner.scan_records(range).await?;
        let mut results = Vec::with_capacity(records.len());
        for (key, record) in records {
            results.push((
                key.clone(),
                Self::resolve_record(&lease, &key, &record).await?,
            ));
        }
        Ok(results)
    }
}

fn generation_path(root: &Path, generation: u64) -> PathBuf {
    root.join(GENERATIONS_DIR).join(format!("{generation:020}"))
}

async fn prepare_root(root: &Path, root_format: RootFormat) -> Result<CurrentState, DBError> {
    match root_format {
        RootFormat::Fresh => {
            let current = CurrentState {
                generation: INITIAL_GENERATION,
                application_sequence_floor: 0,
            };
            bootstrap_generation(root, current.generation).await?;
            write_current(root, current).await?;
            write_marker(&root.join(ROOT_MARKER_FILE)).await?;
            Ok(current)
        }
        RootFormat::IncompleteBootstrap(current) => {
            write_marker(&root.join(ROOT_MARKER_FILE)).await?;
            Ok(current)
        }
        RootFormat::Experimental(current) => Ok(current),
    }
}

async fn bootstrap_generation(root: &Path, generation: u64) -> Result<(), DBError> {
    let generation_dir = generation_path(root, generation);
    let index_dir = generation_dir.join(INDEX_DIR);
    create_dir_all_durable(&generation_dir.join(WAL_DIR)).await?;
    create_dir_all_durable(&index_dir).await?;
    write_marker(&index_dir.join(ROOT_MARKER_FILE)).await?;
    ManifestManager::load_or_create(index_dir.join(MANIFEST_FILE)).await?;
    Ok(())
}

async fn preflight_root(root: &Path) -> Result<RootFormat, DBError> {
    let entries = read_root_entries(root).await?;
    let kind = classify_root(root, &entries)?;
    if kind == StorageKind::Kv2 {
        return Err(DBError::StorageError(
            "KV2 storage root cannot be opened as experimental storage".into(),
        ));
    }
    validate_generation_directory_entries(root).await?;
    let root_format = inspect_root(root).await?;
    if let RootFormat::IncompleteBootstrap(current) | RootFormat::Experimental(current) =
        &root_format
    {
        validate_generation(root, *current).await?;
    }
    Ok(root_format)
}

pub async fn classify_storage_root(root: impl AsRef<Path>) -> Result<StorageKind, DBError> {
    let root = root.as_ref();
    let entries = read_root_entries(root).await?;
    classify_root(root, &entries)
}

async fn read_root_entries(root: &Path) -> Result<BTreeMap<OsString, RootEntryKind>, DBError> {
    match tokio::fs::metadata(root).await {
        Ok(metadata) if metadata.is_dir() => {}
        Ok(_) => {
            return Err(DBError::StorageError(format!(
                "storage root is not a directory: {}",
                root.display()
            )));
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(BTreeMap::new());
        }
        Err(error) => return Err(error.into()),
    }

    let mut directory = tokio::fs::read_dir(root).await?;
    let mut entries = BTreeMap::new();
    while let Some(entry) = directory.next_entry().await? {
        let file_type = entry.file_type().await?;
        let kind = if file_type.is_file() {
            RootEntryKind::File
        } else if file_type.is_dir() {
            RootEntryKind::Directory
        } else {
            RootEntryKind::Other
        };
        entries.insert(entry.file_name(), kind);
    }
    validate_root_entry_types(root, &entries)?;
    Ok(entries)
}

fn validate_root_entry_types(
    root: &Path,
    entries: &BTreeMap<OsString, RootEntryKind>,
) -> Result<(), DBError> {
    for name in ROOT_FILE_ARTIFACTS {
        validate_entry_type(root, entries, name, ExpectedEntryKind::File)?;
    }
    for name in ROOT_DIRECTORY_ARTIFACTS {
        validate_entry_type(root, entries, name, ExpectedEntryKind::Directory)?;
    }
    Ok(())
}

fn validate_entry_type(
    root: &Path,
    entries: &BTreeMap<OsString, RootEntryKind>,
    name: &str,
    expected: ExpectedEntryKind,
) -> Result<(), DBError> {
    let Some(actual) = entries.get(OsStr::new(name)) else {
        return Ok(());
    };
    let valid = matches!(
        (actual, expected),
        (RootEntryKind::File, ExpectedEntryKind::File)
            | (RootEntryKind::Directory, ExpectedEntryKind::Directory)
    );
    if valid {
        Ok(())
    } else {
        Err(DBError::StorageError(format!(
            "unsafe storage artifact type at {}",
            root.join(name).display()
        )))
    }
}

fn classify_root(
    root: &Path,
    entries: &BTreeMap<OsString, RootEntryKind>,
) -> Result<StorageKind, DBError> {
    if entries.is_empty() {
        return Ok(StorageKind::Experimental);
    }
    let has_kv2_artifact = [MANIFEST_FILE, "wal", "sstable"]
        .iter()
        .any(|name| entries.contains_key(OsStr::new(name)));
    let has_experimental_artifact = EXPERIMENTAL_BOOTSTRAP_ARTIFACTS
        .iter()
        .any(|name| entries.contains_key(OsStr::new(name)));
    if has_kv2_artifact && has_experimental_artifact {
        return Err(DBError::StorageError(format!(
            "mixed KV2 and experimental storage artifacts at {}",
            root.display()
        )));
    }
    if has_kv2_artifact {
        let is_kv2_layout = entries.get(OsStr::new(MANIFEST_FILE)) == Some(&RootEntryKind::File)
            && entries.get(OsStr::new("wal")) == Some(&RootEntryKind::Directory)
            && entries.get(OsStr::new("sstable")) == Some(&RootEntryKind::Directory);
        return if is_kv2_layout {
            Ok(StorageKind::Kv2)
        } else {
            Err(DBError::StorageError(format!(
                "incomplete KV2 storage layout at {}",
                root.display()
            )))
        };
    }
    if has_experimental_artifact {
        let has_identity = entries.contains_key(OsStr::new(ROOT_MARKER_FILE))
            || entries.contains_key(OsStr::new(CURRENT_FILE));
        return if has_identity {
            Ok(StorageKind::Experimental)
        } else if entries.len() == 1 && entries.contains_key(OsStr::new(LOCK_FILE)) {
            Err(DBError::StorageError(format!(
                "unknown non-empty storage root at {}",
                root.display()
            )))
        } else {
            Err(DBError::StorageError(format!(
                "incomplete experimental bootstrap artifacts at {}",
                root.display()
            )))
        };
    }
    Err(DBError::StorageError(format!(
        "unknown non-empty storage root at {}",
        root.display()
    )))
}

async fn inspect_root(root: &Path) -> Result<RootFormat, DBError> {
    let marker = match tokio::fs::read(root.join(ROOT_MARKER_FILE)).await {
        Ok(bytes) => Some(bytes),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(error) => return Err(error.into()),
    };
    let current = match tokio::fs::read(root.join(CURRENT_FILE)).await {
        Ok(bytes) => Some(CurrentState::decode(&bytes)?),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(error) => return Err(error.into()),
    };
    match (marker, current) {
        (Some(bytes), Some(current)) => {
            RootMarker::decode(&bytes)?;
            Ok(RootFormat::Experimental(current))
        }
        (Some(_), None) => Err(DBError::Corrupted(
            "experimental root marker has no CURRENT".into(),
        )),
        (None, Some(current)) => Ok(RootFormat::IncompleteBootstrap(current)),
        (None, None) => Ok(RootFormat::Fresh),
    }
}

async fn validate_generation_directory_entries(root: &Path) -> Result<(), DBError> {
    let path = root.join(GENERATIONS_DIR);
    if !is_entry_kind(&path, ExpectedEntryKind::Directory).await? {
        return Ok(());
    }
    let mut entries = tokio::fs::read_dir(path).await?;
    while let Some(entry) = entries.next_entry().await? {
        let name = entry.file_name();
        if parse_generation_name(&name.to_string_lossy()).is_none()
            || !entry.file_type().await?.is_dir()
        {
            return Err(DBError::Corrupted(format!(
                "invalid generation entry: {}",
                entry.path().display()
            )));
        }
    }
    Ok(())
}

async fn validate_generation(root: &Path, current: CurrentState) -> Result<(), DBError> {
    let generation_dir = generation_path(root, current.generation);
    require_entry_kind(
        &root.join(GENERATIONS_DIR),
        ExpectedEntryKind::Directory,
        "required generations directory is missing",
    )
    .await?;
    require_entry_kind(
        &generation_dir,
        ExpectedEntryKind::Directory,
        "required generation directory is missing",
    )
    .await?;
    for name in [WAL_DIR, INDEX_DIR] {
        require_entry_kind(
            &generation_dir.join(name),
            ExpectedEntryKind::Directory,
            "required generation directory is missing",
        )
        .await?;
    }
    for name in [GENERATION_SEAL_FILE, GENERATION_SEAL_TEMP_FILE] {
        optional_entry_kind(&generation_dir.join(name), ExpectedEntryKind::File).await?;
    }

    let index_dir = generation_dir.join(INDEX_DIR);
    require_entry_kind(
        &index_dir.join(ROOT_MARKER_FILE),
        ExpectedEntryKind::File,
        "required index marker is missing",
    )
    .await?;
    optional_entry_kind(
        &index_dir.join(ROOT_MARKER_TEMP_FILE),
        ExpectedEntryKind::File,
    )
    .await?;
    require_entry_kind(
        &index_dir.join(MANIFEST_FILE),
        ExpectedEntryKind::File,
        "required index MANIFEST is missing",
    )
    .await?;
    optional_entry_kind(&index_dir.join("sstable"), ExpectedEntryKind::Directory).await?;

    let marker = tokio::fs::read(index_dir.join(ROOT_MARKER_FILE)).await?;
    RootMarker::decode(&marker)?;
    let seal = read_generation_seal(&generation_dir).await?;
    let manifest = ManifestManager::read_existing_snapshot(&index_dir.join(MANIFEST_FILE)).await?;
    let sstable_dir = index_dir.join("sstable");
    for table in manifest.levels.values().flatten() {
        validate_sstable_path(&sstable_dir, table).await?;
        KV2::validate_manifest_sstable(table).await?;
    }
    let wal_dir = generation_dir.join(WAL_DIR);
    let (recovered_sequence, next_physical_lsn) = WALManager::preflight_recovery(&wal_dir).await?;
    validate_recovery_against_current(
        &wal_dir,
        current,
        seal.as_ref(),
        recovered_sequence,
        next_physical_lsn,
    )
    .await
}

async fn is_entry_kind(path: &Path, expected: ExpectedEntryKind) -> Result<bool, DBError> {
    match tokio::fs::symlink_metadata(path).await {
        Ok(metadata) => {
            let file_type = metadata.file_type();
            Ok(match expected {
                ExpectedEntryKind::File => file_type.is_file(),
                ExpectedEntryKind::Directory => file_type.is_dir(),
            })
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error.into()),
    }
}

async fn require_entry_kind(
    path: &Path,
    expected: ExpectedEntryKind,
    missing_message: &str,
) -> Result<(), DBError> {
    if is_entry_kind(path, expected).await? {
        Ok(())
    } else {
        Err(DBError::Corrupted(format!(
            "{missing_message}: {}",
            path.display()
        )))
    }
}

async fn optional_entry_kind(path: &Path, expected: ExpectedEntryKind) -> Result<(), DBError> {
    match tokio::fs::symlink_metadata(path).await {
        Ok(metadata) => {
            let file_type = metadata.file_type();
            let valid = match expected {
                ExpectedEntryKind::File => file_type.is_file(),
                ExpectedEntryKind::Directory => file_type.is_dir(),
            };
            if valid {
                Ok(())
            } else {
                Err(DBError::StorageError(format!(
                    "unsafe storage artifact type at {}",
                    path.display()
                )))
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

async fn validate_sstable_path(
    sstable_dir: &Path,
    table: &crate::storage::manifest_codec::SSTableMeta,
) -> Result<(), DBError> {
    let table_path = Path::new(&table.path);
    let recorded_sstable_dir = table_path
        .ancestors()
        .skip(1)
        .find(|ancestor| ancestor.file_name() == Some(OsStr::new("sstable")))
        .ok_or_else(|| {
            DBError::Corrupted(format!(
                "manifest SSTable path is outside the index: {}",
                table.path
            ))
        })?;
    if tokio::fs::canonicalize(recorded_sstable_dir).await?
        != tokio::fs::canonicalize(sstable_dir).await?
    {
        return Err(DBError::Corrupted(format!(
            "manifest SSTable path is outside the index: {}",
            table.path
        )));
    }
    let relative = table_path.strip_prefix(recorded_sstable_dir).map_err(|_| {
        DBError::Corrupted(format!(
            "manifest SSTable path is outside the index: {}",
            table.path
        ))
    })?;
    let mut components = relative.components().peekable();
    if components.peek().is_none()
        || !is_entry_kind(recorded_sstable_dir, ExpectedEntryKind::Directory).await?
    {
        return Err(DBError::Corrupted(format!(
            "invalid manifest SSTable path: {}",
            table.path
        )));
    }
    let mut path = recorded_sstable_dir.to_path_buf();
    while let Some(component) = components.next() {
        let Component::Normal(name) = component else {
            return Err(DBError::Corrupted(format!(
                "invalid manifest SSTable path: {}",
                table.path
            )));
        };
        path.push(name);
        let expected = if components.peek().is_some() {
            ExpectedEntryKind::Directory
        } else {
            ExpectedEntryKind::File
        };
        if !is_entry_kind(&path, expected).await? {
            return Err(DBError::Corrupted(format!(
                "invalid manifest SSTable path: {}",
                table.path
            )));
        }
    }
    Ok(())
}

async fn read_current(root: &Path) -> Result<CurrentState, DBError> {
    CurrentState::decode(&tokio::fs::read(root.join(CURRENT_FILE)).await?)
}

async fn write_current(root: &Path, current: CurrentState) -> Result<(), DBError> {
    write_current_temp(root, current).await?;
    commit_current_temp(root).await
}

async fn write_current_temp(root: &Path, current: CurrentState) -> Result<(), DBError> {
    write_temporary_bytes(&root.join(CURRENT_TEMP_FILE), &current.encode()).await
}

async fn commit_current_temp(root: &Path) -> Result<(), DBError> {
    tokio::fs::rename(root.join(CURRENT_TEMP_FILE), root.join(CURRENT_FILE)).await?;
    tokio::fs::File::open(root).await?.sync_all().await?;
    Ok(())
}

async fn write_temporary_bytes(temporary: &Path, bytes: &[u8]) -> Result<(), DBError> {
    let mut file = tokio::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(temporary)
        .await?;
    file.write_all(bytes).await?;
    file.sync_all().await?;
    Ok(())
}

async fn write_marker(path: &Path) -> Result<(), DBError> {
    match tokio::fs::read(path).await {
        Ok(bytes) => {
            RootMarker::decode(&bytes)?;
            return Ok(());
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    let temporary = path.with_extension("tmp");
    let mut file = tokio::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&temporary)
        .await?;
    file.write_all(&RootMarker::encode()).await?;
    file.sync_all().await?;
    drop(file);
    tokio::fs::rename(&temporary, path).await?;
    sync_parent(path).await?;
    Ok(())
}

async fn generation_directory_ids(root: &Path) -> Result<Vec<u64>, DBError> {
    let mut entries = match tokio::fs::read_dir(root.join(GENERATIONS_DIR)).await {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(error.into()),
    };
    let mut ids = Vec::new();
    while let Some(entry) = entries.next_entry().await? {
        let name = entry.file_name();
        let name = name.to_string_lossy();
        let id = parse_generation_name(&name).ok_or_else(|| {
            DBError::Corrupted(format!(
                "unexpected entry in the generations directory: {name}"
            ))
        })?;
        let file_type = entry.file_type().await?;
        if file_type.is_symlink() || !file_type.is_dir() {
            return Err(DBError::Corrupted(format!(
                "generation entry is not a directory: {}",
                entry.path().display()
            )));
        }
        ids.push(id);
    }
    ids.sort_unstable();
    Ok(ids)
}

fn parse_generation_name(name: &str) -> Option<u64> {
    if name.len() != GENERATION_NAME_DIGITS || !name.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    name.parse().ok().filter(|generation| *generation != 0)
}

async fn read_generation_reservation(root: &Path) -> Result<u64, DBError> {
    match tokio::fs::read(root.join(NEXT_GENERATION_FILE)).await {
        Ok(bytes) => Ok(GenerationReservation::decode(&bytes)?.next_generation),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(0),
        Err(error) => Err(error.into()),
    }
}

async fn reserve_next_generation(root: &Path) -> Result<u64, DBError> {
    let reserved = read_generation_reservation(root).await?;
    let next_after_existing = match generation_directory_ids(root).await?.last().copied() {
        Some(highest) => highest
            .checked_add(1)
            .ok_or_else(|| DBError::StorageError("generation id exhausted".into()))?,
        None => INITIAL_GENERATION,
    };
    let next_generation = next_after_existing.max(reserved).max(INITIAL_GENERATION);
    let following = next_generation
        .checked_add(1)
        .ok_or_else(|| DBError::StorageError("generation id exhausted".into()))?;
    let reservation = GenerationReservation {
        next_generation: following,
    };
    write_temporary_bytes(&root.join(NEXT_GENERATION_TEMP_FILE), &reservation.encode()).await?;
    tokio::fs::rename(
        root.join(NEXT_GENERATION_TEMP_FILE),
        root.join(NEXT_GENERATION_FILE),
    )
    .await?;
    tokio::fs::File::open(root).await?.sync_all().await?;
    Ok(next_generation)
}

async fn read_generation_seal(generation_dir: &Path) -> Result<Option<GenerationSeal>, DBError> {
    match tokio::fs::read(generation_dir.join(GENERATION_SEAL_FILE)).await {
        Ok(bytes) => GenerationSeal::decode(&bytes).map(Some),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.into()),
    }
}

async fn write_generation_seal(
    generation_dir: &Path,
    seal: &GenerationSeal,
) -> Result<(), DBError> {
    write_temporary_bytes(
        &generation_dir.join(GENERATION_SEAL_TEMP_FILE),
        &seal.encode(),
    )
    .await?;
    let path = generation_dir.join(GENERATION_SEAL_FILE);
    tokio::fs::rename(generation_dir.join(GENERATION_SEAL_TEMP_FILE), &path).await?;
    sync_parent(&path).await?;
    Ok(())
}

async fn wal_segment_census(wal_dir: &Path) -> Result<Vec<(u64, u64)>, DBError> {
    let mut entries = tokio::fs::read_dir(wal_dir).await?;
    let mut segments = Vec::new();
    while let Some(entry) = entries.next_entry().await? {
        let Some(segment_id) = entry
            .file_name()
            .to_str()
            .and_then(|name| name.strip_suffix(".log"))
            .and_then(|name| name.parse::<u64>().ok())
        else {
            continue;
        };
        let file_type = entry.file_type().await?;
        if file_type.is_symlink() || !file_type.is_file() {
            return Err(DBError::Corrupted(
                "WAL segment is not a regular file".into(),
            ));
        }
        segments.push((segment_id, entry.metadata().await?.len()));
    }
    segments.sort_by_key(|(segment_id, _)| *segment_id);
    Ok(segments)
}

async fn seal_wal_segments(wal_dir: &Path) -> Result<Vec<WalSegmentFingerprint>, DBError> {
    let mut segments = Vec::new();
    for (segment_id, length) in wal_segment_census(wal_dir).await? {
        segments.push(WalSegmentFingerprint {
            segment_id,
            length,
            checksum: wal_segment_prefix_checksum(wal_dir, segment_id, length).await?,
        });
    }
    Ok(segments)
}

async fn wal_segment_prefix_checksum(
    wal_dir: &Path,
    segment_id: u64,
    length: u64,
) -> Result<u32, DBError> {
    let path = wal_dir.join(format!("{segment_id:020}.log"));
    let file = match tokio::fs::File::open(&path).await {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Err(DBError::Corrupted(
                "generation seal WAL segment is missing".into(),
            ));
        }
        Err(error) => return Err(error.into()),
    };
    let mut bytes = Vec::new();
    file.take(length).read_to_end(&mut bytes).await?;
    if bytes.len() as u64 != length {
        return Err(DBError::Corrupted(
            "generation seal WAL segment is truncated".into(),
        ));
    }
    Ok(crc32fast::hash(&bytes))
}

async fn validate_sealed_wal_segments(
    wal_dir: &Path,
    sealed_segments: &[WalSegmentFingerprint],
    census: &[(u64, u64)],
) -> Result<(), DBError> {
    for segment in sealed_segments {
        let observed_length = census
            .iter()
            .find(|(segment_id, _)| *segment_id == segment.segment_id)
            .map(|(_, length)| *length);
        if observed_length.is_none_or(|length| length < segment.length) {
            return Err(DBError::Corrupted(
                "generation seal WAL segment is missing or truncated".into(),
            ));
        }
        if wal_segment_prefix_checksum(wal_dir, segment.segment_id, segment.length).await?
            != segment.checksum
        {
            return Err(DBError::Corrupted(
                "generation seal WAL segment checksum mismatch".into(),
            ));
        }
    }
    let highest_sealed = sealed_segments.last().map(|segment| segment.segment_id);
    if census.iter().any(|(segment_id, _)| {
        Some(*segment_id) < highest_sealed
            && sealed_segments
                .iter()
                .all(|segment| segment.segment_id != *segment_id)
    }) {
        return Err(DBError::Corrupted(
            "generation seal WAL segments were replaced".into(),
        ));
    }
    Ok(())
}

async fn validate_recovery_against_current(
    wal_dir: &Path,
    current: CurrentState,
    seal: Option<&GenerationSeal>,
    recovered_sequence: u64,
    next_physical_lsn: u64,
) -> Result<(), DBError> {
    let Some(seal) = seal else {
        if recovered_sequence < current.application_sequence_floor {
            return Err(DBError::Corrupted(
                "WAL recovery is behind the published application sequence floor".into(),
            ));
        }
        return Ok(());
    };
    if seal.generation != current.generation {
        return Err(DBError::Corrupted(
            "generation seal does not match CURRENT".into(),
        ));
    }
    let census = wal_segment_census(wal_dir).await?;
    validate_sealed_wal_segments(wal_dir, &seal.wal_segments, &census).await?;
    if next_physical_lsn < seal.next_physical_lsn {
        return Err(DBError::Corrupted(
            "WAL recovery is behind the sealed physical log end".into(),
        ));
    }
    if recovered_sequence >= current.application_sequence_floor {
        if seal.application_sequence_floor > current.application_sequence_floor {
            return Err(DBError::Corrupted(
                "generation seal is ahead of the published application sequence floor".into(),
            ));
        }
        return Ok(());
    }
    if seal.application_sequence_floor != current.application_sequence_floor
        || recovered_sequence != seal.max_live_application_sequence
    {
        return Err(DBError::Corrupted(
            "WAL recovery is behind the published application sequence floor".into(),
        ));
    }
    Ok(())
}

async fn cleanup_abandoned_generations(root: &Path, current: u64) -> Result<(), DBError> {
    for generation in generation_directory_ids(root).await? {
        if generation == current {
            continue;
        }
        let path = generation_path(root, generation);
        remove_generation_tree(&path).await?;
    }
    Ok(())
}

async fn reclaim_after_switch(root: &Path, mut old_inner: KV2, keep_generation: u64) -> u64 {
    #[cfg(test)]
    if clean_fault_armed(CleanFaultPoint::DuringCleanup) {
        return unreclaimed_generation_bytes(root, keep_generation).await;
    }
    let shutdown = old_inner.shutdown().await;
    drop(old_inner);
    if let Err(error) = shutdown {
        log::warn!("old generation shutdown failed; deferring cleanup: {error}");
        return unreclaimed_generation_bytes(root, keep_generation).await;
    }
    reclaim_generations(root, keep_generation).await
}

async fn reclaim_generations(root: &Path, keep_generation: u64) -> u64 {
    let generations = match generation_directory_ids(root).await {
        Ok(generations) => generations,
        Err(error) => {
            log::warn!("generation cleanup listing failed: {error}");
            return unreclaimed_generation_bytes(root, keep_generation).await;
        }
    };
    let mut deferred = 0u64;
    for generation in generations {
        if generation == keep_generation {
            continue;
        }
        let path = generation_path(root, generation);
        if let Err(error) = remove_generation_tree(&path).await {
            log::warn!("generation cleanup for {generation} failed: {error}");
            deferred += directory_bytes(&path).await.unwrap_or(0);
        }
    }
    deferred
}

async fn unreclaimed_generation_bytes(root: &Path, keep_generation: u64) -> u64 {
    let mut total = 0u64;
    let mut entries = match tokio::fs::read_dir(root.join(GENERATIONS_DIR)).await {
        Ok(entries) => entries,
        Err(_) => return 0,
    };
    while let Ok(Some(entry)) = entries.next_entry().await {
        let name = entry.file_name();
        if parse_generation_name(&name.to_string_lossy()) == Some(keep_generation) {
            continue;
        }
        if entry
            .file_type()
            .await
            .is_ok_and(|file_type| file_type.is_dir())
        {
            total += directory_bytes(&entry.path()).await.unwrap_or(0);
        }
    }
    total
}

async fn remove_generation_tree(directory: &Path) -> Result<u64, DBError> {
    let mut directories = Vec::new();
    let mut files = Vec::new();
    let mut pending = vec![directory.to_path_buf()];
    while let Some(current) = pending.pop() {
        directories.push(current.clone());
        let mut entries = tokio::fs::read_dir(&current).await?;
        while let Some(entry) = entries.next_entry().await? {
            let file_type = entry.file_type().await?;
            if file_type.is_symlink() {
                return Err(DBError::Corrupted(format!(
                    "refusing to follow a symbolic link during generation cleanup: {}",
                    entry.path().display()
                )));
            }
            if file_type.is_dir() {
                pending.push(entry.path());
            } else if file_type.is_file() {
                files.push(entry.path());
            } else {
                return Err(DBError::Corrupted(format!(
                    "unexpected generation entry during cleanup: {}",
                    entry.path().display()
                )));
            }
        }
    }
    let mut removed = 0u64;
    for file in files {
        removed += tokio::fs::metadata(&file).await?.len();
        tokio::fs::remove_file(&file).await?;
    }
    for directory in directories.into_iter().rev() {
        tokio::fs::remove_dir(&directory).await?;
    }
    if let Some(parent) = directory.parent() {
        tokio::fs::File::open(parent).await?.sync_all().await?;
    }
    Ok(removed)
}

async fn directory_bytes(directory: &Path) -> Result<u64, DBError> {
    let mut total = 0u64;
    let mut pending = vec![directory.to_path_buf()];
    while let Some(current) = pending.pop() {
        let mut entries = match tokio::fs::read_dir(&current).await {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error.into()),
        };
        while let Some(entry) = entries.next_entry().await? {
            let file_type = entry.file_type().await?;
            if file_type.is_dir() {
                pending.push(entry.path());
            } else if file_type.is_file() {
                total += entry.metadata().await?.len();
            }
        }
    }
    Ok(total)
}

struct CopiedLiveIndex {
    memtable: SkipMap<(Vec<u8>, u64), MemtableRecord>,
    memtable_size: usize,
    live_records: u64,
    live_logical_bytes: u64,
    copied_payload_bytes: u64,
    copied_transactions: u64,
    max_live_sequence: u64,
}

impl CopiedLiveIndex {
    fn new() -> Self {
        Self {
            memtable: SkipMap::new(),
            memtable_size: 0,
            live_records: 0,
            live_logical_bytes: 0,
            copied_payload_bytes: 0,
            copied_transactions: 0,
            max_live_sequence: 0,
        }
    }
}

async fn copy_live_records(
    lease: &GenerationState,
    new_wal: &WALManager,
    new_generation: u64,
    live: Vec<(Vec<u8>, MemtableRecord)>,
) -> Result<CopiedLiveIndex, DBError> {
    let mut copied = CopiedLiveIndex::new();
    let mut transaction = Vec::new();
    let mut transaction_bytes = COPY_TRANSACTION_OVERHEAD_BYTES;
    for (key, record) in live {
        if record.record_type != RecordType::Put {
            return Err(DBError::Corrupted(
                "live index snapshot contains a tombstone".into(),
            ));
        }
        let value = ExperimentalEngine::resolve_record(lease, &key, &record).await?;
        let frame_bytes = COPY_UPDATE_FRAME_OVERHEAD_BYTES
            .checked_add(key.len())
            .and_then(|bytes| bytes.checked_add(value.len()))
            .ok_or_else(|| DBError::StorageError("copied WAL frame size overflow".into()))?;
        if !transaction.is_empty()
            && (transaction.len() >= MAX_COPY_TRANSACTION_RECORDS
                || transaction_bytes + frame_bytes > MAX_COPY_TRANSACTION_BYTES)
        {
            append_copy_transaction(&mut copied, new_wal, new_generation, &mut transaction).await?;
            transaction_bytes = COPY_TRANSACTION_OVERHEAD_BYTES;
        }
        copied.live_records += 1;
        copied.live_logical_bytes += (key.len() + value.len()) as u64;
        copied.copied_payload_bytes += value.len() as u64;
        copied.max_live_sequence = copied.max_live_sequence.max(record.lsn);
        transaction_bytes += frame_bytes;
        transaction.push(LogCommand::new(RecordType::Put, key, value, record.lsn));
    }
    if !transaction.is_empty() {
        append_copy_transaction(&mut copied, new_wal, new_generation, &mut transaction).await?;
    }
    Ok(copied)
}

async fn append_copy_transaction(
    copied: &mut CopiedLiveIndex,
    new_wal: &WALManager,
    new_generation: u64,
    transaction: &mut Vec<LogCommand>,
) -> Result<(), DBError> {
    let commands = std::mem::take(transaction);
    let identities: Vec<(Vec<u8>, u64)> = commands
        .iter()
        .map(|command| (command.key.clone(), command.lsn))
        .collect();
    let mut receipts = new_wal
        .append_engine_transactions_with_receipts(vec![commands], None)
        .await?;
    let receipts = receipts
        .pop()
        .ok_or_else(|| DBError::StorageError("new WAL returned no value references".into()))?;
    if receipts.len() != identities.len() {
        return Err(DBError::StorageError(
            "new WAL references do not match the copied transaction".into(),
        ));
    }
    for ((key, application_sequence), receipt) in identities.into_iter().zip(receipts) {
        if receipt.application_sequence != application_sequence
            || receipt.record_type != RecordType::Put
        {
            return Err(DBError::StorageError(
                "new WAL reference does not match the copied record".into(),
            ));
        }
        let reference = ReferenceValue::new(new_generation, receipt)
            .encode()
            .to_vec();
        copied.memtable_size += key.len() + reference.len();
        copied.memtable.insert(
            (key, application_sequence),
            MemtableRecord::new(reference, RecordType::Put, application_sequence),
        );
    }
    copied.copied_transactions += 1;
    Ok(())
}

async fn validate_reference_index(
    inner: &KV2,
    wal: &WALManager,
    generation: u64,
) -> Result<(), DBError> {
    for (key, record) in inner.scan_records(..).await? {
        if record.record_type != RecordType::Put {
            return Err(DBError::Corrupted(
                "new reference index contains a tombstone".into(),
            ));
        }
        let reference = ReferenceValue::decode(&record.value)?;
        if reference.generation != generation {
            return Err(DBError::Corrupted(
                "new reference index points at another generation".into(),
            ));
        }
        if reference.reference.application_sequence != record.lsn {
            return Err(DBError::Corrupted(
                "new reference index sequence mismatch".into(),
            ));
        }
        wal.read_value(&reference.reference, &key, record.lsn)
            .await?;
    }
    Ok(())
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CleanFaultPoint {
    BeforeCurrentTempWrite,
    AfterNewGenerationDurable,
    BeforeCurrentRename,
    AfterCurrentDurableBeforeAdopt,
    DuringCleanup,
    PauseBeforeCurrentRename,
}

#[cfg(test)]
const CLEAN_FAULT_POINTS: [CleanFaultPoint; 6] = [
    CleanFaultPoint::BeforeCurrentTempWrite,
    CleanFaultPoint::AfterNewGenerationDurable,
    CleanFaultPoint::BeforeCurrentRename,
    CleanFaultPoint::AfterCurrentDurableBeforeAdopt,
    CleanFaultPoint::DuringCleanup,
    CleanFaultPoint::PauseBeforeCurrentRename,
];

#[cfg(test)]
static CLEAN_FAULT: std::sync::atomic::AtomicU8 = std::sync::atomic::AtomicU8::new(0);
#[cfg(test)]
static CLEAN_PAUSE: tokio::sync::Notify = tokio::sync::Notify::const_new();

#[cfg(test)]
fn armed_clean_fault() -> Option<CleanFaultPoint> {
    let armed = CLEAN_FAULT.load(std::sync::atomic::Ordering::SeqCst);
    CLEAN_FAULT_POINTS
        .into_iter()
        .find(|point| *point as u8 + 1 == armed)
}

#[cfg(test)]
fn clean_fault_armed(point: CleanFaultPoint) -> bool {
    armed_clean_fault() == Some(point)
}

#[cfg(test)]
fn set_clean_fault(point: Option<CleanFaultPoint>) {
    CLEAN_FAULT.store(
        point.map_or(0, |point| point as u8 + 1),
        std::sync::atomic::Ordering::SeqCst,
    );
}

#[cfg(test)]
async fn clean_fault_point(point: CleanFaultPoint) -> Result<(), DBError> {
    let Some(armed) = armed_clean_fault() else {
        return Ok(());
    };
    match point {
        CleanFaultPoint::BeforeCurrentRename
            if matches!(
                armed,
                CleanFaultPoint::BeforeCurrentRename | CleanFaultPoint::PauseBeforeCurrentRename
            ) =>
        {
            if armed == CleanFaultPoint::PauseBeforeCurrentRename {
                CLEAN_PAUSE.notified().await;
            }
            Err(DBError::StorageError(format!(
                "injected clean fault at {point:?}"
            )))
        }
        _ if armed == point => Err(DBError::StorageError(format!(
            "injected clean fault at {point:?}"
        ))),
        _ => Ok(()),
    }
}

fn build_reference_memtable(
    generation: u64,
    updates: Vec<RecoveredWalUpdate>,
) -> Result<(SkipMap<(Vec<u8>, u64), MemtableRecord>, usize, u64), DBError> {
    let mut newest = BTreeMap::new();
    let mut max_sequence = 0u64;
    for update in updates {
        let record_type = match update.record.record_type {
            1 => RecordType::Put,
            2 => RecordType::Delete,
            _ => {
                return Err(DBError::Corrupted(
                    "Invalid WAL record type during reference recovery".into(),
                ));
            }
        };
        let value = match record_type {
            RecordType::Put => ReferenceValue::new(generation, update.reference)
                .encode()
                .to_vec(),
            RecordType::Delete => Vec::new(),
        };
        max_sequence = max_sequence.max(update.record.lsn);
        let record = MemtableRecord::new(value, record_type, update.record.lsn);
        newest
            .entry(update.record.key)
            .and_modify(|existing: &mut (u64, MemtableRecord)| {
                if record.lsn > existing.0 {
                    *existing = (record.lsn, record.clone());
                }
            })
            .or_insert((record.lsn, record));
    }
    let mut total_size = 0usize;
    let memtable = SkipMap::new();
    for (key, (lsn, record)) in newest {
        total_size += key.len() + record.value.len();
        memtable.insert((key, lsn), record);
    }
    Ok((memtable, total_size, max_sequence))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::api::AsyncKVEngine;
    use crate::storage::{
        record::RecordType,
        recovermanager::wal::{RecoveredWalUpdate, WALRecord, WalValueRef},
    };

    fn reference() -> WalValueRef {
        WalValueRef {
            segment_id: 1,
            transaction_id: 2,
            transaction_start_offset: 36,
            commit_offset: 200,
            commit_lsn: 4,
            frame_offset: 36,
            frame_length: 20 + 42 + 1 + 3,
            physical_lsn: 2,
            application_sequence: 1,
            record_type: RecordType::Put,
            key_length: 1,
            key_checksum: crc32fast::hash(b"k"),
            value_length: 3,
            value_checksum: crc32fast::hash(b"val"),
        }
    }

    #[test]
    fn reference_value_roundtrips_and_rejects_corruption() {
        let value = ReferenceValue::new(7, reference());
        let encoded = value.encode();
        assert_eq!(encoded.len(), REFERENCE_VALUE_ENCODED_SIZE);
        assert_eq!(ReferenceValue::decode(&encoded).unwrap(), value);

        let mut wrong_version = encoded;
        wrong_version[0] = 2;
        assert!(ReferenceValue::decode(&wrong_version).is_err());

        let mut wrong_checksum = encoded;
        wrong_checksum[20] ^= 0xff;
        assert!(ReferenceValue::decode(&wrong_checksum).is_err());

        let mut wrong_generation = encoded;
        wrong_generation[1..9].copy_from_slice(&0u64.to_be_bytes());
        let checksum = crc32fast::hash(&wrong_generation[..REFERENCE_VALUE_ENCODED_SIZE - 4]);
        wrong_generation[REFERENCE_VALUE_ENCODED_SIZE - 4..]
            .copy_from_slice(&checksum.to_be_bytes());
        assert!(ReferenceValue::decode(&wrong_generation).is_err());

        let truncated = &encoded[..REFERENCE_VALUE_ENCODED_SIZE - 1];
        assert!(ReferenceValue::decode(truncated).is_err());
    }

    #[test]
    fn current_state_roundtrips_and_rejects_corruption() {
        let current = CurrentState {
            generation: 3,
            application_sequence_floor: 42,
        };
        let encoded = current.encode();
        assert_eq!(encoded.len(), CURRENT_ENCODED_SIZE);
        assert_eq!(CurrentState::decode(&encoded).unwrap(), current);

        let mut corrupt = encoded;
        corrupt[5] ^= 0x01;
        assert!(CurrentState::decode(&corrupt).is_err());

        let mut zero_generation = encoded;
        zero_generation[1..9].copy_from_slice(&0u64.to_be_bytes());
        let checksum = crc32fast::hash(&zero_generation[..CURRENT_ENCODED_SIZE - 4]);
        zero_generation[CURRENT_ENCODED_SIZE - 4..].copy_from_slice(&checksum.to_be_bytes());
        assert!(CurrentState::decode(&zero_generation).is_err());
    }

    #[test]
    fn root_marker_roundtrips_and_rejects_corruption() {
        let marker = RootMarker::encode();
        RootMarker::decode(&marker).unwrap();
        let mut corrupt = marker;
        corrupt[20] ^= 0x01;
        assert!(RootMarker::decode(&corrupt).is_err());
        assert!(RootMarker::decode(&marker[..marker.len() - 1]).is_err());
    }

    #[test]
    fn recovery_coalesces_newest_references_per_key() {
        let key = b"key".to_vec();
        let payload = b"value".to_vec();
        let updates = vec![
            RecoveredWalUpdate {
                record: WALRecord {
                    record_type: RecordType::Put as u8,
                    lsn: 1,
                    key: key.clone(),
                    value: payload.clone(),
                    checksum: crc32fast::hash(&payload),
                },
                reference: WalValueRef {
                    application_sequence: 1,
                    ..reference()
                },
            },
            RecoveredWalUpdate {
                record: WALRecord {
                    record_type: RecordType::Delete as u8,
                    lsn: 2,
                    key: key.clone(),
                    value: Vec::new(),
                    checksum: 0,
                },
                reference: WalValueRef {
                    record_type: RecordType::Delete,
                    application_sequence: 2,
                    value_length: 0,
                    value_checksum: 0,
                    ..reference()
                },
            },
        ];
        let (memtable, size, max_sequence) = build_reference_memtable(9, updates).unwrap();
        assert_eq!(max_sequence, 2);
        assert_eq!(memtable.len(), 1);
        assert!(memtable.get(&(key.clone(), 1)).is_none());
        let delete = memtable.get(&(key, 2)).unwrap();
        assert_eq!(delete.value().record_type, RecordType::Delete);
        assert!(delete.value().value.is_empty());
        assert_eq!(size, 3);
    }

    #[test]
    fn generation_directories_are_monotonic_twenty_digit_names() {
        let path = generation_path(std::path::Path::new("root"), 7);
        assert_eq!(
            path,
            std::path::Path::new("root")
                .join("generations")
                .join("00000000000000000007")
        );
    }

    #[test]
    fn generation_seal_roundtrips_and_rejects_corruption() {
        let seal = GenerationSeal {
            generation: 4,
            application_sequence_floor: 12,
            max_live_application_sequence: 7,
            next_physical_lsn: 21,
            wal_segments: vec![
                WalSegmentFingerprint {
                    segment_id: 0,
                    length: 36,
                    checksum: 0xabcd_ef01,
                },
                WalSegmentFingerprint {
                    segment_id: 1,
                    length: 512,
                    checksum: 0x1234_5678,
                },
            ],
        };
        let encoded = seal.encode();
        assert_eq!(GenerationSeal::decode(&encoded).unwrap(), seal);

        let mut corrupt = encoded.clone();
        corrupt[10] ^= 0x01;
        assert!(GenerationSeal::decode(&corrupt).is_err());

        let mut corrupt_checksum = encoded.clone();
        corrupt_checksum[GENERATION_SEAL_HEADER_SIZE + 16] ^= 0x01;
        assert!(GenerationSeal::decode(&corrupt_checksum).is_err());

        let mut unordered = seal.clone();
        unordered.wal_segments.swap(0, 1);
        assert!(GenerationSeal::decode(&unordered.encode()).is_err());

        assert!(GenerationSeal::decode(&encoded[..encoded.len() - 1]).is_err());
        assert!(GenerationSeal::decode(&[]).is_err());
    }

    #[test]
    fn generation_reservation_roundtrips_and_rejects_corruption() {
        let reservation = GenerationReservation { next_generation: 9 };
        let encoded = reservation.encode();
        assert_eq!(
            GenerationReservation::decode(&encoded).unwrap(),
            reservation
        );

        let mut corrupt = encoded;
        corrupt[2] ^= 0x01;
        assert!(GenerationReservation::decode(&corrupt).is_err());
        assert!(GenerationReservation::decode(&encoded[..9]).is_err());
    }

    static CLEAN_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    async fn cleanable_engine(directory: &std::path::Path) -> ExperimentalEngine {
        let mut engine = ExperimentalEngine::open(directory).await.unwrap();
        engine
            .put(b"live".to_vec(), b"value".to_vec())
            .await
            .unwrap();
        engine
            .put(b"doomed".to_vec(), b"gone".to_vec())
            .await
            .unwrap();
        engine.delete(b"doomed".to_vec()).await.unwrap();
        engine
    }

    async fn open_reopened(directory: &std::path::Path) -> ExperimentalEngine {
        ExperimentalEngine::open(directory).await.unwrap()
    }

    #[tokio::test]
    async fn clean_fault_before_switch_keeps_old_engine_usable() {
        let _guard = CLEAN_TEST_LOCK.lock().await;
        for point in [
            CleanFaultPoint::BeforeCurrentTempWrite,
            CleanFaultPoint::AfterNewGenerationDurable,
        ] {
            let directory = tempfile::TempDir::new().unwrap();
            let mut engine = cleanable_engine(directory.path()).await;
            set_clean_fault(Some(point));
            assert!(engine.clean().await.is_err());
            set_clean_fault(None);
            assert_eq!(
                engine.get(b"live").await.unwrap().unwrap().as_ref(),
                b"value"
            );
            engine.put(b"after".to_vec(), b"ok".to_vec()).await.unwrap();
            assert_eq!(
                read_current(directory.path()).await.unwrap().generation,
                INITIAL_GENERATION
            );
            assert_eq!(
                generation_directory_ids(directory.path()).await.unwrap(),
                vec![INITIAL_GENERATION, INITIAL_GENERATION + 1]
            );
            drop(engine);
            let mut reopened = open_reopened(directory.path()).await;
            assert_eq!(
                reopened.get(b"live").await.unwrap().unwrap().as_ref(),
                b"value"
            );
            assert_eq!(
                reopened.get(b"after").await.unwrap().unwrap().as_ref(),
                b"ok"
            );
            assert_eq!(
                generation_directory_ids(directory.path()).await.unwrap(),
                vec![INITIAL_GENERATION]
            );
            reopened.shutdown().await.unwrap();
        }
    }

    #[tokio::test]
    async fn clean_fault_after_poison_requires_reopen() {
        let _guard = CLEAN_TEST_LOCK.lock().await;
        let directory = tempfile::TempDir::new().unwrap();
        let mut engine = cleanable_engine(directory.path()).await;
        set_clean_fault(Some(CleanFaultPoint::BeforeCurrentRename));
        assert!(engine.clean().await.is_err());
        set_clean_fault(None);
        assert!(engine.get(b"live").await.is_err());
        assert!(engine.scan(..).await.is_err());
        assert!(
            engine
                .put(b"blocked".to_vec(), b"x".to_vec())
                .await
                .is_err()
        );
        assert!(engine.await_pending_writes().await.is_err());
        assert!(engine.clean().await.is_err());
        assert!(engine.shutdown().await.is_err());
        assert_eq!(
            read_current(directory.path()).await.unwrap().generation,
            INITIAL_GENERATION
        );
        drop(engine);
        let mut reopened = open_reopened(directory.path()).await;
        assert_eq!(
            reopened.get(b"live").await.unwrap().unwrap().as_ref(),
            b"value"
        );
        assert_eq!(
            generation_directory_ids(directory.path()).await.unwrap(),
            vec![INITIAL_GENERATION]
        );
        reopened.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn cancelled_clean_after_poison_requires_reopen() {
        let _guard = CLEAN_TEST_LOCK.lock().await;
        let directory = tempfile::TempDir::new().unwrap();
        let mut engine = cleanable_engine(directory.path()).await;
        set_clean_fault(Some(CleanFaultPoint::PauseBeforeCurrentRename));
        let mut cleaner = Box::pin(engine.clean());
        assert!(
            tokio::time::timeout(Duration::from_secs(5), &mut cleaner)
                .await
                .is_err()
        );
        drop(cleaner);
        set_clean_fault(None);
        assert!(engine.get(b"live").await.is_err());
        drop(engine);
        let mut reopened = open_reopened(directory.path()).await;
        assert_eq!(
            reopened.get(b"live").await.unwrap().unwrap().as_ref(),
            b"value"
        );
        assert_eq!(
            read_current(directory.path()).await.unwrap().generation,
            INITIAL_GENERATION
        );
        reopened.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn clean_fault_after_current_durable_reopens_new_generation() {
        let _guard = CLEAN_TEST_LOCK.lock().await;
        let directory = tempfile::TempDir::new().unwrap();
        let mut engine = cleanable_engine(directory.path()).await;
        set_clean_fault(Some(CleanFaultPoint::AfterCurrentDurableBeforeAdopt));
        assert!(engine.clean().await.is_err());
        set_clean_fault(None);
        assert!(engine.get(b"live").await.is_err());
        assert_eq!(
            read_current(directory.path()).await.unwrap().generation,
            INITIAL_GENERATION + 1
        );
        assert_eq!(
            generation_directory_ids(directory.path()).await.unwrap(),
            vec![INITIAL_GENERATION, INITIAL_GENERATION + 1]
        );
        drop(engine);
        let mut reopened = open_reopened(directory.path()).await;
        assert_eq!(
            reopened.get(b"live").await.unwrap().unwrap().as_ref(),
            b"value"
        );
        assert!(reopened.get(b"doomed").await.unwrap().is_none());
        assert_eq!(
            generation_directory_ids(directory.path()).await.unwrap(),
            vec![INITIAL_GENERATION + 1]
        );
        reopened.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn clean_cleanup_failure_reports_deferred_bytes() {
        let _guard = CLEAN_TEST_LOCK.lock().await;
        let directory = tempfile::TempDir::new().unwrap();
        let mut engine = cleanable_engine(directory.path()).await;
        set_clean_fault(Some(CleanFaultPoint::DuringCleanup));
        let stats = engine.clean().await.unwrap();
        set_clean_fault(None);
        assert_eq!(stats.new_generation, INITIAL_GENERATION + 1);
        assert!(stats.deferred_cleanup_bytes > 0);
        assert_eq!(
            generation_directory_ids(directory.path()).await.unwrap(),
            vec![INITIAL_GENERATION, INITIAL_GENERATION + 1]
        );
        assert_eq!(
            engine.get(b"live").await.unwrap().unwrap().as_ref(),
            b"value"
        );
        engine
            .put(b"after-clean".to_vec(), b"ok".to_vec())
            .await
            .unwrap();
        engine.shutdown().await.unwrap();
        drop(engine);
        let mut reopened = open_reopened(directory.path()).await;
        assert_eq!(
            reopened
                .get(b"after-clean")
                .await
                .unwrap()
                .unwrap()
                .as_ref(),
            b"ok"
        );
        assert_eq!(
            generation_directory_ids(directory.path()).await.unwrap(),
            vec![INITIAL_GENERATION + 1]
        );
        reopened.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn aborted_generation_ids_are_not_reused() {
        let _guard = CLEAN_TEST_LOCK.lock().await;
        let directory = tempfile::TempDir::new().unwrap();
        let mut engine = cleanable_engine(directory.path()).await;
        set_clean_fault(Some(CleanFaultPoint::BeforeCurrentTempWrite));
        assert!(engine.clean().await.is_err());
        set_clean_fault(None);
        assert_eq!(
            read_generation_reservation(directory.path()).await.unwrap(),
            INITIAL_GENERATION + 2
        );
        assert_eq!(
            generation_directory_ids(directory.path()).await.unwrap(),
            vec![INITIAL_GENERATION, INITIAL_GENERATION + 1]
        );
        drop(engine);
        let mut reopened = open_reopened(directory.path()).await;
        assert_eq!(
            generation_directory_ids(directory.path()).await.unwrap(),
            vec![INITIAL_GENERATION]
        );
        let stats = reopened.clean().await.unwrap();
        assert_eq!(stats.old_generation, INITIAL_GENERATION);
        assert_eq!(stats.new_generation, INITIAL_GENERATION + 2);
        reopened.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn seal_rejects_gap_with_missing_wal_segment() {
        let _guard = CLEAN_TEST_LOCK.lock().await;
        let directory = tempfile::TempDir::new().unwrap();
        let mut engine = cleanable_engine(directory.path()).await;
        let stats = engine.clean().await.unwrap();
        assert_eq!(stats.new_generation, INITIAL_GENERATION + 1);
        engine.shutdown().await.unwrap();
        drop(engine);
        let wal_dir = generation_path(directory.path(), INITIAL_GENERATION + 1).join(WAL_DIR);
        let segments = wal_segment_census(&wal_dir).await.unwrap();
        let (last_segment, _) = *segments.last().unwrap();
        tokio::fs::remove_file(wal_dir.join(format!("{last_segment:020}.log")))
            .await
            .unwrap();
        let error = match ExperimentalEngine::open(directory.path()).await {
            Ok(_) => panic!("a cleaned generation opened with a missing WAL segment"),
            Err(error) => error,
        };
        assert!(matches!(error, DBError::Corrupted(_)));
    }
}
