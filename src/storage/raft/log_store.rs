//! Disk-backed `RaftLogStorage` implementation.
//!
//! Persists log entries and votes to `RAFT_DIR/entries.dat` and `RAFT_DIR/vote`.
//! Uses atomic rename (write-to-temp, then rename) for crash safety.

use std::ops::RangeBounds;
use std::path::{Path, PathBuf};

use openraft::storage::{LogFlushed, RaftLogStorage};
use openraft::{
    ErrorSubject, ErrorVerb, Entry, LogId, LogState, OptionalSend, RaftLogReader,
    RaftTypeConfig, StorageError, StorageIOError, Vote,
};

use crate::error::DBError;
use crate::storage::raft::types::TypeConfig;

use std::fmt::Debug;

/// Shared inner state behind an `Arc`.
#[derive(Clone)]
struct Inner {
    entries: std::sync::Arc<tokio::sync::Mutex<Vec<Entry<TypeConfig>>>>,
    vote: std::sync::Arc<tokio::sync::Mutex<Option<Vote<u64>>>>,
}

/// Disk-backed log storage.
///
/// Cloning is cheap — all state is behind `Arc`. This lets `get_log_reader()`
/// return a `LogReader` handle that shares the underlying data.
#[derive(Clone)]
pub struct LogStore {
    base_dir: PathBuf,
    inner: Inner,
}

/// Read-only handle to the shared log store.
pub struct LogReader {
    inner: Inner,
}

impl LogStore {
    /// Open or initialise the log store at `raft_dir`.
    pub async fn open(raft_dir: &Path) -> Result<Self, DBError> {
        tokio::fs::create_dir_all(raft_dir).await?;

        let entries_path = raft_dir.join("entries.dat");
        let vote_path = raft_dir.join("vote");

        // Load existing entries
        let entries = if entries_path.exists() {
            let buf = tokio::fs::read(&entries_path).await?;
            if buf.is_empty() {
                Vec::new()
            } else {
                bincode::deserialize(&buf)
                    .map_err(|e| DBError::StorageError(format!("log deserialize: {e}")))?
            }
        } else {
            Vec::new()
        };

        // Load existing vote
        let vote = if vote_path.exists() {
            let buf = tokio::fs::read(&vote_path).await?;
            if buf.is_empty() {
                None
            } else {
                Some(
                    bincode::deserialize(&buf)
                        .map_err(|e| DBError::StorageError(format!("vote deserialize: {e}")))?,
                )
            }
        } else {
            None
        };

        log::info!(
            "LogStore open at {} ({} entries, vote={:?})",
            raft_dir.display(),
            entries.len(),
            vote,
        );

        Ok(Self {
            base_dir: raft_dir.to_path_buf(),
            inner: Inner {
                entries: std::sync::Arc::new(tokio::sync::Mutex::new(entries)),
                vote: std::sync::Arc::new(tokio::sync::Mutex::new(vote)),
            },
        })
    }

    fn entries_path(&self) -> PathBuf {
        self.base_dir.join("entries.dat")
    }

    fn vote_path(&self) -> PathBuf {
        self.base_dir.join("vote")
    }

    async fn flush_entries(&self) -> Result<(), std::io::Error> {
        let data = {
            let entries = self.inner.entries.lock().await;
            bincode::serialize(&*entries)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?
        };
        atomic_write(&self.entries_path(), &data).await
    }
}

// ── Helper ───────────────────────────────────────────────────────────────────

fn io_error<E: Into<std::io::Error>>(
    subject: ErrorSubject<u64>,
    verb: ErrorVerb,
    err: E,
) -> StorageError<u64> {
    StorageError::IO {
        source: StorageIOError::new(subject, verb, openraft::anyerror::AnyError::new(&err.into())),
    }
}

fn io_error_msg(subject: ErrorSubject<u64>, verb: ErrorVerb, msg: impl ToString) -> StorageError<u64> {
    StorageError::IO {
        source: StorageIOError::new(
            subject,
            verb,
            openraft::anyerror::AnyError::new(&std::io::Error::new(std::io::ErrorKind::Other, msg.to_string())),
        ),
    }
}

// ── RaftLogReader (for LogStore itself, as required by the super-trait) ──

impl RaftLogReader<TypeConfig> for LogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + std::fmt::Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<TypeConfig>>, StorageError<u64>> {
        use std::ops::Bound;
        let entries = self.inner.entries.lock().await;
        let start = match range.start_bound() {
            Bound::Included(i) => *i as usize,
            Bound::Excluded(i) => *i as usize + 1,
            Bound::Unbounded => 0,
        };
        let end = match range.end_bound() {
            Bound::Included(i) => *i as usize + 1,
            Bound::Excluded(i) => *i as usize,
            Bound::Unbounded => entries.len(),
        };

        if start >= entries.len() || start >= end {
            return Ok(Vec::new());
        }
        let end = end.min(entries.len());
        Ok(entries[start..end].to_vec())
    }
}

// ── RaftLogReader for the dedicated reader handle ──

impl RaftLogReader<TypeConfig> for LogReader {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + std::fmt::Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<TypeConfig>>, StorageError<u64>> {
        use std::ops::Bound;
        let entries = self.inner.entries.lock().await;
        let start = match range.start_bound() {
            Bound::Included(i) => *i as usize,
            Bound::Excluded(i) => *i as usize + 1,
            Bound::Unbounded => 0,
        };
        let end = match range.end_bound() {
            Bound::Included(i) => *i as usize + 1,
            Bound::Excluded(i) => *i as usize,
            Bound::Unbounded => entries.len(),
        };

        if start >= entries.len() || start >= end {
            return Ok(Vec::new());
        }
        let end = end.min(entries.len());
        Ok(entries[start..end].to_vec())
    }
}

// ── RaftLogStorage ────────────────────────────────────────────────────────────

impl RaftLogStorage<TypeConfig> for LogStore {
    type LogReader = LogReader;

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, StorageError<u64>> {
        let entries = self.inner.entries.lock().await;
        let last = entries.last().cloned();
        Ok(LogState {
            last_purged_log_id: None,
            last_log_id: last.map(|e| e.log_id),
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        LogReader {
            inner: self.inner.clone(),
        }
    }

    async fn save_vote(&mut self, vote: &Vote<u64>) -> Result<(), StorageError<u64>> {
        {
            let mut v = self.inner.vote.lock().await;
            *v = Some(*vote);
        }
        let data = bincode::serialize(vote)
            .map_err(|e| io_error_msg(ErrorSubject::Vote, ErrorVerb::Write, e))?;
        atomic_write(&self.vote_path(), &data)
            .await
            .map_err(|e| io_error(ErrorSubject::Vote, ErrorVerb::Write, e))?;
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<u64>>, StorageError<u64>> {
        let vote = self.inner.vote.lock().await;
        Ok(*vote)
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<TypeConfig>,
    ) -> Result<(), StorageError<u64>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        {
            let mut e = self.inner.entries.lock().await;
            for entry in entries {
                e.push(entry);
            }
        }
        self.flush_entries()
            .await
            .map_err(|e| io_error(ErrorSubject::Logs, ErrorVerb::Write, e))?;
        callback.log_io_completed(Ok(()));
        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        let keep = (log_id.index + 1) as usize;
        {
            let mut entries = self.inner.entries.lock().await;
            if keep < entries.len() {
                entries.truncate(keep);
            }
        }
        self.flush_entries()
            .await
            .map_err(|e| io_error(ErrorSubject::LogIndex(log_id.index), ErrorVerb::Delete, e))?;
        Ok(())
    }

    async fn purge(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        let end = (log_id.index + 1) as usize;
        {
            let mut entries = self.inner.entries.lock().await;
            if end >= entries.len() {
                entries.clear();
            } else {
                entries.drain(..end);
            }
        }
        self.flush_entries()
            .await
            .map_err(|e| io_error(ErrorSubject::LogIndex(log_id.index), ErrorVerb::Delete, e))?;
        Ok(())
    }
}

async fn atomic_write(path: &Path, data: &[u8]) -> std::io::Result<()> {
    let tmp = path.with_extension("tmp");
    tokio::fs::write(&tmp, data).await?;
    tokio::fs::rename(&tmp, path).await?;
    // Sync the parent directory to ensure rename is durable
    if let Some(parent) = path.parent() {
        let dir_fd = tokio::fs::File::open(parent).await?;
        dir_fd.sync_all().await?;
    }
    Ok(())
}
