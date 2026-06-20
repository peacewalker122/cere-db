//! `RaftStateMachine` implementation for ceredb.
//!
//! Applies committed log entries (serialised `LogCommand`s) to the KV store's
//! `WriteComponent` (memtable pipeline).  Snapshot support is stubbed for now.

use std::io::Cursor;

use std::sync::Arc;

use openraft::storage::{RaftSnapshotBuilder, RaftStateMachine};
use openraft::{EntryPayload, ErrorSubject, ErrorVerb, LogId, OptionalSend, RaftTypeConfig, Snapshot, SnapshotMeta, StorageError, StoredMembership, BasicNode};

use tokio::sync::{Mutex, RwLock};

use crate::storage::raft::types::TypeConfig;
use crate::storage::writemanager::write::WriteComponent;

/// Shared mutable state for the Raft state machine.
struct SMInner {
    /// The last-applied log id (index, term).
    last_applied: Option<LogId<u64>>,
    /// Current cluster membership.
    membership: StoredMembership<u64, <TypeConfig as RaftTypeConfig>::Node>,
}

/// The Raft state machine.
///
/// Wraps a `WriteComponent` handle and applies committed `LogCommand`s to it.
/// Cloning is cheap — the `WriteComponent` is behind an `Arc`.
#[derive(Clone)]
pub struct KVStateMachine {
    /// Shared handle to the KV write pipeline.
    pub write_component: Arc<RwLock<WriteComponent>>,
    inner: Arc<Mutex<SMInner>>,
}

impl KVStateMachine {
    /// Create a new state machine wrapping the given `WriteComponent`.
    pub fn new(write_component: Arc<RwLock<WriteComponent>>) -> Self {
        Self {
            write_component,
            inner: Arc::new(Mutex::new(SMInner {
                last_applied: None,
                membership: StoredMembership::default(),
            })),
        }
    }

    /// Create a new state machine with a pre-configured last-applied log ID
    /// (for use during recovery).
    pub fn with_last_applied(
        write_component: Arc<RwLock<WriteComponent>>,
        last_applied: Option<LogId<u64>>,
        membership: StoredMembership<u64, BasicNode>,
    ) -> Self {
        Self {
            write_component,
            inner: Arc::new(Mutex::new(SMInner {
                last_applied,
                membership,
            })),
        }
    }
}

impl RaftStateMachine<TypeConfig> for KVStateMachine {
    type SnapshotBuilder = NoopSnapshotBuilder;

    async fn applied_state(
        &mut self,
    ) -> Result<
        (
            Option<LogId<u64>>,
            StoredMembership<u64, <TypeConfig as RaftTypeConfig>::Node>,
        ),
        StorageError<u64>,
    > {
        let inner = self.inner.lock().await;
        Ok((inner.last_applied, inner.membership.clone()))
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<<TypeConfig as RaftTypeConfig>::R>, StorageError<u64>>
    where
        I: IntoIterator<Item = <TypeConfig as RaftTypeConfig>::Entry> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let mut results = Vec::new();
        let mut sm = self.inner.lock().await;

        for entry in entries {
            // Update last_applied
            sm.last_applied = Some(entry.log_id);

            match &entry.payload {
                EntryPayload::Normal(data) => {
                    // Apply the command to the write component
                    let wc = self.write_component.write().await;
                    // TODO: actually apply the command bytes to WriteComponent
                    // For now, we just acknowledge
                    let _ = data;
                    drop(wc);
                    results.push(Vec::new());
                }
                EntryPayload::Membership(membership) => {
                    // Store the membership config
                    sm.membership = StoredMembership::new(Some(entry.log_id), membership.clone());
                    results.push(Vec::new());
                }
                EntryPayload::Blank => {
                    results.push(Vec::new());
                }
            }
        }

        Ok(results)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        NoopSnapshotBuilder
    }

    async fn begin_receiving_snapshot(&mut self) -> Result<Box<<TypeConfig as RaftTypeConfig>::SnapshotData>, StorageError<u64>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        _meta: &SnapshotMeta<u64, <TypeConfig as RaftTypeConfig>::Node>,
        _snapshot: Box<<TypeConfig as RaftTypeConfig>::SnapshotData>,
    ) -> Result<(), StorageError<u64>> {
        // Stub: no snapshot support yet
        Ok(())
    }

    async fn get_current_snapshot(&mut self) -> Result<Option<Snapshot<TypeConfig>>, StorageError<u64>> {
        Ok(None)
    }
}

/// No-op snapshot builder (snapshots are out of scope for this phase).
pub struct NoopSnapshotBuilder;

impl RaftSnapshotBuilder<TypeConfig> for NoopSnapshotBuilder {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<u64>> {
        Err(StorageError::from_io_error(
            ErrorSubject::StateMachine,
            ErrorVerb::Read,
            std::io::Error::new(std::io::ErrorKind::Unsupported, "snapshots not implemented"),
        ))
    }
}
