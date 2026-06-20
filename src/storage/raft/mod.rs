//! openraft-backed consensus layer for ceredb.
//!
//! This module wraps `openraft::Raft` and provides a clean interface for
//! the KV engine.

pub mod types;
pub mod log_store;
pub mod state_machine;
pub mod network;
pub mod server;

use std::collections::BTreeMap;
use std::sync::Arc;

use openraft::impls::BasicNode;
use openraft::Raft;

use tokio::sync::RwLock;

use crate::error::DBError;
use crate::storage::raft::log_store::LogStore;
use crate::storage::raft::network::RaftHttpNetwork;
use crate::storage::raft::state_machine::KVStateMachine;
use crate::storage::raft::types::TypeConfig;
use crate::storage::writemanager::write::WriteComponent;

/// Configuration for this node in the Raft cluster.
#[derive(Debug, Clone)]
pub struct RaftNodeConfig {
    /// Unique numeric node identifier.
    pub node_id: u64,
    /// Peer addresses in the form `"host:port"`.
    pub peers: Vec<(u64, String)>,
    /// Local bind address for the Raft HTTP server (e.g. `127.0.0.1:21001`).
    pub http_bind: std::net::SocketAddr,
    /// The directory for persisting Raft log entries and vote.
    pub raft_dir: std::path::PathBuf,
}

/// The consensus layer wraps an `openraft::Raft` instance and supporting
/// components (log storage, HTTP server, network factory).
#[derive(Clone)]
pub struct RaftConsensusLayer {
    pub raft: Arc<Raft<TypeConfig>>,
    pub log_store: LogStore,
    pub network: RaftHttpNetwork,
    node_id: u64,
    /// Shared handle to the write component for reads.
    pub write_component: Arc<RwLock<WriteComponent>>,
}

impl RaftConsensusLayer {
    /// Start the consensus layer.
    ///
    /// Initialises the log store, state machine, and Raft node, spawns the
    /// HTTP server, and returns a handle.
    pub async fn start(
        config: RaftNodeConfig,
        write_component: Arc<RwLock<WriteComponent>>,
    ) -> Result<Self, DBError> {
        // ── Log store ─────────────────────────────────────────────────────
        let log_store = LogStore::open(&config.raft_dir).await?;

        // ── State machine (shares the same WriteComponent handle) ─────────
        let state_machine = KVStateMachine::new(Arc::clone(&write_component));

        // ── Network ───────────────────────────────────────────────────────
        let network = RaftHttpNetwork::new(config.peers.clone());

        // ── Raft config ───────────────────────────────────────────────────
        let raft_config = openraft::Config {
            cluster_name: "wasm-kv".to_string(),
            ..Default::default()
        };
        let raft_config = Arc::new(
            raft_config
                .validate()
                .map_err(|e| DBError::StorageError(format!("invalid raft config: {e}")))?,
        );

        // ── Open node ─────────────────────────────────────────────────────
        let raft = Raft::new(
            config.node_id,
            raft_config,
            network.clone(),
            log_store.clone(),
            state_machine,
        )
        .await
        .map_err(|e| DBError::StorageError(format!("raft init: {e}")))?;
        let raft = Arc::new(raft);

        // ── HTTP server ───────────────────────────────────────────────────
        server::start_raft_http_server(Arc::clone(&raft), config.http_bind).await?;

        log::info!(
            "RaftConsensusLayer started: node={}, bind={}, peers={:?}",
            config.node_id,
            config.http_bind,
            config.peers,
        );

        Ok(Self {
            raft,
            log_store,
            network,
            node_id: config.node_id,
            write_component,
        })
    }

    /// Propose a command (serialized `LogCommand` bytes) to the Raft cluster.
    ///
    /// Only the leader accepts proposals. Returns `Ok` when the entry is
    /// committed to the log (replicated to a quorum).
    pub async fn propose(&self, data: Vec<u8>) -> Result<(), DBError> {
        self.raft
            .client_write(data)
            .await
            .map_err(|e| DBError::StorageError(format!("raft client_write: {e}")))?;
        Ok(())
    }

    /// Check whether this node is the cluster leader.
    pub async fn is_leader(&self) -> bool {
        self.raft.current_leader().await == Some(self.node_id)
    }

    /// Return the known leader node ID, if any.
    pub async fn current_leader(&self) -> Option<u64> {
        self.raft.current_leader().await
    }

    /// Return the node ID of this Raft node.
    pub fn id(&self) -> u64 {
        self.node_id
    }

    /// Return a reference to the underlying openraft `Raft` instance.
    pub fn raft_ref(&self) -> &Arc<Raft<TypeConfig>> {
        &self.raft
    }

    /// Initialize the cluster as a single-node cluster (first node).
    pub async fn initialize(&self) -> Result<(), DBError> {
        self.raft
            .initialize(BTreeMap::from([(self.node_id, BasicNode::default())]))
            .await
            .map_err(|e| DBError::StorageError(format!("raft initialize: {e}")))?;
        log::info!("Raft cluster initialized with node {}", self.node_id);
        Ok(())
    }

    /// Gracefully shut down the Raft node.
    pub async fn shutdown(&self) {
        if let Err(e) = self.raft.shutdown().await {
            log::error!("Raft shutdown error: {e}");
        }
        log::info!("RaftConsensusLayer shutdown (node {})", self.node_id);
    }

    /// Get a reference to the write component (for reads).
    pub fn write_component(&self) -> &Arc<RwLock<WriteComponent>> {
        &self.write_component
    }
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
