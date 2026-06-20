//! HTTP-based `RaftNetwork` and `RaftNetworkFactory` implementation.
//!
//! Uses `reqwest` for client-side RPCs and communicates with the per-node
//! HTTP server (axum-based) on the peer nodes.

use std::sync::Arc;

use openraft::error::{InstallSnapshotError, RPCError, RaftError};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest,
    InstallSnapshotResponse, VoteRequest, VoteResponse,
};
use openraft::OptionalSend;
use tokio::sync::RwLock;

use crate::storage::raft::types::TypeConfig;

/// A factory that creates HTTP clients for each Raft peer.
#[derive(Clone)]
pub struct RaftHttpNetwork {
    /// Map of node ID → base URL (e.g. `http://127.0.0.1:21001`).
    peer_urls: Arc<RwLock<std::collections::HashMap<u64, String>>>,
}

impl RaftHttpNetwork {
    pub fn new(peers: Vec<(u64, String)>) -> Self {
        let mut m = std::collections::HashMap::new();
        for (id, url) in peers {
            m.insert(id, url);
        }
        Self {
            peer_urls: Arc::new(RwLock::new(m)),
        }
    }

    /// Look up the URL for a peer node.
    async fn url_for(&self, target: u64) -> Option<String> {
        self.peer_urls.read().await.get(&target).cloned()
    }

    /// Update or add a peer URL (used during cluster membership changes).
    pub async fn add_peer(&self, id: u64, url: String) {
        self.peer_urls.write().await.insert(id, url);
    }
}

/// A network client for sending RPCs to a specific target node.
pub struct RaftHttpClient {
    target_url: String,
}

impl RaftNetworkFactory<TypeConfig> for RaftHttpNetwork {
    type Network = RaftHttpClient;

    async fn new_client(
        &mut self,
        target: u64,
        _node: &openraft::impls::BasicNode,
    ) -> Self::Network {
        let addr = self
            .url_for(target)
            .await
            .unwrap_or_else(|| format!("node-{target}:21001"));
        let url = format!("http://{addr}");
        RaftHttpClient { target_url: url }
    }
}

impl RaftNetwork<TypeConfig> for RaftHttpClient {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<u64>, RPCError<u64, openraft::impls::BasicNode, RaftError<u64>>>
    {
        let resp = reqwest::Client::new()
            .post(format!("{}/raft/append_entries", self.target_url))
            .json(&rpc)
            .send()
            .await
            .map_err(|e| RPCError::Network(openraft::error::NetworkError::new(&e)))?;

        let resp: AppendEntriesResponse<u64> = resp
            .json()
            .await
            .map_err(|e| RPCError::Network(openraft::error::NetworkError::new(&e)))?;

        Ok(resp)
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<u64>,
        RPCError<u64, openraft::impls::BasicNode, RaftError<u64, InstallSnapshotError>>,
    > {
        let resp = reqwest::Client::new()
            .post(format!("{}/raft/install_snapshot", self.target_url))
            .json(&rpc)
            .send()
            .await
            .map_err(|e| RPCError::Network(openraft::error::NetworkError::new(&e)))?;

        let resp: InstallSnapshotResponse<u64> = resp
            .json()
            .await
            .map_err(|e| RPCError::Network(openraft::error::NetworkError::new(&e)))?;

        Ok(resp)
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<u64>,
        _option: RPCOption,
    ) -> Result<VoteResponse<u64>, RPCError<u64, openraft::impls::BasicNode, RaftError<u64>>>
    {
        let resp = reqwest::Client::new()
            .post(format!("{}/raft/vote", self.target_url))
            .json(&rpc)
            .send()
            .await
            .map_err(|e| RPCError::Network(openraft::error::NetworkError::new(&e)))?;

        let resp: VoteResponse<u64> = resp
            .json()
            .await
            .map_err(|e| RPCError::Network(openraft::error::NetworkError::new(&e)))?;

        Ok(resp)
    }
}
