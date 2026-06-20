//! HTTP server for receiving Raft RPCs from peers.
//!
//! Exposes endpoints:
//! - `POST /raft/append_entries`
//! - `POST /raft/vote`
//! - `POST /raft/install_snapshot`
//! - `GET  /raft/metrics` – current node state (for monitoring dashboards)

use std::sync::Arc;

use axum::extract::State;
use axum::routing::get;
use axum::routing::post;
use axum::{Json, Router};
use openraft::raft::{AppendEntriesRequest, InstallSnapshotRequest, VoteRequest, VoteResponse};
use openraft::Raft;

use crate::storage::raft::types::TypeConfig;

/// Shared application state for the Raft HTTP server.
pub struct RaftServerState {
    pub raft: Arc<Raft<TypeConfig>>,
}

/// Start the Raft HTTP server on the given bind address.
///
/// Spawns a background task.  Returns when the server is bound.
pub async fn start_raft_http_server(
    raft: Arc<Raft<TypeConfig>>,
    bind_addr: std::net::SocketAddr,
) -> Result<(), crate::error::DBError> {
    let state = Arc::new(RaftServerState { raft });

    let app = Router::new()
        .route("/raft/append_entries", post(handle_append_entries))
        .route("/raft/vote", post(handle_vote))
        .route("/raft/install_snapshot", post(handle_install_snapshot))
        .route("/raft/metrics", get(handle_metrics))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(bind_addr).await?;

    log::info!("Raft HTTP server listening on {bind_addr}");

    tokio::spawn(async move {
        if let Err(e) = axum::serve(listener, app).await {
            log::error!("Raft HTTP server error: {e}");
        }
    });

    Ok(())
}

/// Return the current Raft metrics as JSON.
///
/// Used by the monitoring dashboard to display node state, leader info,
/// replication progress, etc.
async fn handle_metrics(
    State(state): State<Arc<RaftServerState>>,
) -> Json<serde_json::Value> {
    let rx = state.raft.metrics();
    let metrics = rx.borrow().clone();
    Json(serde_json::to_value(&metrics).unwrap_or_default())
}

/// Handle `AppendEntries` RPC from a peer leader.
async fn handle_append_entries(
    State(state): State<Arc<RaftServerState>>,
    Json(req): Json<AppendEntriesRequest<TypeConfig>>,
) -> Json<openraft::raft::AppendEntriesResponse<u64>> {
    let resp = state.raft.append_entries(req).await.unwrap_or_else(|e| {
        log::error!("append_entries handler error: {e}");
        // Return a generic failure response
        openraft::raft::AppendEntriesResponse::HigherVote(Default::default())
    });
    Json(resp)
}

/// Handle `Vote` RPC for leader election.
async fn handle_vote(
    State(state): State<Arc<RaftServerState>>,
    Json(req): Json<VoteRequest<u64>>,
) -> Json<VoteResponse<u64>> {
    let resp = state.raft.vote(req).await.unwrap_or_else(|e| {
        log::error!("vote handler error: {e}");
        VoteResponse {
            vote: Default::default(),
            vote_granted: false,
            last_log_id: None,
        }
    });
    Json(resp)
}

/// Handle `InstallSnapshot` RPC.
async fn handle_install_snapshot(
    State(state): State<Arc<RaftServerState>>,
    Json(req): Json<InstallSnapshotRequest<TypeConfig>>,
) -> Json<openraft::raft::InstallSnapshotResponse<u64>> {
    let resp = state
        .raft
        .install_snapshot(req)
        .await
        .unwrap_or_else(|e| {
            log::error!("install_snapshot handler error: {e}");
            openraft::raft::InstallSnapshotResponse {
                vote: Default::default(),
            }
        });
    Json(resp)
}
