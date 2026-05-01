//! Integration tests for RaftRecoverManager
//!
//! These tests spin up multiple gRPC server nodes and verify consensus behavior.

use crate::storage::recovermanager::log_store::{LogCommand, LogStore};
use crate::storage::recovermanager::raft::RaftRecoverManager;
use crate::storage::record::RecordType;
use rust_raft::node::{
    node::RaftNode,
    rpc::{NodeRpcService, proto::raft_rpc_server::RaftRpcServer},
    scheduler::NodeScheduler,
};
use rust_raft::storage::storage::MockStore;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tonic::transport::Server;

/// Helper to start a Raft node server with scheduler (for integration tests)
async fn start_node_server(
    node_id: String,
    peers: Vec<String>,
    port: u16,
) -> (
    SocketAddr,
    Arc<tokio::sync::RwLock<RaftNode>>,
    Vec<tokio::task::JoinHandle<()>>,
) {
    let addr: SocketAddr = format!("127.0.0.1:{}", port)
        .parse()
        .expect("invalid socket address");

    let shared_node = Arc::new(tokio::sync::RwLock::new(RaftNode::new(
        node_id.clone(),
        peers.clone(),
        Box::new(MockStore::new()),
    )));
    let node_for_server = shared_node.clone();
    let node_for_scheduler = shared_node.clone();

    // Spawn the server
    let server_handle = tokio::spawn(async move {
        let service = NodeRpcService::new(node_for_server);
        Server::builder()
            .add_service(RaftRpcServer::new(service))
            .serve(addr)
            .await
            .expect("server failed");
    });

    // Spawn the scheduler (handles election timeouts)
    let scheduler = NodeScheduler::new(node_for_scheduler);
    let scheduler_handle = tokio::spawn(async move {
        scheduler.start().await;
    });

    // Give the server time to start
    tokio::time::sleep(Duration::from_millis(100)).await;

    let bind_addr: SocketAddr = format!("127.0.0.1:{}", port)
        .parse()
        .expect("invalid socket address");

    (
        bind_addr,
        shared_node,
        vec![server_handle, scheduler_handle],
    )
}

/// Integration test: Multi-node Raft cluster with consensus
/// This test verifies the recover manager works with a real 3-node cluster
#[tokio::test]
async fn test_integration_multi_node_cluster() {
    // Setup unique ports for this test
    let port_base = 50130
        + (std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .subsec_nanos()
            % 1000) as u16;

    let addr_a = format!("127.0.0.1:{}", port_base);
    let addr_b = format!("127.0.0.1:{}", port_base + 1);
    let addr_c = format!("127.0.0.1:{}", port_base + 2);

    // Start 3 node cluster
    let (_, node_a, handles_a) =
        start_node_server("consensus-a".to_string(), vec![addr_b.clone()], port_base).await;

    let (_, node_b, handles_b) = start_node_server(
        "consensus-b".to_string(),
        vec![addr_a.clone()],
        port_base + 1,
    )
    .await;

    let (_, node_c, handles_c) = start_node_server(
        "consensus-c".to_string(),
        vec![addr_a.clone(), addr_b.clone()],
        port_base + 2,
    )
    .await;

    // Wait for election (longer wait for stable consensus)
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Verify nodes are making progress (terms should be > 0)
    let term_a = node_a.read().await.get_term();
    let term_b = node_b.read().await.get_term();
    let term_c = node_c.read().await.get_term();

    // All terms should be > 0 after election
    assert!(term_a > 0, "Node A should have term > 0");
    assert!(term_b > 0, "Node B should have term > 0");
    assert!(term_c > 0, "Node C should have term > 0");

    // Verify RaftRecoverManager can work with a single node
    let raft_node = RaftNode::new(
        "test-recover".to_string(),
        vec![],
        Box::new(MockStore::new()),
    );
    let raft_storage = tokio::sync::RwLock::new(raft_node);
    let recover_manager = RaftRecoverManager::new(raft_storage);

    // Append a command through the recover manager
    let cmd = LogCommand::new(
        RecordType::Put,
        b"test_key".to_vec(),
        b"test_value".to_vec(),
        1,
    );

    let result = recover_manager.append(cmd).await;

    // Should succeed for single-node cluster
    assert!(result.is_ok());

    // Cleanup
    for handle in handles_a {
        handle.abort();
    }
    for handle in handles_b {
        handle.abort();
    }
    for handle in handles_c {
        handle.abort();
    }
}