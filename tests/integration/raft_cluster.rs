//! Raft cluster integration tests.
//!
//! Two tiers:
//!
//! **Deterministic simulation tests** (no gRPC, no real timers)
//! Use `rust_raft::sim::Simulation` to verify Raft protocol correctness:
//! elections, crash recovery, client request replication, safety invariants.
//!
//! **Real RaftKV2 smoke tests** (with gRPC + scheduler)
//! Verify leader election, follower write rejection, and data survival across
//! leader failover.

use std::time::Duration;

fn init_logger() {
    let _ = env_logger::builder().is_test(true).try_init();
}

use ceredb::{
    api::api::AsyncKVEngine,
    storage::{
        config::StorageConfig,
        kv2::RaftKV2,
        raft::RaftNodeConfig,
    },
};

// ═══════════════════════════════════════════════════════════════════════════════
// Deterministic Raft Simulation Tests
// ═══════════════════════════════════════════════════════════════════════════════

fn create_sim() -> rust_raft::sim::Simulation {
    rust_raft::sim::Simulation::new(rust_raft::sim::SimulationConfig {
        node_count: 3,
        seed: 42,
        max_ticks: 10_000,
        ..Default::default()
    })
}

#[test]
fn sim_elects_leader() {
    let mut sim = create_sim();
    assert!(sim.run_until(|state| state.has_leader()));
    assert!(sim.state().leader().is_some());
}

#[test]
fn sim_safety_invariants_hold() {
    let mut sim = create_sim();
    sim.run_until(|state| state.has_stable_leader());
    let report = sim.state().verify_all(&sim.leader_elections);
    assert!(report.is_safe(), "{}", report.summary());
}

#[test]
fn sim_crash_and_recover() {
    let mut sim = create_sim();
    assert!(sim.run_until(|state| state.has_stable_leader()));
    let original_leader = sim.state().leader().unwrap();
    sim.crash_node(&original_leader);
    assert!(
        sim.run_until(|state| {
            state.has_leader() && state.leader().as_deref() != Some(&original_leader)
        }),
        "Should elect new leader after crash"
    );
}

#[test]
fn sim_safety_holds_after_crash() {
    let mut sim = create_sim();
    assert!(sim.run_until(|state| state.has_stable_leader()));
    let leader = sim.state().leader().unwrap();
    sim.crash_node(&leader);
    sim.run_until(|state| {
        state.has_leader() && state.leader().as_deref() != Some(&leader)
    });
    let report = sim.state().verify_all(&sim.leader_elections);
    assert!(report.is_safe(), "{}", report.summary());
}

#[test]
fn sim_client_request_reaches_leader_log() {
    let mut sim = create_sim();
    assert!(sim.run_until(|state| state.has_stable_leader()));
    assert!(sim.client_request(b"set x=42"));

    // Run so the entry propagates
    sim.run_until(|_| false);

    let leader_id = sim.state().leader().expect("should have leader");
    let leader_idx = (0..3)
        .find(|&i| sim.state().node_state(i).map(|s| s.id == leader_id).unwrap_or(false))
        .expect("leader index");
    let state = sim.state().node_state(leader_idx).expect("leader state");
    assert_eq!(state.log_len, 1, "Leader should have 1 log entry");

    let report = sim.state().verify_all(&sim.leader_elections);
    assert!(report.is_safe(), "{}", report.summary());
}

// ═══════════════════════════════════════════════════════════════════════════════
// Real RaftKV2 Tests (with gRPC + scheduler)
// ═══════════════════════════════════════════════════════════════════════════════

async fn start_3_node_cluster(
) -> ([tempfile::TempDir; 3], [RaftKV2; 3]) {
    let port_base: u16 = 35200;

    let mut dirs: [Option<tempfile::TempDir>; 3] = Default::default();
    for d in &mut dirs {
        *d = Some(tempfile::tempdir().unwrap());
    }

    let storage_config = StorageConfig::default();

    let raft_configs = [
        RaftNodeConfig {
            node_id: "node-1".to_string(),
            peers: vec![
                format!("127.0.0.1:{}", port_base + 1),
                format!("127.0.0.1:{}", port_base + 2),
            ],
            grpc_bind: format!("127.0.0.1:{}", port_base).parse().unwrap(),
        },
        RaftNodeConfig {
            node_id: "node-2".to_string(),
            peers: vec![
                format!("127.0.0.1:{}", port_base),
                format!("127.0.0.1:{}", port_base + 2),
            ],
            grpc_bind: format!("127.0.0.1:{}", port_base + 1).parse().unwrap(),
        },
        RaftNodeConfig {
            node_id: "node-3".to_string(),
            peers: vec![
                format!("127.0.0.1:{}", port_base),
                format!("127.0.0.1:{}", port_base + 1),
            ],
            grpc_bind: format!("127.0.0.1:{}", port_base + 2).parse().unwrap(),
        },
    ];

    let nodes: [RaftKV2; 3] = {
        let mut futs = Vec::new();
        for i in 0..3 {
            let dir = dirs[i].as_ref().unwrap().path().to_path_buf();
            let raft_cfg = raft_configs[i].clone();
            let storage_cfg = storage_config.clone();
            futs.push(async move {
                RaftKV2::open(&dir, storage_cfg, raft_cfg)
                    .await
                    .expect("RaftKV2::open should succeed")
            });
        }
        let (n0, n1, n2) = tokio::join!(futs.remove(0), futs.remove(0), futs.remove(0));
        [n0, n1, n2]
    };

    let dirs_arr: [tempfile::TempDir; 3] = [
        dirs[0].take().unwrap(),
        dirs[1].take().unwrap(),
        dirs[2].take().unwrap(),
    ];

    (dirs_arr, nodes)
}

async fn wait_for_leader(
    nodes: &[RaftKV2; 3],
    timeout: Duration,
) -> Option<usize> {
    let start = std::time::Instant::now();
    while start.elapsed() < timeout {
        for (i, node) in nodes.iter().enumerate() {
            if node.raft_layer().is_leader().await {
                return Some(i);
            }
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    None
}

async fn wait_for_leader_excluding(
    nodes: &[RaftKV2; 3],
    exclude: &[usize],
    timeout: Duration,
) -> Option<usize> {
    let start = std::time::Instant::now();
    while start.elapsed() < timeout {
        for (i, node) in nodes.iter().enumerate() {
            if exclude.contains(&i) {
                continue;
            }
            if node.raft_layer().is_leader().await {
                return Some(i);
            }
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    None
}

async fn shutdown_all(mut nodes: [RaftKV2; 3]) {
    for i in 0..3 {
        nodes[i].raft_layer().shutdown().await;
    }
}

#[tokio::test]
async fn three_node_cluster_elects_leader() {
    init_logger();
    let (_dirs, nodes) = start_3_node_cluster().await;
    let leader_idx = wait_for_leader(&nodes, Duration::from_secs(25)).await;
    assert!(
        leader_idx.is_some(),
        "cluster should elect a leader within 25 seconds"
    );
    shutdown_all(nodes).await;
}

#[tokio::test]
async fn write_survives_leader_failover() {
    init_logger();
    let (_dirs, mut nodes) = start_3_node_cluster().await;

    let leader_idx = wait_for_leader(&nodes, Duration::from_secs(25)).await
        .expect("cluster should elect a leader");

    // Write a key to the leader
    nodes[leader_idx].put(
        b"failover-key".to_vec(),
        b"failover-value".to_vec(),
    ).await.expect("leader should accept write");

    // Wait for replication to peers (log repair via heartbeats)
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Shut down the leader
    nodes[leader_idx].raft_layer().shutdown().await;

    // Wait for a new leader among remaining nodes
    let new_leader_idx = wait_for_leader_excluding(
        &nodes, &[leader_idx], Duration::from_secs(25),
    ).await.expect("cluster should re-elect a leader after leader shutdown");

    // The data should be on the new leader (replicated via Raft log)
    let val = nodes[new_leader_idx].get(b"failover-key").await.unwrap();
    assert!(val.is_some(), "write should survive leader failover");
    assert_eq!(
        val.as_ref().unwrap().as_ref(),
        b"failover-value",
        "write should survive leader failover: value mismatch"
    );

    // Shut down remaining nodes
    for i in 0..3 {
        if i != leader_idx {
            nodes[i].raft_layer().shutdown().await;
        }
    }
}

#[tokio::test]
async fn delete_survives_leader_failover() {
    init_logger();
    let (_dirs, mut nodes) = start_3_node_cluster().await;

    let leader_idx = wait_for_leader(&nodes, Duration::from_secs(25)).await
        .expect("cluster should elect a leader");

    // Write then delete a key
    nodes[leader_idx].put(b"del-key".to_vec(), b"del-value".to_vec()).await.unwrap();
    tokio::time::sleep(Duration::from_secs(1)).await;
    nodes[leader_idx].delete(b"del-key".to_vec()).await.unwrap();

    // Wait for replication to peers
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Shut down the leader
    nodes[leader_idx].raft_layer().shutdown().await;

    // Wait for new leader among remaining nodes
    let new_leader_idx = wait_for_leader_excluding(
        &nodes, &[leader_idx], Duration::from_secs(25),
    ).await.expect("cluster should re-elect a leader after leader shutdown");

    // The key should be gone on the new leader (delete was replicated)
    let val = nodes[new_leader_idx].get(b"del-key").await.unwrap();
    assert!(
        val.is_none(),
        "delete should survive leader failover: key still present"
    );

    // Shut down remaining nodes
    for i in 0..3 {
        if i != leader_idx {
            nodes[i].raft_layer().shutdown().await;
        }
    }
}

#[tokio::test]
async fn restart_recovery() {
    init_logger();
    let (_dirs, mut nodes) = start_3_node_cluster().await;

    let leader_idx = wait_for_leader(&nodes, Duration::from_secs(25)).await
        .expect("cluster should elect a leader");

    // Write a key
    nodes[leader_idx].put(
        b"restart-key".to_vec(),
        b"restart-value".to_vec(),
    ).await.expect("leader should accept write");
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Shut down all nodes
    for i in 0..3 {
        nodes[i].raft_layer().shutdown().await;
    }

    // Save paths for reopening
    let dir_paths: Vec<_> = _dirs.iter().map(|d| d.path().to_path_buf()).collect();

    // Reopen all nodes (same directories, same config)
    let raft_configs = [
        RaftNodeConfig {
            node_id: "node-1".to_string(),
            peers: vec![
                "127.0.0.1:35201".to_string(),
                "127.0.0.1:35202".to_string(),
            ],
            grpc_bind: "127.0.0.1:35200".parse().unwrap(),
        },
        RaftNodeConfig {
            node_id: "node-2".to_string(),
            peers: vec![
                "127.0.0.1:35200".to_string(),
                "127.0.0.1:35202".to_string(),
            ],
            grpc_bind: "127.0.0.1:35201".parse().unwrap(),
        },
        RaftNodeConfig {
            node_id: "node-3".to_string(),
            peers: vec![
                "127.0.0.1:35200".to_string(),
                "127.0.0.1:35201".to_string(),
            ],
            grpc_bind: "127.0.0.1:35202".parse().unwrap(),
        },
    ];

    let storage_config = StorageConfig::default();
    let mut restarted: [RaftKV2; 3] = {
        let mut futs = Vec::new();
        for i in 0..3 {
            let dir = dir_paths[i].clone();
            let rc = raft_configs[i].clone();
            let sc = storage_config.clone();
            futs.push(async move {
                RaftKV2::open(&dir, sc, rc).await.expect("reopen should succeed")
            });
        }
        let (n0, n1, n2) = tokio::join!(futs.remove(0), futs.remove(0), futs.remove(0));
        [n0, n1, n2]
    };

    // Wait for leader election after restart
    let leader_idx = wait_for_leader(&restarted, Duration::from_secs(25)).await
        .expect("cluster should elect leader after restart");

    // Data should be recovered from Raft log
    let val = restarted[leader_idx].get(b"restart-key").await.unwrap();
    assert!(
        val.is_some(),
        "data should survive full cluster restart"
    );
    assert_eq!(
        val.as_ref().unwrap().as_ref(),
        b"restart-value",
        "data should survive full cluster restart: value mismatch"
    );

    for i in 0..3 {
        restarted[i].raft_layer().shutdown().await;
    }
}

#[tokio::test]
async fn follower_rejects_writes() {
    init_logger();
    let (_dirs, mut nodes) = start_3_node_cluster().await;

    let leader_idx = wait_for_leader(&nodes, Duration::from_secs(25)).await
        .expect("cluster should elect a leader");

    let follower_idx = (leader_idx + 1) % 3;
    let result = nodes[follower_idx].put(
        b"follower-write".to_vec(),
        b"fail".to_vec(),
    ).await;
    assert!(result.is_err(), "follower should reject writes");
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("not the leader") || err.contains("redirect"),
        "error should mention leader redirect, got: {err}"
    );

    let read_result = nodes[follower_idx].get(b"follower-write").await;
    assert!(read_result.is_err(), "follower should reject reads");

    shutdown_all(nodes).await;
}
