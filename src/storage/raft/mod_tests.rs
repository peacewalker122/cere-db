#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::config::StorageConfig;
    use crate::storage::manifest_codec::ManifestManager;
    use crate::storage::raft::{RaftConsensusLayer, RaftNodeConfig};
    use crate::storage::recovermanager::wal::WALManager;
    use crate::storage::writemanager::write::WriteComponent;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::RwLock;

    fn free_addr() -> std::net::SocketAddr {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.local_addr().unwrap()
    }

    async fn setup_write_component(dir: &tempfile::TempDir) -> Arc<RwLock<WriteComponent>> {
        let wal_dir = dir.path().join("wal");
        tokio::fs::create_dir_all(&wal_dir).await.unwrap();
        let dummy_wal = Arc::new(WALManager::new(wal_dir, 1024 * 1024).await.unwrap());
        let wc = WriteComponent::new(
            dir.path().join("sstable"),
            dummy_wal,
            Arc::new(
                ManifestManager::load_or_create(dir.path().join("MANIFEST"))
                    .await
                    .unwrap(),
            ),
            0,
            Arc::new(StorageConfig::default()),
        );
        Arc::new(RwLock::new(wc))
    }

    fn single_node_config(node_id: u64, dir: &tempfile::TempDir) -> RaftNodeConfig {
        RaftNodeConfig {
            node_id,
            peers: vec![],
            http_bind: free_addr(),
            raft_dir: dir.path().join("raft"),
        }
    }

    #[tokio::test]
    async fn start_and_shutdown_single_node() {
        let dir = tempfile::tempdir().unwrap();
        let wc = setup_write_component(&dir).await;
        let config = single_node_config(1, &dir);
        let layer = RaftConsensusLayer::start(config, wc).await.unwrap();
        assert!(!layer.is_leader().await);
        layer.initialize(&[1]).await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(layer.is_leader().await);
        layer.shutdown().await;
    }

    #[tokio::test]
    async fn single_node_propose_and_commit() {
        let dir = tempfile::tempdir().unwrap();
        let wc = setup_write_component(&dir).await;
        let config = single_node_config(1, &dir);
        let layer = RaftConsensusLayer::start(config, wc).await.unwrap();
        layer.initialize(&[1]).await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(layer.is_leader().await);
        layer.propose(b"hello raft".to_vec()).await.unwrap();
        layer.shutdown().await;
    }

    #[tokio::test]
    async fn reject_proposals_to_follower() {
        let dir1 = tempfile::tempdir().unwrap();
        let dir2 = tempfile::tempdir().unwrap();
        let wc1 = setup_write_component(&dir1).await;
        let wc2 = setup_write_component(&dir2).await;
        let addr1 = free_addr();
        let addr2 = free_addr();
        let config1 = RaftNodeConfig {
            node_id: 1,
            peers: vec![(2, addr2.to_string())],
            http_bind: addr1,
            raft_dir: dir1.path().join("raft"),
        };
        let config2 = RaftNodeConfig {
            node_id: 2,
            peers: vec![(1, addr1.to_string())],
            http_bind: addr2,
            raft_dir: dir2.path().join("raft"),
        };
        let _layer1 = RaftConsensusLayer::start(config1, wc1).await.unwrap();
        let layer2 = RaftConsensusLayer::start(config2, wc2).await.unwrap();
        assert!(!layer2.is_leader().await);
        let result = layer2.propose(b"test".to_vec()).await;
        assert!(result.is_err(), "follower should reject proposals");
        layer2.shutdown().await;
    }

    // ═══════════════════════════════════════════════════════════════
    // 3-node cluster tests.  Each test creates its own cluster with
    // independent temp dirs and ports, so they don't interfere.
    // ═══════════════════════════════════════════════════════════════

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn three_node_elects_leader() {
        let d1 = tempfile::tempdir().unwrap();
        let d2 = tempfile::tempdir().unwrap();
        let d3 = tempfile::tempdir().unwrap();

        let w1 = setup_write_component(&d1).await;
        let w2 = setup_write_component(&d2).await;
        let w3 = setup_write_component(&d3).await;

        let a1 = free_addr();
        let a2 = free_addr();
        let a3 = free_addr();

        let l1 = RaftConsensusLayer::start(
            RaftNodeConfig {
                node_id: 1,
                peers: vec![(2, a2.to_string()), (3, a3.to_string())],
                http_bind: a1,
                raft_dir: d1.path().join("raft"),
            },
            w1,
        )
        .await
        .unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await;

        let l2 = RaftConsensusLayer::start(
            RaftNodeConfig {
                node_id: 2,
                peers: vec![(1, a1.to_string()), (3, a3.to_string())],
                http_bind: a2,
                raft_dir: d2.path().join("raft"),
            },
            w2,
        )
        .await
        .unwrap();

        let l3 = RaftConsensusLayer::start(
            RaftNodeConfig {
                node_id: 3,
                peers: vec![(1, a1.to_string()), (2, a2.to_string())],
                http_bind: a3,
                raft_dir: d3.path().join("raft"),
            },
            w3,
        )
        .await
        .unwrap();
        tokio::task::yield_now().await;

        l1.initialize(&[1, 2, 3]).await.unwrap();

        let layers = [l1, l2, l3];
        let leader_id = loop {
            let mut found = None;
            for l in &layers {
                if l.is_leader().await {
                    found = Some(l.id());
                    break;
                }
            }
            if let Some(id) = found {
                break id;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        };

        assert!((1..=3).contains(&leader_id));

        // Non-leaders reject proposals
        for l in &layers {
            if l.id() != leader_id {
                assert!(l.propose(b"reject-me".to_vec()).await.is_err());
            }
        }

        // Leader accepts proposals
        let leader = layers.iter().find(|l| l.id() == leader_id).unwrap();
        leader.propose(b"from-leader".to_vec()).await.unwrap();

        for l in &layers {
            l.shutdown().await;
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn three_node_propose_from_leader() {
        let d1 = tempfile::tempdir().unwrap();
        let d2 = tempfile::tempdir().unwrap();
        let d3 = tempfile::tempdir().unwrap();

        let w1 = setup_write_component(&d1).await;
        let w2 = setup_write_component(&d2).await;
        let w3 = setup_write_component(&d3).await;

        let a1 = free_addr();
        let a2 = free_addr();
        let a3 = free_addr();

        let l1 = RaftConsensusLayer::start(
            RaftNodeConfig {
                node_id: 1,
                peers: vec![(2, a2.to_string()), (3, a3.to_string())],
                http_bind: a1,
                raft_dir: d1.path().join("raft"),
            },
            w1,
        )
        .await
        .unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await;

        let l2 = RaftConsensusLayer::start(
            RaftNodeConfig {
                node_id: 2,
                peers: vec![(1, a1.to_string()), (3, a3.to_string())],
                http_bind: a2,
                raft_dir: d2.path().join("raft"),
            },
            w2,
        )
        .await
        .unwrap();

        let l3 = RaftConsensusLayer::start(
            RaftNodeConfig {
                node_id: 3,
                peers: vec![(1, a1.to_string()), (2, a2.to_string())],
                http_bind: a3,
                raft_dir: d3.path().join("raft"),
            },
            w3,
        )
        .await
        .unwrap();
        tokio::task::yield_now().await;

        l1.initialize(&[1, 2, 3]).await.unwrap();

        let layers = [l1, l2, l3];
        let leader_id = loop {
            let mut found = None;
            for l in &layers {
                if l.is_leader().await {
                    found = Some(l.id());
                    break;
                }
            }
            if let Some(id) = found {
                break id;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        };

        let leader = layers.iter().find(|l| l.id() == leader_id).unwrap();
        for i in 0..5 {
            let data = format!("entry-{i}").into_bytes();
            leader.propose(data).await.unwrap();
        }

        // All nodes have applied entries
        for l in &layers {
            let rx = l.raft.metrics();
            let m = rx.borrow().clone();
            assert!(m.last_applied.is_some(), "node {} has no applied entry", l.id());
        }

        for l in &layers {
            l.shutdown().await;
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn leader_failover() {
        let d1 = tempfile::tempdir().unwrap();
        let d2 = tempfile::tempdir().unwrap();
        let d3 = tempfile::tempdir().unwrap();

        let w1 = setup_write_component(&d1).await;
        let w2 = setup_write_component(&d2).await;
        let w3 = setup_write_component(&d3).await;

        let a1 = free_addr();
        let a2 = free_addr();
        let a3 = free_addr();

        let l1 = RaftConsensusLayer::start(
            RaftNodeConfig {
                node_id: 1,
                peers: vec![(2, a2.to_string()), (3, a3.to_string())],
                http_bind: a1,
                raft_dir: d1.path().join("raft"),
            },
            w1,
        )
        .await
        .unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await;

        let l2 = RaftConsensusLayer::start(
            RaftNodeConfig {
                node_id: 2,
                peers: vec![(1, a1.to_string()), (3, a3.to_string())],
                http_bind: a2,
                raft_dir: d2.path().join("raft"),
            },
            w2,
        )
        .await
        .unwrap();

        let l3 = RaftConsensusLayer::start(
            RaftNodeConfig {
                node_id: 3,
                peers: vec![(1, a1.to_string()), (2, a2.to_string())],
                http_bind: a3,
                raft_dir: d3.path().join("raft"),
            },
            w3,
        )
        .await
        .unwrap();
        tokio::task::yield_now().await;

        l1.initialize(&[1, 2, 3]).await.unwrap();

        let layers = [l1, l2, l3];
        let leader_id = loop {
            let mut found = None;
            for l in &layers {
                if l.is_leader().await {
                    found = Some(l.id());
                    break;
                }
            }
            if let Some(id) = found {
                break id;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        };

        let leader = layers.iter().find(|l| l.id() == leader_id).unwrap();
        leader.propose(b"before-crash".to_vec()).await.unwrap();

        // Crash the leader
        leader.shutdown().await;

        // Wait for new leader election among survivors
        let remaining: Vec<&RaftConsensusLayer> =
            layers.iter().filter(|l| l.id() != leader_id).collect();

        let new_leader_id = loop {
            let mut found = None;
            for l in &remaining {
                if l.is_leader().await {
                    found = Some(l.id());
                    break;
                }
            }
            if let Some(id) = found {
                break id;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        };

        assert_ne!(new_leader_id, leader_id,
            "new leader {} must differ from crashed leader {}", new_leader_id, leader_id);

        let new_leader = remaining.iter().find(|l| l.id() == new_leader_id).unwrap();
        new_leader.propose(b"after-crash".to_vec()).await.unwrap();

        for l in remaining {
            l.shutdown().await;
        }
    }
}
