#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::config::StorageConfig;
    use crate::storage::manifest_codec::ManifestManager;
    use crate::storage::raft::{RaftConsensusLayer, RaftNodeConfig};
    use crate::storage::recovermanager::wal::WALManager;
    use crate::storage::writemanager::write::WriteComponent;
    use std::sync::Arc;
    use tokio::sync::RwLock;

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

    #[tokio::test]
    async fn start_and_shutdown_single_node() {
        let dir = tempfile::tempdir().unwrap();
        let raft_dir = dir.path().join("raft");
        let wc = setup_write_component(&dir).await;

        let config = RaftNodeConfig {
            node_id: 1,
            peers: vec![],
            http_bind: "127.0.0.1:0".parse().unwrap(),
            raft_dir,
        };

        let layer = RaftConsensusLayer::start(config, wc).await.unwrap();

        // Single-node with no peers — not leader until initialized
        assert!(!layer.is_leader().await);

        // Initialize the cluster
        layer.initialize().await.unwrap();
        // Wait for election
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        assert!(layer.is_leader().await);

        layer.shutdown().await;
    }

    #[tokio::test]
    async fn single_node_propose_and_commit() {
        let dir = tempfile::tempdir().unwrap();
        let raft_dir = dir.path().join("raft");
        let wc = setup_write_component(&dir).await;

        let config = RaftNodeConfig {
            node_id: 1,
            peers: vec![],
            http_bind: "127.0.0.1:0".parse().unwrap(),
            raft_dir,
        };

        let layer = RaftConsensusLayer::start(config, wc).await.unwrap();

        layer.initialize().await.unwrap();
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        assert!(layer.is_leader().await);

        // Propose a command
        layer.propose(b"hello raft".to_vec()).await.unwrap();

        layer.shutdown().await;
    }

    #[tokio::test]
    async fn reject_proposals_to_follower() {
        let dir1 = tempfile::tempdir().unwrap();
        let dir2 = tempfile::tempdir().unwrap();

        let wc1 = setup_write_component(&dir1).await;
        let wc2 = setup_write_component(&dir2).await;

        let raft_dir1 = dir1.path().join("raft");
        let raft_dir2 = dir2.path().join("raft");

        let config1 = RaftNodeConfig {
            node_id: 1,
            peers: vec![(2, "127.0.0.1:21012".to_string())],
            http_bind: "127.0.0.1:21011".parse().unwrap(),
            raft_dir: raft_dir1,
        };

        let config2 = RaftNodeConfig {
            node_id: 2,
            peers: vec![(1, "127.0.0.1:21011".to_string())],
            http_bind: "127.0.0.1:21012".parse().unwrap(),
            raft_dir: raft_dir2,
        };

        let _layer1 = RaftConsensusLayer::start(config1, wc1).await.unwrap();
        let layer2 = RaftConsensusLayer::start(config2, wc2).await.unwrap();

        // Node 2 should not be leader
        assert!(!layer2.is_leader().await);

        // Proposing to a follower should fail
        let result = layer2.propose(b"test".to_vec()).await;
        assert!(result.is_err(), "follower should reject proposals");

        layer2.shutdown().await;
    }
}
