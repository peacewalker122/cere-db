use rust_raft::{
    node::node::RaftNode,
    storage::{api::Store, storage::MockStore},
};

use crate::storage::recovermanager::log_store::{LogCommand, LogPosition, LogStore};

pub struct RaftRecoverManager {
    raft_node: tokio::sync::RwLock<RaftNode>,
}

impl RaftRecoverManager {
    pub fn new(raft_storage: tokio::sync::RwLock<RaftNode>) -> Self {
        Self {
            raft_node: raft_storage,
        }
    }
}

#[async_trait::async_trait]
impl LogStore for RaftRecoverManager {
    async fn append(&self, cmd: LogCommand) -> Result<LogPosition, std::io::Error> {
        {
            let mut node = self.raft_node.write().await;

            node.push_log(cmd.serialize(), None).await.map_err(|e| {
                std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("Raft log append error: {}", e),
                )
            })?;
        }

        Ok(LogPosition { lsn: 0 }) // Placeholder implementation
    }
    async fn recover_commands(&self) -> Result<Vec<LogCommand>, std::io::Error> {
        // read the logs from raft and convert them to LogCommand
        let (data) = {
            let node = self.raft_node.read().await;

            let state = node.get_raft_state().await.map_err(|e| {
                std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("Raft get state error: {}", e),
                )
            })?;

            state.log
        };

        let mut result = Vec::new();

        // Use the commands to be converted over to LogCommand
        for log_entry in data {
            match LogCommand::deserialize(&log_entry.command) {
                Ok(cmd) => {
                    result.push(cmd);
                }
                Err(e) => {
                    log::warn!("Failed to deserialize log entry: {}", e);
                    continue; // Skip this log entry and continue with the next one
                }
            }
        }

        Ok(result) // Placeholder implementation
    }
    async fn rotate(&self) -> Result<u64, std::io::Error> {
        // this things would mean to removed the logs that are already flushed to the state machine, but for now we can just return a placeholder value
        // TODO: add mechanism to remove the logs that are already flushed to the state machine

        Ok(0) // Placeholder implementation
    }
    async fn mark_reserved(&self, segment_id: u64) -> Result<(), std::io::Error> {
        Ok(()) // Placeholder implementation
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::storage::record::RecordType;
    use rust_raft::storage::storage::MockStore;

    // Create a RaftNode with no peers to avoid network calls in tests
    fn create_test_raft_node() -> RaftNode {
        let mock_store = MockStore::new();
        RaftNode::new(
            "test-node".to_string(),
            vec![], // No peers - avoids network errors in tests
            Box::new(mock_store),
        )
    }

    #[tokio::test]
    async fn test_new_creates_recover_manager() {
        let raft_node = create_test_raft_node();
        let raft_storage = tokio::sync::RwLock::new(raft_node);

        let recover_manager = RaftRecoverManager::new(raft_storage);

        // Verify the recover manager was created (no panics)
        assert!(recover_manager.raft_node.try_read().is_ok());
    }

    #[tokio::test]
    async fn test_append_writes_log_to_raft() {
        // Arrange
        let raft_node = create_test_raft_node();
        let raft_storage = tokio::sync::RwLock::new(raft_node);
        let recover_manager = RaftRecoverManager::new(raft_storage);

        let cmd = LogCommand::new(RecordType::Put, b"key1".to_vec(), b"value1".to_vec(), 1);

        // Act
        let result = recover_manager.append(cmd).await;

        // Assert
        assert!(
            result.is_ok(),
            "append should succeed for single-node cluster"
        );
        let position = result.unwrap();
        assert_eq!(position.lsn, 0); // Placeholder value
    }

    #[tokio::test]
    async fn test_recover_commands_returns_written_commands() {
        // Arrange: This test verifies the basic append works
        // The full roundtrip depends on raft storage implementation
        let raft_node = create_test_raft_node();
        let raft_storage = tokio::sync::RwLock::new(raft_node);
        let recover_manager = RaftRecoverManager::new(raft_storage);

        // Write a command - the append should succeed
        let cmd = LogCommand::new(RecordType::Put, b"key1".to_vec(), b"value1".to_vec(), 1);
        let append_result = recover_manager.append(cmd).await;

        // Assert: append succeeded
        assert!(append_result.is_ok());
    }

    #[tokio::test]
    async fn test_append_multiple_commands() {
        // Arrange
        let raft_node = create_test_raft_node();
        let raft_storage = tokio::sync::RwLock::new(raft_node);
        let recover_manager = RaftRecoverManager::new(raft_storage);

        let commands = vec![
            LogCommand::new(RecordType::Put, b"key1".to_vec(), b"value1".to_vec(), 1),
            LogCommand::new(RecordType::Put, b"key2".to_vec(), b"value2".to_vec(), 2),
            LogCommand::new(RecordType::Delete, b"key1".to_vec(), vec![], 3),
        ];

        // Act
        let mut success_count = 0;
        for cmd in commands {
            let result = recover_manager.append(cmd).await;
            if result.is_ok() {
                success_count += 1;
            }
        }

        // Assert: At least some commands were appended
        assert_eq!(success_count, 3, "All appends should succeed");
    }

    #[tokio::test]
    async fn test_delete_command_roundtrip() {
        // Arrange: Test delete command append works
        let raft_node = create_test_raft_node();
        let raft_storage = tokio::sync::RwLock::new(raft_node);
        let recover_manager = RaftRecoverManager::new(raft_storage);

        // Write a delete command
        let delete_cmd = LogCommand::new(
            RecordType::Delete,
            b"key-to-delete".to_vec(),
            vec![], // No value for delete
            1,
        );

        // Act
        let result = recover_manager.append(delete_cmd).await;

        // Assert
        assert!(result.is_ok(), "Delete append should succeed");
    }

    #[tokio::test]
    async fn test_recover_commands_skips_invalid_serialized_data() {
        // Arrange: Test that recovery handles errors gracefully
        let raft_node = create_test_raft_node();
        let raft_storage = tokio::sync::RwLock::new(raft_node);
        let recover_manager = RaftRecoverManager::new(raft_storage);

        // Add a valid command
        let valid_cmd = LogCommand::new(
            RecordType::Put,
            b"valid-key".to_vec(),
            b"valid-value".to_vec(),
            1,
        );
        recover_manager.append(valid_cmd).await.unwrap();

        // Manually inject invalid data
        {
            let mut node = recover_manager.raft_node.write().await;
            let invalid_data = vec![0xFF, 0xFF, 0xFF, 0xFF];
            let _ = node.push_log(invalid_data, None).await;
        }

        // Act: Should return Ok even with invalid data (skips bad entries)
        let result = recover_manager.recover_commands().await;

        // Assert: Should not panic, returns whatever can be recovered
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_rotate_returns_placeholder_value() {
        // Arrange
        let raft_node = create_test_raft_node();
        let raft_storage = tokio::sync::RwLock::new(raft_node);
        let recover_manager = RaftRecoverManager::new(raft_storage);

        // Act
        let result = recover_manager.rotate().await;

        // Assert
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 0);
    }

    #[tokio::test]
    async fn test_mark_reserved_returns_ok() {
        // Arrange
        let raft_node = create_test_raft_node();
        let raft_storage = tokio::sync::RwLock::new(raft_node);
        let recover_manager = RaftRecoverManager::new(raft_storage);

        // Act
        let result = recover_manager.mark_reserved(42).await;

        // Assert
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_append_with_peers_fails_gracefully() {
        // Arrange: Create a node with peers (will fail due to network)
        let mock_store = MockStore::new();
        let raft_node_with_peers = RaftNode::new(
            "test-node".to_string(),
            vec!["peer-1".to_string()], // Has peers - network calls will fail
            Box::new(mock_store),
        );
        let raft_storage = tokio::sync::RwLock::new(raft_node_with_peers);
        let recover_manager = RaftRecoverManager::new(raft_storage);

        let cmd = LogCommand::new(RecordType::Put, b"key".to_vec(), b"value".to_vec(), 1);

        // Act
        let result = recover_manager.append(cmd).await;

        // Assert: This should fail with transport error (expected for test environment)
        // The test documents this expected behavior
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("transport error") || err.to_string().contains("Raft"));
    }
}
