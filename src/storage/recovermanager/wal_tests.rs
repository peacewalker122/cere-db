use std::sync::Arc;

use super::*;

fn command(key: &[u8], value: &[u8], lsn: u64) -> LogCommand {
    LogCommand::new(RecordType::Put, key.to_vec(), value.to_vec(), lsn)
}

async fn append_bytes(path: &Path, bytes: &[u8]) {
    let mut file = OpenOptions::new().append(true).open(path).await.unwrap();
    file.write_all(bytes).await.unwrap();
    file.sync_all().await.unwrap();
}

#[tokio::test]
async fn atomic_batch_preserves_application_sequences_and_physical_chains() {
    let directory = tempfile::tempdir().unwrap();
    let wal = WALManager::new(directory.path().into(), 4096)
        .await
        .unwrap();
    let commands = vec![command(b"a", b"one", 10), command(b"b", b"two", 11)];
    wal.write_transaction(commands.clone()).await.unwrap();
    drop(wal);
    let wal = WALManager::new(directory.path().into(), 4096)
        .await
        .unwrap();
    assert_eq!(wal.recover_commands().await.unwrap(), commands);
    wal.append(command(b"c", b"three", 12)).await.unwrap();
    assert_eq!(wal.state.lock().await.next_lsn, 6);
    assert_eq!(wal.recover_commands().await.unwrap().len(), 3);
}

#[tokio::test]
async fn every_torn_transaction_prefix_is_repaired_before_append() {
    let source = tempfile::tempdir().unwrap();
    let wal = WALManager::new(source.path().into(), 4096).await.unwrap();
    wal.append(command(b"base", b"winner", 1)).await.unwrap();
    let path = segment_path(source.path(), 0);
    let baseline = tokio::fs::read(&path).await.unwrap();
    wal.write_transaction(vec![
        command(b"base", b"loser", 2),
        command(b"b", b"loser", 3),
    ])
    .await
    .unwrap();
    let complete = tokio::fs::read(&path).await.unwrap();
    for cut in baseline.len()..complete.len() {
        let directory = tempfile::tempdir().unwrap();
        tokio::fs::write(segment_path(directory.path(), 0), &complete[..cut])
            .await
            .unwrap();
        let wal = WALManager::new(directory.path().into(), 4096)
            .await
            .unwrap();
        assert_eq!(
            wal.recover_commands().await.unwrap(),
            vec![command(b"base", b"winner", 1)],
            "cut {cut}"
        );
        wal.append(command(b"after", b"restart", 4)).await.unwrap();
        drop(wal);
        let wal = WALManager::new(directory.path().into(), 4096)
            .await
            .unwrap();
        assert_eq!(
            wal.recover_commands().await.unwrap(),
            vec![
                command(b"base", b"winner", 1),
                command(b"after", b"restart", 4)
            ],
            "cut {cut}"
        );
    }
}

#[tokio::test]
async fn rotation_retains_all_acknowledged_updates() {
    let directory = tempfile::tempdir().unwrap();
    let wal = WALManager::new(directory.path().into(), 150).await.unwrap();
    for sequence in 1..=12 {
        wal.append(command(b"key", &[sequence as u8], sequence))
            .await
            .unwrap();
    }
    wal.rotate_wal_file().await.unwrap();
    drop(wal);
    let wal = WALManager::new(directory.path().into(), 150).await.unwrap();
    let commands = wal.recover_commands().await.unwrap();
    assert_eq!(commands.len(), 12);
    assert_eq!(commands.last().unwrap().value, vec![12]);
    assert!(segment_path(directory.path(), 0).exists());
    wal.mark_reserved(0).await.unwrap();
    assert!(segment_path(directory.path(), 0).exists());
}

#[tokio::test]
async fn concurrent_writes_and_rotation_keep_one_physical_order() {
    let directory = tempfile::tempdir().unwrap();
    let wal = Arc::new(WALManager::new(directory.path().into(), 256).await.unwrap());
    let mut tasks = Vec::new();
    for sequence in 1u64..=32 {
        let wal = Arc::clone(&wal);
        tasks.push(tokio::spawn(async move {
            wal.append(command(&sequence.to_be_bytes(), b"value", sequence))
                .await
                .unwrap();
            if sequence % 4 == 0 {
                wal.rotate_wal_file().await.unwrap();
            }
        }));
    }
    for task in tasks {
        task.await.unwrap();
    }
    drop(wal);
    let wal = WALManager::new(directory.path().into(), 256).await.unwrap();
    let commands = wal.recover_commands().await.unwrap();
    assert_eq!(commands.len(), 32);
    assert_eq!(
        commands
            .iter()
            .map(|command| command.lsn)
            .collect::<BTreeSet<_>>(),
        (1..=32).collect()
    );
}

#[tokio::test]
async fn complete_corruption_is_rejected_without_truncation() {
    let directory = tempfile::tempdir().unwrap();
    let wal = WALManager::new(directory.path().into(), 4096)
        .await
        .unwrap();
    wal.append(command(b"key", b"value", 1)).await.unwrap();
    drop(wal);
    let path = segment_path(directory.path(), 0);
    let original = tokio::fs::read(&path).await.unwrap();
    let commit_start = original.len() - FRAME_HEADER_SIZE - 17;
    for offset in (WAL_HEADER_SIZE as usize..WAL_HEADER_SIZE as usize + FRAME_HEADER_SIZE)
        .chain(commit_start..commit_start + FRAME_HEADER_SIZE)
        .chain([original.len() - 1])
    {
        let mut corrupt = original.clone();
        corrupt[offset] ^= 1;
        tokio::fs::write(&path, &corrupt).await.unwrap();
        assert!(
            WALManager::new(directory.path().into(), 4096)
                .await
                .is_err(),
            "offset {offset}"
        );
        assert_eq!(tokio::fs::read(&path).await.unwrap(), corrupt);
    }
}

#[tokio::test]
async fn every_incomplete_newest_segment_header_prefix_is_completed_before_append() {
    for version in [LEGACY_WAL_VERSION, WAL_VERSION] {
        let header = WALHeader {
            version,
            ..WALHeader::default()
        }
        .encode();
        for has_prior_commit in [false, true] {
            for cut in 0..header.len() {
                let directory = tempfile::tempdir().unwrap();
                let mut expected = Vec::new();
                if has_prior_commit {
                    let wal = WALManager::new(directory.path().into(), 4096)
                        .await
                        .unwrap();
                    let prior = command(b"prior", b"committed", 1);
                    wal.append(prior.clone()).await.unwrap();
                    expected.push(prior);
                }
                let path = segment_path(directory.path(), u64::from(has_prior_commit));
                tokio::fs::write(&path, &header[..cut]).await.unwrap();
                if has_prior_commit && version == LEGACY_WAL_VERSION && cut >= 16 {
                    assert!(
                        WALManager::new(directory.path().into(), 4096)
                            .await
                            .is_err()
                    );
                    assert_eq!(tokio::fs::read(&path).await.unwrap(), header[..cut]);
                    continue;
                }
                let wal = WALManager::new(directory.path().into(), 4096)
                    .await
                    .unwrap();
                assert_eq!(wal.recover_commands().await.unwrap(), expected);
                let after = command(b"after", b"restart", 2);
                wal.append(after.clone()).await.unwrap();
                expected.push(after);
                drop(wal);
                let wal = WALManager::new(directory.path().into(), 4096)
                    .await
                    .unwrap();
                assert_eq!(wal.recover_commands().await.unwrap(), expected);
            }
        }
    }
}

#[tokio::test]
async fn malformed_newest_headers_and_incomplete_sealed_headers_are_not_repaired() {
    let header = WALHeader::default().encode();
    for cut in 0..=header.len() {
        let directory = tempfile::tempdir().unwrap();
        let path = segment_path(directory.path(), 0);
        let mut bytes = header[..cut].to_vec();
        if cut != 0 {
            bytes[cut - 1] ^= 1;
            tokio::fs::write(&path, &bytes).await.unwrap();
            assert!(
                WALManager::new(directory.path().into(), 4096)
                    .await
                    .is_err()
            );
            assert_eq!(tokio::fs::read(&path).await.unwrap(), bytes);
        }
        if cut < header.len() {
            tokio::fs::write(&path, &header[..cut]).await.unwrap();
            tokio::fs::write(segment_path(directory.path(), 1), &header)
                .await
                .unwrap();
            assert!(
                WALManager::new(directory.path().into(), 4096)
                    .await
                    .is_err()
            );
            assert_eq!(tokio::fs::read(&path).await.unwrap(), header[..cut]);
        }
    }
}

#[tokio::test]
async fn segment_creation_does_not_clobber_an_existing_segment() {
    let directory = tempfile::tempdir().unwrap();
    let wal = WALManager::new(directory.path().into(), 4096)
        .await
        .unwrap();
    wal.append(command(b"key", b"committed", 1)).await.unwrap();
    let path = segment_path(directory.path(), 0);
    let bytes = tokio::fs::read(&path).await.unwrap();
    assert!(create_segment(directory.path(), 0).await.is_err());
    assert_eq!(tokio::fs::read(path).await.unwrap(), bytes);
}

#[tokio::test]
async fn legacy_values_larger_than_version_two_limit_remain_readable() {
    let directory = tempfile::tempdir().unwrap();
    let mut bytes = WALHeader {
        version: LEGACY_WAL_VERSION,
        ..WALHeader::default()
    }
    .encode();
    let start = bytes.len();
    let value_length = MAX_FRAME_SIZE + 1;
    bytes.push(RecordType::Put as u8);
    bytes.extend_from_slice(&7u64.to_be_bytes());
    bytes.extend_from_slice(&3u64.to_be_bytes());
    bytes.extend_from_slice(b"old");
    bytes.extend_from_slice(&(value_length as u64).to_be_bytes());
    bytes.resize(bytes.len() + value_length, 42);
    bytes.extend_from_slice(&crc32fast::hash(&bytes[start..]).to_be_bytes());
    let path = segment_path(directory.path(), 0);
    tokio::fs::write(&path, &bytes).await.unwrap();
    let wal = WALManager::new(directory.path().into(), 4096)
        .await
        .unwrap();
    wal.append(command(b"new", b"value", 8)).await.unwrap();
    drop(wal);
    let wal = WALManager::new(directory.path().into(), 4096)
        .await
        .unwrap();
    let records = wal.recover_commands().await.unwrap();
    assert_eq!(records.len(), 2);
    assert_eq!(records[0].key, b"old");
    assert_eq!(records[0].lsn, 7);
    assert_eq!(records[0].value.len(), value_length);
    assert!(records[0].value.iter().all(|byte| *byte == 42));
    assert_eq!(records[1], command(b"new", b"value", 8));
    assert_eq!(tokio::fs::read(path).await.unwrap(), bytes);
}

#[tokio::test]
async fn oversized_version_two_frames_are_rejected_without_modifying_the_log() {
    let directory = tempfile::tempdir().unwrap();
    let wal = WALManager::new(directory.path().into(), 4096)
        .await
        .unwrap();
    let path = segment_path(directory.path(), 0);
    let original = tokio::fs::read(&path).await.unwrap();
    assert!(
        wal.append(LogCommand::new(
            RecordType::Put,
            b"large".to_vec(),
            vec![42; MAX_FRAME_SIZE + 1],
            1,
        ))
        .await
        .is_err()
    );
    assert_eq!(tokio::fs::read(&path).await.unwrap(), original);
    wal.append(command(b"valid", b"value", 2)).await.unwrap();
    drop(wal);
    let mut bytes = tokio::fs::read(&path).await.unwrap();
    let start = WAL_HEADER_SIZE as usize;
    bytes[start + 8..start + 12].copy_from_slice(&((MAX_FRAME_SIZE + 1) as u32).to_be_bytes());
    let header_checksum = crc32fast::hash(&bytes[start..start + 16]);
    bytes[start + 16..start + 20].copy_from_slice(&header_checksum.to_be_bytes());
    tokio::fs::write(&path, &bytes).await.unwrap();
    assert!(
        WALManager::new(directory.path().into(), 4096)
            .await
            .is_err()
    );
    assert_eq!(tokio::fs::read(path).await.unwrap(), bytes);
}

#[tokio::test]
async fn checksum_valid_broken_chains_are_rejected() {
    for frame in [
        Frame {
            lsn: 2,
            transaction_id: 1,
            previous_lsn: 0,
            record: TransactionRecord::Update(command(b"key", b"value", 1)),
        },
        Frame {
            lsn: 1,
            transaction_id: 0,
            previous_lsn: 0,
            record: TransactionRecord::Update(command(b"key", b"value", 1)),
        },
        Frame {
            lsn: 1,
            transaction_id: 1,
            previous_lsn: 9,
            record: TransactionRecord::Update(command(b"key", b"value", 1)),
        },
        Frame {
            lsn: 1,
            transaction_id: 1,
            previous_lsn: 0,
            record: TransactionRecord::Commit,
        },
    ] {
        let directory = tempfile::tempdir().unwrap();
        drop(
            WALManager::new(directory.path().into(), 4096)
                .await
                .unwrap(),
        );
        append_bytes(
            &segment_path(directory.path(), 0),
            &encode_frame(&frame).unwrap(),
        )
        .await;
        assert!(
            WALManager::new(directory.path().into(), 4096)
                .await
                .is_err()
        );
    }
}

#[tokio::test]
async fn incomplete_sealed_segment_is_rejected() {
    let directory = tempfile::tempdir().unwrap();
    let wal = WALManager::new(directory.path().into(), 4096)
        .await
        .unwrap();
    wal.append(command(b"key", b"value", 1)).await.unwrap();
    wal.rotate_wal_file().await.unwrap();
    drop(wal);
    let path = segment_path(directory.path(), 0);
    let file = OpenOptions::new().write(true).open(path).await.unwrap();
    file.set_len(40).await.unwrap();
    assert!(
        WALManager::new(directory.path().into(), 4096)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn legacy_wal_is_read_without_rewriting_and_new_writes_use_version_two() {
    let directory = tempfile::tempdir().unwrap();
    let mut bytes = WALHeader {
        version: LEGACY_WAL_VERSION,
        ..WALHeader::default()
    }
    .encode();
    let mut record = vec![1];
    record.extend_from_slice(&7u64.to_be_bytes());
    record.extend_from_slice(&3u64.to_be_bytes());
    record.extend_from_slice(b"old");
    record.extend_from_slice(&5u64.to_be_bytes());
    record.extend_from_slice(b"value");
    record.extend_from_slice(&crc32fast::hash(&record).to_be_bytes());
    bytes.extend_from_slice(&record);
    tokio::fs::write(segment_path(directory.path(), 0), &bytes)
        .await
        .unwrap();
    let wal = WALManager::new(directory.path().into(), 4096)
        .await
        .unwrap();
    wal.append(command(b"new", b"value", 8)).await.unwrap();
    assert_eq!(
        tokio::fs::read(segment_path(directory.path(), 0))
            .await
            .unwrap(),
        bytes
    );
    assert_eq!(
        wal.recover_commands().await.unwrap(),
        vec![command(b"old", b"value", 7), command(b"new", b"value", 8)]
    );
    drop(wal);
    let wal = WALManager::new(directory.path().into(), 4096)
        .await
        .unwrap();
    assert_eq!(wal.recover_commands().await.unwrap().len(), 2);
}

#[tokio::test]
async fn rejected_input_does_not_poison_writer() {
    let directory = tempfile::tempdir().unwrap();
    let wal = WALManager::new(directory.path().into(), 4096)
        .await
        .unwrap();
    assert!(wal.write_transaction(Vec::new()).await.is_err());
    assert!(
        wal.append(LogCommand::new(
            RecordType::Delete,
            b"key".to_vec(),
            b"invalid".to_vec(),
            1
        ))
        .await
        .is_err()
    );
    wal.append(command(b"valid", b"value", 2)).await.unwrap();
    assert_eq!(
        wal.recover_commands().await.unwrap(),
        vec![command(b"valid", b"value", 2)]
    );
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn failed_io_poisons_writer_until_reopen() {
    let directory = tempfile::tempdir().unwrap();
    let wal = WALManager::new(directory.path().into(), 4096)
        .await
        .unwrap();
    wal.state.lock().await.file = OpenOptions::new()
        .write(true)
        .open("/dev/full")
        .await
        .unwrap();
    assert!(matches!(
        wal.append(command(b"key", b"value", 1)).await,
        Err(DBError::IO(_))
    ));
    assert!(wal.append(command(b"key", b"value", 1)).await.is_err());
    assert!(wal.rotate_wal_file().await.is_err());
    drop(wal);
    let wal = WALManager::new(directory.path().into(), 4096)
        .await
        .unwrap();
    wal.append(command(b"key", b"value", 1)).await.unwrap();
}
