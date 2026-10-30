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
async fn engine_groups_recover_as_distinct_transactions_with_one_sync() {
    let directory = tempfile::tempdir().unwrap();
    let wal = WALManager::new(directory.path().into(), 4096)
        .await
        .unwrap();
    let first = vec![command(b"a", b"one", 70), command(b"b", b"two", 71)];
    let second = vec![command(b"c", b"three", 72)];
    wal.write_engine_transactions(vec![first.clone(), second.clone()], None)
        .await
        .unwrap();
    assert_eq!(
        wal.sync_data_count
            .load(std::sync::atomic::Ordering::SeqCst),
        1
    );
    let mut analysis = Analysis::default();
    let paths = segment_paths(directory.path(), None).await.unwrap();
    for (_, path) in paths {
        scan_segment(&path, false, &mut analysis).await.unwrap();
    }
    assert_eq!(analysis.committed_transactions.len(), 2);
    assert_eq!(analysis.latest_lsn, 5);
    assert_eq!(
        wal.recover_commands().await.unwrap(),
        [first, second].concat()
    );
    drop(wal);
    let wal = WALManager::new(directory.path().into(), 4096)
        .await
        .unwrap();
    assert_eq!(wal.recover_commands().await.unwrap().len(), 3);
}

#[tokio::test]
async fn invalid_later_group_writes_nothing_and_does_not_poison_writer() {
    let directory = tempfile::tempdir().unwrap();
    let wal = WALManager::new(directory.path().into(), 4096)
        .await
        .unwrap();
    let path = segment_path(directory.path(), 0);
    let before = tokio::fs::read(&path).await.unwrap();
    let result = wal
        .write_engine_transactions(
            vec![
                vec![command(b"valid", b"value", 1)],
                vec![LogCommand::new(
                    RecordType::Delete,
                    b"bad".to_vec(),
                    b"x".to_vec(),
                    2,
                )],
            ],
            None,
        )
        .await;
    assert!(result.is_err());
    assert_eq!(tokio::fs::read(path).await.unwrap(), before);
    wal.append(command(b"after", b"valid", 3)).await.unwrap();
}

#[tokio::test]
async fn grouped_append_rotates_once_and_recovers_every_transaction() {
    let directory = tempfile::tempdir().unwrap();
    let wal = WALManager::new(directory.path().into(), 180).await.unwrap();
    let groups: Vec<_> = (0..4)
        .map(|n| vec![command(&[n], &[n; 40], n as u64 + 1)])
        .collect();
    wal.write_engine_transactions(groups.clone(), None)
        .await
        .unwrap();
    assert_eq!(wal.recover_commands().await.unwrap(), groups.concat());
    assert_eq!(wal.state.lock().await.next_lsn, 9);
    drop(wal);
    let wal = WALManager::new(directory.path().into(), 180).await.unwrap();
    assert_eq!(wal.recover_commands().await.unwrap(), groups.concat());
    assert_eq!(wal.state.lock().await.next_lsn, 9);
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

fn raw_frame(bytes: &[u8], offset: u64) -> (Frame, u32) {
    let offset = usize::try_from(offset).unwrap();
    let header = &bytes[offset..offset + FRAME_HEADER_SIZE];
    let (lsn, body_length) = decode_frame_header(header).unwrap();
    let body_length = usize::try_from(body_length).unwrap();
    let frame_length = FRAME_HEADER_SIZE + body_length;
    let body = &bytes[offset + FRAME_HEADER_SIZE..offset + frame_length];
    verify_frame_body(header, body).unwrap();
    (
        decode_frame(lsn, body).unwrap(),
        u32::try_from(frame_length).unwrap(),
    )
}

fn flatten(receipts: &[Vec<WalValueRef>]) -> Vec<WalValueRef> {
    receipts.iter().flatten().cloned().collect()
}

async fn assert_reference_matches(wal_dir: &Path, reference: &WalValueRef, command: &LogCommand) {
    let bytes = tokio::fs::read(segment_path(wal_dir, reference.segment_id))
        .await
        .unwrap();
    let header = WALHeader::decode(&bytes[..WAL_HEADER_SIZE as usize]).unwrap();
    assert_eq!(header.version, WAL_VERSION);

    let (frame, frame_length) = raw_frame(&bytes, reference.frame_offset);
    assert_eq!(frame_length, reference.frame_length);
    assert_eq!(frame.lsn, reference.physical_lsn);
    assert_eq!(frame.transaction_id, reference.transaction_id);
    let TransactionRecord::Update(decoded) = frame.record else {
        panic!("receipt frame is not an update")
    };
    assert_eq!(decoded, *command);

    let (start, _) = raw_frame(&bytes, reference.transaction_start_offset);
    assert_eq!(start.lsn, reference.transaction_id);
    assert_eq!(start.previous_lsn, 0);
    assert_eq!(start.transaction_id, reference.transaction_id);

    let (commit, _) = raw_frame(&bytes, reference.commit_offset);
    assert!(matches!(commit.record, TransactionRecord::Commit));
    assert_eq!(commit.lsn, reference.commit_lsn);
    assert_eq!(commit.transaction_id, reference.transaction_id);
}

#[tokio::test]
async fn receipts_follow_committed_frames_across_rotation_and_reopen() {
    let directory = tempfile::tempdir().unwrap();
    let wal = WALManager::new(directory.path().into(), 200).await.unwrap();
    let first = vec![
        command(b"first", b"alpha", 1),
        LogCommand::new(RecordType::Delete, b"gone".to_vec(), Vec::new(), 2),
    ];
    let first_receipts = wal
        .append_engine_transactions_with_receipts(vec![first.clone()], None)
        .await
        .unwrap();
    assert_eq!(wal.sync_data_count(), 1);
    assert_eq!(first_receipts.len(), 1);
    assert_eq!(first_receipts[0].len(), 2);
    assert_eq!(first_receipts[0][0].segment_id, 0);
    for (reference, operation) in first_receipts[0].iter().zip(&first) {
        assert_reference_matches(directory.path(), reference, operation).await;
    }
    assert!(
        wal.read_value(&first_receipts[0][1], b"gone", 2)
            .await
            .is_err()
    );

    let groups: Vec<_> = (0..3)
        .map(|n| vec![command(&[b'g', n], &[n; 32], n as u64 + 3)])
        .collect();
    let second_receipts = wal
        .append_engine_transactions_with_receipts(groups.clone(), None)
        .await
        .unwrap();
    assert_eq!(wal.sync_data_count(), 2);
    assert_eq!(second_receipts.len(), groups.len());
    let rotated_segment = second_receipts[0][0].segment_id;
    assert!(rotated_segment > first_receipts[0][0].segment_id);
    assert!(
        second_receipts
            .iter()
            .flatten()
            .all(|reference| reference.segment_id == rotated_segment)
    );
    for (group, references) in groups.iter().zip(&second_receipts) {
        for (operation, reference) in group.iter().zip(references) {
            assert_reference_matches(directory.path(), reference, operation).await;
            assert_eq!(
                wal.read_value(reference, &operation.key, operation.lsn)
                    .await
                    .unwrap(),
                operation.value
            );
        }
    }

    let mut expected_commands = first.clone();
    expected_commands.extend(groups.iter().flatten().cloned());
    let mut expected_references = first_receipts[0].clone();
    expected_references.extend(flatten(&second_receipts));
    assert_eq!(wal.recover_commands().await.unwrap(), expected_commands);
    drop(wal);

    let wal = WALManager::new(directory.path().into(), 200).await.unwrap();
    let recovered = wal.recover_records_with_refs().await.unwrap();
    assert_eq!(
        recovered
            .iter()
            .map(|update| CommandView {
                record_type: update.record.record_type,
                key: update.record.key.clone(),
                value: update.record.value.clone(),
                lsn: update.record.lsn,
            })
            .collect::<Vec<_>>(),
        expected_commands
            .iter()
            .map(|operation| CommandView {
                record_type: operation.record_type as u8,
                key: operation.key.clone(),
                value: operation.value.clone(),
                lsn: operation.lsn,
            })
            .collect::<Vec<_>>()
    );
    assert_eq!(
        recovered
            .iter()
            .map(|update| update.reference.clone())
            .collect::<Vec<_>>(),
        expected_references
    );
    for update in &recovered {
        if update.reference.record_type == RecordType::Put {
            assert_eq!(
                wal.read_value(&update.reference, &update.record.key, update.record.lsn)
                    .await
                    .unwrap(),
                update.record.value
            );
        }
    }
}

#[derive(Debug, PartialEq)]
struct CommandView {
    record_type: u8,
    key: Vec<u8>,
    value: Vec<u8>,
    lsn: u64,
}

#[tokio::test]
async fn read_value_returns_exact_values_and_rejects_reference_mismatch() {
    let directory = tempfile::tempdir().unwrap();
    let wal = WALManager::new(directory.path().into(), 4 * 1024 * 1024)
        .await
        .unwrap();
    let binary_value: Vec<u8> = (0..=255).chain([0, 255, 0, 1]).collect();
    let commands = vec![
        LogCommand::new(RecordType::Put, b"empty".to_vec(), Vec::new(), 1),
        LogCommand::new(
            RecordType::Put,
            "ключ🔑".as_bytes().to_vec(),
            "nilai 値\0biner".as_bytes().to_vec(),
            2,
        ),
        LogCommand::new(
            RecordType::Put,
            b"bin\0key".to_vec(),
            binary_value.clone(),
            3,
        ),
        LogCommand::new(
            RecordType::Put,
            b"large".to_vec(),
            vec![0xAB; 512 * 1024],
            4,
        ),
    ];
    let receipts = wal
        .append_engine_transactions_with_receipts(vec![commands.clone()], None)
        .await
        .unwrap();
    let references = &receipts[0];
    for (reference, operation) in references.iter().zip(&commands) {
        assert_eq!(
            wal.read_value(reference, &operation.key, operation.lsn)
                .await
                .unwrap(),
            operation.value
        );
    }

    assert!(wal.read_value(&references[0], b"other", 1).await.is_err());
    assert!(wal.read_value(&references[0], b"empty", 2).await.is_err());

    let base = references[1].clone();
    let mut forged = Vec::new();
    forged.push({
        let mut value = base.clone();
        value.physical_lsn += 1;
        value
    });
    forged.push({
        let mut value = base.clone();
        value.frame_offset += 1;
        value
    });
    forged.push({
        let mut value = base.clone();
        value.frame_length += 1;
        value
    });
    forged.push({
        let mut value = base.clone();
        value.transaction_start_offset += 1;
        value
    });
    forged.push({
        let mut value = base.clone();
        value.commit_offset += 1;
        value
    });
    forged.push({
        let mut value = base.clone();
        value.commit_lsn += 1;
        value
    });
    forged.push({
        let mut value = base.clone();
        value.transaction_id += 1;
        value
    });
    forged.push({
        let mut value = base.clone();
        value.application_sequence += 1;
        value
    });
    forged.push({
        let mut value = base.clone();
        value.key_length += 1;
        value
    });
    forged.push({
        let mut value = base.clone();
        value.key_checksum ^= 1;
        value
    });
    forged.push({
        let mut value = base.clone();
        value.value_length += 1;
        value
    });
    forged.push({
        let mut value = base.clone();
        value.value_checksum ^= 1;
        value
    });
    forged.push({
        let mut value = base.clone();
        value.segment_id += 1;
        value
    });
    for reference in forged {
        assert!(
            wal.read_value(&reference, &commands[1].key, commands[1].lsn)
                .await
                .is_err()
        );
    }
    let mut deleted = references[0].clone();
    deleted.record_type = RecordType::Delete;
    assert!(
        wal.read_value(&deleted, &commands[0].key, commands[0].lsn)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn value_ref_encoding_round_trips_and_rejects_tampering() {
    let directory = tempfile::tempdir().unwrap();
    let wal = WALManager::new(directory.path().into(), 4096)
        .await
        .unwrap();
    let receipts = wal
        .append_engine_transactions_with_receipts(vec![vec![command(b"key", b"value", 1)]], None)
        .await
        .unwrap();
    let reference = receipts[0][0].clone();
    let encoded = reference.encode();
    assert_eq!(encoded.len(), WAL_VALUE_REF_ENCODED_SIZE);
    assert_eq!(WalValueRef::decode(&encoded).unwrap(), reference);
    for offset in 0..encoded.len() {
        let mut corrupt = encoded;
        corrupt[offset] ^= 1;
        assert!(WalValueRef::decode(&corrupt).is_err(), "offset {offset}");
    }
    assert!(WalValueRef::decode(&encoded[..WAL_VALUE_REF_ENCODED_SIZE - 1]).is_err());
    let mut invalid = reference.clone();
    invalid.transaction_id = 0;
    assert!(WalValueRef::decode(&invalid.encode()).is_err());
    let mut invalid = reference.clone();
    invalid.transaction_start_offset = WAL_HEADER_SIZE - 1;
    assert!(WalValueRef::decode(&invalid.encode()).is_err());
    let mut invalid = reference;
    invalid.record_type = RecordType::Delete;
    assert!(WalValueRef::decode(&invalid.encode()).is_err());
}

#[tokio::test]
async fn read_value_rejects_corrupted_frames_and_missing_segments() {
    let directory = tempfile::tempdir().unwrap();
    let wal = WALManager::new(directory.path().into(), 4096)
        .await
        .unwrap();
    let receipts = wal
        .append_engine_transactions_with_receipts(
            vec![vec![
                command(b"target", b"payload", 1),
                command(b"keep", b"value", 2),
            ]],
            None,
        )
        .await
        .unwrap();
    let reference = &receipts[0][0];
    let path = segment_path(directory.path(), reference.segment_id);
    let value_offset =
        reference.frame_offset + FRAME_HEADER_SIZE as u64 + 42 + u64::from(reference.key_length);
    let mut file = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
    std::io::Seek::seek(&mut file, std::io::SeekFrom::Start(value_offset)).unwrap();
    std::io::Write::write_all(&mut file, &[0xFF]).unwrap();
    file.sync_all().unwrap();
    assert!(wal.read_value(reference, b"target", 1).await.is_err());
    std::fs::remove_file(&path).unwrap();
    assert!(wal.read_value(&receipts[0][1], b"keep", 2).await.is_err());
}

#[tokio::test]
async fn read_value_rejects_legacy_segment_headers() {
    let directory = tempfile::tempdir().unwrap();
    let wal = WALManager::new(directory.path().into(), 4096)
        .await
        .unwrap();
    let receipts = wal
        .append_engine_transactions_with_receipts(vec![vec![command(b"key", b"value", 1)]], None)
        .await
        .unwrap();
    tokio::fs::write(
        segment_path(directory.path(), 0),
        WALHeader {
            version: LEGACY_WAL_VERSION,
            ..WALHeader::default()
        }
        .encode(),
    )
    .await
    .unwrap();
    assert!(wal.read_value(&receipts[0][0], b"key", 1).await.is_err());
}

#[tokio::test]
async fn read_value_rejects_uncommitted_and_non_commit_references() {
    let directory = tempfile::tempdir().unwrap();
    let wal = WALManager::new(directory.path().into(), 4096)
        .await
        .unwrap();
    let torn = command(b"torn", b"value", 1);
    let update = Frame {
        lsn: 1,
        transaction_id: 1,
        previous_lsn: 0,
        record: TransactionRecord::Update(torn.clone()),
    };
    let frame = encode_frame(&update).unwrap();
    append_bytes(&segment_path(directory.path(), 0), &frame).await;
    let reference = WalValueRef {
        segment_id: 0,
        transaction_id: 1,
        transaction_start_offset: WAL_HEADER_SIZE,
        commit_offset: WAL_HEADER_SIZE + frame.len() as u64,
        commit_lsn: 2,
        frame_offset: WAL_HEADER_SIZE,
        frame_length: frame.len() as u32,
        physical_lsn: 1,
        application_sequence: 1,
        record_type: RecordType::Put,
        key_length: torn.key.len() as u32,
        key_checksum: crc32fast::hash(&torn.key),
        value_length: torn.value.len() as u64,
        value_checksum: crc32fast::hash(&torn.value),
    };
    assert!(wal.read_value(&reference, b"torn", 1).await.is_err());

    let second = tempfile::tempdir().unwrap();
    let wal = WALManager::new(second.path().into(), 4096).await.unwrap();
    let receipts = wal
        .append_engine_transactions_with_receipts(
            vec![vec![command(b"a", b"one", 1), command(b"b", b"two", 2)]],
            None,
        )
        .await
        .unwrap();
    let mut forged = receipts[0][0].clone();
    forged.commit_offset = receipts[0][1].frame_offset;
    forged.commit_lsn = receipts[0][1].physical_lsn;
    assert!(wal.read_value(&forged, b"a", 1).await.is_err());
}

#[tokio::test]
async fn recovery_refs_reject_legacy_records_while_normal_recovery_works() {
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
    assert_eq!(
        wal.recover_commands().await.unwrap(),
        vec![command(b"old", b"value", 7)]
    );
    assert!(matches!(
        wal.recover_records_with_refs().await,
        Err(DBError::StorageError(_))
    ));
}

#[tokio::test]
async fn read_value_rejects_overflowing_transaction_lsn_chain() {
    let directory = tempfile::tempdir().unwrap();
    let wal = WALManager::new(directory.path().into(), 4096)
        .await
        .unwrap();
    let selected = command(b"selected", b"value", 1);
    let transaction_id = u64::MAX - 1;
    let first = encode_frame(&Frame {
        lsn: transaction_id,
        transaction_id,
        previous_lsn: 0,
        record: TransactionRecord::Update(selected.clone()),
    })
    .unwrap();
    let second = encode_frame(&Frame {
        lsn: u64::MAX,
        transaction_id,
        previous_lsn: transaction_id,
        record: TransactionRecord::Update(command(b"second", b"value", 2)),
    })
    .unwrap();
    let commit = encode_frame(&Frame {
        lsn: u64::MAX,
        transaction_id,
        previous_lsn: u64::MAX,
        record: TransactionRecord::Commit,
    })
    .unwrap();
    let reference = WalValueRef {
        segment_id: 0,
        transaction_id,
        transaction_start_offset: WAL_HEADER_SIZE,
        commit_offset: WAL_HEADER_SIZE + first.len() as u64 + second.len() as u64,
        commit_lsn: u64::MAX,
        frame_offset: WAL_HEADER_SIZE,
        frame_length: first.len() as u32,
        physical_lsn: transaction_id,
        application_sequence: selected.lsn,
        record_type: RecordType::Put,
        key_length: selected.key.len() as u32,
        key_checksum: crc32fast::hash(&selected.key),
        value_length: selected.value.len() as u64,
        value_checksum: crc32fast::hash(&selected.value),
    };
    let bytes = [first, second, commit].concat();
    append_bytes(&segment_path(directory.path(), 0), &bytes).await;
    let result = wal
        .read_value(&reference, &selected.key, selected.lsn)
        .await;
    assert!(matches!(result, Err(DBError::Corrupted(_))));
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn failed_receipted_append_returns_no_receipts_and_does_not_advance_state() {
    let directory = tempfile::tempdir().unwrap();
    let wal = WALManager::new(directory.path().into(), 4096)
        .await
        .unwrap();
    let before = {
        let state = wal.state.lock().await;
        (state.next_lsn, state.size)
    };
    wal.state.lock().await.file = OpenOptions::new()
        .write(true)
        .open("/dev/full")
        .await
        .unwrap();
    let result = wal
        .append_engine_transactions_with_receipts(vec![vec![command(b"key", b"value", 1)]], None)
        .await;
    assert!(matches!(result, Err(DBError::IO(_))));
    {
        let state = wal.state.lock().await;
        assert_eq!((state.next_lsn, state.size), before);
        assert!(state.poisoned);
    }
    assert!(
        wal.append_engine_transactions_with_receipts(
            vec![vec![command(b"key", b"value", 1)]],
            None,
        )
        .await
        .is_err()
    );
}

#[tokio::test]
async fn normal_append_skips_planned_updates_and_writes_receipted_bytes() {
    let groups: Vec<_> = (0u8..4)
        .map(|n| {
            vec![
                command(&[b'k', n], &[n; 40], u64::from(n) + 1),
                LogCommand::new(
                    RecordType::Delete,
                    vec![b'd', n],
                    Vec::new(),
                    u64::from(n) + 100,
                ),
            ]
        })
        .collect();
    let tail = vec![command(b"tail", b"value", 200)];

    let normal_dir = tempfile::tempdir().unwrap();
    let normal = WALManager::new(normal_dir.path().into(), 300)
        .await
        .unwrap();
    normal
        .write_engine_transactions(groups.clone(), None)
        .await
        .unwrap();
    assert_eq!(normal.planned_update_count(), 0);
    normal.write_transaction(tail.clone()).await.unwrap();
    assert_eq!(normal.planned_update_count(), 0);
    normal
        .write_engine_transaction(tail.clone(), None)
        .await
        .unwrap();
    assert_eq!(normal.planned_update_count(), 0);

    let receipted_dir = tempfile::tempdir().unwrap();
    let receipted = WALManager::new(receipted_dir.path().into(), 300)
        .await
        .unwrap();
    let mut references = flatten(
        &receipted
            .append_engine_transactions_with_receipts(groups.clone(), None)
            .await
            .unwrap(),
    );
    for _ in 0..2 {
        references.push(
            receipted
                .append_engine_transactions_with_receipts(vec![tail.clone()], None)
                .await
                .unwrap()[0][0]
                .clone(),
        );
    }
    assert_eq!(
        receipted.planned_update_count(),
        (groups.iter().map(Vec::len).sum::<usize>() + 2) as u64
    );

    let normal_segments = segment_paths(normal_dir.path(), None).await.unwrap();
    let receipted_segments = segment_paths(receipted_dir.path(), None).await.unwrap();
    assert_eq!(normal_segments.len(), receipted_segments.len());
    assert!(normal_segments.len() > 1);
    for ((normal_id, normal_path), (receipted_id, receipted_path)) in
        normal_segments.iter().zip(&receipted_segments)
    {
        assert_eq!(normal_id, receipted_id);
        assert_eq!(
            tokio::fs::read(normal_path).await.unwrap(),
            tokio::fs::read(receipted_path).await.unwrap()
        );
    }
    drop(normal);
    drop(receipted);

    let normal = WALManager::new(normal_dir.path().into(), 300)
        .await
        .unwrap();
    let recovered = normal.recover_records_with_refs().await.unwrap();
    assert_eq!(
        recovered
            .iter()
            .map(|update| update.reference.clone())
            .collect::<Vec<_>>(),
        references
    );
}
