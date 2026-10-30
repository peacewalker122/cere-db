use std::{
    collections::BTreeMap,
    fs,
    io::Write,
    os::unix::fs::symlink,
    path::{Path, PathBuf},
    process::{Command, Output, Stdio},
    thread,
    time::{Duration, Instant},
};

use ceredb::api::api::AsyncKVEngine;
use ceredb::{DocumentId, ExperimentalEngine, FtsIndex, KV2};
use tempfile::TempDir;

fn run_cli(data_dir: &Path, input: &str) -> Output {
    let mut child = Command::new(env!("CARGO_BIN_EXE_ceredb"))
        .arg("--data-dir")
        .arg(data_dir)
        .arg("--log-level")
        .arg("off")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(input.as_bytes())
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        match child.try_wait().unwrap() {
            Some(_) => return child.wait_with_output().unwrap(),
            None if Instant::now() < deadline => thread::sleep(Duration::from_millis(10)),
            None => {
                let _ = child.kill();
                let output = child.wait_with_output().unwrap();
                panic!(
                    "CLI did not exit within timeout: {}",
                    String::from_utf8_lossy(&output.stderr)
                );
            }
        }
    }
}

fn successful_cli(data_dir: &Path, input: &str) -> String {
    let output = run_cli(data_dir, input);
    assert!(
        output.status.success(),
        "CLI failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap()
}

fn escaped(value: &str) -> String {
    value.chars().flat_map(char::escape_default).collect()
}

#[derive(Debug, PartialEq, Eq)]
enum SnapshotEntry {
    Directory,
    File(Vec<u8>),
    Symlink(PathBuf),
    Other,
}

fn snapshot_tree(root: &Path) -> BTreeMap<PathBuf, SnapshotEntry> {
    fn add_entries(root: &Path, directory: &Path, entries: &mut BTreeMap<PathBuf, SnapshotEntry>) {
        for entry in fs::read_dir(directory).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();
            let relative = path.strip_prefix(root).unwrap().to_path_buf();
            let file_type = entry.file_type().unwrap();
            if file_type.is_dir() {
                entries.insert(relative, SnapshotEntry::Directory);
                add_entries(root, &path, entries);
            } else if file_type.is_file() {
                entries.insert(relative, SnapshotEntry::File(fs::read(path).unwrap()));
            } else if file_type.is_symlink() {
                entries.insert(
                    relative,
                    SnapshotEntry::Symlink(fs::read_link(path).unwrap()),
                );
            } else {
                entries.insert(relative, SnapshotEntry::Other);
            }
        }
    }

    let mut entries = BTreeMap::new();
    add_entries(root, root, &mut entries);
    entries
}

async fn create_kv2_fixture(root: &Path, key: &[u8], value: &[u8]) {
    let mut engine = KV2::open_with_defaults(root).await.unwrap();
    engine.put(key.to_vec(), value.to_vec()).await.unwrap();
    engine.shutdown().await.unwrap();
}

async fn create_experimental_fixture(root: &Path, key: &[u8], value: &[u8]) {
    let mut engine = ExperimentalEngine::open(root).await.unwrap();
    engine.put(key.to_vec(), value.to_vec()).await.unwrap();
    engine.shutdown().await.unwrap();
}

async fn create_experimental_sstable_fixture(root: &Path, key: &[u8], value: &[u8]) {
    let mut engine = ExperimentalEngine::open(root).await.unwrap();
    engine.put(key.to_vec(), value.to_vec()).await.unwrap();
    engine.clean().await.unwrap();
    engine.shutdown().await.unwrap();
}

fn active_generation_dir(root: &Path) -> PathBuf {
    let mut generations = fs::read_dir(root.join("generations"))
        .unwrap()
        .map(|entry| entry.unwrap().path());
    let generation = generations.next().unwrap();
    assert!(generations.next().is_none());
    generation
}

fn create_fifo(path: &Path) {
    let status = Command::new("mkfifo").arg(path).status().unwrap();
    assert!(status.success());
}

fn assert_cli_refused(root: &Path) {
    let output = run_cli(root, "SET should-not-exist value\nEXIT\n");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("Failed to select standalone storage engine")
            || stderr.contains("Failed to initialize ExperimentalEngine"),
        "expected storage refusal, got: {stderr}"
    );
    assert!(!String::from_utf8_lossy(&output.stdout).contains("CLI REPL"));
}

fn assert_refused_unchanged(root: &Path) {
    let before = snapshot_tree(root);
    assert_cli_refused(root);
    assert_eq!(snapshot_tree(root), before);
}

#[tokio::test]
async fn new_standalone_cli_database_supports_kv_and_unicode_fts_across_processes() {
    let parent = TempDir::new().unwrap();
    let data_dir = parent.path().join("new-database");
    let value = "value with spaces 世界";
    let document = "Café 東京 quoted text";

    let output = successful_cli(
        &data_dir,
        &format!(
            "SET raw-key \"{value}\"\nGET raw-key\nDOC PUT doc-1 \"{document}\"\nFTS RANK OR \"東京\"\nEXIT\n"
        ),
    );
    assert!(output.contains(value));
    assert!(output.contains(&format!("\tdoc-1\t{}", escaped(document))));
    assert!(data_dir.join("EXPERIMENTAL-WAL-VALUES").is_file());
    assert!(data_dir.join("CURRENT").is_file());
    assert!(
        data_dir
            .join("generations/00000000000000000001/wal")
            .is_dir()
    );
    assert!(
        data_dir
            .join("generations/00000000000000000001/index/MANIFEST")
            .is_file()
    );
    assert!(!data_dir.join("MANIFEST").exists());

    let reopened = successful_cli(
        &data_dir,
        "GET raw-key\nDOC GET doc-1\nFTS RANK OR \"東京\"\nEXIT\n",
    );
    assert!(reopened.contains(value));
    assert!(reopened.contains(&escaped(document)));
    assert!(reopened.contains(&format!("\tdoc-1\t{}", escaped(document))));
}

#[tokio::test]
async fn data_directory_symlink_remains_supported() {
    let parent = TempDir::new().unwrap();
    let real_root = parent.path().join("real-root");
    let linked_root = parent.path().join("linked-root");
    fs::create_dir(&real_root).unwrap();
    symlink(&real_root, &linked_root).unwrap();
    create_experimental_fixture(&linked_root, b"linked-key", b"linked value").await;

    let output = successful_cli(&linked_root, "GET linked-key\nEXIT\n");
    assert!(output.contains("linked value"));
}

#[tokio::test]
async fn flushed_experimental_store_reopens_through_real_and_aliased_roots() {
    for create_through_alias in [false, true] {
        let parent = TempDir::new().unwrap();
        let real_root = parent.path().join("real-root");
        let linked_root = parent.path().join("linked-root");
        fs::create_dir(&real_root).unwrap();
        symlink(&real_root, &linked_root).unwrap();
        let initial_root = if create_through_alias {
            &linked_root
        } else {
            &real_root
        };
        create_experimental_sstable_fixture(initial_root, b"persisted-key", b"persisted value")
            .await;

        for reopen_root in [&linked_root, &real_root] {
            let output = successful_cli(reopen_root, "GET persisted-key\nEXIT\n");
            assert!(output.contains("persisted value"));
            let mut engine = ExperimentalEngine::open(reopen_root).await.unwrap();
            assert_eq!(
                engine
                    .get(b"persisted-key")
                    .await
                    .unwrap()
                    .unwrap()
                    .as_slice(),
                b"persisted value"
            );
            engine.shutdown().await.unwrap();
        }
    }
}

#[tokio::test]
async fn noncanonical_wal_names_refuse_before_bootstrap_mutation() {
    for name in ["0.log", "+0.log", "000.log"] {
        let root = TempDir::new().unwrap();
        create_experimental_fixture(root.path(), b"key", b"value").await;
        fs::remove_file(root.path().join("EXPERIMENTAL-WAL-VALUES")).unwrap();
        fs::remove_file(root.path().join("LOCK")).unwrap();
        let wal_dir = active_generation_dir(root.path()).join("wal");
        let original = wal_dir.join("00000000000000000000.log");
        let renamed = wal_dir.join(name);
        fs::rename(&original, &renamed).unwrap();
        assert_refused_unchanged(root.path());
        let before = snapshot_tree(root.path());
        assert!(ExperimentalEngine::open(root.path()).await.is_err());
        assert_eq!(snapshot_tree(root.path()), before);
        fs::rename(&renamed, &original).unwrap();
        let output = successful_cli(root.path(), "GET key\nEXIT\n");
        assert!(output.contains("value"));
    }
}

#[tokio::test]
async fn nested_sstable_directory_symlink_remains_refused() {
    let root = TempDir::new().unwrap();
    let external = TempDir::new().unwrap();
    create_experimental_sstable_fixture(root.path(), b"key", b"value").await;
    let level = active_generation_dir(root.path()).join("index/sstable/level-0");
    let target = external.path().join("tables");
    fs::rename(&level, &target).unwrap();
    symlink(&target, &level).unwrap();
    let external_before = snapshot_tree(external.path());
    assert_refused_unchanged(root.path());
    assert_eq!(snapshot_tree(external.path()), external_before);
}

#[tokio::test]
async fn existing_kv2_cli_database_preserves_format_and_records() {
    let directory = TempDir::new().unwrap();
    create_kv2_fixture(directory.path(), b"legacy-key", b"original value").await;

    let output = successful_cli(
        directory.path(),
        "GET legacy-key\nSET legacy-key \"updated value\"\nGET legacy-key\nEXIT\n",
    );
    assert!(output.contains("original value"));
    assert!(output.contains("updated value"));
    assert!(directory.path().join("MANIFEST").is_file());
    assert!(!directory.path().join("EXPERIMENTAL-WAL-VALUES").exists());

    let mut engine = KV2::open_with_defaults(directory.path()).await.unwrap();
    assert_eq!(
        engine.get(b"legacy-key").await.unwrap().unwrap().as_slice(),
        b"updated value"
    );
    engine.shutdown().await.unwrap();
}

#[tokio::test]
async fn existing_experimental_database_reopens_through_new_cli_default() {
    let directory = TempDir::new().unwrap();
    create_experimental_fixture(directory.path(), b"old-key", b"old value").await;

    let output = successful_cli(
        directory.path(),
        "GET old-key\nSET new-key \"new value\"\nEXIT\n",
    );
    assert!(output.contains("old value"));
    assert!(directory.path().join("EXPERIMENTAL-WAL-VALUES").is_file());
    assert!(!directory.path().join("MANIFEST").exists());

    let mut engine = ExperimentalEngine::open(directory.path()).await.unwrap();
    assert_eq!(
        engine.get(b"old-key").await.unwrap().unwrap().as_slice(),
        b"old value"
    );
    assert_eq!(
        engine.get(b"new-key").await.unwrap().unwrap().as_slice(),
        b"new value"
    );
    engine.shutdown().await.unwrap();
}

#[tokio::test]
async fn cli_rejects_marker_temp_symlink_without_changing_external_target() {
    let root = TempDir::new().unwrap();
    let external = TempDir::new().unwrap();
    let sentinel = external.path().join("sentinel");
    fs::write(&sentinel, b"external sentinel bytes").unwrap();
    create_experimental_fixture(root.path(), b"key", b"value").await;
    fs::remove_file(root.path().join("EXPERIMENTAL-WAL-VALUES")).unwrap();
    symlink(&sentinel, root.path().join("EXPERIMENTAL-WAL-VALUES.tmp")).unwrap();

    let before = snapshot_tree(root.path());
    assert_cli_refused(root.path());
    assert_eq!(snapshot_tree(root.path()), before);
    assert_eq!(fs::read(sentinel).unwrap(), b"external sentinel bytes");
}

#[tokio::test]
async fn cli_rejects_unsafe_file_kinds_without_blocking_or_following_links() {
    let fifo_root = TempDir::new().unwrap();
    create_fifo(&fifo_root.path().join("CURRENT"));
    assert_refused_unchanged(fifo_root.path());

    let dangling_link_root = TempDir::new().unwrap();
    symlink("missing-target", dangling_link_root.path().join("CURRENT")).unwrap();
    assert_refused_unchanged(dangling_link_root.path());

    let manifest_fifo_root = TempDir::new().unwrap();
    create_experimental_fixture(manifest_fifo_root.path(), b"key", b"value").await;
    let manifest = active_generation_dir(manifest_fifo_root.path()).join("index/MANIFEST");
    fs::remove_file(&manifest).unwrap();
    create_fifo(&manifest);
    assert_refused_unchanged(manifest_fifo_root.path());

    let index_temp_root = TempDir::new().unwrap();
    let external_temp = TempDir::new().unwrap();
    let temp_sentinel = external_temp.path().join("sentinel");
    fs::write(&temp_sentinel, b"keep index temp target").unwrap();
    create_experimental_fixture(index_temp_root.path(), b"key", b"value").await;
    symlink(
        &temp_sentinel,
        active_generation_dir(index_temp_root.path()).join("index/EXPERIMENTAL-WAL-VALUES.tmp"),
    )
    .unwrap();
    assert_refused_unchanged(index_temp_root.path());
    assert_eq!(fs::read(temp_sentinel).unwrap(), b"keep index temp target");

    let wal_link_root = TempDir::new().unwrap();
    let external_wal = TempDir::new().unwrap();
    let wal_sentinel = external_wal.path().join("sentinel");
    fs::write(&wal_sentinel, b"keep WAL target").unwrap();
    create_experimental_fixture(wal_link_root.path(), b"key", b"value").await;
    let wal_dir = active_generation_dir(wal_link_root.path()).join("wal");
    let wal_segment = fs::read_dir(&wal_dir)
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    fs::remove_file(&wal_segment).unwrap();
    symlink(&wal_sentinel, &wal_segment).unwrap();
    assert_refused_unchanged(wal_link_root.path());
    assert_eq!(fs::read(wal_sentinel).unwrap(), b"keep WAL target");

    let lock_root = TempDir::new().unwrap();
    let external = TempDir::new().unwrap();
    let sentinel = external.path().join("sentinel");
    fs::write(&sentinel, b"keep lock target").unwrap();
    create_experimental_fixture(lock_root.path(), b"key", b"value").await;
    fs::remove_file(lock_root.path().join("LOCK")).unwrap();
    symlink(&sentinel, lock_root.path().join("LOCK")).unwrap();
    assert_refused_unchanged(lock_root.path());
    assert_eq!(fs::read(sentinel).unwrap(), b"keep lock target");
}

#[tokio::test]
async fn direct_experimental_open_rejects_lock_symlink_without_following_it() {
    let root = TempDir::new().unwrap();
    let external = TempDir::new().unwrap();
    let sentinel = external.path().join("sentinel");
    fs::write(&sentinel, b"keep direct-open target").unwrap();
    create_experimental_fixture(root.path(), b"key", b"value").await;
    fs::remove_file(root.path().join("LOCK")).unwrap();
    symlink(&sentinel, root.path().join("LOCK")).unwrap();

    let before = snapshot_tree(root.path());
    assert!(ExperimentalEngine::open(root.path()).await.is_err());
    assert_eq!(snapshot_tree(root.path()), before);
    assert_eq!(fs::read(sentinel).unwrap(), b"keep direct-open target");
}

#[tokio::test]
async fn incomplete_current_bootstrap_recovers_without_falling_back_to_kv2() {
    let directory = TempDir::new().unwrap();
    create_experimental_fixture(directory.path(), b"recovered-key", b"value").await;
    fs::remove_file(directory.path().join("EXPERIMENTAL-WAL-VALUES")).unwrap();

    let output = successful_cli(directory.path(), "GET recovered-key\nEXIT\n");
    assert!(output.contains("value"));
    assert!(directory.path().join("EXPERIMENTAL-WAL-VALUES").is_file());
    assert!(!directory.path().join("MANIFEST").exists());
}

#[tokio::test]
async fn markerless_current_recovery_repairs_torn_wal_tail_after_preflight() {
    let root = TempDir::new().unwrap();
    create_experimental_fixture(root.path(), b"key", b"value").await;
    fs::remove_file(root.path().join("EXPERIMENTAL-WAL-VALUES")).unwrap();
    let wal_dir = active_generation_dir(root.path()).join("wal");
    let segment = fs::read_dir(&wal_dir)
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    let original = fs::read(&segment).unwrap();
    let torn_tail = [0xf1, 0xe2, 0xd3];
    fs::OpenOptions::new()
        .append(true)
        .open(&segment)
        .unwrap()
        .write_all(&torn_tail)
        .unwrap();

    let output = successful_cli(root.path(), "GET key\nEXIT\n");
    assert!(output.contains("value"));
    let recovered = fs::read(segment).unwrap();
    assert!(recovered.starts_with(&original));
    assert!(
        !recovered
            .windows(torn_tail.len())
            .any(|window| window == torn_tail)
    );
}

#[tokio::test]
async fn incomplete_current_preflight_refuses_corruption_without_mutation() {
    let bad_manifest = TempDir::new().unwrap();
    create_experimental_fixture(bad_manifest.path(), b"key", b"value").await;
    fs::remove_file(bad_manifest.path().join("EXPERIMENTAL-WAL-VALUES")).unwrap();
    fs::remove_file(bad_manifest.path().join("LOCK")).unwrap();
    fs::write(
        active_generation_dir(bad_manifest.path()).join("index/MANIFEST"),
        b"\x01",
    )
    .unwrap();
    assert_refused_unchanged(bad_manifest.path());

    let bad_index_marker = TempDir::new().unwrap();
    create_experimental_fixture(bad_index_marker.path(), b"key", b"value").await;
    fs::remove_file(bad_index_marker.path().join("EXPERIMENTAL-WAL-VALUES")).unwrap();
    fs::remove_file(bad_index_marker.path().join("LOCK")).unwrap();
    fs::write(
        active_generation_dir(bad_index_marker.path()).join("index/EXPERIMENTAL-WAL-VALUES"),
        b"invalid marker",
    )
    .unwrap();
    assert_refused_unchanged(bad_index_marker.path());

    let bad_current = TempDir::new().unwrap();
    create_experimental_fixture(bad_current.path(), b"key", b"value").await;
    fs::remove_file(bad_current.path().join("EXPERIMENTAL-WAL-VALUES")).unwrap();
    fs::remove_file(bad_current.path().join("LOCK")).unwrap();
    let current_path = bad_current.path().join("CURRENT");
    let mut current = fs::read(&current_path).unwrap();
    *current.last_mut().unwrap() ^= 1;
    fs::write(current_path, current).unwrap();
    assert_refused_unchanged(bad_current.path());

    let missing_sstable = TempDir::new().unwrap();
    create_experimental_sstable_fixture(missing_sstable.path(), b"key", b"value").await;
    fs::remove_file(missing_sstable.path().join("EXPERIMENTAL-WAL-VALUES")).unwrap();
    fs::remove_file(missing_sstable.path().join("LOCK")).unwrap();
    let sstable_dir = active_generation_dir(missing_sstable.path()).join("index/sstable/level-0");
    let sstable = fs::read_dir(sstable_dir)
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    fs::remove_file(sstable).unwrap();
    assert_refused_unchanged(missing_sstable.path());
}

#[tokio::test]
async fn regular_stale_temporary_artifacts_remain_recoverable() {
    let root = TempDir::new().unwrap();
    create_experimental_fixture(root.path(), b"key", b"value").await;
    fs::remove_file(root.path().join("EXPERIMENTAL-WAL-VALUES")).unwrap();
    fs::write(root.path().join("EXPERIMENTAL-WAL-VALUES.tmp"), b"stale").unwrap();
    fs::write(root.path().join("CURRENT.tmp"), b"stale").unwrap();
    fs::write(root.path().join("NEXT-GENERATION.tmp"), b"stale").unwrap();
    let generation = active_generation_dir(root.path());
    fs::write(generation.join("SEAL.tmp"), b"stale").unwrap();
    fs::write(
        generation.join("index/EXPERIMENTAL-WAL-VALUES.tmp"),
        b"stale",
    )
    .unwrap();

    let output = successful_cli(root.path(), "GET key\nEXIT\n");
    assert!(output.contains("value"));
    assert!(root.path().join("EXPERIMENTAL-WAL-VALUES").is_file());
}

#[tokio::test]
async fn ambiguous_unknown_and_unrecoverable_roots_refuse_without_mutation() {
    let unknown = TempDir::new().unwrap();
    fs::write(unknown.path().join("notes.txt"), b"keep me").unwrap();
    assert_refused_unchanged(unknown.path());

    let lock_only = TempDir::new().unwrap();
    fs::write(lock_only.path().join("LOCK"), b"").unwrap();
    assert_refused_unchanged(lock_only.path());

    let temporary = TempDir::new().unwrap();
    fs::write(temporary.path().join("CURRENT.tmp"), b"partial").unwrap();
    assert_refused_unchanged(temporary.path());

    let marker_only = TempDir::new().unwrap();
    fs::write(
        marker_only.path().join("EXPERIMENTAL-WAL-VALUES"),
        b"partial",
    )
    .unwrap();
    assert_refused_unchanged(marker_only.path());

    let unrecoverable = TempDir::new().unwrap();
    create_experimental_fixture(unrecoverable.path(), b"key", b"value").await;
    fs::remove_file(unrecoverable.path().join("EXPERIMENTAL-WAL-VALUES")).unwrap();
    fs::remove_dir_all(unrecoverable.path().join("generations")).unwrap();
    assert_refused_unchanged(unrecoverable.path());

    let mixed = TempDir::new().unwrap();
    create_kv2_fixture(mixed.path(), b"normal-key", b"normal value").await;
    fs::write(mixed.path().join("CURRENT"), b"partial").unwrap();
    assert_refused_unchanged(mixed.path());
}

#[tokio::test]
async fn exit_and_eof_shutdown_release_experimental_root_lock() {
    let parent = TempDir::new().unwrap();
    for (name, input, key, value) in [
        (
            "exit",
            "SET exit-key exit-value\nEXIT\n",
            b"exit-key".as_slice(),
            b"exit-value".as_slice(),
        ),
        (
            "eof",
            "SET eof-key eof-value\n",
            b"eof-key".as_slice(),
            b"eof-value".as_slice(),
        ),
    ] {
        let data_dir = parent.path().join(name);
        successful_cli(&data_dir, input);

        let mut engine = ExperimentalEngine::open(&data_dir).await.unwrap();
        assert_eq!(engine.get(key).await.unwrap().unwrap().as_slice(), value);
        engine.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn fts_open_failure_does_not_fall_back_to_another_storage_format() {
    let directory = TempDir::new().unwrap();
    create_experimental_fixture(directory.path(), b"key", b"value").await;
    let mut index = FtsIndex::open(ExperimentalEngine::open(directory.path()).await.unwrap())
        .await
        .unwrap();
    index
        .put_document(DocumentId::new("doc").unwrap(), b"search text".to_vec())
        .await
        .unwrap();
    let mut engine = index.into_inner();
    engine.shutdown().await.unwrap();
    drop(engine);

    let mut engine = ExperimentalEngine::open(directory.path()).await.unwrap();
    engine
        .put(b"\0ceredb:fts:v1:m".to_vec(), b"invalid metadata".to_vec())
        .await
        .unwrap();
    engine.shutdown().await.unwrap();
    drop(engine);

    let before = snapshot_tree(directory.path());
    let output = run_cli(directory.path(), "SET should-not-exist value\nEXIT\n");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("Failed to initialize FTS index"),
        "unexpected CLI error: {stderr}"
    );
    assert!(!String::from_utf8_lossy(&output.stdout).contains("CLI REPL"));
    assert_eq!(snapshot_tree(directory.path()), before);
}
