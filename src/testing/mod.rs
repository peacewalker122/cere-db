//! Test utilities for wasm-kv.
//!
//! This module provides temp directory management and SSTable file counting
//! helpers for integration tests.

use std::path::{Path, PathBuf};

/// Creates a new temp directory for testing with automatic cleanup.
///
/// This provides a unique directory for each call.
#[cfg(test)]
pub fn with_temp_dir<T>(test_name: &str, f: impl FnOnce(PathBuf) -> T) -> T {
    let temp_dir = tempfile::Builder::new()
        .prefix(&format!("wasm-kv-{}", test_name))
        .tempdir()
        .expect("Failed to create temp directory");

    let path = temp_dir.into_path();
    let result = f(path.clone());

    // Cleanup is automatic via TempDir drop, but we ensure it's removed
    std::fs::remove_dir_all(&path).ok();

    result
}

/// Non-test placeholder - actual implementation is in #[cfg(test)] block
#[cfg(not(test))]
pub fn with_temp_dir<T>(_test_name: &str, _f: impl FnOnce(PathBuf) -> T) -> T {
    panic!("with_temp_dir is only available in test mode");
}

/// Count SSTable files in a specific level directory.
///
/// Returns 0 if the level directory doesn't exist.
pub fn count_sstable_files(base_dir: &Path, level: u32) -> usize {
    let level_dir = base_dir.join(format!("sstable/level-{}", level));
    std::fs::read_dir(&level_dir)
        .ok()
        .map(|entries| {
            entries
                .filter_map(|e| e.ok())
                .filter(|e| e.path().extension().is_some_and(|ext| ext == "dat"))
                .count()
        })
        .unwrap_or(0)
}

/// Count total SSTable files across all level directories.
pub fn total_sstable_files(base_dir: &Path) -> usize {
    let sstable_dir = base_dir.join("sstable");
    std::fs::read_dir(&sstable_dir)
        .ok()
        .map(|entries| {
            entries
                .filter_map(|e| e.ok())
                .filter(|e| e.path().is_dir())
                .map(|e| count_sstable_files(base_dir, level_from_dir(&e.path())))
                .sum()
        })
        .unwrap_or(0)
}

/// List all SSTable file paths across all level directories.
pub fn list_sstable_files(base_dir: &Path) -> Vec<PathBuf> {
    let sstable_dir = base_dir.join("sstable");
    std::fs::read_dir(&sstable_dir)
        .ok()
        .map(|entries| {
            entries
                .filter_map(|e| e.ok())
                .filter(|e| e.path().is_dir())
                .flat_map(|e| {
                    let level = level_from_dir(&e.path());
                    let level_dir = base_dir.join(format!("sstable/level-{}", level));
                    std::fs::read_dir(&level_dir)
                        .ok()
                        .into_iter()
                        .flatten()
                        .filter_map(|entry| entry.ok())
                        .filter(|entry| entry.path().extension().is_some_and(|ext| ext == "dat"))
                        .map(|entry| entry.path())
                        .collect::<Vec<_>>()
                })
                .collect()
        })
        .unwrap_or_default()
}

/// Extract level number from a directory name like "level-0", "level-1", etc.
fn level_from_dir(dir: &Path) -> u32 {
    dir.file_name()
        .and_then(|name| name.to_str())
        .and_then(|name| name.strip_prefix("level-"))
        .and_then(|num| num.parse().ok())
        .unwrap_or(0)
}
