//! Test utilities for wasm-kv.
//!
//! This module provides temp directory management for tests.

use std::path::PathBuf;

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
