#!/usr/bin/env bash
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"
{ find src tests -type f -name '*.rs' -print
  printf '%s\n' examples/experimental_compare.rs docs/experimental-wal-storage.md Cargo.toml Cargo.lock
} | LC_ALL=C sort | xargs sha256sum
