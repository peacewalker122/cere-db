# CereDB

CereDB is an experimental embedded key-value store written in Rust. Single-node mode persists data with a write-ahead log (WAL) and SSTables. Optional Raft mode adds multi-node replication. The project includes a command-line REPL for trying the store.

## Install and run

Install a Rust toolchain with Rust 2024 edition support, then run:

```bash
git clone https://github.com/peacewalker122/cere-db.git
cd cere-db
cargo run -- --data-dir ./data
```

Cargo builds the project and starts the REPL. The `./data` directory stores database files.

## Use

Enter commands at the prompt:

```text
SET greeting "hello world"
GET greeting
DELETE greeting
EXIT
```

Raft mode is available with `--raft-mode`. See `cargo run -- --help` for CLI options. Raft support is experimental and not production-hardened.
