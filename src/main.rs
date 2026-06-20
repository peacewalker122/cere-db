use ceredb::repl::run_repl_async;
use ceredb::{Config, KV2};
use ceredb::storage::kv2::RaftKV2;
use clap::Parser;
use log::info;

#[tokio::main]
async fn main() {
    // Parse command line arguments
    let config = Config::parse();

    // Initialize logger
    config.init_logger();

    info!("Starting wasm-kv key-value store CLI");
    info!("Data directory: {}", config.data_dir);
    info!("Log level: {:?}", config.get_log_level());

    // Build storage configuration from CLI args
    let storage_config = match config.to_storage_config() {
        Ok(cfg) => cfg,
        Err(err) => {
            eprintln!("Invalid configuration: {err}");
            return;
        }
    };

    info!("Memtable size threshold: {} MB", config.memtable_size_mb);
    info!("SSTable block size: {} KB", config.block_size_kb);
    info!("Max Level-0 files: {}", config.max_level0_files);

    if config.raft_mode {
        info!("Raft mode: enabled");
        let raft_config = config.to_raft_node_config().expect("raft config should be valid");
        info!("Raft node ID: {}", raft_config.node_id);
        info!("Raft peers: {:?}", raft_config.peers);
        info!("Raft HTTP bind: {}", raft_config.http_bind);

        let mut kv = match RaftKV2::open(&config.data_dir, storage_config, raft_config).await {
            Ok(engine) => engine,
            Err(err) => {
                eprintln!("Failed to initialize RaftKV2 engine: {err}");
                return;
            }
        };

        if let Err(err) = run_repl_async(&mut kv).await {
            eprintln!("REPL error: {err}");
        }

        kv.raft_layer().shutdown().await;
    } else {
        info!("Raft mode: disabled (single-node WAL mode)");

        let mut kv = match KV2::open(&config.data_dir, storage_config).await {
            Ok(engine) => engine,
            Err(err) => {
                eprintln!("Failed to initialize KV2 engine: {err}");
                return;
            }
        };

        if let Err(err) = run_repl_async(&mut kv).await {
            eprintln!("REPL error: {err}");
        }
    }
}
