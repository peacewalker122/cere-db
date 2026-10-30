use ceredb::repl::run_repl_async;
use ceredb::storage::experimental::{StorageKind, classify_storage_root};
use ceredb::storage::kv2::RaftKV2;
use ceredb::{Config, ExperimentalEngine, FtsIndex, KV2};
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
        info!("Raft mode: disabled (single-node storage mode)");

        let storage_kind = match classify_storage_root(&config.data_dir).await {
            Ok(kind) => kind,
            Err(err) => {
                eprintln!("Failed to select standalone storage engine: {err}");
                return;
            }
        };

        match storage_kind {
            StorageKind::Experimental => {
                let engine =
                    match ExperimentalEngine::open_with_config(&config.data_dir, storage_config)
                        .await
                    {
                        Ok(engine) => engine,
                        Err(err) => {
                            eprintln!("Failed to initialize ExperimentalEngine: {err}");
                            return;
                        }
                    };

                let mut index = match FtsIndex::open(engine).await {
                    Ok(index) => index,
                    Err(err) => {
                        eprintln!("Failed to initialize FTS index: {err}");
                        return;
                    }
                };

                let repl_result = run_repl_async(&mut index).await;
                let mut engine = index.into_inner();
                let shutdown_result = engine.shutdown().await;
                if let Err(err) = repl_result {
                    eprintln!("REPL error: {err}");
                }
                if let Err(err) = shutdown_result {
                    eprintln!("ExperimentalEngine shutdown error: {err}");
                }
            }
            StorageKind::Kv2 => {
                let engine = match KV2::open(&config.data_dir, storage_config).await {
                    Ok(engine) => engine,
                    Err(err) => {
                        eprintln!("Failed to initialize KV2 engine: {err}");
                        return;
                    }
                };

                let mut index = match FtsIndex::open(engine).await {
                    Ok(index) => index,
                    Err(err) => {
                        eprintln!("Failed to initialize FTS index: {err}");
                        return;
                    }
                };

                let repl_result = run_repl_async(&mut index).await;
                let mut engine = index.into_inner();
                let shutdown_result = engine.shutdown().await;
                if let Err(err) = repl_result {
                    eprintln!("REPL error: {err}");
                }
                if let Err(err) = shutdown_result {
                    eprintln!("KV2 shutdown error: {err}");
                }
            }
        }
    }
}
