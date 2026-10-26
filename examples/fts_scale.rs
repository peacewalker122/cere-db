use ceredb::{DocumentId, FtsIndex, FtsQuery, KV2, StorageConfig};
use serde_json::json;
use std::{
    fs::{self, OpenOptions},
    io::{BufWriter, Write},
    path::PathBuf,
    time::Instant,
};

const TARGET: u64 = 1_000_000;
const FORMAT_VERSION: u32 = 1;
const MEMTABLE_BYTES: u64 = 16 * 1024 * 1024;

fn document(n: u64) -> Vec<u8> {
    let group = n % 1000;
    let repetitions = 1 + n % 4;
    format!(
        "common group{group:04} unique{n:07} {}",
        "variable ".repeat(repetitions as usize)
    )
    .into_bytes()
}

fn nonempty(path: &std::path::Path) -> bool {
    path.exists()
        && fs::read_dir(path)
            .map(|mut entries| entries.next().is_some())
            .unwrap_or(true)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let root = PathBuf::from(
        args.next()
            .ok_or("usage: fts_scale RUN_ROOT [--pilot COUNT]")?,
    );
    let pilot = match (args.next().as_deref(), args.next()) {
        (None, None) => None,
        (Some("--pilot"), Some(count)) => Some(count.parse::<u64>()?),
        _ => return Err("usage: fts_scale RUN_ROOT [--pilot COUNT]".into()),
    };
    let count = pilot.unwrap_or(TARGET);
    if count == 0 || count > TARGET {
        return Err("count must be in 1..=1,000,000".into());
    }
    fs::create_dir_all(&root)?;
    let db = root.join("db");
    if nonempty(&db) {
        return Err(format!(
            "refusing nonempty database {}; this runner does not resume",
            db.display()
        )
        .into());
    }
    let config = StorageConfig::builder()
        .memtable_size_threshold(MEMTABLE_BYTES)
        .build()?;
    let wal_segment_size = config.wal_segment_size;
    let metadata = json!({"status":"IN_PROGRESS","generator_format_version":FORMAT_VERSION,"target_documents":count,"document_id_format":"doc{n:07}, n starts at zero","document_text_format":"UTF-8: common, group{n%1000:04}, unique{n:07}, variable repeated 1+n%4 times","memtable_size_threshold_bytes":MEMTABLE_BYTES,"wal_segment_size_bytes":wal_segment_size,"db_path":db.display().to_string(),"resumable":false,"acknowledgement":"successful public put_document call; not an independent full-index count proof"});
    fs::write(
        root.join("metadata.json"),
        serde_json::to_vec_pretty(&metadata)?,
    )?;
    let log = OpenOptions::new()
        .create_new(true)
        .append(true)
        .open(root.join("progress.jsonl"))?;
    let mut log = BufWriter::new(log);
    let engine = KV2::open(&db, config).await?;
    let mut index = FtsIndex::open(engine).await?;
    let started = Instant::now();
    let mut window = Instant::now();
    let mut window_bytes = 0u64;
    let mut window_docs = 0u64;
    for n in 0..count {
        let id = DocumentId::new(format!("doc{n:07}"))?;
        let text = document(n);
        let bytes = text.len() as u64;
        index.put_document(id, text).await?;
        window_bytes += bytes;
        window_docs += 1;
        let acknowledged = n + 1;
        if acknowledged == 1 || acknowledged % 1000 == 0 || acknowledged == count {
            let elapsed = started.elapsed().as_secs_f64();
            let window_seconds = window.elapsed().as_secs_f64();
            let entry = json!({"acknowledged_seed_writes":acknowledged,"elapsed_seconds":elapsed,"window_seconds":window_seconds,"window_docs_per_second":if window_seconds > 0.0 {window_docs as f64/window_seconds} else {0.0},"window_input_bytes":window_bytes});
            writeln!(log, "{}", entry)?;
            log.flush()?;
            window = Instant::now();
            window_bytes = 0;
            window_docs = 0;
        }
    }
    if pilot.is_some() {
        let first_id = DocumentId::new("doc0000000")?;
        if index.get_document(&first_id).await? != Some(document(0)) {
            return Err("pilot get verification failed".into());
        }
        let common = index
            .search(FtsQuery::And(vec!["common".into()]), count as usize)
            .await?;
        let expected_group = (count + 999) / 1000;
        let group = index
            .search(FtsQuery::And(vec!["group0000".into()]), 1000)
            .await?;
        let unique = index
            .search(FtsQuery::And(vec!["unique0000000".into()]), 10)
            .await?;
        if common.len() != count as usize
            || group.len() != expected_group as usize
            || unique.len() != 1
        {
            return Err(format!(
                "pilot search verification failed: common={}, group={}, unique={}",
                common.len(),
                group.len(),
                unique.len()
            )
            .into());
        }
    }
    let mut engine = index.into_inner();
    engine.shutdown().await?;
    let elapsed = started.elapsed().as_secs_f64();
    let disk_bytes = walk_bytes(&db)?;
    fs::write(
        root.join("metadata.json"),
        serde_json::to_vec_pretty(
            &json!({"status":"COMPLETE","generator_format_version":FORMAT_VERSION,"target_documents":count,"document_id_format":"doc{n:07}, n starts at zero","document_text_format":"UTF-8: common, group{n%1000:04}, unique{n:07}, variable repeated 1+n%4 times","memtable_size_threshold_bytes":MEMTABLE_BYTES,"wal_segment_size_bytes":wal_segment_size,"db_path":db.display().to_string(),"resumable":false,"acknowledgement":"successful public put_document call; not an independent full-index count proof"}),
        )?,
    )?;
    fs::write(
        root.join("completion.json"),
        serde_json::to_vec_pretty(
            &json!({"status":"COMPLETE","acknowledged_seed_writes":count,"elapsed_seconds":elapsed,"average_docs_per_second":count as f64/elapsed,"db_disk_bytes":disk_bytes,"shutdown":"clean"}),
        )?,
    )?;
    Ok(())
}

fn walk_bytes(path: &std::path::Path) -> std::io::Result<u64> {
    let mut total = 0;
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let metadata = entry.metadata()?;
        total += if metadata.is_dir() {
            walk_bytes(&entry.path())?
        } else {
            metadata.len()
        };
    }
    Ok(total)
}
