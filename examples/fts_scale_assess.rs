use ceredb::{Bm25Config, FtsIndex, FtsQuery, KV2, StorageConfig};
use serde_json::json;
use std::{fs, path::PathBuf, time::Instant};

const MEMTABLE_BYTES: u64 = 16 * 1024 * 1024;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let db = PathBuf::from(
        args.next()
            .ok_or("usage: fts_scale_assess DB_ROOT STAGE [RESULT]")?,
    );
    let stage = args.next().ok_or("stage required")?;
    let output = args.next().map(PathBuf::from);
    let corpus_count = args
        .next()
        .map(|value| value.parse::<usize>())
        .transpose()?
        .unwrap_or(1_000_000);
    let config = StorageConfig::builder()
        .memtable_size_threshold(MEMTABLE_BYTES)
        .build()?;
    let open_started = Instant::now();
    let engine = KV2::open(&db, config).await?;
    let index = FtsIndex::open(engine).await?;
    let open_seconds = open_started.elapsed().as_secs_f64();
    let mut result = json!({"stage":stage,"status":"OPENED","db":db,"memtable_size_threshold_bytes":MEMTABLE_BYTES,"open_elapsed_seconds":open_seconds});
    if let Some(path) = &output {
        fs::write(path, serde_json::to_vec_pretty(&result)?)?;
    }
    if stage == "count" {
        let mut low = 0usize;
        let mut high = 1_000_000usize;
        while low < high {
            let mid = low + (high - low) / 2;
            let id = ceredb::DocumentId::new(format!("doc{mid:07}"))?;
            if index.get_document(&id).await?.is_some() {
                low = mid + 1;
            } else {
                high = mid;
            }
        }
        let count = low;
        let mut sample_ids = Vec::new();
        for n in [0, 1, count / 2, count.saturating_sub(2), count.saturating_sub(1), count] {
            if n < 1_000_000 {
                let id = ceredb::DocumentId::new(format!("doc{n:07}"))?;
                sample_ids.push(json!({"id":id.as_str(),"present":index.get_document(&id).await?.is_some()}));
            }
        }
        result["contiguous_id_count"] = json!(count);
        result["boundary_samples"] = json!(sample_ids);
        result["status"] = json!("COUNTED");
        let mut engine = index.into_inner();
        engine.shutdown().await?;
        if let Some(path) = output {
            fs::write(path, serde_json::to_vec_pretty(&result)?)?;
        } else {
            println!("{}", serde_json::to_string(&result)?);
        }
        return Ok(());
    }
    if stage != "open" {
        let (name, query, limit, expected) = match stage.as_str() {
            "unique" => (
                "unique1",
                FtsQuery::And(vec!["unique0000000".into()]),
                10,
                Some(1),
            ),
            "group" => (
                "group0007",
                FtsQuery::And(vec!["group0007".into()]),
                1000,
                Some(corpus_count.div_ceil(1000)),
            ),
            "common_unique" => (
                "common_and_unique",
                FtsQuery::And(vec!["common".into(), "unique0000000".into()]),
                10,
                Some(1),
            ),
            "or_groups" => (
                "or_groups",
                FtsQuery::Or(vec!["group0000".into(), "group0001".into()]),
                1000,
                Some(
                    (corpus_count / 1000 * 2
                        + usize::from(corpus_count % 1000 > 0)
                        + usize::from(corpus_count % 1000 > 1))
                    .min(1000),
                ),
            ),
            "common" => (
                "common",
                FtsQuery::And(vec!["common".into()]),
                100,
                Some(corpus_count.min(100)),
            ),
            "common10" => (
                "common_top10",
                FtsQuery::And(vec!["common".into()]),
                10,
                Some(corpus_count.min(10)),
            ),
            _ => return Err(format!("unknown stage {stage}").into()),
        };
        let query_started = Instant::now();
        let hits = index
            .search_ranked(query.clone(), limit, Bm25Config::default())
            .await?;
        let query_seconds = query_started.elapsed().as_secs_f64();
        let boolean_started = Instant::now();
        let boolean_hits = index.search(query, limit).await?;
        let boolean_seconds = boolean_started.elapsed().as_secs_f64();
        let sampled_ids: Vec<_> = hits
            .iter()
            .take(10)
            .map(|hit| hit.id.as_str().to_owned())
            .collect();
        let unique_ids = hits
            .iter()
            .map(|hit| hit.id.as_str())
            .collect::<std::collections::BTreeSet<_>>()
            .len()
            == hits.len();
        let finite_scores = hits.iter().all(|hit| hit.score.is_finite());
        let ranked_descending = hits.windows(2).all(|pair| pair[0].score >= pair[1].score);
        let correctness = expected.map(|count| hits.len() == count).unwrap_or(true)
            && boolean_hits.len() == hits.len()
            && unique_ids
            && finite_scores
            && ranked_descending;
        result["query"] = json!({"name":name,"elapsed_seconds":query_seconds,"returned":hits.len(),"limit":limit,"expected_returned_count":expected,"count_matches_expectation":correctness,"sampled_ids":sampled_ids,"unique_ids":unique_ids,"finite_scores":finite_scores,"ranked_descending":ranked_descending,"ranking":"public search_ranked with Bm25Config::default","boolean_elapsed_seconds":boolean_seconds,"boolean_returned":boolean_hits.len()});
        result["status"] = json!(if correctness {
            "COMPLETE"
        } else {
            "INCORRECT_RESULT"
        });
    }
    let mut engine = index.into_inner();
    engine.shutdown().await?;
    result["shutdown"] = json!("clean");
    if let Some(path) = output {
        fs::write(path, serde_json::to_vec_pretty(&result)?)?;
    } else {
        println!("{}", serde_json::to_string(&result)?);
    }
    Ok(())
}
