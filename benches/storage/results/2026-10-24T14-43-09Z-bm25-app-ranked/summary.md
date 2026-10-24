# Application-layer BM25 ranked FTS benchmark

## Workload and provenance

- Scenarios: `app/fts_ranked_and_search` and `app/fts_ranked_or_search`.
- Each scenario has 3 measured samples and 1 discarded warmup. Each measurement
  is one parsed and executed CLI command; command execution returns the actual
  `RankedSearch` response with the CLI's fixed 100-result limit.
- Focused release commands (run separately so each slash-qualified filter emits
  only its requested scenario):

  ```sh
  CARGO_TARGET_DIR=/tmp/wasm-kv-ranked-target PATH=/home/peacewalker/.cargo/bin:$PATH \
    cargo bench --bench storage -- --run --suite e2e \
    --filter app/fts_ranked_and_search --records 128 --value-bytes 256 \
    --flush-bytes 512 --repetitions 3 --warmup-runs 1 \
    --data-dir .tmp/ranked-focused-data \
    --output .tmp/ranked-focused-and.json
  CARGO_TARGET_DIR=/tmp/wasm-kv-ranked-target PATH=/home/peacewalker/.cargo/bin:$PATH \
    cargo bench --bench storage -- --run --suite e2e \
    --filter app/fts_ranked_or_search --records 128 --value-bytes 256 \
    --flush-bytes 512 --repetitions 3 --warmup-runs 1 \
    --data-dir .tmp/ranked-focused-data \
    --output .tmp/ranked-focused-or.json
  ```

- Configuration: 128 documents, UTF-8 text corpus matching the corresponding
  unranked app AND/OR cases, query text `common rareterm`, 100-result CLI limit,
  256 configured value bytes, and 512-byte flush threshold. Query setup,
  verification, and cleanup are outside the timed region.
- Correctness checks passed: the app command parser returns a `RankedSearch`
  variant; the executor returns `RankedSearchResults`; AND has exactly one hit
  and OR has the full 100-hit response; all scores are finite and positive;
  results are score-descending with ascending document IDs for ties; repeated
  execution produces exactly the same results and scores.
- Source HEAD: `51d2c8b33383df65c053a600df78d33662f2c483`, with dirty worktree.
  Raw JSON includes tracked changes and source qualification; the revision
  alone does not identify the complete build input. Build used a separate
  `/tmp` target directory so the concurrently running full benchmark's binary
  and artifacts were not overwritten. OS page cache retained; no cold-cache
  claim.
- Raw JSON: [raw.json](raw.json). It combines the two focused release runs and
  includes both command argument vectors, metadata, summaries, and all six
  measured samples.

## Results

Run times include command parsing, dispatch, ranked search, and response
construction. They are not individual-request percentile latency.

| Scenario | Samples | Median run | Range | Operations/s |
| --- | ---: | ---: | ---: | ---: |
| `app/fts_ranked_and_search` | 3 | 0.879 ms | 0.817–1.064 ms | 1,086.8 |
| `app/fts_ranked_or_search` | 3 | 1.769 ms | 1.339–2.241 ms | 560.8 |

The bounded 32-record, 512-byte-flush smoke runs also passed for each exact
scenario filter (one measured sample, no warmup). Their results are scratch
checks, not part of this 128-record artifact.

The concurrent full-suite BM25 archive is
[2026-10-24T14-35-00Z-bm25-naive/raw.json](../2026-10-24T14-35-00Z-bm25-naive/raw.json).
It is referenced without modification; its workload, measurements, and
qualification are distinct from this app-only focused run.
