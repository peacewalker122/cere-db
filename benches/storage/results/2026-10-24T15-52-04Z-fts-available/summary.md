# Available-seed FTS assessment — 97,413 documents (not 1 million)

**Assessment complete.** This measures the data available in the in-progress seed, not the intended million-document target.

## Snapshot and provenance

- Captured at `2026-10-24T15:52:05Z` from `target/scale/2026-10-24T15-24-49Z-fts-1m/db`.
- Source seed PID `1891078` was identity-checked as `target/release/examples/fts_scale target/scale/2026-10-24T15-24-49Z-fts-1m`, SIGSTOPed only during the isolated copy, synced with `sync -f`, then copied with `cp -a --reflink=always`; it was resumed immediately. Pause: **458 ms**. Btrfs reflink support was confirmed and the destination filesystem had **307 GiB free**. The seed remained running afterward; its progress advanced from **97,000 acknowledged** at capture to **104,000 acknowledged** by the final check.
- The capture is a **crash/recovery filesystem snapshot, not an application checkpoint**. It was reopened only from the clone.
- Source HEAD: `51d2c8b33383df65c053a600df78d33662f2c483`. The pre-existing dirty worktree was preserved. SHA-256 fingerprints of its porcelain status and tracked diff, plus dirty paths, are in `summary.json`; the seed generator, assessor and seed-binary fingerprints are recorded there too.
- Snapshot count: **97,413** contiguous deterministic IDs (`doc0000000` through `doc0097412`). This is inferred by binary-searching public `FtsIndex::get_document` over the generator's ID domain. The logged source ACK at capture was a 97,000 lower bound; the clone contains 413 more IDs. Six public boundary/point lookups confirmed IDs 0, 1, midpoint 48,706, 97,411, 97,412, and absence of 97,413. This is not a whole-corpus audit.
- Corpus follows the generator: `common group{n%1000:04} unique{n:07} ` plus `variable ` repeated `1+n%4` times, UTF-8. `StorageConfig` memtable threshold: **16 MiB**. Ranking: public `FtsIndex::search_ranked` with `Bm25Config::default()`.
- Per-process address-space cap: **4 GiB**; reopen timeout: **180 s**; each query-process timeout: **60 s**. Stages ran serially against the copy. No timeout or failed stage.

## Disk and reopen

| Measure | Result |
|---|---:|
| Copy logical bytes | 180,780,399 (172.4 MiB) |
| Copy allocated bytes | 180,867,072 (172.5 MiB) |
| WAL | 125 files, 128,248,912 logical bytes |
| SSTables | 2 files, 52,530,680 logical bytes |
| Manifest | 1 file, 807 logical bytes |
| Public reopen | 2.272 s |
| Reopen process peak RSS | 311,092 KiB |

Disk numbers are per-file logical and allocated size from the cloned database. The copy was made with Btrfs reflinks.

## Query results

Fast query columns show the median of measured samples 2–4 (sample 1 was warmup). Boolean query latency is the paired public `FtsIndex::search` call, also measured inside each process. Each process separately reopens the index; those reopen times are recorded in raw JSON and are not included in the query latency columns.

| Stage | Ranked median | Boolean median | Expected / returned | Check |
|---|---:|---:|---:|---|
| Unique `unique0000000` | 15.47 ms | 14.61 ms | 1 / 1 | pass |
| Group `group0007` | 19.41 ms | 16.06 ms | 98 / 98 | pass |
| `common AND unique0000000` | 1.57 s | 1.48 s | 1 / 1 | pass |
| OR `group0000` / `group0001` | 33.98 ms | 34.80 ms | 196 / 196 | pass |
| Common top 10 | 2.65 s | 1.42 s | 10 / 10 | pass |
| Common top 100, bounded probe | 2.48 s | 1.53 s | 100 / 100 | pass |

For all completed ranked samples, the returned IDs were unique, scores finite, and scores non-increasing. Count expectations were computed from the **97,413-document snapshot**, and Boolean result counts matched the ranked counts.

## Reproduction and raw evidence

Assessor build: `PATH="/home/peacewalker/.cargo/bin:$PATH" cargo build --release --example fts_scale_assess`.

Count check: `timeout --signal=TERM --kill-after=5s 180s prlimit --as=4294967296 -- target/release/examples/fts_scale_assess target/scale/2026-10-24T15-52-04Z-fts-available/db count benches/storage/results/2026-10-24T15-52-04Z-fts-available/count.json`.

Query command form (one isolated process per sample): `timeout --signal=TERM --kill-after=5s 60s prlimit --as=4294967296 -- target/release/examples/fts_scale_assess target/scale/2026-10-24T15-52-04Z-fts-available/db STAGE benches/storage/results/2026-10-24T15-52-04Z-fts-available/STAGE-N.json 97413`, wrapped in `/usr/bin/time -v`.

Raw JSON, per-run GNU time output, stdout, stderr, status, capture metadata, and fingerprints are in this results directory. The captured database is [target/scale/2026-10-24T15-52-04Z-fts-available/db](../../../../target/scale/2026-10-24T15-52-04Z-fts-available/db). The existing million-seed pipeline and its assessment worker were left intact.
