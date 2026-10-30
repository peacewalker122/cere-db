# Matched KV2 vs ExperimentalEngine (WAL-as-storage) — default small-tables suite

Run window (UTC): 2026-10-29T16:27:24Z .. 2026-10-29T16:53:47Z (1583 s)
Harness: `examples/experimental_compare.rs`, schema `ceredb-experimental-wal-comparison/2`
Contract: `docs/experimental-wal-storage.md`
Both engines: same checkout, same `StorageConfig`, same seed, same mutation order.

This is a measured result for these six matched cases on this machine. It is not a general
speedup, capacity, or production-safety claim. Independent review verified the measured
artifacts; the reporting corrections from that review are incorporated below. No performance
rerun was requested. The measured build remains an uncommitted worktree snapshot.

## Provenance

| item | value |
| --- | --- |
| `git rev-parse HEAD` | `fa65cde5d1642a54a576b89f0f2c88dba8a13fb5` |
| working tree | dirty, 12 tracked modifications, 4 relevant untracked source files (`examples/experimental_compare.rs`, `src/storage/experimental.rs`, `tests/integration/experimental.rs`, `docs/experimental-wal-storage.md` in the hashed set) |
| tracked diff sha256 | `8558f33003f1f15f6e595edc194b824de97aa19025d1db3394793846ce37b6dc` |
| source manifest | 61 paths (all `src/**/*.rs`, `tests/**/*.rs`, harness, contract doc, `Cargo.toml`, `Cargo.lock`) |
| source manifest sha256 | `e2d2cbc848183b2848e963f991d2357806d6d133c692d04f61c6138f90aa5951` |
| source stability | identical before build, after build, before measured, after final (`SOURCE_STABLE=yes`) |
| binary | `target/release/examples/experimental_compare`, `21684cbf8b48af86f3847cd713bcdacbe976981bcca06774231dc588447398a2` |
| binary stability | identical pre-pilot, pre-measured, post-final |
| rustc / cargo | 1.97.1 (8bab26f4f 2026-07-14) / 1.97.1 (c980f4866 2026-06-30) |
| host | 11th Gen Intel i5-1135G7, 8 logical CPUs, 16,075,916 kB RAM, kernel 7.1.13-200.fc44.x86_64 |
| dataset root | `/home/peacewalker/project/learn/wasm-kv/target/bench-data/experimental-wal-2026-10-29T16-26-04Z` |
| filesystem | mount `/home`, source `/dev/nvme0n1p3`, fstype `btrfs`, options `rw,relatime,seclabel,compress=zstd:1,ssd,discard=async,space_cache=v2` |
| mount check | before pilot, before measured, after final — identical; dataset is on persistent NVMe home, not `/tmp` |
| data retained | yes, under `target/` (189,182,153 B); **not** copied into this archive |

Only JSON, stdout/stderr, hashes, and commands are archived. No database directories are archived.

## Measured corpus

`--profile small-tables --repetitions 3 --warmups 1`, defaults for values/records/batches:

| case | records | batch sizes | mutations | batch calls | logical input |
| --- | --- | --- | --- | --- | --- |
| 64 B | 16384 | 1, 64 | 22937 | 22937 / 359 | 1,913,624 B |
| 4096 B | 512 | 1, 64 | 716 | 716 / 12 | 2,741,024 B |
| 65536 B | 64 | 1, 16 | 89 | 89 / 6 | 5,441,624 B |

6 cases × 2 engines × 3 measured samples = **36 measured samples**, plus **12 warmup executions**
(1 per case/batch/engine; confirmed as 12 on-disk warmup directories), plus one functional FTS case
of 128 documents in 2 `put_documents` calls of 64 per engine. Warmup and measured sample directories
are all distinct, freshly created, and were never reused.

Live set after mutations (identical for both engines and both orders):

| case | live records | live key+value bytes (space-amp denominator) | live value bytes |
| --- | --- | --- | --- |
| 64 B | 15565 | 1,369,720 B | 996,160 B |
| 4096 B | 486 | 2,002,320 B | 1,990,656 B |
| 65536 B | 61 | 3,999,160 B | 3,997,696 B |

## Results

All six cases are shown separately. Times are medians of three samples in milliseconds;
ranges are minimum to maximum where shown. Ratios divide engine medians before rounding.
Each engine has one discarded warmup per case. Separately calculated component medians
need not add up to the median of the per-sample phase sums.

The intentionally small-table profile uses a 1 MiB memtable threshold, 16 KiB target
SSTables and a 64 KiB L1 base; these are not production defaults. Equal configuration
does not imply equal flush or compaction work.

### Ingestion overview

| Value / batch | KV2 ingest ms (range) | Experimental ingest ms (range) | Experimental / KV2 time |
| --- | ---: | ---: | ---: |
| 64 B / 1 | 96,371.350 (85,732.597 to 115,553.482) | 122,029.362 (86,349.280 to 127,916.474) | 1.2662 |
| 64 B / 64 | 1,045.422 (987.481 to 1,103.459) | 1,125.128 (1,057.632 to 1,292.516) | 1.0762 |
| 4 KiB / 1 | 1,819.608 (1,805.716 to 1,882.851) | 1,879.217 (1,860.501 to 2,009.974) | 1.0328 |
| 4 KiB / 64 | 166.714 (152.631 to 185.841) | 89.955 (82.272 to 101.032) | 0.5396 |
| 64 KiB / 1 | 1,265.953 (1,221.446 to 1,283.169) | 699.252 (680.419 to 712.958) | 0.5524 |
| 64 KiB / 16 | 369.564 (357.501 to 391.219) | 78.430 (77.691 to 95.583) | 0.2122 |

A time ratio above 1 means experimental took longer; below 1 means it took less time.
For identical counts, the inverse is the observed rate ratio. These are descriptive
three-sample results, without confidence intervals.

### 64 B, batch 1

| Timed phase | KV2 ms | Experimental ms |
| --- | ---: | ---: |
| Ingest | 96,371.350 (85,732.597 to 115,553.482) | 122,029.362 (86,349.280 to 127,916.474) |
| Mutations | 46,745.046 (46,138.428 to 49,604.468) | 56,112.639 (52,751.673 to 56,873.897) |
| Pending-write barrier | 0.008 | 1.226 |
| Preclean shutdown | 0.082 | 14.789 |
| Preclean reopen | 90.807 (63.953 to 91.760) | 251.210 (195.272 to 260.561) |
| Clean pause (inside clean elapsed) | n/a | 5,539.179 |
| Clean elapsed | n/a | 5,559.936 |
| Postclean shutdown | n/a | 0.138 |
| Postclean reopen | n/a | 80.483 |
| Final shutdown | 0.084 | 0.197 |
| Lifecycle phase sum | 142,573.802 (135,429.029 to 162,389.528) | 184,465.795 (148,060.290 to 189,857.132) |
| Warm point gets (64 calls) | 6.644 | 10.367 |
| Warm scan (128 entries) | 0.366 | 20.519 |

| Closed root state | KV2 bytes / amplification | Experimental bytes / amplification |
| --- | ---: | ---: |
| Preclean | 3,266,360 / 2.384692 | 6,741,180 / 4.921575 |
| Postclean | n/a | 4,743,572 / 3.463169 |
| Final, after reopen and shutdown | 3,266,360 / 2.384692 | 4,743,572 / 3.463169 |

Live-byte denominator: 1,369,720. Experimental postclean / KV2 preclean bytes: **1.452250**. This compares explicitly different lifecycle states; the experimental clean cost is reported above.

| Root category, bytes | KV2 preclean | Experimental preclean | Experimental postclean |
| --- | ---: | ---: | ---: |
| wal_bytes | 1,956,383 | 4,184,999 | 2,337,403 |
| sstable_bytes | 1,309,648 | 2,492,160 | 2,405,480 |
| metadata_bytes | 329 | 64,021 | 689 |
| other_bytes | 0 | 0 | 0 |
| file_count | 10 | 96 | 19 |

KV2 preclean registered tables: L0=1; total 1.
Experimental preclean registered tables: L0=1, L1=4, L2=15, L3=54; total 74.
Experimental postclean registered tables: L0=1; total 1.

Cleaner: 15,565 live records, 996,160 payload bytes copied in 61 transactions, 1 index SSTable, 0 deferred bytes. Observed root coexistence sample: 6,741,180 before, 11,484,703 peak sample, 4,743,572 after.

Warm-read time ratios, experimental / KV2: point gets **1.5605**, scan **56.1099**.

### 64 B, batch 64

| Timed phase | KV2 ms | Experimental ms |
| --- | ---: | ---: |
| Ingest | 1,045.422 (987.481 to 1,103.459) | 1,125.128 (1,057.632 to 1,292.516) |
| Mutations | 394.612 (373.784 to 405.502) | 569.685 (535.764 to 714.950) |
| Pending-write barrier | 0.002 | 0.008 |
| Preclean shutdown | 0.015 | 2,676.636 |
| Preclean reopen | 38.519 (35.916 to 44.432) | 105.264 (65.869 to 122.424) |
| Clean pause (inside clean elapsed) | n/a | 6,404.592 |
| Clean elapsed | n/a | 6,420.123 |
| Postclean shutdown | n/a | 0.076 |
| Postclean reopen | n/a | 66.659 |
| Final shutdown | 0.074 | 0.248 |
| Lifecycle phase sum | 1,478.614 (1,397.272 to 1,553.551) | 10,926.603 (10,744.540 to 12,374.986) |
| Warm point gets (64 calls) | 5.835 | 10.430 |
| Warm scan (128 entries) | 0.378 | 17.203 |

| Closed root state | KV2 bytes / amplification | Experimental bytes / amplification |
| --- | ---: | ---: |
| Preclean | 2,862,736 / 2.090015 | 5,925,303 / 4.325923 |
| Postclean | n/a | 4,743,573 / 3.463170 |
| Final, after reopen and shutdown | 2,862,736 / 2.090015 | 4,743,573 / 3.463170 |

Live-byte denominator: 1,369,720. Experimental postclean / KV2 preclean bytes: **1.657007**. This compares explicitly different lifecycle states; the experimental clean cost is reported above.

| Root category, bytes | KV2 preclean | Experimental preclean | Experimental postclean |
| --- | ---: | ---: | ---: |
| wal_bytes | 1,547,098 | 3,349,505 | 2,337,403 |
| sstable_bytes | 1,315,308 | 2,510,604 | 2,405,480 |
| metadata_bytes | 330 | 65,194 | 690 |
| other_bytes | 0 | 0 | 0 |
| file_count | 8 | 94 | 19 |

KV2 preclean registered tables: L0=1; total 1.
Experimental preclean registered tables: L0=1, L1=4, L2=15, L3=55; total 75.
Experimental postclean registered tables: L0=1; total 1.

Cleaner: 15,565 live records, 996,160 payload bytes copied in 61 transactions, 1 index SSTable, 0 deferred bytes. Observed root coexistence sample: 5,925,303 before, 10,668,827 peak sample, 4,743,573 after.

Warm-read time ratios, experimental / KV2: point gets **1.7875**, scan **45.5274**.

### 4 KiB, batch 1

| Timed phase | KV2 ms | Experimental ms |
| --- | ---: | ---: |
| Ingest | 1,819.608 (1,805.716 to 1,882.851) | 1,879.217 (1,860.501 to 2,009.974) |
| Mutations | 1,142.980 (1,116.242 to 1,181.652) | 810.891 (652.852 to 836.428) |
| Pending-write barrier | 0.002 | 0.007 |
| Preclean shutdown | 2,815.395 | 7.000 |
| Preclean reopen | 43.528 (37.163 to 62.610) | 20.697 (16.325 to 25.284) |
| Clean pause (inside clean elapsed) | n/a | 541.469 |
| Clean elapsed | n/a | 548.699 |
| Postclean shutdown | n/a | 0.056 |
| Postclean reopen | n/a | 15.976 |
| Final shutdown | 0.038 | 0.125 |
| Lifecycle phase sum | 5,821.552 (5,192.932 to 6,043.783) | 3,290.420 (3,085.878 to 3,448.166) |
| Warm point gets (64 calls) | 84.934 | 44.569 |
| Warm scan (128 entries) | 7.563 | 102.262 |

| Closed root state | KV2 bytes / amplification | Experimental bytes / amplification |
| --- | ---: | ---: |
| Preclean | 2,917,974 / 1.457297 | 2,812,381 / 1.404561 |
| Postclean | n/a | 2,108,392 / 1.052975 |
| Final, after reopen and shutdown | 2,917,974 / 1.457297 | 2,108,392 / 1.052975 |

Live-byte denominator: 2,002,320. Experimental postclean / KV2 preclean bytes: **0.722553**. This compares explicitly different lifecycle states; the experimental clean cost is reported above.

| Root category, bytes | KV2 preclean | Experimental preclean | Experimental postclean |
| --- | ---: | ---: | ---: |
| wal_bytes | 660,326 | 2,812,304 | 2,032,634 |
| sstable_bytes | 2,193,598 | 0 | 75,228 |
| metadata_bytes | 64,050 | 77 | 530 |
| other_bytes | 0 | 0 | 0 |
| file_count | 90 | 16 | 11 |

KV2 preclean registered tables: L0=1, L1=5, L2=20, L3=60; total 86.
Experimental preclean registered tables: none; total 0.
Experimental postclean registered tables: L0=1; total 1.

Cleaner: 486 live records, 1,990,656 payload bytes copied in 2 transactions, 1 index SSTable, 0 deferred bytes. Observed root coexistence sample: 2,812,381 before, 4,920,724 peak sample, 2,108,392 after.

Warm-read time ratios, experimental / KV2: point gets **0.5247**, scan **13.5212**.

### 4 KiB, batch 64

| Timed phase | KV2 ms | Experimental ms |
| --- | ---: | ---: |
| Ingest | 166.714 (152.631 to 185.841) | 89.955 (82.272 to 101.032) |
| Mutations | 43.786 (41.918 to 48.196) | 38.094 (36.124 to 40.634) |
| Pending-write barrier | 0.014 | 0.007 |
| Preclean shutdown | 2,991.672 | 5.990 |
| Preclean reopen | 37.245 (36.029 to 47.423) | 12.090 (11.406 to 12.387) |
| Clean pause (inside clean elapsed) | n/a | 463.677 |
| Clean elapsed | n/a | 469.147 |
| Postclean shutdown | n/a | 0.044 |
| Postclean reopen | n/a | 18.066 |
| Final shutdown | 0.030 | 0.091 |
| Lifecycle phase sum | 3,262.980 (3,218.724 to 3,765.002) | 646.392 (621.915 to 743.948) |
| Warm point gets (64 calls) | 62.302 | 38.645 |
| Warm scan (128 entries) | 6.860 | 100.899 |

| Closed root state | KV2 bytes / amplification | Experimental bytes / amplification |
| --- | ---: | ---: |
| Preclean | 2,911,893 / 1.454260 | 2,786,333 / 1.391552 |
| Postclean | n/a | 2,108,393 / 1.052975 |
| Final, after reopen and shutdown | 2,911,893 / 1.454260 | 2,108,393 / 1.052975 |

Live-byte denominator: 2,002,320. Experimental postclean / KV2 preclean bytes: **0.724063**. This compares explicitly different lifecycle states; the experimental clean cost is reported above.

| Root category, bytes | KV2 preclean | Experimental preclean | Experimental postclean |
| --- | ---: | ---: | ---: |
| wal_bytes | 644,488 | 2,786,256 | 2,032,634 |
| sstable_bytes | 2,202,276 | 0 | 75,228 |
| metadata_bytes | 65,129 | 77 | 531 |
| other_bytes | 0 | 0 | 0 |
| file_count | 91 | 16 | 11 |

KV2 preclean registered tables: L0=1, L1=5, L2=20, L3=61; total 87.
Experimental preclean registered tables: none; total 0.
Experimental postclean registered tables: L0=1; total 1.

Cleaner: 486 live records, 1,990,656 payload bytes copied in 2 transactions, 1 index SSTable, 0 deferred bytes. Observed root coexistence sample: 2,786,333 before, 4,894,677 peak sample, 2,108,393 after.

Warm-read time ratios, experimental / KV2: point gets **0.6203**, scan **14.7072**.

### 64 KiB, batch 1

| Timed phase | KV2 ms | Experimental ms |
| --- | ---: | ---: |
| Ingest | 1,265.953 (1,221.446 to 1,283.169) | 699.252 (680.419 to 712.958) |
| Mutations | 514.705 (495.531 to 514.946) | 236.972 (227.551 to 240.620) |
| Pending-write barrier | 0.002 | 0.002 |
| Preclean shutdown | 5,511.454 | 12.762 |
| Preclean reopen | 34.777 (34.375 to 51.061) | 18.205 (16.070 to 19.144) |
| Clean pause (inside clean elapsed) | n/a | 322.351 |
| Clean elapsed | n/a | 331.464 |
| Postclean shutdown | n/a | 0.041 |
| Postclean reopen | n/a | 20.764 |
| Final shutdown | 0.034 | 0.114 |
| Lifecycle phase sum | 7,324.978 (7,260.840 to 7,442.459) | 1,311.122 (1,302.184 to 1,383.586) |
| Warm point gets (61 calls) | 99.540 | 120.785 |
| Warm scan (61 entries) | 6.652 | 114.563 |

| Closed root state | KV2 bytes / amplification | Experimental bytes / amplification |
| --- | ---: | ---: |
| Preclean | 7,640,225 / 1.910457 | 5,451,520 / 1.363166 |
| Postclean | n/a | 4,013,218 / 1.003515 |
| Final, after reopen and shutdown | 7,640,225 / 1.910457 | 4,013,218 / 1.003515 |

Live-byte denominator: 3,999,160. Experimental postclean / KV2 preclean bytes: **0.525275**. This compares explicitly different lifecycle states; the experimental clean cost is reported above.

| Root category, bytes | KV2 preclean | Experimental preclean | Experimental postclean |
| --- | ---: | ---: | ---: |
| wal_bytes | 197,751 | 5,451,443 | 4,003,051 |
| sstable_bytes | 7,373,744 | 0 | 9,656 |
| metadata_bytes | 68,730 | 77 | 511 |
| other_bytes | 0 | 0 | 0 |
| file_count | 114 | 33 | 10 |

KV2 preclean registered tables: L2=3, L3=15, L4=46; total 64.
Experimental preclean registered tables: none; total 0.
Experimental postclean registered tables: L0=1; total 1.

Cleaner: 61 live records, 3,997,696 payload bytes copied in 1 transactions, 1 index SSTable, 0 deferred bytes. Observed root coexistence sample: 5,451,520 before, 9,464,689 peak sample, 4,013,218 after.

Warm-read time ratios, experimental / KV2: point gets **1.2134**, scan **17.2211**.

### 64 KiB, batch 16

| Timed phase | KV2 ms | Experimental ms |
| --- | ---: | ---: |
| Ingest | 369.564 (357.501 to 391.219) | 78.430 (77.691 to 95.583) |
| Mutations | 126.298 (111.584 to 131.695) | 51.104 (38.780 to 57.868) |
| Pending-write barrier | 0.002 | 0.002 |
| Preclean shutdown | 6,030.141 | 13.042 |
| Preclean reopen | 45.789 (38.833 to 58.365) | 16.439 (16.324 to 18.412) |
| Clean pause (inside clean elapsed) | n/a | 346.880 |
| Clean elapsed | n/a | 354.464 |
| Postclean shutdown | n/a | 0.037 |
| Postclean reopen | n/a | 20.843 |
| Final shutdown | 0.048 | 0.076 |
| Lifecycle phase sum | 6,591.352 (6,462.570 to 7,345.927) | 545.944 (530.144 to 554.760) |
| Warm point gets (61 calls) | 88.165 | 114.185 |
| Warm scan (61 entries) | 6.413 | 119.669 |

| Closed root state | KV2 bytes / amplification | Experimental bytes / amplification |
| --- | ---: | ---: |
| Preclean | 5,533,351 / 1.383628 | 5,447,657 / 1.362200 |
| Postclean | n/a | 4,013,219 / 1.003515 |
| Final, after reopen and shutdown | 5,533,351 / 1.383628 | 4,013,219 / 1.003515 |

Live-byte denominator: 3,999,160. Experimental postclean / KV2 preclean bytes: **0.725278**. This compares explicitly different lifecycle states; the experimental clean cost is reported above.

| Root category, bytes | KV2 preclean | Experimental preclean | Experimental postclean |
| --- | ---: | ---: | ---: |
| wal_bytes | 197,455 | 5,447,580 | 4,003,051 |
| sstable_bytes | 5,266,960 | 0 | 9,656 |
| metadata_bytes | 68,936 | 77 | 512 |
| other_bytes | 0 | 0 | 0 |
| file_count | 82 | 11 | 10 |

KV2 preclean registered tables: L2=3, L3=15, L4=46; total 64.
Experimental preclean registered tables: none; total 0.
Experimental postclean registered tables: L0=1; total 1.

Cleaner: 61 live records, 3,997,696 payload bytes copied in 1 transactions, 1 index SSTable, 0 deferred bytes. Observed root coexistence sample: 5,447,657 before, 9,460,827 peak sample, 4,013,219 after.

Warm-read time ratios, experimental / KV2: point gets **1.2951**, scan **18.6603**.

## Interpretation

- **64 B:** experimental ingestion medians are longer, and cleaned total space is still
  45.2% / 65.7% larger than KV2 at batches 1 / 64. The cleaner pauses for 5.539 / 6.405 s;
  experimental batch-64 preclean shutdown also takes 2.677 s. Warm scans take 56.1 / 45.5
  times as long. The 107-byte reference exceeds the payload size. Preclean experimental
  manifests register 74 / 75 tables through L3, versus one L0 table in KV2. Higher levels
  establish compaction occurred; compaction operation counts and timing attribution were
  not measured. The substantially overlapping batch-1 ingest ranges do not establish a
  robust timing direction or explain the variance.
- **4 KiB:** batch-1 ingestion is similar, with the experimental median 3.3% longer;
  batch-64 ingestion has an observed rate ratio of 1.85. Cleaned total space is 27.7% /
  27.6% below KV2 preclean, with 0.541 / 0.464 s clean pauses. Warm scans take 13.5 /
  14.7 times as long; warm point gets are faster in this corpus. Experimental preclean
  indexes have no SSTables, while KV2 has registered higher levels, so this is not a
  matched-compaction-work experiment.
- **64 KiB:** observed ingestion rate ratios are 1.81 / 4.71 at batches 1 / 16. Cleaned
  total space is 47.5% / 27.5% below KV2 preclean, with 0.322 / 0.347 s clean pauses.
  Warm point gets take 1.21 / 1.30 times as long and scans take 17.2 / 18.7 times as long.
  Experimental preclean indexes again have no SSTables; cleaning builds one L0 table.

Much of the phase-sum difference for larger values lies in KV2 preclean shutdown:
2.815 / 2.992 s at 4 KiB and 5.511 / 6.030 s at 64 KiB. `KV2::shutdown` waits for
admitted writes and joins the compaction worker, including queued compaction draining.
It does not explicitly flush the memtable. Internal costs were not profiled; the measured
shutdown cannot be attributed exclusively to compaction, flushing, scheduling or I/O.

## Accounting and limits

- `lifecycle_phase_sum` adds ingest, mutate, barrier, preclean shutdown/reopen, experimental
  clean elapsed and postclean shutdown/reopen, and final shutdown. It excludes verification,
  census, manifest-inspection and reporting gaps. It is not continuous wall time or an
  end-to-end throughput measure. Clean pause is included in elapsed, never added twice.
- Total root bytes count WAL, SSTables, manifests, metadata and generations. Amplification
  divides those bytes by the verified live key+value bytes. KV2 checkpoint reclamation is
  natural; experimental retains its generation log until explicit cleaning. Before and
  after cleaning are distinct states and both are reported.
- All 18 measured experimental samples have zero deferred cleanup and one remaining
  generation after cleaning. Their observed coexistence sample equals before + after - 49
  bytes. That is an observation at one point, not a continuous peak or a GC guarantee.
- Reads and cleaning follow verification and are warm-cache. Each experimental value read
  validates its entire referenced transaction, not just the requested value. Full verification
  scans outside the timers retain that cost; cleaner-internal validation is inside its timer.
- Values are seeded high-entropy bytes, reducing compressibility. Root logical bytes and
  `st_blocks * 512` do not measure unique compressed physical usage on Btrfs with CoW/reflinks.
- The run used serialized calls, a two-worker runtime, no CPU affinity and no cache dropping.
  Three samples and small forced table targets do not establish production capacity,
  sustained growth behaviour, cold-cache performance or general performance parity.
- No small-value inline hybrid was implemented or tested. Only 64 B, 4 KiB and 64 KiB
  payloads were measured; 8-byte FTS counters were not a performance case.
- Retained-history replay and whole-transaction value validation remain recovery/read costs.
  These bounded histories do not establish long-running recovery or read scalability.
- FTS is functional equivalence only: all 128 documents, Boolean result IDs and ranked
  ID order/score bits match before and after cleaning and reopening. No ranking-quality
  or FTS performance conclusion follows.
- No power-loss, production-safety, Raft, external-engine or lifetime write-amplification
  conclusion follows. The cleaner remains an explicit full-generation write pause.
- **The measured process exit status was not captured.** Complete JSON, the final results
  line, empty stderr and invariant checks support completed measurement, but normal process
  exit remains unverified. Build and pilot exit statuses were recorded as 0. No rerun was
  requested by independent review.

## Review and artifacts

Independent review recomputed all six cases and verified raw-versus-stdout consistency,
retained final-root censuses, source/binary stability and the persistent dataset mount.
The four reporting findings (shutdown attribution, batch labels, read ratios/uncertainty,
and table-count interpretation) are corrected in this summary. Raw measurements, logs and
source provenance remain unchanged. The audit narrative was also clarified where noted.

| Path | Contents |
| --- | --- |
| `raw/results.json` | Immutable 36-sample measurement JSON |
| `raw/pilot-results.json` | Immutable pilot JSON |
| `logs/` | Build, pilot and measured outputs; command and run window |
| `provenance/` | Frozen source/binary hashes, environment, dataset and mount records |
| `audit/recompute.py` | Original benchmark-worker recomputation |
| `audit/audit-run-output.txt` | Original 43 checks and pilot audit |
| `audit/recomputed-summary.json` | Per-case derived medians, ranges, counts and ratios |
| `audit/measurement-graph.md` | Measurement flow |
| `audit/audit.md` | Benchmark-worker audit narrative with reporting corrections |

Raw SHA-256: measured `d431c7eeffb1daed3ddea4dbe32f71490a9a43775dc79ebcc4d98ce5fc990752`;
pilot `0c947dae3afa27b13ff3d6c9ae86d3ebbed09a271fd6332103655324722fe945`.
