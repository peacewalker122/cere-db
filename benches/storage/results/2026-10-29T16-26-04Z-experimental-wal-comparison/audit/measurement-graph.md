# Implemented measurement graph

Call-graph of what the frozen binary actually executed. `→` is "calls", `⇒` is "yields".

## Entry

```
main(Cli) ⇒ Result<(), HarnessError>
→ pilot_profile(cli)                      [--pilot only: small-tables, 32 rec, 4 KiB, batch 8, 1 sample, 0 warmups]
→ case_specs(cli)                         [benchmark only: default value/record/batch matrix + guards]
→ require_persistent_datadir(base)
   → prepare_run_root(base)               [absent-or-empty, symlink-ancestor rejection, exclusive .harness-run-owner]
   → fs::canonicalize → mount_info_for(canonical)   [/proc/self/mountinfo longest-prefix match]
   ⇒ {path, mount_point=/home, fstype=btrfs, source=/dev/nvme0n1p3, transient_rejected=[tmpfs,ramfs,devtmpfs]}
```

## Pilot path (validated the harness, not a performance result)

```
run_pilot(base, config, filesystem)
→ run_sample(Reference,     pilot/reference)
→ run_sample(Experimental,  pilot/experimental)
→ pilot_mode_guards(pilot/mode-guards)  → clean ×2 → expect_closed(9 refused ops) → reopen
→ fts_reference(pilot/fts-baseline)  → fts_experimental(pilot/fts-experimental) → compare_fts
⇒ pilot/results.json   (schema ceredb-experimental-wal-comparison-pilot/2)
```

## Benchmark path

```
run_benchmark(cli, config, cases, base, filesystem)
for each case in {64B×16384, 4096B×512, 65536B×64}
  for each batch_size in {1,64} / {1,64} / {1,16}
    warmups (cli.warmups = 1)
      run_sample(Reference,    <case>-<batch>/warmup-0/reference)
      run_sample(Experimental, <case>-<batch>/warmup-0/experimental)
    measured samples (cli.repetitions = 3), alternating engine order
      sample 0: Reference → Experimental
      sample 1: Experimental → Reference
      sample 2: Reference → Experimental
    ⇒ 6 samples per case-batch, 36 measured samples total
    ⇒ level-evidence {reference_preclean_compaction_observed,
                      experimental_preclean_compaction_observed,
                      experimental_postclean_compaction_observed}
fts (functional equivalence only, one run)
  fts_reference(fts/baseline) → fts_experimental(fts/experimental) → compare_fts
  ⇒ 128 documents, 2 put_documents calls of 64, Boolean AND/OR + ranked AND/OR score bits equal
    after preclean reopen and after clean+reopen
⇒ results.json   (schema ceredb-experimental-wal-comparison/2)
```

## One sample

```
run_sample(kind, spec, config, dir)
→ create_fresh_dir(dir)                    [exclusive create; existing path is a hard failure]
→ Workload::build(WorkloadParams)
   → key_for(index,seed)  = 24 B (8 B big-endian index ‖ 16 B SplitMix64 bytes)
   → value_for(index,round,value_bytes,seed)  [high-entropy, no repetitive pattern]
   → shuffled_indices → overwrite 25% → delete 10% → reinsert 50% of deleted
   ⇒ append batches, mutate batches, expected BTreeMap, deleted/absent probes, summary
→ measure_reference | measure_experimental
```

### measure_reference

```
KV2::open
→ phase_ingest(append batches)            [clocked: write_batch loop only]
→ phase_ingest(mutate batches)            [clocked]
→ phase_settle                            [clocked: await_pending_writes]
→ timed(shutdown)                         [clocked: shutdown_preclean, immediately after settle]
→ observe_closed(Reference, dir)          [unclocked: census, level_report, generation_ids]
→ timed(KV2::open)                        [clocked: reopen_preclean — includes WAL replay]
→ verify_engine("reference reopened")     [unclocked: exact full scan == expected, ≤16 point probes,
                                            deleted probe, absent probe]
→ read_benchmark                          [clocked: ≤64 warm point gets, one ≤128-entry scan]
→ timed(shutdown)                         [clocked: shutdown_final]
→ census(dir)                             [unclocked: final_census]
```

### measure_experimental

```
ExperimentalEngine::open_with_config
→ phase_ingest(append) → phase_ingest(mutate) → phase_settle
→ timed(shutdown)                         [shutdown_preclean]
→ observe_closed(Experimental, dir)
→ timed(open_with_config)                 [reopen_preclean — replays retained generation log]
→ verify_engine("experimental preclean reopen")
→ clean()                                 [CleanStats: write_pause, elapsed, copied payload/txns,
                                            before/peak/after root bytes, deferred bytes]
→ timed(shutdown)                         [shutdown_postclean]
→ observe_closed(Experimental, dir)       [postclean census + levels + generations]
→ timed(open_with_config)                 [reopen_postclean]
→ verify_engine("experimental cleaned reopen")
→ read_benchmark
→ timed(shutdown)                         [shutdown_final]
→ census(dir)                             [final_census]
```

### Clocks

```
phase_sum_ns(sample) = ingest + mutate + settle
                     + preclean_shutdown + preclean_reopen
                     + clean_elapsed                      [pause is inside elapsed, not added again]
                     + postclean_shutdown + postclean_reopen   [experimental only, else 0]
                     + final_shutdown
```
Excluded by construction: input/expected construction, verification scans and probes, every census,
manifest/level reads, JSON serialisation, FTS comparison, process start-up and file writes.

## Closed-root observation

```
observe_closed(kind, dir)
→ census(dir)                             [recursive; symlink and non-file entries rejected]
   → classify by path component relative to the store root:
     ".../wal/..." → wal_bytes, ".../sstable/..." → sstable_bytes,
     file name in {MANIFEST,CURRENT,CURRENT.tmp,LOCK,SEAL,SEAL.tmp,NEXT-GENERATION,
                   NEXT-GENERATION.tmp,EXPERIMENTAL-WAL-VALUES} → metadata_bytes, else other_bytes
   ⇒ total_bytes (denominator for space amplification), file_count, os_allocated_bytes = st_blocks*512
→ level_report(kind, dir)
   Reference:     ManifestManager::load_or_create(root/MANIFEST)
   Experimental:  generation_ids(root/generations) → newest → index/MANIFEST
   ⇒ per-level table counts, total_records, compaction_observed = any(level>0 with tables>0)
→ generation_ids(dir)
```

No engine is open during any census or manifest read; the harness never opens a second live engine
on the same root.

## Audit path (post-run, independent of the harness)

```
recompute.py
→ parse raw/results.json
→ re-derive config, case matrix, sample/warmup counts (also by walking the on-disk directories)
→ re-add census breakdowns, workload identities, phase sums, summary medians
→ re-divide space amplification from total_bytes / live key+value bytes
→ re-check clean identities and peak bounds, generation counts, read operation counts
→ compare FTS Boolean/ranked ID and score-bit sequences across reference, exp-preclean, exp-postclean
⇒ audit-run-output.txt (43 [OK], AUDIT RESULT: PASS) + recomputed-summary.json
```
