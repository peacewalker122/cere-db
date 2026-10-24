# FTS million-document seed — IN_PROGRESS

- Target: 1,000,000 acknowledged public `FtsIndex::put_document` writes.
- Started: 2026-10-24 15:26 UTC (UTC run label `2026-10-24T15-24-49Z`).
- Dataset: `target/scale/2026-10-24T15-24-49Z-fts-1m/db`.
- Runner: `examples/fts_scale.rs`; release binary, 16 MiB memtable threshold; WAL retained.
- PID and OS `flock` lock file: run root `pid` and `.lock`; non-resumable fresh run.
- Pilot: 100 writes, exact get passed, `common` returned 100, `group0000` returned 1, `unique0000000` returned 1, clean shutdown; fresh-run rejection verified.
- Latest observed progress (2026-10-24 15:32 UTC): 55,000 acknowledgements at 487.31 s elapsed; latest 1,000-write window 84.10 docs/s. The observed window rate is substantially below the early estimate; no ETA is claimed. Process chain: PID 1891074 `flock` → PID 1891077 `/usr/bin/time` → PID 1891078 `fts_scale`. No completion marker exists yet; seed remains active. Seed RSS ~245 MiB; host `MemAvailable` ~5.8 GiB. No independent reopen/query assessment has started.
- Logs and live progress: run root `progress.jsonl`, `stdout.log`, `stderr.log`; progress records every 1,000 acknowledgements and flushes immediately. `completion.json` is written only after clean shutdown.
- Resource snapshot: 307 GiB filesystem free at launch; peak RSS is tracked by `/usr/bin/time -v` and is recorded in `stderr.log` after process exit.
- Reproduction: see `command.txt`; launch was guarded by `flock -n` and did not reset prior state.
- The acknowledgement count is successful seed writes, not an independently verified total index count. No full-text query benchmark has been run on the million-document corpus.
