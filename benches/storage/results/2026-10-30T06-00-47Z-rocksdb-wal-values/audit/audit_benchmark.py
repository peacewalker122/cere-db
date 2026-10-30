import hashlib
import json
import math
import os
import statistics
import stat
import sys
from pathlib import Path

run_root = Path(sys.argv[1]).resolve()
repo_root = Path.cwd().resolve()
engines = ("kv2_inline", "experimental_wal_values", "rocks_db")
engine_names = {"kv2_inline": "KV2 inline", "experimental_wal_values": "Experimental WAL", "rocks_db": "RocksDB"}
expected_cases = [
    ("v256_n1024_b1", 1024, 256, 1),
    ("v256_n1024_b16", 1024, 256, 16),
    ("v256_n1024_b64", 1024, 256, 64),
    ("v256_n1024_b256", 1024, 256, 256),
    ("v4096_n512_b1", 512, 4096, 1),
    ("v4096_n512_b64", 512, 4096, 64),
    ("v65536_n64_b1", 64, 65536, 1),
    ("v65536_n64_b16", 64, 65536, 16),
]
permutations = (
    ("kv2_inline", "experimental_wal_values", "rocks_db"),
    ("experimental_wal_values", "rocks_db", "kv2_inline"),
    ("rocks_db", "kv2_inline", "experimental_wal_values"),
)
lifecycle_fields = (
    "fresh_open_ns",
    "append_ns",
    "mutations_ns",
    "barrier_drain_ns",
    "close_ns",
    "reopen_uncleaned_ns",
    "warm_get_uncleaned_ns",
    "warm_scan_uncleaned_ns",
    "warm_get_post_clean_ns",
    "warm_scan_post_clean_ns",
    "barrier_after_clean_ns",
    "close_after_clean_ns",
    "reopen_post_clean_ns",
    "close_after_verification_ns",
    "close_post_clean_verification_ns",
)
phase_fields = (
    "fresh_open_ns",
    "append_ns",
    "mutations_ns",
    "barrier_drain_ns",
    "close_ns",
    "reopen_uncleaned_ns",
    "warm_get_uncleaned_ns",
    "warm_scan_uncleaned_ns",
    "clean_elapsed_ns",
    "clean_write_pause_ns",
    "barrier_after_clean_ns",
    "close_after_clean_ns",
    "reopen_post_clean_ns",
    "warm_get_post_clean_ns",
    "warm_scan_post_clean_ns",
    "close_after_verification_ns",
    "close_post_clean_verification_ns",
    "lifecycle_sum_ns",
)


def check(condition, message):
    if not condition:
        raise AssertionError(message)


def read_report(path):
    return json.loads(path.read_text())


def read_status(path, key):
    values = {}
    for line in path.read_text().splitlines():
        if "=" in line:
            name, value = line.split("=", 1)
            values[name] = value
    check(values.get(key) == "0", f"{path.name}: {key} missing or nonzero")
    return values


def parse_findmnt(path):
    lines = path.read_text().splitlines()
    check(len(lines) == 2, f"unexpected findmnt capture: {path}")
    fields = lines[1].split()
    check(len(fields) == 4, f"cannot parse findmnt capture: {path}")
    return dict(zip(("target", "fstype", "source", "options"), fields))


def expected_workload(case):
    records = case["records"]
    key_bytes = case["key_bytes"]
    value_bytes = case["value_bytes"]
    batch_size = case["batch_size"]
    overwrite_count = records // 4
    approximate_deletes = (records + 5) // 10
    delete_count = approximate_deletes if approximate_deletes % 2 == 0 else approximate_deletes + 1
    reinsert_count = delete_count // 2
    append_operations = records
    mutation_operations = overwrite_count + delete_count + reinsert_count
    live_records = records - delete_count + reinsert_count
    return {
        "overwrite_operations": overwrite_count,
        "delete_operations": delete_count,
        "reinsert_operations": reinsert_count,
        "append": {
            "operations": append_operations,
            "batch_calls": math.ceil(append_operations / batch_size),
            "logical_bytes": append_operations * (key_bytes + value_bytes),
        },
        "mutations": {
            "operations": mutation_operations,
            "batch_calls": math.ceil(mutation_operations / batch_size),
            "logical_bytes": (overwrite_count + reinsert_count) * (key_bytes + value_bytes) + delete_count * key_bytes,
        },
        "live_records": live_records,
        "live_key_value_logical_bytes": live_records * (key_bytes + value_bytes),
        "warm_get_calls": min(live_records, 8),
        "warm_scan_entries": min(live_records, 8),
        "verification_point_gets": min(live_records, 16),
    }


def check_snapshot(snapshot, expected_live_bytes):
    census = snapshot["census"]
    category_bytes = sum(category["logical_bytes"] for category in census["categories"].values())
    category_files = sum(category["file_count"] for category in census["categories"].values())
    check(census["total_file_logical_bytes"] == category_bytes, "closed census category byte sum mismatch")
    check(census["file_count"] == category_files, "closed census category file sum mismatch")
    check(snapshot["live_key_value_logical_bytes"] == expected_live_bytes, "closed census live-byte denominator mismatch")
    expected_ratio = census["total_file_logical_bytes"] / expected_live_bytes
    check(math.isclose(snapshot["root_bytes_per_live_logical_byte"], expected_ratio, rel_tol=1e-12, abs_tol=1e-12), "closed census ratio mismatch")
    return census


def file_category(relative):
    parts = relative.parts
    name = parts[-1] if parts else ""
    if "wal" in parts or (name.endswith(".log") and name[:-4].isdigit()):
        return "wal"
    if "sstable" in parts or name.endswith((".sst", ".ldb")):
        return "sstable"
    if name == "MANIFEST" or name.startswith("MANIFEST-"):
        return "manifest"
    if name in {"LOG", "CURRENT", "LOCK", "IDENTITY", "OPTIONS"} or name.startswith(("LOG.", "OPTIONS-", "SEAL", "NEXT-GENERATION")) or name == "EXPERIMENTAL-WAL-VALUES":
        return "metadata"
    return "other"


def actual_census(root):
    categories = {}
    file_count = 0
    total_bytes = 0
    allocated_bytes = 0
    allocated_known = True
    for directory, dirs, files in os.walk(root, topdown=True, followlinks=False):
        base = Path(directory)
        for name in list(dirs):
            path = base / name
            if path.is_symlink():
                dirs.remove(name)
                files.append(name)
        for name in files:
            path = base / name
            metadata = path.lstat()
            check(stat.S_ISREG(metadata.st_mode) or stat.S_ISLNK(metadata.st_mode), f"unexpected filesystem entry: {path}")
            category = file_category(path.relative_to(root))
            total = categories.setdefault(category, {"file_count": 0, "logical_bytes": 0})
            total["file_count"] += 1
            total["logical_bytes"] += metadata.st_size
            file_count += 1
            total_bytes += metadata.st_size
            if hasattr(metadata, "st_blocks"):
                allocated_bytes += metadata.st_blocks * 512
            else:
                allocated_known = False
    check(file_count == sum(item["file_count"] for item in categories.values()), f"actual census file category sum mismatch: {root}")
    check(total_bytes == sum(item["logical_bytes"] for item in categories.values()), f"actual census byte category sum mismatch: {root}")
    return {
        "total_file_logical_bytes": total_bytes,
        "file_count": file_count,
        "st_blocks_bytes_informational": allocated_bytes if allocated_known else None,
        "categories": categories,
    }


def value_at(row, field):
    if field == "clean_elapsed_ns":
        return None if row["clean"] is None else row["clean"]["elapsed_ns"]
    if field == "clean_write_pause_ns":
        return None if row["clean"] is None else row["clean"]["write_pause_ns"]
    return row["timings"][field]


def stats(values):
    values = [value for value in values if value is not None]
    if not values:
        return None
    return {"median": statistics.median(values), "min": min(values), "max": max(values)}


def ratio_stat(numerator, denominator):
    check(denominator != 0, "cannot compute ratio with zero denominator")
    ratio = numerator / denominator
    return {
        "experimental_over_rocksdb_time_ratio": ratio,
        "experimental_minus_rocksdb_ns": numerator - denominator,
        "experimental_time_change_percent": (ratio - 1.0) * 100.0,
        "direction": "faster" if ratio < 1.0 else "slower" if ratio > 1.0 else "equal",
    }


def validate_report(report, mode, dataset_root):
    check(report["schema_version"] == 2, f"{mode}: schema version is not 2")
    check(report["mode"] == ("functional_pilot" if mode == "pilot" else "benchmark_matrix"), f"{mode}: mode mismatch")
    check(report["performance_result"] is (mode == "full"), f"{mode}: performance_result mismatch")
    check(report["storage_config"]["memtable_size_threshold_bytes"] == 1024 * 1024, f"{mode}: CereDB memtable setting mismatch")
    check(report["storage_config"]["wal_segment_size_bytes"] == 1024 * 1024, f"{mode}: CereDB WAL setting mismatch")
    check(report["storage_config"]["other_settings"] == "StorageConfig::default() for remaining CereDB settings", f"{mode}: CereDB defaults mismatch")
    check(report["key_bytes"] == 20 and report["seed_hex"] == "0x6c756e6177616c76", f"{mode}: key or workload seed mismatch")
    check(report["rocksdb_config"] == {
        "crate_version": "0.24.0",
        "native_version": "10.4.2",
        "write_buffer_size_bytes": 1024 * 1024,
        "wal_enabled": True,
        "synchronous_writes": True,
        "compression": "disabled",
    }, f"{mode}: RocksDB config mismatch")
    check(report["mount_before"] == report["mount_after"], f"{mode}: report mount changed")
    check(report["mount_before"]["dataset_path"] == str(dataset_root), f"{mode}: report dataset path mismatch")
    check(report["mount_before"]["fstype"] == "btrfs", f"{mode}: expected Btrfs dataset")
    check("compress=zstd:1" in report["mount_before"]["options"].split(","), f"{mode}: expected compress=zstd:1 mount option missing")
    if mode == "pilot":
        cases = [("v4096_n20_b1", 20, 4096, 1)]
        samples, warmups = 1, 0
    else:
        cases = expected_cases
        samples, warmups = 3, 1
    check(report["measured_samples_per_engine_case"] == samples, f"{mode}: measured sample count mismatch")
    check(report["warmups_per_engine_case"] == warmups, f"{mode}: warmup count mismatch")
    expected_measured = len(cases) * len(engines) * samples
    expected_warmups = len(cases) * len(engines) * warmups
    check(report["expected_measured_executions"] == expected_measured, f"{mode}: expected measured count mismatch")
    check(report["expected_warmup_executions"] == expected_warmups, f"{mode}: expected warmup count mismatch")
    rows = report["executions"]
    check(len(rows) == expected_measured + expected_warmups, f"{mode}: execution row count mismatch")
    check([case["id"] for case in (row["case"] for row in rows)][::len(engines) * (samples + warmups)] == [case[0] for case in cases], f"{mode}: case order mismatch")
    by_case_engine = {}
    seen_paths = set()
    final_censuses = []
    for row in rows:
        case = row["case"]
        check(row["engine"] in engines, f"{mode}: unknown engine {row['engine']}")
        expected_case = next((item for item in cases if item[0] == case["id"]), None)
        check(expected_case is not None, f"{mode}: unexpected case {case['id']}")
        check(case == {"id": expected_case[0], "records": expected_case[1], "key_bytes": 20, "value_bytes": expected_case[2], "batch_size": expected_case[3]}, f"{mode}: case dimensions mismatch {case}")
        expected = expected_workload(case)
        check(row["append"] == expected["append"], f"{mode}/{case['id']}/{row['engine']}: append metrics mismatch")
        check(row["mutations"] == expected["mutations"], f"{mode}/{case['id']}/{row['engine']}: mutation metrics mismatch")
        for field in ("overwrite_operations", "delete_operations", "reinsert_operations", "live_records", "live_key_value_logical_bytes"):
            check(row[field] == expected[field], f"{mode}/{case['id']}/{row['engine']}: {field} mismatch")
        check(row["verification_uncleaned"] == {
            "full_scan_entries": expected["live_records"],
            "deterministic_point_gets": expected["verification_point_gets"],
            "deleted_probe_missing": True,
            "never_present_probe_missing": True,
        }, f"{mode}/{case['id']}/{row['engine']}: uncleaned verification mismatch")
        warm_reads = {
            "get_calls": expected["warm_get_calls"],
            "get_entries": expected["warm_get_calls"],
            "scan_calls": 1,
            "scan_entries": expected["warm_scan_entries"],
        }
        check(row["warm_reads_natural"] == warm_reads, f"{mode}/{case['id']}/{row['engine']}: natural warm-read workload mismatch")
        check_snapshot(row["natural_closed"], expected["live_key_value_logical_bytes"])
        timings = row["timings"]
        for key, value in timings.items():
            if key == "lifecycle_sum_ns":
                continue
            check(value is None or (isinstance(value, int) and value >= 0), f"{mode}/{case['id']}/{row['engine']}: invalid timer {key}")
        expected_lifecycle = sum(timings[field] or 0 for field in lifecycle_fields)
        if row["clean"] is not None:
            expected_lifecycle += row["clean"]["elapsed_ns"]
        check(timings["lifecycle_sum_ns"] == expected_lifecycle, f"{mode}/{case['id']}/{row['engine']}: lifecycle sum mismatch")
        expected_path = Path(row["store_path"])
        check(expected_path.is_absolute() and expected_path.resolve() == expected_path, f"{mode}: store path not canonical: {expected_path}")
        check(expected_path.is_dir() and not expected_path.is_symlink(), f"{mode}: retained store path missing or symlinked: {expected_path}")
        check(expected_path.is_relative_to(dataset_root), f"{mode}: store path outside dataset root: {expected_path}")
        check("/tmp/" not in str(expected_path), f"{mode}: /tmp database path found")
        check(str(expected_path) not in seen_paths, f"{mode}: duplicate store path")
        seen_paths.add(str(expected_path))
        key = (case["id"], row["engine"])
        by_case_engine.setdefault(key, []).append(row)
        latest_snapshot = row["post_clean_closed"] if row["engine"] == "experimental_wal_values" else row["natural_closed"]
        if row["engine"] == "experimental_wal_values":
            check(row["clean"] is not None and row["post_clean_closed"] is not None and row["verification_post_clean"] is not None and row["warm_reads_post_clean"] is not None, f"{mode}/{case['id']}: missing experimental clean/post-clean report fields")
            clean = row["clean"]
            check(clean["old_generation"] + 1 == clean["new_generation"], f"{mode}/{case['id']}: cleaner generation transition mismatch")
            check(clean["live_records"] == expected["live_records"] and clean["live_logical_bytes"] == expected["live_key_value_logical_bytes"], f"{mode}/{case['id']}: cleaner live state mismatch")
            check(clean["copied_payload_bytes"] == expected["live_records"] * case["value_bytes"], f"{mode}/{case['id']}: cleaner copied payload byte count mismatch")
            check(clean["copied_transactions"] > 0 and clean["index_sstables_written"] > 0, f"{mode}/{case['id']}: cleaner copy transaction/SSTable counters are zero")
            check(clean["total_root_bytes_before"] == row["natural_closed"]["census"]["total_file_logical_bytes"], f"{mode}/{case['id']}: cleaner before bytes disagree with natural census")
            check(clean["total_root_bytes_after"] == row["post_clean_closed"]["census"]["total_file_logical_bytes"], f"{mode}/{case['id']}: cleaner after bytes disagree with post-clean census")
            check(clean["observed_peak_bytes"] >= max(clean["total_root_bytes_before"], clean["total_root_bytes_after"]), f"{mode}/{case['id']}: cleaner sampled peak below closed bytes")
            check(clean["deferred_cleanup_bytes"] >= 0 and clean["write_pause_ns"] <= clean["elapsed_ns"], f"{mode}/{case['id']}: cleaner timing/cleanup stats invalid")
            check(row["verification_post_clean"] == row["verification_uncleaned"], f"{mode}/{case['id']}: post-clean verification mismatch")
            check(row["warm_reads_post_clean"] == warm_reads, f"{mode}/{case['id']}: post-clean warm-read workload mismatch")
            check_snapshot(row["post_clean_closed"], expected["live_key_value_logical_bytes"])
            generation_root = expected_path / "generations"
            generation_dirs = sorted(path.name for path in generation_root.iterdir() if path.is_dir() and not path.is_symlink())
            check(len(generation_dirs) == 1, f"{mode}/{case['id']}: expected one remaining generation, found {generation_dirs}")
        else:
            check(row["clean"] is None and row["post_clean_closed"] is None and row["verification_post_clean"] is None and row["warm_reads_post_clean"] is None, f"{mode}/{case['id']}/{row['engine']}: non-experimental post-clean fields must be null")
            for field in ("warm_get_post_clean_ns", "warm_scan_post_clean_ns", "barrier_after_clean_ns", "close_after_clean_ns", "reopen_post_clean_ns", "close_post_clean_verification_ns"):
                check(timings[field] is None, f"{mode}/{case['id']}/{row['engine']}: non-experimental timer {field} must be null")
        census = actual_census(expected_path)
        generations = []
        if row["engine"] == "experimental_wal_values":
            generations = sorted(path.name for path in (expected_path / "generations").iterdir() if path.is_dir() and not path.is_symlink())
            other_files = census["categories"].get("other", {}).get("file_count", 0)
            check(other_files == 0, f"{mode}/{case['id']}: unexpected Experimental root files outside known categories")
        expected_census = latest_snapshot["census"]
        final_censuses.append({
            "case": case["id"],
            "engine": row["engine"],
            "warmup": row["warmup"],
            "sample_ordinal": row["sample_ordinal"],
            "store_path": str(expected_path),
            "reported_latest_closed_census": expected_census,
            "actual_final_filesystem_census_without_engine_open": census,
            "delta_actual_minus_reported_bytes": census["total_file_logical_bytes"] - expected_census["total_file_logical_bytes"],
            "remaining_generation_dirs": generations,
            "level_counts": row["natural_closed"]["levels"],
            "level_evidence": row["natural_closed"]["level_evidence"],
        })
    for index, case in enumerate(cases):
        case_id = case[0]
        for engine in engines:
            group = by_case_engine[(case_id, engine)]
            measured = [row for row in group if not row["warmup"]]
            warmups_rows = [row for row in group if row["warmup"]]
            check(len(measured) == samples and len(warmups_rows) == warmups, f"{mode}/{case_id}/{engine}: measured/warmup row counts mismatch")
            check(sorted(row["sample_ordinal"] for row in measured) == list(range(1, samples + 1)), f"{mode}/{case_id}/{engine}: sample ordinals mismatch")
            check(all(row["sample_ordinal"] == 0 for row in warmups_rows), f"{mode}/{case_id}/{engine}: warmup ordinal mismatch")
            for row in group:
                ordinal = 0 if mode == "pilot" or row["warmup"] else row["sample_ordinal"]
                expected_order = permutations[(index + ordinal) % 3]
                check(tuple(row["execution_order"]) == expected_order, f"{mode}/{case_id}/{engine}: order rotation mismatch")
                check(row["execution_order"][row["engine_position"]] == engine, f"{mode}/{case_id}/{engine}: execution position mismatch")
    return rows, by_case_engine, final_censuses


def summarize_case(case_id, case_rows, census_rows):
    result = {"case": case_id, "engines": {}}
    for engine in engines:
        rows = [row for row in case_rows[engine] if not row["warmup"]]
        base = rows[0]
        expected = expected_workload(base["case"])
        phase_summaries = {field: stats([value_at(row, field) for row in rows]) for field in phase_fields}
        natural_bytes = [row["natural_closed"]["census"]["total_file_logical_bytes"] for row in rows]
        natural_ratio = [row["natural_closed"]["root_bytes_per_live_logical_byte"] for row in rows]
        actual_bytes = [next(item for item in census_rows if item["case"] == case_id and item["engine"] == engine and not item["warmup"] and item["sample_ordinal"] == row["sample_ordinal"])["actual_final_filesystem_census_without_engine_open"]["total_file_logical_bytes"] for row in rows]
        item = {
            "append_metrics": expected["append"],
            "append_ops_per_second_from_median_timer": expected["append"]["operations"] * 1_000_000_000 / phase_summaries["append_ns"]["median"],
            "mutation_metrics": expected["mutations"],
            "mutation_ops_per_second_from_median_timer": expected["mutations"]["operations"] * 1_000_000_000 / phase_summaries["mutations_ns"]["median"],
            "phase_ns": phase_summaries,
            "natural_closed_total_file_bytes": stats(natural_bytes),
            "natural_closed_bytes_per_live_logical_byte": stats(natural_ratio),
            "actual_final_filesystem_total_file_bytes": stats(actual_bytes),
            "levels": [{"sample_ordinal": row["sample_ordinal"], "levels": row["natural_closed"]["levels"], "evidence": row["natural_closed"]["level_evidence"]} for row in rows],
            "natural_reads": base["warm_reads_natural"],
        }
        if engine == "experimental_wal_values":
            item["post_clean_total_file_bytes"] = stats([row["post_clean_closed"]["census"]["total_file_logical_bytes"] for row in rows])
            item["post_clean_bytes_per_live_logical_byte"] = stats([row["post_clean_closed"]["root_bytes_per_live_logical_byte"] for row in rows])
            item["post_clean_reads"] = base["warm_reads_post_clean"]
            item["clean_stats"] = {
                key: stats([row["clean"][key] for row in rows])
                for key in ("total_root_bytes_before", "observed_peak_bytes", "total_root_bytes_after", "deferred_cleanup_bytes", "live_records", "live_logical_bytes", "copied_payload_bytes", "copied_transactions", "index_sstables_written", "write_pause_ns", "elapsed_ns")
            }
        result["engines"][engine] = item
    exp = result["engines"]["experimental_wal_values"]
    rocks = result["engines"]["rocks_db"]
    result["experimental_vs_rocksdb"] = {
        phase: ratio_stat(exp["phase_ns"][phase]["median"], rocks["phase_ns"][phase]["median"])
        for phase in ("append_ns", "mutations_ns", "reopen_uncleaned_ns", "warm_get_uncleaned_ns", "warm_scan_uncleaned_ns", "lifecycle_sum_ns")
    }
    result["experimental_vs_rocksdb"]["append_rate_ratio"] = exp["append_ops_per_second_from_median_timer"] / rocks["append_ops_per_second_from_median_timer"]
    result["experimental_vs_rocksdb"]["mutation_rate_ratio"] = exp["mutation_ops_per_second_from_median_timer"] / rocks["mutation_ops_per_second_from_median_timer"]
    result["experimental_cleaned_vs_natural_rocksdb_footprint"] = exp["post_clean_total_file_bytes"]["median"] / rocks["natural_closed_total_file_bytes"]["median"]
    result["experimental_natural_vs_natural_rocksdb_footprint"] = exp["natural_closed_total_file_bytes"]["median"] / rocks["natural_closed_total_file_bytes"]["median"]
    return result


def ns(values):
    if values is None:
        return "—"
    return f"{values['median']:,} [{values['min']:,}–{values['max']:,}]"


def compact_bytes(values):
    if values is None:
        return "—"
    return f"{values['median']:,.0f} [{values['min']:,.0f}–{values['max']:,.0f}]"


def make_summary(pilot, full, full_cases, final_censuses, run_status, binary_hash):
    mount = full["mount_before"]
    env = (run_root / "environment.txt").read_text().splitlines()
    environment_lines = [line for line in env if line.startswith(("rustc ", "cargo ", "cc=", "cxx=", "Linux ", "Model name:", "CPU(s):", "MemTotal:", "MemAvailable:"))]
    lines = [
        "# Final matched KV storage benchmark",
        "",
        f"Run root: `{run_root}`",
        f"Base commit: `{run_status['head']}`; initial worktree was dirty; no selected source inputs changed during run: **PASS** ({run_status['source_hash_count']} SHA-256 entries, all captures identical).",
        f"Fresh release binary SHA-256: `{binary_hash}`; before pilot, before full, and after full identical: **PASS**.",
        f"Pilot: exit `{run_status['pilot_exit_status']}`, schema 2, 3 engine rows, `performance_result=false`; functional checks: **PASS**.",
        f"Full matrix: exit `{run_status['full_exit_status']}`, schema 2, 96 execution rows (72 measured + 24 warmups), 8 cases × 3 engines; report `performance_result=true`.",
        f"Filesystem: `{mount['target']}` / `{mount['fstype']}` / `{mount['source']}`; options `{mount['options']}`; exact dataset mount identity/options unchanged.",
        f"RocksDB: crate `{full['rocksdb_config']['crate_version']}`, native `{full['rocksdb_config']['native_version']}`, WAL sync enabled, 1 MiB write buffer, compression disabled.",
        f"CereDB: 1 MiB memtable + 1 MiB WAL; other settings `StorageConfig::default()`.",
        "",
        "## Per-case results",
        "",
        "Times are median `[min–max]` ns across three measured samples; warmups excluded. Rates divide actual timed-loop operation counts by median append/mutation timer only. `Experimental/RocksDB` time ratio below 1 means Experimental was faster; above 1 means slower. KV2 is current-run control, not historical baseline.",
    ]
    for case in full_cases:
        case_id = case["case"]
        lines += ["", f"### `{case_id}`", "", "#### Timed writes and natural state", "", "| Engine | Append ns [range] | Append ops/s | Append ops / calls / input B | Mutation ns [range] | Mutation ops/s | Mutation ops / calls / input B | Natural closed root B [range] | B/live byte | Actual final root B [range] |", "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|"]
        for engine in engines:
            item = case["engines"][engine]
            append = item["append_metrics"]
            mutation = item["mutation_metrics"]
            natural_ratio = item["natural_closed_bytes_per_live_logical_byte"]["median"]
            lines.append(
                f"| {engine_names[engine]} | {ns(item['phase_ns']['append_ns'])} | {item['append_ops_per_second_from_median_timer']:,.3f} | {append['operations']:,} / {append['batch_calls']:,} / {append['logical_bytes']:,} | {ns(item['phase_ns']['mutations_ns'])} | {item['mutation_ops_per_second_from_median_timer']:,.3f} | {mutation['operations']:,} / {mutation['batch_calls']:,} / {mutation['logical_bytes']:,} | {compact_bytes(item['natural_closed_total_file_bytes'])} | {natural_ratio:.6f} | {compact_bytes(item['actual_final_filesystem_total_file_bytes'])} |"
            )
        lines += ["", "#### Phase timings", "", "| Phase | KV2 inline | Experimental WAL | RocksDB |", "|---|---:|---:|---:|"]
        for field in phase_fields:
            label = field.removesuffix("_ns").replace("_", " ")
            values = [ns(case["engines"][engine]["phase_ns"][field]) for engine in engines]
            lines.append(f"| {label} | {values[0]} | {values[1]} | {values[2]} |")
        exp = case["engines"]["experimental_wal_values"]
        rocks = case["engines"]["rocks_db"]
        lines += ["", "#### Experimental cleaning and post-clean reads", "", f"Natural Experimental root: **{compact_bytes(exp['natural_closed_total_file_bytes'])} B**; post-clean Experimental root: **{compact_bytes(exp['post_clean_total_file_bytes'])} B**; natural RocksDB root: **{compact_bytes(rocks['natural_closed_total_file_bytes'])} B**. These are different lifecycle states; cleaned Experimental bytes are not presented as equivalent to natural RocksDB.", "", f"Post-clean Experimental B/live byte: median {exp['post_clean_bytes_per_live_logical_byte']['median']:.6f} (range {exp['post_clean_bytes_per_live_logical_byte']['min']:.6f}–{exp['post_clean_bytes_per_live_logical_byte']['max']:.6f}). Natural Experimental B/live byte: median {exp['natural_closed_bytes_per_live_logical_byte']['median']:.6f}.", "", "| Clean stat | Median [min–max] |", "|---|---:|"]
        for key, label in (("total_root_bytes_before", "Root bytes before"), ("observed_peak_bytes", "Sampled peak bytes"), ("total_root_bytes_after", "Root bytes after"), ("deferred_cleanup_bytes", "Deferred cleanup bytes"), ("live_records", "Live records"), ("live_logical_bytes", "Live key+value bytes"), ("copied_payload_bytes", "Copied payload bytes"), ("copied_transactions", "Copied transactions"), ("index_sstables_written", "Index SSTables written"), ("write_pause_ns", "Cleaner write pause ns"), ("elapsed_ns", "Cleaner elapsed ns")):
            lines.append(f"| {label} | {compact_bytes(exp['clean_stats'][key])} |")
        lines += ["", f"Warm reads each sample: natural 8 get calls/8 entries + 1 scan/8 entries; post-clean Experimental same 8/8 + 1/8. Natural reads use `warm_get_uncleaned_ns`/`warm_scan_uncleaned_ns`; post-clean reads use separate timers. Experimental post-clean reopen median/range: {ns(exp['phase_ns']['reopen_post_clean_ns'])} ns. Cleaner `write_pause_ns` is nested in `elapsed_ns` and is not double-counted in lifecycle sum.", "", "#### Experimental / RocksDB direction", "", "| Timed phase | Experimental/RocksDB median time ratio | Difference (Exp − RocksDB) ns | Direction |", "|---|---:|---:|---|"]
        for field in ("append_ns", "mutations_ns", "reopen_uncleaned_ns", "warm_get_uncleaned_ns", "warm_scan_uncleaned_ns", "lifecycle_sum_ns"):
            comparison = case["experimental_vs_rocksdb"][field]
            lines.append(f"| {field.removesuffix('_ns').replace('_', ' ')} | {comparison['experimental_over_rocksdb_time_ratio']:.6f}× | {comparison['experimental_minus_rocksdb_ns']:+,} | Experimental {comparison['direction']} |")
        lines.append(f"| Append throughput | {case['experimental_vs_rocksdb']['append_rate_ratio']:.6f}× rate | — | higher rate favors Experimental |")
        lines.append(f"| Mutation throughput | {case['experimental_vs_rocksdb']['mutation_rate_ratio']:.6f}× rate | — | higher rate favors Experimental |")
        lines += ["", f"Footprint ratios: natural Experimental / natural RocksDB = {case['experimental_natural_vs_natural_rocksdb_footprint']:.6f}×; post-clean Experimental / natural RocksDB = {case['experimental_cleaned_vs_natural_rocksdb_footprint']:.6f}× (non-equivalent lifecycle stages).", "", "Observed level counts and evidence:"]
        for engine in engines:
            item = case["engines"][engine]
            evidence = "; ".join(f"sample {level['sample_ordinal']}: {json.dumps(level['levels'], sort_keys=True)} ({level['evidence']})" for level in item["levels"])
            lines.append(f"- {engine_names[engine]}: {evidence}")
    actual_summary = {}
    for engine in engines:
        rows = [item for item in final_censuses if item["engine"] == engine]
        actual_summary[engine] = {
            "execution_roots_audited_without_engine_open": len(rows),
            "roots_matching_latest_reported_closed_total": sum(item["delta_actual_minus_reported_bytes"] == 0 for item in rows),
            "total_file_bytes_delta_min": min(item["delta_actual_minus_reported_bytes"] for item in rows),
            "total_file_bytes_delta_max": max(item["delta_actual_minus_reported_bytes"] for item in rows),
        }
    lines += [
        "",
        "## Independent audit and limits",
        "",
        "- Pilot schema-2 functional run completed first; all three engines verified full scan (19 rows), 16 deterministic point gets, absent deleted/never-present probes, 8 natural gets/entries and one 8-entry scan. Experimental clean/reopen repeated exact verification and warm-read counts.",
        "- Full report rows independently checked against eight requested case shapes, all 96 unique retained stores, 72 measured rows, 24 warmups, three samples per engine/case, one warmup per engine/case, and rotating engine order.",
        "- Operation/batch/input-byte/live-row totals were recomputed from each execution row's case dimensions and mutation policy; all rows match. Full scans and read counts match expected live rows and deterministic bounded workloads.",
        "- Every reported closed census category sum, file count, bytes/live ratio, and `lifecycle_sum_ns` was recomputed. Lifecycle uses each non-overlapping phase once; cleaner elapsed counted once; nested pause excluded from second addition.",
        "- Independently walked every retained store root using filesystem metadata only, without opening any DB engine. All Experimental stores have exactly one remaining generation and no `other`-category files. Actual final root census is retained per store in `audit.json`; RocksDB post-reopen final census is kept distinct from its earlier natural closed census/live_files observation.",
        f"- Actual root census match summary (including warmups): `{json.dumps(actual_summary, sort_keys=True)}`.",
        "- Host: " + "; ".join(environment_lines) + ". No CPU affinity or power-loss test. Three low-record samples per case; warm cache and Btrfs CoW/compression affect results. This is not sustained-capacity evidence.",
        "- CereDB uses public async APIs; RocksDB uses synchronous calls on benchmark thread with sync WAL. CereDB's write API clones owned vectors; RocksDB takes borrowed slices and copies internally. Those API costs remain inside write timers; APIs are not identical at allocation/scheduling boundaries.",
        "- Append/mutation rates count only actual timed append/mutation operations, never unclocked setup or probes. They are derived from median phase time, not per-operation latency distributions.",
        "- Experimental clean-and-repack is explicit; its pause, elapsed time, sampled peak, before/after/deferred bytes, payload bytes, transaction count, and index SSTable count are reported. Sampled peak is not continuous. Cleaned Experimental state is not RocksDB's natural closed state.",
        "- Natural KV2 and RocksDB were not force-flushed or manually compacted. RocksDB drop cancels/unschedules pending jobs and waits outstanding jobs; it does not prove all maintenance completed. Level metadata is observed at the report's stated point, not equalized.",
        "- The pre-reopen natural census and RocksDB live_files-after-reopen level counts are distinct moments. The final on-disk census is independently captured after process close; new metadata can make it differ from the earlier closed census.",
        "- Fresh-open timers include startup/recovery work; no profiler attributes causal costs. FTS is not included and no FTS performance result is claimed. The earlier patterned 256 B/1k run with its 251-value pattern differs from this fixed-seed high-entropy workload and current source; qualitative context only, never a numerical before/after baseline. No etcd comparison.",
        "- These results establish this matched local run only: no power-loss safety, external service parity, general speedup, or capacity claim.",
    ]
    return "\n".join(lines) + "\n"


check(run_root.is_relative_to(repo_root / "target"), "runtime evidence must remain under target")
check(read_status(run_root / "build-status.txt", "build_exit_status")["build_exit_status"] == "0", "release build status invalid")
pilot_status = read_status(run_root / "pilot-status.txt", "pilot_exit_status")
full_status = read_status(run_root / "full-status.txt", "full_exit_status")
run_status = {
    "head": next(line.split("=", 1)[1] for line in (run_root / "preflight.txt").read_text().splitlines() if line.startswith("HEAD=")),
    "pilot_exit_status": pilot_status["pilot_exit_status"],
    "full_exit_status": full_status["full_exit_status"],
}
check(run_status["head"] == "fa65cde5d1642a54a576b89f0f2c88dba8a13fb5", "base HEAD differs from requested qualification")
hash_files = [
    "source-hashes-before-build.sha256",
    "source-hashes-after-build.sha256",
    "source-hashes-before-pilot.sha256",
    "source-hashes-before-full.sha256",
    "source-hashes-after-full.sha256",
    "source-hashes-post-audit.sha256",
]
hash_contents = [(run_root / name).read_bytes() for name in hash_files]
check(all(content == hash_contents[0] for content in hash_contents[1:]), "source freeze hash captures changed")
source_lines = hash_contents[0].decode().splitlines()
check(len(source_lines) == 66, f"expected 66 source hash entries, got {len(source_lines)}")
run_status["source_hash_count"] = len(source_lines)
binary_hash_files = ["binary-before-pilot.sha256", "binary-before-full.sha256", "binary-after-full.sha256", "binary-post-audit.sha256"]
binary_contents = [(run_root / name).read_bytes() for name in binary_hash_files]
check(all(content == binary_contents[0] for content in binary_contents[1:]), "release binary SHA-256 changed")
binary_hash = binary_contents[0].decode().split()[0]
check(binary_hash == "5b06ca38c67b4dd16a4336fe5b2fe70f196e79c8d2ba6d63f4e962ab3b71f6c3", "release binary hash differs from captured build")
input_hashes = {}
for line in (run_root / "benchmark-input-checks.txt").read_text().splitlines():
    digest, path = line.split("  ", 1)
    input_hashes[path] = digest
check(input_hashes["tools/storage_compare/src/bin/wal_values.rs"] == "9441863cd42a5dc8ebc9926c0326803aedc24553c66f9dafe36c31d49480babc", "benchmark harness hash mismatch")
check(input_hashes["tools/storage_compare/README.md"] == "a22873659c8eaaa7594bafb63bf5ca8a19e8379cc1de6e6f67afe0936d2e143e", "benchmark README hash mismatch")
run_status["binary_sha256"] = binary_hash
run_status["source_hash_captures_identical"] = True

reports = {}
audits = {}
for mode in ("pilot", "full"):
    dataset_root = run_root / mode
    report = read_report(dataset_root / "report.json")
    for capture_name in (("pilot-mount-before.txt", "pilot-mount-after.txt") if mode == "pilot" else ("full-mount-before.txt", "full-mount-after.txt")):
        external_mount = parse_findmnt(run_root / capture_name)
        expected_mount = report["mount_before"]
        check(all(external_mount[key] == expected_mount[key] for key in external_mount), f"{mode}: external findmnt mismatch in {capture_name}")
    check(parse_findmnt(run_root / ("pilot-mount-before.txt" if mode == "pilot" else "full-mount-before.txt")) == parse_findmnt(run_root / ("pilot-mount-after.txt" if mode == "pilot" else "full-mount-after.txt")), f"{mode}: external mount capture changed")
    rows, by_case_engine, final_censuses = validate_report(report, mode, dataset_root)
    reports[mode] = report
    audits[mode] = {
        "report_path": str(dataset_root / "report.json"),
        "schema_version": report["schema_version"],
        "mode": report["mode"],
        "performance_result": report["performance_result"],
        "execution_rows": len(rows),
        "measured_rows": sum(not row["warmup"] for row in rows),
        "warmup_rows": sum(row["warmup"] for row in rows),
        "case_ids": [item[0] for item in ([("v4096_n20_b1", 20, 4096, 1)] if mode == "pilot" else expected_cases)],
        "retained_unique_store_paths": len({row["store_path"] for row in rows}),
        "mount": report["mount_before"],
        "actual_final_filesystem_census_without_engine_open": final_censuses,
    }

full_case_summaries = []
for case_id, _, _, _ in expected_cases:
    groups = {engine: [row for row in audits["full"]["actual_final_filesystem_census_without_engine_open"] if row["case"] == case_id and row["engine"] == engine] for engine in engines}
    case_rows = {engine: [row for row in reports["full"]["executions"] if row["case"]["id"] == case_id and row["engine"] == engine] for engine in engines}
    full_case_summaries.append(summarize_case(case_id, case_rows, audits["full"]["actual_final_filesystem_census_without_engine_open"]))

summary = {
    "status": run_status,
    "benchmark_input_hashes": input_hashes,
    "pilot": audits["pilot"],
    "full": {
        key: value for key, value in audits["full"].items() if key != "actual_final_filesystem_census_without_engine_open"
    },
    "full_cases": full_case_summaries,
    "audit_checks": {
        "pilot_report_schema_and_functionality": "PASS",
        "full_report_schema_and_execution_counts": "PASS",
        "matched_workload_counts_batches_bytes_and_live_rows": "PASS",
        "verification_and_natural_post_clean_read_counts": "PASS",
        "mount_source_target_fstype_and_normalized_options": "PASS",
        "lifecycle_nonoverlap_and_clean_elapsed_once": "PASS",
        "reported_census_categories_and_ratios": "PASS",
        "clean_stats_and_one_remaining_generation": "PASS",
        "all_retained_roots_filesystem_censused_without_engine_open": "PASS",
        "source_hash_freeze_and_binary_hash_stability": "PASS",
        "numeric_process_exit_statuses": "PASS",
    },
    "notes": [
        "Natural closed census, post-clean closed census, and post-reopen final filesystem census are separate observations.",
        "RocksDB level counts are live_files observations after timed reopen; CereDB levels are public manifest snapshots while closed.",
        "No database engine was opened during the independent final filesystem census.",
        "Performance rates use actual operations divided by median append/mutation phase time only.",
        "No historical benchmark was used as a numerical baseline; no FTS or etcd result is claimed.",
    ],
}
summary["full"]["actual_final_filesystem_census_without_engine_open"] = audits["full"]["actual_final_filesystem_census_without_engine_open"]
(run_root / "audit.json").write_text(json.dumps(summary, indent=2) + "\n")
(run_root / "summary.md").write_text(make_summary(reports["pilot"], reports["full"], full_case_summaries, audits["full"]["actual_final_filesystem_census_without_engine_open"], run_status, binary_hash))
print(json.dumps({
    "audit": "PASS",
    "pilot_rows": audits["pilot"]["execution_rows"],
    "full_rows": audits["full"]["execution_rows"],
    "full_measured": audits["full"]["measured_rows"],
    "full_warmups": audits["full"]["warmup_rows"],
    "source_hash_entries": run_status["source_hash_count"],
    "binary_sha256": binary_hash,
    "experimental_remaining_generations": sum(len(row["remaining_generation_dirs"]) == 1 for row in audits["full"]["actual_final_filesystem_census_without_engine_open"] if row["engine"] == "experimental_wal_values"),
    "actual_census_roots": len(audits["full"]["actual_final_filesystem_census_without_engine_open"]),
    "outputs": [str(run_root / "audit.json"), str(run_root / "summary.md")],
}, indent=2))
