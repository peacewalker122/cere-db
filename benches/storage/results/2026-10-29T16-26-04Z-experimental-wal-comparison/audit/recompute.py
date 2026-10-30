#!/usr/bin/env python3
import json
import math
import os
import sys

ARCH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW = sys.argv[1] if len(sys.argv) > 1 else os.path.join(ARCH, "raw", "results.json")
DATA_DIR = sys.argv[2] if len(sys.argv) > 2 else ""
HEAD = sys.argv[3] if len(sys.argv) > 3 else ""

failures = []


def check(name, condition, detail=""):
    if condition:
        print(f"[OK]   {name}")
    else:
        print(f"[FAIL] {name} {detail}")
        failures.append(f"{name} {detail}")


def close(a, b, rel=1e-12):
    if a is None or b is None:
        return a is None and b is None
    if a == b:
        return True
    scale = max(abs(a), abs(b), 1e-300)
    return abs(a - b) / scale <= rel


def median(values):
    values = sorted(values)
    return None if not values else values[len(values) // 2]


with open(RAW, "rb") as handle:
    doc = json.load(handle)

check("schema", doc.get("schema") == "ceredb-experimental-wal-comparison/2", doc.get("schema"))
check("lifecycle_basis", doc.get("lifecycle_basis") == "explicit_phase_sum", doc.get("lifecycle_basis"))
if HEAD:
    check("git_head_matches_provenance", doc.get("git_head") == HEAD, f"{doc.get('git_head')} != {HEAD}")
check("profile", doc.get("profile") == "small-tables", doc.get("profile"))
check("repetitions", doc.get("repetitions") == 3, doc.get("repetitions"))
check("warmups", doc.get("warmups") == 1, doc.get("warmups"))
check("seed", doc.get("seed") == 0x6A09E667F3BCC909, doc.get("seed"))
check("mutation_percents", (doc.get("overwrite_percent"), doc.get("delete_percent"), doc.get("reinsert_percent")) == (25, 10, 50))

expected_config = {
    "memtable_size_threshold": 1024 * 1024,
    "sstable_block_size": 4096,
    "max_level0_files": 2,
    "target_sstable_bytes": 16 * 1024,
    "level_base_bytes": 64 * 1024,
    "level_size_multiplier": 4,
    "wal_segment_size": 256 * 1024,
}
config = doc.get("config", {})
for key, value in expected_config.items():
    check(f"config.{key}", config.get(key) == value, f"{config.get(key)} != {value}")

filesystem = doc.get("filesystem", {})
check("filesystem.persistent", filesystem.get("fstype") not in ("tmpfs", "ramfs", "devtmpfs"), filesystem.get("fstype"))
check("filesystem.mount", filesystem.get("mount_point") == "/home", filesystem.get("mount_point"))
check("filesystem.fresh", filesystem.get("fresh_run_root") is True and filesystem.get("existing_paths_deleted") is False)

expected_cases = [
    (64, 16384, 1),
    (64, 16384, 64),
    (4096, 512, 1),
    (4096, 512, 64),
    (65536, 64, 1),
    (65536, 64, 16),
]
cases = doc.get("cases", [])
observed_cases = sorted((c["value_bytes"], c["records"], c["batch_size"]) for c in cases)
check("case_count", len(cases) == 6, len(cases))
check("case_matrix", observed_cases == sorted(expected_cases), observed_cases)
check("case_names", all(c.get("name") if "name" in c else True for c in cases))

total_measured = 0
total_warmups = 0
phase_errors = 0
census_errors = 0
amp_errors = 0
stats_errors = 0
workload_errors = 0
clean_errors = 0
generation_errors = 0
read_errors = 0
summary_rows = []

for case in cases:
    label = f"raw{case['value_bytes']}B/{case['records']}rec/batch{case['batch_size']}"
    samples = case["samples"]
    reference = [s for s in samples if s["engine"] == "reference-kv2"]
    experimental = [s for s in samples if s["engine"] == "experimental-wal"]
    check_set = (
        len(samples) == 6
        and len(reference) == 3
        and len(experimental) == 3
        and sorted(s["sample_index"] for s in reference) == [0, 1, 2]
        and sorted(s["sample_index"] for s in experimental) == [0, 1, 2]
        and all(s["warmup"] is False for s in samples)
    )
    if not check_set:
        check(f"{label}.sample_set", False, len(samples))
    total_measured += len(samples)
    total_warmups += 2

    for sample in samples:
        total = sample["root"]["preclean"]["total_bytes"]
        parts = sum(
            sample["root"]["preclean"][key]
            for key in ("wal_bytes", "sstable_bytes", "metadata_bytes", "other_bytes")
        )
        if total != parts:
            census_errors += 1
        workload = sample["workload"]
        if workload["live_logical_bytes"] != workload["live_records"] * 24 + workload["live_value_bytes"]:
            workload_errors += 1
        if workload["mutations"] != (
            workload["append_mutations"]
            + workload["overwrite_mutations"]
            + workload["delete_mutations"]
            + workload["reinsert_mutations"]
        ):
            workload_errors += 1
        expected_batches = math.ceil(workload["append_mutations"] / sample["batch_size"]) + math.ceil(
            (workload["overwrite_mutations"] + workload["delete_mutations"] + workload["reinsert_mutations"])
            / sample["batch_size"]
        )
        if workload["batches"] != expected_batches:
            workload_errors += 1
        closed = sample["closed"]
        for stage in ("preclean", "postclean"):
            observation = closed.get(stage)
            if observation is None:
                continue
            closed_total = observation["root"]["total_bytes"]
            closed_parts = sum(
                observation["root"][key]
                for key in ("wal_bytes", "sstable_bytes", "metadata_bytes", "other_bytes")
            )
            if closed_total != closed_parts:
                census_errors += 1
            if observation["root"]["file_count"] <= 0:
                census_errors += 1
        for stage in ("preclean", "postclean", "final"):
            value = sample["root"]["space_amplification"][stage]
            if value is not None and not close(value, sample["root"][stage]["total_bytes"] / workload["live_logical_bytes"], 1e-9):
                amp_errors += 1
        elapsed = sample["elapsed_ns"]
        parts_sum = (
            elapsed["ingest"]
            + elapsed["mutate"]
            + elapsed["settle"]
            + elapsed["shutdown_preclean"]
            + elapsed["reopen_preclean"]
            + (elapsed["clean_elapsed"] or 0)
            + (elapsed["shutdown_postclean"] or 0)
            + (elapsed["reopen_postclean"] or 0)
            + elapsed["shutdown_final"]
        )
        if elapsed["lifecycle_phase_sum"] != parts_sum:
            phase_errors += 1
        reads = sample["reads"]
        if reads is not None:
            if reads["point_gets"] != min(64, workload["live_records"]):
                read_errors += 1
            if reads["point_hits"] != reads["point_gets"]:
                read_errors += 1
            if reads["scan_entries"] != min(128, workload["live_records"]):
                read_errors += 1
        if sample["engine"] == "experimental-wal":
            clean = sample["clean"]
            if clean is None:
                clean_errors += 1
                continue
            if clean["live_records"] != workload["live_records"]:
                clean_errors += 1
            if clean["live_logical_bytes"] != workload["live_logical_bytes"]:
                clean_errors += 1
            if clean["copied_payload_bytes"] != workload["live_value_bytes"]:
                clean_errors += 1
            if clean["new_generation"] != clean["old_generation"] + 1:
                clean_errors += 1
            if not (clean["elapsed_ns"] >= clean["write_pause_ns"] > 0):
                clean_errors += 1
            if clean["deferred_cleanup_bytes"] != 0:
                clean_errors += 1
            peak_low = max(clean["total_root_bytes_before"], clean["total_root_bytes_after"])
            peak_high = clean["total_root_bytes_before"] + clean["total_root_bytes_after"]
            if not (peak_low <= clean["observed_peak_bytes"] <= peak_high):
                clean_errors += 1
            if len(closed["postclean"]["generations"]) != 1:
                generation_errors += 1
            if len(closed["preclean"]["generations"]) < 1:
                generation_errors += 1
        else:
            if sample["clean"] is not None or sample["closed"]["postclean"] is not None:
                clean_errors += 1

    for engine in ("reference", "experimental"):
        summary = case["summary"][engine]
        engine_samples = [s for s in samples if s["engine"] == summary["engine"]]
        for field in ("ingest", "mutate", "settle", "lifecycle_phase_sum", "shutdown_preclean", "reopen_preclean", "shutdown_final"):
            values = [s["elapsed_ns"][field] for s in engine_samples]
            stats = summary["elapsed_ns"][field]
            if stats["count"] != len(values) or stats["median_ns"] != median(values) or stats["min_ns"] != min(values) or stats["max_ns"] != max(values):
                stats_errors += 1
        for field in ("preclean_total_bytes", "final_total_bytes"):
            values = [
                s["root"]["preclean"]["total_bytes"] if field == "preclean_total_bytes" else s["root"]["final"]["total_bytes"]
                for s in engine_samples
            ]
            stats = summary["space"][field]
            if stats["median_ns"] != median(values) or stats["min_ns"] != min(values) or stats["max_ns"] != max(values):
                stats_errors += 1
        amps = [s["root"]["space_amplification"]["preclean"] for s in engine_samples]
        stats = summary["space"]["space_amplification_preclean"]
        if not close(stats["median"], median(amps), 1e-9):
            stats_errors += 1

    ref = [s for s in samples if s["engine"] == "reference-kv2"]
    exp = [s for s in samples if s["engine"] == "experimental-wal"]

    def med(rows, path):
        values = []
        for row in rows:
            cursor = row
            for key in path:
                cursor = cursor[key]
            values.append(cursor)
        return median(values)

    row = {
        "case": label,
        "value_bytes": case["value_bytes"],
        "records": case["records"],
        "batch_size": case["batch_size"],
        "live_records": ref[0]["workload"]["live_records"],
        "live_logical_bytes": ref[0]["workload"]["live_logical_bytes"],
        "mutations": ref[0]["workload"]["mutations"],
        "batches": ref[0]["workload"]["batches"],
        "input_bytes": ref[0]["workload"]["logical_input_bytes"],
    }
    for name, path in (
        ("ingest", ("elapsed_ns", "ingest")),
        ("mutate", ("elapsed_ns", "mutate")),
        ("settle", ("elapsed_ns", "settle")),
        ("preclean_shutdown", ("elapsed_ns", "shutdown_preclean")),
        ("preclean_reopen", ("elapsed_ns", "reopen_preclean")),
        ("final_shutdown", ("elapsed_ns", "shutdown_final")),
        ("phase_sum", ("elapsed_ns", "lifecycle_phase_sum")),
        ("point_get", ("elapsed_ns", "point_get")),
        ("scan", ("elapsed_ns", "scan")),
    ):
        row[f"ref_{name}_median_ns"] = med(ref, path)
        row[f"exp_{name}_median_ns"] = med(exp, path)
    row["exp_clean_pause_median_ns"] = med(exp, ("elapsed_ns", "clean_pause"))
    row["exp_clean_elapsed_median_ns"] = med(exp, ("elapsed_ns", "clean_elapsed"))
    row["exp_postclean_shutdown_median_ns"] = med(exp, ("elapsed_ns", "shutdown_postclean"))
    row["exp_postclean_reopen_median_ns"] = med(exp, ("elapsed_ns", "reopen_postclean"))
    for name, field in (
        ("ingest", "ingest"),
        ("mutate", "mutate"),
        ("preclean_reopen", "reopen_preclean"),
        ("phase_sum", "lifecycle_phase_sum"),
    ):
        ref_values = sorted(r["elapsed_ns"][field] for r in ref)
        exp_values = sorted(r["elapsed_ns"][field] for r in exp)
        row[f"ref_{name}_range_ns"] = [ref_values[0], ref_values[-1]]
        row[f"exp_{name}_range_ns"] = [exp_values[0], exp_values[-1]]
    for stage in ("preclean", "postclean", "final"):
        ref_values = sorted(r["root"][stage]["total_bytes"] for r in ref if r["root"][stage] is not None)
        exp_values = sorted(r["root"][stage]["total_bytes"] for r in exp if r["root"][stage] is not None)
        row[f"ref_{stage}_total_median"] = median(ref_values)
        row[f"exp_{stage}_total_median"] = median(exp_values)
        row[f"ref_{stage}_total_range"] = [ref_values[0], ref_values[-1]] if ref_values else None
        row[f"exp_{stage}_total_range"] = [exp_values[0], exp_values[-1]] if exp_values else None
        def amp_range(rows, stage):
            values = [r["root"]["space_amplification"][stage] for r in rows if r["root"]["space_amplification"][stage] is not None]
            return [min(values), max(values)] if values else None

        row[f"ref_{stage}_amp_range"] = amp_range(ref, stage)
        row[f"exp_{stage}_amp_range"] = amp_range(exp, stage)
    row["ref_preclean_breakdown_median"] = {
        key: med(ref, ("root", "preclean", key))
        for key in ("total_bytes", "wal_bytes", "sstable_bytes", "metadata_bytes", "other_bytes", "file_count")
    }
    row["exp_preclean_breakdown_median"] = {
        key: med(exp, ("root", "preclean", key))
        for key in ("total_bytes", "wal_bytes", "sstable_bytes", "metadata_bytes", "other_bytes", "file_count")
    }
    row["exp_postclean_breakdown_median"] = {
        key: med(exp, ("root", "postclean", key))
        for key in ("total_bytes", "wal_bytes", "sstable_bytes", "metadata_bytes", "other_bytes", "file_count")
    }
    row["ref_preclean_levels"] = ref[0]["closed"]["preclean"]["levels"]
    row["exp_preclean_levels"] = exp[0]["closed"]["preclean"]["levels"]
    row["exp_postclean_levels"] = exp[0]["closed"]["postclean"]["levels"]
    row["ref_preclean_breakdown_range"] = {
        key: [min(r["root"]["preclean"][key] for r in ref), max(r["root"]["preclean"][key] for r in ref)]
        for key in ("wal_bytes", "sstable_bytes", "metadata_bytes", "other_bytes")
    }
    row["exp_preclean_breakdown_range"] = {
        key: [min(r["root"]["preclean"][key] for r in exp), max(r["root"]["preclean"][key] for r in exp)]
        for key in ("wal_bytes", "sstable_bytes", "metadata_bytes", "other_bytes")
    }
    row["exp_clean_median"] = {
        key: med(exp, ("clean", key))
        for key in (
            "write_pause_ns",
            "elapsed_ns",
            "copied_payload_bytes",
            "copied_transactions",
            "index_sstables_written",
            "total_root_bytes_before",
            "observed_peak_bytes",
            "total_root_bytes_after",
            "deferred_cleanup_bytes",
            "live_records",
            "live_logical_bytes",
        )
    }
    row["ratios"] = {
        "ingest_exp_over_ref_median": row["exp_ingest_median_ns"] / row["ref_ingest_median_ns"],
        "mutate_exp_over_ref_median": row["exp_mutate_median_ns"] / row["ref_mutate_median_ns"],
        "preclean_reopen_exp_over_ref_median": row["exp_preclean_reopen_median_ns"] / row["ref_preclean_reopen_median_ns"],
        "phase_sum_exp_over_ref_median": row["exp_phase_sum_median_ns"] / row["ref_phase_sum_median_ns"],
        "point_get_exp_over_ref_median": row["exp_point_get_median_ns"] / row["ref_point_get_median_ns"],
        "scan_exp_over_ref_median": row["exp_scan_median_ns"] / row["ref_scan_median_ns"],
        "preclean_total_exp_over_ref_median": row["exp_preclean_total_median"] / row["ref_preclean_total_median"],
        "postclean_exp_over_ref_preclean_median": row["exp_postclean_total_median"] / row["ref_preclean_total_median"],
        "final_total_exp_over_ref_median": row["exp_final_total_median"] / row["ref_final_total_median"],
    }
    summary_rows.append(row)

check("measured_samples_36", total_measured == 36, total_measured)
check("census_breakdown_sums", census_errors == 0, census_errors)
check("workload_identities", workload_errors == 0, workload_errors)
check("space_amplification_recompute", amp_errors == 0, amp_errors)
check("lifecycle_phase_sum_recompute", phase_errors == 0, phase_errors)
check("summary_stats_recompute", stats_errors == 0, stats_errors)
check("clean_stats_identities", clean_errors == 0, clean_errors)
check("generations_reclaimed", generation_errors == 0, generation_errors)
check("read_operation_counts", read_errors == 0, read_errors)

fts = doc.get("fts")
check("fts_present", isinstance(fts, dict))
if isinstance(fts, dict):
    check("fts_role", fts.get("role") == "functional_equivalence_only", fts.get("role"))
    check("fts_equivalent", fts.get("equivalent") is True, fts.get("equivalent"))
    check("fts_documents", fts.get("documents") == 128, fts.get("documents"))
    baseline = fts["baseline"]["preclean_observation"]["observation"]
    pre = fts["experimental"]["preclean_observation"]["observation"]
    post = fts["experimental"]["postclean_observation"]["observation"]
    check("fts_documents_checked", baseline["documents_checked"] == 128 and pre["documents_checked"] == 128 and post["documents_checked"] == 128)
    check("fts_boolean_and", baseline["boolean_and_ids"] == pre["boolean_and_ids"] == post["boolean_and_ids"])
    check("fts_boolean_or", baseline["boolean_or_ids"] == pre["boolean_or_ids"] == post["boolean_or_ids"])
    check("fts_ranked_and", baseline["ranked_and"] == pre["ranked_and"] == post["ranked_and"])
    check("fts_ranked_or", baseline["ranked_or"] == pre["ranked_or"] == post["ranked_or"])
    check("fts_and_nonempty", len(baseline["boolean_and_ids"]) > 0, len(baseline["boolean_and_ids"]))
    check("fts_or_nonempty", len(baseline["boolean_or_ids"]) > 0, len(baseline["boolean_or_ids"]))

if DATA_DIR:
    missing = []
    for case in cases:
        base = os.path.join(DATA_DIR, f"{case['case']}-{case['batch_size']}")
        for engine in ("reference", "experimental"):
            warmup_path = os.path.join(base, "warmup-0", engine)
            if not os.path.isdir(warmup_path):
                missing.append(warmup_path)
        for engine in ("reference-kv2", "experimental-wal"):
            for index in range(3):
                sample_path = os.path.join(base, f"sample-{index}", engine)
                if not os.path.isdir(sample_path):
                    missing.append(sample_path)
    check("sample_and_warmup_dirs_present", not missing, missing[:3])
    warmup_dirs = 0
    for case in cases:
        base = os.path.join(DATA_DIR, f"{case['case']}-{case['batch_size']}")
        for engine in ("reference", "experimental"):
            if os.path.isdir(os.path.join(base, "warmup-0", engine)):
                warmup_dirs += 1
    check("warmup_executions_12", warmup_dirs == 12, warmup_dirs)

with open(os.path.join(ARCH, "audit", "recomputed-summary.json"), "w") as handle:
    json.dump(summary_rows, handle, indent=2)

print()
print("RECOMPUTED TABLE")
header = ("case", "ingest x", "mutate x", "reopen x", "phase x", "preclean x", "postclean/preclean_ref x", "exp_amp_post", "ref_amp_pre")
print(" | ".join(header))
for row in summary_rows:
    print(
        " | ".join(
            [
                row["case"],
                f"{row['ratios']['ingest_exp_over_ref_median']:.2f}",
                f"{row['ratios']['mutate_exp_over_ref_median']:.2f}",
                f"{row['ratios']['preclean_reopen_exp_over_ref_median']:.2f}",
                f"{row['ratios']['phase_sum_exp_over_ref_median']:.2f}",
                f"{row['ratios']['preclean_total_exp_over_ref_median']:.2f}",
                f"{row['ratios']['postclean_exp_over_ref_preclean_median']:.2f}",
                f"{(row['exp_postclean_amp_range'] or [None])[0]:.3f}" if row["exp_postclean_amp_range"] else "n/a",
                f"{row['ref_preclean_amp_range'][0]:.3f}",
            ]
        )
    )

print()
if failures:
    print(f"AUDIT RESULT: FAIL ({len(failures)})")
    for item in failures:
        print(f"  - {item}")
    sys.exit(1)
print("AUDIT RESULT: PASS")
