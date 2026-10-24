#!/usr/bin/env bash
set -u

root=${1:?usage: fts_scale_assess.sh RUN_ROOT}
assessment="$root/assessment"
result_dir="benches/storage/results/2026-10-24T15-24-49Z-fts-1m"
mkdir -p "$assessment"
exec 9>"$root/.assessment-worker.lock"
flock -n 9 || exit 73
printf '%s\n' "$$" >"$assessment/worker.pid"
printf 'worker_pid=%s\nstarted_utc=%s\n' "$$" "$(date -u +%FT%TZ)" >>"$assessment/worker.txt"

emit_failure() {
  local reason=$1
  printf '{"status":"FAILED_PARTIAL","reason":"%s","worker_pid":%s,"updated_utc":"%s"}\n' "$reason" "$$" "$(date -u +%FT%TZ)" >"$assessment/assessment.json"
  printf '\n## Automatic assessment\n\nStatus: FAILED_PARTIAL\n\nReason: %s\n' "$reason" >"$result_dir/assessment.md"
}

printf 'Waiting for seed lock release: %s/.lock\n' "$root" >>"$assessment/worker.txt"
flock "$root/.lock" true
seed_pid=$(cat "$root/pid")
if kill -0 "$seed_pid" 2>/dev/null || [[ ! -s "$root/completion.json" ]]; then
  emit_failure "seed remains active or completion.json is missing after lock release"
  exit 1
fi
if ! /usr/bin/python3 - "$root" <<'PY'
import json, pathlib, sys
r = pathlib.Path(sys.argv[1]); m = json.loads((r / "metadata.json").read_text()); c = json.loads((r / "completion.json").read_text()); lines = (r / "progress.jsonl").read_text().splitlines(); last = json.loads(lines[-1]) if lines else {}; t = (r / "stderr.log").read_text(errors="replace")
assert m.get("status") == "COMPLETE", "metadata is not COMPLETE"
assert c.get("status") == "COMPLETE" and c.get("shutdown") == "clean", "completion marker is not clean"
assert c.get("acknowledged_seed_writes") == 1_000_000 and last.get("acknowledged_seed_writes") == 1_000_000, "million acknowledgements not reached"
assert "Exit status: 0" in t, "GNU time does not confirm runner exit 0"
source_line = pathlib.Path("benches/storage/results/2026-10-24T15-24-49Z-fts-1m/source.txt").read_text().strip()
import hashlib
assert source_line == "source_sha256=" + hashlib.sha256(pathlib.Path("examples/fts_scale.rs").read_bytes()).hexdigest(), "seed source fingerprint changed"
PY
then
  emit_failure "completion marker, million acknowledgements, or GNU time clean-exit validation failed"
  exit 1
fi

metadata="$assessment/assessment.json"
python3 - "$root" "$metadata" "$$" <<'PY'
import json, pathlib, sys
r=pathlib.Path(sys.argv[1]); p=pathlib.Path(sys.argv[2]); c=json.loads((r/"completion.json").read_text()); m=json.loads((r/"metadata.json").read_text())
import hashlib
sha=lambda f: hashlib.sha256(pathlib.Path(f).read_bytes()).hexdigest()
cmd=pathlib.Path("benches/storage/results/2026-10-24T15-24-49Z-fts-1m/command.txt")
p.write_text(json.dumps({"status":"RUNNING","worker_pid":int(sys.argv[3]),"seed_pid":int((r/"pid").read_text()),"seed_completion":c,"seed_metadata":m,"seed_command_file":str(cmd),"seed_command":cmd.read_text().strip(),"source_sha256":sha("examples/fts_scale.rs"),"seed_binary_sha256":sha("target/release/examples/fts_scale"),"config":{"memtable_size_threshold_bytes":16777216},"resources":{"assessment_memory_limit_bytes":4294967296,"open_timeout_seconds":180,"query_timeout_seconds":60},"stages":{},"seed_progress_file":str(r/"progress.jsonl"),"seed_stdout_file":str(r/"stdout.log"),"seed_stderr_file":str(r/"stderr.log")},indent=2)+"\n")
PY

run_stage() {
  local stage=$1 limit=$2 repeat=$3 n=1
  while (( n <= repeat )); do
    local label="$stage-$n" out="$assessment/$stage-$n.json" rc=0
    timeout --signal=TERM --kill-after=5s "${limit}s" prlimit --as=4294967296 -- target/release/examples/fts_scale_assess "$root/db" "$stage" "$out" 1000000 >"$assessment/$label.stdout" 2>"$assessment/$label.stderr" || rc=$?
    if (( rc != 0 )); then
      local kind=QUERY_FAILED
      [[ "$stage" == open ]] && kind=OPEN_FAILED
      (( rc == 124 )) && kind=TIMEOUT
      (( rc == 137 )) && kind=RESOURCE_OR_FORCED_KILL
      printf '{"stage":"%s","status":"%s","exit_code":%s,"stderr_file":"%s"}\n' "$label" "$kind" "$rc" "$assessment/$label.stderr" >"$assessment/$label.status.json"
      emit_failure "$label $kind; inspect its stderr log"
      return 1
    fi
    if [[ "$stage" != open ]] && ! python3 - "$out" <<'PY'
import json, pathlib, sys
assert json.loads(pathlib.Path(sys.argv[1]).read_text()).get("status") == "COMPLETE"
PY
    then
      printf '{"stage":"%s","status":"INCORRECT_RESULT"}\n' "$label" >"$assessment/$label.status.json"
      emit_failure "$label returned an incorrect bounded result"
      return 1
    fi
    python3 - "$metadata" "$label" "$out" <<'PY'
import json, pathlib, sys
p=pathlib.Path(sys.argv[1]); d=json.loads(p.read_text()); d["stages"][sys.argv[2]]=json.loads(pathlib.Path(sys.argv[3]).read_text()); p.write_text(json.dumps(d,indent=2)+"\n")
PY
    local sample_type=timed
    (( n == 1 && repeat > 1 )) && sample_type=warmup
    printf '{"stage":"%s","status":"COMPLETE","sample_type":"%s","timeout_seconds":%s,"memory_limit_bytes":4294967296}\n' "$label" "$sample_type" "$limit" >"$assessment/$label.status.json"
    ((n+=1))
  done
}

run_stage open 180 1 || exit 1
for stage in unique group common_unique or_groups; do run_stage "$stage" 60 4 || exit 1; done
run_stage common 60 1 || exit 1
python3 - "$metadata" "$root/db" <<'PY'
import json, pathlib, sys
p=pathlib.Path(sys.argv[1]); d=json.loads(p.read_text()); db=pathlib.Path(sys.argv[2]); fs=[f for f in db.rglob("*") if f.is_file()]; d["status"]="COMPLETE"; d["disk"]={"logical_bytes":sum(f.stat().st_size for f in fs),"allocated_bytes":sum(f.stat().st_blocks*512 for f in fs),"files":len(fs),"wal_files":sum("wal" in f.parts for f in fs),"sstable_files":sum("sstable" in f.parts for f in fs),"manifest_files":sum("manifest" in f.name.lower() for f in fs)}
import re
t=pathlib.Path(d["seed_stderr_file"]).read_text(errors="replace")
def field(pattern):
 m=re.search(pattern,t,re.M); return m.group(1).strip() if m else None
d["seed_gnu_time"]={"user_seconds":field(r"User time \(seconds\): (.*)"),"system_seconds":field(r"System time \(seconds\): (.*)"),"cpu_percent":field(r"Percent of CPU this job got: (.*)"),"max_rss_kib":field(r"Maximum resident set size \(kbytes\): (.*)"),"exit_status":field(r"Exit status: (.*)")}
p.write_text(json.dumps(d,indent=2)+"\n")
out=pathlib.Path("benches/storage/results/2026-10-24T15-24-49Z-fts-1m/assessment.md"); lines=["# FTS million-document assessment", "", "Status: COMPLETE", "", f"Seed acknowledged {d['seed_completion']['acknowledged_seed_writes']:,} documents; elapsed {d['seed_completion']['elapsed_seconds']:.3f}s; clean shutdown.", f"Disk: {d['disk']['logical_bytes']:,} logical bytes, {d['disk']['allocated_bytes']:,} allocated bytes; WAL={d['disk']['wal_files']}, SSTable={d['disk']['sstable_files']}, manifest={d['disk']['manifest_files']}.", "", "Public ranked FTS API, default BM25. Each stage is a separate bounded process and records reopen separately. Expected results are bounded by the returned limit; sampled IDs are not independent proof of total index completeness.", "", "| Stage | Open s | Query s | Returned | Expected | Status |", "|---|---:|---:|---:|---:|---|"]
for name,v in d["stages"].items():
 q=v.get("query",{}); lines.append(f"| {name} | {v.get('open_elapsed_seconds','n/a')} | {q.get('elapsed_seconds','n/a')} | {q.get('returned','n/a')} | {q.get('expected_returned_count','n/a')} | {v['status']} |")
lines += ["", f"Assessment JSON: `{sys.argv[1]}`", "Seed logs remain in the run root.", ""]
out.write_text("\n".join(lines))
PY
printf 'assessment=COMPLETE\nfinished_utc=%s\n' "$(date -u +%FT%TZ)" >>"$assessment/worker.txt"
