#!/usr/bin/env bash
# Closeout checklist item 2: rerun the collection checks and one offline recheck per collector
# from a clean copy that holds only what Git would commit.
# Stage A runs the acquisition gate with no data/ folder. Stage B links the real data/ folder
# into the copy and runs each collector's offline recheck (no --fetch, no --execute), then
# proves data/ was not changed by comparing a size and modification-time manifest before and after.
# No network, AWS or S3 access is used. Usage: clean_checkout_replay.sh <output_dir>
set -uo pipefail
repo="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"
out="$(mkdir -p "$1" && cd "$1" && pwd)"
py="$repo/.venv/bin/python"
clean="$(mktemp -d)/repo"
mkdir -p "$clean"
chmod 700 "$(dirname "$clean")"
cd "$repo"
git ls-files -z --cached --others --exclude-standard | rsync -a --from0 --files-from=- "$repo/" "$clean/"
# Git LFS files are real files in this working copy, so the copy holds their contents.
ln -s "$repo/.venv" "$clean/.venv"
ln -s "$repo/.tools" "$clean/.tools"
cp "$repo/.env" "$clean/.env" && chmod 600 "$clean/.env"
git -C "$clean" init -q && git -C "$clean" add -A >/dev/null 2>&1
printf '{"clean_copy_files": %s}\n' "$(git -C "$clean" ls-files | wc -l | tr -d ' ')" >"$out/clean_copy.json"
record() { printf '{"stage": "%s", "step": "%s", "exit": %s}\n' "$1" "$2" "$3" >>"$out/results.jsonl"; }

# Stage A: the gate in a copy with no data/ folder. SKIP_STAGE_A=1 reruns stage B only.
cd "$clean"
if [[ "${SKIP_STAGE_A:-0}" != "1" ]]; then
  PYTHON_BIN="$py" bash scripts/acquisition/run_checks.sh >"$out/A_run_checks.log" 2>&1
  record A run_checks $?
fi

# Stage B: offline rechecks against the stored evidence. The gate writes its artifacts under the
# copy's own data/ folder; keep them, then replace that folder with a link to the real data/.
if [[ -d "$clean/data" && ! -L "$clean/data" ]]; then
  mkdir -p "$out/A_gate_data" && cp -R "$clean/data/." "$out/A_gate_data/" && rm -rf "$clean/data"
fi
ln -s "$repo/data" "$clean/data"
[[ -L "$clean/data" && -f "$clean/data/redownload_checks/20260929/queue.json" ]] || { record B data_link 1; exit 1; }
manifest() { "$py" - "$repo/data" "$1" <<'PY'
import json, os, sys
root, target = sys.argv[1], sys.argv[2]
skip = os.path.join(root, "e2e", "closeout_clean_20261001")
rows = {}
for folder, subfolders, names in os.walk(root):
    if folder.startswith(skip):
        subfolders[:] = []
        continue
    for name in names:
        path = os.path.join(folder, name)
        if name == ".DS_Store" or os.path.islink(path):
            continue
        stat = os.stat(path)
        rows[os.path.relpath(path, root)] = [stat.st_size, stat.st_mtime_ns]
json.dump(rows, open(target, "w"), sort_keys=True)
print(len(rows))
PY
}
manifest "$out/B_data_manifest_before.json" >"$out/B_manifest_before_count.txt"
for module in collect_bls_api collect_census_acs_api collect_census_acs_detailed collect_hud_api store_hud_xlsx store_wonder_export store_cms_owners store_hcai_util store_onc_mu; do
  "$py" -m "scripts.acquisition.$module" >"$out/B_${module}.log" 2>&1
  record B "$module" $?
done
"$py" -m scripts.acquisition.reconcile_census_acs_api_e2e --output "$out/B_reconcile_census_acs.json" >"$out/B_reconcile_census_acs.log" 2>&1
record B reconcile_census_acs_api_e2e $?
"$py" -m scripts.acquisition.run_mmd_api_e2e --output "$out/B_mmd_api_e2e.json" >"$out/B_mmd_api_e2e.log" 2>&1
record B run_mmd_api_e2e $?
"$py" -m scripts.acquisition.redownload --preflight >"$out/B_redownload_preflight.log" 2>&1
record B redownload_preflight $?
manifest "$out/B_data_manifest_after.json" >"$out/B_manifest_after_count.txt"
"$py" - "$out" <<'PY'
import json, sys
out = sys.argv[1]
before = json.load(open(f"{out}/B_data_manifest_before.json"))
after = json.load(open(f"{out}/B_data_manifest_after.json"))
changed = sorted(k for k in before.keys() & after.keys() if before[k] != after[k])
report = {"files_before": len(before), "files_after": len(after), "added": sorted(after.keys() - before.keys()), "removed": sorted(before.keys() - after.keys()), "changed": changed}
json.dump(report, open(f"{out}/B_data_unchanged.json", "w"), indent=2)
print(json.dumps({k: (len(v) if isinstance(v, list) else v) for k, v in report.items()}))
PY
record B data_unchanged_report $?
rm -f "$clean/.env"
rm -rf "$(dirname "$clean")"
