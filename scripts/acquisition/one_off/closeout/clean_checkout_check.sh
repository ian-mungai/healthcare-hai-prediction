#!/usr/bin/env bash
# Run the acquisition checks and every offline E2E suite in a copy that holds only
# Git-tracked files plus scripts/acquisition and config/acquisition (no data/).
# Usage: clean_checkout_check.sh <clean_copy_dir> <output_dir>
set -uo pipefail
clean="$1"; out="$(mkdir -p "$2" && cd "$2" && pwd)"
repo="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"
py="$repo/.venv/bin/python"
mkdir -p "$out"
cd "$clean"
PYTHON_BIN="$py" bash scripts/acquisition/run_checks.sh >"$out/run_checks.log" 2>&1
printf '{"suite": "run_checks", "exit": %s}\n' "$?" >>"$out/results.jsonl"
for suite in bls_api census_acs_api census_acs_detailed cms_owners code_versions hcai_util hud_api hud_xlsx mmd_api onc_mu privacy registry_additions registry_migration wonder_export; do
  "$py" -m "scripts.acquisition.run_${suite}_e2e" --output "$clean/e2e_out/${suite}.json" >"$out/${suite}.log" 2>&1
  printf '{"suite": "%s", "exit": %s}\n' "$suite" "$?" >>"$out/results.jsonl"
done
