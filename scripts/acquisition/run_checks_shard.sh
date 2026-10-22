#!/usr/bin/env bash
# One shard of the acquisition pytest run, for a GitHub CI shard job: bash scripts/acquisition/run_checks_shard.sh K/N
# It runs only the tests assigned to shard K of N (scripts/acquisition/shards.py), measures coverage into data/e2e/acquisition_shards/coverage/
# without enforcing a target (only the combining job enforces it, on the merged data) and seals the manifest of the shard with the pytest
# exit status and the hash of its coverage data. The shard fails when pytest fails, and a failed or missing manifest fails the combining job.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$repo_root"
python_bin="${PYTHON_BIN:-$repo_root/.venv/bin/python}"
# ShellCheck runs without -x here and checks timing.sh as its own input.
# shellcheck source=/dev/null
source "$repo_root/scripts/quality/timing.sh"
timing_entry
shard="${1:?usage: run_checks_shard.sh K/N}"
index="${shard%%/*}"
directory="${ACQUISITION_SHARD_DIR:-$repo_root/data/e2e/acquisition_shards}"
printf 'ran\n' >>"${ACQUISITION_RAN_MARKER:-/dev/null}"
mkdir -p "$directory/coverage"
export COVERAGE_FILE="$directory/coverage/shard${index}.coverage"
rm -f "$COVERAGE_FILE" "$COVERAGE_FILE".* "$directory/shard_${index}_of_${shard#*/}.json"
status=0
timed "acquisition shard $shard: pytest" "$python_bin" -m pytest scripts/acquisition/tests tests/test_source_registry.py \
  -p scripts.acquisition.shards --shard "$shard" --shard-output "$directory" \
  --cov=scripts.acquisition --cov-config=config/acquisition/coverage.ini --cov-report= --cov-fail-under=0 -q || status=$?
"$python_bin" -m scripts.acquisition.shards seal --directory "$directory" --shard "$shard" --pytest-exit "$status" || status=1
exit "$status"
