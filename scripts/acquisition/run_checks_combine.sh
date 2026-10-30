#!/usr/bin/env bash
# The combining job of the acquisition checks (GitHub CI): Ruff, MyPy and the source registry, then the gate of scripts/acquisition/shards.py over
# the shard manifests and coverage data in data/e2e/acquisition_shards/ (downloaded from the shard jobs): every shard present, passed and of this run,
# every collected test run once, and the merged coverage at 90% or more. ACQUISITION_EXPECT=skip (a range that does not reach the acquisition
# checks) runs only the gate, which then requires every shard manifest to say skipped. ACQUISITION_SHARD_COUNT is the number of shards (default 4, the CI count).
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$repo_root"
python_bin="${PYTHON_BIN:-$repo_root/.venv/bin/python}"
# ShellCheck runs without -x here and checks timing.sh as its own input.
# shellcheck source=/dev/null
source "$repo_root/scripts/quality/timing.sh"
timing_entry
directory="${ACQUISITION_SHARD_DIR:-$repo_root/data/e2e/acquisition_shards}"
expect="${ACQUISITION_EXPECT:-run}"
printf 'ran\n' >>"${ACQUISITION_RAN_MARKER:-/dev/null}"
if [[ "$expect" == "run" ]]; then
  timed "acquisition combine: ruff" "$python_bin" -m ruff check --no-respect-gitignore scripts/acquisition tests/test_source_registry.py
  timed "acquisition combine: mypy" "$python_bin" -m scripts.quality.typecheck --runtime acquisition --check-untyped-defs --disallow-untyped-defs
  timed "acquisition combine: source registry" "$python_bin" -m scripts.acquisition.source_registry
  timed "acquisition combine: source verification log" "$python_bin" -m scripts.acquisition.source_verification_log --check
  timed "acquisition combine: receipt index" "$python_bin" -m scripts.acquisition.source_receipt_index --check
  timed "acquisition combine: shards E2E" "$python_bin" -m scripts.acquisition.run_shards_e2e
fi
timed "acquisition combine: gate expecting $expect" "$python_bin" -m scripts.acquisition.shards gate --directory "$directory" --count "${ACQUISITION_SHARD_COUNT:-4}" \
  --expect "$expect" --fail-under 90 -- scripts/acquisition/tests tests/test_source_registry.py
