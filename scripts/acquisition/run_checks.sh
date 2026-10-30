#!/usr/bin/env bash
# The acquisition checks: Ruff, MyPy, registry validation and the offline E2E suites with the 90% coverage gate, as one run (the pre-push hook and a
# local run). GitHub CI runs the same checks as shards plus a combining job: ACQUISITION_PART=shard (with ACQUISITION_SHARD=K/N) runs
# scripts/acquisition/run_checks_shard.sh and ACQUISITION_PART=combine runs scripts/acquisition/run_checks_combine.sh, through
# scripts/acquisition/run_checks_ci.sh. Each command prints its start, end and seconds (scripts/quality/timing.sh).
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$repo_root"
python_bin="${PYTHON_BIN:-$repo_root/.venv/bin/python}"
case "${ACQUISITION_PART:-}" in
  "") ;;
  shard) exec bash scripts/acquisition/run_checks_shard.sh "${ACQUISITION_SHARD:?ACQUISITION_PART=shard needs ACQUISITION_SHARD=K/N}" ;;
  combine) exec bash scripts/acquisition/run_checks_combine.sh ;;
  *) printf 'ACQUISITION_PART must be empty, shard or combine, not %s\n' "$ACQUISITION_PART" >&2; exit 2 ;;
esac
# ShellCheck runs without -x here and checks timing.sh as its own input.
# shellcheck source=/dev/null
source "$repo_root/scripts/quality/timing.sh"
timing_entry
timed "acquisition: shell syntax" bash -n scripts/acquisition/run_checks.sh scripts/acquisition/run_checks_ci.sh scripts/acquisition/run_checks_ci_e2e.sh scripts/acquisition/run_checks_shard.sh scripts/acquisition/run_checks_combine.sh
timed "acquisition: ruff" "$python_bin" -m ruff check --no-respect-gitignore scripts/acquisition tests/test_source_registry.py
timed "acquisition: mypy" "$python_bin" -m scripts.quality.typecheck --runtime acquisition --check-untyped-defs --disallow-untyped-defs
timed "acquisition: source registry" "$python_bin" -m scripts.acquisition.source_registry
timed "acquisition: source verification log" "$python_bin" -m scripts.acquisition.source_verification_log --check
timed "acquisition: receipt index" "$python_bin" -m scripts.acquisition.source_receipt_index --check
timed "acquisition: shards E2E" "$python_bin" -m scripts.acquisition.run_shards_e2e
timed "acquisition: pytest with coverage gate" "$python_bin" -m pytest scripts/acquisition/tests tests/test_source_registry.py --cov=scripts.acquisition \
  --cov-config=config/acquisition/coverage.ini --cov-report=term-missing --cov-fail-under=90 -q
