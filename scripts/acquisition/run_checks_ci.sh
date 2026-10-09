#!/usr/bin/env bash
# GitHub CI entry for the acquisition checks: they run only when the pushed range changes a
# path the local acquisition-checks hook watches. pre-commit applies that hook's own files pattern, so CI and the local
# hook share one path list. Without a usable range (manual run, first push of a branch, rewritten history), every check
# runs. E2E: bash scripts/acquisition/run_checks_ci_e2e.sh
#   RANGE_START=<sha> RANGE_END=<sha> bash scripts/acquisition/run_checks_ci.sh
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$repo_root"
python_bin="${PYTHON_BIN:-$repo_root/.venv/bin/python}"
summary="${GITHUB_STEP_SUMMARY:-/dev/null}"
start="${RANGE_START:-}"
end="${RANGE_END:-}"

if [[ -z "$start" || -z "$end" || "$start" =~ ^0+$ ]] || ! git cat-file -e "$start^{commit}" 2>/dev/null || ! git cat-file -e "$end^{commit}" 2>/dev/null; then
  echo "Acquisition checks: no usable range (start '${start:-none}'), so every check runs." | tee -a "$summary"
  exec bash scripts/acquisition/run_checks.sh
fi
echo "Acquisition checks: range $start..$end; they run only if it changes a path the acquisition-checks hook watches." | tee -a "$summary"
exec "$python_bin" -m pre_commit run acquisition-checks --hook-stage pre-commit --from-ref "$start" --to-ref "$end"
