#!/usr/bin/env bash
# GitHub CI entry for one launcher E2E shard: it runs only when the pushed range changes a path the launcher, its job
# templates or its fake Docker read. Without a usable range (manual run, first push of a branch, rewritten history),
# the shard always runs. A documentation, dbt model or Terraform change alone skips it; the core job still runs.
#   RANGE_START=<sha> RANGE_END=<sha> LAUNCHER_SHARD=1/2 bash scripts/lakehouse/run_launcher_ci.sh
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$repo_root"
summary="${GITHUB_STEP_SUMMARY:-/dev/null}"
start="${RANGE_START:-}"
end="${RANGE_END:-}"
watched='^(orchestration/|scripts/|config/|services/|data_contracts/|dbt/packages\.yml$|dbt/package-lock\.yml$|docker-compose\.yaml$|requirements\.txt$|\.pre-commit-config\.yaml$|\.github/workflows/ci\.yml$)'

if [[ -z "$start" || -z "$end" || "$start" =~ ^0+$ ]] || ! git cat-file -e "$start^{commit}" 2>/dev/null || ! git cat-file -e "$end^{commit}" 2>/dev/null; then
  echo "Launcher E2E: no usable range (start '${start:-none}'), so shard ${LAUNCHER_SHARD:-all} runs." | tee -a "$summary"
elif ! git diff --name-only "$start" "$end" | grep -Eq "$watched"; then
  echo "Launcher E2E: range $start..$end changes no watched path, so shard ${LAUNCHER_SHARD:-all} is skipped." | tee -a "$summary"
  exit 0
else
  echo "Launcher E2E: range $start..$end changes a watched path, so shard ${LAUNCHER_SHARD:-all} runs." | tee -a "$summary"
fi
CI_PART=launcher exec bash scripts/run_ci.sh
