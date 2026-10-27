#!/usr/bin/env bash
# The acquisition-checks entry of the pre-push stage (.pre-commit-config.yaml). pre-commit applies the hook's files pattern, so this runs only when the pushed
# range reaches the acquisition checks. After a passing full-scope pre-push run for the same head (scripts/quality/pre_push.py writes a marker in the Git
# directory) the full scope already ran the suite: this skips once and removes the marker. Anything else runs scripts/acquisition/run_checks.sh unchanged,
# also in GitHub CI (scripts/acquisition/run_checks_ci.sh), where no marker exists.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$repo_root"
head="${PRE_COMMIT_TO_REF:-$(git rev-parse HEAD)}"
marker="$(git rev-parse --git-path hai_pre_push_full)"
if [[ -f "$marker" && "$(<"$marker")" == "$head" ]]; then
  rm -f "$marker"
  echo "Acquisition checks: the full-scope pre-push run of $head already ran them."
  exit 0
fi
exec bash scripts/acquisition/run_checks.sh
