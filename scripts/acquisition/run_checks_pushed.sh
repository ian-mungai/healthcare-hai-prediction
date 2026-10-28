#!/usr/bin/env bash
# The acquisition-checks entry of the pre-push stage (.pre-commit-config.yaml). The pre-push run (scripts/quality/pre_push.py) runs this hook once for the
# whole pushed range (merge base with origin/main to the pushed head), as GitHub CI does (scripts/acquisition/run_checks_ci.sh), so pre-commit applies the
# files pattern of the hook to that range. The native hook of the push itself sees only the first pushed reference and its own range: it skips, and pre-commit
# marks it with PRE_COMMIT_REMOTE_NAME. Every other run (the pre-push run, GitHub CI) runs scripts/acquisition/run_checks.sh unchanged.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$repo_root"
if [[ -n "${PRE_COMMIT_REMOTE_NAME:-}" ]]; then
  echo "Acquisition checks: the pre-push run (scripts/quality/pre_push.py) runs them once for the whole pushed range."
  exit 0
fi
exec bash scripts/acquisition/run_checks.sh
