#!/usr/bin/env bash
# Start, stop or inspect the AWS host (start, refresh, bootstrap, stop or status).
#
#   bash scripts/host.sh [arguments]
#
# Runs scripts.infrastructure.host_session with the repository's own environment, from the repository root. Every argument passes through unchanged.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"
if [[ ! -x .venv/bin/python ]]; then
  printf 'host.sh: the .venv environment is missing. Run bash scripts/install.sh first.\n' >&2
  exit 1
fi
exec .venv/bin/python -m scripts.infrastructure.host_session "$@"
