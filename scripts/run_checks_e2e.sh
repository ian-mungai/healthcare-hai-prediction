#!/usr/bin/env bash
# Run the single end-to-end check of the repository on its own.
#
#   bash scripts/run_checks_e2e.sh [arguments]
#
# Runs scripts.quality.run_checks_e2e with the repository's own environment, from the repository root. Every argument passes through unchanged.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"
if [[ ! -x .venv/bin/python ]]; then
  printf 'run_checks_e2e.sh: the .venv environment is missing. Run bash scripts/install.sh first.\n' >&2
  exit 1
fi
exec .venv/bin/python -m scripts.quality.run_checks_e2e "$@"
