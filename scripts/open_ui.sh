#!/usr/bin/env bash
# Open one web interface of the host in a browser through Session Manager.
#
#   bash scripts/open_ui.sh [arguments]
#
# Runs scripts.infrastructure.open_ui with the repository's own environment, from the repository root. Every argument passes through unchanged.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"
if [[ ! -x .venv/bin/python ]]; then
  printf 'open_ui.sh: the .venv environment is missing. Run bash scripts/install.sh first.\n' >&2
  exit 1
fi
exec .venv/bin/python -m scripts.infrastructure.open_ui "$@"
