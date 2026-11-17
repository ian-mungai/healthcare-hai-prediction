#!/usr/bin/env bash
# Make or check every Git-ignored input of a new worktree.
#
#   bash scripts/worktree_setup.sh [arguments]
#
# Runs scripts.quality.worktree_setup with the repository's own environment, from the repository root. Every argument passes through unchanged.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"
if [[ ! -x .venv/bin/python ]]; then
  printf 'worktree_setup.sh: the .venv environment is missing. Run bash scripts/install.sh first.\n' >&2
  exit 1
fi
exec .venv/bin/python -m scripts.quality.worktree_setup "$@"
