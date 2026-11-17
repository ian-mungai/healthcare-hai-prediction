#!/usr/bin/env bash
# Run the single end-to-end check of the repository on its own.
#
#   bash scripts/run_checks_e2e.sh [arguments]
#
# Runs scripts.quality.run_checks_e2e with the repository's own environment, from the repository root. Every argument passes through unchanged.
set -euo pipefail

# Follow a symbolic link to the real script first (old Bash on macOS has no readlink -f), then take the repository root from the real location.
source_path="${BASH_SOURCE[0]}"
while [[ -L "$source_path" ]]; do
  link_directory="$(cd -P "$(dirname "$source_path")" && pwd)"
  source_path="$(readlink "$source_path")"
  [[ "$source_path" == /* ]] || source_path="$link_directory/$source_path"
done
repo_root="$(cd -P "$(dirname "$source_path")/.." && pwd)"
cd "$repo_root"
if [[ ! -x .venv/bin/python ]]; then
  printf 'run_checks_e2e.sh: the .venv environment is missing. Run bash scripts/install.sh first.\n' >&2
  exit 1
fi
exec .venv/bin/python -m scripts.quality.run_checks_e2e "$@"
