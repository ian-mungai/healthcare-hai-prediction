#!/usr/bin/env bash
# Open one web interface of the host in a browser through Session Manager, or list the web interfaces.
#
#   bash scripts/open_ui.sh <name> [--local-port N]
#   bash scripts/open_ui.sh --list
#
# scripts.infrastructure.open_ui validates everything first (the name, the port, .env and the one running host) and writes the exact command. This script then
# replaces itself with that command (exec), so no process supervises the session and a terminal Ctrl+C reaches aws and the Session Manager plugin as when aws is
# run by hand. Every argument passes through to open_ui unchanged. Nothing is started when the validation refuses or is interrupted.
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
  printf 'open_ui.sh: the .venv environment is missing. Run bash scripts/install.sh first.\n' >&2
  exit 1
fi

command_file="$(mktemp "${TMPDIR:-/tmp}/hai_open_ui.XXXXXX")"
trap 'rm -f "$command_file"' EXIT
.venv/bin/python -m scripts.infrastructure.open_ui --exec-args-file "$command_file" "$@"
command=()
while IFS= read -r -d '' part; do
  command+=("$part")
done < "$command_file"
# An empty file means there is nothing to start (--list).
if ((${#command[@]} == 0)); then
  exit 0
fi
rm -f "$command_file"
trap - EXIT
exec "${command[@]}"
