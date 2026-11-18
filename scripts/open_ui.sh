#!/usr/bin/env bash
# Open one web interface of the host in a browser through Session Manager, or list the web interfaces.
#
#   bash scripts/open_ui.sh <name> [--local-port N]
#   bash scripts/open_ui.sh --list
#
# scripts.infrastructure.open_ui validates everything first (the name, the port, .env and the one running host) and prints the exact command, one part per line
# (it refuses any part that holds a line break or a NUL). This script then replaces itself with that command (exec), so no process supervises the session, no
# file is made and a terminal Ctrl+C reaches aws and the Session Manager plugin as when aws is run by hand. Nothing is started when the validation refuses or is
# interrupted; this script then exits with the code of open_ui (130 for Ctrl+C). The internal --emit-command option belongs to this script alone: a caller that
# passes it is refused. macOS bash is 3.2: it has no mapfile and a variable cannot hold a NUL, and a process substitution would run open_ui with SIGINT ignored,
# so the command is read from a foreground command substitution and split on line breaks by parameter expansion, with no globbing or IFS.
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
for argument in "$@"; do
  case "$argument" in
    --emit-command*)
      printf 'open_ui.sh: that option is internal and cannot be used.\n' >&2
      exit 2
      ;;
  esac
done

status=0
output="$(.venv/bin/python -m scripts.infrastructure.open_ui --emit-command "$@")" || status=$?
if ((status != 0)); then
  exit "$status"
fi
# No command with a zero status means there is nothing to start (--list, whose table open_ui printed itself).
if [[ -z "$output" ]]; then
  exit 0
fi
command=()
rest="$output"
while [[ -n "$rest" ]]; do
  command+=("${rest%%$'\n'*}")
  if [[ "$rest" == *$'\n'* ]]; then
    rest="${rest#*$'\n'}"
  else
    rest=""
  fi
done
exec "${command[@]}"
