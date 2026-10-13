#!/usr/bin/env bash
# Open an interactive DuckDB shell on the bronze tables, attached read-only through the read-only catalog principal.
#
#   scripts/lakehouse/query.sh                       # interactive shell
#   scripts/lakehouse/query.sh -c "select 1" -csv    # one query
#
# The launcher starts the catalog if needed and runs the shell as a session-bound attempt of the template query_reader: Ctrl-C reaches
# DuckDB as input, and closing the terminal detaches the session. No Docker command runs here.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$root"
exec .venv/bin/python -m orchestration.launcher query "$@"
