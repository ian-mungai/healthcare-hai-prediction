#!/usr/bin/env bash
# Start the DuckDB UI on http://localhost:4213, reachable from this Mac only (template ui_reader). Ctrl-C stops it.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$root"
exec .venv/bin/python -m orchestration.launcher ui
