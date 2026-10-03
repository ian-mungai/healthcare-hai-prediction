#!/usr/bin/env bash
# Open the DuckDB UI at http://localhost:4213 with the bronze tables attached read-only. Stop it with Ctrl-C.
#
#   scripts/lakehouse/ui.sh
#
# The UI runs in the analytics container behind a relay published on this Mac's loopback only. Its web pages come from
# https://ui.duckdb.org (the extension's default); queries and data stay in the local DuckDB process. Saved notebooks
# live in the ignored data/analytics/ui/ folder. Docker reads only the owner-only env file in data/lakehouse/secrets/.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$root"
.venv/bin/python -m scripts.lakehouse.catalog up >&2
mkdir -p data/analytics/ui
echo "DuckDB UI starting at http://localhost:4213 (Ctrl-C to stop)" >&2
exec env -i PATH="$PATH" HOME="$HOME" docker compose --project-directory "$root" -f "$root/docker-compose.yaml" \
    --env-file "$root/data/lakehouse/secrets/compose.env" --profile query run --rm --service-ports --quiet-pull -T \
    analytics-ui
