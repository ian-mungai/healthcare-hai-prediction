#!/usr/bin/env bash
# Open DuckDB with the bronze tables attached read-only, inside the analytics container.
#
#   scripts/lakehouse/query.sh                                   # interactive shell
#   scripts/lakehouse/query.sh -c "SELECT count(*) FROM cms_hai_state"   # one statement, then exit
#
# Arguments go to the DuckDB CLI. The catalog script starts Polaris and creates the read-only principal first; Docker
# reads only the owner-only env file in data/lakehouse/secrets/, never the repository's .env.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$root"
.venv/bin/python -m scripts.lakehouse.catalog up >&2
tty_flag=()
[[ -t 0 && -t 1 ]] || tty_flag=(-T)
exec env -i PATH="$PATH" HOME="$HOME" docker compose --project-directory "$root" -f "$root/docker-compose.yaml" \
    --env-file "$root/data/lakehouse/secrets/compose.env" --profile query run --rm --quiet-pull "${tty_flag[@]}" \
    analytics duckdb -init /opt/analytics/bronze.sql "$@"
