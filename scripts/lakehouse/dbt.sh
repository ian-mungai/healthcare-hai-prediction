#!/usr/bin/env bash
# Run dbt in the analytics image against the bronze tables, attached read-only through the read-only catalog principal.
#
#   scripts/lakehouse/dbt.sh build          # build the staging models and run their tests
#   scripts/lakehouse/dbt.sh ls             # list the project's nodes
#
# Arguments go to dbt. Models are written to data/analytics/dbt/staging.duckdb; run artifacts go to data/analytics/dbt/.
# The catalog script starts Polaris and creates the read-only principal first; Docker reads only the owner-only env
# file in data/lakehouse/secrets/, never the repository's .env.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$root"
.venv/bin/python -m scripts.lakehouse.catalog up >&2
mkdir -p "$root/data/analytics/dbt"
exec env -i PATH="$PATH" HOME="$HOME" docker compose --project-directory "$root" -f "$root/docker-compose.yaml" \
    --env-file "$root/data/lakehouse/secrets/compose.env" --profile query run --rm --quiet-pull -T analytics-dbt "$@"
