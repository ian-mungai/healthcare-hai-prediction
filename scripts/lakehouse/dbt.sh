#!/usr/bin/env bash
# Run dbt through the launcher (orchestration/launcher.py), the only process that starts containers.
#
#   scripts/lakehouse/dbt.sh deps           # install the packages from dbt/package-lock.yml (template dbt_deps)
#   scripts/lakehouse/dbt.sh build          # a real build of a real transform run (template real_dbt; refused until provenance is filled)
#
# `deps` writes the shared package folder data/analytics/dbt/dbt_packages/. Every other dbt command is a launch of a real run on the
# run's snapshot, with the catalog's read-only principal; the run keeps its files in data/analytics/dbt/runs/<run_id>/.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$root"
exec .venv/bin/python -m orchestration.launcher dbt "$@"
