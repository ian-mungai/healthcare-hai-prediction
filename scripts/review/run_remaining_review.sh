#!/usr/bin/env bash
# Repeat the real offline review entry points, preserving independent output sets.
set -euo pipefail

if [[ $# -ne 3 ]]; then
  printf 'Usage: bash scripts/review/run_remaining_review.sh SOURCE_ROOT PROJECT_PYTHON BUNDLED_PYTHON\n' >&2
  exit 2
fi

source_root=$1
project_python=$2
bundled_python=$3
review_output=data/schema_review/2026_09_29

for run in 1 2; do
  "$project_python" -m scripts.review.review_remaining \
    --source-root "$source_root" \
    --inventory "$review_output/inputs/capture_inventory.json" \
    --output "$review_output/verified_run$run"
  "$project_python" -m scripts.review.profile_crosswalk_mortality \
    --source-root "$source_root" \
    --inventory "$review_output/inputs/capture_inventory.json" \
    --output "$review_output/verified_full$run"
  "$bundled_python" -m scripts.review.inspect_documents \
    --source-root "$source_root" \
    --profiles "$review_output/verified_run$run/sources" \
    --output "$review_output/verified_documents$run.json"
done

