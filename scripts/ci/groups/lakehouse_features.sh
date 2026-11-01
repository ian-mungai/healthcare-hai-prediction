#!/usr/bin/env bash
# The lakehouse_features check group of scripts/run_ci.sh. The dispatcher runs each entry in its own shell process (run_group), which loads only
# scripts/quality/timing.sh (timed) and this file, then calls lakehouse_features_sources, lakehouse_features_columns, lakehouse_features_combine or lakehouse_features_store. It gets
# python_bin, repo_root, scratch, quality_output, e2e_output and CI_TIMING_FILE beside the exported environment. This file defines functions only, checked by scripts/quality/ci_group_files.py.
# shellcheck disable=SC2154,SC2329

lakehouse_features_sources() {
  local shard="${1:?}"
  # The feature phase reads gold only, and the gold build selections leave it out.
  if [[ "$shard" == 1/* ]]; then
    timed "lakehouse_features: feature sources check" "$python_bin" -m scripts.lakehouse.feature_sources --check
  fi
  timed "lakehouse_features: feature sources E2E" "$python_bin" -m scripts.lakehouse.run_feature_sources_e2e --shard "$shard" \
    --output-directory "$quality_output/feature_shards/sources/shard_${shard%%/*}"
}

lakehouse_features_columns() {
  local shard="${1:?}"
  # Every output column of a feature model is documented with its data type, checked without the dbt contract that also enforces it.
  if [[ "$shard" == 1/* ]]; then
    timed "lakehouse_features: feature columns check" "$python_bin" -m scripts.lakehouse.feature_columns --check
  fi
  timed "lakehouse_features: feature columns E2E" "$python_bin" -m scripts.lakehouse.run_feature_columns_e2e --shard "$shard" \
    --output-directory "$quality_output/feature_shards/columns/shard_${shard%%/*}"
}

lakehouse_features_combine() {
  local spec="${1:?}" suite="${1%%/*}" count="${1##*/}"
  timed "lakehouse_features: combine $spec" "$python_bin" -m scripts.lakehouse.feature_shards combine --suite "$suite" --count "$count" \
    --directory "$quality_output/feature_shards/$suite"
}

lakehouse_features_store() {
  # The feature store's own gates, all offline and light: the generated schema, models, YAML and Feast definitions are what the generator writes; the static
  # cases (pin, plan, contract, masks, no teardown, closed settings); the Feast job and historical helper against stub catalogs; the AWS profile against stand-ins; the online protocol against an
  # in-process fake Redis; and the feature pipeline's renders, broker approval and DAG graphs. The builds, the Feast image and the real Redis stay in the heavy lane.
  timed "lakehouse_features: feature schema check" "$python_bin" -m scripts.features.feature_schema check
  timed "lakehouse_features: feature store static E2E" "$python_bin" -m scripts.features.run_feature_store_e2e --mode static
  timed "lakehouse_features: Feast local cases" "$python_bin" -m scripts.features.feast_cases --part local
  timed "lakehouse_features: Feast AWS profile cases with stand-ins" "$python_bin" -m scripts.features.feast_cases --part aws
  timed "lakehouse_features: online protocol cases with fake Redis" "$python_bin" -m scripts.features.online_cases --part fake
  # The spill directory and the byte cap on every DuckDB connection of a layer phase: a real spill through the helper and through the generated profiles, and
  # a scan that no phase opens a connection without the helper.
  timed "lakehouse_features: DuckDB bounds cases" "$repo_root/.tools/sqlfluff/bin/python" -m scripts.lakehouse.run_duckdb_bounds_cases
  timed "lakehouse_features: features pipeline light E2E" "$python_bin" -m scripts.orchestration.run_features_pipeline_light
}
