#!/usr/bin/env bash
# The lakehouse_features check group of scripts/run_ci.sh. The dispatcher runs each entry in its own shell process (run_group), which loads only
# scripts/quality/timing.sh (timed) and this file, then calls lakehouse_features_sources, lakehouse_features_columns or lakehouse_features_combine. It gets
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
