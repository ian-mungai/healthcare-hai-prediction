#!/usr/bin/env bash
# The lakehouse_features check group of scripts/run_ci.sh. The dispatcher runs the group in its own shell process (run_group), which loads only
# scripts/quality/timing.sh (timed) and this file, and then calls the entry function. It gets python_bin, repo_root, scratch, quality_output, e2e_output and
# CI_TIMING_FILE from the dispatcher beside the exported environment. This file defines functions only: the guard scripts/quality/ci_group_files.py checks it.
# shellcheck disable=SC2154,SC2329

lakehouse_features() {
  local python_bin="${python_bin:?}"
  # The feature phase reads gold only, and the gold build selections leave it out.
  timed "lakehouse_features: feature sources check" "$python_bin" -m scripts.lakehouse.feature_sources --check
  timed "lakehouse_features: feature sources E2E" "$python_bin" -m scripts.lakehouse.run_feature_sources_e2e
  # Every output column of a feature model is documented with its data type, checked without the dbt contract that also enforces it.
  timed "lakehouse_features: feature columns check" "$python_bin" -m scripts.lakehouse.feature_columns --check
  timed "lakehouse_features: feature columns E2E" "$python_bin" -m scripts.lakehouse.run_feature_columns_e2e
}
