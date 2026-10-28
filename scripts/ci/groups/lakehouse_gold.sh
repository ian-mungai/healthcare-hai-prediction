#!/usr/bin/env bash
# The lakehouse_gold check group (its three units) of scripts/run_ci.sh. The dispatcher runs the group in its own shell process (run_group), which loads only
# scripts/quality/timing.sh (timed) and this file, and then calls the entry function. It gets python_bin, repo_root, scratch, quality_output, e2e_output and
# CI_TIMING_FILE from the dispatcher beside the exported environment. This file defines functions only: the guard scripts/quality/ci_group_files.py checks it.
# shellcheck disable=SC2154,SC2329

# The gold layer is three units (CI_UNITS) that share no folder, port or process: GitHub CI runs the model metadata E2E, the longest command, in
# its own job, and locally the units run together except writer_setup, which has timing assertions and runs alone before them.
lakehouse_gold_checks() {
  # Reference seeds of the gold control bridge, the formula recipes and the repeatable negative cases of their check.
  timed "lakehouse_gold: bridge references check" "$python_bin" -m scripts.lakehouse.bridge_references --check
  timed "lakehouse_gold: bridge references E2E" "$python_bin" -m scripts.lakehouse.run_bridge_references_e2e
  timed "lakehouse_gold: identity splits check" "$python_bin" -m scripts.lakehouse.identity_splits --check
  timed "lakehouse_gold: source labels check" "$python_bin" -m scripts.lakehouse.source_labels --check
  timed "lakehouse_gold: county change seed check" "$python_bin" -m scripts.lakehouse.county_change_seed --check
  timed "lakehouse_gold: identity seeds E2E" "$python_bin" -m scripts.lakehouse.run_identity_seeds_e2e
  timed "lakehouse_gold: county change seed E2E" "$python_bin" -m scripts.lakehouse.run_county_change_seed_e2e --output "$quality_output/county_change_seed.json"
  # The gold Great Expectations suites must match the gold contract they are generated from.
  timed "lakehouse_gold: suites check" "$python_bin" -m scripts.lakehouse.gold_checks check-suites
  timed "lakehouse_gold: publish contract E2E" "$python_bin" -m scripts.lakehouse.run_gold_publish_e2e --mode contract
  timed "lakehouse_gold: catalog registration E2E" "$python_bin" -m scripts.lakehouse.run_catalog_registration_e2e --out "$quality_output/catalog_registration" --mutations
  timed "lakehouse_gold: model metadata check" "$python_bin" -m scripts.quality.model_metadata
}

lakehouse_gold_writer_setup() {
  timed "lakehouse_gold: writer set-up E2E" "$python_bin" -m scripts.lakehouse.run_gold_publish_e2e --mode writer_setup
}

lakehouse_gold_model_metadata_e2e() {
  timed "lakehouse_gold: model metadata E2E" "$python_bin" -m scripts.quality.run_model_metadata_e2e --workers "${MODEL_METADATA_WORKERS:-auto}"
}
