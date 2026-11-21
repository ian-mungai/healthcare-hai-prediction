#!/usr/bin/env bash
# The configuration_checks check group of scripts/run_ci.sh. The dispatcher runs the group in its own shell process (run_group), which loads only
# scripts/quality/timing.sh (timed) and this file, and then calls the entry function. It gets python_bin, repo_root, scratch, quality_output, e2e_output and
# CI_TIMING_FILE from the dispatcher beside the exported environment. This file defines functions only: the guard scripts/quality/ci_group_files.py checks it.
# shellcheck disable=SC2154,SC2329

configuration_checks() {
  timed "configuration_checks: infrastructure E2E" "$python_bin" -m scripts.infrastructure.run_e2e --output-directory "$e2e_output/infrastructure"
  timed "configuration_checks: activation inputs E2E" "$python_bin" -m scripts.infrastructure.run_activation_inputs_e2e --out "$quality_output/activation_inputs"
  timed "configuration_checks: job IAM E2E" "$python_bin" -m scripts.infrastructure.run_jobs_iam_e2e --output "$quality_output/jobs_iam"
  timed "configuration_checks: Airflow launch policy cases" "$python_bin" -m scripts.infrastructure.airflow_launch_policy_cases --output "$quality_output/airflow_launch_policy"
  timed "configuration_checks: Catalog setup policy cases" "$python_bin" -m scripts.infrastructure.catalog_setup_policy_cases --output "$quality_output/catalog_setup_policy"
  timed "configuration_checks: quality E2E" "$python_bin" -m scripts.quality.run_e2e --output-directory "$quality_output/e2e"
  timed "configuration_checks: case shards E2E" "$python_bin" -m scripts.quality.run_case_shards_e2e --output-directory "$quality_output/case_shards"
  timed "configuration_checks: persistent OpenMetadata bootstrap E2E" "$python_bin" -m scripts.infrastructure.run_openmetadata_persistent_e2e --output-root "$e2e_output/openmetadata_persistent"
  timed "configuration_checks: OpenMetadata catalog task E2E" "$python_bin" -m scripts.infrastructure.run_openmetadata_e2e --output-directory "$e2e_output/openmetadata"
  timed "configuration_checks: secret store E2E" "$python_bin" -m scripts.infrastructure.run_secret_store_e2e
}
