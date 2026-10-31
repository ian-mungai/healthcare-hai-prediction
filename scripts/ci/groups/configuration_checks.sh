#!/usr/bin/env bash
# The configuration_checks check group of scripts/run_ci.sh. The dispatcher runs the group in its own shell process (run_group), which loads only
# scripts/quality/timing.sh (timed) and this file, and then calls the entry function. It gets python_bin, repo_root, scratch, quality_output, e2e_output and
# CI_TIMING_FILE from the dispatcher beside the exported environment. This file defines functions only: the guard scripts/quality/ci_group_files.py checks it.
# shellcheck disable=SC2154,SC2329

configuration_checks() {
  local policy_output="${POLICY_ARTIFACT_DIR:-$repo_root/data/e2e/policies/$(date -u +%Y%m%dT%H%M%SZ)_$$}"
  timed "configuration_checks: infrastructure E2E" "$python_bin" -m scripts.infrastructure.run_e2e --output-directory "$e2e_output"
  timed "configuration_checks: activation inputs E2E" "$python_bin" -m scripts.infrastructure.run_activation_inputs_e2e --out "$quality_output/activation_inputs"
  timed "configuration_checks: policy E2E" "$python_bin" -m scripts.infrastructure.run_policy_e2e --output-directory "$policy_output"
  timed "configuration_checks: job IAM E2E" "$python_bin" -m scripts.infrastructure.run_jobs_iam_e2e --output "$quality_output/jobs_iam"
  timed "configuration_checks: broker policy cases" "$python_bin" -m scripts.infrastructure.broker_policy_cases --output "$quality_output/broker_policy"
  timed "configuration_checks: quality E2E" "$python_bin" -m scripts.quality.run_e2e --output-directory "$quality_output/e2e"
  timed "configuration_checks: OpenMetadata catalog task E2E" "$python_bin" -m scripts.infrastructure.run_openmetadata_e2e --output-directory "$e2e_output/openmetadata"
  timed "configuration_checks: secret store E2E" "$python_bin" -m scripts.infrastructure.run_secret_store_e2e
  timed "configuration_checks: monitoring files E2E" "$python_bin" -m scripts.infrastructure.run_monitoring_e2e
  timed "configuration_checks: host session E2E" "$python_bin" -m scripts.infrastructure.run_host_session_e2e
  timed "configuration_checks: host session plants" "$python_bin" -m scripts.infrastructure.host_session_plants
  timed "configuration_checks: AWS catalog setup E2E" "$python_bin" -m scripts.lakehouse.run_aws_catalog_setup_e2e --out "$quality_output/aws_catalog_setup" --mutations
  timed "configuration_checks: Glue mirror E2E" "$python_bin" -m scripts.lakehouse.run_glue_mirror_e2e --out "$quality_output/glue_mirror"
  timed "configuration_checks: install the pinned DVC" "$python_bin" -m scripts.quality.install_dvc
  timed "configuration_checks: DVC setup E2E" "$python_bin" -m scripts.quality.run_dvc_e2e
  timed "configuration_checks: DVC training baseline E2E" "$python_bin" -m scripts.training.run_dvc_baseline_e2e
  timed "configuration_checks: drift monitoring E2E" "$python_bin" -m monitoring.run_drift_e2e
  timed "configuration_checks: task cleanup E2E" "$python_bin" -m scripts.infrastructure.run_aws_tasks_e2e
  timed "configuration_checks: task cleanup plants" "$python_bin" -m scripts.infrastructure.aws_tasks_plants
  timed "configuration_checks: IAM preflight E2E" "$python_bin" -m scripts.infrastructure.run_iam_preflight_e2e --output-directory "$quality_output/iam_preflight"
  timed "configuration_checks: pytest with coverage" "$python_bin" -m pytest --cov=scripts.infrastructure --cov-report=term-missing -q
}
