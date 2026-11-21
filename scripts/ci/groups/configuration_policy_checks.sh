#!/usr/bin/env bash
# The configuration_policy_checks check group of scripts/run_ci.sh: the policy E2E, the OpenMetadata image mirror E2E, the monitoring files, DVC and drift monitoring E2Es, the task cleanup E2Es and the pytest run with coverage, split from configuration_checks so the groups run side by side (the same checks, each once). The dispatcher runs the group in its own shell process (run_group), which loads only
# scripts/quality/timing.sh (timed) and this file, and then calls the entry function. It gets python_bin, repo_root, scratch, quality_output, e2e_output and
# CI_TIMING_FILE from the dispatcher beside the exported environment. This file defines functions only: the guard scripts/quality/ci_group_files.py checks it.
# shellcheck disable=SC2154,SC2329

configuration_policy_checks() {
  local policy_output="${POLICY_ARTIFACT_DIR:-$repo_root/data/e2e/policies/$(date -u +%Y%m%dT%H%M%SZ)_$$}"
  timed "configuration_policy_checks: policy E2E" "$python_bin" -m scripts.infrastructure.run_policy_e2e --output-directory "$policy_output"
  timed "configuration_policy_checks: OpenMetadata image mirror E2E" "$python_bin" -m scripts.infrastructure.run_push_images_e2e
  timed "configuration_policy_checks: monitoring files E2E" "$python_bin" -m scripts.infrastructure.run_monitoring_e2e
  timed "configuration_policy_checks: install the pinned DVC" "$python_bin" -m scripts.quality.install_dvc
  timed "configuration_policy_checks: DVC setup E2E" "$python_bin" -m scripts.quality.run_dvc_e2e
  timed "configuration_policy_checks: DVC training baseline E2E" "$python_bin" -m scripts.training.run_dvc_baseline_e2e
  timed "configuration_policy_checks: drift monitoring E2E" "$python_bin" -m monitoring.run_drift_e2e
  timed "configuration_policy_checks: task cleanup E2E" "$python_bin" -m scripts.infrastructure.run_aws_tasks_e2e
  timed "configuration_policy_checks: task cleanup plants" "$python_bin" -m scripts.infrastructure.aws_tasks_plants
  timed "configuration_policy_checks: pytest with coverage" "$python_bin" -m pytest --cov=scripts.infrastructure --cov-report=term-missing -q
}
