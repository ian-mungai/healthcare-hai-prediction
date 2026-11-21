#!/usr/bin/env bash
# The configuration_host_checks check group of scripts/run_ci.sh: the layer acceptance and report, the host session, the host code one-shot, the MLflow, Postgres and catalog restore E2Es and the IAM
# preflight, split from configuration_checks so the groups run side by side (the same checks, each once). The owner scripts, host session plants, AWS catalog and Glue mirror
# E2Es are the configuration_owner_checks group. The dispatcher runs the group in its own shell process (run_group), which loads only scripts/quality/timing.sh (timed) and
# this file, and then calls the entry function. It gets python_bin, repo_root, scratch, quality_output, e2e_output and CI_TIMING_FILE from the dispatcher beside the exported
# environment. This file defines functions only: the guard scripts/quality/ci_group_files.py checks it.
# shellcheck disable=SC2154,SC2329

configuration_host_checks() {
  timed "configuration_host_checks: layer acceptance E2E" "$python_bin" -m scripts.lakehouse.run_layer_acceptance_e2e --out "$quality_output/layer_acceptance"
  timed "configuration_host_checks: layer report E2E" "$python_bin" -m scripts.lakehouse.run_layer_report_e2e --out "$quality_output/layer_report"
  timed "configuration_host_checks: host session E2E" "$python_bin" -m scripts.infrastructure.run_host_session_e2e
  timed "configuration_host_checks: services apply launch hold E2E" "$python_bin" -m scripts.infrastructure.run_services_apply_e2e
  timed "configuration_host_checks: host code one-shot E2E" "$python_bin" -m scripts.infrastructure.run_code_fetch_e2e
  timed "configuration_host_checks: MLflow user one-shot E2E" "$python_bin" -m scripts.infrastructure.run_mlflow_user_e2e
  timed "configuration_host_checks: Postgres backup and restore E2E" "$python_bin" -m scripts.infrastructure.run_postgres_backup_e2e
  timed "configuration_host_checks: catalog restore E2E" "$python_bin" -m scripts.lakehouse.run_catalog_restore_e2e
  timed "configuration_host_checks: committed snapshot cases" "$python_bin" -m scripts.lakehouse.run_committed_snapshot_cases
  timed "configuration_host_checks: IAM preflight E2E" "$python_bin" -m scripts.infrastructure.run_iam_preflight_e2e --output-directory "$quality_output/iam_preflight"
}
