#!/usr/bin/env bash
# The configuration_checks check group of scripts/run_ci.sh. The dispatcher runs the group in its own shell process (run_group), which loads only
# scripts/quality/timing.sh (timed) and this file, and then calls the entry function. It gets python_bin, repo_root, scratch, quality_output, e2e_output and
# CI_TIMING_FILE from the dispatcher beside the exported environment. This file defines functions only: the guard scripts/quality/ci_group_files.py checks it.
# shellcheck disable=SC2154,SC2329

configuration_checks() {
  local policy_output="${POLICY_ARTIFACT_DIR:-$repo_root/data/e2e/policies/$(date -u +%Y%m%dT%H%M%SZ)_$$}"
  timed "configuration_checks: infrastructure E2E" "$python_bin" -m scripts.infrastructure.run_e2e --output-directory "$e2e_output"
  timed "configuration_checks: policy E2E" "$python_bin" -m scripts.infrastructure.run_policy_e2e --output-directory "$policy_output"
  timed "configuration_checks: quality E2E" "$python_bin" -m scripts.quality.run_e2e --output-directory "$quality_output/e2e"
  timed "configuration_checks: secret store E2E" "$python_bin" -m scripts.infrastructure.run_secret_store_e2e
  timed "configuration_checks: pytest with coverage" "$python_bin" -m pytest --cov=scripts.infrastructure --cov-report=term-missing -q
}
