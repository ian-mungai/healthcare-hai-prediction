#!/usr/bin/env bash
# The registry_checks check group of scripts/run_ci.sh: the ingestion registry tests, which the pytest run of configuration_policy_checks leaves out, split from that group because they passed its job timeout (the same tests, each once). The dispatcher runs the group in its own shell process (run_group), which loads only
# scripts/quality/timing.sh (timed) and this file, and then calls the entry function. It gets python_bin, repo_root, scratch, quality_output, e2e_output and
# CI_TIMING_FILE from the dispatcher beside the exported environment. This file defines functions only: the guard scripts/quality/ci_group_files.py checks it.
# shellcheck disable=SC2154,SC2329

registry_checks() {
  timed "registry_checks: ingestion registry tests" "$python_bin" -m pytest tests/test_ingestion_registry*.py -q
}
