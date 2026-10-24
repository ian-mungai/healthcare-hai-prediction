#!/usr/bin/env bash
# The lakehouse_silver check group of scripts/run_ci.sh. The dispatcher runs the group in its own shell process (run_group), which loads only
# scripts/quality/timing.sh (timed) and this file, and then calls the entry function. It gets python_bin, repo_root, scratch, quality_output, e2e_output and
# CI_TIMING_FILE from the dispatcher beside the exported environment. This file defines functions only: the guard scripts/quality/ci_group_files.py checks it.
# shellcheck disable=SC2154,SC2329

lakehouse_silver() {
  # The silver suites must match the kept-model contract, the moved checks and the baselines they are generated from.
  timed "lakehouse_silver: suite check" "$python_bin" -m scripts.lakehouse.run_silver_e2e --check-suites
}
