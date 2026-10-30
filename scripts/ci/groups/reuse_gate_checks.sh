#!/usr/bin/env bash
# The reuse_gate_checks check group of scripts/run_ci.sh. The dispatcher runs the group in its own shell process (run_group), which loads only
# scripts/quality/timing.sh (timed) and this file, and then calls the entry function. It gets python_bin, repo_root, scratch, quality_output, e2e_output and
# CI_TIMING_FILE from the dispatcher beside the exported environment. This file defines functions only: the guard scripts/quality/ci_group_files.py checks it.
# shellcheck disable=SC2154,SC2329

reuse_gate_checks() {
  timed "reuse_gate_checks: CI groups reuse and shard cases" "$python_bin" -m scripts.quality.run_ci_groups_e2e --reuse-only
  timed "reuse_gate_checks: CI groups gate stage cases" "$python_bin" -m scripts.quality.run_ci_groups_e2e --gate-stage-only
}
