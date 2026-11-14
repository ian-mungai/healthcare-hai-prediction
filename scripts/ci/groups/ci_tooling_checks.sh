#!/usr/bin/env bash
# The ci_tooling_checks check group of scripts/run_ci.sh. The dispatcher runs the group in its own shell process (run_group), which loads only
# scripts/quality/timing.sh (timed) and this file, and then calls the entry function. It gets python_bin, repo_root, scratch, quality_output, e2e_output and
# CI_TIMING_FILE from the dispatcher beside the exported environment. This file defines functions only: the guard scripts/quality/ci_group_files.py checks it.
# The E2Es of the check tooling itself that select groups and stop processes: the group selection and the process group stop. They start scratch repositories,
# worktrees and real hooks, so they are one hosted job (Core CI Tooling Checks). The Docker-call rebind, the container import hook and the worker deadlines are the
# ci_tooling_runtime group (Core CI Tooling Runtime Checks), so the two groups run side by side.
# shellcheck disable=SC2154,SC2329

ci_tooling_checks() {
  timed "ci_tooling_checks: CI groups selection E2E" "$python_bin" -m scripts.quality.run_ci_groups_e2e --selection-only
  timed "ci_tooling_checks: process group stop E2E" "$python_bin" -m scripts.quality.run_process_e2e
}
