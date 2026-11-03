#!/usr/bin/env bash
# The ci_tooling_checks check group of scripts/run_ci.sh. The dispatcher runs the group in its own shell process (run_group), which loads only
# scripts/quality/timing.sh (timed) and this file, and then calls the entry function. It gets python_bin, repo_root, scratch, quality_output, e2e_output and
# CI_TIMING_FILE from the dispatcher beside the exported environment. This file defines functions only: the guard scripts/quality/ci_group_files.py checks it.
# The E2Es of the check tooling itself: the Docker-call rebind, the container import hook, the group selection and the process and worker deadline stops. They
# start scratch repositories, worktrees and real hooks, so they are one hosted job (Core CI Tooling Checks).
# shellcheck disable=SC2154,SC2329

ci_tooling_checks() {
  timed "ci_tooling_checks: docker_calls rebind E2E" "$python_bin" -m scripts.quality.run_docker_calls_rebind_e2e
  timed "ci_tooling_checks: container import hook E2E" "$python_bin" -m scripts.quality.run_container_mounts_e2e --output "$quality_output/container_mounts"
  timed "ci_tooling_checks: CI groups selection E2E" "$python_bin" -m scripts.quality.run_ci_groups_e2e --selection-only
  timed "ci_tooling_checks: process group stop E2E" "$python_bin" -m scripts.quality.run_process_e2e
  timed "ci_tooling_checks: CI worker deadline E2E" "$python_bin" -m scripts.quality.run_worker_deadline_e2e
}
