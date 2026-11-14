#!/usr/bin/env bash
# The ci_tooling_runtime check group of scripts/run_ci.sh: the E2Es of the check tooling itself that run the Docker-call rebind, the container import hook and the CI
# worker deadlines. They start scratch repositories, worktrees and real hooks, so they are one hosted job (Core CI Tooling Runtime Checks), split from
# ci_tooling_checks so the two groups run side by side (the same checks, each once). The dispatcher runs the group in its own shell process (run_group), which loads
# only scripts/quality/timing.sh (timed) and this file, and then calls the entry function. It gets python_bin, repo_root, scratch, quality_output, e2e_output and
# CI_TIMING_FILE from the dispatcher beside the exported environment. This file defines functions only: the guard scripts/quality/ci_group_files.py checks it.
# shellcheck disable=SC2154,SC2329

ci_tooling_runtime() {
  timed "ci_tooling_runtime: docker_calls rebind E2E" "$python_bin" -m scripts.quality.run_docker_calls_rebind_e2e
  timed "ci_tooling_runtime: container import hook E2E" "$python_bin" -m scripts.quality.run_container_mounts_e2e --output "$quality_output/container_mounts"
  timed "ci_tooling_runtime: CI worker deadline E2E" "$python_bin" -m scripts.quality.run_worker_deadline_e2e
}
