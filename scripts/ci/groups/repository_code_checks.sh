#!/usr/bin/env bash
# The repository_code_checks check group of scripts/run_ci.sh. The dispatcher runs the group in its own shell process (run_group), which loads only
# scripts/quality/timing.sh (timed) and this file, and then calls the entry function. It gets python_bin, repo_root, scratch, quality_output, e2e_output and
# CI_TIMING_FILE from the dispatcher beside the exported environment. This file defines functions only: the guard scripts/quality/ci_group_files.py checks it.
# shellcheck disable=SC2154,SC2329

python_suppressions() { git ls-files -z --cached --others --exclude-standard -- '*.py' | xargs -0 "$python_bin" -m scripts.quality.repo_checks suppressions; }
python_subprocess_imports() { git ls-files -z --cached --others --exclude-standard -- '*.py' | xargs -0 "$python_bin" -m scripts.quality.repo_checks subprocess-imports; }
# Only orchestration/launcher.py may call Docker (D1 section 2 item 4).
process_start_guard() { git ls-files -z --cached --others --exclude-standard -- '*.py' '*.sh' | xargs -0 "$python_bin" -m scripts.quality.docker_calls; }

repository_code_checks() {
  timed "repository_code_checks: suppressions" python_suppressions
  timed "repository_code_checks: subprocess imports" python_subprocess_imports
  timed "repository_code_checks: process start guard" process_start_guard
  timed "repository_code_checks: docker_calls rebind E2E" "$python_bin" -m scripts.quality.run_docker_calls_rebind_e2e
  timed "repository_code_checks: CI group files" "$python_bin" -m scripts.quality.ci_group_files
  timed "repository_code_checks: container imports" "$python_bin" -m scripts.quality.container_mounts
  timed "repository_code_checks: container import hook E2E" "$python_bin" -m scripts.quality.run_container_mounts_e2e --output "$quality_output/container_mounts"
  timed "repository_code_checks: documentation review E2E" "$python_bin" -m scripts.quality.run_documentation_review_e2e
  timed "repository_code_checks: CI groups selection E2E" "$python_bin" -m scripts.quality.run_ci_groups_e2e --selection-only
  timed "repository_code_checks: scratch worktree E2E" "$python_bin" -m scripts.quality.run_scratch_worktree_e2e
  timed "repository_code_checks: CI reuse E2E" "$python_bin" -m scripts.quality.run_ci_reuse_e2e
}
