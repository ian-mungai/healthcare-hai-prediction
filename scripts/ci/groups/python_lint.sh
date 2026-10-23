#!/usr/bin/env bash
# The python_lint check group of scripts/run_ci.sh. The dispatcher runs the group in its own shell process (run_group), which loads only
# scripts/quality/timing.sh (timed) and this file, and then calls the entry function. It gets python_bin, repo_root, scratch, quality_output, e2e_output and
# CI_TIMING_FILE from the dispatcher beside the exported environment. This file defines functions only: the guard scripts/quality/ci_group_files.py checks it.
# shellcheck disable=SC2154,SC2329

python_lint() {
  timed "python_lint: typecheck" "$python_bin" -m scripts.quality.typecheck --check-untyped-defs --disallow-untyped-defs
}
