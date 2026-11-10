#!/usr/bin/env bash
# The orchestration_engine_e2e check group of scripts/run_ci.sh. The dispatcher runs the group in its own shell process (run_group), which loads only
# scripts/quality/timing.sh (timed) and this file, and then calls the entry function. It gets python_bin, repo_root, scratch, quality_output, e2e_output and
# CI_TIMING_FILE from the dispatcher beside the exported environment. This file defines functions only: the guard scripts/quality/ci_group_files.py checks it.
# The lean broker, stack, supervisor and transform broker E2Es run against a fake Docker Engine: their cases have timing assertions, so they are one
# hosted job (Core Orchestration Engine Tests) that runs nothing else. The broker E2E (scripts.orchestration.run_broker_e2e) is out of the gate: the broker
# is decided for removal with the direct ECS launch and nothing runs it (owner, Nov 9 2026).
# shellcheck disable=SC2154,SC2329

orchestration_engine_e2e() {
  timed "orchestration_engine_e2e: lean broker E2E" "$python_bin" -m scripts.orchestration.run_ecs_broker_e2e
  timed "orchestration_engine_e2e: stack E2E" "$python_bin" -m scripts.orchestration.run_stack_e2e
  timed "orchestration_engine_e2e: supervisor E2E" "$python_bin" -m scripts.orchestration.run_supervisor_e2e
  timed "orchestration_engine_e2e: transform broker E2E" "$python_bin" -m scripts.orchestration.run_transform_broker_e2e
}
