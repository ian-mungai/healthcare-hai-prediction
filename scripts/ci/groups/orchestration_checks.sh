#!/usr/bin/env bash
# The orchestration_checks check group of scripts/run_ci.sh. The dispatcher runs the group in its own shell process (run_group), which loads only
# scripts/quality/timing.sh (timed) and this file, and then calls the entry function. It gets python_bin, repo_root, scratch, quality_output, e2e_output and
# CI_TIMING_FILE from the dispatcher beside the exported environment. This file defines functions only: the guard scripts/quality/ci_group_files.py checks it.
# shellcheck disable=SC2154,SC2329

orchestration_checks() {
  timed "orchestration_checks: lineage E2E" "$python_bin" -m scripts.orchestration.run_lineage_e2e
  timed "orchestration_checks: lineage completion E2E" "$python_bin" -m scripts.orchestration.run_lineage_completion_e2e
  timed "orchestration_checks: lineage amendment E2E" "$python_bin" -m scripts.orchestration.run_lineage_amendment_e2e
  timed "orchestration_checks: publish contracts" "$python_bin" -m scripts.orchestration.run_publish_contracts
  timed "orchestration_checks: light pipeline" "$python_bin" -m scripts.orchestration.run_pipeline_light
  timed "orchestration_checks: stack E2E" "$python_bin" -m scripts.orchestration.run_stack_e2e
  timed "orchestration_checks: broker E2E" "$python_bin" -m scripts.orchestration.run_broker_e2e
  timed "orchestration_checks: ECS preparation" "$python_bin" -m scripts.orchestration.run_ecs_preparation --out "$quality_output/ecs_preparation"
  timed "orchestration_checks: supervisor E2E" "$python_bin" -m scripts.orchestration.run_supervisor_e2e
  timed "orchestration_checks: transform broker E2E" "$python_bin" -m scripts.orchestration.run_transform_broker_e2e
  # The offline checks skip the built Airflow image, which a CI runner does not have.
  timed "orchestration_checks: offline checks" "$python_bin" -m scripts.orchestration.offline_checks --no-image
  timed "orchestration_checks: failing first check" "$python_bin" -m scripts.orchestration.run_failing_first --check
}
