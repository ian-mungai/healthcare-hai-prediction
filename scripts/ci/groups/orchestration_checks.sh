#!/usr/bin/env bash
# The orchestration_checks check group of scripts/run_ci.sh. The dispatcher runs the group in its own shell process (run_group), which loads only
# scripts/quality/timing.sh (timed) and this file, and then calls the entry function. It gets python_bin, repo_root, scratch, quality_output, e2e_output and
# CI_TIMING_FILE from the dispatcher beside the exported environment. This file defines functions only: the guard scripts/quality/ci_group_files.py checks it.
# shellcheck disable=SC2154,SC2329

orchestration_checks() {
  timed "orchestration_checks: SDK worker fixtures" "$python_bin" -B -m scripts.orchestration.sdk_worker_cases --check-fixtures --fixtures "$repo_root/data_contracts/orchestration/examples/sdk_worker_cases" --revision "$(git rev-parse HEAD)" --output "$quality_output/sdk_worker_cases/report.json"
  timed "orchestration_checks: SDK lifecycle cases" "$python_bin" -B -m scripts.orchestration.sdk_lifecycle_cases --fixtures "$repo_root/data_contracts/orchestration/examples/sdk_worker_cases" --output "$quality_output/sdk_lifecycle_cases/report.json"
  timed "orchestration_checks: SDK runtime cases" "$python_bin" -B -m scripts.orchestration.sdk_runtime_cases --mode native --cases all --output "$quality_output/sdk_runtime_cases/report.json"
  timed "orchestration_checks: SDK source mutations" "$python_bin" -B -m scripts.orchestration.sdk_static_mutations --output "$quality_output/sdk_static_mutations/report.json"
  timed "orchestration_checks: catalog transport cases" "$python_bin" -B -m scripts.orchestration.catalog_transport_cases --mutations --output "$quality_output/catalog_transport_cases/report.json"
  if [[ "$(uname -s)" == Linux ]]; then
    timed "orchestration_checks: SDK supervisor Linux source smoke" "$python_bin" -B -m scripts.orchestration.sdk_supervisor_cases --mode native --source-smoke --output "$quality_output/sdk_supervisor_cases/report.json"
  else
    timed "orchestration_checks: SDK supervisor source checks" "$python_bin" -B -m scripts.orchestration.sdk_supervisor_cases --mode static --source-smoke --output "$quality_output/sdk_supervisor_cases/report.json"
  fi
  timed "orchestration_checks: lineage E2E" "$python_bin" -m scripts.orchestration.run_lineage_e2e
  timed "orchestration_checks: ECS placement static proof" "$python_bin" -m scripts.orchestration.run_ecs_target_e2e --proof S
  timed "orchestration_checks: ECS placement handler proof" "$python_bin" -m scripts.orchestration.run_ecs_target_e2e --proof D
  timed "orchestration_checks: lean broker E2E" "$python_bin" -m scripts.orchestration.run_ecs_broker_e2e
  timed "orchestration_checks: layer driver E2E" "$python_bin" -m scripts.lakehouse.run_layer_run_e2e
  timed "orchestration_checks: bronze driver E2E" "$python_bin" -m scripts.lakehouse.run_bronze_run_e2e
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
  timed "orchestration_checks: model API E2E" "$python_bin" -B -m scripts.serving.run_serving_checks --output "$quality_output/serving_checks"
  # The page cases need Streamlit, which the runner does not install, so the runner reports them as blocked here. They run in the page image.
  timed "orchestration_checks: Streamlit client E2E" "$python_bin" -B -m scripts.streamlit_app.run_streamlit_checks --output "$quality_output/streamlit_checks"
  timed "orchestration_checks: offline checks" "$python_bin" -m scripts.orchestration.offline_checks --no-image
  timed "orchestration_checks: failing first check" "$python_bin" -m scripts.orchestration.run_failing_first --check
}
