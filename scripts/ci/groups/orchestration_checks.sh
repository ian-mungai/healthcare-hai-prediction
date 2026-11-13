#!/usr/bin/env bash
# The orchestration_checks check group of scripts/run_ci.sh. The dispatcher runs the group in its own shell process (run_group), which loads only
# scripts/quality/timing.sh (timed) and this file, and then calls the entry function. It gets python_bin, repo_root, scratch, quality_output, e2e_output and
# CI_TIMING_FILE from the dispatcher beside the exported environment. This file defines functions only: the guard scripts/quality/ci_group_files.py checks it.
# shellcheck disable=SC2154,SC2329

orchestration_checks() {
  timed "orchestration_checks: SDK worker fixtures" "$python_bin" -B -m scripts.orchestration.sdk_worker_cases --check-fixtures --fixtures "$repo_root/data_contracts/orchestration/examples/sdk_worker_cases" --revision "$(git rev-parse HEAD)" --output "$quality_output/sdk_worker_cases/report.json"
  timed "orchestration_checks: SDK source mutations" "$python_bin" -B -m scripts.orchestration.sdk_static_mutations --output "$quality_output/sdk_static_mutations/report.json"
  timed "orchestration_checks: catalog transport cases" "$python_bin" -B -m scripts.orchestration.catalog_transport_cases --mutations --output "$quality_output/catalog_transport_cases/report.json"
  timed "orchestration_checks: lineage E2E" "$python_bin" -m scripts.orchestration.run_lineage_e2e
  timed "orchestration_checks: ECS placement static proof" "$python_bin" -m scripts.orchestration.run_ecs_target_e2e --proof S
  timed "orchestration_checks: task guard cases" "$python_bin" -m scripts.orchestration.task_guard_cases
  timed "orchestration_checks: trigger_dag cases" "$python_bin" -B -m scripts.orchestration.trigger_dag_cases --output "$quality_output/trigger_dag_cases/report.json"
  timed "orchestration_checks: direct ECS launch E2E" "$python_bin" -m scripts.orchestration.run_ecs_launch_e2e
  timed "orchestration_checks: Airflow launch mode E2E" "$python_bin" -m scripts.orchestration.run_airflow_mode_e2e
  timed "orchestration_checks: layer driver E2E" "$python_bin" -m scripts.lakehouse.run_layer_run_e2e
  timed "orchestration_checks: bronze driver E2E" "$python_bin" -m scripts.lakehouse.run_bronze_run_e2e
  timed "orchestration_checks: lineage completion E2E" "$python_bin" -m scripts.orchestration.run_lineage_completion_e2e
  timed "orchestration_checks: lineage service cases" "$python_bin" -m scripts.orchestration.run_lineage_service_cases
  timed "orchestration_checks: publish contracts" "$python_bin" -m scripts.orchestration.run_publish_contracts
  timed "orchestration_checks: light pipeline" "$python_bin" -m scripts.orchestration.run_pipeline_light
  timed "orchestration_checks: ECS preparation" "$python_bin" -m scripts.orchestration.run_ecs_preparation --out "$quality_output/ecs_preparation"
  # The offline checks skip the built Airflow image, which a CI runner does not have.
  timed "orchestration_checks: model API E2E" "$python_bin" -B -m scripts.serving.run_serving_checks --output "$quality_output/serving_checks"
  # The page cases need Streamlit, which the runner does not install, so the runner reports them as blocked here. They run in the page image.
  timed "orchestration_checks: Streamlit client E2E" "$python_bin" -B -m scripts.streamlit_app.run_streamlit_checks --output "$quality_output/streamlit_checks"
  timed "orchestration_checks: offline checks" "$python_bin" -m scripts.orchestration.offline_checks --no-image
}
