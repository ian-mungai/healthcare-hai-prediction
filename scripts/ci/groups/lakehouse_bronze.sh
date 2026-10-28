#!/usr/bin/env bash
# The lakehouse_bronze check group of scripts/run_ci.sh. The dispatcher runs the group in its own shell process (run_group), which loads only
# scripts/quality/timing.sh (timed) and this file, and then calls the entry function. It gets python_bin, repo_root, scratch, quality_output, e2e_output and
# CI_TIMING_FILE from the dispatcher beside the exported environment. This file defines functions only: the guard scripts/quality/ci_group_files.py checks it.
# shellcheck disable=SC2154,SC2329

lakehouse_bronze() {
  timed "lakehouse_bronze: resource checks" "$python_bin" -m scripts.lakehouse.run_resource_checks
  timed "lakehouse_bronze: run lock E2E" "$python_bin" -m scripts.lakehouse.run_lock_e2e
  timed "lakehouse_bronze: catalog journal E2E" "$python_bin" -m scripts.lakehouse.run_catalog_journal_e2e --output "$quality_output/catalog_journal"
  timed "lakehouse_bronze: catalog journal planted defects" "$python_bin" -m scripts.lakehouse.run_catalog_journal_defects --output "$quality_output/catalog_journal_defects"
  timed "lakehouse_bronze: provenance E2E" "$python_bin" -m scripts.lakehouse.run_provenance_e2e
  timed "lakehouse_bronze: storage single copy E2E" "$python_bin" -m scripts.lakehouse.run_storage_single_copy_e2e --report "$quality_output/storage_single_copy.json"
  timed "lakehouse_bronze: bootstrap diagnostics E2E" "$python_bin" -m scripts.lakehouse.run_bronze_diagnostics_e2e --report "$quality_output/bronze_diagnostics.json"
  timed "lakehouse_bronze: ECS staging contract cases" "$python_bin" -m scripts.orchestration.ecs_stage_contract_cases --report "$repo_root/data/e2e/ecs_target/stage_contract_$(date -u +%Y%m%dT%H%M%SZ)_$$/report.json"
  timed "lakehouse_bronze: ECS staging runtime cases" "$python_bin" -m scripts.orchestration.ecs_stage_runtime_cases --report "$repo_root/data/e2e/ecs_target/stage_runtime_$(date -u +%Y%m%dT%H%M%SZ)_$$/report.json"
  timed "lakehouse_bronze: contracts E2E" "$python_bin" -m scripts.orchestration.run_contracts_e2e
}
