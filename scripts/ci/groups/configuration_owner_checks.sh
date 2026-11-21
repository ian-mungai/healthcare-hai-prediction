#!/usr/bin/env bash
# The configuration_owner_checks check group of scripts/run_ci.sh: the owner scripts E2E, the host session plants, the AWS catalog setup E2E and the Glue mirror E2E, split from configuration_host_checks so the groups run side by side (the same checks, each once). The dispatcher runs the group in its own shell process (run_group), which loads only
# scripts/quality/timing.sh (timed) and this file, and then calls the entry function. It gets python_bin, repo_root, scratch, quality_output, e2e_output and
# CI_TIMING_FILE from the dispatcher beside the exported environment. This file defines functions only: the guard scripts/quality/ci_group_files.py checks it.
# shellcheck disable=SC2154,SC2329

configuration_owner_checks() {
  timed "configuration_owner_checks: owner scripts E2E" "$python_bin" -m scripts.infrastructure.run_owner_scripts_e2e --out "$quality_output/owner_scripts"
  timed "configuration_owner_checks: host session plants" "$python_bin" -m scripts.infrastructure.host_session_plants
  timed "configuration_owner_checks: AWS catalog setup E2E" "$python_bin" -m scripts.lakehouse.run_aws_catalog_setup_e2e --out "$quality_output/aws_catalog_setup" --mutations
  timed "configuration_owner_checks: Glue mirror E2E" "$python_bin" -m scripts.lakehouse.run_glue_mirror_e2e --out "$quality_output/glue_mirror"
}
