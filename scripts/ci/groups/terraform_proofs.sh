#!/usr/bin/env bash
# The terraform_proofs check group of scripts/run_ci.sh: the two Terraform proofs of the jobs stack that take the longest, split from terraform_checks so the two groups run
# side by side (the same checks, each once). The dispatcher runs the group in its own shell process (run_group), which loads only scripts/quality/timing.sh (timed) and
# this file, and then calls the entry function. It gets python_bin, repo_root, scratch, quality_output, e2e_output and CI_TIMING_FILE from the dispatcher beside the
# exported environment. This file defines functions only: the guard scripts/quality/ci_group_files.py checks it.
# shellcheck disable=SC2154,SC2329

terraform_proofs() {
  export TF_PLUGIN_CACHE_DIR="${TF_PLUGIN_CACHE_DIR:-$scratch/provider_cache}"
  mkdir -p "$TF_PLUGIN_CACHE_DIR"
  # Isolate tests from local credentials, deployment inputs and real Terraform state (this group runs in its own
  # subshell, so the other groups keep their environment).
  unset AWS_PROFILE AWS_DEFAULT_PROFILE AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN AWS_SECURITY_TOKEN
  unset AWS_WEB_IDENTITY_TOKEN_FILE AWS_ROLE_ARN AWS_CONTAINER_CREDENTIALS_FULL_URI AWS_CONTAINER_CREDENTIALS_RELATIVE_URI
  unset AWS_CONTAINER_AUTHORIZATION_TOKEN AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE
  export AWS_CONFIG_FILE="$scratch/empty_aws_config"
  export AWS_SHARED_CREDENTIALS_FILE="$scratch/empty_aws_credentials"
  export AWS_EC2_METADATA_DISABLED=true TF_IN_AUTOMATION=1 TF_INPUT=0
  touch "$AWS_CONFIG_FILE" "$AWS_SHARED_CREDENTIALS_FILE"
  # Every task definition of the jobs plan, child modules included, is an EC2 bridge task, and a task definition planted through each way of writing one is caught.
  timed "terraform_proofs: jobs host-only proof" "$python_bin" -m scripts.infrastructure.run_jobs_host_only_e2e --out "$e2e_output/jobs_host_only"
  # The apply that removes the retired Fargate job definitions only forgets them: the plan against the historical state changes exactly the 19 retired addresses and no host definition,
  # and the plan after it is empty.
  timed "terraform_proofs: retired definitions proof" "$python_bin" -m scripts.infrastructure.run_retired_definitions_e2e --out "$e2e_output/retired_definitions"
  # A jobs apply that replaces a task definition registers the new revision before it retires the old one: the plan of a replacement of all sixteen host and publisher definitions
  # creates first, and the old revision stays registered (skip_destroy), so a registration that ECS refuses keeps the old definition.
  timed "terraform_proofs: jobs replace order proof" "$python_bin" -m scripts.infrastructure.run_jobs_replace_order_e2e --out "$e2e_output/jobs_replace_order"
  # A services apply that replaces a task definition registers the new revision before it retires the old one: the plan of a replacement of all twelve service and four one-shot definitions
  # creates first, the old revision stays registered (skip_destroy), and every fixed service updates to the new revision, so a registration that ECS refuses keeps the old definition.
  timed "terraform_proofs: services replace order proof" "$python_bin" -m scripts.infrastructure.run_services_replace_order_e2e --out "$e2e_output/services_replace_order"
}
