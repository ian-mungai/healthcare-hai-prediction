#!/usr/bin/env bash
# The terraform_checks check group of scripts/run_ci.sh. The dispatcher runs the group in its own shell process (run_group), which loads only
# scripts/quality/timing.sh (timed) and this file, and then calls the entry function. It gets python_bin, repo_root, scratch, quality_output, e2e_output and
# CI_TIMING_FILE from the dispatcher beside the exported environment. This file defines functions only: the guard scripts/quality/ci_group_files.py checks it.
# shellcheck disable=SC2154,SC2329

terraform_checks() {
  timed "terraform_checks: format" terraform fmt -check -recursive infra
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
  # The stacks run one after the other: Terraform's provider cache is not safe for concurrent init.
  for stack in storage iam platform services modules/bronze_writer_job jobs jobs/identity_probe; do
    source_dir="$repo_root/infra"
    if [[ "$stack" != "storage" ]]; then
      source_dir="$repo_root/infra/$stack"
    fi
    stack_root="$scratch/${stack//\//_}"
    test_dir="$stack_root/${source_dir#"$repo_root"/}"
    mkdir -p "$test_dir"
    for config_read_by_stacks in config/lakehouse/bronze_tables.json config/lakehouse/aws_bronze_grants.json; do
      mkdir -p "$stack_root/$(dirname "$config_read_by_stacks")"
      cp "$repo_root/$config_read_by_stacks" "$stack_root/$config_read_by_stacks"
    done
    # Copy every file Git tracks in the stack folder (the nested iam stack left out of storage), so a new template folder is
    # always tested and local state, variable files and provider folders never are. The listing starts at the repository root
    # with the folder as a pathspec: inside a commit hook GIT_DIR is set, and git -C <folder> would then list the whole tree.
    prefix="${source_dir#"$repo_root"/}/"
    while IFS= read -r -d '' file; do
      relative="${file#"$prefix"}"
      if [[ "$stack" == "storage" && "$relative" == iam/* ]]; then
        continue
      fi
      mkdir -p "$test_dir/$(dirname "$relative")"
      cp "$repo_root/$file" "$test_dir/$relative"
    done < <(git -C "$repo_root" ls-files -z -- "$prefix")
    # The jobs root uses the services flow test module as its stand-in network (source "../services/tests/flow").
    if [[ "$stack" == "jobs" ]]; then
      while IFS= read -r -d '' file; do
        mkdir -p "$stack_root/$(dirname "$file")"
        cp "$repo_root/$file" "$stack_root/$file"
      done < <(git -C "$repo_root" ls-files -z -- infra/services/tests/flow/)
    fi
    printf '\nChecking %s Terraform configuration\n' "$stack"
    timed "terraform_checks: $stack init" terraform -chdir="$test_dir" init -backend=false -input=false -lockfile=readonly -no-color
    timed "terraform_checks: $stack validate" terraform -chdir="$test_dir" validate -no-color
    timed "terraform_checks: $stack test" terraform -chdir="$test_dir" test -no-color
  done
  static_failed=0
  for stack in storage iam platform services modules/bronze_writer_job jobs jobs/identity_probe; do
    source_dir="$repo_root/infra"
    if [[ "$stack" != "storage" ]]; then
      source_dir="$repo_root/infra/$stack"
    fi
    if ! timed "terraform_checks: $stack static checks" "$python_bin" -m scripts.quality.static_checks --directory "$source_dir" --output-directory "$quality_output/$stack"; then
      static_failed=1
    fi
  done
  if [[ "$static_failed" != 0 ]]; then
    printf '\nTerraform static checks failed; review the retained quality evidence.\n' >&2
    return 1
  fi
}
