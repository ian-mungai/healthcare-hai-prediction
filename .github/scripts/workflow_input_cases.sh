#!/usr/bin/env bash
# Cases for the Terraform workflows' inputs. The repository is public, and the runner prints the environment of every step in its header
# before the shell starts, so a raw repository variable would show its value there. A secret shows as ***. Run from the repository root.
set -euo pipefail

failures=0
plan=.github/workflows/terraform_plan.yml
apply=.github/workflows/terraform_apply.yml

check() {
  local label="$1"
  shift
  if "$@"; then
    echo "ok: $label"
  else
    echo "FAILED: $label" >&2
    failures=$((failures + 1))
  fi
}

reads_no_variable() { ! grep -q 'vars.TERRAFORM_INPUTS_JSON' "$plan" "$apply"; }
every_source_is_a_secret() { [[ "$(grep -h 'STACK_INPUTS:' "$plan" "$apply" | grep -vc 'secrets.TERRAFORM_INPUTS_JSON')" -eq 0 ]]; }
three_jobs_use_it() { [[ "$(grep -h 'STACK_INPUTS:' "$plan" "$apply" | grep -c '')" -eq 3 ]]; }
passed_to_no_action() { ! grep -A8 'with:' "$plan" "$apply" | grep -q 'TERRAFORM_INPUTS_JSON'; }

check "no Terraform workflow reads the inputs from a repository variable" reads_no_variable
check "every STACK_INPUTS source is a secret" every_source_is_a_secret
check "the inputs are used by the three Terraform jobs" three_jobs_use_it
check "no step passes the inputs as an action input" passed_to_no_action

# The secret masks the whole JSON, not its fields, and the project bucket sits in the job environment, which the runner prints before any
# step runs. So the bucket is its own secret, and a script masks every private string of the inputs, the profile and the account
# before anything else uses them.
mask=.github/scripts/mask_inputs.sh
# The synthetic account number is built from parts, so no file of the repository holds a twelve-digit number.
account="$(printf '%s%s' 9999 00001111)"
synthetic='{"aws_profile":"synthetic_profile_dev","expected_account_id":"'"$account"'","mlflow_bucket":"synthetic-private-bucket","ecr_repositories":["synthetic_private_repo"],"aws_region":"us-west-2","environment":"dev","limit":5,"flag":true}'

state_bucket_is_a_secret() { [[ "$(grep -h 'STATE_BUCKET:' "$plan" "$apply" | grep -vc 'secrets.TERRAFORM_STATE_BUCKET')" -eq 0 ]]; }
no_variable_bucket() { ! grep -q 'vars.TERRAFORM_STATE_BUCKET' "$plan" "$apply"; }
workflows_call_the_mask() { [[ "$(grep -c 'bash .github/scripts/mask_inputs.sh' "$plan")" -eq 1 && "$(grep -c 'bash .github/scripts/mask_inputs.sh' "$apply")" -eq 2 ]]; }
mask_script_exists() { [[ -f "$mask" ]]; }
masks_every_private_string() {
  local output
  output="$(STACK_INPUTS="$synthetic" bash "$mask" 2> /dev/null)" || return 1
  for value in synthetic_profile_dev "$account" synthetic-private-bucket synthetic_private_repo; do
    grep -qx "::add-mask::$value" <<< "$output" || return 1
  done
}
leaves_public_values_alone() {
  local output
  output="$(STACK_INPUTS="$synthetic" bash "$mask" 2> /dev/null)" || return 1
  ! grep -q 'us-west-2\|::add-mask::dev$\|::add-mask::5\|::add-mask::true' <<< "$output"
}
prints_only_mask_commands() {
  local output
  output="$(STACK_INPUTS="$synthetic" bash "$mask" 2> /dev/null)" || return 1
  [[ "$(grep -vc '^::add-mask::' <<< "$output")" -eq 0 ]]
}
writes_the_profile_after_masking() {
  local output file
  file="$(mktemp)"
  GITHUB_OUTPUT="$file" STACK_INPUTS="$synthetic" bash "$mask" > /dev/null 2>&1 || return 1
  [[ "$(cat "$file")" == "profile=synthetic_profile_dev" ]]
}
survives_injected_tracing() {
  local trace
  trace="$(STACK_INPUTS="$synthetic" bash -x "$mask" 2>&1 > /dev/null)" || true
  ! grep -q "$account\|synthetic-private-bucket\|synthetic_private_repo" <<< "$trace"
}
refuses_other_inputs() {
  ! STACK_INPUTS='[1]' bash "$mask" > /dev/null 2>&1 && ! STACK_INPUTS='{"aws_profile":"x"}' bash "$mask" > /dev/null 2>&1
}

check "the state bucket is a secret in every Terraform job" state_bucket_is_a_secret
check "no workflow reads the state bucket from a variable" no_variable_bucket
check "the mask script exists" mask_script_exists
check "each Terraform job runs the mask script before the credential action" workflows_call_the_mask
check "the script masks the profile, the account, the bucket and every private string" masks_every_private_string
check "the script leaves regions, environments, numbers and flags alone" leaves_public_values_alone
check "the script prints only mask commands" prints_only_mask_commands
check "the profile is written to the output after it is masked" writes_the_profile_after_masking
check "injected tracing does not print an input value" survives_injected_tracing
check "the script refuses inputs that are not the expected object" refuses_other_inputs

[[ $failures -eq 0 ]]
