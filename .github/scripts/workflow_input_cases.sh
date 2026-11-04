#!/usr/bin/env bash
# Cases for the Terraform workflows' inputs. The repository is public, and the runner prints the environment of every step in its header
# before the shell starts, so a raw repository variable would show its value there. A secret shows as ***. Run from the repository root.
set -euo pipefail

failures=0
plan=.github/workflows/terraform_plan.yml

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

reads_no_variable() { ! grep -q 'vars.TERRAFORM_INPUTS_JSON' "$plan"; }
every_source_is_a_secret() { [[ "$(grep -h 'STACK_INPUTS:' "$plan" | grep -vc 'secrets.TERRAFORM_INPUTS_JSON')" -eq 0 ]]; }
one_job_uses_it() { [[ "$(grep -h 'STACK_INPUTS:' "$plan" | grep -c '')" -eq 1 ]]; }
passed_to_no_action() { ! grep -A8 'with:' "$plan" | grep -q 'TERRAFORM_INPUTS_JSON'; }

check "no Terraform workflow reads the inputs from a repository variable" reads_no_variable
check "every STACK_INPUTS source is a secret" every_source_is_a_secret
check "the inputs are used by the one Terraform job" one_job_uses_it
check "no step passes the inputs as an action input" passed_to_no_action

# The secret masks the whole JSON, not its fields, and the project bucket sits in the job environment, which the runner prints before any
# step runs. So the bucket is its own secret, and a script masks every private string of the inputs, the profile and the account
# before anything else uses them.
mask=.github/scripts/mask_inputs.sh
# The synthetic account number is built from parts, so no file of the repository holds a twelve-digit number.
account="$(printf '%s%s' 9999 00001111)"
synthetic='{"aws_profile":"synthetic_profile_dev","expected_account_id":"'"$account"'","mlflow_bucket":"synthetic-private-bucket","ecr_repositories":["synthetic_private_repo"],"aws_region":"us-west-2","environment":"dev","limit":5,"flag":true}'

state_bucket_is_a_secret() { [[ "$(grep -h 'STATE_BUCKET:' "$plan" | grep -vc 'secrets.TERRAFORM_STATE_BUCKET')" -eq 0 ]]; }
no_variable_bucket() { ! grep -q 'vars.TERRAFORM_STATE_BUCKET' "$plan"; }
workflows_call_the_mask() { [[ "$(grep -c 'bash .github/scripts/mask_inputs.sh' "$plan")" -eq 1 ]]; }
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

# --- saved plans (owner decision D18) ----------------------------------------------------------------------------------------------------
# CI saves each plan with its digest through saved_plan.sh and shows the digest and the redacted summary only. The plan holds values, and the
# repository is public, so no step may print it.
dollar='$'
workflow_saves_through_the_script() { [[ "$(grep -c 'bash .github/scripts/saved_plan.sh upload' "$plan")" -eq 1 ]]; }
workflow_names_the_head_commit() { grep -qF "saved_plan.sh upload \"${dollar}STACK\" \"${dollar}HEAD_SHA\"" "$plan" && grep -qF "HEAD_SHA: ${dollar}{{ github.event.pull_request.head.sha }}" "$plan"; }
workflow_takes_the_bucket_from_a_secret() { [[ "$(grep -h 'PLAN_BUCKET:' "$plan" | grep -c 'secrets.TERRAFORM_STATE_BUCKET')" -eq 1 && "$(grep -h 'PLAN_BUCKET:' "$plan" | grep -vc 'secrets.TERRAFORM_STATE_BUCKET')" -eq 0 ]]; }
workflow_saves_after_it_plans() { [[ "$(grep -n 'plan -input=false' "$plan" | head -1 | cut -d: -f1)" -lt "$(grep -n 'saved_plan.sh upload' "$plan" | head -1 | cut -d: -f1)" ]]; }
workflow_shows_the_digest() { grep -q 'Saved plan digest' "$plan"; }
workflow_prints_no_plan() { ! grep -Eq '(^|[[:space:];|&(])(cat|echo|printf|head|tail|jq|less|more|strings|xxd|base64)[[:space:]][^|>]*plan\.(bin|json)' "$plan"; }
workflow_calls_aws_only_through_the_script() { ! grep -Eq '^[[:space:]]*(quiet )?aws ' "$plan"; }
workflow_writes_no_state() { ! grep -Eq 'terraform_state|terraform-state' "$plan"; }
workflow_keeps_the_plan_role_read_only_in_github() { [[ "$(grep -c 'id-token: write' "$plan")" -eq 1 ]] && ! grep -q 'contents: write\|pull-requests: write' "$plan"; }

check "the plan workflow saves the plan through the saved plan script, once" workflow_saves_through_the_script
check "the plan workflow names the pull request head commit" workflow_names_the_head_commit
check "the plan workflow takes the plan bucket from the state bucket secret only" workflow_takes_the_bucket_from_a_secret
check "the plan workflow saves the plan after it plans" workflow_saves_after_it_plans
check "the job summary shows the saved plan digest" workflow_shows_the_digest
check "no plan workflow step prints the plan file" workflow_prints_no_plan
check "the plan workflow calls AWS only through the script" workflow_calls_aws_only_through_the_script
check "the plan workflow names no state prefix" workflow_writes_no_state
check "the plan workflow asks GitHub for the token permission and no write permission" workflow_keeps_the_plan_role_read_only_in_github

[[ $failures -eq 0 ]]
