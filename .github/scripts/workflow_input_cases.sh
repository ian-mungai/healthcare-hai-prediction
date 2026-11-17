#!/usr/bin/env bash
# Cases for the Terraform workflows' inputs. The repository is public, and the runner prints the environment of every step in its header
# before the shell starts, so a raw repository variable would show its value there. A secret shows as ***. The last section checks the hosted cache keys and paths of
# ci.yml, which no local run can try. Run from the repository root.
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

# --- the Redis server of the hosted runner (owner decision D18) ---------------------------------------------------------------------------------------
# redis_acl_cases.sh starts a real redis-server, and the hosted runner image has none. The core job of the workflow installs the exact package of the Ubuntu 24.04 archive for the
# part that runs terraform_checks only, and the step fails when that version is not in the archive. Source of the version: packages.ubuntu.com, noble and noble-updates,
# redis-server 5:7.0.15-1ubuntu0.24.04.5 (published 2026-10-06), whose redis-tools dependency is the same version. A newer upload makes the step fail until the pin is moved.
workflow=.github/workflows/ci.yml
group=scripts/ci/groups/terraform_checks.sh
redis_version='5:7.0.15-1ubuntu0.24.04.5'
redis_step() { awk '/^      - name: Install the pinned Redis server$/ {on = 1; print; next} on && /^      - / {exit} on && /^  [a-z_]+:$/ {exit} on {print}' "$workflow" 2> /dev/null || true; }
install_line() { redis_step | grep -E '^\s+sudo apt-get install' || true; }

install_pins_the_server() { [[ "$(install_line | grep -c "redis-server=\$REDIS_VERSION")" -eq 1 ]]; }
install_pins_the_tools() { [[ "$(install_line | grep -c "redis-tools=\$REDIS_VERSION")" -eq 1 ]]; }
version_is_the_archive_version() { [[ "$(redis_step | grep -c "REDIS_VERSION: \"$redis_version\"")" -eq 1 ]]; }
no_other_redis_package() { [[ "$(install_line | tr ' ' '\n' | grep redis | sort | tr '\n' ' ')" == "\"redis-server=${dollar}REDIS_VERSION\" \"redis-tools=${dollar}REDIS_VERSION\" " ]]; }
step_stops_on_the_first_error() { redis_step | grep -q 'set -euo pipefail'; }
step_checks_the_installed_version() { redis_step | grep -q "dpkg-query --show" && redis_step | grep -qF "== \"${dollar}REDIS_VERSION\""; }
step_does_not_ignore_failure() { [[ -n "$(redis_step)" ]] && ! redis_step | grep -qE '\|\| *(true|:)|continue-on-error|2> */dev/null'; }
exactly_one_job_asks_for_the_server() { [[ "$(grep -c 'name: Install the pinned Redis server' "$workflow")" -eq 1 && "$(grep -c 'redis-server=' "$workflow")" -eq 1 ]]; }
core_job() { awk '/^  core:$/ {on = 1; next} on && /^  [a-z_]+:$/ {exit} on' "$workflow"; }
part_runs_terraform_checks() { python3 -c 'import json, sys; sys.exit("terraform_checks" not in json.load(open("config/quality/ci_groups.json"))["parts"]["core_sql_terraform"])'; }
that_job_runs_the_core_part() {
  core_job | grep -qF 'name: Install the pinned Redis server' && core_job | grep -qF "CI_PART: \${{ matrix.part }}" && core_job | grep -qx ' *- part: core_sql_terraform' && part_runs_terraform_checks
}
group_has_no_skip() { ! grep -qE 'GITHUB_ACTIONS|::warning' "$group"; }
group_runs_the_cases_in_every_environment() { [[ "$(grep -c 'bash .github/scripts/redis_acl_cases.sh' "$group")" -eq 1 ]] && ! grep -B2 'redis_acl_cases.sh' "$group" | grep -qE '^\s*(if|elif|else)'; }
local_ci_uses_the_installed_server() { ! grep -qE 'apt-get|brew ' "$group"; }

check "the install pins redis-server to the exact archive version" install_pins_the_server
check "the install pins redis-tools to the same version" install_pins_the_tools
check "the pin is the version of the Ubuntu 24.04 archive" version_is_the_archive_version
check "the install names no other Redis package" no_other_redis_package
check "the install step stops on the first error" step_stops_on_the_first_error
check "the install step checks the installed version against the pin" step_checks_the_installed_version
check "the install step does not ignore a failure" step_does_not_ignore_failure
check "exactly one workflow job installs the server, and no shared step does" exactly_one_job_asks_for_the_server
check "that job is the one that runs the core part, with terraform_checks" that_job_runs_the_core_part
check "the terraform_checks group no longer skips the Redis cases on a hosted runner" group_has_no_skip
check "the group runs the Redis cases in every environment" group_runs_the_cases_in_every_environment
check "the group installs nothing itself, so local CI keeps using the machine's server" local_ci_uses_the_installed_server

# --- the hosted caches: keys and paths -----------------------------------------------------------------------------------------------------------------
# The hosted caches (the pip and npm caches of the setup steps, the dbt package archive and the Terraform providers) cannot be tried on this machine, and a bad key
# fails quietly: a hashFiles over a file that does not exist hashes nothing, so the key never changes and an old entry is restored for ever. These cases read the
# workflow for what a local run can prove: every key input is a tracked file, no cache has a constant key, the provider key follows every lock file and the cache
# paths are the folders the jobs read. A hosted run proves the rest (see "first hosted run" in the issue register): the first run on main saves each entry, the next
# run restores it (a hit), and a changed lock file misses.
tracked() { git ls-files --error-unmatch -- "$1" > /dev/null 2>&1; }
key_inputs() { grep -oE "hashFiles\([^)]*\)" "$1" | grep -oE "'[^']+'" | tr -d "'" | sort -u; }
dependency_paths() { grep -E '^ +cache-dependency-path:' "$1" | sed -E 's/^ +cache-dependency-path: *//' | sort -u; }
every_key_input_is_tracked() {
  local file count=0
  while IFS= read -r file; do
    count=$((count + 1))
    tracked "$file" || return 1
  done < <(key_inputs "$1"; dependency_paths "$1")
  [[ $count -ge 4 ]]
}
# The key line of each actions/cache step: the first key after the step's uses line.
cache_keys() { awk '/uses: actions\/cache@/ {on = 1; next} on && /^ +key:/ {print; on = 0}' "$1"; }
no_cache_has_a_constant_key() {
  local keys
  keys="$(cache_keys "$1")"
  [[ -n "$keys" && "$(grep -c 'hashFiles(' <<< "$keys")" -eq "$(grep -c '' <<< "$keys")" ]]
}
lock_files() { if [[ -n "${CACHE_LOCK_FILES:-}" ]]; then tr ' ' '\n' <<< "$CACHE_LOCK_FILES"; else git ls-files '*.terraform.lock.hcl'; fi; }
# The provider key names some lock files. Another stack's lock file may stand outside the key only when it is byte for byte equal to one the key names, since the
# stacks then need the same providers.
provider_key_follows_every_lock_file() {
  local key named file same
  key="$(cache_keys "$1" | grep 'terraform-providers')"
  named="$(grep -oE "'[^']+'" <<< "$key" | tr -d "'" | grep 'terraform.lock.hcl' || true)"
  [[ -n "$named" ]] || return 1
  while IFS= read -r file; do
    grep -qxF "$file" <<< "$named" && continue
    same=0
    while IFS= read -r other; do
      cmp -s "$file" "$other" && same=1
    done <<< "$named"
    [[ $same -eq 1 ]] || return 1
  done < <(lock_files)
}
# The cache restores a folder, so the job must read that folder: the Terraform cache path is the TF_PLUGIN_CACHE_DIR the setup step exports, and the dbt cache path is
# where scripts/lakehouse/dbt_package_archive.py reads and writes its archives.
terraform_cache_path_is_the_one_exported() {
  local cached exported
  cached="$(grep -A5 'name: Restore the Terraform provider cache' "$1" | grep -oE 'path: .*' | sed -E 's/^path: //')"
  cached="${cached/#\~/\$HOME}"
  exported="$(grep -oE 'TF_PLUGIN_CACHE_DIR=[^ "]+' "$1" | head -1 | sed -E 's/^TF_PLUGIN_CACHE_DIR=//')"
  [[ -n "$cached" && "$cached" == "$exported" ]]
}
dbt_cache_path_is_the_archive_folder() {
  local cached
  cached="$(grep -A5 'name: Restore the dbt package archive' "$1" | grep -oE 'path: .*' | sed -E 's/^path: //')"
  [[ -n "$cached" ]] && grep -qF "Path(\"$cached\")" scripts/lakehouse/dbt_package_archive.py
}

check "every key input and cache dependency path of the workflow is a tracked file" every_key_input_is_tracked "$workflow"
check "no hosted cache has a constant key" no_cache_has_a_constant_key "$workflow"
check "the Terraform provider key follows every lock file" provider_key_follows_every_lock_file "$workflow"
check "the Terraform cache path is the folder the setup step exports" terraform_cache_path_is_the_one_exported "$workflow"
check "the dbt cache path is the folder of the package archive" dbt_cache_path_is_the_archive_folder "$workflow"

# Each defect is planted in a copy of the workflow, and the case that should see it must fail.
planted_dir="$(mktemp -d)"
plant() { sed -E "$1" "$workflow" > "$planted_dir/ci.yml"; }
refuses() { ! "$@" "$planted_dir/ci.yml"; }
plant "s#'dbt/package-lock.yml'#'dbt/package-lock.yaml'#"
check "a key input that is not a tracked file is refused" refuses every_key_input_is_tracked
plant "s#key: dbt-archive-.*#key: dbt-archive-v1#"
check "a constant cache key is refused" refuses no_cache_has_a_constant_key
plant "s#TF_PLUGIN_CACHE_DIR=\\\$HOME/.terraform.d/plugin-cache#TF_PLUGIN_CACHE_DIR=\\\$HOME/.terraform.d/other#"
check "a Terraform cache path that the setup step does not export is refused" refuses terraform_cache_path_is_the_one_exported
plant "s#path: data/analytics/dbt/package_archive#path: data/analytics/dbt/archive#"
check "a dbt cache path that is not the archive folder is refused" refuses dbt_cache_path_is_the_archive_folder
printf 'one\n' > "$planted_dir/a.lock.hcl"
printf 'two\n' > "$planted_dir/b.lock.hcl"
plant "s#hashFiles\\('infra/.terraform.lock.hcl', 'infra/iam/.terraform.lock.hcl'\\)#hashFiles('infra/.terraform.lock.hcl')#"
different_lock_outside_the_key() { CACHE_LOCK_FILES="infra/.terraform.lock.hcl $planted_dir/b.lock.hcl" provider_key_follows_every_lock_file "$1"; }
check "a lock file that differs from the ones the provider key names is refused" refuses different_lock_outside_the_key
rm -rf "$planted_dir"

[[ $failures -eq 0 ]]
