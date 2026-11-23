#!/usr/bin/env bash
# Cases for the workflow inputs of .github/workflows/ci.yml: the Redis server of the hosted runner, the hosted cache keys and paths, which no local run can try, and
# the job listing check of the CI Checks job (.github/scripts/ci_gate_jobs.sh). Run from the repository root.
set -euo pipefail

failures=0

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



dollar='$'


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
step_has_a_timeout() { [[ "$(redis_step | grep -cxF '        timeout-minutes: 5')" -eq 1 && "$(redis_step | grep -c 'timeout-minutes:')" -eq 1 ]]; }
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
check "the install step stops after 5 minutes, so a hung apt-get fails the job early" step_has_a_timeout
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

# --- the job listing check of the CI Checks job ---------------------------------------------------------------------------------------------------
# Every run is a full run: the listing must hold exactly the names of the registry, each once, and every job must have succeeded.
gate_dir="$(mktemp -d)"
printf '["CI Checks", "Job A", "Job B (shard 1 of 2)", "Job B (shard 2 of 2)"]\n' > "$gate_dir/registry.json"
gate_listing() { printf '%s\n' "$@" | tr '|' '\t' > "$gate_dir/jobs.tsv"; JOBS_REGISTRY="$gate_dir/registry.json" GATE_JOBS_FILE="$gate_dir/jobs.tsv" bash .github/scripts/ci_gate_jobs.sh; }
gate_passes() { gate_listing "$@" > /dev/null 2>&1; }
gate_fails_naming() { local name="$1"; shift; local output; output="$(gate_listing "$@" 2>&1)" && return 1; grep -qF -- "$name" <<< "$output"; }
all_green=("CI Checks|completed|success" "Job A|completed|success" "Job B (shard 1 of 2)|completed|success" "Job B (shard 2 of 2)|completed|success")
check "the job listing check passes when every registry job succeeded" gate_passes "${all_green[@]}"
check "the job listing check fails and names a missing matrix leg" gate_fails_naming "Job B (shard 2 of 2): expected" "${all_green[@]:0:3}"
check "the job listing check fails and names a job that is not in the registry" gate_fails_naming "Job C: listed, but not in" "${all_green[@]}" "Job C|completed|success"
check "the job listing check fails and names a job listed twice" gate_fails_naming "Job A: listed more than once" "${all_green[@]}" "Job A|completed|success"
check "the job listing check fails and names a skipped job" gate_fails_naming "Job A: conclusion 'skipped'" "CI Checks|completed|success" "Job A|completed|skipped" "${all_green[@]:2}"
check "the job listing check fails and names a failed job" gate_fails_naming "Job A: conclusion 'failure'" "CI Checks|completed|success" "Job A|completed|failure" "${all_green[@]:2}"
check "the job listing check fails and names a job that has not completed" gate_fails_naming "Job A: status 'in_progress'" "CI Checks|completed|success" "Job A|in_progress|" "${all_green[@]:2}"
check "the job listing check fails for an empty listing and names every expected job" gate_fails_naming "Job A: expected" "CI Checks|completed|success"
rm -rf "$gate_dir"

[[ $failures -eq 0 ]]
