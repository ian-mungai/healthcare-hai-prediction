#!/usr/bin/env bash
set -euo pipefail

# CI_PART selects what runs: "all" (the default, every part), "core" (everything except the launcher E2E and the
# acquisition gate) or "launcher" (the gates and the launcher E2E, one LAUNCHER_SHARD=K/N of it if set). GitHub CI runs
# "core" and the launcher shards as parallel jobs.
# CI_SCOPE selects the check groups (scripts/quality/ci_groups.py, config/quality/ci_groups.json): "full" (the default,
# every group: run it before every push), "staged" (the groups the staged paths touch: the local_ci_affected commit
# hook) or "range" (the groups RANGE_START..RANGE_END touches: GitHub CI). The privacy, secret, credential, data-file,
# writing, Markdown lint and documentation gates run in every scope and in every part, also when no group is selected.
# CI_LIST_ONLY=1 prints the plan and runs nothing. Each step's seconds are printed and added to the job summary (GITHUB_STEP_SUMMARY).
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"
bash -n scripts/run_ci.sh scripts/infrastructure/*.sh
python_bin="${PYTHON_BIN:-$repo_root/.venv/bin/python}"
part="${CI_PART:-all}"
case "$part" in
  all | core | launcher) ;;
  *) printf 'CI_PART must be all, core or launcher, not %s\n' "$part" >&2; exit 2 ;;
esac
scope="${CI_SCOPE:-full}"
case "$scope" in
  full | staged | range) ;;
  *) printf 'CI_SCOPE must be full, staged or range, not %s\n' "$scope" >&2; exit 2 ;;
esac
summary="${GITHUB_STEP_SUMMARY:-/dev/null}"
scratch="$(mktemp -d "${TMPDIR:-/tmp}/hai_ci.XXXXXX")"
# Only a run that sets ci_finished=1 exits 0: bash 3.2 (macOS /bin/bash) reports status 0 to this trap when an unbound
# variable ends the script, so the trap fails every other exit itself.
trap 'rm -rf "$scratch"; [[ "${ci_finished:-0}" == 1 ]] || exit 1' EXIT

plan_args=(--scope "$scope" --part "$part" --format shell)
if [[ "$scope" == "range" ]]; then
  plan_args+=(--from "${RANGE_START:-}" --to "${RANGE_END:-}")
fi
plan="$("$python_bin" -m scripts.quality.ci_groups plan "${plan_args[@]}")"
selected=" "
while read -r kind group rest; do
  case "$kind" in
    reason) printf 'Check groups (scope %s, part %s): %s %s\n' "$scope" "$part" "$group" "$rest" | tee -a "$summary" ;;
    run) selected+="$group "; printf 'run %s\n' "$group" ;;
    skip) printf 'skipped %s: %s\n' "$group" "$rest" | tee -a "$summary" ;;
  esac
done <<<"$plan"
if [[ "${CI_LIST_ONLY:-0}" == "1" ]]; then
  ci_finished=1
  exit 0
fi
is_selected() { [[ "$selected" == *" $1 "* ]]; }
printf '\n| Step | Seconds |\n| --- | ---: |\n' >>"$summary"
record() { printf '%s: %s s\n' "$1" "$2"; printf '| %s | %s |\n' "$1" "$2" >>"$summary"; }

started=$SECONDS
"$python_bin" -m scripts.quality.scan_secrets
# The local dataset folders moved into data/datasets/; an old folder that appears again would split the data (failure mode 227).
"$python_bin" -c 'import json, pathlib, sys; old = [p for p in json.load(open("config/data_paths.json"))["moved"] if pathlib.Path(p).exists()]; sys.exit("old dataset folders exist again: " + ", ".join(old) if old else 0)'
# Repository checks over every file Git tracks or would track; the same checks run as pre-commit hooks.
"$python_bin" -m scripts.quality.repo_checks lint-settings
"$python_bin" -m scripts.quality.repo_checks env-example
"$python_bin" -m scripts.quality.repo_checks privacy-scan
"$python_bin" -m scripts.quality.repo_checks writing-check
git ls-files -z --cached --others --exclude-standard | xargs -0 "$python_bin" -m scripts.quality.repo_checks credential-files
git ls-files -z --cached --others --exclude-standard | xargs -0 "$python_bin" -m scripts.quality.repo_checks data-files
# pre-commit's types: [markdown] is the extensions md and markdown in any letter case; the hook excludes data/.
git ls-files -z --cached --others --exclude-standard -- ':(icase)*.md' ':(icase)*.markdown' ':(exclude)data' \
  | xargs -0 "$repo_root/.tools/markdownlint-cli2/node_modules/.bin/markdownlint-cli2" --config .markdownlint-cli2.jsonc
if [[ "${GITHUB_ACTIONS:-false}" == "true" ]]; then
  "$python_bin" -m scripts.quality.documentation_review check-untracked
else
  "$python_bin" -m scripts.quality.documentation_review check
fi
record gates $((SECONDS - started))

launcher_e2e() {
  "$python_bin" -m scripts.lakehouse.run_launcher_e2e ${LAUNCHER_SHARD:+--shard "$LAUNCHER_SHARD"}
}

if [[ "$part" != "core" ]] && is_selected launcher; then
  started=$SECONDS
  launcher_e2e
  record "launcher${LAUNCHER_SHARD:+ shard $LAUNCHER_SHARD}" $((SECONDS - started))
fi
if [[ "$part" == "launcher" ]]; then
  printf '\nThe launcher part passed.\n'
  ci_finished=1
  exit 0
fi

e2e_output="${E2E_ARTIFACT_DIR:-$repo_root/data/e2e/configuration/$(date -u +%Y%m%dT%H%M%SZ)_$$}"
quality_output="${QUALITY_ARTIFACT_DIR:-$repo_root/data/e2e/quality/$(date -u +%Y%m%dT%H%M%SZ)_$$}"

python_lint() {
  "$python_bin" -m ruff format --check scripts tests
  "$python_bin" -m ruff check scripts tests
  "$python_bin" -m mypy --check-untyped-defs --disallow-untyped-defs scripts/process.py scripts/infrastructure scripts/quality tests/test_project_config.py tests/support.py
}

lakehouse_checks() {
  # Reference seeds of the gold control bridge, the formula recipes and the repeatable negative cases of their check.
  "$python_bin" -m scripts.lakehouse.bridge_references --check
  "$python_bin" -m scripts.lakehouse.run_bridge_references_e2e
  # The gold Great Expectations suites must match the gold contract they are generated from.
  "$python_bin" -m scripts.lakehouse.gold_checks check-suites
  # The silver suites must match the kept-model contract, the moved checks and the baselines they are generated from.
  "$python_bin" -m scripts.lakehouse.run_silver_e2e --check-suites
  "$python_bin" -m scripts.lakehouse.run_resource_checks
  "$python_bin" -m scripts.lakehouse.run_lock_e2e
  "$python_bin" -m scripts.orchestration.run_contracts_e2e
}

orchestration_checks() {
  "$python_bin" -m scripts.orchestration.run_stack_e2e
  "$python_bin" -m scripts.orchestration.run_broker_e2e
  "$python_bin" -m scripts.orchestration.run_supervisor_e2e
  # The offline checks skip the built Airflow image, which a CI runner does not have.
  "$python_bin" -m scripts.orchestration.offline_checks --no-image
  "$python_bin" -m scripts.orchestration.run_failing_first --check
}

repository_code_checks() {
  git ls-files -z --cached --others --exclude-standard -- '*.py' | xargs -0 "$python_bin" -m scripts.quality.repo_checks suppressions
  git ls-files -z --cached --others --exclude-standard -- '*.py' | xargs -0 "$python_bin" -m scripts.quality.repo_checks subprocess-imports
  # Only orchestration/launcher.py may call Docker (D1 section 2 item 4).
  git ls-files -z --cached --others --exclude-standard -- '*.py' '*.sh' | xargs -0 "$python_bin" -m scripts.quality.docker_calls
  "$python_bin" -m scripts.quality.run_checks_e2e
  "$python_bin" -m scripts.quality.run_documentation_review_e2e
  "$python_bin" -m scripts.quality.run_ci_groups_e2e --selection-only
}

configuration_checks() {
  "$python_bin" -m scripts.infrastructure.run_e2e --output-directory "$e2e_output"
  "$python_bin" -m scripts.quality.run_e2e --output-directory "$quality_output/e2e"
  "$python_bin" -m pytest --cov=scripts.infrastructure --cov-report=term-missing -q
}


sql_style() {
  .tools/sqlfluff/bin/sqlfluff lint dbt/models dbt/tests
}

terraform_checks() {
  terraform fmt -check -recursive infra
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
  # The two stacks run one after the other: Terraform's provider cache is not safe for concurrent init.
  for stack in storage iam; do
    source_dir="$repo_root/infra"
    if [[ "$stack" == "iam" ]]; then
      source_dir="$repo_root/infra/iam"
    fi
    test_dir="$scratch/$stack"
    mkdir -p "$test_dir"
    cp "$source_dir"/*.tf "$source_dir/.terraform.lock.hcl" "$test_dir/"
    cp -R "$source_dir/tests" "$test_dir/tests"
    if [[ "$stack" == "iam" ]]; then
      cp -R "$source_dir/policies" "$test_dir/policies"
    fi
    printf '\nChecking %s Terraform configuration\n' "$stack"
    terraform -chdir="$test_dir" init -backend=false -input=false -lockfile=readonly -no-color
    terraform -chdir="$test_dir" validate -no-color
    terraform -chdir="$test_dir" test -no-color
  done
  static_failed=0
  for stack in storage iam; do
    source_dir="$repo_root/infra"
    if [[ "$stack" == "iam" ]]; then
      source_dir="$repo_root/infra/iam"
    fi
    if ! "$python_bin" -m scripts.quality.static_checks --directory "$source_dir" --output-directory "$quality_output/$stack"; then
      static_failed=1
    fi
  done
  if [[ "$static_failed" != 0 ]]; then
    printf '\nTerraform static checks failed; review the retained quality evidence.\n' >&2
    return 1
  fi
}

groups=()
for group in python_lint lakehouse_checks orchestration_checks repository_code_checks configuration_checks sql_style terraform_checks; do
  if is_selected "$group"; then
    groups+=("$group")
  fi
done
pids=()
for group in ${groups[@]+"${groups[@]}"}; do
  (
    set -euo pipefail
    group_started=$SECONDS
    "$group"
    printf '%s\n' $((SECONDS - group_started)) >"$scratch/$group.seconds"
  ) >"$scratch/$group.log" 2>&1 &
  pids+=("$!")
done
failed=()
for index in ${groups[@]+"${!groups[@]}"}; do
  if ! wait "${pids[$index]}"; then
    failed+=("${groups[$index]}")
  fi
done
for group in ${groups[@]+"${groups[@]}"}; do
  printf '\n== %s ==\n' "$group"
  cat "$scratch/$group.log"
  record "$group" "$(cat "$scratch/$group.seconds" 2>/dev/null || printf 'failed')"
done
if ((${#failed[@]})); then
  printf '\nLocal CI failed in: %s\n' "${failed[*]}" >&2
  exit 1
fi

if [[ "$part" == "all" ]] && is_selected acquisition; then
  started=$SECONDS
  bash scripts/acquisition/run_checks.sh
  record acquisition $((SECONDS - started))
fi

printf '\nAll local CI checks passed without AWS credentials.\n'
ci_finished=1
