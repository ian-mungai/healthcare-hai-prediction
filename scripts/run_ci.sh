#!/usr/bin/env bash
set -euo pipefail

# CI_PART selects what runs: "all" (the default, every part), "core" (everything except the launcher E2E, the
# acquisition gate and the three lakehouse layers), "launcher" (the gates and the launcher E2E, one LAUNCHER_SHARD=K/N of it
# if set), "bronze", "silver" or "gold" (the gates and that lakehouse layer's checks) or "gates" (the gates only). GitHub CI runs
# "core", the launcher shards and the three layers as parallel jobs; "gates" is the quick part of a reused pull request result.
# CI_UNITS selects the units of the gold layer (checks, writer_setup, model_metadata_e2e; default all three). GitHub CI runs the model
# metadata E2E in its own job. A unit marked alone below (writer_setup: it has timing assertions) runs by itself, before the others.
# CI_SCOPE selects the check groups (scripts/quality/ci_groups.py, config/quality/ci_groups.json): "full" (the default,
# every group: run it before every push), "staged" (the groups the staged paths touch: the local_ci_affected commit
# hook) or "range" (the groups RANGE_START..RANGE_END touches: GitHub CI). The privacy, secret, credential, data-file,
# writing, Markdown lint and documentation gates run in every scope and in every part, also when no group is selected.
# CI_LIST_ONLY=1 prints the plan and runs nothing. Each group's seconds and each command's start, end and seconds (scripts/quality/timing.sh)
# are printed and added to the job summary (GITHUB_STEP_SUMMARY).
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"
bash -n scripts/run_ci.sh scripts/infrastructure/*.sh
# ShellCheck runs without -x here and checks timing.sh as its own input.
# shellcheck source=/dev/null
source "$repo_root/scripts/quality/timing.sh"
python_bin="${PYTHON_BIN:-$repo_root/.venv/bin/python}"
part="${CI_PART:-all}"
case "$part" in
  all | core | launcher | bronze | silver | gold | gates) ;;
  *) printf 'CI_PART must be all, core, launcher, bronze, silver, gold or gates, not %s\n' "$part" >&2; exit 2 ;;
esac
gold_units="checks writer_setup model_metadata_e2e"
units="${CI_UNITS:-$gold_units}"
for unit in $units; do
  case " $gold_units " in
    *" $unit "*) ;;
    *) printf 'CI_UNITS must name units of the gold layer (%s), not %s\n' "$gold_units" "$unit" >&2; exit 2 ;;
  esac
done
scope="${CI_SCOPE:-full}"
case "$scope" in
  full | staged | range) ;;
  *) printf 'CI_SCOPE must be full, staged or range, not %s\n' "$scope" >&2; exit 2 ;;
esac
summary="${GITHUB_STEP_SUMMARY:-/dev/null}"
scratch="$(mktemp -d "${TMPDIR:-/tmp}/hai_ci.XXXXXX")"
# Every command's row goes to its own file (the main shell and each parallel unit), so rows never interleave; the exit trap writes them all
# to the job summary in a fixed order, also when a gate or a unit failed.
export CI_TIMING_FILE="$scratch/main.timing"
timing_files=("$CI_TIMING_FILE")
finish() {
  if [[ -s "$scratch/main.timing" ]]; then
    timing_table "${timing_files[@]}" >>"$summary"
  fi
  rm -rf "$scratch"
}
# Only a run that sets ci_finished=1 exits 0: bash 3.2 (macOS /bin/bash) reports status 0 to this trap when an unbound
# variable ends the script, so the trap fails every other exit itself.
trap 'finish; [[ "${ci_finished:-0}" == 1 ]] || exit 1' EXIT

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


# Each gate is one command (or one pipeline in a function), wrapped in timed so its start, end and seconds are printed and listed.
credential_files() { git ls-files -z --cached --others --exclude-standard | xargs -0 "$python_bin" -m scripts.quality.repo_checks credential-files; }
data_files() { git ls-files -z --cached --others --exclude-standard | xargs -0 "$python_bin" -m scripts.quality.repo_checks data-files; }
# pre-commit's types: [markdown] is the extensions md and markdown in any letter case; the hook excludes data/.
markdown_lint() {
  git ls-files -z --cached --others --exclude-standard -- ':(icase)*.md' ':(icase)*.markdown' ':(exclude)data' \
    | xargs -0 "$repo_root/.tools/markdownlint-cli2/node_modules/.bin/markdownlint-cli2" --config .markdownlint-cli2.jsonc
}

started=$SECONDS
timed "gates: secret scan" "$python_bin" -m scripts.quality.scan_secrets
# The local dataset folders moved into data/datasets/; an old folder that appears again would split the data (failure mode 227).
timed "gates: moved dataset folders" "$python_bin" -c 'import json, pathlib, sys; old = [p for p in json.load(open("config/data_paths.json"))["moved"] if pathlib.Path(p).exists()]; sys.exit("old dataset folders exist again: " + ", ".join(old) if old else 0)'
# Repository checks over every file Git tracks or would track; the same checks run as pre-commit hooks.
timed "gates: lint settings" "$python_bin" -m scripts.quality.repo_checks lint-settings
timed "gates: env example" "$python_bin" -m scripts.quality.repo_checks env-example
timed "gates: privacy scan" "$python_bin" -m scripts.quality.repo_checks privacy-scan
timed "gates: writing check" "$python_bin" -m scripts.quality.repo_checks writing-check --block dash --block slash
timed "gates: credential files" credential_files
timed "gates: data files" data_files
timed "gates: markdown lint" markdown_lint
if [[ "${GITHUB_ACTIONS:-false}" == "true" ]]; then
  timed "gates: documentation review" "$python_bin" -m scripts.quality.documentation_review check-untracked
else
  timed "gates: documentation review" "$python_bin" -m scripts.quality.documentation_review check
fi
timed "gates: fast source checks" "$python_bin" -m scripts.quality.lint_sources fast
record gates $((SECONDS - started))

if [[ "$part" == "gates" ]]; then
  printf '\nThe gates part passed.\n'
  ci_finished=1
  exit 0
fi

launcher_e2e() {
  "$python_bin" -m scripts.lakehouse.run_launcher_e2e ${LAUNCHER_SHARD:+--shard "$LAUNCHER_SHARD"}
}

if [[ "$part" != "core" ]] && is_selected launcher; then
  started=$SECONDS
  timed "launcher E2E${LAUNCHER_SHARD:+ shard $LAUNCHER_SHARD}" launcher_e2e
  record "launcher${LAUNCHER_SHARD:+ shard $LAUNCHER_SHARD}" $((SECONDS - started))
fi
if [[ "$part" == "launcher" ]]; then
  printf '\nThe launcher part passed.\n'
  ci_finished=1
  exit 0
fi

e2e_output="${E2E_ARTIFACT_DIR:-$repo_root/data/e2e/configuration/$(date -u +%Y%m%dT%H%M%SZ)_$$}"
policy_output="${POLICY_ARTIFACT_DIR:-$repo_root/data/e2e/policies/$(date -u +%Y%m%dT%H%M%SZ)_$$}"
quality_output="${QUALITY_ARTIFACT_DIR:-$repo_root/data/e2e/quality/$(date -u +%Y%m%dT%H%M%SZ)_$$}"

python_lint() {
  timed "python_lint: typecheck" "$python_bin" -m scripts.quality.typecheck --check-untyped-defs --disallow-untyped-defs
}

lakehouse_bronze() {
  timed "lakehouse_bronze: resource checks" "$python_bin" -m scripts.lakehouse.run_resource_checks
  timed "lakehouse_bronze: run lock E2E" "$python_bin" -m scripts.lakehouse.run_lock_e2e
  timed "lakehouse_bronze: provenance E2E" "$python_bin" -m scripts.lakehouse.run_provenance_e2e
  timed "lakehouse_bronze: storage single copy E2E" "$python_bin" -m scripts.lakehouse.run_storage_single_copy_e2e --report "$quality_output/storage_single_copy.json"
  timed "lakehouse_bronze: contracts E2E" "$python_bin" -m scripts.orchestration.run_contracts_e2e
}

lakehouse_silver() {
  # The silver suites must match the kept-model contract, the moved checks and the baselines they are generated from.
  timed "lakehouse_silver: suite check" "$python_bin" -m scripts.lakehouse.run_silver_e2e --check-suites
}

# The gold layer is three units (CI_UNITS) that share no folder, port or process: GitHub CI runs the model metadata E2E, the longest command, in
# its own job, and locally the units run together except writer_setup, which has timing assertions and runs alone before them.
lakehouse_gold_checks() {
  # Reference seeds of the gold control bridge, the formula recipes and the repeatable negative cases of their check.
  timed "lakehouse_gold: bridge references check" "$python_bin" -m scripts.lakehouse.bridge_references --check
  timed "lakehouse_gold: bridge references E2E" "$python_bin" -m scripts.lakehouse.run_bridge_references_e2e
  timed "lakehouse_gold: identity splits check" "$python_bin" -m scripts.lakehouse.identity_splits --check
  timed "lakehouse_gold: source labels check" "$python_bin" -m scripts.lakehouse.source_labels --check
  timed "lakehouse_gold: identity seeds E2E" "$python_bin" -m scripts.lakehouse.run_identity_seeds_e2e
  # The gold Great Expectations suites must match the gold contract they are generated from.
  timed "lakehouse_gold: suites check" "$python_bin" -m scripts.lakehouse.gold_checks check-suites
  timed "lakehouse_gold: publish contract E2E" "$python_bin" -m scripts.lakehouse.run_gold_publish_e2e --mode contract
  timed "lakehouse_gold: model metadata check" "$python_bin" -m scripts.quality.model_metadata
}

lakehouse_gold_writer_setup() {
  timed "lakehouse_gold: writer set-up E2E" "$python_bin" -m scripts.lakehouse.run_gold_publish_e2e --mode writer_setup
}

lakehouse_gold_model_metadata_e2e() {
  timed "lakehouse_gold: model metadata E2E" "$python_bin" -m scripts.quality.run_model_metadata_e2e --workers "${MODEL_METADATA_WORKERS:-auto}"
}

orchestration_checks() {
  timed "orchestration_checks: lineage E2E" "$python_bin" -m scripts.orchestration.run_lineage_e2e
  timed "orchestration_checks: lineage completion E2E" "$python_bin" -m scripts.orchestration.run_lineage_completion_e2e
  timed "orchestration_checks: lineage amendment E2E" "$python_bin" -m scripts.orchestration.run_lineage_amendment_e2e
  timed "orchestration_checks: publish contracts" "$python_bin" -m scripts.orchestration.run_publish_contracts
  timed "orchestration_checks: light pipeline" "$python_bin" -m scripts.orchestration.run_pipeline_light
  timed "orchestration_checks: stack E2E" "$python_bin" -m scripts.orchestration.run_stack_e2e
  timed "orchestration_checks: broker E2E" "$python_bin" -m scripts.orchestration.run_broker_e2e
  timed "orchestration_checks: supervisor E2E" "$python_bin" -m scripts.orchestration.run_supervisor_e2e
  timed "orchestration_checks: transform broker E2E" "$python_bin" -m scripts.orchestration.run_transform_broker_e2e
  # The offline checks skip the built Airflow image, which a CI runner does not have.
  timed "orchestration_checks: offline checks" "$python_bin" -m scripts.orchestration.offline_checks --no-image
  timed "orchestration_checks: failing first check" "$python_bin" -m scripts.orchestration.run_failing_first --check
}

python_suppressions() { git ls-files -z --cached --others --exclude-standard -- '*.py' | xargs -0 "$python_bin" -m scripts.quality.repo_checks suppressions; }
python_subprocess_imports() { git ls-files -z --cached --others --exclude-standard -- '*.py' | xargs -0 "$python_bin" -m scripts.quality.repo_checks subprocess-imports; }
# Only orchestration/launcher.py may call Docker (D1 section 2 item 4).
process_start_guard() { git ls-files -z --cached --others --exclude-standard -- '*.py' '*.sh' | xargs -0 "$python_bin" -m scripts.quality.docker_calls; }

repository_code_checks() {
  timed "repository_code_checks: suppressions" python_suppressions
  timed "repository_code_checks: subprocess imports" python_subprocess_imports
  timed "repository_code_checks: process start guard" process_start_guard
  timed "repository_code_checks: check samples E2E" "$python_bin" -m scripts.quality.run_checks_e2e
  timed "repository_code_checks: documentation review E2E" "$python_bin" -m scripts.quality.run_documentation_review_e2e
  timed "repository_code_checks: CI groups selection E2E" "$python_bin" -m scripts.quality.run_ci_groups_e2e --selection-only
  timed "repository_code_checks: CI reuse E2E" "$python_bin" -m scripts.quality.run_ci_reuse_e2e
}

configuration_checks() {
  timed "configuration_checks: infrastructure E2E" "$python_bin" -m scripts.infrastructure.run_e2e --output-directory "$e2e_output"
  timed "configuration_checks: policy E2E" "$python_bin" -m scripts.infrastructure.run_policy_e2e --output-directory "$policy_output"
  timed "configuration_checks: quality E2E" "$python_bin" -m scripts.quality.run_e2e --output-directory "$quality_output/e2e"
  timed "configuration_checks: secret store E2E" "$python_bin" -m scripts.infrastructure.run_secret_store_e2e
  timed "configuration_checks: pytest with coverage" "$python_bin" -m pytest --cov=scripts.infrastructure --cov-report=term-missing -q
}

sql_style() {
  timed "sql_style: SQL style" "$python_bin" -m scripts.quality.lint_sources sql
}

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
  for stack in storage iam platform; do
    source_dir="$repo_root/infra"
    if [[ "$stack" != "storage" ]]; then
      source_dir="$repo_root/infra/$stack"
    fi
    test_dir="$scratch/$stack"
    mkdir -p "$test_dir"
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
    printf '\nChecking %s Terraform configuration\n' "$stack"
    timed "terraform_checks: $stack init" terraform -chdir="$test_dir" init -backend=false -input=false -lockfile=readonly -no-color
    timed "terraform_checks: $stack validate" terraform -chdir="$test_dir" validate -no-color
    timed "terraform_checks: $stack test" terraform -chdir="$test_dir" test -no-color
  done
  static_failed=0
  for stack in storage iam platform; do
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

# The work items: one per group, and for the gold layer one per selected unit. An alone item runs by itself, before the others.
alone_items=" lakehouse_gold_writer_setup "
alone=()
shared=()
for group in python_lint lakehouse_bronze lakehouse_silver lakehouse_gold orchestration_checks repository_code_checks configuration_checks sql_style terraform_checks; do
  if ! is_selected "$group"; then
    continue
  fi
  if [[ "$group" != "lakehouse_gold" ]]; then
    shared+=("$group")
    continue
  fi
  for unit in $gold_units; do
    case " $units " in
      *" $unit "*)
        if [[ "$alone_items" == *" lakehouse_gold_$unit "* ]]; then
          alone+=("lakehouse_gold_$unit")
        else
          shared+=("lakehouse_gold_$unit")
        fi
        ;;
    esac
  done
done

failed=()
# run_items <item>...: start the items at the same time, wait for all, then print each log and its seconds in the given order.
run_items() {
  local item index pids=() items=("$@")
  for item in "$@"; do
    (
      set -euo pipefail
      export CI_TIMING_FILE="$scratch/$item.timing"
      item_started=$SECONDS
      "$item"
      printf '%s\n' $((SECONDS - item_started)) >"$scratch/$item.seconds"
    ) >"$scratch/$item.log" 2>&1 &
    pids+=("$!")
    timing_files+=("$scratch/$item.timing")
  done
  for index in ${pids[@]+"${!pids[@]}"}; do
    if ! wait "${pids[$index]}"; then
      failed+=("${items[$index]}")
    fi
  done
  for item in "$@"; do
    printf '\n== %s ==\n' "$item"
    cat "$scratch/$item.log"
    record "$item" "$(cat "$scratch/$item.seconds" 2>/dev/null || printf 'failed')"
  done
}
if ((${#alone[@]})); then
  run_items "${alone[@]}"
fi
if ((${#failed[@]})); then
  printf '\nLocal CI failed in: %s\n' "${failed[*]}" >&2
  exit 1
fi
if ((${#shared[@]})); then
  run_items "${shared[@]}"
fi
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
