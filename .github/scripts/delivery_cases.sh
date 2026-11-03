#!/usr/bin/env bash
# Cases for the image push and model API deploy workflows: the build arguments that honor the registry's dockerfile field, and the deployment check that
# refuses a service that ECS rolled back. Synthetic service documents only; nothing calls AWS or Docker. Run from the repository root.
set -euo pipefail

failures=0
build_args=.github/scripts/image_build_args.sh
deployment_check=.github/scripts/deployment_check.sh
images=infra/services/images.json
push=.github/workflows/push_image.yml
deploy=.github/workflows/deploy_model_api.yml
scratch="$(mktemp -d)"
trap 'rm -r "$scratch"' EXIT

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

# --- image build arguments -------------------------------------------------------------------------------------------------------------
cat > "$scratch/images.json" << 'JSON'
{"images": {
  "rooted": {"kind": "build", "context": ".", "dockerfile": "services/rooted/Dockerfile", "tag": "1"},
  "nested": {"kind": "build", "context": "services/nested", "dockerfile": "Dockerfile.arm", "tag": "1"},
  "plain": {"kind": "build", "context": "services/plain", "tag": "1"},
  "shared": {"kind": "build", "context": "services/shared", "build_contexts": {"hai": "services/hai", "dvc": "scripts/dvc"}, "tag": "1"},
  "mirrored": {"kind": "mirror", "tag": "1"}
}}
JSON

args_of() { bash "$build_args" "$scratch/images.json" "$1"; }

script_exists() { [[ -f "$build_args" ]]; }
rooted_names_its_dockerfile() { [[ "$(args_of rooted)" == $'--file=services/rooted/Dockerfile\n.' ]]; }
nested_joins_context_and_dockerfile() { [[ "$(args_of nested)" == $'--file=services/nested/Dockerfile.arm\nservices/nested' ]]; }
plain_has_no_file_option() { [[ "$(args_of plain)" == "services/plain" ]]; }
shared_lists_build_contexts() { [[ "$(args_of shared)" == $'--build-context=dvc=scripts/dvc\n--build-context=hai=services/hai\nservices/shared' ]]; }
refuses_a_mirror_and_an_unknown_image() { [[ -f "$build_args" ]] && ! args_of mirrored > /dev/null 2>&1 && ! args_of absent > /dev/null 2>&1; }
real_dockerfiles_exist() {
  local name file found=0
  while IFS= read -r name; do
    file="$(args_of_real "$name" | grep '^--file=' | cut -d= -f2-)" || return 1
    [[ -f "$file" ]] || return 1
    found=$((found + 1))
  done < <(jq -r '.images | to_entries[] | select(.value.kind == "build" and .value.dockerfile != null) | .key' "$images")
  [[ $found -ge 2 ]]
}
args_of_real() { bash "$build_args" "$images" "$1"; }
model_api_and_streamlit_name_their_dockerfiles() {
  [[ "$(args_of_real model_api | grep -c '^--file=services/model_api/Dockerfile$')" -eq 1 && "$(args_of_real streamlit | grep -c '^--file=services/streamlit/Dockerfile$')" -eq 1 ]]
}
push_workflow_uses_the_script() {
  local old_form="--push \"\$context\""
  grep -q 'bash .github/scripts/image_build_args.sh' "$push" && ! grep -qF -- "$old_form" "$push"
}

check "the build arguments script exists" script_exists
check "a build with the repository root as context passes its Dockerfile" rooted_names_its_dockerfile
check "a Dockerfile of a nested context is joined to the context" nested_joins_context_and_dockerfile
check "a build without a dockerfile field has no file option" plain_has_no_file_option
check "named build contexts come before the context, in key order" shared_lists_build_contexts
check "a mirror and an unknown image are refused" refuses_a_mirror_and_an_unknown_image
check "every dockerfile field of the registry names a file that exists" real_dockerfiles_exist
check "model_api and streamlit build from their own Dockerfiles" model_api_and_streamlit_name_their_dockerfiles
check "the push workflow takes its build arguments from the script" push_workflow_uses_the_script

# --- deployment check ------------------------------------------------------------------------------------------------------------------
# The synthetic account number is built from parts, so no file of the repository holds a twelve-digit number.
account="$(printf '%s%s' 1111 11111111)"
new_arn="arn:aws:ecs:us-west-2:${account}:task-definition/example_api:8"
old_arn="arn:aws:ecs:us-west-2:${account}:task-definition/example_api:7"
requested="ecs-svc/1111111111111111111"
rollback_id="ecs-svc/2222222222222222222"

service() { # task definition of the service, desired, running and the deployments array
  jq -n --arg arn "$1" --argjson desired "$2" --argjson running "$3" --argjson deployments "$4" \
    '{services: [{serviceName: "example_api", taskDefinition: $arn, desiredCount: $desired, runningCount: $running, deployments: $deployments}], failures: []}'
}
deployment() { # id, status, rollout state, task definition
  jq -n --arg id "$1" --arg status "$2" --arg state "$3" --arg arn "$4" '{id: $id, status: $status, rolloutState: $state, taskDefinition: $arn}'
}
verdict() { printf '%s' "$1" | bash "$deployment_check" "$new_arn" "$requested" > /dev/null 2>&1; }

completed="$(service "$new_arn" 2 2 "[$(deployment "$requested" PRIMARY COMPLETED "$new_arn")]")"
rolled_back="$(service "$old_arn" 2 2 "[$(deployment "$rollback_id" PRIMARY COMPLETED "$old_arn"),$(deployment "$requested" INACTIVE FAILED "$new_arn")]")"
in_progress="$(service "$new_arn" 2 2 "[$(deployment "$requested" PRIMARY IN_PROGRESS "$new_arn")]")"
no_tasks="$(service "$new_arn" 2 0 "[$(deployment "$requested" PRIMARY COMPLETED "$new_arn")]")"
fewer_tasks="$(service "$new_arn" 2 1 "[$(deployment "$requested" PRIMARY COMPLETED "$new_arn")]")"
superseded="$(service "$new_arn" 2 2 "[$(deployment "$rollback_id" PRIMARY COMPLETED "$new_arn")]")"
other_revision="$(service "$old_arn" 2 2 "[$(deployment "$requested" PRIMARY COMPLETED "$old_arn")]")"
no_service='{"services": [], "failures": [{"reason": "MISSING"}]}'

check_script_exists() { [[ -f "$deployment_check" ]]; }
accepts_the_completed_rollout() { verdict "$completed"; }
refuses_a_rolled_back_service_that_still_runs_tasks() { [[ -f "$deployment_check" ]] && ! verdict "$rolled_back"; }
refuses_a_rollout_still_in_progress() { [[ -f "$deployment_check" ]] && ! verdict "$in_progress"; }
refuses_a_service_without_running_tasks() { [[ -f "$deployment_check" ]] && ! verdict "$no_tasks"; }
refuses_fewer_running_than_desired() { [[ -f "$deployment_check" ]] && ! verdict "$fewer_tasks"; }
refuses_a_superseded_deployment() { [[ -f "$deployment_check" ]] && ! verdict "$superseded"; }
refuses_another_task_definition() { [[ -f "$deployment_check" ]] && ! verdict "$other_revision"; }
refuses_a_missing_service() { [[ -f "$deployment_check" ]] && ! verdict "$no_service"; }
refuses_empty_input() { [[ -f "$deployment_check" ]] && ! verdict ""; }
refuses_a_malformed_deployment_id() { [[ -f "$deployment_check" ]] && ! printf '%s' "$completed" | bash "$deployment_check" "$new_arn" "None" > /dev/null 2>&1; }
deploy_workflow_verifies_the_rollout() {
  grep -q 'bash .github/scripts/deployment_check.sh' "$deploy" && grep -q 'describe-task-definition' "$deploy" && grep -q 'actions/checkout' "$deploy"
}

check "the deployment check script exists" check_script_exists
check "a completed rollout of the requested deployment passes" accepts_the_completed_rollout
check "a service that ECS rolled back to the old revision is refused although tasks run" refuses_a_rolled_back_service_that_still_runs_tasks
check "a rollout that is still in progress is refused" refuses_a_rollout_still_in_progress
check "a service without running tasks is refused" refuses_a_service_without_running_tasks
check "fewer running tasks than desired are refused" refuses_fewer_running_than_desired
check "a deployment that another one superseded is refused" refuses_a_superseded_deployment
check "a service on another task definition is refused" refuses_another_task_definition
check "a missing service is refused" refuses_a_missing_service
check "empty input is refused" refuses_empty_input
check "a deployment id that is not an ECS one is refused" refuses_a_malformed_deployment_id
check "the deploy workflow resolves the revision, then verifies the rollout with the script" deploy_workflow_verifies_the_rollout

[[ $failures -eq 0 ]]
