#!/usr/bin/env bash
# Cases for the configuration of the serving services in the services stack (owner decision D18). GitHub registers new task definition revisions of the model API and
# the dashboard and rolls the services to them, so an owner apply must neither roll a release back nor destroy a service when the two services moved to their own
# resource. The stack's plan tests check the planned values. This reads the text of main.tf for what a plan test cannot see (moved blocks and lifecycle), and
# scripts/infrastructure/run_services_move_e2e.py proves the move with a real plan, and run_serving_transition_e2e.py the first apply after it. Run from the repository root.
set -euo pipefail

main="${SERVICES_MAIN:-infra/services/main.tf}"
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

# The text of one resource block: from its header to the first line that is a closing brace in column one.
block_of() {
  awk -v header="$1" '$0 == header {on = 1} on {print} on && $0 == "}" {exit}' "$main"
}
shared="$(block_of 'resource "aws_ecs_service" "service" {')"
serving="$(block_of 'resource "aws_ecs_service" "serving" {')"

has_resource() { [[ -n "$2" ]] && grep -q "$1" <<< "$2"; }
check "the shared service resource exists" has_resource 'resource "aws_ecs_service" "service"' "$shared"
check "the serving service resource exists" has_resource 'resource "aws_ecs_service" "serving"' "$serving"

only_serving_ignores_the_task_definition() {
  [[ -n "$serving" ]] && grep -Eqx ' {4}ignore_changes += \[task_definition\]' <<< "$serving" && [[ "$(grep -c 'ignore_changes' <<< "$serving")" -eq 1 ]]
}
check "the serving services ignore changes to their task definition, and only to it" only_serving_ignores_the_task_definition

# A service that exists before this change runs a revision with the shared execution role and the shared Redis password, and the release check refuses the next
# revision rendered from it. So each serving service is replaced once, when the execution role of its task definition changes (the first owner apply), and by nothing
# else: a trigger on the whole task definition would roll every release back, one on another attribute would never fire.
replaced_by_the_execution_role_only() {
  [[ -n "$serving" && "$(grep -c 'replace_triggered_by' <<< "$serving")" -eq 1 ]] &&
    grep -Eqx ' {4}replace_triggered_by += \[aws_ecs_task_definition\.service\[each\.key\]\.execution_role_arn\]' <<< "$serving"
}
check "each serving service is replaced once when the execution role of its task definition changes, and by nothing else" replaced_by_the_execution_role_only
serving_definitions_take_the_serving_role() {
  grep -q 'execution_role_arn *= contains(local.serving_keys, each.key) ? var.serving_execution_role_arn : var.execution_role_arn' "$main"
}
check "the task definitions of the serving services name the serving execution role, which the trigger watches" serving_definitions_take_the_serving_role
check "the shared services do not ignore changes: an owner apply still rolls them to Terraform's revision" test -n "$shared" -a -z "$(grep 'ignore_changes\|lifecycle' <<< "$shared" || true)"

# Whole for_each lines, so a changed operator or key expression fails: the shared resource leaves out the catalog and the serving services, the serving resource takes exactly the serving services.
serving_set_is_one_local() {
  grep -Fqx '  for_each = { for key, service in local.services : key => service if contains(local.serving_keys, key) }' <<< "$serving" &&
    grep -Fqx '  for_each = { for key, service in local.services : key => service if key != "openmetadata" && !contains(local.serving_keys, key) }' <<< "$shared"
}
check "the two resources split the services by the same local list" serving_set_is_one_local

# The pairs of the moved blocks, read line by line.
moved_pairs() {
  awk '/^moved \{/ {on = 1; from = ""; to = ""} on && /from =/ {from = $3} on && /to   =/ {to = $3} on && /^\}/ {print from " " to; on = 0}' "$main"
}
moved_blocks_are_exactly_the_two_serving_services() {
  [[ "$(moved_pairs | sort | tr '\n' ';')" == 'aws_ecs_service.service["model_api"] aws_ecs_service.serving["model_api"];aws_ecs_service.service["streamlit"] aws_ecs_service.serving["streamlit"];' ]]
}
check "a moved block carries each serving service from the shared resource to the serving one, and no other block moves" moved_blocks_are_exactly_the_two_serving_services

# The serving resource must keep every argument the service had, or the move replaces or changes it.
arguments_of() { grep -E '^  [a-z_]+ += ' <<< "$1" | sed -E 's/^  ([a-z_]+) .*/\1/' | sort | tr '\n' ' '; }
same_arguments() { [[ -n "$shared" && -n "$serving" && "$(arguments_of "$shared")" == "$(arguments_of "$serving")" ]]; }
check "the serving resource sets the same arguments as the shared one" same_arguments
same_values() {
  local name
  for name in name cluster desired_count deployment_minimum_healthy_percent deployment_maximum_percent enable_ecs_managed_tags propagate_tags tags; do
    [[ -n "$shared" && "$(grep -E "^  $name += " <<< "$shared")" == "$(grep -E "^  $name += " <<< "$serving")" ]] || return 1
  done
}
check "the serving resource keeps the name, cluster, count, tag and deployment settings of the shared one" same_values
same_blocks() {
  [[ -n "$shared" && "$(grep -A3 'capacity_provider_strategy\|deployment_circuit_breaker' <<< "$shared")" == "$(grep -A3 'capacity_provider_strategy\|deployment_circuit_breaker' <<< "$serving")" ]]
}
check "the serving resource keeps the capacity provider strategy and the circuit breaker" same_blocks

# --- the proofs are in a gate ------------------------------------------------------------------------------------------------------------------------
# A test that no gate runs protects nothing. The terraform_checks group runs the move proof, the release check on the real definitions and the Redis cases, and the
# group is selected by the paths of the files they depend on.
group=scripts/ci/groups/terraform_checks.sh
selectors=config/quality/ci_groups.json
runs_once() { [[ "$(grep -c -- "$1" "$group")" -eq 1 ]]; }
check "the terraform_checks group runs the services move proof" runs_once 'scripts.infrastructure.run_services_move_e2e --output-directory'
check "the terraform_checks group runs the release check on the real definitions" runs_once 'scripts.infrastructure.run_serving_release_real_e2e --output-directory'
check "the terraform_checks group runs the serving transition proof" runs_once 'scripts.infrastructure.run_serving_transition_e2e --output-directory'
check "the terraform_checks group runs the Redis ACL cases" runs_once 'bash .github/scripts/redis_acl_cases.sh'
selected() { jq -e --arg path "$1" '.groups.terraform_checks | any(. as $pattern | $path | test($pattern))' "$selectors" > /dev/null 2>&1; }
check "a change of the move proof runner selects the group" selected scripts/infrastructure/run_services_move_e2e.py
check "a change of the real definitions runner selects the group" selected scripts/infrastructure/run_serving_release_real_e2e.py
check "a change of the transition proof runner selects the group" selected scripts/infrastructure/run_serving_transition_e2e.py
check "a change of the serving release configuration selects the group" selected config/aws/serving_release.json
check "a change of the Redis ACL cases selects the group" selected .github/scripts/redis_acl_cases.sh
check "an unrelated runner does not select the group" test "$(selected scripts/infrastructure/run_policy_e2e.py && echo selected || echo not)" = not

[[ $failures -eq 0 ]]
