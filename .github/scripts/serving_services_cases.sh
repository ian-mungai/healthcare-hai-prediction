#!/usr/bin/env bash
# Cases for the configuration of the serving services in the services stack (owner decision D18). GitHub registers new task definition revisions of the model API and
# the dashboard and rolls the services to them, so an owner apply must neither roll a release back nor destroy a service when the two services moved to their own
# resource. The stack's plan tests check the planned values. This reads the text of main.tf for what a plan test cannot see (moved blocks and lifecycle), and
# scripts/infrastructure/run_services_move_e2e.py proves the move with a real plan. Run from the repository root.
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
  [[ -n "$serving" ]] && grep -qx '    ignore_changes = \[task_definition\]' <<< "$serving" && [[ "$(grep -c 'ignore_changes' <<< "$serving")" -eq 1 ]]
}
check "the serving services ignore changes to their task definition, and only to it" only_serving_ignores_the_task_definition
check "the shared services do not ignore changes: an owner apply still rolls them to Terraform's revision" test -n "$shared" -a -z "$(grep 'ignore_changes\|lifecycle' <<< "$shared" || true)"

serving_set_is_one_local() {
  grep -q 'if contains(local.serving_keys, key) }' <<< "$serving" && grep -q 'if !contains(local.serving_keys, key) }' <<< "$shared"
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

[[ $failures -eq 0 ]]
