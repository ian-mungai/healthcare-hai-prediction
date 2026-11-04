#!/usr/bin/env bash
# Model releases (owner decision D18): GitHub registers new serving task definition revisions itself. Two modes, both reading a task definition on standard input:
#   render <service> <digest>   Reads the running revision, as "aws ecs describe-task-definition --query taskDefinition" prints it, and prints the registration input
#                               for the next one: the read-only fields dropped, the image of the container named for the service set to the project repository at
#                               <digest>, the execution role set to the dedicated serving execution role and the Project tag that the deploy role's registration requires.
#   check <service>             Exits 1 unless the definition names only what the serving tasks may use: the family of the service, its task role, the serving
#                               execution role, images of its own repository pinned by digest, and secrets from its own list of Parameter Store parameters (config/aws/serving_release.json).
#                               It also refuses registry credentials, environment files and secret log options, which name other stores. It reads the definition
#                               bare or wrapped as {"taskDefinition": ...}, so the same check covers the definition before it is registered and the revision after.
# The service is model_api or streamlit. The names come from PROJECT_NAME, ENVIRONMENT, AWS_ACCOUNT_ID and AWS_REGION. Every refusal exits 1.
set -euo pipefail

config="${SERVING_RELEASE_CONFIG:-config/aws/serving_release.json}"

refuse() {
  echo "$1" >&2
  exit 1
}

mode="${1:-}"
service="${2:-}"
[[ "$mode" == render || "$mode" == check ]] || { echo "Use render or check." >&2; exit 2; }
[[ "$service" =~ ^[a-z_]+$ ]] || refuse "Use a service name."
jq -e --arg service "$service" '.services[$service] != null' "$config" > /dev/null 2>&1 || refuse "The service is not a served one."

project="${PROJECT_NAME:-}"
environment="${ENVIRONMENT:-}"
account="${AWS_ACCOUNT_ID:-}"
region="${AWS_REGION:-}"
[[ "$project" =~ ^[a-z][a-z0-9_]{0,36}$ ]] || refuse "Set PROJECT_NAME."
[[ "$environment" =~ ^[a-z]{2,10}$ ]] || refuse "Set ENVIRONMENT."
[[ "$account" =~ ^[0-9]{12}$ ]] || refuse "Set AWS_ACCOUNT_ID to the twelve digit account."
[[ "$region" =~ ^[a-z]{2}-[a-z]+-[0-9]$ ]] || refuse "Set AWS_REGION."

task_role="$(jq -r --arg service "$service" '.services[$service].task_role' "$config")"
repository="$(jq -r --arg service "$service" '.services[$service].image_repository' "$config")"
execution_role="$(jq -r '.execution_role' "$config")"
role_arn() { printf 'arn:aws:iam::%s:role/%s_%s_%s' "$account" "$project" "$1" "$environment"; }
registry="$account.dkr.ecr.$region.amazonaws.com"
repository_name="${project}_${repository}_${environment}"
parameter_prefix="arn:aws:ssm:$region:$account:parameter"
allowed_parameters="$(jq -c --arg service "$service" --arg prefix "$parameter_prefix" '[.services[$service].parameters[] | $prefix + .]' "$config")"

definition="$(cat)"
[[ -n "$definition" ]] || refuse "No task definition was given."
definition="$(printf '%s' "$definition" | jq -c 'if type == "object" and has("taskDefinition") then .taskDefinition else . end')" || refuse "The task definition is not JSON."

case "$mode" in
  render)
    digest="${3:-}"
    [[ "$digest" =~ ^sha256:[0-9a-f]{64}$ ]] || refuse "Use an image digest, sha256: and 64 hex digits."
    printf '%s' "$definition" | jq -e --arg service "$service" '[.containerDefinitions[]? | select(.name == $service)] | length == 1' > /dev/null || refuse "The definition has no single container named for the service."
    printf '%s' "$definition" | jq --arg service "$service" --arg image "$registry/$repository_name@$digest" --arg execution "$(role_arn "$execution_role")" --arg project "$project" '
      del(.taskDefinitionArn, .revision, .status, .requiresAttributes, .compatibilities, .registeredAt, .registeredBy, .deregisteredAt)
      | .executionRoleArn = $execution
      | .tags = [{key: "Project", value: $project}]
      | .containerDefinitions |= map(if .name == $service then .image = $image else . end)'
    ;;
  check)
    printf '%s' "$definition" | jq -e \
      --arg family "${project}_${environment}_${service}" --arg task_role "$(role_arn "$task_role")" --arg execution_role "$(role_arn "$execution_role")" \
      --arg registry "$registry" --arg repository "$repository_name" --argjson parameters "$allowed_parameters" '
      def image_ok: type == "string" and startswith($registry + "/" + $repository + "@sha256:") and (ltrimstr($registry + "/" + $repository + "@sha256:") | test("^[0-9a-f]{64}$"));
      .family == $family
      and .taskRoleArn == $task_role
      and .executionRoleArn == $execution_role
      and ((.containerDefinitions // []) | length >= 1)
      and all(.containerDefinitions[]; (.image | image_ok)
        and (has("repositoryCredentials") | not)
        and (has("environmentFiles") | not)
        and all((.secrets // [])[]; .valueFrom as $source | $parameters | index($source) != null)
        and all((.logConfiguration.secretOptions // [])[]; .valueFrom as $source | $parameters | index($source) != null))' > /dev/null 2>&1 \
      || refuse "The task definition names a role, a secret or an image repository that the serving tasks may not use."
    echo "The task definition names only the serving roles, secrets and image repository."
    ;;
esac
