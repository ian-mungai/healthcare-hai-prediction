#!/usr/bin/env bash
# Cases for serving_release.sh (owner decision D18): GitHub registers new serving task definition revisions itself, so the script builds the revision from
# the running one with a new image, and a check refuses any definition that names a role, a secret or an image repository the serving tasks may not use.
# The definitions are synthetic and nothing calls AWS. Run from the repository root.
set -euo pipefail

script=".github/scripts/serving_release.sh"
config="config/aws/serving_release.json"
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

# The synthetic account number is built from parts, so no file of the repository holds a twelve-digit number.
account="$(printf '%s%s' 1111 11111111)"
export PROJECT_NAME="example_project" ENVIRONMENT="dev" AWS_ACCOUNT_ID="$account" AWS_REGION="us-west-2"
other_account="$(printf '%s%s' 2222 22222222)"
registry="$account.dkr.ecr.us-west-2.amazonaws.com"
role() { printf 'arn:aws:iam::%s:role/%s' "$account" "$1"; }
parameter() { printf 'arn:aws:ssm:us-west-2:%s:parameter%s' "$account" "$1"; }
digest="sha256:$(printf 'a%.0s' {1..64})"
other_digest="sha256:$(printf 'b%.0s' {1..64})"

# A running revision as "aws ecs describe-task-definition --query taskDefinition" prints it, with the read-only fields.
definition() { # service
  case "$1" in
    model_api)
      jq -n --arg role "$(role example_project_model_api_task_dev)" --arg execution "$(role example_project_serving_execution_dev)" --arg image "$registry/example_project_model_api_dev@$digest" \
        --arg hashes "$(parameter /healthcare_hai_prediction/model_api_key_hashes)" --arg user "$(parameter /healthcare_hai_prediction/model_api_mlflow_user)" \
        '{taskDefinitionArn: "arn:aws:ecs:us-west-2:x:task-definition/example_project_dev_model_api:3", revision: 3, status: "ACTIVE", registeredAt: "2026-11-01T00:00:00Z", registeredBy: "x",
          requiresAttributes: [{name: "x"}], compatibilities: ["EC2"], family: "example_project_dev_model_api", taskRoleArn: $role, executionRoleArn: $execution,
          networkMode: "host", requiresCompatibilities: ["EC2"], volumes: [],
          containerDefinitions: [{name: "model_api", image: $image, memory: 1280, secrets: [{name: "MODEL_API_KEY_HASHES", valueFrom: $hashes}, {name: "MLFLOW_TRACKING_USERNAME", valueFrom: $user}],
            logConfiguration: {logDriver: "awslogs", options: {"awslogs-group": "example_project_model_api_dev"}}}]}'
      ;;
    streamlit)
      jq -n --arg role "$(role example_project_streamlit_task_dev)" --arg execution "$(role example_project_serving_execution_dev)" --arg image "$registry/example_project_streamlit_dev@$digest" \
        --arg key "$(parameter /healthcare_hai_prediction/streamlit_model_api_key)" \
        '{taskDefinitionArn: "arn:aws:ecs:us-west-2:x:task-definition/example_project_dev_streamlit:2", revision: 2, status: "ACTIVE", family: "example_project_dev_streamlit",
          taskRoleArn: $role, executionRoleArn: $execution, networkMode: "host", requiresCompatibilities: ["EC2"], volumes: [],
          containerDefinitions: [{name: "streamlit", image: $image, memory: 512, secrets: [{name: "MODEL_API_KEY", valueFrom: $key}]}]}'
      ;;
  esac
}

verdict() { # service, definition JSON on the second argument; true when the check accepts
  printf '%s' "$2" | bash "$script" check "$1" > /dev/null 2>&1
}
refusal() { # service, definition JSON; true when the check exits 1 (its own refusal, not a missing script)
  local code=0
  printf '%s' "$2" | bash "$script" check "$1" > /dev/null 2>&1 || code=$?
  [[ "$code" -eq 1 ]]
}
vary() { # service, jq filter, optional value that the filter reads as env.V (the quotes stay out of the shell strings: macOS bash 3.2 mangles braces inside them)
  definition "$1" | V="${3:-}" jq -c "$2"
}

check "the script and its configuration exist" test -f "$script" -a -f "$config"

# --- check: what the running definitions look like passes ------------------------------------------------------------------------------------
good_api="$(definition model_api)"
good_page="$(definition streamlit)"
check "the model API definition passes" verdict model_api "$good_api"
check "the dashboard definition passes" verdict streamlit "$good_page"
check "a definition wrapped as describe-task-definition prints it passes" verdict model_api "$(jq -c '{taskDefinition: .}' <<< "$good_api")"

# --- check: roles ----------------------------------------------------------------------------------------------------------------------------
for name in example_project_task_execution_dev example_project_polaris_task_dev example_project_control_plane_task_dev example_project_ecs_host_dev \
  example_project_job_execution_dev example_project_gh_deploy_dev example_project_streamlit_task_dev example_project_model_api_task_prod other_project_model_api_task_dev administrator; do
  check "the task role $name is refused for the model API" refusal model_api "$(vary model_api '.taskRoleArn = env.V' "$(role "$name")")"
done
check "the model API task role is refused for the dashboard" refusal streamlit "$(vary streamlit '.taskRoleArn = env.V' "$(role example_project_model_api_task_dev)")"
for name in example_project_task_execution_dev example_project_job_execution_dev example_project_polaris_task_dev example_project_serving_execution_prod administrator; do
  check "the execution role $name is refused" refusal model_api "$(vary model_api '.executionRoleArn = env.V' "$(role "$name")")"
done
check "a task role in another account is refused" refusal model_api "$(vary model_api '.taskRoleArn = env.V' "arn:aws:iam::$other_account:role/example_project_model_api_task_dev")"
check "a role path is refused" refusal model_api "$(vary model_api '.taskRoleArn = env.V' "arn:aws:iam::$account:role/team/example_project_model_api_task_dev")"
check "a missing task role is refused" refusal model_api "$(vary model_api 'del(.taskRoleArn)')"
check "a missing execution role is refused" refusal model_api "$(vary model_api 'del(.executionRoleArn)')"

# --- check: image repositories ---------------------------------------------------------------------------------------------------------------
for image in "$registry/example_project_airflow_dev@$digest" "$registry/example_project_model_api_prod@$digest" "$registry/other_project_model_api_dev@$digest" \
  "$registry/example_project_model_api_dev_extra@$digest" "$registry/example_project_streamlit_dev@$digest" "$other_account.dkr.ecr.us-west-2.amazonaws.com/example_project_model_api_dev@$digest" \
  "$account.dkr.ecr.us-east-1.amazonaws.com/example_project_model_api_dev@$digest" "docker.io/library/python@$digest" "public.ecr.aws/x/example_project_model_api_dev@$digest" \
  "$registry/example_project_model_api_dev" "$registry/example_project_model_api_dev:latest" "$registry/example_project_model_api_dev@sha256:abc" "example_project_model_api_dev@$digest"; do
  check "the image $image is refused for the model API" refusal model_api "$(vary model_api '.containerDefinitions[0].image = env.V' "$image")"
done
check "an image tag of the right repository is refused, because a revision pins a digest" refusal model_api "$(vary model_api '.containerDefinitions[0].image = env.V' "$registry/example_project_model_api_dev:1-arm64")"
check "a sidecar container with a foreign image is refused" refusal model_api "$(vary model_api '.containerDefinitions += [{name: "sidecar", image: env.V}]' "docker.io/x/y@$digest")"
check "a sidecar container of the right repository passes" verdict model_api "$(vary model_api '.containerDefinitions += [{name: "sidecar", image: env.V}]' "$registry/example_project_model_api_dev@$other_digest")"
check "a definition without containers is refused" refusal model_api "$(vary model_api '.containerDefinitions = []')"
check "a container without an image is refused" refusal model_api "$(vary model_api 'del(.containerDefinitions[0].image)')"

# --- check: secrets --------------------------------------------------------------------------------------------------------------------------
for name in /healthcare_hai_prediction/redis_password /healthcare_hai_prediction/postgres_password /healthcare_hai_prediction/polaris_root_secret \
  /healthcare_hai_prediction/airflow_fernet_key /healthcare_hai_prediction/streamlit_model_api_key /healthcare_hai_prediction/model_api_key_hashes_extra \
  /other_project/model_api_key_hashes /healthcare_hai_prediction/*; do
  check "the parameter $name is refused for the model API" refusal model_api "$(vary model_api '.containerDefinitions[0].secrets += [{name: "X", valueFrom: env.V}]' "$(parameter "$name")")"
done
check "the Redis password of the model API's own read-only user passes" verdict model_api "$(vary model_api '.containerDefinitions[0].secrets += [{name: "REDIS_PASSWORD", valueFrom: env.V}]' "$(parameter /healthcare_hai_prediction/model_api_redis_password)")"
check "the Redis password of the model API is refused for the dashboard" refusal streamlit "$(vary streamlit '.containerDefinitions[0].secrets += [{name: "X", valueFrom: env.V}]' "$(parameter /healthcare_hai_prediction/model_api_redis_password)")"
check "a parameter of the model API is refused for the dashboard" refusal streamlit "$(vary streamlit '.containerDefinitions[0].secrets += [{name: "X", valueFrom: env.V}]' "$(parameter /healthcare_hai_prediction/model_api_key_hashes)")"
check "a parameter in another region is refused" refusal model_api "$(vary model_api '.containerDefinitions[0].secrets[0].valueFrom |= sub("us-west-2"; "us-east-1")')"
check "a parameter in another account is refused" refusal model_api "$(vary model_api '.containerDefinitions[0].secrets[0].valueFrom |= sub("1111"; "2222")')"
check "a Secrets Manager secret is refused" refusal model_api "$(vary model_api '.containerDefinitions[0].secrets += [{name: "X", valueFrom: env.V}]' "arn:aws:secretsmanager:us-west-2:$account:secret:census_api_key-AbCdEf")"
check "a bare parameter name is refused" refusal model_api "$(vary model_api '.containerDefinitions[0].secrets += [{name: "X", valueFrom: "/healthcare_hai_prediction/model_api_key_hashes"}]')"
check "a secret in a log option is refused" refusal model_api "$(vary model_api '.containerDefinitions[0].logConfiguration.secretOptions = [{name: "x", valueFrom: env.V}]' "$(parameter /healthcare_hai_prediction/redis_password)")"
check "a secret of a sidecar container is refused" refusal model_api "$(vary model_api '.containerDefinitions += [{name: "sidecar", image: .containerDefinitions[0].image, secrets: [{name: "X", valueFrom: env.V}]}]' "$(parameter /healthcare_hai_prediction/redis_password)")"
check "registry credentials are refused" refusal model_api "$(vary model_api '.containerDefinitions[0].repositoryCredentials = {credentialsParameter: env.V}' "arn:aws:secretsmanager:us-west-2:$account:secret:x-AbCdEf")"
check "environment files are refused" refusal model_api "$(vary model_api '.containerDefinitions[0].environmentFiles = [{value: "arn:aws:s3:::example-data-bucket/env.txt", type: "s3"}]')"

# --- check: family and input -----------------------------------------------------------------------------------------------------------------
for family in example_project_dev_airflow example_project_prod_model_api other_project_dev_model_api example_project_dev_streamlit example_project_dev_model_api_x; do
  check "the family $family is refused for the model API" refusal model_api "$(vary model_api '.family = env.V' "$family")"
done
check "a service that is not served is refused" refusal airflow "$good_api"
check "a service with a path is refused" refusal ../model_api "$good_api"
check "empty input is refused" refusal model_api ""
check "input that is not JSON is refused" refusal model_api "not json"
check "an account that is not twelve digits is refused" test "$(printf '%s' "$good_api" | AWS_ACCOUNT_ID=123 bash "$script" check model_api > /dev/null 2>&1 && echo 0 || echo $?)" = 1
check "a missing environment variable is refused" test "$(printf '%s' "$good_api" | env -u PROJECT_NAME bash "$script" check model_api > /dev/null 2>&1 && echo 0 || echo $?)" = 1

# --- render ----------------------------------------------------------------------------------------------------------------------------------
rendered_api="$(printf '%s' "$good_api" | bash "$script" render model_api "$other_digest" 2> /dev/null || true)"
check "render sets the new image digest on the named container" test "$(jq -r '.containerDefinitions[0].image' <<< "${rendered_api:-null}" 2> /dev/null)" = "$registry/example_project_model_api_dev@$other_digest"
check "render drops the read-only fields" test -n "$rendered_api" -a "$(jq -c '[has("taskDefinitionArn"), has("revision"), has("status"), has("registeredAt"), has("registeredBy"), has("requiresAttributes"), has("compatibilities")] | any' <<< "${rendered_api:-null}" 2> /dev/null)" = "false"
check "render keeps the family and the task role" test "$(jq -r '[.family, .taskRoleArn] | join(",")' <<< "${rendered_api:-null}" 2> /dev/null)" = "example_project_dev_model_api,$(role example_project_model_api_task_dev)"
check "render sets the serving execution role" test "$(jq -r '.executionRoleArn' <<< "${rendered_api:-null}" 2> /dev/null)" = "$(role example_project_serving_execution_dev)"
check "render keeps the container settings" test "$(jq -c '.containerDefinitions[0] | del(.image) == ($ARGS.named.good.containerDefinitions[0] | del(.image))' --argjson good "$good_api" <<< "${rendered_api:-null}" 2> /dev/null)" = "true"
check "render tags the revision with the project, which the deploy role's registration requires" test "$(jq -c '.tags' <<< "${rendered_api:-null}" 2> /dev/null)" = '[{"key":"Project","value":"example_project"}]'
check "what render prints passes the check" verdict model_api "${rendered_api:-null}"
check "render replaces the shared execution role of the running revision" test "$(printf '%s' "$(vary model_api '.executionRoleArn = env.V' "$(role example_project_task_execution_dev)")" | bash "$script" render model_api "$digest" 2> /dev/null | jq -r '.executionRoleArn' 2> /dev/null)" = "$(role example_project_serving_execution_dev)"
check "render prints the dashboard revision" test "$(printf '%s' "$good_page" | bash "$script" render streamlit "$other_digest" 2> /dev/null | jq -r '.containerDefinitions[0].image' 2> /dev/null)" = "$registry/example_project_streamlit_dev@$other_digest"
sidecar_image="$registry/example_project_model_api_dev@$other_digest"
with_sidecar="$(vary model_api '.containerDefinitions += [{name: "sidecar", image: env.V}]' "$sidecar_image")"
check "render changes the image of the named container only" test "$(printf '%s' "$with_sidecar" | bash "$script" render model_api "$digest" 2> /dev/null | jq -r '[.containerDefinitions[] | .image] | join(",")' 2> /dev/null)" = "$registry/example_project_model_api_dev@$digest,$sidecar_image"
render_refusal() { local code=0; printf '%s' "$good_api" | bash "$script" render "$@" > /dev/null 2>&1 || code=$?; [[ "$code" -eq 1 ]]; }
check "render refuses a digest that is not sha256 and 64 hex digits" render_refusal model_api "sha256:abc"
check "render refuses an image reference in place of a digest" render_refusal model_api "$registry/other@$digest"
check "render refuses a tag" render_refusal model_api "latest"
check "render refuses a service that is not served" render_refusal airflow "$digest"
check "render refuses a definition whose container is not named for the service" test "$(printf '%s' "$(vary model_api '.containerDefinitions[0].name = "other"')" | bash "$script" render model_api "$digest" > /dev/null 2>&1 && echo 0 || echo $?)" = 1
check "render keeps a foreign secret so that the check can refuse it" test "$(printf '%s' "$(vary model_api '.containerDefinitions[0].secrets += [{name: "X", valueFrom: env.V}]' "$(parameter /healthcare_hai_prediction/redis_password)")" | bash "$script" render model_api "$digest" 2> /dev/null | bash "$script" check model_api > /dev/null 2>&1 && echo 0 || echo $?)" = 1

# --- the configuration ------------------------------------------------------------------------------------------------------------------------
check "the configuration names exactly the two serving services" test "$(jq -r '.services | keys | join(",")' "$config")" = "model_api,streamlit"
check "the configuration names no control-plane parameter" test "$(jq -r '[.services[].parameters[]] | map(select(test("postgres|/redis_password$|airflow|marquez|polaris_root|mlflow_db|mlflow_admin|broker|grafana"))) | length' "$config")" = "0"
variables=infra/iam/variables.tf
default_of() { grep -A6 "^variable \"$1\"" "$variables" | grep -m1 'default' | sed 's/.*= *"\(.*\)"/\1/'; }
all_parameters_match_the_variables() {
  local name expected
  expected="$(for name in model_api_key_hashes_name aws_polaris_serving_client_id_name aws_polaris_serving_client_secret_name model_api_mlflow_user_name model_api_mlflow_credential_name model_api_redis_password_name streamlit_model_api_key_name; do default_of "$name"; done | sort | tr '\n' ',')"
  [[ -n "$expected" && "$expected" == "$(jq -r '[.services[].parameters[]] | sort | join(",")' "$config"),"  ]]
}
check "the configured parameters equal the defaults of the IAM stack variables" all_parameters_match_the_variables

[[ $failures -eq 0 ]]
