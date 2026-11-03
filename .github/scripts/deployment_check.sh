#!/usr/bin/env bash
# Decides from the output of "aws ecs describe-services" (on standard input) whether the requested deployment of a service rolled out. ECS can finish a
# deployment by rolling back to the old revision and still report a stable service with running tasks, so the check reads the deployment itself:
# the service runs the requested task definition, the deployment with the requested ID is the primary one, its rollout is COMPLETED on that task
# definition, and the running task count equals the desired count and is at least one.
# Usage: aws ecs describe-services ... | bash .github/scripts/deployment_check.sh <task-definition-arn> <deployment-id>
set -euo pipefail

arn="${1:?the task definition ARN}"
deployment="${2:?the deployment ID}"
[[ "$deployment" =~ ^ecs-svc/[0-9]+$ ]] || {
  echo "The deployment ID must look like ecs-svc/<digits>." >&2
  exit 1
}
document="$(cat)"
[[ -n "$document" ]] || {
  echo "No service description was given." >&2
  exit 1
}

jq -e --arg arn "$arn" --arg id "$deployment" '
  (.services | length) == 1
  and (.services[0] as $service
    | $service.taskDefinition == $arn
    and ($service.runningCount >= 1)
    and ($service.runningCount == $service.desiredCount)
    and ([$service.deployments[] | select(.id == $id)] as $found
      | ($found | length) == 1
      and $found[0].status == "PRIMARY"
      and $found[0].rolloutState == "COMPLETED"
      and $found[0].taskDefinition == $arn))' <<< "$document" > /dev/null || {
  echo "The requested deployment did not roll out: the service may have rolled back, still be rolling out, be superseded or run fewer tasks than desired." >&2
  jq -r '.services[0] | "service task definition: \(.taskDefinition // "none")\ndesired \(.desiredCount // "?") running \(.runningCount // "?")\n" + ([.deployments[]? | "deployment \(.id) \(.status) \(.rolloutState) \(.taskDefinition)"] | join("\n"))' <<< "$document" >&2 || true
  exit 1
}
