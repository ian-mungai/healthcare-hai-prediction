#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$repo_root"
python_bin="${PYTHON_BIN:-$repo_root/.venv/bin/python}"
stack=storage
admin_profile=""
preparation_inputs=""
preparation_sha256=""
identity_probe=false
mlflow_retention=false
athena_retention=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --iam | --platform | --services | --jobs)
      [[ "$stack" == storage ]] || { printf 'Choose one stack: --iam, --platform, --services or --jobs.\n' >&2; exit 2; }
      stack="${1#--}"
      shift
      ;;
    --mlflow-retention)
      [[ "$mlflow_retention" == false ]] || { printf 'Select the MLflow retention rule once.\n' >&2; exit 2; }
      mlflow_retention=true
      shift
      ;;
    --athena-retention)
      [[ "$athena_retention" == false ]] || { printf 'Select the Athena retention rule once.\n' >&2; exit 2; }
      athena_retention=true
      shift
      ;;
    --identity-probe)
      [[ "$identity_probe" == false ]] || { printf 'Select the identity probe once.\n' >&2; exit 2; }
      identity_probe=true
      shift
      ;;
    --preparation-inputs)
      [[ -z "$preparation_inputs" && $# -ge 2 && "$2" != -* ]] || { printf 'Supply one preparation input path.\n' >&2; exit 2; }
      preparation_inputs="$2"
      shift 2
      ;;
    --preparation-sha256)
      [[ -z "$preparation_sha256" && $# -ge 2 && "$2" != -* ]] || { printf 'Supply one preparation SHA-256.\n' >&2; exit 2; }
      preparation_sha256="$2"
      shift 2
      ;;
    --admin-profile) admin_profile="${2:?Supply the approved administrator profile}"; shift 2 ;;
    *) break ;;
  esac
done
if [[ "$identity_probe" == true ]]; then
  [[ "$stack" == jobs ]] || { printf 'The identity probe is allowed only with --jobs.\n' >&2; exit 2; }
fi
if [[ "$mlflow_retention" == true ]]; then
  [[ "$stack" == storage ]] || { printf 'The MLflow retention rule belongs to the storage stack: use it without --iam, --platform, --services or --jobs.\n' >&2; exit 2; }
fi
if [[ "$athena_retention" == true ]]; then
  [[ "$stack" == storage ]] || { printf 'The Athena retention rule belongs to the storage stack: use it without --iam, --platform, --services or --jobs.\n' >&2; exit 2; }
fi
if [[ -n "$preparation_inputs" || -n "$preparation_sha256" ]]; then
  [[ "$stack" == jobs || "$stack" == services || "$stack" == platform ]] || { printf 'Preparation inputs are allowed only with --jobs, --services or --platform.\n' >&2; exit 2; }
fi
if [[ ( -n "$preparation_inputs" && -z "$preparation_sha256" ) || ( -z "$preparation_inputs" && -n "$preparation_sha256" ) ]]; then
  printf 'Supply both --preparation-inputs and --preparation-sha256.\n' >&2
  exit 2
fi
action="${1:?Use plan, apply or validate}"
shift
case "$action" in
  plan|apply|validate) ;;
  *) printf 'Use plan, apply or validate.\n' >&2; exit 2 ;;
esac
for argument in "$@"; do
  case "$argument" in
    -var*|-chdir*) printf 'Set project inputs only in .env.\n' >&2; exit 2 ;;
  esac
done

# Terraform also injects arguments from the environment, including during saved-plan inspection.
for name in "${!TF_CLI_ARGS@}"; do
  case "$name" in
    TF_CLI_ARGS|TF_CLI_ARGS_*)
      if [[ -n "${!name}" ]]; then
        printf 'Unset %s; pass Terraform options explicitly and set project inputs only in .env.\n' "$name" >&2
        exit 2
      fi
      ;;
  esac
done

for name in "${!TF_VAR_@}"; do
  printf 'Unset %s; use the reviewed preparation inputs.\n' "$name" >&2
  exit 2
done

directory="$repo_root/infra"
renderer=scripts.infrastructure.render_project_config
render_args=(--stack "$stack")
if [[ "$stack" == storage && "$mlflow_retention" == true ]]; then
  render_args+=(--mlflow-retention)
fi
if [[ "$stack" == storage && "$athena_retention" == true ]]; then
  render_args+=(--athena-retention)
fi
if [[ "$stack" == platform || "$stack" == services ]]; then
  # The shared renderer writes only the inputs the selected stack declares.
  directory="$directory/$stack"
  renderer=scripts.infrastructure.render_platform_config
  render_args=(--stack "$stack")
  if [[ -n "$preparation_inputs" ]]; then
    render_args+=(--preparation-inputs "$preparation_inputs" --preparation-sha256 "$preparation_sha256")
  fi
fi
if [[ "$stack" == jobs ]]; then
  directory="$directory/jobs"
  renderer=scripts.infrastructure.render_jobs_config
  render_args=()
  if [[ "$identity_probe" == true ]]; then
    directory="$directory/identity_probe"
    render_args+=(--identity-probe)
  fi
  if [[ -n "$preparation_inputs" ]]; then
    render_args+=(--preparation-inputs "$preparation_inputs" --preparation-sha256 "$preparation_sha256")
  fi
fi
if [[ "$stack" == iam ]]; then
  [[ -n "$admin_profile" ]] || { printf 'IAM updates require --admin-profile.\n' >&2; exit 2; }
  directory="$directory/iam"
  render_args+=(--admin-profile "$admin_profile")
elif [[ -n "$admin_profile" ]]; then
  printf 'Administrator profiles are allowed only with --iam.\n' >&2
  exit 2
fi

if [[ "$action" == apply ]]; then
  [[ $# -eq 1 && "$1" != -* ]] || { printf 'Apply requires one reviewed saved-plan path.\n' >&2; exit 2; }
  plan_path="$1"
  if [[ "$plan_path" != /* ]]; then
    plan_path="$directory/$plan_path"
  fi
  render_args+=(--check-plan "$plan_path")
fi

if [[ "$action" != validate ]]; then
  render_args+=(--verify-identity)
fi
unset AWS_PROFILE AWS_DEFAULT_PROFILE AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN AWS_SECURITY_TOKEN
# Services validation needs no rendered inputs or applied platform stack.
if [[ "$stack" != services || "$action" != validate ]]; then
  "$python_bin" -m "$renderer" ${render_args[@]+"${render_args[@]}"}
fi
if [[ "$action" == validate ]]; then
  exec terraform -chdir="$directory" validate "$@"
fi
if [[ "$stack" == iam && "$action" == plan ]]; then
  exec terraform -chdir="$directory" plan -input=false "-var=admin_profile=$admin_profile" "$@"
fi
if [[ "$stack" == services && "$action" == apply ]]; then
  # Launch hold during services applies (owner A, Nov 10 2026): take the apply lock (one services apply at a time, across machines), set the
  # hold before the apply changes anything, and clear the hold and release the lock only after the ECS services reach steady state. A failed
  # apply or settle leaves both until the owner resolves them with the lock's token.
  reason="terraform.sh --services apply ${plan_path##*/}"
  token="$("$python_bin" -m scripts.infrastructure.services_apply lock --reason "$reason")" || {
    printf 'The apply lock was not taken, so the services apply did not start.\n' >&2
    exit 1
  }
  resolve_hint() {
    printf 'The launch hold and the apply lock stay set, so no host job launches and no other services apply starts. Check the apply and the services, then run:\n' >&2
    printf '  %s -m scripts.infrastructure.services_apply resolve --statement %s --token %s\n' "$python_bin" "'<what you checked>'" "'$token'" >&2
  }
  "$python_bin" -m scripts.infrastructure.services_apply hold --reason "$reason" || {
    printf 'The launch hold could not be set, so the services apply did not start. The apply lock stays set, so no host job launches and no other services apply starts. Check the hold, then run:\n' >&2
    printf '  %s -m scripts.infrastructure.services_apply resolve --statement %s --token %s\n' "$python_bin" "'<what you checked>'" "'$token'" >&2
    exit 1
  }
  if ! terraform -chdir="$directory" apply -input=false "$@"; then
    printf 'The services apply failed.\n' >&2
    resolve_hint
    exit 1
  fi
  exec "$python_bin" -m scripts.infrastructure.services_apply settle --token "$token"
fi
exec terraform -chdir="$directory" "$action" -input=false "$@"
