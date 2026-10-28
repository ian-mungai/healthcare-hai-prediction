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

while [[ $# -gt 0 ]]; do
  case "$1" in
    --iam | --platform | --services | --jobs)
      [[ "$stack" == storage ]] || { printf 'Choose one stack: --iam, --platform, --services or --jobs.\n' >&2; exit 2; }
      stack="${1#--}"
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
if [[ -n "$preparation_inputs" || -n "$preparation_sha256" || "$identity_probe" == true ]]; then
  [[ "$stack" == jobs ]] || { printf 'Preparation inputs and the identity probe are allowed only with --jobs.\n' >&2; exit 2; }
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
if [[ "$stack" == platform || "$stack" == services ]]; then
  # The shared renderer writes only the inputs the selected stack declares.
  directory="$directory/$stack"
  renderer=scripts.infrastructure.render_platform_config
  render_args=(--stack "$stack")
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
exec terraform -chdir="$directory" "$action" -input=false "$@"
