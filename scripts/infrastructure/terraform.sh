#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$repo_root"
python_bin="${PYTHON_BIN:-$repo_root/.venv/bin/python}"
stack=storage
admin_profile=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --iam) stack=iam; shift ;;
    --admin-profile) admin_profile="${2:?Supply the approved administrator profile}"; shift 2 ;;
    *) break ;;
  esac
done
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

directory="$repo_root/infra"
render_args=(--stack "$stack")
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
"$python_bin" -m scripts.infrastructure.render_project_config "${render_args[@]}"
if [[ "$action" == validate ]]; then
  exec terraform -chdir="$directory" validate "$@"
fi
if [[ "$stack" == iam && "$action" == plan ]]; then
  exec terraform -chdir="$directory" plan -input=false "-var=admin_profile=$admin_profile" "$@"
fi
exec terraform -chdir="$directory" "$action" -input=false "$@"
