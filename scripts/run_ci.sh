#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"
bash -n scripts/run_ci.sh scripts/infrastructure/*.sh
python_bin="${PYTHON_BIN:-$repo_root/.venv/bin/python}"
"$python_bin" -m ruff format --check scripts tests
"$python_bin" -m ruff check scripts tests
"$python_bin" -m mypy --check-untyped-defs --disallow-untyped-defs scripts/process.py scripts/infrastructure scripts/quality tests/test_project_config.py tests/support.py
"$python_bin" -m scripts.quality.scan_secrets
# Repository checks over every file Git tracks or would track; the same checks run as pre-commit hooks.
"$python_bin" -m scripts.quality.repo_checks lint-settings
"$python_bin" -m scripts.quality.repo_checks env-example
git ls-files -z --cached --others --exclude-standard | xargs -0 "$python_bin" -m scripts.quality.repo_checks credential-files
git ls-files -z --cached --others --exclude-standard | xargs -0 "$python_bin" -m scripts.quality.repo_checks data-files
git ls-files -z --cached --others --exclude-standard -- '*.py' | xargs -0 "$python_bin" -m scripts.quality.repo_checks suppressions
git ls-files -z --cached --others --exclude-standard -- '*.py' | xargs -0 "$python_bin" -m scripts.quality.repo_checks subprocess-imports
if [[ "${GITHUB_ACTIONS:-false}" == "true" ]]; then
  "$python_bin" -m scripts.quality.documentation_review check-untracked
else
  "$python_bin" -m scripts.quality.documentation_review check
fi
"$python_bin" -m scripts.quality.run_checks_e2e
"$python_bin" -m scripts.quality.run_documentation_review_e2e
e2e_output="${E2E_ARTIFACT_DIR:-$repo_root/data/e2e/configuration/$(date -u +%Y%m%dT%H%M%SZ)_$$}"
"$python_bin" -m scripts.infrastructure.run_e2e --output-directory "$e2e_output"
quality_output="${QUALITY_ARTIFACT_DIR:-$repo_root/data/e2e/quality/$(date -u +%Y%m%dT%H%M%SZ)_$$}"
"$python_bin" -m scripts.quality.run_e2e --output-directory "$quality_output/e2e"
"$python_bin" -m pytest --cov=scripts.infrastructure --cov-report=term-missing -q
terraform fmt -check -recursive infra

scratch="$(mktemp -d "${TMPDIR:-/tmp}/hai_ci.XXXXXX")"
trap 'rm -rf "$scratch"' EXIT
export TF_PLUGIN_CACHE_DIR="${TF_PLUGIN_CACHE_DIR:-$scratch/provider_cache}"
mkdir -p "$TF_PLUGIN_CACHE_DIR"

# Isolate tests from local credentials, deployment inputs and real Terraform state.
unset AWS_PROFILE AWS_DEFAULT_PROFILE AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN AWS_SECURITY_TOKEN
unset AWS_WEB_IDENTITY_TOKEN_FILE AWS_ROLE_ARN AWS_CONTAINER_CREDENTIALS_FULL_URI AWS_CONTAINER_CREDENTIALS_RELATIVE_URI
unset AWS_CONTAINER_AUTHORIZATION_TOKEN AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE
export AWS_CONFIG_FILE="$scratch/empty_aws_config"
export AWS_SHARED_CREDENTIALS_FILE="$scratch/empty_aws_credentials"
export AWS_EC2_METADATA_DISABLED=true TF_IN_AUTOMATION=1 TF_INPUT=0
touch "$AWS_CONFIG_FILE" "$AWS_SHARED_CREDENTIALS_FILE"

for stack in storage iam; do
  source_dir="$repo_root/infra"
  if [[ "$stack" == "iam" ]]; then
    source_dir="$repo_root/infra/iam"
  fi
  test_dir="$scratch/$stack"
  mkdir -p "$test_dir"
  cp "$source_dir"/*.tf "$source_dir/.terraform.lock.hcl" "$test_dir/"
  cp -R "$source_dir/tests" "$test_dir/tests"
  if [[ "$stack" == "iam" ]]; then
    cp -R "$source_dir/policies" "$test_dir/policies"
  fi
  printf '\nChecking %s Terraform configuration\n' "$stack"
  terraform -chdir="$test_dir" init -backend=false -input=false -lockfile=readonly -no-color
  terraform -chdir="$test_dir" validate -no-color
  terraform -chdir="$test_dir" test -no-color
done

static_failed=0
for stack in storage iam; do
  source_dir="$repo_root/infra"
  if [[ "$stack" == "iam" ]]; then
    source_dir="$repo_root/infra/iam"
  fi
  if ! "$python_bin" -m scripts.quality.static_checks --directory "$source_dir" --output-directory "$quality_output/$stack"; then
    static_failed=1
  fi
done
if [[ "$static_failed" != 0 ]]; then
  printf '\nTerraform static checks failed; review the retained quality evidence.\n' >&2
  exit 1
fi

printf '\nAll local CI checks passed without AWS credentials.\n'
