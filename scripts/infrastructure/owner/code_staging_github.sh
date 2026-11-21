#!/usr/bin/env bash
# Code staging setup on GitHub: set up the GitHub side of the code-staging workflow with your signed-in gh. It creates the code-staging environment (no reviewers, no wait timer,
# deployments from main only) and the repository variables CODE_RUN_PARAMETER, AWS_REGION and OIDC_DELIVERY_ENABLED.
#
# Nothing here has run. Nothing is typed: the project name and the region come from .env, and the repository and the delivery flag come from the output github_oidc_contract of the
# IAM stack, read with the profile in .env. The script reads everything back and stops with a failing line if any part differs. It changes nothing it already has, so you can run
# it again. The output names each part and masks the bucket, the profile, the project and the region. Run it from any folder of the main checkout, after the IAM stack that creates
# the staging role is applied. Afterwards open the repository's Settings, then Environments, and look at code-staging: it must show no required reviewers, no wait timer and the one
# branch main. Then push to main once the delivery flag is true: the Stage code workflow runs and names the current code run in its parameter.
set -euo pipefail
trap 'echo "Stopped at line $LINENO. Read the line above, fix it and run the script again: it changes nothing it already has." >&2; exit 1' ERR
cd "$(git rev-parse --show-toplevel)"
value() {
  local line
  line="$(grep -E "^$1=" .env | head -n 1 || true)"
  [[ -n "$line" ]] || { echo "$1 is not in .env" >&2; return 1; }
  line="${line#*=}"; line="${line#\"}"; printf '%s\n' "${line%\"}"
}
region="$(value AWS_REGION)"
profile="$(value AWS_PROFILE)"
bucket="$(value S3_BUCKET)"
project="$(value PROJECT_NAME)"
mask() { sed -e "s|$bucket|<bucket>|g" -e "s|$profile|<profile>|g" -e "s|$project|<project>|g" -e "s|$region|<region>|g"; }
gh auth status > /dev/null 2>&1 || { echo "gh is not signed in: run gh auth login first" >&2; exit 1; }
AWS_PROFILE="$profile" .venv/bin/python -m scripts.infrastructure.github_code_staging_bootstrap 2>&1 | mask
