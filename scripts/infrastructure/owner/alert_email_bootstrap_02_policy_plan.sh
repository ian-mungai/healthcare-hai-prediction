#!/usr/bin/env bash
# Alert email at bootstrap, step 2 of 11: Plan the project user's SNS policy (administrator profile).
# Nothing in this step has run against AWS. Run it from any folder of the main checkout. It runs one call of the helper scripts.infrastructure.alert_email,
# which reads .env and the Terraform outputs, checks every result, stops at the first failed or unexpected read or argument and prints status words only
# (<subject>: <status>). Exit code 0 means the step is done, 1 means it stopped and 3 means a subscription is still pending. The identity and exit status of
# every command (never their output) go to the ignored file plans/alert_email/last_run.log.
# Set ADMIN_PROFILE to the administrator profile name first (export ADMIN_PROFILE=...). Before this step, set ALERT_EMAIL=<one address> in .env (step 1): one plain address, no name,
# no list, no spaces. An empty or missing ALERT_EMAIL creates no subscription, so skip the rest of the bootstrap. The address reaches only the alerts root.
# The helper stops if the plan holds a step for any policy but the SNS policy and stores the Plan digest exactly as the plan printed it in plans/alert_email/policy_digest.txt.
set -euo pipefail
trap 'echo "step: stopped"' ERR
cd "$(dirname "$0")/../../.."
.venv/bin/python -m scripts.infrastructure.alert_email policy-plan --admin-profile "${ADMIN_PROFILE:-}"
