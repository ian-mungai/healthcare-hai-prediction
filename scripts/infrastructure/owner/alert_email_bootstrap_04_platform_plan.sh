#!/usr/bin/env bash
# Alert email at bootstrap, step 4 of 11: Plan the platform stack as in the platform step of the bootstrap (project profile).
# Nothing in this step has run against AWS. Run it from any folder of the main checkout. It runs one call of the helper scripts.infrastructure.alert_email,
# which reads .env and the Terraform outputs, checks every result, stops at the first failed or unexpected read or argument and prints status words only
# (<subject>: <status>). Exit code 0 means the step is done, 1 means it stopped and 3 means a subscription is still pending. The identity and exit status of
# every command (never their output) go to the ignored file plans/alert_email/last_run.log.
# The stack creates the alerts topic and holds no subscription. If the bootstrap already applied it, the plan shows no change. The helper stops if the plan deletes or replaces anything.
set -euo pipefail
trap 'echo "step: stopped"' ERR
cd "$(dirname "$0")/../../.."
.venv/bin/python -m scripts.infrastructure.alert_email plan platform
