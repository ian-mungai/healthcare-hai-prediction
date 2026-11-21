#!/usr/bin/env bash
# Alert email at bootstrap, step 7 of 11: Prepare the alerts root (project profile).
# Nothing in this step has run against AWS. Run it from any folder of the main checkout. It runs one call of the helper scripts.infrastructure.alert_email,
# which reads .env and the Terraform outputs, checks every result, stops at the first failed or unexpected read or argument and prints status words only
# (<subject>: <status>). Exit code 0 means the step is done, 1 means it stopped and 3 means a subscription is still pending. The identity and exit status of
# every command (never their output) go to the ignored file plans/alert_email/last_run.log.
# The helper downloads the pinned provider and renders the ignored inputs from .env and the platform output.
set -euo pipefail
trap 'echo "step: stopped"' ERR
cd "$(dirname "$0")/../../.."
.venv/bin/python -m scripts.infrastructure.alert_email prepare
