#!/usr/bin/env bash
# Alert email teardown, step 4 of 4: Check that neither topic has a subscription of the address left (project profile).
# Nothing in this step has run against AWS. Run it from any folder of the main checkout. It runs one call of the helper scripts.infrastructure.alert_email,
# which reads .env and the Terraform outputs, checks every result, stops at the first failed or unexpected read or argument and prints status words only
# (<subject>: <status>). Exit code 0 means the step is done, 1 means it stopped and 3 means a subscription is still pending. The identity and exit status of
# every command (never their output) go to the ignored file plans/alert_email/last_run.log.
# A subscription that is still pending was never confirmed: it delivers nothing and AWS expires it on its own, and the step ends with exit code 3. The teardown is not complete
# until a later run of this step reports none on both topics. A confirmed subscription that the state did not hold stops the step with exit code 1. Then write the result, the
# subscriptions that remain and the ongoing cost (none) in the run record under plans/alert_email.
set -euo pipefail
trap 'echo "step: stopped"' ERR
cd "$(dirname "$0")/../../.."
.venv/bin/python -m scripts.infrastructure.alert_email subscriptions --expect none
