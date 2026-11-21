#!/usr/bin/env bash
# Alert email teardown, step 1 of 4: Read which subscriptions the alerts root state holds (project profile).
# Nothing in this step has run against AWS. Run it from any folder of the main checkout. It runs one call of the helper scripts.infrastructure.alert_email,
# which reads .env and the Terraform outputs, checks every result, stops at the first failed or unexpected read or argument and prints status words only
# (<subject>: <status>). Exit code 0 means the step is done, 1 means it stopped and 3 means a subscription is still pending. The identity and exit status of
# every command (never their output) go to the ignored file plans/alert_email/last_run.log.
# Run the teardown before the platform stack and the IAM stack are removed: the alerts root reads the platform output when it renders its inputs. The project user destroys the
# subscriptions with the project profile and keeps everything else: the topics, the platform stack, .env and the state of the other stacks. The same steps apply when the alert
# address changes: run the teardown with the old address in place, change ALERT_EMAIL and run the bootstrap again. A failed state read, or a resource other than the two
# subscriptions, stops the step.
set -euo pipefail
trap 'echo "step: stopped"' ERR
cd "$(dirname "$0")/../../.."
.venv/bin/python -m scripts.infrastructure.alert_email state
