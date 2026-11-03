#!/usr/bin/env bash
# Registers a mask for every private string of the Terraform stack inputs, then writes the project profile to the step output. The inputs
# come from the repository secret in STACK_INPUTS. GitHub masks that secret only as one exact string, so each field needs its own mask
# before anything prints it, and the repository is public. Tracing is turned off first, so an injected bash -x cannot print a value.
# Only mask commands go to standard output. Regions, short names such as dev, numbers and flags stay readable in the log.
set +x
set -euo pipefail

inputs="${STACK_INPUTS:?The stack inputs are missing.}"

if ! printf '%s' "$inputs" | jq -e 'type == "object" and (.aws_profile | type == "string") and (.expected_account_id | type == "string")' > /dev/null 2>&1; then
  echo "The stack inputs are not the expected object." >&2
  exit 1
fi

values="$(printf '%s' "$inputs" | jq -r '.. | strings | select(length >= 4 and (contains("\n") | not) and (test("^[a-z]{2}(-[a-z]+)+-[0-9]+$") | not))')"
while IFS= read -r value; do
  [[ -n "$value" ]] && printf '::add-mask::%s\n' "$value"
done <<< "$values"

profile="$(printf '%s' "$inputs" | jq -r .aws_profile)"
printf 'profile=%s\n' "$profile" >> "${GITHUB_OUTPUT:-/dev/null}"
