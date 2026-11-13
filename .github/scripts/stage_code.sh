#!/usr/bin/env bash
# Stages the code archive of one commit on main as a code run and names that run in one parameter (owner decision "Ingestion code and inputs staged on AWS,
# never locally"). The monthly publisher run and the daily retries bind to the current code run by its digest.
#   stage_code.sh <code.tar.gz> <code.sha256> <commit-time-stamp> <commit> <head-of-main>
#       Needs DATA_BUCKET (the project bucket) and CODE_RUN_PARAMETER (the parameter that names the current code run). <commit-time-stamp> is the
#       commit's own time, YYYYMMDDTHHMMSSZ in UTC, and <commit> its 40 hex digits. The code run is "<stamp>-<first eight digits of the commit>", so the same
#       commit always has the same run. The script creates lakehouse/run_stage/<run>/inputs/code.tar.gz, then code.sha256, each only when no object of that key
#       exists, then puts the parameter, but only when <head-of-main> (the 40 hex digits of the commit that main points at when the script runs, read by the
#       workflow through GitHub) is <commit>. A rerun of an older push, or a push that main has moved past, stages its files and leaves the parameter alone, so the
#       parameter never moves back to older code. A second push of the same commit finds both files staged and puts the same parameter again. It prints
#       "code_run=<run>" when it named the run and "code_run_not_named=<run>" when it did not.
# The staging role may only create those two keys and put that one parameter, so the script makes no other call. Every refusal and every failed call exits 1.
set -euo pipefail

refuse() {
  echo "$1" >&2
  exit 1
}

archive="${1:-}"
digest_file="${2:-}"
stamp="${3:-}"
commit="${4:-}"
head_of_main="${5:-}"
[[ "${DATA_BUCKET:-}" =~ ^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$ ]] || refuse "Set DATA_BUCKET to the project bucket name."
[[ "${CODE_RUN_PARAMETER:-}" =~ ^(/[A-Za-z0-9_]+)+$ ]] || refuse "Set CODE_RUN_PARAMETER to a parameter path of letters, digits and underscores."
[[ "$stamp" =~ ^[0-9]{8}T[0-9]{6}Z$ ]] || refuse "Use the commit time as YYYYMMDDTHHMMSSZ."
[[ "$commit" =~ ^[0-9a-f]{40}$ ]] || refuse "Use the 40 hex digits of the commit."
[[ "$head_of_main" =~ ^[0-9a-f]{40}$ ]] || refuse "Use the 40 hex digits of the commit that main points at."
[[ -f "$archive" && -s "$archive" && -f "$digest_file" ]] || refuse "The archive or its digest file is missing or empty."
digest="$(sha256sum "$archive" | cut -d' ' -f1)"
[[ "$digest" =~ ^[0-9a-f]{64}$ && "$(tr -d '[:space:]' < "$digest_file")" == "$digest" ]] || refuse "The digest file does not hold the archive's SHA-256."

run_id="$stamp-${commit:0:8}"
prefix="lakehouse/run_stage/$run_id/inputs"

# A key that already exists answers PreconditionFailed: the same commit staged it before, and the code run is never replaced.
create_once() {
  local key="$1" body="$2" errors
  errors="$(mktemp)"
  if aws s3api put-object --bucket "$DATA_BUCKET" --key "$key" --body "$body" --if-none-match '*' --server-side-encryption AES256 > /dev/null 2> "$errors"; then
    rm -f "$errors"
    return 0
  fi
  if grep -q "PreconditionFailed" "$errors"; then
    rm -f "$errors"
    return 0
  fi
  rm -f "$errors"
  refuse "Creating $key failed. Its output stays out of the log."
}

# The archive goes first, so a digest file never exists without its archive, and the parameter goes last, so it never names a run that is not staged.
create_once "$prefix/code.tar.gz" "$archive"
create_once "$prefix/code.sha256" "$digest_file"
if [[ "$head_of_main" != "$commit" ]]; then
  printf 'code_run_not_named=%s\n' "$run_id"
  exit 0
fi
aws ssm put-parameter --name "$CODE_RUN_PARAMETER" --type String --value "$run_id" --overwrite > /dev/null 2>&1 || refuse "Naming the current code run failed."
printf 'code_run=%s\n' "$run_id"
