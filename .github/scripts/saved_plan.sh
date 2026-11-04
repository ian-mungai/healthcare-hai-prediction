#!/usr/bin/env bash
# Saves a Terraform plan with its SHA-256 digest in the project bucket, and fetches it back for the owner's apply (owner decision D18). The
# repository is public, so job logs are public: the plan holds values and never reaches a log. Only the digest is printed.
#   upload <stack> <commit> <plan-file>
#       Needs PLAN_BUCKET. Writes terraform_plans/<stack>/<commit>/plan.bin and plan.sha256 (the SHA-256 of the plan file), then prints
#       "digest=<hex>". The stack is one of platform, services and jobs; the commit is the 40 hex digits of the pull request head.
#   fetch <stack> <commit> <expected-digest> <output-folder>
#       The owner's apply path. Needs PLAN_BUCKET. <expected-digest> is the digest the plan job printed in its summary. Downloads the digest
#       object and the plan, and writes <output-folder>/plan.bin only when both equal the expected digest. Otherwise it exits 1 and writes nothing.
# Every refusal and every failed AWS call exits 1. The plan role may only write under terraform_plans/; nothing here lists or deletes an object.
set -euo pipefail

prefix="terraform_plans"
empty_digest="e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

refuse() {
  echo "$1" >&2
  exit 1
}

well_formed() {
  [[ "$1" =~ ^[0-9a-f]{64}$ && "$1" != "$empty_digest" ]]
}

digest_of() {
  local hash
  hash="$(sha256sum "$1" | cut -d' ' -f1)" || return 1
  well_formed "$hash" || return 1
  printf '%s' "$hash"
}

check_names() {
  local stack="$1" commit="$2"
  case "$stack" in
    platform | services | jobs) ;;
    *) refuse "Use the stack platform, services or jobs." ;;
  esac
  [[ "$commit" =~ ^[0-9a-f]{40}$ ]] || refuse "Use the 40 hex digits of the commit."
  [[ "${PLAN_BUCKET:-}" =~ ^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$ ]] || refuse "Set PLAN_BUCKET to the project bucket name."
}

action="${1:-}"
case "$action" in
  upload)
    stack="${2:-}"
    commit="${3:-}"
    plan_file="${4:-}"
    check_names "$stack" "$commit"
    [[ -f "$plan_file" && -s "$plan_file" ]] || refuse "The plan file is missing or empty."
    digest="$(digest_of "$plan_file")" || refuse "The plan has no well formed digest."
    key="$prefix/$stack/$commit"
    digest_file="$(mktemp)"
    trap 'rm -f "$digest_file"' EXIT
    printf '%s\n' "$digest" > "$digest_file"
    # The plan goes first, so a digest object never exists without its plan.
    aws s3api put-object --bucket "$PLAN_BUCKET" --key "$key/plan.bin" --body "$plan_file" --server-side-encryption AES256 > /dev/null 2>&1 || refuse "Saving the plan failed. Its output stays out of the public log."
    aws s3api put-object --bucket "$PLAN_BUCKET" --key "$key/plan.sha256" --body "$digest_file" --server-side-encryption AES256 > /dev/null 2>&1 || refuse "Saving the digest failed. Its output stays out of the public log."
    printf 'digest=%s\n' "$digest"
    ;;
  fetch)
    stack="${2:-}"
    commit="${3:-}"
    expected="${4:-}"
    output_folder="${5:-}"
    check_names "$stack" "$commit"
    well_formed "$expected" || refuse "The expected digest is missing or malformed."
    [[ -d "$output_folder" ]] || refuse "The output folder does not exist."
    [[ ! -e "$output_folder/plan.bin" ]] || refuse "A plan file is already in the output folder."
    key="$prefix/$stack/$commit"
    scratch="$(mktemp -d)"
    trap 'rm -r "$scratch"' EXIT
    aws s3api get-object --bucket "$PLAN_BUCKET" --key "$key/plan.sha256" "$scratch/plan.sha256" > /dev/null 2>&1 || refuse "The saved digest could not be fetched."
    aws s3api get-object --bucket "$PLAN_BUCKET" --key "$key/plan.bin" "$scratch/plan.bin" > /dev/null 2>&1 || refuse "The saved plan could not be fetched."
    saved="$(tr -d '[:space:]' < "$scratch/plan.sha256")"
    [[ "$saved" == "$expected" ]] || refuse "The saved digest differs from the digest of the plan job."
    actual="$(digest_of "$scratch/plan.bin")" || refuse "The saved plan has no well formed digest."
    [[ "$actual" == "$expected" ]] || refuse "The saved plan does not match the digest of the plan job. Do not apply it."
    mv "$scratch/plan.bin" "$output_folder/plan.bin"
    echo "The saved plan matches the digest. It is at $output_folder/plan.bin."
    ;;
  *)
    echo "Use upload or fetch." >&2
    exit 2
    ;;
esac
