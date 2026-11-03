#!/usr/bin/env bash
# Reads `terraform show -json` of a saved plan on stdin. The repository is public, so job logs are public. Modes:
#   digest         prints the SHA-256 of every changed resource with all of its values (before, after, unknown and sensitive markers,
#                  replacement paths), every output change and every input variable. The values are hashed and never printed.
#   verify DIGEST  computes the same digest and succeeds only when DIGEST is a well-formed digest and equals it.
#   summary        prints only each changed resource address and its action, for the job summary. Instance keys show as [*].
# The timestamp and the Terraform version are left out, so two plans of the same changes and values give the same digest.
# A failed or empty step never yields a digest: each command is assigned on its own and its status is checked before its output is used.
set -euo pipefail

empty_digest="e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

well_formed() {
  [[ "$1" =~ ^[0-9a-f]{64}$ && "$1" != "$empty_digest" ]]
}

compute_digest() {
  local plan normalized hash
  plan="$(cat)" || return 1
  [[ -n "$plan" ]] || { echo "The plan is empty." >&2; return 1; }
  normalized="$(printf '%s' "$plan" | jq -S '{
    resource_changes: [(.resource_changes // [])[] | select(.change.actions != ["no-op"]) | {address, index, action_reason, change}],
    output_changes: (.output_changes // {}),
    variables: (.variables // {})
  }')" || { echo "Reading the plan failed." >&2; return 1; }
  [[ -n "$normalized" ]] || { echo "The plan gave no normalized form." >&2; return 1; }
  hash="$(printf '%s' "$normalized" | sha256sum | cut -d' ' -f1)" || return 1
  well_formed "$hash" || { echo "The digest is not well formed." >&2; return 1; }
  printf '%s' "$hash"
}

case "${1:-}" in
  digest)
    digest="$(compute_digest)"
    printf '%s\n' "$digest"
    ;;
  verify)
    approved="${2:-}"
    well_formed "$approved" || { echo "The approved digest is missing or malformed." >&2; exit 1; }
    actual="$(compute_digest)"
    well_formed "$actual" || exit 1
    if [[ "$actual" != "$approved" ]]; then
      echo "A value, an output or an input differs from the plan the owner approved." >&2
      exit 1
    fi
    echo "The plan digest equals the approved digest."
    ;;
  summary)
    # An instance key can be an input value, so the address keeps its shape and each key shows as [*].
    jq -r '[(.resource_changes // [])[] | select(.change.actions != ["no-op"]) | "\(.change.actions | join("+")) \(.address | gsub("\\[[^\\]]*\\]"; "[*]"))"] | sort | .[]'
    ;;
  *)
    echo "Use digest, verify or summary." >&2
    exit 2
    ;;
esac
