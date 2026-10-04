#!/usr/bin/env bash
# Full-project pattern scan for the privacy review: counts per file only, never matched values.
# Scans tracked, untracked, ignored and hidden files (including decompressed .gz), excluding .git and this folder.
set -euo pipefail
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"
cd "$repo_root"
out="data/privacy_review/20260927/patterns"
mkdir -p "$out"
account="$(sed -n 's/^AWS_ACCOUNT_ID=//p' .env | tr -d '"[:space:]')"
bucket_hint="$(sed -n 's/^PROJECT_NAME=//p' .env | tr -d '"[:space:]')"
home_path="$HOME"

scan() {
  local name="$1"
  shift
  rg --no-ignore --hidden --text --search-zip --count-matches --no-messages \
    -g '!.git/' -g '!data/privacy_review/' "$@" . > "$out/$name.counts" || true
  printf '%s files=%s\n' "$name" "$(wc -l < "$out/$name.counts" | tr -d ' ')"
}

scan email -e '[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}'
scan us_phone -e '\(?\b[2-9][0-9]{2}\)?[-. ][0-9]{3}[-. ][0-9]{4}\b'
scan ssn_shape -e '\b[0-9]{3}-[0-9]{2}-[0-9]{4}\b'
scan aws_arn_with_account -e 'arn:aws:[a-z0-9-]+:[a-z0-9-]*:[0-9]{12}:'
scan aws_access_key_id -e '\b(AKIA|ASIA)[0-9A-Z]{16}\b'
scan private_key_header -e '-----BEGIN [A-Z ]*PRIVATE KEY-----'
scan project_account_id -F -e "$account"
scan local_home_path -F -e "$home_path"
scan project_name -F -e "$bucket_hint"
# The owner's address comes from local Git settings, never from this file.
user_email="$(git config user.email || true)"
[[ -n "$user_email" ]] && scan user_email -F -e "$user_email"
scan secret_names -e '(census_api_key|bls_api_key|registrationkey|api_key)'
printf 'done %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
