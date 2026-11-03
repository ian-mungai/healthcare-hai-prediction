#!/usr/bin/env bash
# Cases for plan_digest.sh: the digest must change with any changed value or output and stay put otherwise. Run from the repository root.
set -euo pipefail

script=".github/scripts/plan_digest.sh"
base='{"timestamp":"2026-10-30T20:00:00Z","terraform_version":"1.16.1","variables":{"image":{"value":"sha256:aaa"}},"resource_changes":[{"address":"aws_ecs_task_definition.api","change":{"actions":["update"],"before":{"image":"sha256:old"},"after":{"image":"sha256:aaa"},"after_unknown":{},"after_sensitive":{}}},{"address":"aws_s3_bucket.x","change":{"actions":["no-op"],"before":{"a":1},"after":{"a":1}}}],"output_changes":{"api_url":{"actions":["update"],"before":"a","after":"b"}}}'
failures=0

digest() { printf '%s' "$1" | bash "$script" digest; }
# A digest of nothing would differ from the base for the wrong reason, so every variant must parse.

expect() {
  local label="$1" want="$2" got="$3"
  if [[ "$want" == "same" && "$got" == "$(digest "$base")" ]] || [[ "$want" == "different" && "$got" != "$(digest "$base")" ]]; then
    echo "ok: $label"
  else
    echo "FAILED: $label (expected $want)" >&2
    failures=$((failures + 1))
  fi
}

vary() { printf '%s' "$base" | jq -c "$1"; }

expect "same plan, new timestamp and version" same "$(digest "$(vary '.timestamp = "2026-10-31T01:00:00Z" | .terraform_version = "1.16.2"')")"
expect "same plan, an unchanged resource differs" same "$(digest "$(vary '.resource_changes[1].change.after.a = 2')")"
expect "a changed value, same address and action" different "$(digest "$(vary '.resource_changes[0].change.after.image = "sha256:bbb"')")"
expect "a changed output, same resources" different "$(digest "$(vary '.output_changes.api_url.after = "c"')")"
expect "a changed input variable only" different "$(digest "$(vary '.variables.image.value = "sha256:zzz"')")"
expect "a changed action" different "$(digest "$(vary '.resource_changes[0].change.actions = ["delete","create"]')")"
expect "an added output" different "$(digest "$(vary '.output_changes.extra = {"actions":["create"],"after":"x"}')")"
expect "a replacement path" different "$(digest "$(vary '.resource_changes[0].change.replace_paths = [["image"]]')")"

summary="$(printf '%s' "$base" | bash "$script" summary)"
if [[ "$summary" == "update aws_ecs_task_definition.api" ]]; then
  echo "ok: the summary names addresses and actions only"
else
  echo "FAILED: summary" >&2
  failures=$((failures + 1))
fi
if printf '%s' "$summary" | grep -q 'sha256'; then
  echo "FAILED: the summary shows a value" >&2
  failures=$((failures + 1))
fi

# A resource address can carry an input value as its instance key. The summary is public, so it keeps the address and drops the key.
keyed='{"resource_changes":[{"address":"aws_ecr_repository.image[\"synthetic_private_repo\"]","change":{"actions":["create"]}},{"address":"module.m[\"a b\"].aws_s3_bucket.x[0]","change":{"actions":["update"]}}]}'
keyed_summary="$(printf '%s' "$keyed" | bash "$script" summary)"
if [[ "$keyed_summary" == *synthetic_private_repo* || "$keyed_summary" == *"a b"* ]]; then
  echo "FAILED: the summary prints an instance key" >&2
  failures=$((failures + 1))
else
  echo "ok: the summary prints no instance key"
fi
if [[ "$keyed_summary" == *'create aws_ecr_repository.image[*]'* && "$keyed_summary" == *'update module.m[*].aws_s3_bucket.x[*]'* ]]; then
  echo "ok: the summary keeps the address and marks each key"
else
  echo "FAILED: the summary does not keep the address shape" >&2
  failures=$((failures + 1))
fi

# A failing digest command must stop the comparison. A stand-in jq fails only the digest filter (the one run with -S), the way a broken
# dependency would, and the empty-input digest must never be accepted as an approved or an actual value.
bin_directory="$(mktemp -d)"
real_jq="$(command -v jq)"
cat > "$bin_directory/jq" <<STAND_IN
#!/usr/bin/env bash
if [[ "\${1:-}" == "-S" ]]; then
  exit 42
fi
exec "$real_jq" "\$@"
STAND_IN
chmod +x "$bin_directory/jq"
empty_digest="e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
good="$(digest "$base")"
changed="$(vary '.resource_changes[0].change.after.image = "sha256:bbb"')"

status() { "$@" > /dev/null 2>&1 && echo 0 || echo $?; }
check() {
  local label="$1" want="$2" got="$3"
  if [[ "$want" == "zero" && "$got" == "0" ]] || [[ "$want" == "nonzero" && "$got" != "0" ]]; then
    echo "ok: $label"
  else
    echo "FAILED: $label (expected $want, got $got)" >&2
    failures=$((failures + 1))
  fi
}

check "verify accepts the approved digest" zero "$(printf '%s' "$base" | status bash "$script" verify "$good")"
check "verify refuses a changed plan" nonzero "$(printf '%s' "$changed" | status bash "$script" verify "$good")"
check "verify refuses an empty approved digest" nonzero "$(printf '%s' "$base" | status bash "$script" verify "")"
check "verify refuses a malformed approved digest" nonzero "$(printf '%s' "$base" | status bash "$script" verify "not-a-digest")"
check "verify refuses the empty-input digest as approved" nonzero "$(printf '%s' "$base" | status bash "$script" verify "$empty_digest")"
check "digest fails when its command fails" nonzero "$(printf '%s' "$base" | PATH="$bin_directory:$PATH" status bash "$script" digest)"
check "digest prints nothing when its command fails" zero "$([[ -z "$(printf '%s' "$base" | PATH="$bin_directory:$PATH" bash "$script" digest 2> /dev/null)" ]] && echo 0 || echo 1)"
check "verify fails when the digest command fails, whatever was approved" nonzero "$(printf '%s' "$changed" | PATH="$bin_directory:$PATH" status bash "$script" verify "$empty_digest")"
check "verify refuses an empty plan" nonzero "$(printf '' | status bash "$script" verify "$good")"
check "verify refuses a plan that is not JSON" nonzero "$(printf 'not json' | status bash "$script" verify "$good")"

# The workflows must use verify, never compare a command substitution of the digest.
check "the apply workflow compares with verify" zero "$(grep -q 'plan_digest.sh verify' .github/workflows/terraform_apply.yml && echo 0 || echo 1)"
check "no workflow compares a digest inside a test" zero "$(grep -n 'plan_digest.sh digest' .github/workflows/*.yml | grep -q '\[\[' && echo 1 || echo 0)"
check "the Terraform steps stop on the first error" zero "$(grep -c 'set -uo pipefail' .github/workflows/terraform_plan.yml .github/workflows/terraform_apply.yml | grep -vq ':0$' && echo 1 || echo 0)"

[[ $failures -eq 0 ]]
