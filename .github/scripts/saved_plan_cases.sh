#!/usr/bin/env bash
# Cases for saved_plan.sh: CI saves each Terraform plan with its SHA-256 digest under one private prefix of the project bucket, and the owner's
# apply path downloads the plan and refuses it unless the digest matches. A stand-in "aws" keeps the objects in a folder, so nothing reaches AWS.
# The bucket name and the plan bytes are synthetic. Run from the repository root.
set -euo pipefail

script=".github/scripts/saved_plan.sh"
failures=0
scratch="$(mktemp -d)"
trap 'rm -r "$scratch"' EXIT

check() {
  local label="$1"
  shift
  if "$@"; then
    echo "ok: $label"
  else
    echo "FAILED: $label" >&2
    failures=$((failures + 1))
  fi
}

# The stand-in implements put-object and get-object of "aws s3api" against $FAKE_S3/<bucket>/<key>, and logs each call. FAKE_S3_FAIL makes every call fail.
bin_directory="$scratch/bin"
mkdir -p "$bin_directory"
cat > "$bin_directory/aws" << 'STAND_IN'
#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >> "$FAKE_S3/calls.log"
if [[ -n "${FAKE_S3_FAIL:-}" ]] || { [[ -n "${FAKE_S3_FAIL_KEY:-}" ]] && [[ "$*" == *"--key "*"${FAKE_S3_FAIL_KEY}"* ]]; }; then
  echo "An error occurred (AccessDenied) when calling the operation" >&2
  exit 255
fi
[[ "$1" == "s3api" ]] || { echo "unexpected service $1" >&2; exit 2; }
operation="$2"
shift 2
bucket="" key="" body="" output=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --bucket) bucket="$2"; shift 2 ;;
    --key) key="$2"; shift 2 ;;
    --body) body="$2"; shift 2 ;;
    --server-side-encryption) shift 2 ;;
    --checksum-algorithm) shift 2 ;;
    *) output="$1"; shift ;;
  esac
done
case "$operation" in
  put-object)
    mkdir -p "$FAKE_S3/objects/$bucket/$(dirname "$key")"
    cp "$body" "$FAKE_S3/objects/$bucket/$key"
    echo '{"ETag": "\"synthetic\""}'
    ;;
  get-object)
    [[ -f "$FAKE_S3/objects/$bucket/$key" ]] || { echo "An error occurred (NoSuchKey) when calling the GetObject operation" >&2; exit 254; }
    cp "$FAKE_S3/objects/$bucket/$key" "$output"
    echo '{"ContentLength": 1}'
    ;;
  *) echo "unexpected operation $operation" >&2; exit 2 ;;
esac
STAND_IN
chmod +x "$bin_directory/aws"

export FAKE_S3="$scratch/s3"
export PLAN_BUCKET="synthetic-private-plan-bucket"
sha="0123456789abcdef0123456789abcdef01234567"
other_sha="fedcba9876543210fedcba9876543210fedcba98"
plan="$scratch/plan.bin"
printf 'synthetic-plan-bytes-with-a-synthetic-secret-value' > "$plan"
good="$(sha256sum "$plan" | cut -d' ' -f1)"
empty_digest="e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
prefix="$FAKE_S3/objects/$PLAN_BUCKET/terraform_plans"
mkdir -p "$FAKE_S3"

run() { PATH="$bin_directory:$PATH" bash "$script" "$@"; }
status() { "$@" > /dev/null 2>&1 && echo 0 || echo $?; }
check_status() {
  local label="$1" want="$2" got="$3"
  # A refusal is the script's own exit code 1: a missing script (127) or a crash is not a refusal.
  if [[ "$want" == "zero" && "$got" == "0" ]] || [[ "$want" == "nonzero" && "$got" == "1" ]]; then
    echo "ok: $label"
  else
    echo "FAILED: $label (expected $want, got $got)" >&2
    failures=$((failures + 1))
  fi
}
fresh() { find "$FAKE_S3" -mindepth 1 -delete 2> /dev/null || true; mkdir -p "$FAKE_S3"; }

# At least one call must exist, so a script that never ran cannot pass these.
keys_stay_in_the_prefix() {
  [[ -f "$FAKE_S3/calls.log" ]] && [[ "$(grep -c -- '--key ' "$FAKE_S3/calls.log")" -ge 1 ]] && [[ "$(grep -c -- '--key terraform_plans/' "$FAKE_S3/calls.log")" -eq "$(grep -c -- '--key ' "$FAKE_S3/calls.log")" ]]
}
calls_name_only_the_project_bucket() {
  [[ -f "$FAKE_S3/calls.log" ]] && [[ "$(grep -c -- '--bucket ' "$FAKE_S3/calls.log")" -ge 1 ]] && [[ "$(grep -c -- "--bucket $PLAN_BUCKET " "$FAKE_S3/calls.log")" -eq "$(grep -c -- '--bucket ' "$FAKE_S3/calls.log")" ]]
}

check "the saved plan script exists" test -f "$script"

# --- upload ------------------------------------------------------------------------------------------------------------------------------
fresh
output="$(run upload platform "$sha" "$plan" 2> /dev/null || true)"
check "upload prints the digest of the plan file" test "$output" = "digest=$good"
check "upload writes the plan under its stack and commit" cmp -s "$plan" "$prefix/platform/$sha/plan.bin"
check "upload writes the digest beside the plan" test "$(cat "$prefix/platform/$sha/plan.sha256" 2> /dev/null)" = "$good"
check "upload writes exactly those two objects" test "$(find "$FAKE_S3/objects" -type f | wc -l | tr -d ' ')" = "2"
check "upload prints neither the bucket nor the plan bytes" test -n "$output" -a -z "$(printf '%s' "$output" | grep -E 'synthetic-private-plan-bucket|synthetic-plan-bytes|synthetic-secret' || true)"
check "every key the upload names starts with the plans prefix" keys_stay_in_the_prefix
check "upload names only the project bucket" calls_name_only_the_project_bucket

fresh
check_status "upload refuses the iam stack" nonzero "$(status run upload iam "$sha" "$plan")"
check_status "upload refuses the storage stack" nonzero "$(status run upload storage "$sha" "$plan")"
check_status "upload refuses a stack name that climbs out of the prefix" nonzero "$(status run upload "../terraform_state/platform" "$sha" "$plan")"
check_status "upload refuses a commit that is not 40 hex digits" nonzero "$(status run upload platform "../iam" "$plan")"
check_status "upload refuses a short commit" nonzero "$(status run upload platform "0123456" "$plan")"
check_status "upload refuses a missing plan file" nonzero "$(status run upload platform "$sha" "$scratch/absent.bin")"
: > "$scratch/empty.bin"
check_status "upload refuses an empty plan file" nonzero "$(status run upload platform "$sha" "$scratch/empty.bin")"
check_status "upload refuses a missing bucket" nonzero "$(PLAN_BUCKET="" status run upload platform "$sha" "$plan")"
check_status "upload refuses a bucket name with a slash" nonzero "$(PLAN_BUCKET="bucket/terraform_state" status run upload platform "$sha" "$plan")"
refused_upload_writes_no_object() {
  fresh
  local code
  code="$(status run upload iam "$sha" "$plan")"
  [[ "$code" == "1" ]] && [[ "$(find "$FAKE_S3" -type f -not -name calls.log | wc -l | tr -d ' ')" == "0" ]]
}
check "a refused upload writes no object" refused_upload_writes_no_object
check_status "upload fails when AWS refuses" nonzero "$(FAKE_S3_FAIL=1 status run upload platform "$sha" "$plan")"
prints_no_digest_when_aws_refuses() {
  local printed
  fresh
  printed="$(FAKE_S3_FAIL=1 run upload platform "$sha" "$plan" 2> /dev/null || true)"
  [[ -z "$printed" ]] && grep -q 'put-object' "$FAKE_S3/calls.log"
}
check "upload prints no digest when AWS refuses, and tried to write" prints_no_digest_when_aws_refuses

leaves_no_digest_without_a_plan() {
  local code
  fresh
  code="$(FAKE_S3_FAIL_KEY=plan.bin status run upload platform "$sha" "$plan")"
  [[ "$code" == "1" ]] && [[ ! -e "$prefix/platform/$sha/plan.sha256" ]] && grep -q 'put-object' "$FAKE_S3/calls.log"
}
check "a digest object never exists without its plan when the plan save fails" leaves_no_digest_without_a_plan

# --- fetch -------------------------------------------------------------------------------------------------------------------------------
download="$scratch/download"
mkdir -p "$download"
fresh
run upload services "$sha" "$plan" > /dev/null 2>&1 || true
check_status "fetch accepts the digest that matches" zero "$(status run fetch services "$sha" "$good" "$download")"
check "fetch writes the plan it verified" cmp -s "$plan" "$download/plan.bin"

empty_target="$scratch/download_refused"
mkdir -p "$empty_target"
flipped="${good:0:63}$([[ "${good:63:1}" == "0" ]] && echo 1 || echo 0)"
check_status "fetch refuses an expected digest that differs" nonzero "$(status run fetch services "$sha" "$flipped" "$empty_target")"
check_status "fetch refuses a malformed expected digest" nonzero "$(status run fetch services "$sha" "not-a-digest" "$empty_target")"
check_status "fetch refuses an empty expected digest" nonzero "$(status run fetch services "$sha" "" "$empty_target")"
check_status "fetch refuses the iam stack" nonzero "$(status run fetch iam "$sha" "$good" "$empty_target")"
check_status "fetch refuses a commit that was not saved" nonzero "$(status run fetch services "$other_sha" "$good" "$empty_target")"
check_status "fetch refuses a stack that was not saved" nonzero "$(status run fetch jobs "$sha" "$good" "$empty_target")"

# An empty plan with the digest of empty input saved beside it must not pass when the owner holds that digest.
mkdir -p "$prefix/jobs/$other_sha"
: > "$prefix/jobs/$other_sha/plan.bin"
printf '%s\n' "$empty_digest" > "$prefix/jobs/$other_sha/plan.sha256"
check_status "fetch refuses an empty plan saved with the digest of empty input" nonzero "$(status run fetch jobs "$other_sha" "$empty_digest" "$empty_target")"

# A plan replaced after the upload, with the digest object left alone, must not pass; nor a digest object replaced to match a replaced plan
# when the owner holds the digest the job printed.
mkdir -p "$prefix/services/$sha"
printf 'tampered plan bytes' > "$prefix/services/$sha/plan.bin"
check_status "fetch refuses a plan that was replaced after the upload" nonzero "$(status run fetch services "$sha" "$good" "$empty_target")"
tampered="$(sha256sum "$prefix/services/$sha/plan.bin" | cut -d' ' -f1)"
printf '%s\n' "$tampered" > "$prefix/services/$sha/plan.sha256"
check_status "fetch refuses a plan and a digest object that were both replaced" nonzero "$(status run fetch services "$sha" "$good" "$empty_target")"
cp "$plan" "$prefix/services/$sha/plan.bin"
check_status "fetch refuses a digest object that differs from the digest the owner holds" nonzero "$(status run fetch services "$sha" "$good" "$empty_target")"
refused_fetch_leaves_no_plan() {
  local code
  code="$(status run fetch services "$sha" "$good" "$empty_target")"
  [[ "$code" == "1" ]] && [[ ! -e "$empty_target/plan.bin" ]]
}
check "a refused fetch leaves no plan file" refused_fetch_leaves_no_plan

fresh
run upload jobs "$sha" "$plan" > /dev/null 2>&1 || true
printf 'older bytes' > "$download/plan.bin"
check_status "fetch will not overwrite a plan file that is already there" nonzero "$(status run fetch jobs "$sha" "$good" "$download")"
check_status "fetch refuses an output folder that does not exist" nonzero "$(status run fetch jobs "$sha" "$good" "$scratch/absent")"
check "every key the fetch names starts with the plans prefix" keys_stay_in_the_prefix

# --- the script keeps to its job -----------------------------------------------------------------------------------------------------------
check "the script never names the state prefix" test -f "$script" -a -z "$(grep -n 'terraform_state' "$script" || true)"
check "the script never lists or deletes an object" test -f "$script" -a -z "$(grep -En 'delete-object|list-objects|s3 rm|s3 sync' "$script" || true)"
check "the script names the plans prefix once, in one variable" test -f "$script" -a "$(grep -c terraform_plans "$script" 2> /dev/null || echo 0)" -ge 1

[[ $failures -eq 0 ]]
