#!/usr/bin/env bash
# Cases for stage_code.sh: each push to main stages the commit's code archive as a code run and names it in one parameter. A stand-in "aws" keeps the
# objects in a folder and refuses a second create of a key, so nothing reaches AWS. The bucket, the parameter and the archive bytes are synthetic.
# Run from the repository root.
set -euo pipefail

script=".github/scripts/stage_code.sh"
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

# The stand-in implements "s3api put-object" with --if-none-match and "ssm put-parameter" against $FAKE_AWS, and logs each call. FAKE_FAIL_KEY makes
# a put of a key that holds the text fail with AccessDenied.
bin_directory="$scratch/bin"
mkdir -p "$bin_directory"
cat > "$bin_directory/aws" << 'STAND_IN'
#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >> "$FAKE_AWS/calls.log"
service="$1"
operation="$2"
shift 2
bucket="" key="" body="" name="" value="" once=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --bucket) bucket="$2"; shift 2 ;;
    --key) key="$2"; shift 2 ;;
    --body) body="$2"; shift 2 ;;
    --if-none-match) once="$2"; shift 2 ;;
    --name) name="$2"; shift 2 ;;
    --value) value="$2"; shift 2 ;;
    --server-side-encryption | --type) shift 2 ;;
    --overwrite) shift ;;
    *) echo "unexpected argument $1" >&2; exit 2 ;;
  esac
done
if [[ -n "${FAKE_FAIL_KEY:-}" && "$key" == *"${FAKE_FAIL_KEY}"* ]]; then
  echo "An error occurred (AccessDenied) when calling the PutObject operation" >&2
  exit 255
fi
case "$service $operation" in
  "s3api put-object")
    [[ "$once" == "*" ]] || { echo "An error occurred (AccessDenied) when calling the PutObject operation" >&2; exit 255; }
    if [[ -f "$FAKE_AWS/objects/$bucket/$key" ]]; then
      echo "An error occurred (PreconditionFailed) when calling the PutObject operation: At least one of the pre-conditions you specified did not hold" >&2
      exit 254
    fi
    mkdir -p "$FAKE_AWS/objects/$bucket/$(dirname "$key")"
    cp "$body" "$FAKE_AWS/objects/$bucket/$key"
    echo '{"ETag": "\"synthetic\""}'
    ;;
  "ssm put-parameter")
    mkdir -p "$FAKE_AWS/parameters"
    printf '%s' "$value" > "$FAKE_AWS/parameters/$(printf '%s' "$name" | tr '/' '_')"
    echo '{"Version": 1}'
    ;;
  *) echo "unexpected call $service $operation" >&2; exit 2 ;;
esac
STAND_IN
chmod +x "$bin_directory/aws"

export PATH="$bin_directory:$PATH"
export FAKE_AWS="$scratch/aws"
mkdir -p "$FAKE_AWS"
export DATA_BUCKET="synthetic-project-bucket"
export CODE_RUN_PARAMETER="/synthetic_project/current_code_run"
sha="0123456789abcdef0123456789abcdef01234567"
other_sha="fedcba9876543210fedcba9876543210fedcba98"
stamp="20261111T120000Z"
run_id="${stamp}-01234567"
archive="$scratch/code.tar.gz"
digest_file="$scratch/code.sha256"
printf 'synthetic-code-archive-bytes' > "$archive"
printf '%s\n' "$(sha256sum "$archive" | cut -d' ' -f1)" > "$digest_file"

stage() {
  bash "$script" "$archive" "$digest_file" "$stamp" "$sha" "$sha"
}

reset() {
  rm -r "$FAKE_AWS"
  mkdir -p "$FAKE_AWS"
  : > "$FAKE_AWS/calls.log"
}

stage_then_names_the_run() {
  reset
  local output
  output="$(stage)"
  [[ "$output" == "code_run=$run_id" ]] \
    && [[ -f "$FAKE_AWS/objects/$DATA_BUCKET/lakehouse/run_stage/$run_id/inputs/code.tar.gz" ]] \
    && [[ -f "$FAKE_AWS/objects/$DATA_BUCKET/lakehouse/run_stage/$run_id/inputs/code.sha256" ]] \
    && [[ "$(cat "$FAKE_AWS/parameters/_synthetic_project_current_code_run")" == "$run_id" ]]
}
check "a push stages the archive and its digest under the run and names the run in the parameter" stage_then_names_the_run

order_is_archive_digest_parameter() {
  reset
  stage > /dev/null
  [[ "$(grep -n "" "$FAKE_AWS/calls.log" | sed -e 's/ --.*//; s/^[0-9]*://' | tr '\n' ',')" == "s3api put-object,s3api put-object,ssm put-parameter," ]] \
    && [[ "$(grep -n "code.tar.gz" "$FAKE_AWS/calls.log" | cut -d: -f1)" == "1" ]] \
    && [[ "$(grep -n "code.sha256" "$FAKE_AWS/calls.log" | cut -d: -f1)" == "2" ]]
}
check "the archive goes first, then its digest, then the parameter, so nothing names a missing archive" order_is_archive_digest_parameter

every_put_is_create_once() {
  reset
  stage > /dev/null
  [[ "$(grep -c "put-object" "$FAKE_AWS/calls.log")" == "2" ]] && [[ "$(grep "put-object" "$FAKE_AWS/calls.log" | grep -c -e "--if-none-match \*")" == "2" ]]
}
check "each object put carries the create-once condition" every_put_is_create_once

only_puts_are_called() {
  reset
  stage > /dev/null
  ! grep -v -e "^s3api put-object " -e "^ssm put-parameter " "$FAKE_AWS/calls.log" | grep -q .
}
check "the script calls nothing but put-object and put-parameter" only_puts_are_called

the_same_push_again_changes_nothing() {
  reset
  stage > /dev/null
  cp "$FAKE_AWS/objects/$DATA_BUCKET/lakehouse/run_stage/$run_id/inputs/code.tar.gz" "$scratch/first.tar.gz"
  local output
  output="$(stage)"
  [[ "$output" == "code_run=$run_id" ]] \
    && cmp -s "$scratch/first.tar.gz" "$FAKE_AWS/objects/$DATA_BUCKET/lakehouse/run_stage/$run_id/inputs/code.tar.gz" \
    && [[ "$(cat "$FAKE_AWS/parameters/_synthetic_project_current_code_run")" == "$run_id" ]]
}
check "the same commit again finds its files staged, replaces nothing and names the same run" the_same_push_again_changes_nothing

a_later_commit_gets_its_own_run() {
  reset
  stage > /dev/null
  local later="20261112T090000Z"
  bash "$script" "$archive" "$digest_file" "$later" "$other_sha" "$other_sha" > /dev/null
  [[ "$(cat "$FAKE_AWS/parameters/_synthetic_project_current_code_run")" == "${later}-fedcba98" ]] \
    && [[ -f "$FAKE_AWS/objects/$DATA_BUCKET/lakehouse/run_stage/$run_id/inputs/code.tar.gz" ]]
}
check "a later commit stages a new run, names it and leaves the earlier run's files" a_later_commit_gets_its_own_run

refused_with_no_call() {
  reset
  if "$@" > /dev/null 2> "$scratch/error.txt"; then
    return 1
  fi
  [[ ! -s "$FAKE_AWS/calls.log" ]]
}
wrong_digest() {
  printf '%s\n' "$(printf 'another archive' | sha256sum | cut -d' ' -f1)" > "$scratch/wrong.sha256"
  refused_with_no_call bash "$script" "$archive" "$scratch/wrong.sha256" "$stamp" "$sha"
}
check "a digest file that is not the archive's digest is refused before any call" wrong_digest
check "a malformed commit is refused before any call" refused_with_no_call bash "$script" "$archive" "$digest_file" "$stamp" "not-a-commit" "$sha"
check "a malformed stamp is refused before any call" refused_with_no_call bash "$script" "$archive" "$digest_file" "yesterday" "$sha" "$sha"
check "a missing archive is refused before any call" refused_with_no_call bash "$script" "$scratch/missing.tar.gz" "$digest_file" "$stamp" "$sha" "$sha"
check "a bucket that is not a bucket name is refused before any call" refused_with_no_call env DATA_BUCKET="Not A Bucket" bash "$script" "$archive" "$digest_file" "$stamp" "$sha" "$sha" "$sha"
check "a parameter name with a pattern is refused before any call" refused_with_no_call env CODE_RUN_PARAMETER="/synthetic_project/*" bash "$script" "$archive" "$digest_file" "$stamp" "$sha" "$sha" "$sha"

# Ordering: the parameter moves only to the commit that is the head of main when it is written. A rerun of an older push finds a newer head, stages its files
# (they are create-once and harmless) and leaves the parameter alone, so a delayed or repeated run can never name older code.
an_older_rerun_leaves_the_newer_run_named() {
  reset
  local later="20261112T090000Z"
  stage > /dev/null
  bash "$script" "$archive" "$digest_file" "$later" "$other_sha" "$other_sha" > /dev/null
  : > "$FAKE_AWS/calls.log"
  local output
  output="$(bash "$script" "$archive" "$digest_file" "$stamp" "$sha" "$other_sha")"
  [[ "$(cat "$FAKE_AWS/parameters/_synthetic_project_current_code_run")" == "${later}-fedcba98" ]] \
    && [[ "$output" == "code_run_not_named=$run_id" ]] \
    && ! grep -q "ssm put-parameter" "$FAKE_AWS/calls.log"
}
check "a rerun of an older push leaves the newer commit's run named" an_older_rerun_leaves_the_newer_run_named

a_push_that_is_not_the_head_stages_but_names_nothing() {
  reset
  local output
  output="$(bash "$script" "$archive" "$digest_file" "$stamp" "$sha" "$other_sha")"
  [[ "$output" == "code_run_not_named=$run_id" ]] \
    && [[ -f "$FAKE_AWS/objects/$DATA_BUCKET/lakehouse/run_stage/$run_id/inputs/code.tar.gz" ]] \
    && [[ -f "$FAKE_AWS/objects/$DATA_BUCKET/lakehouse/run_stage/$run_id/inputs/code.sha256" ]] \
    && [[ ! -d "$FAKE_AWS/parameters" ]]
}
check "a push whose commit is no longer the head of main stages its files and names no run" a_push_that_is_not_the_head_stages_but_names_nothing

check "a malformed head of main is refused before any call" refused_with_no_call bash "$script" "$archive" "$digest_file" "$stamp" "$sha" "main"
check "a missing head of main is refused before any call" refused_with_no_call bash "$script" "$archive" "$digest_file" "$stamp" "$sha"

a_failed_archive_put_names_no_run() {
  reset
  if FAKE_FAIL_KEY="code.tar.gz" stage > /dev/null 2>&1; then
    return 1
  fi
  [[ ! -d "$FAKE_AWS/parameters" ]] && ! grep -q "code.sha256" "$FAKE_AWS/calls.log"
}
check "a failed archive put stops before the digest and the parameter" a_failed_archive_put_names_no_run

a_failed_digest_put_names_no_run() {
  reset
  if FAKE_FAIL_KEY="code.sha256" stage > /dev/null 2>&1; then
    return 1
  fi
  [[ ! -d "$FAKE_AWS/parameters" ]]
}
check "a failed digest put stops before the parameter" a_failed_digest_put_names_no_run

nothing_secret_is_printed() {
  reset
  stage > "$scratch/out.txt" 2>&1
  ! grep -q -e "$DATA_BUCKET" -e "synthetic-code-archive-bytes" "$scratch/out.txt"
}
check "the output names the run only: no bucket and no archive content" nothing_secret_is_printed

# The archive carries what Git LFS stores (uv.lock, and the baseline run registry once it is tracked). A checkout without lfs: true leaves pointer files in the clone,
# and the pushed commit's archive would then hold pointer files. The workflow file is the contract, so these cases read it.
workflow=".github/workflows/stage_code.yml"
checkout_step() { awk '/uses: actions\/checkout@/ { found = 1 } found && /^      - name:/ && seen { exit } found { seen = 1; print }' "$workflow"; }

the_checkout_fetches_lfs_files() { checkout_step | grep -q '^          lfs: true$'; }
check "the workflow checks out the pushed commit with its Git LFS files" the_checkout_fetches_lfs_files

the_checkout_is_the_first_step() { [[ "$(grep -m1 -E '^      - name:' "$workflow")" == *"Checkout"* ]]; }
check "the checkout runs before any step that reads the repository" the_checkout_is_the_first_step

if [[ "$failures" -ne 0 ]]; then
  echo "$failures stage_code case(s) failed." >&2
  exit 1
fi
echo "All stage_code cases passed."
