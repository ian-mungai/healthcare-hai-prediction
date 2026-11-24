#!/usr/bin/env bash
# State protection, step 1 of the Terraform state move: turn on the state prefix of the project bucket (project profile, storage stack).
#
# Nothing in this script has run against AWS. The apply is the owner's: the script shows the plan and applies it only after you type apply. The apply adds the lifecycle rules
# expire_replaced_state_versions and expire_saved_plans and the state statements of the bucket policy (infra/main.tf): only the project user may touch terraform_state/ for the
# storage, platform, services, jobs and alerts stacks, only the administrator you name below may touch terraform_state/iam/, and every other principal is denied. Versioning of
# the bucket is already on. The saved plan must hold exactly two changes, an update in place of aws_s3_bucket_lifecycle_configuration.data and of aws_s3_bucket_policy.tls: step 1
# reads the plan's JSON and stops before the question on any other change, any other action (a destroy or a replacement) or a missing update. The apply keeps the administrator you
# name as the approved state input of the storage stack (plans/state_migration/state_protection.json), so every later storage plan and apply through the wrapper carries it and a
# plan that would remove the state grants is refused. The last command reads the live bucket and stops unless the protection is there: run the state moves (state_migration.sh,
# state_migration_iam.sh) after it.
# Run it from any folder of the main checkout, in one shell. It makes one new folder under the ignored plans/ folder that holds every file it saves (RECORD_DIR). A long plan goes
# to a file in that folder and only its change lines reach the terminal. Rollback: docs/terraform_state_migration.md.
set -euo pipefail
trap 'echo "Stopped at line $LINENO. Read the output above, fix the cause and run the script again: every step changes nothing it already changed." >&2' ERR
cd "$(dirname "$0")/../../.."

confirm() {
  printf '%s Type apply to continue: ' "$1"
  read -r CONFIRMATION
  [ "$CONFIRMATION" = apply ] || { echo "Not applied." >&2; exit 1; }
}

# Step 0: the settings that every later step reads.
RECORD_DIR="plans/state_prefix_$(date -u +%Y%m%dT%H%M%SZ)"
mkdir -p "$RECORD_DIR"
printf 'State administrator, as role/NAME or user/NAME (the principal that applies the IAM stack): '
read -r STATE_ADMIN
printf '%s %s\n' "$RECORD_DIR" "$STATE_ADMIN"

# Step 1: plan the storage stack with the state prefix on, then read the saved plan itself: exactly the two updates in place below and nothing else. The log has no color codes.
STORAGE_PLAN="$PWD/$RECORD_DIR/storage.tfplan"
bash scripts/infrastructure/terraform.sh --state-admin "$STATE_ADMIN" plan -no-color -out "$STORAGE_PLAN" > "$RECORD_DIR/storage_plan.txt"
grep -E '^  # |^Plan:' "$RECORD_DIR/storage_plan.txt"
.venv/bin/python -m scripts.infrastructure.state_migration approve-plan --stack storage --plan "$STORAGE_PLAN" \
  --update aws_s3_bucket_lifecycle_configuration.data --update aws_s3_bucket_policy.tls

# Step 2: apply exactly that plan. The wrapper refuses a plan that does not match .env and the option.
confirm "Apply exactly the storage plan above?"
bash scripts/infrastructure/terraform.sh --state-admin "$STATE_ADMIN" apply "$STORAGE_PLAN" > "$RECORD_DIR/storage_apply.txt"
grep -E 'Apply complete|Error' "$RECORD_DIR/storage_apply.txt"

# Step 3: read the live bucket: the identity, the version history, the state statements of the policy and the lifecycle rule must all be there.
.venv/bin/python -m scripts.infrastructure.state_migration check --stack storage
