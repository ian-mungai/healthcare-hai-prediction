#!/usr/bin/env bash
# State move of one stack into the project bucket (project profile): storage, jobs, platform, services or alerts. The IAM stack has its own script (state_migration_iam.sh).
#
# Nothing in this script has run against AWS. Run state_prefix_apply.sh first (the bucket must hold the state protection), then this script once for each stack, as
#     scripts/infrastructure/owner/state_migration.sh storage
# and the same for alerts, jobs, platform and services. Run it from the checkout whose stack folders hold the live terraform.tfstate files and start no apply, host session change
# or other Terraform command of the stack while it runs. When that file is a link (the run tree links to the files of the checkout that applied), Terraform empties the file the
# link points to: its content stays in the record backup below and in terraform.tfstate.backup beside the link. It makes one folder, plans/state_migration/, with every file it
# saves, and it prints status words only. Each step stops at the first problem and changes nothing it did not finish:
#   1. check: the identity of the project profile, the local state, the bucket's version history, policy and lifecycle rule, and the remote key and its lock (read only).
#   2. plan before: the plan of the stack as it is today. Its change list is kept. A stack with pending work shows it here, and the plan after must show the same list.
#   3. migrate: you type migrate. It backs up the local state to plans/state_migration/<stack>.local_state.json (owner only), copies it to
#      terraform_state/<stack>/terraform.tfstate with a write that refuses an existing key, reads it back and compares its bytes, writes infra/<stack>/backend.tf, runs
#      terraform init -reconfigure and reads the state through the backend. The serial, the lineage and the resource count must equal the local ones. Terraform empties the local
#      terraform.tfstate and keeps its content in terraform.tfstate.backup beside it. A second run for a stack that finished is refused.
#   4. plan after, then 5. verify: the plan runs through the backend (it takes and releases the lock file), and verify compares the state, the lock and the two change lists.
# Last, commit infra/<stack>/backend.tf: it holds no bucket, region or account value. Rollback: docs/terraform_state_migration.md.
set -euo pipefail
stopped() {
  if [ "$1" -eq 3 ]; then
    echo "Stopped at line $2: the stack is NOT back on its local state. Follow the rollback in docs/terraform_state_migration.md before any other step." >&2
  else
    echo "Stopped at line $2. Read the status line above, fix the cause and run the script again: a step that finished is not repeated, and a stack that finished is refused." >&2
  fi
}
trap 'stopped "$?" "$LINENO"' ERR
cd "$(dirname "$0")/../../.."

confirm() {
  printf '%s Type migrate to continue: ' "$1"
  read -r CONFIRMATION
  [ "$CONFIRMATION" = migrate ] || { echo "Not migrated." >&2; exit 1; }
}

# Step 0: the stack this run moves.
STACK="${1:?Name the stack: storage, alerts, jobs, platform or services}"
printf 'Stack: %s\n' "$STACK"

# Step 1: read only. Stop on any status that is not ok, present, absent or on. A run that stopped after the switch (its record says copied) prints resume here, and steps 2 and 3 then finish it from the record.
.venv/bin/python -m scripts.infrastructure.state_migration check --stack "$STACK"

# Step 2: the plan before the move. A run that stopped after the copy keeps its earlier plan (a new plan cannot run on a half-switched stack), and step 3 resumes from the record.
.venv/bin/python -m scripts.infrastructure.state_migration plan --phase before --stack "$STACK"

# Step 3: the move.
confirm "Copy the local state of $STACK into the bucket and switch its backend?"
.venv/bin/python -m scripts.infrastructure.state_migration migrate --stack "$STACK"

# Step 4: the plan after the move, through the backend.
.venv/bin/python -m scripts.infrastructure.state_migration plan --phase after --stack "$STACK"

# Step 5: verify the state, the lock and the two change lists. Then commit the backend file of this stack.
.venv/bin/python -m scripts.infrastructure.state_migration verify --stack "$STACK"
git status --short -- "infra/*/backend.tf" infra/backend.tf
