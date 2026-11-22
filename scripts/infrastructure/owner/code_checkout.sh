#!/usr/bin/env bash
# Code checkout on the host: put the reviewed code of one run on the host as /data/repo, the checkout that Airflow and the OpenMetadata bootstrap mount.
#
# Nothing in this script has run against AWS. Each apply is the owner's: the script shows every plan and applies it only after you type apply. It is for an existing
# installation: the code one-shot task definition (the family whose name ends in _code_fetch) is already registered, because host_session start --code-run refuses before
# the host turns on when it is not. A fresh stack, where it is not registered yet, needs a first registration that this script does not cover.
# Run it from any folder of the main checkout, in one shell: every step reads the variables that the settings step sets. It makes the one new folder under the ignored plans/
# folder that holds every file it saves (RECORD_DIR), reads the project profile and the region from .env with the project's own dotenv parser, reads the data bucket name
# from the storage stack and asks for the administrator profile. Read the printed values and stop if one is empty. A failed services apply keeps its apply lock: run
# scripts/infrastructure/owner/code_checkout_resolve_apply_lock.sh after you checked the apply and the services.
set -euo pipefail
trap 'echo "Stopped at line $LINENO. Read the output above, fix the cause and run the script again: every step changes nothing it already changed. A failed services apply keeps its lock: see code_checkout_resolve_apply_lock.sh." >&2' ERR
cd "$(dirname "$0")/../../.."

confirm() {
  printf '%s Type apply to continue: ' "$1"
  read -r CONFIRMATION
  [ "$CONFIRMATION" = apply ] || { echo "Not applied." >&2; exit 1; }
}

# Step 0: the settings that every later step reads.
RECORD_DIR="plans/code_checkout_$(date -u +%Y%m%dT%H%M%SZ)"
mkdir -p "$RECORD_DIR"
PROJECT_PROFILE="$(.venv/bin/python -m scripts.infrastructure.project_setting aws_profile)"
REGION="$(.venv/bin/python -m scripts.infrastructure.project_setting aws_region)"
DATA_BUCKET="$(terraform -chdir=infra output -raw bucket_name)"
printf 'Administrator profile: '
read -r ADMIN_PROFILE
printf '%s %s %s %s %s\n' "$RECORD_DIR" "$PROJECT_PROFILE" "$REGION" "$DATA_BUCKET" "$ADMIN_PROFILE"

# Step 1: infra/iam (administrator profile). Set host_code_fetch_prepared to true in config/aws/iam_preparation.json first. The committed file holds flags and non-environment
# values only: the account, the topic ARN and the secret ARNs are built from .env when the inputs are rendered. The plan must add the role host_code_fetch_task and its one
# policy code_archive_read, which allows s3:GetObject on lakehouse/run_stage/*/inputs/code.tar.gz and nothing else. Stop on any other change. Then the project user's policy: the
# project user can pass only the roles that its policy lists, and the services apply of step 5 registers the code one-shot task definition with this role, so the policy file
# now lists host_code_fetch_task. The plan must change the iam policy to add the role. Stop and ask on any other change you did not expect.
IAM_PLAN="$PWD/$RECORD_DIR/iam.tfplan"
bash scripts/infrastructure/terraform.sh --iam --admin-profile "$ADMIN_PROFILE" plan -out "$IAM_PLAN"
confirm "Apply exactly the IAM plan above?"
bash scripts/infrastructure/terraform.sh --iam --admin-profile "$ADMIN_PROFILE" apply "$IAM_PLAN"
terraform -chdir=infra/iam output -json host_code_fetch_role > "$RECORD_DIR/host_code_fetch_role.json"
cat "$RECORD_DIR/host_code_fetch_role.json"
.venv/bin/python -m infra.iam.scripts.manage_policies plan --admin-profile "$ADMIN_PROFILE" | tee "$RECORD_DIR/policy_plan.txt"
confirm "Apply exactly the policy plan above, with the Plan digest it printed?"
PLAN_DIGEST="$(sed -n 's/^Plan digest: //p' "$RECORD_DIR/policy_plan.txt")"
CONFIRM_IAM_POLICIES=apply-healthcare-hai-prediction-policies .venv/bin/python -m infra.iam.scripts.manage_policies apply --admin-profile "$ADMIN_PROFILE" --digest "$PLAN_DIGEST"

# Step 2: images (project profile). The code one-shot runs the training image, which carries the fetch script of the training task. Its digest must be in the services image
# digests. When it is not, the first command prints the build and push commands and stops the script: run them, record the digests as the plan says, then run this script again.
# The second command asks ECR once (read only, with a time limit) whether it still holds the recorded digest: a push from another checkout can make the lifecycle policy
# expire it. When it does not, the command prints the plan again and stops; when ECR cannot be asked, it stops and changes nothing.
terraform -chdir=infra/platform output -json ecr_repository_urls > "$RECORD_DIR/repository_urls.json"
.venv/bin/python -m scripts.infrastructure.push_images require --repository-urls "$RECORD_DIR/repository_urls.json" --profile "$PROJECT_PROFILE" --region "$REGION" --only training
.venv/bin/python -m scripts.infrastructure.image_digest_check --repository-urls "$RECORD_DIR/repository_urls.json" --profile "$PROJECT_PROFILE" --region "$REGION" --only training

# Step 3: stage the code (project profile). The first command makes a new run ID. The archive comes from scripts.lakehouse.code_archive, which adds the verified dbt packages.
# The code digest is the content of code.sha256.
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-$(openssl rand -hex 4)"
CODE_DIR="/tmp/code_$RUN_ID"
.venv/bin/python -m scripts.lakehouse.code_archive build --output "$CODE_DIR"
aws s3 cp "$CODE_DIR/code.tar.gz" "s3://$DATA_BUCKET/lakehouse/run_stage/$RUN_ID/inputs/code.tar.gz" --profile "$PROJECT_PROFILE" --region "$REGION"
aws s3 cp "$CODE_DIR/code.sha256" "s3://$DATA_BUCKET/lakehouse/run_stage/$RUN_ID/inputs/code.sha256" --profile "$PROJECT_PROFILE" --region "$REGION"
CODE_SHA256="$(cat "$CODE_DIR/code.sha256")"
printf '%s %s\n' "$RUN_ID" "$CODE_SHA256"

# Step 4: start the host with that code (project profile). Apply the services stack only while the host runs: with no instance, the ECS deployment circuit breaker rolls every
# service back. After host_prepare the code one-shot downloads the archive, checks it against the digest, unpacks it into a private folder, renames that folder to a folder of
# /data/repo_releases/ named by the code digest only when it holds every file of the archive and only then points /data/repo at it. A second code task then checks that
# /data/repo shows that release and that the release holds every file of its archive. When a code task fails, a third task makes /data/repo show a verified checkout again, and
# the start names what it shows. The services start after it, so they mount the new checkout. A refused or failed fetch keeps the previous checkout and stops the start before
# any service. Repeating the start with the same code changes nothing. A later start without --code-run keeps the checkout and reuses its code: the install writes the run ID
# beside the digest (the file run_id in the folder of that digest under /data/repo_releases/) and the start reads it with a code task, so a start fails on a host that has no
# checkout or whose checkout has no run ID record (a checkout that an older installer made needs one start with --code-run first). After the services are stable the start also
# reloads the online store, one launch of the features task for each online model set (m1, m2 and m4), because the store keeps no snapshot. A failed reload stops the host like
# any other failed step. The start refuses before the host turns on when the jobs stack has no features host task.
.venv/bin/python -m scripts.infrastructure.host_session start --code-run "$RUN_ID" --code-sha256 "$CODE_SHA256"

# Step 5: infra/services (project profile), while the host runs. Write the reviewed file services_code.json in the record folder from the complete settings that the last services
# plan or apply rendered on this machine, and bind it to its digest. The services renderer treats this file as the whole set of nondefault services inputs, so a file that holds
# only the code one-shot role and the bucket would reset every other setting, such as mlflow_enabled, to its default. The tool keeps every setting that a services preparation
# file supports, sets code_fetch_task_role_arn to the role of step 1 and code_bucket_name to the data bucket, checks the result against the preparation schema and prints setting
# names only. Read the printed names and then the file: kept must list every nondefault services setting that you run today. The tool stops and names a setting that the services
# stack declares and no preparation file can carry. Stop and ask in that case. The plan must change at most the code one-shot task definition (the family whose name ends in
# _code_fetch) and nothing else. A plan that shows another change means a setting was lost from the file: do not apply it. The shasum command puts the complete file's SHA-256
# in SERVICES_INPUTS_SHA256, and the plan and the apply both use it.
printf '%s\n' "$DATA_BUCKET" > "$RECORD_DIR/data_bucket.txt"
.venv/bin/python -m scripts.infrastructure.services_code_inputs --existing infra/services/deployment.auto.tfvars.json --role-file "$RECORD_DIR/host_code_fetch_role.json" --bucket-file "$RECORD_DIR/data_bucket.txt" --output "$RECORD_DIR/services_code.json"
cat "$RECORD_DIR/services_code.json"
SERVICES_INPUTS_SHA256="$(shasum -a 256 "$RECORD_DIR/services_code.json" | cut -d' ' -f1)"
printf '%s\n' "$SERVICES_INPUTS_SHA256"
SERVICES_PLAN="$PWD/$RECORD_DIR/services.tfplan"
bash scripts/infrastructure/terraform.sh --services --preparation-inputs "$RECORD_DIR/services_code.json" --preparation-sha256 "$SERVICES_INPUTS_SHA256" plan -out "$SERVICES_PLAN"
confirm "Apply exactly the services plan above?"
# The services apply first takes the apply lock, so no other services apply starts, then sets the launch hold before it changes anything. While the lock exists or the hold is set,
# no host job launches. It clears the hold and deletes its own lock only after the ECS services reach steady state. If the apply, the hold write or that wait fails, the lock (and
# the hold, when it was set) stay: run code_checkout_resolve_apply_lock.sh. A new code one-shot revision from this apply does not change /data/repo now: the checkout of step 4
# stays, and the next host start runs the new revision. To use it now, run step 4 again with the same run ID.
bash scripts/infrastructure/terraform.sh --services --preparation-inputs "$RECORD_DIR/services_code.json" --preparation-sha256 "$SERVICES_INPUTS_SHA256" apply "$SERVICES_PLAN"

# Step 6: to move to newer code, run this script again: it makes a new run ID. A running Airflow keeps the checkout it started with until the next start. Stop the host when the
# session ends with: .venv/bin/python -m scripts.infrastructure.host_session stop
echo "Code checkout done for run $RUN_ID."
