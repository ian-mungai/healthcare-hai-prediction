---
title: Terraform state migration
description: How the state of the six stacks moves into the project bucket, what protects it, the owner steps and rollback.
last_updated: 2026-11-24
---

# Terraform state migration

Each stack kept its Terraform state in a local `terraform.tfstate` file. The migration copies each file once into the project bucket, checks the copy, switches the stack to the S3 backend and refuses a second run. The OIDC plan role stays off until all six stacks are moved and the owner turns it on in its own change.

## Design

- **Where:** the project bucket, key `terraform_state/<stack>/terraform.tfstate`, for the stacks `storage` (the `infra` root), `iam`, `platform`, `services`, `jobs` and `alerts`. Workspaces would use `terraform_state/<stack>/workspaces/`, so every state request stays under the prefix that the bucket policy and the plan role policy name.
- **Backend:** `infra/<stack>/backend.tf` holds the key, `use_lockfile = true` and `encrypt = true`. The lock is the S3 lock file `terraform.tfstate.tflock` beside the state (a conditional write), so there is no DynamoDB table. The bucket, the region and the profile come from `terraform init -backend-config` options, so no account value is committed. The migration script writes the file (`state_migration backend`) and the owner commits it after the move.
- **Protection** (`infra/main.tf`, switch `terraform_state_enabled`, applied by `owner/state_prefix_apply.sh`): versioning is on. Replaced state versions expire after 90 days and the current state never expires. The project user may read and write the state keys and the lock keys of the storage, platform, services, jobs and alerts stacks only. Every other principal is denied the whole prefix except the plan role and the administrator named in `terraform_state_admin_principals`. The project user and the plan role are denied `terraform_state/iam/`.
- **Profile split:** the iam root is applied with the administrator profile, so its state is moved with `--admin-profile` and no other profile touches `terraform_state/iam/`. The other five stacks use the project profile of `.env`. The script refuses an administrator profile for a project stack and refuses the iam stack without one. It also refuses an administrator profile that signs in as the project user.

## What the script proves

`scripts/infrastructure/state_migration.py` copies the state with a write that refuses an existing key, reads it back and compares the bytes and the fingerprint (serial, lineage, resource count and instance count). It then runs `terraform init -reconfigure` (Terraform sees equal states, moves nothing and empties the local file) and reads the state through the backend with `terraform state pull`. The fingerprint must equal the local one at each step. The plan before and the plan after must give the same change list. The run is recorded in `plans/state_migration/<stack>.json` (stage `copied`, then `done`). A stack with the record `done` or with a switched backend is refused. A remote key that holds other bytes than the local state is refused. A remote key with the same bytes is adopted, which resumes a run that stopped after the copy.

The E2E is `scripts/infrastructure/run_state_migration_e2e.py` (real Terraform and a loopback stand-in of the bucket). It covers the full run of all six stacks, the second-run refusal, an interrupted copy, a read-back mismatch, a lost write race, ten refused starting states, the profile split, a linked state file and the backend text.

## Owner order

1. `scripts/infrastructure/owner/state_prefix_apply.sh` (project profile): plans and applies the state protection to the storage stack, then reads the live bucket.
2. `scripts/infrastructure/owner/state_migration.sh <stack>` for `storage`, `alerts`, `jobs`, `platform` and `services` (project profile).
3. `scripts/infrastructure/owner/state_migration_iam.sh` (administrator profile).
4. Commit the six `backend.tf` files. The OIDC plan role stays off.

## After the move

Terraform empties the local `terraform.tfstate` and keeps the old content in `terraform.tfstate.backup`. When the file is a link, Terraform empties the file the link points to. The script's own copy of the state stays in `plans/state_migration/<stack>.local_state.json` (owner only). Two project tools still read a local file: `host_session` reads `infra/services/terraform.tfstate` and `alert_email` tests that `infra/alerts/terraform.tfstate` exists. They stop working for a moved stack until they read the state through `terraform state pull` or `terraform output`.

## Rollback

- **Stopped before `backend: switched`:** nothing to undo. The local state and the stack folder are as they were. A copy that was written stays in the bucket and the next run adopts it. If it holds other bytes, an administrator deletes `terraform_state/<stack>/terraform.tfstate` and the next run copies again.
- **A failed switch:** the script removes `backend.tf`, writes the original bytes back to the local file and runs `terraform init -reconfigure` on the local backend. It prints `backend: restored`.
- **After `migration: done`:** no apply may run. Then, in the stack folder: `terraform state pull > plans/state_migration/<stack>.pulled.json` (this holds any change applied after the move), remove `backend.tf`, copy the pulled file to `terraform.tfstate`, run `terraform init -reconfigure -input=false -lockfile=readonly` and a plan through `scripts/infrastructure/terraform.sh`, which must show the change list of the plan after. Delete `plans/state_migration/<stack>.json`. An administrator then deletes the remote key before a new migration, because the remote state differs from the old local copy once an apply has run.
- **A stuck lock:** the lock file is `terraform_state/<stack>/terraform.tfstate.tflock`. Use `terraform force-unlock` with the lock ID from the error or delete that key as the stack's own profile.
- **A lost local state:** S3 versioning keeps replaced versions for 90 days. The migration backup holds the pre-move state.
