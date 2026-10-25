# AWS bronze writer task definition

This optional module defines the bronze writer task and its dispatch contract. Task registration requires `register_definition = true`.

## Reader policy delta

The reader delta in `infra/iam/bronze_writer.tf` renders `infra/iam/owner_policies/bronze_reader_storage.json` for `job_read_task`. The owner approved `s3:GetObjectVersion` and `s3:ListBucketVersions` on Oct 24 2026 so completion checks can identify and read metadata object versions.

The delta permits `s3:GetObject` and `s3:GetObjectVersion` on objects below these four prefixes. It permits `s3:ListBucket` and `s3:ListBucketVersions` with those prefixes as conditions.

```text
lakehouse/bronze_e2e/
lakehouse/bronze_dictionary_e2e/
lakehouse/smoke_check_e2e/
lakehouse/duckdb_view_check_e2e/
```

Canonical reads depend on the reader role's existing policy. Applying or attaching the delta requires the separate infrastructure approval.

## Module checks

Run from the repository root with Terraform 1.16 and the pinned AWS provider available.

```bash
terraform -chdir=infra/modules/bronze_writer_job init -backend=false -input=false -lockfile=readonly -no-color
terraform -chdir=infra/modules/bronze_writer_job validate -no-color
terraform -chdir=infra/modules/bronze_writer_job test -no-color
```

The committed lock file matches the IAM stack's AWS provider 6.66.0 selection and checksums. The module tests use a mock AWS provider. The Terraform CI group copies tracked module files into its scratch folder and runs these commands plus TFLint, Checkov and Trivy static checks. Infrastructure paths select that group.
