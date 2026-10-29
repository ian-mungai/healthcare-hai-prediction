# AWS bronze writer task definition

This optional module defines the bronze writer task and its dispatch contract. Task registration requires `register_definition = true`.

## Reader policy

The `aws_iam_role_policy.job_reader_e2e` resource in `infra/iam/job_writer.tf` attaches `infra/iam/owner_policies/job_reader_e2e.json` to `job_read_task` when `job_writer_e2e_prepared` is true.

The policy permits `s3:GetObject` on the three `silver_e2e`, `gold_e2e` and `features_e2e` prefixes and the four bronze test prefixes below. It permits `s3:ListBucket` with those seven prefixes as conditions. Completion checks also use `s3:GetObjectVersion` and `s3:ListBucketVersions` on only the four bronze test prefixes, so they can identify and read metadata object versions.

```text
lakehouse/bronze_e2e/
lakehouse/bronze_dictionary_e2e/
lakehouse/smoke_check_e2e/
lakehouse/duckdb_view_check_e2e/
```

Canonical reads depend on the reader role's existing policy. Applying or attaching this qualification policy requires separate infrastructure approval.

## Module checks

Run from the repository root with Terraform 1.16 and the pinned AWS provider available.

```bash
terraform -chdir=infra/modules/bronze_writer_job init -backend=false -input=false -lockfile=readonly -no-color
terraform -chdir=infra/modules/bronze_writer_job validate -no-color
terraform -chdir=infra/modules/bronze_writer_job test -no-color
```

The committed lock file matches the IAM stack's AWS provider 6.66.0 selection and checksums. The module tests use a mock AWS provider. The Terraform CI group copies tracked module files into its scratch folder and runs these commands plus TFLint, Checkov and Trivy static checks. Infrastructure paths select that group.
