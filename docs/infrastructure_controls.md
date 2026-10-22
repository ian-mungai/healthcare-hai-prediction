---
title: Infrastructure Controls
description: Environments, bucket lifecycle rules, the lifecycle permission and the deferred storage controls.
last_updated: 2026-10-13
---

# Infrastructure Controls

**Environments.** Both stacks tag resources with `Environment = dev`. A `prod`
environment and an `infra/modules/` layout are deferred until the next
infrastructure component is added.

**Lifecycle rules.** The bucket-wide rule, applied and read back from S3, aborts incomplete multipart uploads seven days after
initiation, with no expiration or transition of completed objects, historical
versions or delete markers. One exception covers only `lakehouse/`, where
Iceberg replaces and deletes its own table files: replaced versions there expire
30 days after they are replaced and orphaned delete markers are removed; current
lakehouse files and everything outside `lakehouse/` never expire. Local mock-plan tests, source-shape checks and live
configuration readback pass. The seven-day scheduler itself has not been
observed in a timed test and no deliberately incomplete upload was created for
verification.

The project's IAM policy grants only bucket-scoped
`s3:PutLifecycleConfiguration` for lifecycle changes; the storage stack
holds only the rules above. IAM administration is used only for policy updates;
storage deployment uses the project profile. This permission can change the
bucket's entire lifecycle configuration, so future changes still require full
plan review and preservation of existing rules. Private plans and live
verification evidence remain untracked under
`data/infrastructure/lifecycle_deployment/`.

**Pipeline roles.** `infra/iam/` also defines two IAM roles for pipeline jobs, a lakehouse reader and a lakehouse writer. Only the project's
own IAM user can assume them, through a policy that allows `sts:AssumeRole` on exactly these two roles. The reader reads the bronze, silver and gold
prefixes of `lakehouse/`. The writer holds the object actions of the lakehouse policy on `lakehouse/silver/`, `lakehouse/silver_e2e/`,
`lakehouse/gold/`, `lakehouse/gold_e2e/` and `lakehouse/catalog_backups/` only. A session lasts at most 7200 seconds. The launcher asks for a
session per job (`docs/lakehouse_usage.md`, Pipeline AWS access), so no long-lived key reaches a container.

**Single-copy content folders.** The project's S3 policy has four statements for `bronze_objects/sha256/` and
`bronze_evidence/sha256/`, the folders that single-copy storage uses. One reads current objects and exact versions
(`s3:GetObject` and `s3:GetObjectVersion`). One allows `s3:PutObject` only when the request carries the `If-None-Match`
header and only for a key whose leaf after `sha256/` has exactly 64 characters (64 `?` wildcards), so an empty, a shorter
and a longer leaf are not created. Deletion, tagging and multipart actions are not granted there. One denies writes,
tagging and multipart actions in these two folders for the key shapes that the older wildcard grants (`*/references/*`,
`*/data/*`, `*/audit/*`, `*/manifests/*` and `*/*/datasets/*`) match. One denies `s3:PutObject` for every key under
`sha256/` that holds a percent sign, so a leaf with a literal encoded separator is not created. IAM resource patterns
have only `*` and `?`, so no pattern limits the leaf to hexadecimal characters. The length-only pattern is the
strongest form that IAM can express, so the policy does not prove that the 64 characters are hexadecimal digits. The
only writer is the store step. It computes the leaf as the SHA-256 of the file. The index loader checks the leaf: it
refuses every key that is not exactly `sha256/<checksum>`, where the checksum is the SHA-256 recorded for that object.
The plan tests evaluate the rendered policy for nested keys, empty, short, long and percent leaves and the older
wildcard shapes in both folders. They fail when a write is allowed there. They also fail when the exact key loses its
conditional create or when a statement is missing or wider than these folders. The owner reviews and applies the policy
with the steps in [Switching on single-copy storage](lakehouse_usage.md#switching-on-single-copy-storage). The condition
is untested against real AWS.

**Deferred controls.** Apply the project's engineering requirements when
development reaches the relevant stage. Future requirements guide later work;
deferral does not mean a control is implemented. The following
acquisition-stage deferrals apply to `aws_s3_bucket.data` only. Checkov
and trivy evidence retain each exception, resource and reason.

| Deferred control | Reason | Review trigger |
| --- | --- | --- |
| Event notifications (`CKV2_AWS_62`) | No event consumer exists. | Implementing an event consumer. |
| Cross-region replication (`CKV_AWS_144`) | Recovery objectives and a destination are not established. | Recovery design, before production. |
| KMS (`CKV_AWS_145`, trivy `AWS-0132`) | Acquisition uses SSE-S3/AES256. | Coordinated ingestion and IAM migration, before production. |
| Access logging (`CKV_AWS_18`, trivy `AWS-0089`) | Acquisition-stage deferral of a separate logging destination; the request-audit gap is accepted for this stage. | Before additional user or service access, production or new sensitive-data use. |

Acquisition
receipts establish pipeline provenance, not an independent record of every
bucket request. Review CloudTrail data events alongside server access logging
when a review trigger is reached. Server access logs are best-effort, not a
complete audit trail ([AWS documentation](https://docs.aws.amazon.com/AmazonS3/latest/userguide/ServerLogs.html)).

Existing AES256 encryption, private access, versioning and destruction
protection are preserved. Synthetic E2E checks verify that the four exceptions
do not suppress other resources or the versioning control. Passing static
checks with these exceptions does not establish that deferred controls exist.
No AWS changes implementing these four deferred controls have been applied.
