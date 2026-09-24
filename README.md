# healthcare-hai-prediction

Hospital-acquired infection prediction research for a Data Engineer. This fresh
repository builds on the capstone concept, not its implementation. Same-period
prediction is primary; forecasting is a later extension. Acquisition and S3
storage precede comprehensive schema review and tutorial drafting. Availability
does not establish clinical validity or model eligibility.

## Repository map

- `infra/`: private, versioned S3 configuration; `infra/iam/`: scoped service policies.
- `scripts/infrastructure/`: dotenv rendering, identity guards and configuration E2E.
- `scripts/quality/`: pinned tool installation, secret scanning and Terraform analysis.
- `scripts/run_ci.sh`: the shared local and GitHub CI entry point.
- Local acquisition code, configuration, data and audit evidence remain ignored by request.

## Data handling

Published hospital-level datasets are public reference data subject to their
source terms. Public availability does not remove personal-data safeguards.
Any specifically approved source containing personal names or contact details
is classified as Confidential and requires a documented ingestion exception.

For the approved HCAI exception, the untouched download stays local with
owner-only access. Only the privacy-filtered derivative and non-personal
references or provenance may enter the project's private S3 storage. The
derivative remains under privacy and modeling review; redaction does not
establish formal anonymization or clinical validity. Raw retention duration
remains unresolved. FileVault was verified on Sep 24 2026 (UTC); that host check
does not establish compliance or resolve retention requirements.

Tests use generic synthetic data. Real personal values must never appear in
fixtures, logs, tutorials, published artifacts or commits. Acquisition inputs,
operational evidence and the detailed exception record remain untracked.

The explicitly approved live S3 verification tool is the sole opt-in exception
to synthetic-only verification. Normal CI does not run it or contact AWS.
Deployment configuration is Internal; credentials are Restricted and never
belong in tracked files. Classification stays in metadata and this README.
Per the project decision on Sep 24 2026 (UTC), do not add an AWS
`DataClassification` tag. This existing project exception remains unchanged;
bundle v1.2 resource-level classification guidance requires a separate review
before changing tags.

## Quality checks

Run from this repository's root with its existing Python 3.12 `.venv`.
Dependencies are shared across development and production in `requirements.txt`.
Gitleaks and TFLint are installed only in ignored `.tools/`; versions and
publisher SHA-256 values are pinned in `config/quality_tools.json`.

```sh
.venv/bin/python -m pip install -r requirements.txt
.venv/bin/python scripts/quality/install_tools.py
PRE_COMMIT_HOME="$PWD/.tools/pre_commit_cache" .venv/bin/python -m pre_commit install
env PYTHON_BIN="$PWD/.venv/bin/python" \
  TF_PLUGIN_CACHE_DIR="$PWD/infra/.terraform/providers" \
  /bin/bash scripts/run_ci.sh
```

The native installer supports macOS ARM64 and Linux AMD64. Repeating a verified
installation preserves bytes and modification times; an unexpected existing
binary is rejected rather than overwritten. `--archive-directory` supports
offline installation from the exact pinned release filenames. Do not replace
the shared system tools to satisfy this project.

CI checks Ruff formatting and security rules, MyPy, outgoing-source secret
scanning, configuration and quality E2E, retained regression safeguards,
Terraform mock tests, TFLint and Checkov. Pre-commit scans the actual staged
index, including changes hidden by a clean working copy, before local CI.
The source scan excludes ignored acquisition data; neither scan sends files to
an external service. Secret values and matching source fragments are omitted
from scanner diagnostics.

Each CI run retains configuration evidence under `data/e2e/configuration/` and
quality evidence under `data/e2e/quality/`. E2E reports include source hashes,
expected and observed process outcomes, repeat commands and tested limits.
GitHub Actions retains these synthetic artifacts for 14 days. Local TFLint
needs permission to start its bundled plugin; a sandbox denial is a blocked
check, not a passing result.

These checks are not deployment or clinical validation. The ignored acquisition
implementation requires its separate synthetic verification before local use;
a clean GitHub checkout cannot exercise files intentionally excluded from Git.

## Pending design reviews

The approved lifecycle rule was applied and read back from S3 on Sep 24 2026 (UTC).
It aborts incomplete multipart uploads seven days after initiation, with no
expiration or transition of completed objects, historical versions or delete
markers. Local mock-plan tests, source-shape checks and live configuration
readback pass. The seven-day scheduler itself has not been observed in a timed
test, and no deliberately incomplete upload was created for verification.

Pre-deployment inspection confirmed no existing lifecycle configuration. The
separately approved IAM plan added only bucket-scoped
`s3:PutLifecycleConfiguration`; the approved storage plan created only the
cleanup rule. IAM administration was used only for the policy update, while
storage deployment used the project profile. This permission can change the
bucket's entire lifecycle configuration, so future changes still require full
plan review and preservation of existing rules. Private plans and live
verification evidence remain untracked under
`data/infrastructure/lifecycle_deployment/`.

Apply bundle requirements when development reaches the relevant stage. Future
requirements guide later work; deferral does not mean a control is implemented.
The following acquisition-stage deferrals are approved for `aws_s3_bucket.data`
only. Checkov evidence retains each skipped check, resource and reason.

| Deferred control | Reason | Review trigger |
| --- | --- | --- |
| Event notifications (`CKV2_AWS_62`) | No event consumer exists yet. | Implementing an event consumer. |
| Cross-region replication (`CKV_AWS_144`) | Recovery objectives and a destination are not established. | Recovery design, before production. |
| KMS (`CKV_AWS_145`) | Acquisition currently uses SSE-S3/AES256. | Coordinated ingestion and IAM migration, before production. |
| Access logging (`CKV_AWS_18`) | Approved acquisition-stage deferral of a separate logging destination; request-audit gap accepted for this stage. | Before additional user or service access, production or newly approved sensitive-data use. |

Access logging was explicitly deferred on Sep 24 2026 (UTC). Acquisition
receipts establish pipeline provenance, not an independent record of every
bucket request. Review CloudTrail data events alongside server access logging
when a review trigger is reached. Server access logs are best-effort, not a
complete audit trail ([AWS documentation](https://docs.aws.amazon.com/AmazonS3/latest/userguide/ServerLogs.html)).

Existing AES256 encryption, private access, versioning and destruction
protection are preserved. Synthetic E2E checks verify that the four exceptions
do not suppress other resources or the versioning control. Passing static
checks with these exceptions does not establish that deferred controls exist.
No AWS changes implementing these four deferred controls have been applied.

Environment naming and dev/prod separation require a decision before changing
tags or resource layout. Remote state protection remains deferred until the
end of the project as requested. Raw personal-data retention remains a separate
unresolved control. No tutorial files are added to this repository.
