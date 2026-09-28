# healthcare-hai-prediction

Predicting hospital healthcare-associated infection rates from public CMS, Census, BLS and state data, built as a reproducible data engineering project on AWS.

Hospital-acquired infection prediction research for a Data Engineer. This fresh
repository builds on the capstone concept, not its implementation. Same-period
prediction is primary; forecasting is a later extension. Acquisition and S3
storage precede comprehensive schema review and tutorial drafting. Availability
does not establish clinical validity or model eligibility.

## Table of Contents

- [Security](#security)
- [Background](#background)
- [Install](#install)
- [Usage](#usage)
- [Architecture](#architecture)
- [Data](#data)
- [Quality Checks](#quality-checks)
- [Deploy and Teardown](#deploy-and-teardown)
- [Repository Layout](#repository-layout)
- [Limitations](#limitations)
- [Contributing](#contributing)
- [License](#license)

## Security

Deployment configuration is Internal; credentials are Restricted and never
belong in tracked files. Settings come only from the ignored `.env`, described
by `.env.example`. API keys live in AWS Secrets Manager and are read only at run
time (see [Install](#install)). Every commit is scanned for secrets, credential
files and data files (see [Quality Checks](#quality-checks)). Report a suspected
secret or personal value in this repository to the repository owner privately.

## Background

The project asks whether a hospital's infection score can be predicted from
same-period public measures, and why a simpler earlier model explained little
of the variation. It is in the acquisition stage: public sources are collected
through publisher APIs first, then download URLs, into private versioned S3
storage with a receipt, hash and version readback for every object. Schema
review, modeling and serving come later. The [Architecture](#architecture)
diagram shows what is built and what is planned.

## Install

Prerequisites: macOS on Apple silicon or Linux AMD64, Python 3.12, Git,
Terraform 1.16.1 and AWS CLI v2. Google Chrome renders the architecture diagram.

Run from this repository's root with its existing Python 3.12 `.venv`.
Dependencies are shared across development and production in `requirements.txt`.
Gitleaks, TFLint and trivy are installed only in ignored `.tools/`; versions and
publisher SHA-256 values are pinned in `config/quality_tools.json`.

```sh
python3.12 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
.venv/bin/python scripts/quality/install_tools.py
PRE_COMMIT_HOME="$PWD/.tools/pre_commit_cache" .venv/bin/python -m pre_commit install
cp .env.example .env
```

Fill every value in `.env`; the file stays out of Git. `AWS_PROFILE` is the
project profile, named `<PROJECT_NAME>_<ENVIRONMENT>` after its same-named IAM
user. IAM changes use a separate administrator profile that is never stored in
`.env`.

The native installer supports macOS ARM64 and Linux AMD64. Repeating a verified
installation preserves bytes and modification times; an unexpected existing
binary is rejected rather than overwritten. `--archive-directory` supports
offline installation from the exact pinned release filenames. Do not replace
the shared system tools to satisfy this project.

Named manual steps. These are the only setup steps no script performs:

1. **API keys.** Request the Census and BLS API keys from their publishers and
   store each in AWS Secrets Manager, in the configured region, as a secret named
   `census_api_key` or `bls_api_key` holding JSON with one field, `api_key`.
   The keys are shared by several projects, so they are created by hand outside
   Terraform. Store the HUD USPS crosswalk token separately as `hud_api_key`,
   with the token alone as its plaintext value (no `Bearer ` prefix). The IAM
   configuration grants read access to these three names and never reads a
   value; each permission update requires a reviewed saved plan before apply.
2. **Administrator profile.** Configure an AWS CLI profile with IAM permissions
   for the IAM stack. It is passed on the command line, never stored in `.env`.

## Usage

Run the local CI, the same script GitHub Actions runs, without AWS credentials:

```sh
env PYTHON_BIN="$PWD/.venv/bin/python" \
  TF_PLUGIN_CACHE_DIR="$PWD/infra/.terraform/providers" \
  /bin/bash scripts/run_ci.sh
```

Each CI run retains configuration evidence under `data/e2e/configuration/` and
quality evidence under `data/e2e/quality/`. E2E reports include source hashes,
expected and observed process outcomes, repeat commands and tested limits.
GitHub Actions retains these synthetic artifacts for 14 days. Local TFLint
needs permission to start its bundled plugin; a sandbox denial is a blocked
check, not a passing result.

The E2E runners can also run on their own; each needs a new output directory:

```sh
.venv/bin/python -m scripts.infrastructure.run_e2e --output-directory <new_directory>
.venv/bin/python -m scripts.quality.run_e2e --output-directory <new_directory>
.venv/bin/python -m scripts.quality.run_checks_e2e
.venv/bin/python -m scripts.quality.run_documentation_review_e2e
```

Before every commit, stage the intended files by name, then draft, complete and
stage the documentation review record (see [Quality Checks](#quality-checks)).

## Architecture

![Architecture diagram](docs/architecture/architecture.png)

The diagram source is `docs/architecture/architecture.html`; the PNG is rendered
from it with headless Chrome. Dashed components are planned, not built.

```sh
"/Applications/Google Chrome.app/Contents/MacOS/Google Chrome" --headless=new --hide-scrollbars \
  --window-size=1200,1330 --virtual-time-budget=5000 \
  --screenshot=docs/architecture/architecture.png "file://$PWD/docs/architecture/architecture.html"
```

## Data

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

The data bucket carries its own `DataClassification` tag of Confidential
(project decision, Sep 27 2026). Redacted derivatives are not formally
anonymized. Hospital chief executives in HCAI annual financial files and state
officials' work contacts in a CMS contacts table are treated as Confidential:
originals stay local and only redacted copies belong in S3. The initial 218
Excel/contact versions were removed on Sep 28 2026. Redacted replacements for
75 further versions are stored; those originals were deleted and verified on
Sep 28 2026. Retirement records cover all 293 removed versions, storage
reconciliation passed, and the temporary deletion grant was removed.
Operational evidence stays in ignored local
folders; this status does not clear privacy or modeling holds.

Tests use generic synthetic data. Real personal values must never appear in
fixtures, logs, tutorials, published artifacts or commits. Acquisition inputs,
operational evidence and the detailed exception record remain untracked.

The explicitly approved live S3 verification tool is the sole opt-in exception
to synthetic-only verification. Normal CI does not run it or contact AWS.

## Quality Checks

CI checks Ruff formatting and security rules, MyPy, outgoing-source secret
scanning, repository checks, configuration and quality E2E, retained regression
safeguards, Terraform mock tests, TFLint, Checkov and `trivy config` (its
embedded checks only, run offline). Pre-commit scans the actual staged index,
including changes hidden by a clean working copy, before local CI. The source
scan excludes ignored acquisition data; neither scan sends files to an external
service. Secret values and matching source fragments are omitted from scanner
diagnostics.

Pre-commit (`pre-commit` and `commit-msg` stages) and `scripts/run_ci.sh`
enforce these repository rules at the pinned versions:

- **Secrets and credential files:** gitleaks scans the staged index; `.env`
  (but not `.env.example`), Terraform state and saved plans, keys and cloud
  credential files are blocked.
- **Data files:** `.csv`, `.tsv`, `.parquet`, `.xlsx`, `.xls`, `.jsonl`,
  `.ndjson`, `.avro`, `.db` and `.sqlite` files are allowed only in
  `tests/fixtures/`, for synthetic samples. Any file over 5 MB is blocked
  unless Git LFS stores it. Acquired data stays in S3 and ignored local folders.
- **Commit messages:** Conventional Commits and no AI attribution, checked in
  the full message by the `commit-msg` hook and, in GitHub Actions, for every
  pushed commit. AI credit and agent-session lines are rejected; human
  co-authors and factual tool mentions are allowed. Detection uses an explicit
  agent/vendor list, so new forms need a failing sample before the check is
  extended. Review pull request text separately; a Git hook cannot read it.
- **Docs match the code:** every environment variable the code reads is listed
  in `.env.example`; a removed script, flag or variable no longer appears in the
  docs; and every commit carries `.documentation_review.json`, a per-document
  review outcome bound to the staged snapshot. Draft it with
  `.venv/bin/python -m scripts.quality.documentation_review prepare --reviewer <name> --reviewed-at <UTC>`
  after staging, review every document, fill each outcome and note, then stage it.
- **Lint settings:** Ruff keeps rule sets `E`, `F`, `I`, `B`, `UP`, `S`, `SIM`
  and `T20` at line length 160 with no ignore or exclude settings; MyPy keeps
  `--check-untyped-defs` and `--disallow-untyped-defs`. The only allowed
  suppression is Ruff `S603` in `scripts/process.py`, the one process launcher;
  other scripts start programs through it and run as modules
  (`.venv/bin/python -m scripts.quality.scan_secrets`). Tests use
  `tests/support.py` `check()` instead of `assert`.

Each check has a bad and a good sample run through the real `pre-commit` entry
point (`scripts/quality/run_checks_e2e.py` and
`scripts/quality/run_documentation_review_e2e.py`); both run in local CI.
Every detection check was run on the whole repository before it was enabled,
with no false positives, so they block from the start. There are no bypasses:
never use `SKIP=` or `git commit --no-verify`. If a check blocks a valid change,
fix the check or ask the repository owner.

## Deploy and Teardown

`scripts/infrastructure/terraform.sh` renders Terraform inputs from `.env`,
verifies the project identity and runs Terraform with the named profile. Apply
only a reviewed saved plan:

```sh
bash scripts/infrastructure/terraform.sh plan -out=<plan_file>
bash scripts/infrastructure/terraform.sh apply <plan_file>
bash scripts/infrastructure/terraform.sh --iam --admin-profile <administrator_profile> plan -out=<plan_file>
bash scripts/infrastructure/terraform.sh --iam --admin-profile <administrator_profile> apply <plan_file>
```

Terraform state stays local in each stack folder and is never committed.
Remote state protection remains deferred until the end of the project as
requested.

**Teardown.** The data bucket is the project's persistent store of immutable
source data, not a demo stack, so it has no destructive teardown switch
(project exception, Sep 27 2026). `force_destroy` is false and
`prevent_destroy` is set; removing the bucket would need a separately reviewed
change that lifts both protections first. The bucket keeps its established name
rather than the `<project>-<component>-<environment>` pattern, because renaming
it would mean moving all stored data (project exception, Sep 27 2026).

**Environments.** Both stacks tag resources with `Environment = dev`. A `prod`
environment and an `infra/modules/` layout are deferred until the next
infrastructure component is added (project decision, Sep 27 2026).

**Lifecycle rule.** The approved lifecycle rule was applied and read back from
S3 on Sep 24 2026 (UTC). It aborts incomplete multipart uploads seven days after
initiation, with no expiration or transition of completed objects, historical
versions or delete markers. Local mock-plan tests, source-shape checks and live
configuration readback pass. The seven-day scheduler itself has not been
observed in a timed test, and no deliberately incomplete upload was created for
verification.

Pre-deployment inspection confirmed no existing lifecycle configuration. The
separately approved IAM plan added only bucket-scoped
`s3:PutLifecycleConfiguration`; the approved storage plan created only the
cleanup rule. IAM administration was used only for the policy update, while
storage deployment used the project profile. This permission can change the
bucket's entire lifecycle configuration, so future changes still require full
plan review and preservation of existing rules. Private plans and live
verification evidence remain untracked under
`data/infrastructure/lifecycle_deployment/`.

**Deferred controls.** Apply the project's engineering requirements when
development reaches the relevant stage. Future requirements guide later work;
deferral does not mean a control is implemented. The following
acquisition-stage deferrals are approved for `aws_s3_bucket.data` only. Checkov
and trivy evidence retain each exception, resource and reason.

| Deferred control | Reason | Review trigger |
| --- | --- | --- |
| Event notifications (`CKV2_AWS_62`) | No event consumer exists yet. | Implementing an event consumer. |
| Cross-region replication (`CKV_AWS_144`) | Recovery objectives and a destination are not established. | Recovery design, before production. |
| KMS (`CKV_AWS_145`, trivy `AWS-0132`) | Acquisition currently uses SSE-S3/AES256. | Coordinated ingestion and IAM migration, before production. |
| Access logging (`CKV_AWS_18`, trivy `AWS-0089`) | Approved acquisition-stage deferral of a separate logging destination; request-audit gap accepted for this stage. | Before additional user or service access, production or newly approved sensitive-data use. |

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

## Repository Layout

- `infra/`: private, versioned S3 configuration; `infra/iam/`: scoped service policies.
- `scripts/process.py`: the one process launcher every script uses to start programs.
- `scripts/infrastructure/`: dotenv rendering, identity guards and configuration E2E.
- `scripts/quality/`: pinned tool installation, secret scanning, repository checks, documentation review and Terraform analysis.
- `scripts/run_ci.sh`: the shared local and GitHub CI entry point.
- `tests/`: retained pytest regression safeguards and their `check()` helper.
- `config/quality_tools.json`: pinned native tool versions and publisher hashes.
- `docs/architecture/`: the architecture diagram source and its rendered PNG.
- `.github/`: the CI workflow and the pull request template.
- Local acquisition code, configuration, data and audit evidence remain ignored by request.
  They are committed, with full documentation of the collection process, at the
  end of the data collection stage. No tutorial files are added to this repository.

## Limitations

These checks are not deployment or clinical validation. The ignored acquisition
implementation requires its separate synthetic verification before local use;
a clean GitHub checkout cannot exercise files intentionally excluded from Git.
Nothing in S3 is approved for modeling yet: definitions, geography and modeling
reviews remain open. Raw personal-data retention remains a separate unresolved
control. The deferred controls above are not implemented.

## Contributing

Individual project; contributions are not accepted.

## License

All rights reserved.
