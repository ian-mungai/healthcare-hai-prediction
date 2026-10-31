---
title: Install details
description: Resource limits, pinned tools, the native installer and the manual setup steps behind the README's Install.
last_updated: 2026-10-29
---

# Install details

This document gives the resource limits, the pinned tools, the native installer and the manual setup steps behind Install in the README. It is for developers who set up this project on a Mac or a Linux machine.

## Resources

The lakehouse runs in Docker Desktop (32 GiB of memory on the development Mac). Every job launch computes one resource plan after the catalog services
start. Its container limit is the smaller of two values:

- Docker's total memory, minus the memory that running containers use, minus 8 GiB of headroom.
- The Mac's free, file-backed and purgeable memory, plus the memory that Docker's VM already holds and no running container uses (less a 2 GiB VM base), minus 2 GiB.

The launcher adds that VM memory to the Mac value before it takes the smaller value, so a build that follows another build can reuse it. The launcher skips the Mac cap once Docker's VM holds its full allocation.

That figure is the free memory. An allocation takes 75% of it (`ALLOCATION_SHARE_PERCENT` in `memory_budget.py`), so a process that grows before the containers start still has room. The plan record shows both figures. A container budget below 4 GiB, after the 75% cap, stops the launch.

The wait for memory used elsewhere starts at the first reading that shows too little. The attempt record stores the start, so a launcher restart keeps it. Once 600 seconds (`MEMORY_WAIT_DEADLINE`) have passed, the next launcher tick or reading ends the queued launch as a failed create. Its message names memory, the free figure and the need of the last short reading. A pending or failed reading, a degraded launcher and a good reading that arrives after the deadline do not extend the wait. Only a good reading before the deadline clears it. A start cutoff that comes first still ends the launch.

DuckDB receives 80% of the container
budget, rounded down to whole GB, to leave room for allocations outside its buffer
limit. The 4 GiB floor applies to the container, so the engine allowance is smaller.
This reserve does not guarantee that a workload cannot run out of memory.

Spark keeps its heap calculation. The heap is the smaller of the container budget divided by
1.10 and the budget minus 384 MiB, rounded down to whole GiB. Job parallelism uses
the smaller positive CPU count from `sysctl -n hw.ncpu` and Docker. The same count
sets Spark workers, lakehouse dbt model concurrency and DuckDB engine threads.
Fixture dbt model concurrency and the lint target stay at one for deterministic checks.
`scripts/lakehouse/memory_budget.py --launch-json` shows the complete plan.
The launcher (`orchestration/launcher.py`), the only process that starts containers, applies that plan to every job it starts.
The analytics SQL settings are read-only Compose mounts and need no image rebuild.

When Polaris or PostgreSQL starts, `scripts/lakehouse/catalog.py` gives it a
container limit of 25% or 10% of the available budget (at least 1 GiB or 256 MiB).
Those limits stay fixed for that container's lifetime. A service-derived job limit
in the private Compose env file lets catalog-only commands render inactive profiles.
Every job launcher overrides it with its fresh plan. Use the launch scripts to run jobs.
Close containers and apps you no longer use before a heavy run.
Docker's VM keeps a heavy build's memory, much of it compressed by macOS, so
`.venv/bin/python -m scripts.lakehouse.catalog release` stops this project's catalog and
quits Docker Desktop. It quits nothing while another project's container runs.
The gold and staging E2Es call it after a real build. It skips unless the run created a container. The next real run starts Docker Desktop
again (`catalog up`). `run_staging_e2e --real-only` runs the
real builds and their checks without the fixture cases, for changes that touch no fixture.

Run the non-Docker resource contracts with
`.venv/bin/python -m scripts.lakehouse.run_resource_checks`.
The artifact under `data/e2e/resource_limits/` records synthetic resource readings,
repeated CLI results and configuration checks. It does not prove live container limits.
Verify those limits through Docker inspection and engine settings before you declare runtime
resource conformance complete.

## Tools

Run from this repository's root with its existing Python 3.12 `.venv`.
Development and production share the dependencies in `requirements.txt`.
A requirement used only as a command carries `# tool: <use>`. A requirement imported by
name carries `# dynamic: <where>`. The dependency check reads both markers.

The installers place the tools as follows:

- Gitleaks, TFLint, trivy, OSV-Scanner, zizmor and typos install only into the ignored `.tools/` directory. Their versions and
  publisher SHA-256 values are pinned in `config/quality_tools.json`.
- The Python lint tools (import-linter, check-jsonschema) install into `.tools/` with `scripts.quality.install_lint_tools` from hash-pinned
  requirements (`docs/quality_checks.md`, Linters beside lint_sources). A check prefers the `.tools/bin` binary of OSV-Scanner, zizmor and typos and
  otherwise uses the Homebrew binary at the pinned version. There is one trivy, the one in `.tools/bin`.
- The markdownlint hook's `markdownlint-cli2` installs into `.tools/markdownlint-cli2` with `npm ci`
  from the lockfile in `scripts/quality/markdownlint/`. The lockfile pins the integrity hash of each package. The tool needs Node.js 22 or later.
- The `sqlfluff` hook's SQLFluff and its dbt templater install into `.tools/sqlfluff` from the hash-pinned
  `scripts/quality/sqlfluff/requirements.txt`. The same command installs the dbt
  packages from `dbt/package-lock.yml` into the ignored `data/analytics/dbt/dbt_packages`.
- The launcher's `dbt_deps` job installs the same packages without network from a local archive. `scripts.lakehouse.dbt_package_archive fetch`
  downloads the tarballs named in `config/lakehouse/dbt_package_archive.json`, rebuilds the archive under the ignored
  `data/analytics/dbt/package_archive/` and refuses any byte that differs from the record.
- The schema-review tools in `scripts/review/` need the hash-pinned packages in `requirements-review.txt`.
  They install into the ignored `.review_dependencies/` (Apple silicon only). The header of the file gives the command.

## Native installer

The native installer supports macOS ARM64 and Linux AMD64. Repeating a verified
installation preserves bytes and modification times. The installer rejects an unexpected existing
binary and does not overwrite it. `--archive-directory` supports
offline installation from the exact pinned release filenames. Do not replace
the shared system tools to satisfy this project.

## Named manual steps

No script performs the following setup steps. Do them by hand:

1. **Secrets.** Request the Census and BLS API keys from their publishers.
   The four shared API keys live in AWS Secrets Manager, in the configured
   region, under the name that its variable in `infra/iam/variables.tf` holds as
   default: `census_api_key` (`census_secret_name`, the Census API key),
   `bls_api_key` (`bls_secret_name`, the BLS API key), `hud_api_key`
   (`hud_secret_name`, the HUD USPS crosswalk token) and `jev_api_key`
   (`jev_secret_name`, the TypeSafe Jev API key). Every secret holds its value
   as plain text, with no JSON. The HUD secret holds the token alone, with no
   `Bearer` prefix (the collector adds the header). The BLS, Census and HUD
   collectors read the plain-text value with boto3. The keys are shared by
   several projects, so they are created by hand outside Terraform. The
   `secretsmanager` policy file grants read and describe on exactly these four
   names and never reads a value. Each permission update is read with
   `manage_policies plan` and applied only with the confirmation word.

   Every project secret is an AWS Systems Manager Parameter Store SecureString
   parameter in the configured region, named
   `/healthcare_hai_prediction/<name_with_underscores>`, on the standard tier
   (no charge and at most 4 KB per value) and encrypted with the AWS managed
   key `aws/ssm`. Each holds its value as plain text. The Airflow administrator
   password is `/healthcare_hai_prediction/airflow_admin`
   (`airflow_admin_secret_name`) with the username `admin`. It exists already
   (see the Airflow administrator password step below). The project profile
   may read it and never write it. The other 15 are
   managed, so no person creates them by hand. They hold the local catalog,
   workflow, lineage and feature store credentials, each as plain text:
   `postgres_password`, `polaris_root_secret`, `polaris_reader_client_id`,
   `polaris_reader_client_secret`, `polaris_gold_client_id`,
   `polaris_gold_client_secret`, `polaris_silver_client_id`,
   `polaris_silver_client_secret`, `airflow_db_password`, `airflow_fernet_key`,
   `airflow_jwt_secret`, `airflow_api_secret_key`, `marquez_db_password`,
   `redis_password`, `mlflow_db_password` and `grafana_admin_password`. Each name is a root variable in `infra/iam/variables.tf`
   (the secret's last path part with `_name`, for example
   `postgres_password_name`) with its full name as default. A name starts with a
   slash and has up to 15 parts of letters, digits and underscores. It never
   starts with `aws` or `ssm` (the AWS naming rules for a hierarchy). The `ssm`
   policy file grants the project profile `ssm:GetParameter` on the 17 parameter
   ARNs and `ssm:PutParameter` on the 16 managed ones. It grants nothing else. It grants no
   `kms` action (the key policy of the AWS managed key admits the account's
   principals through Parameter Store), no `DescribeParameters`, no
   `GetParametersByPath` and no wildcard. A parameter ARN is
   `arn:aws:ssm:<region>:<account>:parameter/<name without its leading slash>`.
   Terraform never creates or reads a secret, a secret version or a parameter,
   so no value enters a plan or the state. The saved-plan check
   (`render_project_config.py --check-plan`) refuses any plan that holds a
   Secrets Manager or Parameter Store resource or data source. The one exception
   is a data source of a public AWS parameter under `/aws/service/`. The saved-plan check accepts only the committed names.
   The names appear once, as the defaults of those variables. The policy files hold
   no name. `render_policy` repeats the list elements that hold
   `${SHARED_SECRET_NAME}`, `${READ_PARAMETER_NAME}` and `${MANAGED_PARAMETER_NAME}`
   once per name, taking the names from the variable defaults through
   `scripts/infrastructure/secret_store.py`. The `sts` policy file takes the two
   lakehouse role names (`lakehouse_reader_role_name`, `lakehouse_writer_role_name`)
   and the task execution policy takes its parameter names from the same
   variables, not from `PROJECT_NAME`. There are eight, plus a ninth while `monitoring_enabled` is true.

   To add a secret:

   1. Add one more variable with its name as default to `infra/iam/variables.tf`.
   2. Add the variable to the matching list in `infra/iam/main.tf`. The output `project_secret_names`
      checks that every name differs.
   3. Add the name to the names in `secret_store.py`. A new
      shared key also needs its variable in `SHARED_VARIABLES`.

   The policy files follow the default. `run_policy_e2e` and `run_secret_store_e2e` fail until the
   lists agree.

   The setup code
   (`scripts.lakehouse.catalog up`, the writer principal setup, the orchestration
   supervisor and the launcher) generates or reads each value in memory through
   `scripts/infrastructure/secret_store.py`. It stores the value in Parameter Store (a
   create never overwrites. Every write is read back) and passes it to a
   container through its environment. No file on the host holds a secret. State
   that already exists keeps the value it was created with. The code refuses to
   generate a replacement next to an existing database or catalog realm, so copy
   the current value to the same parameter name first.

   **Airflow administrator password.** The one-time copy of the hand-made Secrets
   Manager secret `healthcare-hai-prediction/airflow-admin` to the parameter is
   done and verified. No step here repeats it. No project policy grants that
   secret. No project policy grants creating or overwriting the parameter. The
   project profile can only read it. A new password needs the administrator
   profile. The owner may delete the old secret by hand.

   First read after the IAM apply: if reading a parameter fails with
   `AccessDeniedException` on `kms:Decrypt`, the key policy of `aws/ssm` does not
   admit the profile. Report it and do not widen the grant.
2. **Administrator profile.** Configure an AWS CLI profile with IAM permissions
   for the IAM stack and the policy scripts. It is passed on the command line, never stored in `.env`.
