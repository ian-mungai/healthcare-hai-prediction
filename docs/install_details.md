---
title: Install Details
description: Resource limits, pinned tools, the native installer and the manual setup steps behind the README's Install.
last_updated: 2026-10-22
---

# Install Details

## Resources

The lakehouse runs in Docker Desktop (32 GiB of memory on the development Mac). Every job launch computes one resource plan after the catalog services
start. Its container limit is the smaller of Docker's total minus running containers'
use minus 8 GiB headroom and the Mac's free, file-backed and purgeable memory minus
2 GiB, plus the memory Docker's VM already holds that no running container uses, less a
2 GiB VM base, so a build that follows another can reuse it. The Mac cap is skipped once
Docker's VM holds its full allocation.
That figure is the free memory. An allocation takes 75% of it (`ALLOCATION_SHARE_PERCENT` in `memory_budget.py`), so a process that grows
before the containers start still has room; the plan record shows both figures. A container budget below 4 GiB, after the 75% cap, stops the
launch. The wait for memory used elsewhere starts at the first reading that shows too little and is stored in the attempt record, so a
launcher restart keeps it. Once 600 seconds (`MEMORY_WAIT_DEADLINE`) have passed, the next launcher tick or reading ends the queued launch
as a failed create whose message names memory, the free figure and the need of the last short reading. A pending or failed reading, a
degraded launcher and a good reading that arrives after the deadline do not extend the wait; only a good reading before the deadline clears
it. A start cutoff that comes first still ends the launch. DuckDB receives 80% of the container
budget, rounded down to whole GB, to leave room for allocations outside its buffer
limit. The 4 GiB floor applies to the container, so the engine allowance is smaller.
This reserve does not guarantee that a workload cannot run out of memory.

Spark retains its heap calculation: the smaller of the container budget divided by
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
in the private Compose env file lets catalog-only commands render inactive profiles;
every job launcher overrides it with its fresh plan. Use the launch scripts to run jobs.
Close containers and apps you no longer use before a heavy run.
Docker's VM keeps a heavy build's memory, much of it compressed by macOS, so
`.venv/bin/python -m scripts.lakehouse.catalog release` stops this project's containers and
restarts Docker Desktop; it restarts nothing while another project's container runs.
The staging E2E calls it after each real build. `run_staging_e2e --real-only` runs the
real builds and their checks without the fixture cases, for changes that touch no fixture.

Run the non-Docker resource contracts with
`.venv/bin/python -m scripts.lakehouse.run_resource_checks`.
The artifact under `data/e2e/resource_limits/` records synthetic resource readings,
repeated CLI results and configuration checks. It does not prove live container limits;
verify those through Docker inspection and engine settings before declaring runtime
resource conformance complete.

## Tools

Run from this repository's root with its existing Python 3.12 `.venv`.
Dependencies are shared across development and production in `requirements.txt`;
a requirement used only as a command carries `# tool: <use>` and one imported by
name carries `# dynamic: <where>`, so the dependency check reads them correctly.
Gitleaks, TFLint and trivy are installed only in ignored `.tools/`; versions and
publisher SHA-256 values are pinned in `config/quality_tools.json`. The markdownlint
hook's `markdownlint-cli2` is installed into `.tools/markdownlint-cli2` with `npm ci`
from the lockfile in `scripts/quality/markdownlint/`, which pins each package's
integrity hash; it needs Node.js 22 or later. The `sqlfluff` hook's SQLFluff and
its dbt templater are installed into `.tools/sqlfluff` from the hash-pinned
`scripts/quality/sqlfluff/requirements.txt`; the same command installs the dbt
packages from `dbt/package-lock.yml` into the ignored `data/analytics/dbt/dbt_packages`. The launcher's `dbt_deps`
job installs the same packages without network from a local archive: `scripts.lakehouse.dbt_package_archive fetch`
downloads the tarballs named in `config/lakehouse/dbt_package_archive.json`, rebuilds the archive under the ignored
`data/analytics/dbt/package_archive/` and refuses any byte that differs from the record. The schema-review tools in
`scripts/review/` need the hash-pinned packages in `requirements-review.txt`,
installed into the ignored `.review_dependencies/` (Apple silicon only); the file's
header gives the command.

## Native Installer

The native installer supports macOS ARM64 and Linux AMD64. Repeating a verified
installation preserves bytes and modification times; an unexpected existing
binary is rejected rather than overwritten. `--archive-directory` supports
offline installation from the exact pinned release filenames. Do not replace
the shared system tools to satisfy this project.

## Named Manual Steps

Named manual steps. These are the only setup steps no script performs:

1. **Secrets.** Request the Census and BLS API keys from their publishers.
   The four shared API keys live in AWS Secrets Manager, in the configured
   region, under the name that its variable in `infra/iam/variables.tf` holds as
   default: `census_api_key` (`census_secret_name`, the Census API key),
   `bls_api_key` (`bls_secret_name`, the BLS API key), `hud_api_key`
   (`hud_secret_name`, the HUD USPS crosswalk token) and `jev_api_key`
   (`jev_secret_name`, the TypeSafe Jev API key). Every secret holds its value
   as plain text, with no JSON; the HUD secret holds the token alone, with no
   `Bearer` prefix (the collector adds the header). The BLS, Census and HUD
   collectors read the plain-text value with boto3. The keys are shared by
   several projects, so they are created by hand outside Terraform. The
   `secretsmanager` policy file grants read and describe on exactly these four
   names and never reads a value. Each permission update is read with
   `manage_policies plan` and applied only with the confirmation word.

   Every project secret is an AWS Systems Manager Parameter Store SecureString
   parameter in the configured region, named
   `/healthcare_hai_prediction/<name_with_underscores>`, on the standard tier
   (no charge; a value holds at most 4 KB) and encrypted with the AWS managed
   key `aws/ssm`. Each holds its value as plain text. The Airflow administrator
   password is `/healthcare_hai_prediction/airflow_admin`
   (`airflow_admin_secret_name`; the username is `admin`): a person creates it
   once under the project profile (clone step below), which may read it and may
   create it but never overwrite it. The other 14 are
   managed: no person creates them by hand. They hold the local catalog,
   workflow, lineage and feature store credentials, each as plain text:
   `postgres_password`, `polaris_root_secret`, `polaris_reader_client_id`,
   `polaris_reader_client_secret`, `polaris_gold_client_id`,
   `polaris_gold_client_secret`, `polaris_silver_client_id`,
   `polaris_silver_client_secret`, `airflow_db_password`, `airflow_fernet_key`,
   `airflow_jwt_secret`, `airflow_api_secret_key`, `marquez_db_password` and
   `redis_password`. Each name is a root variable in `infra/iam/variables.tf`
   (the secret's last path part with `_name`, for example
   `postgres_password_name`) with its full name as default. A name starts with a
   slash and has up to 15 parts of letters, digits and underscores. It never
   starts with `aws` or `ssm` (the AWS naming rules for a hierarchy). The `ssm`
   policy file grants the project profile `ssm:GetParameter` on the 15 parameter
   ARNs and `ssm:PutParameter` on the 14 managed ones. It grants nothing else: no
   `kms` action (the key policy of the AWS managed key admits the account's
   principals through Parameter Store), no `DescribeParameters`, no
   `GetParametersByPath` and no wildcard. A parameter ARN is
   `arn:aws:ssm:<region>:<account>:parameter/<name without its leading slash>`.
   Terraform never creates or reads a secret, a secret version or a parameter,
   so no value enters a plan or the state; the saved-plan check
   (`render_project_config.py --check-plan`) refuses any plan that holds a
   Secrets Manager or Parameter Store resource or data source (the one exception
   is a data source of a public AWS parameter under `/aws/service/`). The saved-plan check accepts only the committed names.
   The names appear once: the defaults of those variables. The policy files hold
   no name; `render_policy` repeats the list elements that hold
   `${SHARED_SECRET_NAME}`, `${READ_PARAMETER_NAME}` and `${MANAGED_PARAMETER_NAME}`
   once per name, taking the names from the variable defaults through
   `scripts/infrastructure/secret_store.py`. The `sts` policy file takes the two
   lakehouse role names (`lakehouse_reader_role_name`, `lakehouse_writer_role_name`)
   and the task execution policy takes its eight parameter names from the same
   variables, not from `PROJECT_NAME`. To add a secret, add one more
   variable with its name as default to `infra/iam/variables.tf` and add it to
   the matching list in `infra/iam/main.tf` (the output `project_secret_names`
   checks that every name differs) and to the names in `secret_store.py` (a new
   shared key also needs its variable in `SHARED_VARIABLES`); the policy files
   follow the default. `run_policy_e2e` and `run_secret_store_e2e` fail until the
   lists agree. The setup code
   (`scripts.lakehouse.catalog up`, the writer principal setup, the orchestration
   supervisor and the launcher) generates or reads each value in memory through
   `scripts/infrastructure/secret_store.py`, stores it in Parameter Store (a
   create never overwrites; every write is read back) and passes it to a
   container through its environment. No file on the host holds a secret. State
   that already exists keeps the value it was created with: the code refuses to
   generate a replacement next to an existing database or catalog realm, so copy
   the current value to the same parameter name first.

   **Clone the Airflow administrator password to Parameter Store (the owner, once).**
   If `healthcare-hai-prediction/airflow-admin` was created by hand in Secrets
   Manager, copy it with one command under the project profile; run it only after
   the IAM apply of the `secretsmanager` and `ssm` policies and only when the owner
   says so. The project policies allow exactly this: reading that one secret
   (`CloneLegacyAirflowAdminPassword`) and creating the parameter without
   overwrite (`CreateAirflowAdminPasswordOnce`, condition `ssm:Overwrite` false).
   The administrator profile only grants permissions. The value stays in a shell variable and goes to
   the AWS CLI on standard input (`file://` prefix: [loading a parameter from a
   file](https://docs.aws.amazon.com/cli/latest/userguide/cli-usage-parameters-file.html)),
   so it is in no file and in no process argument. The subshell stops at the
   first failure and the write never overwrites. The command ends with a
   comparison of the readback:

   ```sh
   ( set -eu; P=<PROJECT_PROFILE>; R=<REGION>; OLD=healthcare-hai-prediction/airflow-admin; NEW=/healthcare_hai_prediction/airflow_admin
     v=$(aws secretsmanager get-secret-value --profile "$P" --region "$R" --secret-id "$OLD" --query SecretString --output text)
     printf %s "$v" | aws ssm put-parameter --profile "$P" --region "$R" --name "$NEW" --type SecureString --tier Standard --value file:///dev/stdin >/dev/null
     [ "$(aws ssm get-parameter --profile "$P" --region "$R" --name "$NEW" --with-decryption --query Parameter.Value --output text)" = "$v" ] && echo "cloned and verified" )
   ```

   Without `--overwrite` the command refuses an existing parameter, so a rerun
   changes nothing. After the check the owner may delete the old secret by hand;
   then remove the two clone statements from the policies in the next IAM change.
   First read after the IAM apply: if reading a parameter fails with
   `AccessDeniedException` on `kms:Decrypt`, the key policy of `aws/ssm` does not
   admit the profile; report it instead of widening the grant.
2. **Administrator profile.** Configure an AWS CLI profile with IAM permissions
   for the IAM stack and the policy scripts. It is passed on the command line, never stored in `.env`.
