---
title: Install Details
description: Resource limits, pinned tools, the native installer and the manual setup steps behind the README's Install.
last_updated: 2026-10-13
---

# Install Details

## Resources

The lakehouse runs in Docker Desktop (32 GiB of memory on the development Mac). Every job launch computes one resource plan after the catalog services
start. Its container limit is the smaller of Docker's total minus running containers'
use minus 8 GiB headroom and the Mac's free, file-backed and purgeable memory minus
2 GiB, plus the memory Docker's VM already holds that no running container uses, less a
2 GiB VM base, so a build that follows another can reuse it. The Mac cap is skipped once
Docker's VM holds its full allocation.
A container budget below 4 GiB stops the launch. DuckDB receives 80% of the container
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
   Store every secret in AWS Secrets Manager, in the configured region, under
   the name that its variable in `infra/iam/variables.tf` holds as default:
   `census_api_key` (`census_secret_name`, the Census API key), `bls_api_key`
   (`bls_secret_name`, the BLS API key), `hud_api_key` (`hud_secret_name`, the
   HUD USPS crosswalk token), `jev_api_key` (`jev_secret_name`, the TypeSafe Jev
   API key) and `healthcare-hai-prediction/airflow-admin`
   (`airflow_admin_secret_name`, the Airflow administrator password; the
   username is `admin`). Every secret holds its value as plain text, with no
   JSON; the HUD secret holds the token alone, with no `Bearer` prefix (the
   collector adds the header). The BLS, Census and HUD collectors read the
   plain-text value with boto3. The keys are shared by several projects, so they are
   created by hand outside Terraform. The IAM configuration grants read access
   to exactly these five names and never reads a value; each permission update
   requires a reviewed saved plan before apply. The saved-plan check accepts
   only these committed names. To add a secret, add one more variable with its
   name as default to `infra/iam/variables.tf` and add it to the `secret_names`
   list in `infra/iam/main.tf`; every name must differ.

   The project profile also manages 13 secrets that no person creates by hand.
   They hold the local catalog, workflow and lineage credentials, each as plain
   text under `healthcare-hai-prediction/`: `postgres-password`,
   `polaris-root-secret`, `polaris-reader-client-id`,
   `polaris-reader-client-secret`, `polaris-gold-client-id`,
   `polaris-gold-client-secret`, `polaris-silver-client-id`,
   `polaris-silver-client-secret`, `airflow-db-password`, `airflow-fernet-key`,
   `airflow-jwt-secret`, `airflow-api-secret-key` and `marquez-db-password`.
   Each name is a root variable in `infra/iam/variables.tf` (the secret's last
   path part with `_name`, for example `postgres_password_name`) with its name as
   default. The IAM configuration grants the project profile read, describe,
   create and put on exactly these names. Terraform never creates a secret or a
   secret version, so no value enters a plan or the state; the saved-plan check
   (`render_project_config.py --check-plan`) refuses any plan that holds a secret
   resource or data source. The setup code
   (`scripts.lakehouse.catalog up`, the writer principal setup, the orchestration
   supervisor and the launcher) generates or reads each value in memory, stores
   it in Secrets Manager and passes it to a container through its environment.
   No file on the host holds a secret. State that already exists keeps the value
   it was created with: the code refuses to generate a replacement next to an
   existing database or catalog realm, so copy the current value to the same
   secret name first.
2. **Administrator profile.** Configure an AWS CLI profile with IAM permissions
   for the IAM stack. It is passed on the command line, never stored in `.env`.
