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
packages from `dbt/package-lock.yml` into the ignored `data/analytics/dbt/dbt_packages`. The schema-review tools in
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

1. **API keys.** Request the Census and BLS API keys from their publishers and
   store each in AWS Secrets Manager, in the configured region, as a secret named
   `census_api_key` or `bls_api_key` holding JSON with one field, `api_key`.
   The keys are shared by several projects, so they are created by hand outside
   Terraform. Store the HUD USPS crosswalk token separately as `hud_api_key`,
   in the same JSON shape with the token alone in `api_key` (no `Bearer`
   prefix; the collector adds the header). The IAM
   configuration grants read access to these three names and never reads a
   value; each permission update requires a reviewed saved plan before apply.
2. **Administrator profile.** Configure an AWS CLI profile with IAM permissions
   for the IAM stack. It is passed on the command line, never stored in `.env`.
