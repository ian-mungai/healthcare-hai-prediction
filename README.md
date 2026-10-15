# healthcare-hai-prediction

Predicts hospital healthcare-associated infection rates from public CMS, Census, BLS and state data on AWS.

**Status:** In development.

## Install

Prerequisites: macOS on Apple silicon or Linux AMD64, Python 3.12, Git,
Terraform 1.16.1 and AWS CLI v2. Node.js 18 or later and Google Chrome render the
architecture diagram. The lakehouse runs in Docker Desktop; the launcher (`orchestration/launcher.py`) computes a
memory plan for every job from what Docker and the Mac have free and stops a launch whose container budget is below
4 GiB. Native quality tools install into the ignored `.tools/` at versions and publisher SHA-256 values pinned in
`config/quality_tools.json`.

```sh
python3.12 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
.venv/bin/python scripts/quality/install_tools.py
.venv/bin/python -m scripts.quality.install_markdownlint
.venv/bin/python -m scripts.quality.install_sqlfluff
.venv/bin/python -m scripts.lakehouse.dbt_package_archive fetch
PRE_COMMIT_HOME="$PWD/.tools/pre_commit_cache" .venv/bin/python -m pre_commit install
cp .env.example .env
```

Fill every value in `.env`; the file stays out of Git. `AWS_PROFILE` is the
project profile, named `<PROJECT_NAME>_<ENVIRONMENT>` after its same-named IAM
user. IAM changes use a separate administrator profile that is never stored in
`.env`.

The services are defined in `docker-compose.yaml`, which the launcher reads as its service model; the launcher, not
Compose, starts every container:

- PostgreSQL and Apache Polaris 1.8.0 use digest-pinned images.
- The Spark 4.1.2 (Iceberg 1.11.0), DuckDB 1.5.6 analytics, Great Expectations quality and Polaris bootstrap images are
  built from `services/`, with their Python packages hash-pinned; each job image carries the deadline entrypoint.

The launcher uses local images only and never pulls or builds one. Before the first catalog command, pull the two pinned
images and build the others once:

```sh
docker pull postgres:17@sha256:d74eeac9a635390a49bc21bd49fccd973de707e2a53a76ac49b552b8712ec46f
docker pull apache/polaris:1.8.0@sha256:347615e736cab2c9be0d8c464552ac930be4755302bb0aeb745216bbae99ba5b
docker build -t hai-polaris-admin:1.8.0 --build-context hai=./services/hai services/lakehouse/polaris_admin
docker build -t hai-lakehouse-spark:4.1.2-iceberg1.11.0 --build-context hai=./services/hai services/lakehouse/spark
docker build -t hai-analytics:duckdb1.5.6-dbt1.11.15 --build-context hai=./services/hai services/analytics
docker build -t hai-quality:gx1.23.2-duckdb1.5.6 --build-context hai=./services/hai services/quality
```

Then start the catalog from the main checkout (a launcher running in another checkout refuses it):

```sh
.venv/bin/python -m scripts.lakehouse.catalog up
```

`up` is idempotent. On first use it generates the catalog credentials into the ignored, owner-only
`data/lakehouse/secrets/` and never prints them. It creates the `hai_lakehouse` catalog, limited to `lakehouse/` in the
project bucket. `down` stops the containers and keeps the catalog database.

## Usage

Run the full local CI, which the pre-commit hooks and GitHub Actions also run, without AWS credentials. Run it before
every push; a commit runs only the check groups its staged paths touch:

```sh
env PYTHON_BIN="$PWD/.venv/bin/python" \
  TF_PLUGIN_CACHE_DIR="$PWD/infra/.terraform/providers" \
  /bin/bash scripts/run_ci.sh
```

The E2E runners can also run on their own; each needs a new output directory:

```sh
.venv/bin/python -m scripts.infrastructure.run_e2e --output-directory <new_directory>
.venv/bin/python -m scripts.quality.run_e2e --output-directory <new_directory>
.venv/bin/python -m scripts.quality.run_checks_e2e
.venv/bin/python -m scripts.quality.run_documentation_review_e2e
```

Each run writes its report under `data/e2e/` (the configuration E2E under `data/e2e/configuration/`, the quality E2E
under `data/e2e/quality/`). How the checks run and what each one blocks is in `docs/quality_checks.md`. The lakehouse
commands are in `docs/lakehouse_usage.md`. The resource limits, pinned tools and manual set-up steps behind Install are
in `docs/install_details.md`. The environments, bucket lifecycle rules and deferred storage controls are in
`docs/infrastructure_controls.md`. Rendering the architecture diagram is in `docs/architecture_rendering.md`.
