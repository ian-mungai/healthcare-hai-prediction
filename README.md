# healthcare-hai-prediction

Predicts hospital healthcare-associated infection rates from public CMS, Census, BLS and state data on AWS.

**Status:** In development.

## Install

Prerequisites: macOS on Apple silicon or Linux AMD64, Python 3.12, Git,
Terraform 1.16.1, AWS CLI v2 and Node.js 22 or later for Markdown checks. Google Chrome renders the
architecture diagram. The lakehouse runs in Docker Desktop. The launcher (`orchestration/launcher.py`) computes a
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

Fill every value in `.env`. The file stays out of Git. `AWS_PROFILE` is the
project profile, named `<PROJECT_NAME>_<ENVIRONMENT>` after its same-named IAM
user. IAM changes use a separate administrator profile that is never stored in
`.env`. No secret belongs in `.env` or in any other file on the host: every project secret
lives in AWS Systems Manager Parameter Store and the four shared API keys in AWS Secrets Manager (`docs/install_details.md`).

The project profile holds only the permissions of the approved list, as one policy file per AWS service in
`infra/iam/policies/` (`healthcare_hai_prediction_<service>_policy.json`, with `${AWS_REGION}`, `${AWS_ACCOUNT_ID}`,
`${PROJECT_NAME}`, `${ENVIRONMENT}` and `${DATA_BUCKET_NAME}` placeholders). The owner applies them with the administrator
profile. The policy scripts read the values from `.env`. The secret and parameter names are not in the files: they are the
defaults of the secret-name root variables in `infra/iam/variables.tf`, which `render_policy` reads. Render to review, plan
(changes nothing), then apply with the confirmation word:

```sh
.venv/bin/python -m infra.iam.scripts.render_policy --output-directory <REVIEW_DIRECTORY>
.venv/bin/python -m infra.iam.scripts.manage_policies plan --admin-profile <ADMIN_PROFILE>
CONFIRM_IAM_POLICIES=apply-healthcare-hai-prediction-policies .venv/bin/python -m infra.iam.scripts.manage_policies apply --admin-profile <ADMIN_PROFILE>
```

The apply creates or updates the policies, attaches them to two groups and adds the project user to both. It also removes
the earlier grouped policies. `infra.iam.scripts.export_policies` writes the policies in IAM back as templates, to see
drift. The roles, the network, the host launch template and Auto Scaling group and the cost budget stay in the IAM
Terraform stack (`bash scripts/infrastructure/terraform.sh --iam --admin-profile <ADMIN_PROFILE>`), which the owner applies
first. The project profile then applies the platform stack (`--platform`). The order and the checks are in
`docs/infrastructure_controls.md`.

After the foundation is applied and the image digests are recorded, create a services plan with the project profile.
The wrapper renders `infra/services/deployment.auto.tfvars.json` from `.env` and the platform outputs without printing
values. The image digests stay in the separate `infra/services/images.auto.tfvars.json` written by `push_images record`.
Run from the repository root. Relative plan paths are inside `infra/services/`:

```sh
bash scripts/infrastructure/terraform.sh --services plan -out=services.tfplan
```

The owner reviews the saved plan before applying it. The wrapper refuses changed inputs, image digests or defaults:

```sh
bash scripts/infrastructure/terraform.sh --services apply services.tfplan
```

The first-start order and one-shot tasks are in `docs/aws_control_plane.md`.

The services are defined in `docker-compose.yaml`, which the launcher reads as its service model. The launcher, not
Compose, starts every container:

- PostgreSQL and Apache Polaris 1.8.0 use digest-pinned images.
- The Spark 4.1.2 (Iceberg 1.11.0), DuckDB 1.5.6 analytics, Great Expectations quality and Polaris bootstrap images are
  built from `services/`, with their Python packages hash-pinned. Each job image carries the deadline entrypoint.

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

`up` is idempotent. On first use it generates the catalog credentials straight into AWS Parameter Store (no file on the
host holds them) and never prints them. It creates the `hai_lakehouse` catalog, limited to `lakehouse/` in the
project bucket. `down` stops the containers and keeps the catalog database.

## Data layers

| Layer | Stored form | Build and publication |
| --- | --- | --- |
| Bronze | Source tables in Iceberg | Acquisition jobs load the approved source files. |
| Silver | Validated atomic tables in Iceberg namespace `silver`, under `lakehouse/silver/` | Trigger `silver_transform_dag` by hand (`scripts.orchestration.trigger_dag`) to pin bronze, build and validate silver, then publish a snapshot set. |
| Gold | Atomic facts, dimensions and bridges in Iceberg | Trigger `gold_transform_dag` by hand (`scripts.orchestration.trigger_dag`) to select a committed silver snapshot set, build and validate gold, then publish it. |
| Features | Models in `dbt/models/features/` and formula recipes in `dbt/seeds/features/` | Build research joins, alignments and derived ratios from gold as views when the feature phase needs them. |

Publication verification is pending. Fixture runs use a test registry and local Iceberg catalog. Silver and gold writes follow the run mode. `publish_fixture` runs write only under `lakehouse/silver_e2e/` and `lakehouse/gold_e2e/`. `real` runs write only under `lakehouse/silver/` and `lakehouse/gold/`. The launcher refuses any other silver or gold write. Local builds use DuckDB. Athena access through an AWS Glue catalog mirror belongs to the final AWS migration stage. Asset scheduling stays disabled. `gold_maintenance_dag` retains registered snapshot sets when maintaining gold.

## Usage

Run the full local CI, which the pre-commit hooks and GitHub Actions also run, without AWS credentials. Run it before
every push. A commit runs only the check groups its staged paths touch:

```sh
env PYTHON_BIN="$PWD/.venv/bin/python" \
  TF_PLUGIN_CACHE_DIR="$PWD/infra/.terraform/providers" \
  /bin/bash scripts/run_ci.sh
```

The E2E runners can also run on their own. Each needs a new output directory:

```sh
.venv/bin/python -m scripts.infrastructure.run_e2e --output-directory <new_directory>
.venv/bin/python -m scripts.infrastructure.run_policy_e2e --output-directory <new_directory>
.venv/bin/python -m scripts.quality.run_e2e --output-directory <new_directory>
.venv/bin/python -m scripts.quality.run_checks_e2e
.venv/bin/python -m scripts.quality.run_documentation_review_e2e
.venv/bin/python -m scripts.infrastructure.run_secret_store_e2e
```

Each run writes its report under `data/e2e/` (the configuration E2E under `data/e2e/configuration/`, the policy E2E under
`data/e2e/policies/`, the quality E2E under `data/e2e/quality/`). How the checks run and what each one blocks is in `docs/quality_checks.md`. The lakehouse
commands are in `docs/lakehouse_usage.md`. The resource limits, pinned tools and manual set-up steps behind Install are
in `docs/install_details.md`. The environments, the AWS stacks and project policies, bucket lifecycle rules and deferred controls are in
`docs/infrastructure_controls.md`. Rendering the architecture diagram is in `docs/architecture_rendering.md`.
