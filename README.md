# healthcare-hai-prediction

Predicts hospital healthcare-associated infection rates from public CMS, Census, BLS and state data on AWS.

**Status:** In development.

## Install

Prerequisites: macOS on Apple silicon, Python 3.12, Git, Terraform 1.16.1, AWS CLI v2, Node.js 22 or later and Docker Desktop.

1. Create the environment, install the pinned tools and the hash-locked host dependencies and install the hooks:

   ```sh
   python3.12 -m venv .venv
   .venv/bin/python scripts/quality/install_tools.py
   .venv/bin/python -m scripts.quality.lock_env export host --output "$TMPDIR/host_lock"
   .venv/bin/python -m pip install --require-hashes --only-binary=:all: -r "$TMPDIR/host_lock/requirements.txt"
   .venv/bin/python -m scripts.quality.install_markdownlint
   .venv/bin/python -m scripts.quality.install_sqlfluff
   .venv/bin/python -m scripts.quality.install_dvc
   .venv/bin/python -m scripts.quality.install_lint_tools
   .venv/bin/python -m scripts.lakehouse.dbt_package_archive fetch
   PRE_COMMIT_HOME="$PWD/.tools/pre_commit_cache" .venv/bin/python -m pre_commit install
   cp .env.example .env
   ```

2. Fill every value in `.env`. Git ignores the file. `.env.example` lists the settings:

   | Setting | Value |
   | --- | --- |
   | `AWS_REGION` | The AWS Region of the project |
   | `AWS_PROFILE` | The project profile, named `<PROJECT_NAME>_<ENVIRONMENT>` after its same-named IAM user |
   | `AWS_ACCOUNT_ID` | The AWS account ID |
   | `PROJECT_NAME` | The project name |
   | `ENVIRONMENT` | The environment name (`dev`) |
   | `S3_BUCKET` | The project data bucket |

3. Store no secret in `.env` or in any other file on the host. Project secrets live in AWS Systems Manager Parameter Store and the shared API keys in AWS Secrets Manager.

## Usage

Run the full local checks without AWS credentials. Each run writes its report under `data/e2e/`.

```sh
env PYTHON_BIN="$PWD/.venv/bin/python" TF_PLUGIN_CACHE_DIR="$PWD/infra/.terraform/providers" /bin/bash scripts/run_ci.sh
```

Run one end-to-end (E2E) check on its own. A runner that takes an output directory needs a new one:

```sh
.venv/bin/python -m scripts.quality.run_checks_e2e
```

Install the push gate once with `.venv/bin/python -m scripts.quality.pre_push install-shim`.
