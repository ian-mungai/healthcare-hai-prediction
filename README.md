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
   .venv/bin/python -m pip install --no-cache-dir --require-hashes --only-binary=:all: -r "$TMPDIR/host_lock/requirements.txt"
   .venv/bin/python -m pip check
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
   | `ALERT_EMAIL` | Optional. One address for every project alert topic (run failures and cost alerts); empty creates no subscription |

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

## Web interfaces

Six services have a web interface. You open each one from your Mac through its own Session Manager document, which fixes the host port and takes only the local port as a parameter. No other port of the host is reachable from the Mac. Every other service is internal. Session Manager refuses every other document, except the older `<PROJECT_NAME>_catalog_forward_<ENVIRONMENT>` document of the on-demand catalog task, which forwards only port 8585 and exists only while that task is prepared. Terraform declares the services in `infra/services/` and the documents in `infra/iam/`. `tests/test_readme_web_interfaces.py` fails when this section differs from that code or from the project policy.

An optional service runs only while its Terraform switch is on. Every switch defaults to off. Its document exists only while the service is prepared. Airflow always runs, so it has no switch.

| Service | Container | Port | Web interface | Health path | Terraform switch |
| --- | --- | --- | --- | --- | --- |
| `airflow_apiserver` | `airflow_apiserver` | 8080 | Airflow web interface and API | `/api/v2/version` | None |
| `openmetadata` | `openmetadata_server` | 8585 | OpenMetadata catalog and API under `/api` | None. | `openmetadata_persistent_enabled` |
| `mlflow` | `mlflow` | 5100 | MLflow tracking server | `/health`. The container check also accepts the answer 401. | `mlflow_enabled` |
| `streamlit` | `streamlit` | 8501 | Streamlit page | `/_stcore/health` | `streamlit_enabled` |
| `monitoring` | `grafana` | 3001 | Grafana dashboards | `/api/health` | `monitoring_enabled` |
| `monitoring` | `prometheus` | 9090 | Prometheus query page | `/-/healthy` | `monitoring_enabled` |

### Open a web interface from your Mac

Replace `<HOST_INSTANCE_ID>` with the instance ID of the host (the instance tagged `Name` `<PROJECT_NAME>_host_<ENVIRONMENT>`). Replace `<AWS_PROFILE>`, `<AWS_REGION>`, `<PROJECT_NAME>` and `<ENVIRONMENT>` with the values of the same names in `.env`.

1. Start the host:

   ```sh
   .venv/bin/python -m scripts.infrastructure.host_session start
   ```

2. Run the command of the interface you need and keep it running. The local port is the one the browser uses. Pick another number when a local service already holds it:

   ```sh
   aws ssm start-session --target <HOST_INSTANCE_ID> --document-name <PROJECT_NAME>_airflow_forward_<ENVIRONMENT> --parameters localPortNumber=8080 --profile <AWS_PROFILE> --region <AWS_REGION>
   aws ssm start-session --target <HOST_INSTANCE_ID> --document-name <PROJECT_NAME>_openmetadata_forward_<ENVIRONMENT> --parameters localPortNumber=8585 --profile <AWS_PROFILE> --region <AWS_REGION>
   aws ssm start-session --target <HOST_INSTANCE_ID> --document-name <PROJECT_NAME>_mlflow_forward_<ENVIRONMENT> --parameters localPortNumber=5100 --profile <AWS_PROFILE> --region <AWS_REGION>
   aws ssm start-session --target <HOST_INSTANCE_ID> --document-name <PROJECT_NAME>_streamlit_forward_<ENVIRONMENT> --parameters localPortNumber=8501 --profile <AWS_PROFILE> --region <AWS_REGION>
   aws ssm start-session --target <HOST_INSTANCE_ID> --document-name <PROJECT_NAME>_grafana_forward_<ENVIRONMENT> --parameters localPortNumber=3001 --profile <AWS_PROFILE> --region <AWS_REGION>
   aws ssm start-session --target <HOST_INSTANCE_ID> --document-name <PROJECT_NAME>_prometheus_forward_<ENVIRONMENT> --parameters localPortNumber=9090 --profile <AWS_PROFILE> --region <AWS_REGION>
   ```

3. Open `http://127.0.0.1:<LOCAL_PORT>` in a browser. A health path answers at `http://127.0.0.1:<LOCAL_PORT><PATH>`.
