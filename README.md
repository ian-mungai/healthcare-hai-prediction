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

## Services and ports

Every service runs as an Amazon Elastic Container Service (ECS) task in host network mode on the one container host, so each port below is a port of the host. Terraform declares the services in `infra/services/`. `tests/test_readme_service_ports.py` fails when these tables differ from that code, from `infra/iam/host_job_firewall.nft` or from the Terraform outputs.

An optional service starts only while its switch is on. Run the tests with `.venv/bin/python -m pytest tests/test_readme_service_ports.py`.

| Service | Terraform switch | Default |
| --- | --- | --- |
| `postgres` | None | On |
| `redis` | None | On |
| `polaris` | None | On |
| `airflow_apiserver` | None | On |
| `airflow_scheduler` | None | On |
| `airflow_dag_processor` | None | On |
| `airflow_triggerer` | None | On |
| `marquez` | `marquez_enabled` | Off |
| `openmetadata` | `openmetadata_persistent_enabled` | Off |
| `mlflow` | `mlflow_enabled` | Off |
| `model_api` | `model_api_enabled` | Off |
| `streamlit` | `streamlit_enabled` | Off |
| `monitoring` | `monitoring_enabled` | Off |

The Airflow DAG processor, the Airflow triggerer and the one-shot tasks open no port. A port with a health path answers HTTP on `127.0.0.1`. A port that says "open to jobs" is also open on the host private IP address, the `host_private_ip` output of the platform stack, to the job containers.

| Service | Container | Port | Use and job access | Health or metrics path |
| --- | --- | --- | --- | --- |
| `postgres` | `postgres` | 5432 | Database of Polaris, Airflow, Marquez, MLflow and OpenMetadata. Loopback only. | None. The container check runs `pg_isready`. |
| `redis` | `redis` | 6379 | Online feature store, password required. Host private IP, open to jobs. | None. The container check runs `redis-cli ping`. |
| `polaris` | `polaris` | 8181 | Polaris REST catalog under `/api/catalog`. Host private IP, open to jobs. | None. |
| `polaris` | `polaris` | 8182 | Polaris management interface. Loopback only. | `/q/health` |
| `airflow_apiserver` | `airflow_apiserver` | 8080 | Airflow web interface and API. Loopback only. | `/api/v2/version` |
| `airflow_scheduler` | `airflow_scheduler` | 8974 | Scheduler health server. Loopback only. | `/health` |
| `marquez` | `marquez_api` | 5000 | Marquez API. Loopback only. | None. |
| `marquez` | `marquez_api` | 5001 | Marquez admin interface. Loopback only. | `/healthcheck` |
| `marquez` | `marquez_web` | 3000 | Marquez web interface. Loopback only. | `/healthcheck` |
| `openmetadata` | `opensearch` | 9200 | Search index of OpenMetadata. Loopback only. | `/_cluster/health` |
| `openmetadata` | `openmetadata_server` | 8585 | OpenMetadata web interface and API under `/api`. Host private IP, open to jobs. | None. |
| `openmetadata` | `openmetadata_server` | 8586 | OpenMetadata admin interface. Not open to jobs. | `/healthcheck` |
| `mlflow` | `mlflow` | 5100 | MLflow tracking server and web interface. Host private IP, open to jobs. | `/health`. The container check also accepts the answer 401. |
| `model_api` | `model_api` | 8000 | Model API. Loopback only. | `/v1/live` |
| `streamlit` | `streamlit` | 8501 | Streamlit page. Loopback only. | `/_stcore/health` |
| `monitoring` | `prometheus` | 9090 | Prometheus web interface. Loopback only. | `/-/healthy` |
| `monitoring` | `grafana` | 3001 | Grafana web interface. Loopback only. | `/api/health` |
| `monitoring` | `statsd_exporter` | 9102 | Metrics of the StatsD exporter. Loopback only. | `/metrics` |
| `monitoring` | `statsd_exporter` | 9125 (UDP) | StatsD input for the Airflow metrics. Loopback only. | None. |

### Job container network

Job containers run on the Docker bridge `docker0` of the host. The host firewall applies these rules to their traffic in order. The first match decides. Rules that accept the return traffic of established connections are not listed.

| Chain | Destination | Port | Action |
| --- | --- | --- | --- |
| input | 169.254.169.254 | any | drop |
| input | any | udp 53 | accept |
| input | any | tcp 53 | accept |
| input | 169.254.170.2 | tcp 80 | accept |
| input | 127.0.0.1 | tcp 51679 | accept |
| input | Host's own addresses | tcp 5100, 6379, 8181, 8585 | accept |
| input | any | any | drop |
| forward | 169.254.169.254 | any | drop |
| forward | any | udp 53 | accept |
| forward | any | tcp 53 | accept |
| forward | any | tcp 443 | accept |
| forward | any | any | drop |

### Reach a port from your Mac

Session Manager forwards a local port to a port on the host loopback. Replace `<HOST_INSTANCE_ID>` with the instance ID of the host (the instance tagged `Name` `<PROJECT_NAME>_host_<ENVIRONMENT>`) and `<PORT>` with a port from the table. Use the project profile from `.env`.

1. Start the host:

   ```sh
   .venv/bin/python -m scripts.infrastructure.host_session start
   ```

2. Forward the port and keep the command running:

   ```sh
   aws ssm start-session --target <HOST_INSTANCE_ID> --document-name AWS-StartPortForwardingSession --parameters '{"portNumber":["<PORT>"],"localPortNumber":["<PORT>"]}'
   ```

3. Open `http://127.0.0.1:<PORT>` in a browser. A port with a health path answers at `http://127.0.0.1:<PORT><PATH>`.

For the OpenMetadata web interface, use the project document, which fixes the host port at 8585 and takes the local port as a parameter:

```sh
aws ssm start-session --target <HOST_INSTANCE_ID> --document-name <PROJECT_NAME>_openmetadata_forward_<ENVIRONMENT> --parameters localPortNumber=8585
```
