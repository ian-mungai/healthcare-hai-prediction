---
title: Monitoring stack
description: Prometheus, Grafana and a StatsD exporter on the AWS host, with files, memory limits, retention and owner steps.
last_updated: 2026-10-30
audience: [developer]
---

# Monitoring stack

Prometheus, Grafana and a StatsD exporter run as one ECS task named `monitoring` on the AWS container host. Everything listens on `127.0.0.1`, so the stack is reachable only through a Session Manager tunnel. The task is defined in `infra/services/monitoring.tf` and exists only while `monitoring_enabled` is true. The default is false. A plan with the flag false changes nothing, Airflow included.

## What runs

| Container | Image (mirrored to ECR by digest) | Port on the host | Purpose |
| --- | --- | --- | --- |
| `monitoring_prepare` | the Prometheus image | none | Hands the two data folders to the users of Prometheus and Grafana, then exits. It runs as root for a second with a read-only root file system and three capabilities |
| `prometheus` | `prom/prometheus` v3.15.1 | 9090 | Scrapes the targets, stores samples and evaluates the alert rules |
| `grafana` | `grafana/grafana` 13.2.5 (Ubuntu variant) | 3001 | One provisioned dashboard on the Prometheus data source |
| `statsd_exporter` | `prom/statsd-exporter` v0.31.0 | 9102 (web), 9125 (UDP) | Turns the StatsD events of Airflow into Prometheus metrics |

Airflow sends its metrics to `127.0.0.1:9125` when the flag is true (`AIRFLOW__METRICS__STATSD_*` in `infra/services/airflow.tf`). Grafana uses port 3001 because Marquez uses 3000. No Alertmanager runs, so an alert shows as firing in Prometheus and Grafana and sends no message.

## What the dashboard shows

The dashboard `monitoring/grafana/dashboards/hai_overview.json` has four rows:

- **DAG:** success rate over 24 hours, runs per hour by DAG and state, run duration at the 95th percentile, scheduler heartbeats, DAG files that fail to import and executor slots.
- **Jobs:** the 95th percentile duration of each task and the tasks finished per hour by state. The Spark, training and promotion jobs run behind Airflow tasks, so a task duration is the time the DAG waits for that job. No metric comes from inside the ECS job tasks.
- **API:** requests per second, the share of server errors and the 95th percentile latency. These panels stay empty until the model API exposes the metrics listed under `hai_api` in `monitoring/known_metrics.json` at `127.0.0.1:8000/metrics`.
- **Monitoring:** scrape targets that are up, the Prometheus store size against its limit and the StatsD events that the mapping drops.

The FinOps panel is not part of this stack. It comes after November 15 2026.

## Reaching it

Open a Session Manager port forward to the host for port 3001 (Grafana) or 9090 (Prometheus). Sign in to Grafana as `admin`. The password is the Parameter Store parameter `/healthcare_hai_prediction/grafana_admin_password` (variable `grafana_admin_password_name`). Terraform builds only the parameter ARN, so the value never enters a plan or the state. The execution role injects it into the Grafana container as `GF_SECURITY_ADMIN_PASSWORD`. No other container receives it.

## Files

Configuration comes from the repository checkout that Airflow already mounts (`dag_code_path`, by default `<data mount>/repo`), as read-only folders:

| Path | Role |
| --- | --- |
| `monitoring/prometheus/prometheus.yml` | Scrape jobs `prometheus`, `statsd_exporter` and `hai_api` at 30 second intervals |
| `monitoring/prometheus/alerts.yml` | Alert rules for the scheduler, DAG and task failures, DAG import errors, the exporter and the API |
| `monitoring/statsd/mapping.yml` | The Airflow StatsD names that become metrics, with a final rule that drops everything else |
| `monitoring/known_metrics.json` | Metrics of Prometheus and the exporter themselves, with the contract the model API has to meet |
| `monitoring/grafana/provisioning/datasources/prometheus.yml` | The one data source |
| `monitoring/grafana/provisioning/dashboards/dashboards.yml` | Loads the dashboard folder and keeps the user interface from changing it |
| `monitoring/grafana/dashboards/hai_overview.json` | The dashboard |

A change to a dashboard or rule is a change in the checkout and a restart of the monitoring service. Grafana rereads dashboard files every 60 seconds on its own.

## Memory

The host is short of memory. These limits are estimates, not measurements. They come from the expected size (a few hundred series, one dashboard, one user) and are set small. Replace them after the silver and gold runs show the real peak.

| Container | Hard limit (MiB) | Reservation (MiB) | Soft Go limit (MiB) |
| --- | --- | --- | --- |
| `prometheus` | 256 | 128 | 204 |
| `grafana` | 256 | 128 | 204 |
| `statsd_exporter` | 64 | 32 | 51 |
| `monitoring_prepare` | 32 | 16 | none |

The soft limit is 80 percent of the hard limit, set as `GOMEMLIMIT`, so the Go runtime collects garbage before the container reaches its limit. With Marquez on, the hard limits of all containers add up to 14304 MiB against the 14336 MiB the plan allows, so the stack fits by 32 MiB. It leaves no room for MLflow or the broker. The memory cut after measurement or a larger host comes before those services.

To change a limit, set `monitoring_memory_mib` in the services variable file. It holds a `limit` and a `reservation` for `prometheus`, `grafana` and `statsd_exporter`. The soft limit follows. A reservation above its limit is refused at plan time. So is a limit below 128 MiB (Prometheus, Grafana) or 32 MiB (exporter). A plan whose total passes the host budget is refused by the existing memory checks.

To measure, read the memory of each container during a silver and a gold run (`docker stats` on the host or the ECS task memory metric) and compare it with the table. A container that the kernel killed for memory restarts through its restart policy. ECS shows the stop reason.

## Retention and how it stops

- **Prometheus samples:** kept for 15 days (`prometheus_retention_days`) and at most 2048 MB (`prometheus_retention_size_mb`) of blocks and write-ahead log. Prometheus deletes the oldest blocks once either limit is passed. The check runs as it cuts and compacts blocks, so the folder can pass the limit briefly. Both flags are in the task definition (`--storage.tsdb.retention.time` and `--storage.tsdb.retention.size`). The variables refuse zero and more than 90 days or 16384 MB.
- **Series count:** the last rule of the mapping drops every StatsD event it does not list, so the number of series is bounded by the DAG and task names. The panel "StatsD events dropped by the mapping" shows what is dropped.
- **StatsD exporter:** keeps nothing on disk.
- **Grafana:** keeps only its small database and sessions on the data volume. Dashboards come from files and it stores no samples.
- **Container logs:** go to the `monitoring` CloudWatch log group, which keeps seven days.

Plan for about 2 GB on the data volume for Prometheus plus the Grafana database. This is a target, not a hard cap: retention deletes only whole blocks, so the write-ahead log, recent samples and a running compaction can pass it for a time.

## Owner steps

Nothing here is applied or pushed by the code change. Each step is the owner's, under the project profile unless noted.

1. **Foundation:** set `monitoring_enabled = true` in the platform variable file, then plan and apply the platform stack (three ECR repositories `prometheus`, `grafana` and `statsd_exporter` plus the `monitoring` log group) and the IAM stack (the execution role may read the Grafana password parameter and the project user may create it).
2. **Password:** create the Grafana password once. The command prints only whether it created the parameter:

   ```sh
   AWS_PROFILE=<PROJECT_PROFILE> .venv/bin/python -c "from pathlib import Path; from scripts.infrastructure import secret_store as s; print('created' if s.open_store(Path('.')).ensure(s.GRAFANA_ADMIN_PASSWORD, s.new_password)[1] else 'already present')"
   ```

3. **Images:** push the three images. The list pins each by digest (`infra/services/images.json`). Print the commands with `.venv/bin/python -m scripts.infrastructure.push_images plan --repository-urls <URLS_FILE> --profile <PROJECT_PROFILE> --region <REGION> --only prometheus grafana statsd_exporter`, run them, then record the digests with `push_images record prometheus=sha256:<DIGEST> grafana=sha256:<DIGEST> statsd_exporter=sha256:<DIGEST>`.
4. **Checkout:** make sure the repository checkout on the host contains the `monitoring/` folder at the Airflow code path.
5. **Flag:** set `monitoring_enabled = true` in the services variable file, plan, read the plan and apply it. The plan adds the `monitoring` task definition and service and changes the four Airflow task definitions (the StatsD settings), so Airflow restarts. Apply it when no DAG run is active.
6. **Check:** through the tunnel, open `http://127.0.0.1:9090/targets` and confirm the three jobs are up (`hai_api` stays down until the API runs). On the host, `curl -s http://127.0.0.1:9102/metrics | grep '^airflow_'` shows the Airflow series once a DAG has run.

To remove the stack, set the flag to false and apply. The service goes and Airflow restarts without StatsD. The two data folders stay on the volume until someone deletes them.

## Checks

- `terraform test` in `infra/services` (`tests/monitoring.tftest.hcl`) uses a mocked provider. It checks the flag-off plan, loopback binds, digests, the password path, the limits, the retention flags, the Airflow settings, users and mounts.
- `.venv/bin/python -m scripts.infrastructure.run_monitoring_e2e` runs the monitoring file check on the tracked files and on 19 copies that each carry one planted defect. The check (`scripts/infrastructure/monitoring_files.py`) parses every file, checks that each metric in a dashboard or alert comes from the mapping, the known list or the API contract, that label matchers name labels the metric has and that scrape jobs, ports and mounted folders agree with the Terraform task. The `configuration_checks` group runs it.

## What is not proven

- No image has run. The health check commands (`wget` in the Prometheus and exporter images, `bash` in the Grafana image) and the flags are from the image documentation.
- The Airflow StatsD names in the mapping come from the Airflow metrics reference. No scheduler has sent them to the exporter. The Airflow image must also contain the `statsd` package: check it with `python -c "import statsd"` inside the image.
- The model API does not exist yet, so its metric names are a contract and not an observation.
- The memory limits are estimates.
- The check reads PromQL with patterns, not a parser. It does not check label names after `by` and does not run a query.
