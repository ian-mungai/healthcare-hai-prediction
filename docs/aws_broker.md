---
title: AWS job broker
description: How the owner applies the lean job broker, records the first signed publishes and starts the bronze-triggered chain.
last_updated: 2026-10-30
audience: [developer]
---

# AWS job broker

The job broker is the only path from an Airflow DAG to work on AWS. It keeps the HTTP routes and the ledger of the host broker and starts ECS tasks in place of launcher containers. The DAGs and their client do not change. Nothing here has run against AWS. The offline checks cover the adapter, the broker, the chain DAG, the Terraform plans and the policies. The first real run proves the rest.

## What runs

| Part | Where | What it does |
| --- | --- | --- |
| Broker service | The container host, loopback port 18790 | Takes a DAG step, writes the ledger line, starts or joins the job task and reports its state |
| Job task | Fargate, on demand | One task per job, run and attempt. A job is `train`, `silver`, `gold`, `features`, `drift`, `promote` or `metadata` |
| Chain DAG `bronze_chain_dag` | Airflow | The owner starts it by hand. It asks the broker whether the chain is open, then triggers silver, gold, features and drift in order and last starts `metadata_ingest_dag` without waiting for it |

The table `config/orchestration/ecs_broker_jobs.json` names the job of each DAG step. A step with no job (acquire, seal, release and the gates) answers at once. The first step of a job starts its task and the later steps join it, because the containers of one job share a workspace. A job passes only when every container exits 0 and every acceptance container appears in the task report. A job that runs past its deadline is stopped.

The `train`, `bronze`, `silver`, `gold`, `features` and `promote` jobs have task definitions behind flags that default to false. The `promote` job uses the training task role and is set by `promote_enabled`. The `bronze` job is set by `bronze_job_enabled`. The `drift` job has its task definition behind `drift_enabled` in `infra/jobs` and its role behind `drift_prepared` in `infra/iam`. Both default to false. A job name stays out of `broker_job_families` until its task definition and role exist.

The `metadata` job starts the on-demand OpenMetadata catalog task. Its task definition is set by `openmetadata_enabled` in `infra/jobs` and its role by `openmetadata_prepared` in `infra/iam` (see the OpenMetadata catalog document). The task ends by itself at its lifetime. The broker deadline in `config/orchestration/ecs_broker_jobs.json` is that lifetime plus 900 seconds, 8100 seconds for the default of 7200. Change both together, because an offline case compares them.

## Apply order

Each apply is the owner's. Show the plan first and apply exactly that plan.

1. `infra/iam` (administrator profile): set `broker_prepared = true`. Add each job family to `broker_job_families` only after its task definition and role exist. The apply creates the role `broker_task` and the token grant on the host execution role.
2. Policy scripts (administrator profile): `manage_policies plan`, then `manage_policies apply` with the typed confirmation. The project user's policy now passes the `broker_task` role.
3. Token (administrator profile): put a new random value in the parameter `broker_token` without printing it.

   ```sh
   umask 077
   openssl rand -hex 32 > "$TMPDIR/broker_token"
   aws ssm put-parameter --profile <ADMIN_PROFILE> --region <REGION> --name /healthcare_hai_prediction/broker_token --type SecureString --value "file://$TMPDIR/broker_token"
   ```

4. `infra/services`: set `broker_enabled = true` and set `broker_task_role_arn` to the ARN in the `broker_roles` output. Set `broker_jobs` to the job subnet, the job security groups, the jobs log group, the data bucket, the private address of the host and the families. `security_group_ids` holds one group per network class: `lakehouse` is the group of the same name in the `job_network` output of `infra/iam`. The group `training` is `security_group_id` of the `training_network` output. The silver, gold and features jobs run in `lakehouse`, which reaches the catalog and Redis. The train, promote and drift jobs run in `training`, which also reaches MLflow. The broker gives each task only the group of its own class. Each family you enable needs its class here. Then run the host preparation task again. It creates the broker state folder on the data volume.
5. Check that the broker service reached a `COMPLETED` deployment, as for the other services.
6. For each run, stage the code and add the digest beside it. Build the archive with `scripts.lakehouse.code_archive`, not with `git archive` alone: the builder adds the verified dbt packages from the pinned local package archive (on a fresh clone, rebuild that archive first with `python -m scripts.lakehouse.dbt_package_archive fetch`), because the task has no network to install them. The same revision and packages always give the same bytes. The digest file is the code digest that the broker passes to the task. The pin refuses an archive without the packages before it claims the run ID. A chain needs this once, under the bronze run ID. The chain gate names that run for the four layer runs and for the training that the drift run triggers. Each of them fetches the bronze run's code and reads its digest. The broker tells the task which run holds the code in the variable `CODE_RUN_ID`. A drift run that someone starts by hand has its own staged code. The training it triggers fetches that code:

   ```sh
   .venv/bin/python -m scripts.lakehouse.code_archive build --output /tmp/code_<RUN_ID>
   aws s3 cp /tmp/code_<RUN_ID>/code.tar.gz s3://<DATA_BUCKET>/lakehouse/run_stage/<RUN_ID>/inputs/code.tar.gz --profile <PROJECT_PROFILE>
   aws s3 cp /tmp/code_<RUN_ID>/code.sha256 s3://<DATA_BUCKET>/lakehouse/run_stage/<RUN_ID>/inputs/code.sha256 --profile <PROJECT_PROFILE>
   ```

7. After new bronze data is registered and the first publishes are signed, start `bronze_chain_dag` by hand as described below.

The training DAG sends `mlflow=local` because the DAG runtime accepts nothing else. The broker passes `mlflow=aws` to the task.

## Promote a model

A promotion is a run of `model_promotion_dag`. The broker starts the `promote` job only when the owner has approved exactly that promotion. Stage the code as above under the promotion's run ID. Then write the approval as one file per run and copy it beside the code:

```sh
printf '{"run_id": "<RUN_ID>", "dag_id": "model_promotion_dag", "scope": "promote", "approved_on": "<YYYY-MM-DD>", "hai_type": "<HAI_TYPE>", "model_version": "<VERSION>"}' > /tmp/promotion_approval.json
aws s3 cp /tmp/promotion_approval.json s3://<DATA_BUCKET>/lakehouse/run_stage/<RUN_ID>/inputs/promotion_approval.json --profile <PROJECT_PROFILE>
```

Use `"scope": "rollback"` for a rollback. The record must name the run, the DAG, the operation, the HAI type and the model version of the request. Its date must not be after today. Any other field or value is refused. The record covers one run, so each promotion needs its own. Register the task definition in `infra/jobs` with `promote_enabled = true` after `train_enabled`. Name `promote` in the families of the IAM and services stacks. Then trigger `model_promotion_dag` in Airflow with the same run ID, HAI type and version. Training runs need no approval record.

## Layer jobs

Each of the `silver`, `gold` and `features` jobs is one Fargate task. The first container (`init`) hands the two volumes to the job user, as for training. Then the containers of the job table run in order, each after the success of the one before. Each runs one phase of `scripts/lakehouse/layer_run.py`, so the code that runs is the archive the run staged.

| Job | Phases in order | Reads | Writes |
| --- | --- | --- | --- |
| `silver` | fetch, pin, build, validate, verify, export, publish, upload | bronze through the reader pair | silver tables and its run prefix |
| `gold` | fetch, pin, build, validate, export, publish, upload | the silver run named by `INPUT_RUN_ID` | gold tables, catalog backups and its run prefix |
| `features` | fetch, pin, build, validate, export, publish, feast | the gold run named by `INPUT_RUN_ID` | features tables, the Feast registry object |

The publish phase runs the publish acceptance checks and exits non-zero on any failure. Before the commit, every table's row count and content fingerprint must equal its candidate snapshot. The candidates must also be exactly the contract's tables. After the commit, the commit record must name every candidate snapshot ID and follow the backup that followed the reconcile. The `publish_runs` row is the last catalog write of the publish step. The upload and feast phases refuse a run whose records fail these checks. A run ID is used once: the pin phase writes `state/pinned.json` under the run prefix only if it does not exist, so a second pin of the same run ID fails by name before it writes anything shared. An interrupted run restarts under a new run ID after the owner has checked the abandoned catalog branch. A dry run takes no run ID and leaves no state. Before any catalog write it also lists the versions of the input prefix again and compares them with the listing taken at the pin: bronze for silver, silver for gold and gold for features. Any added, replaced or deleted version stops the publish. A prefix too large to list within the bound is refused. Silver must pin exactly the tables of `config/lakehouse/bronze_tables.json`. Gold must pin exactly the tables of the silver publish contract. That is a contract check and not a comparison with the silver `publish_runs` row, so the owner compares that row by hand at the first gold run.

Apply the layer jobs in this order. Each apply is the owner's.

1. `infra/iam` (administrator profile): set `job_writer_layers_prepared = true` and `job_layer_runs_prepared = true`, then plan and apply. The roles read only their input layer and may list the object versions of that one prefix. Silver and gold put objects under `lakehouse/layer_runs/<layer>/` only. Features reads and writes the one Feast registry object and reads the Redis password parameter.
2. Put each layer's catalog client pair in the secret store, as for the writer roles.
3. `infra/jobs`: set `layer_jobs_enabled = true` and `layer_jobs = ["silver"]` first, with the image digests, the role ARNs from the `job_roles` output, `writer_parameter_names` and the jobs log group. Add `gold` and `features` after the silver publish passes. For features, set `layer_feast_redis_host`. The output `layer_run_tasks` lists the variables of each container.
4. Add the family to `broker_job_families` in `infra/iam` and apply, then add it to `broker_jobs` in `infra/services`.

### Dry run

A dry run runs the phases up to export for real, because they write only the task's own folder. The publish phase then reads the catalog without writing and prints the plan of the real publish: each table with rows to write, whether it exists, rows added and deleted, the `publish_runs` row, the backup name and the run branch. It saves nothing to the bucket, the upload phase uploads nothing and the feast phase does nothing. Read the plan in the jobs log group, stream `<layer>/publish/<task id>`. The line that starts with `DRY_RUN_PLAN` and a space holds the whole plan as JSON. If the plan holds a line that starts with `UNKNOWN` or `CATALOG`, the dry run could not read that catalog state. The plan is still printed and saved. Then the publish phase exits with an error, so a green dry run always has a complete plan.

Set the override `LAYER_DRY_RUN=1` on every container of a hand-started task. The first dry phase marks the run folder, so a later phase without the override stays dry.

Write the overrides with `scripts.orchestration.job_overrides`. It reads the containers and the empty variables of the job from the broker table and refuses by name when a value is missing, so a task never starts with a variable empty. `--code-run` is the run whose staged code the task fetches: the approved run ID itself for a standalone run, the bronze run ID for a chain child. Silver:

```sh
.venv/bin/python -m scripts.orchestration.job_overrides --job silver --run <RUN_ID> --code-run <CODE_RUN_ID> --code-sha256 <CODE_SHA256> \
  --polaris-uri http://<HOST_PRIVATE_IP>:8181/api/catalog --dry-run > overrides.json
```

Gold needs the committed silver run as its input run and features needs the committed gold run:

```sh
.venv/bin/python -m scripts.orchestration.job_overrides --job gold --run <RUN_ID> --code-run <CODE_RUN_ID> --code-sha256 <CODE_SHA256> \
  --polaris-uri http://<HOST_PRIVATE_IP>:8181/api/catalog --input-run <SILVER_RUN_ID> --dry-run > overrides.json
.venv/bin/python -m scripts.orchestration.job_overrides --job features --run <RUN_ID> --code-run <CODE_RUN_ID> --code-sha256 <CODE_SHA256> \
  --polaris-uri http://<HOST_PRIVATE_IP>:8181/api/catalog --input-run <GOLD_RUN_ID> --dry-run > overrides.json
```

Pass `--overrides file://overrides.json` to the run-task call. For a real run, leave out `--dry-run`.

## Bronze job

The `bronze` job writes one new source capture into bronze. It is one Fargate task with the `init` step and the containers `fetch`, `pin`, `load` and `check`. Each runs one phase of `scripts/lakehouse/bronze_run.py` from the archive the run staged. The task role is the bronze writer role and only the `load` container receives the bronze catalog pair. The job is not part of the signed chain, because bronze comes first.

Two values change from run to run, the tables and the target. Both come at dispatch from a file the run stages beside its code archive. Both are checked twice. The broker reads `bronze_request.json` before it starts the task. It refuses a table outside the table map, a repeated table, a target other than `real` or `qualify`, a qualification run that spans two collections and any key beyond the three below. The `pin` phase checks the same values against the table map of the unpacked code. The `load` and `check` phases refuse values that differ from the pin.

```json
{"tables": ["<TABLE_A>", "<TABLE_B>"], "target": "qualify", "dry_run": true}
```

| Target | Writes |
| --- | --- |
| `real` | `bronze` and `bronze_dictionary` |
| `qualify` | `bronze_e2e` and `bronze_dictionary_e2e`, for the tables of one collection |

The `load` phase runs the existing bronze loader. It lists the versioned manifests of the collection, reads each object by its recorded version, checks its SHA-256 and size, replaces the rows of each object in place and publishes one metadata generation. A retry of one run therefore writes the same rows and no second generation. The broker also starts no second task for the same run and attempt. The `check` phase accepts the loader report only when there is exactly one report, it passed and it names the requested tables and no others. A failed phase fails the task.

A dry run (`"dry_run": true` or `BRONZE_DRY_RUN=1` on every container of a hand-started task) runs `pin`, then reads the manifests and every object version, checks each hash and size and prints the plan. It starts no loader and needs no catalog, so it writes nothing to the catalog or the bucket. Read the plan in the jobs log group, stream `bronze/load/<task id>`. The line that starts with `DRY_RUN_PLAN` and a space holds the plan as JSON.

Apply the bronze job in this order. Each apply is the owner's.

1. `infra/iam` (administrator profile): add `bronze` to `broker_job_families`, then plan and apply. The broker role gains the bronze task definition, the pass grant for the bronze writer role and the read of `bronze_request.json`. The bronze writer role gains the object prefixes of its two test twins.
2. Catalog (owner): the bronze writer principal needs create and load rights on the namespaces `bronze_e2e` and `bronze_dictionary_e2e` before a qualification run. The production namespaces keep their rights.
3. `infra/jobs`: set `bronze_job_enabled = true` with the Spark image digest, the execution and bronze writer role ARNs, `writer_parameter_names.write_bronze_task` and the jobs log group. The output `bronze_run_task` lists the variables of each container.
4. `infra/services`: add `bronze` to `broker_jobs.families` and apply, so the broker restarts with the family.
5. Stage the run as for the layer jobs and add the request beside the code archive:

   ```sh
   aws s3 cp /tmp/bronze_request.json s3://<DATA_BUCKET>/lakehouse/run_stage/<RUN_ID>/inputs/bronze_request.json --profile <PROJECT_PROFILE>
   ```

6. Submit the run on the host. The step name is `bronze_load` and the run ID is the one of the staged folder:

   ```sh
   curl -s -H "Authorization: Bearer $BROKER_TOKEN" -d '{"request_version": 2, "template": "bronze_load", "run_id": "<RUN_ID>", "mode": "real", "attempt": 1, "dag_id": "manual_bronze", "task_id": "bronze_load", "dag_run_id": "manual__<RUN_ID>"}' http://127.0.0.1:18790/v1/submit
   ```

   Poll `/v1/status` with `{"submission_id": "<RUN_ID>:bronze_load:real:1"}` until it is `succeeded` or `failed`. A second submit of the same run and attempt returns the same handle. A new attempt number starts a new task that repeats the same rows.

When a `real` run passes, the `check` log line prints the command that starts the chain by hand:

```sh
airflow dags trigger bronze_chain_dag --conf '{"run_id": "<BRONZE_RUN_ID>", "mode": "real"}'
```

A `qualify` run never prints it, because the chain reads production bronze.

The qualification tables stay in the test twins until the owner drops them from the catalog. No job removes them.

## Sign the first publishes

The chain stays closed until the first silver and gold publishes pass by hand and both reports are signed. The broker then needs a record of the two runs. Put it once, after the second signature (administrator profile):

```sh
aws ssm put-parameter --profile <ADMIN_PROFILE> --region <REGION> --name /healthcare_hai_prediction/first_publishes_signed --type String --value '{"silver": "<SILVER_RUN_ID>", "gold": "<GOLD_RUN_ID>"}'
```

Until the parameter exists and holds exactly those two run IDs, the broker refuses the chain gate and every silver, gold, features and drift job. The training job does not depend on it.

## Start the chain

The owner starts `bronze_chain_dag` by hand after new bronze data is registered. A sensor that starts it comes later. Airflow listens on the host's loopback port 8080. Reach it through a port forward and trigger the DAG with a configuration:

```sh
aws ssm start-session --profile <PROJECT_PROFILE> --region <REGION> --target <INSTANCE_ID> \
  --document-name AWS-StartPortForwardingSession --parameters portNumber=8080,localPortNumber=8080
```

```json
{"run_id": "<BRONZE_RUN_ID>", "mode": "real"}
```

Every wait in the chain has a limit. A layer's trigger gives up after eight hours (drift after three). The whole chain run ends after 28 hours. The chain starts the DAG `drift_monitoring_dag` and then starts the catalog task as its last step. The catalog start waits for nothing and handles its own trigger errors, so a failed or slow catalog task never fails or delays a chain run.

Every run ID and every triggered DAG run ID derives from the bronze run ID. A second trigger for the same bronze run names the DAG runs that exist and starts nothing. Gold needs a silver run that passed. Features needs a gold run that passed. A failed acceptance check therefore stops the chain before the later layers. A failed layer is not retried under the same IDs. Its pin phase writes `state/pinned.json` once, so a layer that failed after the pin cannot run again under its run ID. The chain derives the same child IDs from the same bronze run ID. After a failure, the owner checks the abandoned catalog branch. Then stage the code under a new run ID (step 6 of the setup) and trigger `bronze_chain_dag` with that new run ID. Do not clear the failed DAG run to retry it: the next attempt stops at the pin by name.

## Stop

Set `broker_enabled = false` in `infra/services` and apply the shown plan. The ledger on the data volume stays. Cancel a running job from its DAG task, which stops its Fargate task. The broker records the end only after the task reports STOPPED. It waits up to two minutes for that. If the task is still stopping then, the cancel fails by name and records nothing, so repeat it. A retry of a job is refused while the task of an earlier attempt has not reached STOPPED. Check that no job task runs:

```sh
aws ecs list-tasks --profile <PROJECT_PROFILE> --region <REGION> --cluster <CLUSTER_NAME> --desired-status RUNNING
```

## Not proven

- Whether `ecs:DescribeTasks` and `ecs:StopTask` honor the `HaiJob` tag condition. If not, the broker role can stop any task in the cluster.
- Whether the memory-backed folder in the scheduler is writable by the Airflow user.
- Whether `RunTask` returns the same task for a repeated client token after that task stopped.
- Whether the Airflow trigger operator renders the run ID templates as the offline Jinja check does.
- The `promote` task and phase against a real MLflow server: the offline cases use a file based registry stand-in.
- The bronze job on AWS. The driver ran only against stand-ins of the bucket and the loader. Whether the Spark image carries the driver's imports and `boto3`. Whether the bronze writer principal holds the catalog rights for the test twins.
- Any layer job on AWS. The driver ran only against stand-ins of the Spark, analytics, quality and Feast modules. The read-only plan module `publish_plan.py` never ran.
- Whether the rebuilt analytics image loads its DuckDB extensions as the layer user. After the owner rebuilds the image, run `docker run --rm --user 10001:10001 -e HOME=/tmp --read-only --tmpfs /tmp <ANALYTICS_IMAGE> duckdb -c "SET extension_directory='/opt/duckdb/extensions'; SET autoinstall_known_extensions=false; LOAD iceberg; LOAD httpfs; LOAD aws; LOAD avro; SELECT extension_name, loaded FROM duckdb_extensions() WHERE loaded"`. All four must show as loaded.
- Whether the Spark image carries boto3 for the fetch phase. Whether the quality and analytics images can run the driver's imports.
- Whether the task role reads of the input layer and the Feast registry are enough. Whether the Feast phase reaches the host Redis from the lakehouse network.
- The `drift` task definition and role against AWS. The offline plans and the chain cases cover only their declarations.
