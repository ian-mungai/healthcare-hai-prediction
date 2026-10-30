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
| Job task | Fargate, on demand | One task per job, run and attempt. A job is `train`, `silver`, `gold`, `features`, `drift` or `promote` |
| Chain DAG `bronze_chain_dag` | Airflow | The owner starts it by hand. It asks the broker whether the chain is open, then triggers silver, gold, features and drift in order |

The table `config/orchestration/ecs_broker_jobs.json` names the job of each DAG step. A step with no job (acquire, seal, release and the gates) answers at once. The first step of a job starts its task and the later steps join it, because the containers of one job share a workspace. A job passes only when every container exits 0 and every acceptance container appears in the task report. A job that runs past its deadline is stopped.

The `train`, `silver`, `gold`, `features` and `promote` jobs have task definitions behind flags that default to false. The `promote` job uses the training task role and is set by `promote_enabled`. The `drift` job still needs its own task definition and role. A job name stays out of `broker_job_families` until its task definition and role exist.

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

4. `infra/services`: set `broker_enabled = true` and set `broker_task_role_arn` to the ARN in the `broker_roles` output. Set `broker_jobs` to the job subnet, the job security group, the jobs log group, the data bucket, the private address of the host and the families. Then run the host preparation task again. It creates the broker state folder on the data volume.
5. Check that the broker service reached a `COMPLETED` deployment, as for the other services.
6. For each run, stage the code as for a training run and add the digest beside it. A chain needs this once, under the bronze run ID. The chain gate names that run for the four layer runs, so each layer reads the bronze run's digest:

   ```sh
   git archive --format=tar.gz --output=/tmp/code.tar.gz HEAD
   shasum -a 256 /tmp/code.tar.gz | cut -d ' ' -f 1 > /tmp/code.sha256
   aws s3 cp /tmp/code.tar.gz s3://<DATA_BUCKET>/lakehouse/run_stage/<RUN_ID>/inputs/code.tar.gz --profile <PROJECT_PROFILE>
   aws s3 cp /tmp/code.sha256 s3://<DATA_BUCKET>/lakehouse/run_stage/<RUN_ID>/inputs/code.sha256 --profile <PROJECT_PROFILE>
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

The publish phase runs the publish acceptance checks and exits non-zero on any failure. Before the commit, every table's row count and content fingerprint must equal its candidate snapshot. The candidates must also be exactly the contract's tables. After the commit, the commit record must name every candidate snapshot ID and follow the backup that followed the reconcile. The `publish_runs` row is the last catalog write of the publish step. The upload and feast phases refuse a run whose records fail these checks. A retry of the same run repeats the steps on the same keys and writes no second publication.

Apply the layer jobs in this order. Each apply is the owner's.

1. `infra/iam` (administrator profile): set `job_writer_layers_prepared = true` and `job_layer_runs_prepared = true`, then plan and apply. The roles read only their input layer. Silver and gold put objects under `lakehouse/layer_runs/<layer>/` only. Features reads and writes the one Feast registry object and reads the Redis password parameter.
2. Put each layer's catalog client pair in the secret store, as for the writer roles.
3. `infra/jobs`: set `layer_jobs_enabled = true` and `layer_jobs = ["silver"]` first, with the image digests, the role ARNs from the `job_roles` output, `writer_parameter_names` and the jobs log group. Add `gold` and `features` after the silver publish passes. For features, set `layer_feast_redis_host`. The output `layer_run_tasks` lists the variables of each container.
4. Add the family to `broker_job_families` in `infra/iam` and apply, then add it to `broker_jobs` in `infra/services`.

### Dry run

A dry run runs the phases up to export for real, because they write only the task's own folder. The publish phase then reads the catalog without writing and prints the plan of the real publish: each table with rows to write, whether it exists, rows added and deleted, the `publish_runs` row, the backup name and the run branch. It saves nothing to the bucket, the upload phase uploads nothing and the feast phase does nothing. Read the plan in the jobs log group, stream `<layer>/publish/<task id>`. The line that starts with `DRY_RUN_PLAN` and a space holds the whole plan as JSON.

Set the override `LAYER_DRY_RUN=1` on every container of a hand-started task. The first dry phase marks the run folder, so a later phase without the override stays dry.

```sh
OVERRIDES=$(for n in fetch pin build validate verify export publish upload; do jq -n --arg n "$n" --arg r "$RUN_ID" --arg c "$CODE_SHA256" --arg p "$POLARIS_URI" \
  '{name:$n,environment:[{name:"RUN_ID",value:$r},{name:"CODE_SHA256",value:$c},{name:"POLARIS_URI",value:$p},{name:"LAYER_DRY_RUN",value:"1"}]}'; done | jq -s '{containerOverrides:.}')
```

Use the containers of the job instead of this silver list. Pass `--overrides "$OVERRIDES"` to the run-task call.

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

Every wait in the chain has a limit. A layer's trigger gives up after eight hours (drift after three). The whole chain run ends after 28 hours. The DAG `drift_monitoring_dag` lives on the drift branch, so the drift step has no child to start until that branch is combined. A check in the offline suite fails at that point until the branch's DAG is listed as present.

Every run ID and every triggered DAG run ID derives from the bronze run ID. A second trigger for the same bronze run names the DAG runs that exist and starts nothing. Gold needs a silver run that passed. Features needs a gold run that passed. A failed acceptance check therefore stops the chain before the later layers. After a failure, clear the failed layer's own DAG run to retry it.

## Stop

Set `broker_enabled = false` in `infra/services` and apply the shown plan. The ledger on the data volume stays. Cancel a running job from its DAG task, which stops its Fargate task. Check that no job task runs:

```sh
aws ecs list-tasks --profile <PROJECT_PROFILE> --region <REGION> --cluster <CLUSTER_NAME> --desired-status RUNNING
```

## Not proven

- Whether `ecs:DescribeTasks` and `ecs:StopTask` honor the `HaiJob` tag condition. If not, the broker role can stop any task in the cluster.
- Whether the memory-backed folder in the scheduler is writable by the Airflow user.
- Whether `RunTask` returns the same task for a repeated client token after that task stopped.
- Whether the Airflow trigger operator renders the run ID templates as the offline Jinja check does.
- The `promote` task and phase against a real MLflow server: the offline cases use a file based registry stand-in.
- Any layer job on AWS. The driver ran only against stand-ins of the Spark, analytics, quality and Feast modules. The read-only plan module `publish_plan.py` never ran.
- Whether the Spark image carries boto3 for the fetch phase. Whether the quality and analytics images can run the driver's imports.
- Whether the task role reads of the input layer and the Feast registry are enough. Whether the Feast phase reaches the host Redis from the lakehouse network.
- The `drift` task definition and role.
