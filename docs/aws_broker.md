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

The `train` job exists today. Five jobs need task definitions first: `silver`, `gold`, `features`, `drift`, `promote`. The first four also need the layer driver. Their names stay out of `broker_job_families` until those exist. The `promote` job uses the training task role.

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

Use `"scope": "rollback"` for a rollback. The record must name the run, the DAG, the operation, the HAI type and the model version of the request. Its date must not be after today. Any other field or value is refused. The record covers one run, so each promotion needs its own. Then trigger `model_promotion_dag` in Airflow with the same run ID, HAI type and version. Training runs need no approval record.

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
- The task definitions and layer driver of the silver, gold, features and drift jobs.
- The task definition of the `promote` job and a `promote` phase in the AWS training run script. Until they exist the broker accepts and checks the approval but has no task to start.
