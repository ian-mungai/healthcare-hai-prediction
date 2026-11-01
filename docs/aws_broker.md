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
| Job task | Fargate, on demand | One task per job, run and attempt. A job is `train`, `bronze`, `silver`, `gold`, `features`, `drift`, `promote`, `serve` or `metadata` |
| Chain DAG `bronze_chain_dag` | Airflow | The owner starts it by hand. It asks the broker whether the chain is open, then triggers silver, gold, features and drift in order and last starts `metadata_ingest_dag` without waiting for it |

The table `config/orchestration/ecs_broker_jobs.json` names the job of each DAG step. A step with no job (acquire, seal, release and the gates) answers at once. The first step of a job starts its task and the later steps join it, because the containers of one job share a workspace. A job passes only when every container exits 0 and every acceptance container appears in the task report. A job that runs past its deadline is stopped.

The `train`, `bronze`, `silver`, `gold`, `features` and `promote` jobs have task definitions behind flags that default to false. The `promote` job uses the training task role and is set by `promote_enabled`. The `bronze` job is set by `bronze_job_enabled`. The `drift` job has its task definition behind `drift_enabled` in `infra/jobs` and its role behind `drift_prepared` in `infra/iam`. Both default to false. A job name stays out of `broker_job_families` until its task definition and role exist.

The `serve` job deploys a registered model version beside the production model or rolls the canary back for `serving_deploy_dag`. Its step is `serve_deploy`. The steps `acquire_run`, `snapshot_serving`, the two serving gates and the release steps answer at once. The owner stages `serving_request.json` beside the code archive of the run, with the HAI type, the mode, the candidate version and the percent. The broker reads it once, checks it with `orchestration/serving_contract.py`, keeps the checked request with the intent and passes it to the `serve` container. Every attempt of the run must use the request its first attempt had checked. The broker refuses a changed one and asks for a new run ID. The task has the containers `fetch` and `serve`, runs in the training network, uses the training image and its own role `job_serve_task` and logs in to MLflow with the administrator password. The deploy tool stops after 600 seconds and the broker deadline is 900 seconds. The task definition is set by `serve_enabled` in `infra/jobs`, the role by `serve_prepared` in `infra/iam`. Both default to false. The name `serve` stays out of `broker_job_families` and `broker_jobs.families` until the owner names it.

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

Each of the `silver`, `gold` and `features` jobs is one Fargate task. The first container (`init`) hands the two volumes to the job user, as for training. Then the containers of the job table run in order, each after the success of the one before. Each runs one phase of `scripts/lakehouse/layer_run.py`, so the code that runs is the archive the run staged. A `guard` container, with no dependency, runs beside the phases. It is essential, like the last phase. It reads the task metadata endpoint and exits with an error as soon as a phase is stopped with an error or without an exit code, which stops the whole task. Without it a failed phase leaves the later phases waiting for its success and the task PENDING until somebody stops it.

| Job | Phases in order | Reads | Writes |
| --- | --- | --- | --- |
| `silver` | fetch, pin, build, validate, verify, export, publish, upload | bronze through the reader pair | silver tables and its run prefix |
| `gold` | fetch, pin, build, validate, export, publish, upload | the silver run named by `INPUT_RUN_ID` | gold tables, catalog backups and its run prefix |
| `features` | fetch, pin, build, validate, export, publish, feast | the gold run named by `INPUT_RUN_ID` | features tables, the Feast registry object |

The publish phase runs the publish acceptance checks and exits non-zero on any failure. Before the commit, every table's row count and content fingerprint must equal its candidate snapshot. The candidates must also be exactly the contract's tables. After the commit, the commit record must name every candidate snapshot ID and follow the backup that followed the reconcile. The `publish_runs` row is the last catalog write of the publish step. The upload and feast phases refuse a run whose records fail these checks. A run ID is used once: the pin phase writes `state/pinned.json` under the run prefix only if it does not exist, so a second pin of the same run ID fails by name before it writes anything shared. An interrupted run restarts under a new run ID after the owner has checked the abandoned catalog branch. A dry run takes no run ID and leaves no state. Before any catalog write it also lists the versions of the input prefix again and compares them with the listing taken at the pin: bronze for silver, silver for gold and gold for features. Any added, replaced or deleted version stops the publish. A prefix too large to list within the bound is refused. Silver must pin exactly the tables of `config/lakehouse/bronze_tables.json`. The gold pin step reads the selected silver `publish_runs` row and pins its complete snapshot set. The publish phase then checks the pinned tables against the tables of the silver publish contract. The first gold run adds a hand check: the report compares the gold pin with the accepted silver record, which the silver run compared with its `publish_runs` row.

The owner runs silver first and adds gold and features after the silver publish passes. The numbered list under "Silver first run" is the whole sequence from clean defaults.

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

## Silver first run

Nothing in this list has run against AWS. Each apply is the owner's. Show the plan first and apply exactly that plan. Run every command from the repository root of the main checkout. The list starts from the state that `docs/infrastructure_controls.md` and `docs/aws_control_plane.md` describe: the storage, IAM, platform and services stacks are applied, the catalog `hai_lakehouse` exists and the bronze tables are registered in it. The host may be off. Keep every file this list saves in one new folder under the ignored `plans/` folder. The list calls it `<RECORD_DIR>`. Signed reports stay in that folder and never go into Git.

1. Read the platform values (project profile). Keep the cluster name, the project tag, the jobs group from the output log_group_names and the image repository addresses:

   ```sh
   mkdir -p <RECORD_DIR>
   terraform -chdir=infra/platform output -raw cluster_name
   terraform -chdir=infra/platform output -raw project_tag
   terraform -chdir=infra/platform output -json log_group_names
   terraform -chdir=infra/platform output -json ecr_repository_urls > <RECORD_DIR>/repository_urls.json
   ```

2. `infra/iam` (administrator profile): set `job_writer_layers_prepared`, `job_layer_runs_prepared` and `job_endpoints_enabled` to `true` in `config/aws/iam_preparation.json`. The file stays on your machine and is never committed. The third flag creates the private endpoints for ECR, the log service, Parameter Store and S3 and their HTTPS rules. Without it the silver task has no way to pull its image. `<PLAN>` is a file name such as `silver_iam.tfplan`. The wrapper writes it in the stack folder and reads it from there on apply:

   ```sh
   bash scripts/infrastructure/terraform.sh --iam --admin-profile <ADMIN_PROFILE> plan -out <PLAN>
   bash scripts/infrastructure/terraform.sh --iam --admin-profile <ADMIN_PROFILE> apply <PLAN>
   ```

   The plan must add the roles `job_write_silver_task`, `job_write_gold_task` and `job_write_features_task`, the layer run grants and the endpoints. It can also replace the host launch template, because the template reads the newest recommended machine image and the applied template may be older. That change is approved and step 4 tests it. Stop on any other change. Stop on any destroy of a role or a policy. After the apply read the outputs and stop if `endpoints_enabled` is not `true`:

   ```sh
   terraform -chdir=infra/iam output -json job_network
   terraform -chdir=infra/iam output -json job_roles
   terraform -chdir=infra/iam output -json job_writer_layer_contract
   ```

   Keep the `subnet_id` and the `lakehouse` security group of `job_network`, the ARNs of `execution` and `write_silver_task` in `job_roles` and the two silver names in `job_writer_layer_contract`.
3. Policy scripts (administrator profile): show the plan, then apply with the typed confirmation. The project user's policy must pass `job_execution` and `job_write_silver_task`, start tagged tasks in the project cluster and read log events:

   ```sh
   .venv/bin/python -m infra.iam.scripts.manage_policies plan --admin-profile <ADMIN_PROFILE>
   CONFIRM_IAM_POLICIES=apply-healthcare-hai-prediction-policies .venv/bin/python -m infra.iam.scripts.manage_policies apply --admin-profile <ADMIN_PROFILE>
   ```

4. Test host start (project profile). If step 2 replaced the launch template, this is the first start on the new machine image. Every service must reach a stable state before step 5. Stop the list on any failure. Then find the host. Require exactly one result and keep its instance ID and private address:

   ```sh
   .venv/bin/python -m scripts.infrastructure.host_session start
   .venv/bin/python -m scripts.infrastructure.host_session status
   aws ec2 describe-instances --profile <PROJECT_PROFILE> --region <REGION> --filters Name=tag:Name,Values=<PROJECT_NAME>_host_<ENVIRONMENT> Name=tag:Project,Values=<PROJECT_TAG> Name=instance-state-name,Values=running --query 'Reservations[].Instances[].{id:InstanceId,address:PrivateIpAddress}'
   ```

5. Catalog identities (project profile): create the reader and the silver writer, approve the namespace `silver` for the writer and read the stored pair names back. The check reports the pairs that the project user's policy lets it read. The `features` pair stays with the administrator and the check names it as an administrator item without reading it:

   ```sh
   .venv/bin/python -m scripts.lakehouse.aws_catalog_setup principal --identity reader --instance <HOST_INSTANCE_ID>
   .venv/bin/python -m scripts.lakehouse.aws_catalog_setup principal --identity silver --instance <HOST_INSTANCE_ID> --namespace-approval <APPROVAL_ID>
   .venv/bin/python -m scripts.lakehouse.aws_catalog_setup check
   ```

   Both `aws_polaris_reader` parameters and both `aws_polaris_silver` parameters must show a type and a version.
6. Images (project profile): the repositories `spark`, `analytics` and `quality` exist by default. Print the plan of the build and push, run the printed commands one by one and record the pushed digests. Each image reference is `<ACCOUNT>.dkr.ecr.<REGION>.amazonaws.com/<PROJECT_NAME>_<IMAGE>_<ENVIRONMENT>@sha256:<DIGEST>`. The analytics image must come from this tree, because it carries the DuckDB extensions for the layer user. Check that it loads them as that user before any task starts. All four extensions must show as loaded:

   ```sh
   .venv/bin/python -m scripts.infrastructure.push_images plan --repository-urls <RECORD_DIR>/repository_urls.json --profile <PROJECT_PROFILE> --region <REGION> --only spark analytics quality
   docker run --rm --user 10001:10001 -e HOME=/tmp --read-only --tmpfs /tmp <ANALYTICS_IMAGE> duckdb -c "SET extension_directory='/opt/duckdb/extensions'; SET autoinstall_known_extensions=false; LOAD iceberg; LOAD httpfs; LOAD aws; LOAD avro; SELECT extension_name, loaded FROM duckdb_extensions() WHERE loaded"
   ```

7. `infra/jobs` (project profile): write a reviewed file `jobs_silver.json` in `<RECORD_DIR>` and bind it to its digest. It holds `layer_jobs_enabled` set to `true`, `layer_jobs` set to `["silver"]`, the three references from step 6 in `images`, the ARNs `execution` and `write_silver_task` of the `job_roles` output from step 2 in `job_role_arns`, the two silver names of the `job_writer_layer_contract` output from step 2 in `writer_parameter_names` under `write_silver_task` and the jobs group of the platform output log_group_names from step 1 in `log_group_name`:

   ```json
   {"schema_version": 1, "root": "jobs", "variables": {"layer_jobs_enabled": true, "layer_jobs": ["silver"], "images": {"analytics": "<REF>", "quality": "<REF>", "spark": "<REF>"},
    "job_role_arns": {"execution": "<EXECUTION_ARN>", "write_silver_task": "<SILVER_TASK_ARN>"}, "writer_parameter_names": {"write_silver_task": ["<ID_NAME>", "<SECRET_NAME>"]}, "log_group_name": "<JOBS_GROUP>"}}
   ```

   ```sh
   shasum -a 256 <RECORD_DIR>/jobs_silver.json
   bash scripts/infrastructure/terraform.sh --jobs --preparation-inputs <RECORD_DIR>/jobs_silver.json --preparation-sha256 <SHA256> plan -out <PLAN>
   bash scripts/infrastructure/terraform.sh --jobs --preparation-inputs <RECORD_DIR>/jobs_silver.json --preparation-sha256 <SHA256> apply <PLAN>
   ```

   The plan must register one task definition, `<PROJECT_NAME>_<ENVIRONMENT>_job_silver` and nothing else. After the apply, `terraform -chdir=infra/jobs output -json layer_run_tasks` must list `silver` with the network `lakehouse` and no public address.
8. Stage the code and approve the run. Choose a new run ID. Use it for the dry run and the real run. It is also the code run ID. The archive comes from `scripts.lakehouse.code_archive`, which adds the verified dbt packages. Read "Apply order" step 6 for the rebuild of the package archive on a fresh clone. The code digest is the content of `code.sha256`. The approval record carries the run ID and the code digest. The owner types the text that the command prints, so it is written only on purpose and only before the task exists:

   ```sh
   printf '%s-%s\n' "$(date -u +%Y%m%dT%H%M%SZ)" "$(openssl rand -hex 4)"
   .venv/bin/python -m scripts.lakehouse.code_archive build --output /tmp/code_<RUN_ID>
   aws s3 cp /tmp/code_<RUN_ID>/code.tar.gz s3://<DATA_BUCKET>/lakehouse/run_stage/<RUN_ID>/inputs/code.tar.gz --profile <PROJECT_PROFILE> --region <REGION>
   aws s3 cp /tmp/code_<RUN_ID>/code.sha256 s3://<DATA_BUCKET>/lakehouse/run_stage/<RUN_ID>/inputs/code.sha256 --profile <PROJECT_PROFILE> --region <REGION>
   cat /tmp/code_<RUN_ID>/code.sha256
   CONFIRM_APPROVE=approve-silver-<RUN_ID>-<CODE_SHA256_FIRST_12> .venv/bin/python -m scripts.lakehouse.layer_acceptance approve --layer silver --run <RUN_ID> --code-sha256 <CODE_SHA256> --out <RECORD_DIR>/approval.json
   ```

9. IAM preflight (project profile). Run it right before the first task, after the policy apply of step 3 and the jobs apply of step 7. It simulates, read only, each call that the silver path makes: the image pulls, log writes and parameter reads of the execution role, the code, input, run state, table, backup and upload calls of the silver task role and the RunTask, tagging, PassRole, describe, stop, list and log read calls of the project user. It prints one `PASS` or `FAIL` line per call and exits 0 only when every call passes. A `FAIL` line names the role, the action and the resource that is denied. Stop the list on any `FAIL` line or any other exit status. Repair the policy in step 2 or step 3, apply it again and rerun this step. A denied simulation means the policy of step 3 was not applied from this tree, because the project user simulates its own calls under the statement `SimulateProjectPrincipals`. The check proves the grants of the policy documents only. It does not prove that an image, a parameter or the task definition exists:

   ```sh
   .venv/bin/python -m scripts.infrastructure.iam_preflight --scope silver
   ```

10. Dry run (project profile). Write the overrides with `--dry-run`. Start the task in the private subnet with the `lakehouse` group of step 2. The explicit `Project` tag is required, because the IAM condition checks it on the RunTask request. Continue only when `failures` is empty. Keep the task ARN. The task ID is the part after the last slash of the ARN:

    ```sh
    .venv/bin/python -m scripts.orchestration.job_overrides --job silver --run <RUN_ID> --code-run <RUN_ID> --code-sha256 <CODE_SHA256> --polaris-uri http://<HOST_PRIVATE_IP>:8181/api/catalog --dry-run > <RECORD_DIR>/overrides_dry.json
    aws ecs run-task --profile <PROJECT_PROFILE> --region <REGION> --cluster <CLUSTER_NAME> --task-definition <PROJECT_NAME>_<ENVIRONMENT>_job_silver --capacity-provider-strategy '[{"capacityProvider":"FARGATE","weight":1}]' --tags '[{"key":"Project","value":"<PROJECT_TAG>"}]' --network-configuration '{"awsvpcConfiguration":{"subnets":["<SUBNET_ID>"],"securityGroups":["<LAKEHOUSE_SECURITY_GROUP>"],"assignPublicIp":"DISABLED"}}' --overrides file://<RECORD_DIR>/overrides_dry.json --query '{taskArn:tasks[0].taskArn,failures:failures}'
    basename <TASK_ARN>
    ```

    A lost reply must be settled before another task starts: list the running tasks of the family first. Do not start a second task for the same run.
11. Wait for the dry run and read its result. The wait ends after 10 minutes. Repeat it. A phase that fails ends the task within about 20 seconds, because the `guard` container of the task definition stops the task. Amazon ECS does not do that by itself: the phases after a failed one wait for its success and never start. The task stays PENDING. If a task shows PENDING with a phase STOPPED and a nonzero or missing exit code, the task definition has no guard: stop it with the command below and apply `infra/jobs` again. The task deadline is 6 hours from the start of its first container. The driver stops a phase that passes it. The guard ends the task 15 minutes later, at 6 hours and 15 minutes. If the task has not stopped 6.5 hours after the launch, the guard did not run: stop it and take the stop as a failed run. A stopped task is not a good task. Save the describe output. The command checks that one task stopped and that every container exited 0, including `init`, `fetch`, `pin`, `build`, `validate`, `verify`, `export`, `publish` and `upload`. Only the `guard` container is left out, because ECS stops it when `upload` ends the task. Then save the publish stream of this task and check its plan. The check refuses a plan with unknown state or a catalog error and a publish that would be blocked. It also refuses a plan of another run. Read the whole plan yourself before step 12, because the owner approves the plan and not the check:

    ```sh
    aws ecs wait tasks-stopped --profile <PROJECT_PROFILE> --region <REGION> --cluster <CLUSTER_NAME> --tasks <TASK_ARN>
    aws ecs describe-tasks --profile <PROJECT_PROFILE> --region <REGION> --cluster <CLUSTER_NAME> --tasks <TASK_ARN> --query '{failures:failures,tasks:tasks[].{taskArn:taskArn,taskDefinitionArn:taskDefinitionArn,createdAt:createdAt,lastStatus:lastStatus,stopCode:stopCode,stoppedReason:stoppedReason,overrides:overrides,containers:containers[].{name:name,lastStatus:lastStatus,exitCode:exitCode,reason:reason}}}' > <RECORD_DIR>/describe_dry.json
    .venv/bin/python -m scripts.lakehouse.layer_acceptance task --layer silver --describe <RECORD_DIR>/describe_dry.json
    aws logs get-log-events --profile <PROJECT_PROFILE> --region <REGION> --log-group-name <JOBS_GROUP> --log-stream-name silver/publish/<TASK_ID> --start-from-head > <RECORD_DIR>/plan_log.json
    .venv/bin/python -m scripts.lakehouse.layer_acceptance plan --layer silver --run <RUN_ID> --log <RECORD_DIR>/plan_log.json
    ```

    Only at the deadline, stop the task and wait until it shows STOPPED. Then take the run as failed:

    ```sh
    aws ecs stop-task --profile <PROJECT_PROFILE> --region <REGION> --cluster <CLUSTER_NAME> --task <TASK_ARN> --reason owner-deadline
    ```

12. Real run (project profile). Save the listing of all object versions under `lakehouse/` first. It is the baseline for the input version check and the write scope check. Write the overrides without `--dry-run` and start the task as in step 10. Wait and read the result as in step 11. Describe within one hour of the stop, because ECS forgets stopped tasks after about an hour. Use the `describe_real.json` file name and the real task ARN:

    ```sh
    aws s3api list-object-versions --profile <PROJECT_PROFILE> --region <REGION> --bucket <DATA_BUCKET> --prefix lakehouse/ --query '{versions:Versions[].{key:Key,version:VersionId,etag:ETag,bytes:Size},deleted:DeleteMarkers[].{key:Key,version:VersionId}}' --output json > <RECORD_DIR>/before.json
    .venv/bin/python -m scripts.orchestration.job_overrides --job silver --run <RUN_ID> --code-run <RUN_ID> --code-sha256 <CODE_SHA256> --polaris-uri http://<HOST_PRIVATE_IP>:8181/api/catalog > <RECORD_DIR>/overrides_real.json
    aws ecs run-task --profile <PROJECT_PROFILE> --region <REGION> --cluster <CLUSTER_NAME> --task-definition <PROJECT_NAME>_<ENVIRONMENT>_job_silver --capacity-provider-strategy '[{"capacityProvider":"FARGATE","weight":1}]' --tags '[{"key":"Project","value":"<PROJECT_TAG>"}]' --network-configuration '{"awsvpcConfiguration":{"subnets":["<SUBNET_ID>"],"securityGroups":["<LAKEHOUSE_SECURITY_GROUP>"],"assignPublicIp":"DISABLED"}}' --overrides file://<RECORD_DIR>/overrides_real.json --query '{taskArn:tasks[0].taskArn,failures:failures}'
    aws ecs wait tasks-stopped --profile <PROJECT_PROFILE> --region <REGION> --cluster <CLUSTER_NAME> --tasks <TASK_ARN>
    aws ecs describe-tasks --profile <PROJECT_PROFILE> --region <REGION> --cluster <CLUSTER_NAME> --tasks <TASK_ARN> --query '{failures:failures,tasks:tasks[].{taskArn:taskArn,taskDefinitionArn:taskDefinitionArn,createdAt:createdAt,lastStatus:lastStatus,stopCode:stopCode,stoppedReason:stoppedReason,overrides:overrides,containers:containers[].{name:name,lastStatus:lastStatus,exitCode:exitCode,reason:reason}}}' > <RECORD_DIR>/describe_real.json
    .venv/bin/python -m scripts.lakehouse.layer_acceptance task --layer silver --describe <RECORD_DIR>/describe_real.json
    ```

13. Collect the evidence (project profile). Save the version listing again and the uploaded run records. Then check that no task of the job is left running:

    ```sh
    aws s3api list-object-versions --profile <PROJECT_PROFILE> --region <REGION> --bucket <DATA_BUCKET> --prefix lakehouse/ --query '{versions:Versions[].{key:Key,version:VersionId,etag:ETag,bytes:Size},deleted:DeleteMarkers[].{key:Key,version:VersionId}}' --output json > <RECORD_DIR>/after.json
    aws s3 cp s3://<DATA_BUCKET>/lakehouse/layer_runs/silver/<RUN_ID>/ <RECORD_DIR>/evidence/ --recursive --exclude '*' --include '*.json' --profile <PROJECT_PROFILE> --region <REGION>
    aws ecs list-tasks --profile <PROJECT_PROFILE> --region <REGION> --cluster <CLUSTER_NAME> --family <PROJECT_NAME>_<ENVIRONMENT>_job_silver --desired-status RUNNING > <RECORD_DIR>/running.json
    ```

14. Stop the host and verify it (project profile). The stop waits until the group has no instance. Fargate tasks do not stop with the host, so step 13 already listed them. Then write the report. It reads every saved file once and refuses a link or a file that is not a record. It names the SHA-256 of each file it read. It checks the whole run against the publish contract of the code revision that the run unpacked: every table set and namespace, the rows and the content fingerprint of every table against its candidate and its export, the run, the sealed identity and the consumer, manifest and candidate hashes, the commit record that follows the backup and names every snapshot ID, the input listing digest of the pin against the listing you saved before the task (H2), the pinned bronze table set (H3), the write scope and the silver namespace. It also checks that the saved task is this run's real task: its definition, its overrides and the approved code digest. Then it checks that no task is running and that the host is stopped. The report is written to the ignored folder only. It prints the text that signs it as `sign_with`. Read the report, then sign it with that text and your name. A report with a failed check is not signed. Do not start gold with a failed or unsigned report:

    ```sh
    .venv/bin/python -m scripts.infrastructure.host_session stop
    .venv/bin/python -m scripts.infrastructure.host_session status > <RECORD_DIR>/host.json
    .venv/bin/python -m scripts.lakehouse.layer_acceptance report --layer silver --run <RUN_ID> --approval <RECORD_DIR>/approval.json --describe <RECORD_DIR>/describe_real.json --evidence <RECORD_DIR>/evidence --before <RECORD_DIR>/before.json --after <RECORD_DIR>/after.json --running-tasks <RECORD_DIR>/running.json --host-status <RECORD_DIR>/host.json --out <RECORD_DIR>/qualification_silver.json
    CONFIRM_SIGN=<SIGN_TEXT> .venv/bin/python -m scripts.lakehouse.layer_acceptance sign --report <RECORD_DIR>/qualification_silver.json --owner "<OWNER>"
    ```

15. If gold does not follow in the same window, turn the endpoints off to stop their hourly cost. Set `job_endpoints_enabled` to `false` in `config/aws/iam_preparation.json`, plan as in step 2 and confirm that the plan only removes the endpoints and their rules. Apply it and read `job_network` again. `endpoints_enabled` must be `false`. Roles, secrets and data stay. Gold and features come next in "Gold and features after silver".

## Gold and features after silver

Gold runs only after the silver publish passed every check and its report is signed. The roles and grants for gold exist from step 2 of the silver list. Features is last and is not ready to run.

1. Gold catalog identity (project profile). If step 15 turned the endpoints off, turn them on again as in step 2 of the silver list. Start the host and find it as in step 4. Approve the namespace `gold` for the writer and read the pair names back. Both `aws_polaris_gold` parameters must show a type and a version:

   ```sh
   .venv/bin/python -m scripts.infrastructure.host_session start
   .venv/bin/python -m scripts.lakehouse.aws_catalog_setup principal --identity gold --instance <HOST_INSTANCE_ID> --namespace-approval <APPROVAL_ID>
   .venv/bin/python -m scripts.lakehouse.aws_catalog_setup check
   ```

2. `infra/jobs` (project profile): write a new reviewed file `jobs_gold.json` in `<RECORD_DIR>`. It is the silver file with `layer_jobs` set to `["silver", "gold"]`, the ARN of `write_gold_task` from the `job_roles` output added to `job_role_arns` and the two gold names of the `job_writer_layer_contract` output added to `writer_parameter_names` under `write_gold_task`. The images are the same three. Bind it to its digest. The plan must register `<PROJECT_NAME>_<ENVIRONMENT>_job_gold` and nothing else:

   ```sh
   shasum -a 256 <RECORD_DIR>/jobs_gold.json
   bash scripts/infrastructure/terraform.sh --jobs --preparation-inputs <RECORD_DIR>/jobs_gold.json --preparation-sha256 <SHA256> plan -out <PLAN>
   bash scripts/infrastructure/terraform.sh --jobs --preparation-inputs <RECORD_DIR>/jobs_gold.json --preparation-sha256 <SHA256> apply <PLAN>
   ```

3. Stage new code under a new gold run ID and approve it as in step 8 of the silver list, with `gold` in the approval. The input of gold is the signed silver run, so the overrides name it. Gold has no `verify` container. Run the dry run, read its result and check its plan as in steps 10 and 11 with the family `job_gold`, the stream `gold/publish/<TASK_ID>` and `--layer gold`:

   ```sh
   CONFIRM_APPROVE=approve-gold-<RUN_ID>-<CODE_SHA256_FIRST_12> .venv/bin/python -m scripts.lakehouse.layer_acceptance approve --layer gold --run <RUN_ID> --code-sha256 <CODE_SHA256> --out <RECORD_DIR>/approval_gold.json
   .venv/bin/python -m scripts.orchestration.job_overrides --job gold --run <RUN_ID> --code-run <RUN_ID> --code-sha256 <CODE_SHA256> --polaris-uri http://<HOST_PRIVATE_IP>:8181/api/catalog --input-run <SILVER_RUN_ID> --dry-run > <RECORD_DIR>/overrides_gold_dry.json
   .venv/bin/python -m scripts.lakehouse.layer_acceptance plan --layer gold --run <RUN_ID> --log <RECORD_DIR>/plan_log_gold.json
   ```

4. Real gold run and evidence. Save the listing before, start the task, wait, read the result, save the listing after and the uploaded records, as in steps 12 and 13 with the family `job_gold` and the overrides without `--dry-run`:

   ```sh
   .venv/bin/python -m scripts.orchestration.job_overrides --job gold --run <RUN_ID> --code-run <RUN_ID> --code-sha256 <CODE_SHA256> --polaris-uri http://<HOST_PRIVATE_IP>:8181/api/catalog --input-run <SILVER_RUN_ID> > <RECORD_DIR>/overrides_gold_real.json
   .venv/bin/python -m scripts.lakehouse.layer_acceptance task --layer gold --describe <RECORD_DIR>/describe_gold.json
   aws s3 cp s3://<DATA_BUCKET>/lakehouse/layer_runs/gold/<RUN_ID>/ <RECORD_DIR>/evidence_gold/ --recursive --exclude '*' --include '*.json' --profile <PROJECT_PROFILE> --region <REGION>
   ```

5. Stop the host, write the report and sign it. The pin of the gold run must name the signed silver run and its complete snapshot set, so the report reads the silver evidence beside the gold evidence:

   ```sh
   .venv/bin/python -m scripts.infrastructure.host_session stop
   .venv/bin/python -m scripts.infrastructure.host_session status > <RECORD_DIR>/host_gold.json
   .venv/bin/python -m scripts.lakehouse.layer_acceptance report --layer gold --run <RUN_ID> --approval <RECORD_DIR>/approval_gold.json --describe <RECORD_DIR>/describe_gold.json --evidence <RECORD_DIR>/evidence_gold --input-evidence <RECORD_DIR>/evidence --input-report <RECORD_DIR>/qualification_silver.json --before <RECORD_DIR>/before_gold.json --after <RECORD_DIR>/after_gold.json --running-tasks <RECORD_DIR>/running_gold.json --host-status <RECORD_DIR>/host_gold.json --out <RECORD_DIR>/qualification_gold.json
   CONFIRM_SIGN=<SIGN_TEXT> .venv/bin/python -m scripts.lakehouse.layer_acceptance sign --report <RECORD_DIR>/qualification_gold.json --owner "<OWNER>"
   ```

6. Turn the endpoints off at the end of the window, as in step 15 of the silver list.

Features takes the signed gold run as its input and is not ready to run. Before its dry run these steps are needed. None of them is part of the silver or gold acceptance:

- The repository `feature_store` is not among the default repositories. Write a reviewed platform file with `ecr_repositories` set to the 11 default names plus `feature_store` and apply its plan as in the training guide. Then build and push the image with `push_images plan --only feature_store` and keep the digest.
- The shared execution role cannot pull that image. Set `job_features_image_prepared` to `true` in `config/aws/iam_preparation.json` beside the three layer flags and apply the IAM plan. The plan must add one policy on the execution role and nothing else.
- The features pair stays with the administrator. `aws_catalog_setup` runs only under the project profile, so no documented command creates the features pair yet. This is open.
- Add `layer_jobs` the value `features`, `train_images.feature_store`, the `write_features_task` role and pair and `layer_feast_redis_host` (the host private address) to the reviewed jobs file. Name the signed gold run in `--input-run`.
- The acceptance command supports silver and gold only. The features H3 check stays open and is not built, so no features publish can be accepted.

## Bronze job

The `bronze` job writes one new source capture into bronze. It is one Fargate task with the `init` step and the containers `fetch`, `pin`, `load` and `check`. Each runs one phase of `scripts/lakehouse/bronze_run.py` from the archive the run staged. The task role is the bronze writer role and only the `load` container receives the bronze catalog pair. The job is not part of the signed chain, because bronze comes first.

Two values change from run to run, the tables and the target. Both come at dispatch from a file the run stages beside its code archive. Both are checked twice. The broker reads `bronze_request.json` before it starts the task. It refuses a table outside the table map, a repeated table, a target other than `real` or `qualify`, a qualification run that spans two collections and any key beyond the three below. The broker reads the file once, persists the checked values and their digest with the intent and passes them to every container and every retry. The `pin` phase reads the staged file again and refuses it when its digest differs from the one the broker checked, so a file replaced after the check cannot change the run. A request marked as a dry run cannot run live. The `pin` phase also checks the same values against the table map of the unpacked code. The `load` and `check` phases refuse values that differ from the pin.

The request also names the digest of a source inventory. The inventory lists the manifest versions and object versions the run may read. The `load` phase reads only those versions, in the dry run and in the real loader. A capture that arrives after the inventory was built is therefore not part of the run. Every attempt of the run loads the same objects. A different capture needs a new run ID with a new inventory. A selected table with no objects in the inventory is refused by name, both when the owner builds the inventory and at the pin, in dry and real runs. This also stops an empty plan from passing when a capture sits outside the prefixes the loader lists. All `load` imports (`boto3`, `pdfplumber`, `openpyxl`, `xlrd`, `pyreadstat` and Spark) are installed in the Spark image. The bronze driver E2E checks this against the image requirements.

```json
{"tables": ["<TABLE_A>", "<TABLE_B>"], "target": "qualify", "dry_run": true, "inventory_sha256": "<SHA256_OF_THE_INVENTORY>"}
```

| Target | Writes |
| --- | --- |
| `real` | `bronze` and `bronze_dictionary` |
| `qualify` | `bronze_e2e` and `bronze_dictionary_e2e`, for the tables of one collection |

The `load` phase runs the existing bronze loader. It lists the versioned manifests of the collection, reads each object by its recorded version, checks its SHA-256 and size, replaces the rows of each object in place and publishes one metadata generation. A retry of one run therefore writes the same rows and no second generation. The broker also starts no second task for the same run and attempt. The `check` phase accepts the loader report only when there is exactly one report, it passed and it names the requested tables and no others. A failed phase fails the task.

A dry run (`"dry_run": true` or `BRONZE_DRY_RUN=1` on every container of a hand-started task) runs `pin`, then reads the manifests and every object version, checks each hash and size and prints the plan. It starts no loader and needs no catalog, so it writes nothing to the catalog or the bucket. Read the plan in the jobs log group, stream `bronze/load/<task id>`. The line that starts with `DRY_RUN_PLAN` and a space holds the plan as JSON.

Apply the bronze job in this order. Each apply is the owner's. Set each switch through the wrapper as `docs/infrastructure_controls.md` describes under activation inputs. The IAM stack takes its file. The platform, services and jobs stacks take a reviewed file and its digest.

1. `infra/iam` (administrator profile): add `bronze` to `broker_job_families`, then plan and apply. The broker role gains the bronze task definition, the pass grant for the bronze writer role and the read of `bronze_request.json`. The bronze writer role gains the object prefixes of its two test twins.
2. Catalog (owner): the bronze writer principal needs create and load rights on the namespaces `bronze_e2e` and `bronze_dictionary_e2e` before a qualification run. The production namespaces keep their rights.
3. `infra/jobs`: set `bronze_job_enabled = true` with the Spark image digest, the execution and bronze writer role ARNs, `writer_parameter_names.write_bronze_task` and the jobs log group. The output `bronze_run_task` lists the variables of each container.
4. `infra/services`: add `bronze` to `broker_jobs.families` and apply, so the broker restarts with the family.
5. Stage the run as for the layer jobs. Then build the inventory and the request with the project profile and copy both beside the code archive:

   ```sh
   AWS_PROFILE=<PROJECT_PROFILE> AWS_REGION=<REGION> .venv/bin/python -m scripts.lakehouse.bronze_run inventory --run <RUN_ID> --bucket <DATA_BUCKET> --tables <TABLE_A>,<TABLE_B> --target qualify --dry-run --out /tmp/bronze_stage
   aws s3 cp /tmp/bronze_stage/bronze_inventory.json s3://<DATA_BUCKET>/lakehouse/run_stage/<RUN_ID>/inputs/bronze_inventory.json --profile <PROJECT_PROFILE>
   aws s3 cp /tmp/bronze_stage/bronze_request.json s3://<DATA_BUCKET>/lakehouse/run_stage/<RUN_ID>/inputs/bronze_request.json --profile <PROJECT_PROFILE>
   ```

   Leave out `--dry-run` for a live run. Read the printed object and manifest counts before the copy.

6. Submit the run on the host. The step name is `bronze_load` and the run ID is the one of the staged folder:

   ```sh
   curl -s -H "Authorization: Bearer $BROKER_TOKEN" -d '{"request_version": 2, "template": "bronze_load", "run_id": "<RUN_ID>", "mode": "real", "attempt": 1, "dag_id": "manual_bronze", "task_id": "bronze_load", "dag_run_id": "manual__<RUN_ID>"}' http://127.0.0.1:18790/v1/submit
   ```

   Poll `/v1/status` with `{"submission_id": "<RUN_ID>:bronze_load:real:1"}` until it is `succeeded` or `failed`. A second submit of the same run and attempt returns the same handle. A new attempt number starts a new task that repeats the same rows. Every attempt of a run must use the request and the inventory that its first attempt had checked. The broker refuses a changed one by name and tells the owner to start a new run ID. It keeps this binding in its ledger on the host volume, so a task started by hand outside the broker is not bound.

When a `real` run passes, the `check` log line prints the command that starts the chain by hand:

```sh
airflow dags trigger bronze_chain_dag --conf '{"run_id": "<BRONZE_RUN_ID>", "mode": "real"}'
```

A `qualify` run never prints it, because the chain reads production bronze.

Every container of the job starts through the in-image deadline runner with one absolute deadline that the broker fixes when it writes the intent. No phase, including `fetch` and the dry load, can outlive it, even when no caller asks for the status. The broker cancels the job at the same deadline as a second control.

A qualification run publishes the metadata of its selected tables only. A real run keeps the check that the first metadata publication covers the whole committed table map.

The qualification tables stay in the test twins until the owner drops them from the catalog. No job removes them.

## Sign the first publishes

The chain stays closed until the first silver and gold publishes pass by hand and both reports are signed with `layer_acceptance`, as the silver list and the gold list describe. The broker then needs a record of the two runs. Put it once, after the second signature (administrator profile):

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
