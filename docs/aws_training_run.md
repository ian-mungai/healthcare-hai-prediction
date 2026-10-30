---
title: AWS training run
description: How the owner applies the training pieces on AWS, starts one training run and stops the MLflow service.
last_updated: 2026-10-30
audience: [developer]
---

# AWS training run

A training run on AWS is one Fargate task started by hand with `aws ecs run-task`. It reads the published feature tables, versions the training matrix with DVC and logs the runs to the MLflow service on the container host. The owner approves every apply and every run.

Nothing here has run against AWS. The offline checks cover the Terraform plans, the IAM policies, the task phases and the fetch step. The first real run proves the rest.

## What runs

The task `<project>_<environment>_job_train` has five containers that share `/workspace` and `/tmp`. Each one starts after the success of the one before, so a failed `init` stops the task before any code is fetched.

| Container | Image | What it does |
| --- | --- | --- |
| `init` | training | Runs as root for one command, `chown 10001:10001 /workspace /tmp`. Fargate creates the volumes owned by root and every other container runs as user 10001 |
| `fetch` | training | Downloads `code.tar.gz` of the run, checks `CODE_SHA256`, unpacks it to `/workspace/code` and links the image's DVC into it |
| `prepare` | training | Records the code identity and the parameters, then runs the guard |
| `extract` | feature store | Reads one feature publication through the catalog and writes the training matrices. Only this container holds the catalog reader pair |
| `train` | training | Runs `version_extract`, `fit`, `mediation`, `hac_validate`, `log_run` and `promotion_check` |

The task role reads the stage inputs, the feature tables and the Feast registry. It reads and writes `dvc/`. It writes nothing in the lakehouse and has no access to `mlflow/`. The `train` container logs in to the MLflow server at a private address on port 5100 as the administrator account. The server asks for a login on every request and grants nothing by default, because every service of the host shares the host network. The `train` container and the owner hold the one password, from Parameter Store. The server proxies the artifacts into `mlflow/` of the data bucket.

## Apply order

Each apply is the owner's. Show the plan first and apply exactly that plan.

1. `infra/iam` (administrator profile): in `config/aws/iam_preparation.json` on your machine, set `training_prepared` and `job_endpoints_enabled` to true. Leave the file uncommitted. A key that stays out of the file is false. Apply with `bash scripts/infrastructure/terraform.sh --iam --admin-profile <ADMIN_PROFILE>` as for the rest of the IAM stack. The `job_endpoints_enabled` switch creates the S3 gateway endpoint and the interface endpoints for ECR (two), Logs and Parameter Store. Without it a task in the closed job network has no route. It cannot pull its images. It cannot write its logs or read its secrets either. The `training_prepared` switch adds the `job_train_task` and `mlflow_task` roles with their policies. It also adds the `job_training` security group and the rules for ports 8181 and 5100 on the host.
2. Policy scripts (administrator profile): `manage_policies plan`, then `manage_policies apply` with the typed confirmation. The project user's IAM policy now passes the `job_train_task` and `mlflow_task` roles. Without it the services and jobs applies fail on the PassRole check.
3. `infra` (project profile): apply the `expire_replaced_mlflow_versions` lifecycle rule.
4. `infra/platform`: add `mlflow`, `training` and `feature_store` to the ECR repositories and `mlflow` to the log groups.
5. Images: build the `mlflow`, `training` and `feature_store` images and push them. See "Images" below. Then record the digests with `scripts.infrastructure.push_images record`.
6. Secrets: put the values of the `mlflow_db_password` and `mlflow_admin_password` parameters with `scripts/infrastructure/secret_store.py`. Use new random values. MLflow reads the administrator password only when it first creates the account. To change it later, change it through the MLflow user interface first and then the parameter.
7. `infra/services`: write a nonsecret reviewed inputs file with `schema_version` 1, `root` `services` and the variables `mlflow_enabled` (true), `mlflow_task_role_arn` (the `mlflow_task` ARN from the `training_roles` output), `mlflow_host_ip` (the private address of the host) and `mlflow_artifact_bucket`. The schema is `data_contracts/infrastructure/services_preparation.schema.json`. Plan and apply with `bash scripts/infrastructure/terraform.sh --services --preparation-inputs <FILE> --preparation-sha256 <SHA256>`. Then restart the Postgres service once so its short database container creates the `mlflow` and `mlflow_auth` databases.
8. `infra/jobs`: write the nonsecret reviewed inputs file (`schema_version` 1, `root` `jobs`, the variables `train_enabled`, `train_images`, `train_roles`, `train_resources`, `mlflow_admin_password_name` and `log_group_name` as `data_contracts/infrastructure/jobs_preparation.schema.json` defines them). `train_images` holds the repository URL and digest of the `training` and `feature_store` images. `train_roles` holds the `execution` and `train_task` ARNs from the IAM outputs. Plan and apply with `bash scripts/infrastructure/terraform.sh --jobs --preparation-inputs <FILE> --preparation-sha256 <SHA256>`, as `docs/ecs_target_contract.md` shows.

## Images

Neither the MLflow image nor the training image existed in the repository before. The Dockerfiles and the inputs are in `services/mlflow` and `services/training`. The hash-pinned `requirements.txt` of each is not committed yet. The lead compiles it inside the pinned base image:

```sh
docker run --rm --platform linux/arm64 --memory 4g --user "$(id -u):$(id -g)" -e HOME=/tmp \
  --volume "$PWD":/work --workdir /work \
  python:3.12-slim-bookworm@sha256:54c85f3c47607a77f32adec749d3c81d1348bf25833671f512b26a9b6d778cb3 \
  sh services/training/compile_lock.sh
```

Run the same command with `services/mlflow/compile_lock.sh`. Review the version diff of the training lock against `docs/analysis_plan.md` before the first real run. No project file pinned the model libraries before this lock. The DVC lock in `scripts/quality/dvc` lists hashes for Apple silicon and x86 Linux. The `linux/arm64` build of the training image may fail on a missing hash.

## Start one run

1. Pick a run ID of the form `YYYYMMDDTHHMMSSZ-xxxxxxxx` (eight hex digits).
2. Stage the code of the commit you want to train from:

   ```sh
   git archive --format=tar.gz --output=/tmp/code.tar.gz HEAD
   shasum -a 256 /tmp/code.tar.gz
   aws s3 cp /tmp/code.tar.gz s3://<DATA_BUCKET>/lakehouse/run_stage/<RUN_ID>/inputs/code.tar.gz --profile <PROJECT_PROFILE>
   ```

3. Write the parameters. `features_run` is `latest` or the run ID of one feature publication. `mlflow` must be `aws`:

   ```json
   {"features_run": "latest", "population": "primary", "hai_types": ["HAI_1"], "trigger": "manual", "mlflow": "aws"}
   ```

4. Start the task. The subnet and the security group come from the `training_network` output of the IAM stack. The addresses are the private address of the host:

   ```sh
   aws ecs run-task --profile <PROJECT_PROFILE> --region <REGION> --cluster <CLUSTER_NAME> --task-definition <PROJECT>_<ENVIRONMENT>_job_train \
     --capacity-provider-strategy capacityProvider=FARGATE,weight=1 --propagate-tags TASK_DEFINITION --enable-ecs-managed-tags --tags key=Project,value=<PROJECT_TAG> \
     --network-configuration 'awsvpcConfiguration={subnets=[<SUBNET_ID>],securityGroups=[<TRAINING_SECURITY_GROUP>],assignPublicIp=DISABLED}' \
     --overrides file://overrides.json --query '{taskArn:tasks[0].taskArn,failures:failures}'
   ```

   `overrides.json` fills the empty variables, one block per container. The `train_run_task` output of the jobs stack lists them:

   ```json
   {"containerOverrides": [
     {"name": "fetch", "environment": [{"name": "RUN_ID", "value": "<RUN_ID>"}, {"name": "CODE_SHA256", "value": "<SHA256>"}]},
     {"name": "prepare", "environment": [{"name": "RUN_ID", "value": "<RUN_ID>"}, {"name": "CODE_SHA256", "value": "<SHA256>"}, {"name": "TRAINING_PARAMETERS", "value": "<PARAMETERS_JSON_AS_A_STRING>"}]},
     {"name": "extract", "environment": [{"name": "RUN_ID", "value": "<RUN_ID>"}, {"name": "POLARIS_URI", "value": "http://<HOST_PRIVATE_IP>:8181/api/catalog"}]},
     {"name": "train", "environment": [{"name": "RUN_ID", "value": "<RUN_ID>"}, {"name": "MLFLOW_TRACKING_URI", "value": "http://<HOST_PRIVATE_IP>:5100"}]}
   ]}
   ```

5. Watch the log group `<project>_jobs_<environment>`. Each finished step prints one JSON line. A failure prints one JSON line that names the step. Stop a task that must not continue with `aws ecs stop-task`.
6. A rerun uses a new task and the same run ID. DVC stores the matrix once by content and `log_run` logs each run once by its key, so a repeat adds nothing.

Read the runs in the MLflow interface through a port forward to the host, as for Airflow. Sign in as `admin` with the value of the `mlflow_admin_password` parameter:

```sh
aws ssm start-session --profile <PROJECT_PROFILE> --region <REGION> --target <INSTANCE_ID> \
  --document-name AWS-StartPortForwardingSession --parameters portNumber=5100,localPortNumber=5100
```

## Cost and stop

The four interface endpoints bill for every hour they exist, whether or not a task runs (about 0.04 dollars per hour in total, from the earlier design, so check the current price list). Keep them only for the length of a run.

The MLflow service adds no instance. It runs beside the catalog on the host that is already up, with a 640 MiB limit. The host costs about 0.11 dollars per hour on demand while it is up (check the current price list). A training task of 4 vCPU and 16 GiB costs about 0.19 dollars per hour on Fargate and is capped by the action budgets, 3 hours for `fit`. Storage in the database and in `mlflow/` is small.

Stop steps, in this order:

1. The task stops itself when its last container ends. Check with `aws ecs describe-tasks` that no `job_train` task still runs.
2. Stop the MLflow service: in `infra/services`, plan and apply again without the reviewed inputs file. A file that sets `mlflow_enabled` to false works too. The apply removes the service and its task definition link. The database on the host volume and the artifacts in `mlflow/` stay.
3. Turn the host off as for the rest of the control plane, with `aws autoscaling set-desired-capacity ... --desired-capacity 0`. Verify through `describe-services` and `describe-auto-scaling-groups` that nothing billable still runs.
4. Turn the job endpoints off: in `infra/iam`, set `job_endpoints_enabled = false` and apply the shown plan. It removes the gateway endpoint, the four interface endpoints and their rules. Verify that none is left:

   ```sh
   aws ec2 describe-vpc-endpoints --profile <PROJECT_PROFILE> --region <REGION> --filters Name=tag:Project,Values=<PROJECT_TAG> \
     --query 'VpcEndpoints[].{id:VpcEndpointId,type:VpcEndpointType,state:State}'
   ```

   An empty list or only `deleted` states is the result you want. A listed endpoint in another state still bills.
5. Leave `training_prepared`, `train_enabled` and the lifecycle rule in place. They cost nothing while idle. Remove the roles only with an approved destroy plan.

## Not proven

- Whether the `init` step leaves `/workspace` and `/tmp` writable for user 10001 on Fargate. Whether Fargate accepts the dropped capability list that keeps only the owner change is also open. The offline plan test proves the definition only. It checks the order, the command, the capability list and that every writable mount of every container is one of the two volumes.
- The `linux/arm64` build of both new images and the DVC hashes for that platform.
- The library versions of the model code against real data.
- The MLflow server memory limit of 640 MiB under a real run.
- The authentication setup against the pinned MLflow version: the settings file keys, the health route answering without a login and the training client sending its login. The offline cases cover the start script and the task definitions only.
- Whether the endpoints alone give the task every route it needs. The job network has no NAT.
- Every network path between the task and the host. Only the plans and policies are checked offline.
