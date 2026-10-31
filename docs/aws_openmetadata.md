---
title: AWS OpenMetadata catalog
description: How the owner applies, starts, browses and stops the on-demand OpenMetadata catalog task.
last_updated: 2026-10-30
audience: [developer]
---

# AWS OpenMetadata catalog

OpenMetadata answers "what data exists and what does it mean" for the lakehouse. It runs as one on-demand Fargate task that the job broker starts and that stops itself. Nothing here has run against AWS or Docker. The offline checks cover the scripts, the configuration, the deadline wrapper, the read-only policy, the DAG and the Terraform plans. The first real start proves the rest.

## What runs

One task of the family `<project>_<environment>_job_metadata` runs these containers in order. A new start builds an empty catalog, because the database and the search index live on the task's own storage and vanish with it.

| Container | Image | What it does |
| --- | --- | --- |
| `init` | database | Writes the deadline, a random database password and the tracked scripts and configuration into the task folders, then hands the folders to their users |
| `database` | database | The catalog database. Stops when the server stops or at the deadline plus two minutes |
| `search` | search | The search index. Stops at the deadline plus two minutes |
| `migrate` | server | Prepares the empty database, retrying for up to five minutes while the database starts |
| `server` | server | The catalog and its UI on port 8585. Stops at the deadline |
| `bootstrap` | ingestion | Replaces the default administrator password with the one from the parameter store and saves the ingestion token in an owner-only file |
| `ingest_polaris` | ingestion | Reads table and column definitions from Polaris with the reader credentials |
| `ingest_dbt` | ingestion | Downloads the dbt files from `s3://<data bucket>/metadata/dbt/` and links models, columns and tests to the Polaris tables |
| `ingest_marquez` | ingestion | Copies Marquez job lineage into table lineage. It exists only when `openmetadata_marquez_url` is set |

The ingestion steps run one after the other and each runs even when the one before it failed. The Polaris step needs the bootstrap to have passed.

Airflow runs `metadata_ingest_dag`. Its one step asks the broker for the `metadata` job and waits for the task to end. The DAG has no schedule and no retry, because a retry would start a second paid task. The broker table, the launcher template and the chain DAG live with the broker code. Once that code carries the `metadata` job, the chain DAG starts `metadata_ingest_dag` as its last step and does not wait for it, so a failed catalog task never fails a chain run. The start itself is a Python task that runs only after the drift layer succeeded. It creates the catalog DAG run with one bounded call to the Airflow REST API (ten seconds at most) and treats an existing run as done. It logs any error at error level and returns it in the task result without raising, so a missing or paused catalog DAG or a refused call cannot fail the chain either. Look for `catalog start failed` in the `start_metadata` task log or result. An upstream failure still fails the chain.

## How the task stays bounded

Five limits apply. Any one of them ends the task.

1. `openmetadata_lifetime_seconds` (default 7200, ceiling 14400). The `init` container writes the deadline. The wrapper `scripts/infrastructure/openmetadata_bounded_run.sh` stops `server`, `search` and `database` at it. The server, the search index and the database are all essential containers, so when the server stops at its deadline ECS stops the other two. A wrapper that receives that stop at or after the deadline exits 0. A stop before the deadline exits 1. Every container therefore ends with 0 at a normal end. The broker reads that as a normal end.
2. A file `/run/om/abort`. The bootstrap writes it when the server never gets healthy or no password logs in. The migration script `scripts/infrastructure/openmetadata_migrate.sh` writes it after its last failed try (30 tries ten seconds apart). The wrapper then stops the long-lived containers at once, so a task with no usable catalog does not run to the deadline.
3. A server that stops by itself before the deadline, because it is essential. The task stops and the broker reads the failure.
4. The broker deadline, the lifetime plus 900 seconds. The broker stops the task when it polls and finds it still running.
5. The DAG step limit of 4.5 hours, the ceiling plus 30 minutes.

If Airflow and the broker are both down, limit 1 still stops the task. The budget alerts report spend but stop nothing.

## Sampling and profiling stay off

Some tables hold Confidential copies, so the catalog reads definitions and never rows. Four independent controls hold:

1. The tracked configuration in `config/metadata/ingestion/` has no profiler, no sampling and no data-quality step. `scripts/infrastructure/openmetadata_ingest.py` refuses a configuration that sets any such key to a value other than false. It also refuses a pipeline other than a metadata or dbt pipeline. A source type other than Iceberg or dbt is refused too. It checks before it fills any placeholder, so an environment value cannot switch one on. The offline check `real_configs_have_no_profiler_or_sampling` runs it on the tracked files. The check `every_profiler_or_sampling_mutant_is_refused` runs it on nine changed copies.
2. The server setting `PIPELINE_SERVICE_CLIENT_ENABLED=false` removes the ability to deploy or trigger ingestion pipelines, so nobody can start a profiler run from the UI. A Terraform plan test asserts it.
3. The catalog task role reads only Iceberg `metadata/` files and the dbt files. It denies every write. It cannot read a data file. The policy `infra/iam/owner_policies/openmetadata_catalog_read.json` is evaluated offline against both kinds of path.
4. The server, bootstrap and ingestion containers have no route to the internet. The lakehouse job group reaches only the registry, S3, Parameter Store, logs and the host ports listed below.

## Cost

At the default of 4 vCPU and 24 GiB the task costs about $0.27 an hour on x86 and about $0.21 an hour on Arm, plus a cent or less for the 30 GiB of task storage. These figures are the owner's range. The plan output `openmetadata_run_task.hourly_usd_range` reproduces them from per-unit list prices that were not checked against the current price list. A two-hour start costs about $0.54 on x86 and $0.43 on Arm. The task defaults to x86. Arm is allowed only when all four pushed images list `linux/arm64`, which is unverified (see `config/metadata/images.json`).

## Owner steps

Each apply is the owner's. Show the plan first and apply exactly that plan.

1. Platform stack: add `openmetadata_server`, `openmetadata_ingestion`, `openmetadata_search` and `openmetadata_database` to `ecr_repositories` in the platform variable file, then apply the shown plan.
2. Images: pick a release of the four upstream images in `config/metadata/images.json` and decide the platform. If you want Arm, run `docker buildx imagetools inspect` for each and require `linux/arm64`. Push each by digest to its repository and record the four digests in the `openmetadata_images` variable of `infra/jobs`. Keep the variable file out of Git.
3. Start commands: read the entry point of the server, search and database images and correct `openmetadata_commands` in `infra/jobs` if it differs from the default.
4. Password: put a new value in the parameter `/<project>/openmetadata_admin_password` without printing it. The server needs at least eight characters with an upper-case letter, a lower-case letter, a digit and a special character.

   ```sh
   umask 077
   openssl rand -base64 24 | tr -d '\n' | sed 's/$/aA1!/' > "$TMPDIR/om_admin"
   aws ssm put-parameter --profile <ADMIN_PROFILE> --region <REGION> --name /healthcare_hai_prediction/openmetadata_admin_password --type SecureString --value "file://$TMPDIR/om_admin"
   ```

5. dbt files: the gold publish job copies `manifest.json`, `catalog.json` (when the run wrote it) and `run_results.json` to `s3://<DATA_BUCKET>/metadata/dbt/` in its upload phase, with the gold writer role. The copy is best effort: a refused or failed copy is logged and never fails the publish. A catalog start finds the files of the latest gold publish. Before the first gold publish with this change, copy the files by hand under the project profile.
6. `infra/iam` (administrator profile): set `openmetadata_prepared = true`. This creates the role `job_openmetadata_task`, the image and parameter grants on the execution role and two security group rules. Then run `manage_policies plan` and `manage_policies apply` for the project user's new pass-role statement and the remote-host port forward document.
7. `infra/jobs` (project profile): set `openmetadata_enabled = true` with the four digests, the two roles from the IAM outputs and the log group. The apply registers a task definition and starts nothing.
8. Broker: the broker table already holds the `metadata` job with a deadline of the lifetime plus 900 seconds. Add the `metadata` family to the broker role and to the broker service families. Without the broker, use the hand start below.
9. Marquez step, off until after the batch merge: Marquez listens on `127.0.0.1` of the host today, so the task cannot reach it. Later, change its listener in the services stack to the host's private address, set `openmetadata_marquez_reachable = true` in `infra/iam` and set `openmetadata_marquez_url` in `infra/jobs`. In the same change add `ingest_marquez` with the variable `RUN_ID` to the `metadata` job of `config/orchestration/ecs_broker_jobs.json`, because the task then has that container and a plan test pins the difference. The step pages through all namespaces and datasets. Every request it makes ends within one five minute budget. The catalog database name `hai_lakehouse` is set in both `polaris.json` and `marquez.json`. An offline check compares them.
10. Start `metadata_ingest_dag` by hand once with `{"run_id": "<RUN_ID>", "mode": "real"}` and read the logs of the stream `metadata/<container>/<task id>` in the jobs log group.

## Start by hand

A hand start needs the cluster, the job subnet and the lakehouse job security group from the IAM outputs and the private address of the host. It needs no broker. The task runs the same containers and stops itself at the same deadline. Write the overrides with `scripts.orchestration.job_overrides`, which reads the container and its empty variables from the broker table and refuses by name when a value is missing:

```sh
.venv/bin/python -m scripts.orchestration.job_overrides --job metadata --run <RUN_ID> \
  --polaris-uri http://<HOST_PRIVATE_IP>:8181/api/catalog > overrides.json
aws ecs run-task --profile <PROJECT_PROFILE> --region <REGION> --cluster <CLUSTER_NAME> \
  --capacity-provider-strategy capacityProvider=FARGATE,weight=1 --task-definition <PROJECT>_<ENVIRONMENT>_job_metadata \
  --network-configuration "awsvpcConfiguration={subnets=[<SUBNET_ID>],securityGroups=[<LAKEHOUSE_SECURITY_GROUP_ID>],assignPublicIp=DISABLED}" \
  --overrides file://overrides.json
```

## Open the UI privately

The task has no public address and the UI port accepts only the container host. Use a Session Manager port forward from the host to the task's private address.

```sh
TASK=$(aws ecs list-tasks --profile <PROJECT_PROFILE> --region <REGION> --cluster <CLUSTER_NAME> --family <PROJECT>_<ENVIRONMENT>_job_metadata --desired-status RUNNING --query 'taskArns[0]' --output text)
TASK_IP=$(aws ecs describe-tasks --profile <PROJECT_PROFILE> --region <REGION> --cluster <CLUSTER_NAME> --tasks "$TASK" --query 'tasks[0].attachments[0].details[?name==`privateIPv4Address`].value | [0]' --output text)
aws ssm start-session --profile <PROJECT_PROFILE> --region <REGION> --target <INSTANCE_ID> \
  --document-name <PROJECT>_catalog_forward_<ENVIRONMENT> --parameters "host=$TASK_IP,localPortNumber=8585"
```

Open `http://localhost:8585` and sign in as `admin@open-metadata.org` with the stored password. The session document `<PROJECT>_catalog_forward_<ENVIRONMENT>` fixes the destination port at 8585 and accepts only an address in the platform network. It exists only while `openmetadata_prepared` is true. The project user's policy names only this document, not the general AWS remote-host document.

## Stop

The task stops itself at the end of its lifetime. To stop it earlier, cancel the DAG task, which makes the broker stop it. Or stop it by hand:

```sh
aws ecs stop-task --profile <PROJECT_PROFILE> --region <REGION> --cluster <CLUSTER_NAME> --task "$TASK" --reason "owner stop"
aws ecs list-tasks --profile <PROJECT_PROFILE> --region <REGION> --cluster <CLUSTER_NAME> --desired-status RUNNING
```

To switch the feature off, set `openmetadata_enabled = false` in `infra/jobs` and apply the shown plan. The task definition revisions stay, because they carry `skip_destroy`. The images, the parameter and the dbt files stay too.

## Not proven

- Every image detail: the upstream names, that each image has `linux/arm64`, the start commands, the users and the folders each writes, whether the database image has `sh`, `date`, `head`, `od`, `tr` and `base64` and the server, search and database images have `sh`, `date`, `cat`, `sleep` and `kill`. It is also unproven whether a read-only root file system works for the ingestion image.
- The OpenMetadata environment variable names, the Iceberg connection fields (the Polaris address, the credential pair and the warehouse), the dbt local configuration and the REST routes of the bootstrap and Marquez steps. The stand-ins encode the author's reading of them.
- Whether the Iceberg connector reads through the catalog's own file access or through the task role. Whether `metadata/` files alone suffice.
- Whether Marquez dataset names match `dataset_pattern` in `config/metadata/ingestion/marquez.json`. Unmapped datasets are counted, never guessed.
- Whether a catalog of this size starts within the 24 GiB and the five-minute migration retry window.
- Whether `OPENMETADATA_HEAP_OPTS`, the search heap setting and `node.store.allow_mmap=false` give a server that fits and starts on Fargate.
- Whether the Airflow task process holds `AIRFLOW__API_AUTH__JWT_SECRET`. Also whether the API server accepts a token signed with it and the body `dag_run_id`, `conf` and a null `logical_date` on `POST /api/v2/dags/metadata_ingest_dag/dagRuns`. A wrong answer shows as `catalog start failed` in the task result and the catalog does not start.
- Whether the session document is accepted as written and the forward reaches a Fargate task. Whether the project user's session limit allows it.
- The reviewer read the OpenMetadata 1.12.5 source and found that its Iceberg connector names the database `default` unless `databaseName` is set. The tracked configuration sets it, but no release is named in `config/metadata/images.json`, so check the effective database name and the connector fields against the release you choose. The same source passes a null token to its REST client, which can suppress the credential exchange with Polaris. It also starts the server with `/openmetadata-start.sh`. Check both before a paid start.
