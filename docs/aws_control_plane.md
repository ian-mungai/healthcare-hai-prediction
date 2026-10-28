---
title: AWS control plane
description: Control-plane services on the AWS container host, with inputs, secrets, images and the Mac launcher changes.
last_updated: 2026-10-28
audience: [developer]
---

# AWS control plane

The always-on control plane runs as ECS services on the single `r7g.large` container host, defined in `infra/services/`. The AWS foundation supplies the cluster, the host and its roles through input variables. The owner approves each apply.

## What runs

Every service uses host networking, so no task has its own network interface. The one-shot `host_prepare` task is the exception: it uses network mode `none` (supported by ECS on Linux EC2 hosts, [task networking](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task-networking.html)), so the one container that runs as root has no network and cannot reach the loopback listeners or bypass the metadata firewall rule. Services reach each other on `127.0.0.1`. One task runs at a time for each service, because host ports and data directories allow no second copy.

| Service | Containers | Reserved / limit (MiB) | Listens on | State |
| --- | --- | ---: | --- | --- |
| `postgres` | Postgres 17. A short container that creates the `airflow` and `marquez` roles and databases and the Polaris schema, then exits | 1600 / 2176 | 127.0.0.1:5432 | `<data>/postgres` |
| `redis` | Redis 7.2 with a password and no eviction (Feast online store) | 512 / 1024 | 6379 | `<data>/redis` |
| `polaris` | Polaris 1.8.0 | 1536 / 2048 | 8181 (REST), 127.0.0.1:8182 (management and health) | none |
| `airflow_apiserver` | migration (runs once), API server | 1280 / 2176 | 127.0.0.1:8080 | `<data>/airflow/logs` |
| `airflow_scheduler` | scheduler, started with `--skip-serve-logs` | 1024 / 1536 | 127.0.0.1:8974 (health) | `<data>/airflow` |
| `airflow_dag_processor` | DAG processor | 768 / 1280 | none | `<data>/airflow/logs` |
| `airflow_triggerer` | triggerer, started with `--skip-serve-logs` | 512 / 768 | none | `<data>/airflow/logs` |
| `marquez` (off by default) | API, web UI | 896 / 1536 | 127.0.0.1:5000, 5001, 3000 | none |

Two one-shot task definitions run from the same cluster by hand, with no service (launch them as shown under "Launching a one-shot task"): `host_prepare` (creates the data directories) and `polaris_bootstrap` (bootstraps the Polaris realm). Their hard limits (128 and 1024 MiB) count in the 2 GiB headroom check but not in the reservation budget.

The reservations of the always-on containers add up to 7232 MiB (8128 with Marquez) of the 10752 MiB budget. The budget is the host's 16384 MiB minus 5632 MiB kept for the operating system, the ECS agent and the later services (MLflow, prediction API, Streamlit, Prometheus, Grafana). The limits are first estimates, not measurements. Terraform refuses a plan whose reservations exceed the budget or whose limits leave under 2 GiB for the system.

Only Polaris (8181) and Redis (6379) listen on a non-loopback interface. The foundation security group opens both to the job tasks only. Every other listener is on 127.0.0.1: reach the Airflow and Marquez interfaces with an SSM port-forwarding session, with no inbound rule.

Services that bind every interface by default and how each is held to loopback (checked against the 3.3.2 and Quarkus sources):

| Listener | Default | Setting |
| --- | --- | --- |
| Scheduler health server (8974) | every interface (`[scheduler] scheduler_health_check_server_host`) | `AIRFLOW__SCHEDULER__SCHEDULER_HEALTH_CHECK_SERVER_HOST=127.0.0.1` |
| Scheduler and triggerer log servers (8793, 8794) | every interface, no host option (`serve_logs` passes `host=""` to uvicorn) | `--skip-serve-logs`; task logs are read from the shared `<data>/airflow/logs` volume |
| Polaris management interface (8182) | Quarkus management default | `QUARKUS_MANAGEMENT_HOST=127.0.0.1` |
| Marquez API and admin connectors (5000, 5001) | every interface (Dropwizard) | `bindHost: 127.0.0.1` in the generated configuration |
| Marquez web (3000) | every interface (`app.listen(port)`) | the arm64 build edits the listen call to use `WEB_HOST` (default 127.0.0.1) |
| Airflow API server (8080), Postgres (5432) | not applicable | `--host 127.0.0.1`, `listen_addresses=127.0.0.1` |

## Inputs from the foundation

| Variable | Meaning |
| --- | --- |
| `cluster_name`, `ec2_capacity_provider_name` | The cluster and the capacity provider of the host |
| `execution_role_arn` | Pulls ECR images, writes logs, reads the parameters in the secrets table |
| `polaris_task_role_arn` | Read and write on the `lakehouse/` prefix of the data bucket. Used by Polaris only |
| `control_plane_task_role_arn` | No data permission. Used by the other services |
| `log_group_names` | Map with `postgres`, `redis`, `polaris`, `airflow`, `marquez` (groups may repeat) |
| `ecr_repository_urls` | Map with `postgres`, `redis`, `polaris`, `polaris_admin` and `airflow`, plus `marquez` and `marquez_web` when Marquez is on |
| `project_tag` | The foundation `project_tag` output: the project name, the same `Project` tag value as the data bucket. Every task definition and service carries it; the budget counts the tagged costs of the services the AWS-first design adds |
| `data_mount_path` | Absolute mount path of the data volume on the host |
| `image_digests` | Written by `scripts/infrastructure/push_images.py record` |

The variable names equal the foundation output names. What the foundation and the secret store must provide is listed under "Needs from the foundation and the secret store".

## Users

Every long-running container runs as a numeric non-root user, because the foundation blocks the instance metadata service with a host firewall rule for non-root users only (host networking puts the containers on the host's network). A plan test fails if a long-running container has no `user`, uid 0 or `root`.

| Container | User | Why |
| --- | --- | --- |
| `postgres`, `postgres_databases` | 999:999 | Started directly as the `postgres` user, so the entrypoint skips its root step (`chown` and `gosu`). `host_prepare` makes the data directory theirs, mode 0700 |
| `redis` | 999:999 | The image's `redis` user |
| `polaris`, `polaris_bootstrap` | 10000:10001 | The `polaris` user of the Polaris images |
| `airflow-*` | 50000:0 | The `airflow` user of the official image |
| `marquez_api` | 10001:10001 | The image has no non-root user. A numeric user needs no passwd entry: the JVM reads the jar (world-readable) and the script writes `/tmp` |
| `marquez_web` | 1000:1000 | The `node` user of the Node image. Port 3000 needs no privilege |

One exception: the one-shot `host_prepare` task runs as root, because it creates the data directories and changes their owners, which only root may do. It has a read-only root file system, drops every capability except `CHOWN`, `DAC_OVERRIDE` and `FOWNER`, runs a fixed script that makes no network call, has network mode `none` and lives for about a second. If the foundation creates these directories with the right owners at host boot, the task can be removed.

## Secrets

Every secret is a Parameter Store SecureString parameter, read by the execution role and injected through the task definition `secrets` field ([ECS: pass Parameter Store parameters](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/secrets-envvar-ssm-paramstore.html)). The plan builds each `valueFrom` from the name: `arn:aws:ssm:<region>:<account>:parameter<name>`, where the name starts with a slash, so the ARN has the name without its leading slash after `parameter/` ([parameter ARN format](https://servicereference.us-east-1.amazonaws.com/v1/ssm/ssm.json)). No data source reads a parameter, so no value is in a file, an environment literal, a command line, the plan or the state. Names are root variables with the committed defaults of the secret migration, validated as slash paths of letters, digits and underscores. The four shared API keys stay in Secrets Manager and are not injected here.

| Variable | Default name | Used by |
| --- | --- | --- |
| `postgres_password_name` | `/healthcare_hai_prediction/postgres_password` | Postgres superuser `polaris`, Polaris, Polaris bootstrap |
| `airflow_db_password_name` | `/healthcare_hai_prediction/airflow_db_password` | Postgres role `airflow`, Airflow |
| `marquez_db_password_name` | `/healthcare_hai_prediction/marquez_db_password` | Postgres role `marquez`, Marquez |
| `airflow_fernet_key_name`, `airflow_jwt_secret_name`, `airflow_api_secret_key_name` | `/healthcare_hai_prediction/airflow_fernet_key`, `_jwt_secret`, `_api_secret_key` | Airflow |
| `redis_password_name` | `/healthcare_hai_prediction/redis_password` | Redis and the Feast clients |
| `polaris_root_secret_name` | `/healthcare_hai_prediction/polaris_root_secret` | The Polaris bootstrap task |

A plan test (`infra/services/tests/flow`) reads every command line of a container that receives a secret and follows each variable that receives it through assignments, `printf`, `echo`, redirections and here-documents. Any write to a file, any use as a program argument and any print to the log fails the test. Every container also sets hard and soft `core` limits of 0 and every health check stays inside the ECS limits (1 to 10 retries), so a long start-up is covered by the interval and start period.

How each secret reaches its program without a command line or a file:

- **Airflow database URL.** Airflow's `_CMD` option runs `bash -c 'printf %s "...$AIRFLOW_DB_PASSWORD..."'`. The shell expands the variable inside the `printf` builtin, so no argument holds the password.
- **Database role passwords.** The `postgres_databases` container feeds `psql` a here-document on standard input. `\getenv` ([PostgreSQL 17 psql](https://www.postgresql.org/docs/17/app-psql.html)) copies each password from the environment into a psql variable. `:'name'` quotes it into `ALTER ROLE`. The script first sets `log_statement = 'none'`, `log_min_error_statement = 'panic'` and `log_min_duration_statement = -1` for the session and sets `VERBOSITY terse`, so neither the statement log nor an error report repeats the statement. Checked on a local PostgreSQL 14 server: with `log_statement = 'ddl'` the unprotected script logged `ALTER ROLE airflow PASSWORD '...'`; the protected script logged nothing, also for a failing statement. `\getenv` itself is not in version 14; it is documented for 17. Residual: while the statement runs, a superuser can see it in `pg_stat_activity`.
- **Redis.** `redis-server -` reads its configuration from standard input. The script builds the lines with the `printf` builtin inside a bash process substitution (`< <(...)`, a pipe) and quotes the password for the configuration parser. Checked with a local Redis 8.10 and a password that holds a quote, a backslash and a space: `CONFIG GET requirepass` returned it, standard input was a pipe, no process argument held it and no file under the working folder held it. The `requirepass` command form was rejected: a started server accepts connections without a password until the command runs.
- **Polaris bootstrap.** `polaris-admin-tool bootstrap` takes `--credential realm,id,secret` (a command argument) or `--credentials-file` ([admin tool](https://polaris.apache.org/releases/1.8.0/admin-tool/)). The task passes `--credentials-file /dev/stdin` and writes the JSON credential through the `printf` builtin to a pipe. The file option opens `uri.toURL().openStream()` ([RootCredentialsSet](https://github.com/apache/polaris/blob/apache-polaris-1.8.0/polaris-core/src/main/java/org/apache/polaris/core/persistence/bootstrap/RootCredentialsSet.java)), which reads a pipe once. Not run: it needs the image. Verify at the first run that the task finishes with exit 0.
- **Airflow sign-in.** The Simple auth manager reads passwords only from a JSON file (`SimpleAuthManager.get_passwords()` opens `simple_auth_manager_passwords_file`; the login service calls that static method, so a subclass cannot replace it; [auth manager guide](https://airflow.apache.org/docs/apache-airflow/3.3.2/core-concepts/auth-manager/simple/index.html), [source](https://github.com/apache/airflow/blob/3.3.2/airflow-core/src/airflow/api_fastapi/auth/managers/simple/services/login.py)). It has no in-memory option, so the stack sets `AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_ALL_ADMINS=true`. This is the accepted access control: the API server listens on 127.0.0.1 only. The only way in is a Session Manager port-forwarding session, which IAM authenticates. The Airflow administrator password (`/healthcare_hai_prediction/airflow_admin`) is not injected. The alternative that keeps a password is the FAB auth manager (the provider is in the image constraints): it stores a password hash in the metadata database, but its first-user step needs a verified in-memory path and a running host, so it is not chosen here.

## AWS catalog setup

The owner creates the AWS catalog `hai_lakehouse`, sets up its five principals and checks them with `scripts/lakehouse/aws_catalog_setup.py`, under the project profile only:

```sh
.venv/bin/python -m scripts.lakehouse.aws_catalog_setup catalog --instance <HOST_INSTANCE_ID>
.venv/bin/python -m scripts.lakehouse.aws_catalog_setup principal --identity <IDENTITY> --instance <HOST_INSTANCE_ID> [--namespace-approval <APPROVAL_ID>]
.venv/bin/python -m scripts.lakehouse.aws_catalog_setup check [--instance <HOST_INSTANCE_ID>]
```

- **Profile.** The tool stops unless the configured profile resolves to the project user (`verify_project_identity`). It needs no administrator profile. The project user's SSM statement `SetUpAwsPolarisPairs` allows `ssm:GetParameter` and `ssm:PutParameter` on exactly the ten AWS Polaris pair names (`docs/infrastructure_controls.md`).
- **Route.** Every catalog request goes through the owned Session Manager tunnel and loopback relay (`orchestration/catalog_transport.py`) after the network acceptance check: one running host with no public address. Neither local port may be 8181, the Mac Polaris port, so no request reaches the Mac catalog. Each request runs once within the step's deadline (`--seconds`, 600 by default). At that deadline a watchdog shuts the socket, so a peer that sends a few bytes at a time or stays silent cannot hold the reply past it. A lost reply fails the step and is not repeated.
- **Catalog.** `catalog` creates the catalog with the body of the Mac setup on `s3://<S3_BUCKET>/lakehouse/` with static credentials, then grants `CATALOG_MANAGE_CONTENT` to `catalog_admin`. A rerun over a matching catalog reports `already present`. A catalog with another location or credential mode is refused and never changed.
- **Credentials.** The Polaris root secret is read from Parameter Store into memory only. It is never printed, logged or written. Errors carry the step and the HTTP status only.
- **Check.** `check` reports each of the ten pair names (`aws_polaris_<identity>_client_id` and `_client_secret` for `reader`, `e2e`, `bronze`, `silver` and `gold`) with its presence, type and version. It reads without decryption and drops the encrypted value unread, so it never shows a value. With `--instance` it also reports whether the AWS catalog is `present` or `absent` and whether each identity's grants match its plan (`matches`, `differs` or `absent`). It changes nothing.
- **Principals.** `principal` sets up one identity: `reader` (`lakehouse_reader`), `e2e` (`test_writer`), `bronze` (`bronze_writer`), `silver` (`silver_writer`) or `gold` (`gold_writer`). Each gets a principal role and a catalog role of the same name. The reader gets the catalog-wide read privileges of the Mac reader. `bronze` and `e2e` get `bronze_writer.grant_plan`. `e2e` also gets the table grants on `silver_e2e`, `gold_e2e` and `features_e2e`. `silver` and `gold` get `polaris_writer.grants` on their own namespace only.
- **Grant check.** After the grants the tool lists the catalog role's grants and compares them with the plan. Any extra, missing or duplicate grant stops it before the catalog role is assigned and before a pair is issued.
- **Role check.** Polaris gives a principal every privilege of every role it holds. So before any change the tool lists the principal's principal roles and that principal role's catalog roles on `hai_lakehouse`. Any role other than the identity's own stops it with `roles`. The tool never removes that role. With `--instance`, `check` reports such an identity as `differs`.
- **Namespace approval.** Every namespace a grant names must exist. Absent ones are created only with `--namespace-approval`. The tool stores the approval once under `data/lakehouse/catalog_setup/namespace_approvals/<identity>.json` with the exact namespace set before it creates anything. It refuses a different approval for the same identity. For `bronze` and `e2e` the approval also grants `NAMESPACE_CREATE` (phase `active`). Without one they get the `prepared` plan.
- **Pairs.** The issued client ID and secret go from the Polaris reply straight to Parameter Store (`principal_credentials`). A stored pair that Polaris accepts is kept. A missing, half-written or refused pair is reset and stored again. Updates of one principal run one at a time under `data/lakehouse/principal_locks/aws_<principal>.lock` in the main checkout. The tool takes that lock first, within the step's deadline, so nothing changes before it. Every later Parameter Store read and write stays within the same deadline.
- **Live writer tries.** A reset would void the pair a running task holds. So the credential step holds the launcher registry lock (`data/orchestration/registry/registry.lock` in the main checkout), under which the launcher admits every try, for at most 20 seconds. It stops with `writer_try` while the registry holds any cloud try without a confirmed end (`fenced` or `complete`). It also stops when the registry cannot be read. The registry does not record which pair a try holds, so any live cloud try stops the setup of every identity.
- **Names.** The ten names are the defaults of their variables in `infra/iam/variables.tf`. The tool refuses names that repeat, lose their own leaf or match a managed Mac name, so a Mac and an AWS writer never share a pair.

The stand-in E2E is `.venv/bin/python -m scripts.lakehouse.run_aws_catalog_setup_e2e --out <new_directory> --mutations`. It uses a loopback Polaris, the Parameter Store stand-in and the real owned relay. It contacts no AWS service.

## Needs from the foundation and the secret store

| Need | Who | Why |
| --- | --- | --- |
| `ssm:GetParameters` for the execution role on the eight parameter ARNs in the secrets table (`output.parameter_arns_used`) and no other action: no `kms:Decrypt` (the AWS managed key `aws/ssm` needs none; a customer managed key would), no Secrets Manager action | Foundation IAM | The task definitions inject the parameters; [task execution role](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task_execution_IAM_role.html#task-execution-secrets) |
| Parameters `/healthcare_hai_prediction/redis_password` and `/healthcare_hai_prediction/polaris_root_secret` | Secret store (the project profile, through `secret_store.MANAGED`) | Both are new; a task that injects a missing parameter does not start |
| Host security group closed to the internet, with 8181 and 6379 open only to the job security group | Foundation network | These are the only non-loopback listeners |
| `ecs:RegisterTaskDefinition`, `ecs:CreateService`, `ecs:UpdateService`, `ecs:DescribeServices`, `ecs:TagResource` and `iam:PassRole` on the three roles | Foundation IAM | The apply creates the task definitions and services |
| Host firewall rule that blocks the instance metadata service for every non-root user | Foundation host | Keeps the containers away from the host role's credentials; see "Users" |
| `kernel.core_pattern` set to a plain file path that does not start with a pipe character (for example a root-only directory), with `fs.suid_dumpable=0` | Foundation host | Every container sets hard and soft `core` ulimits of 0, but Linux ignores that limit when the pattern pipes the dump to a program. A native or JVM crash must not write injected credentials from memory to a file |
| ECS agent with host-network task roles enabled (`ECS_ENABLE_TASK_IAM_ROLE_NETWORK_HOST`, believed to default to true, not verified) | Foundation host | Task roles must work under host networking |
| Repositories `polaris_admin` (and `marquez`, `marquez_web` when Marquez is on) | Foundation registry | They already exist in the foundation's list |

## Images

All images must be `linux/arm64`. `scripts/infrastructure/push_images.py` prints the commands (it starts no program, because only the launcher may call Docker). The lead runs them and records the digests.

| Image | Source | arm64 status |
| --- | --- | --- |
| `postgres`, `polaris`, `egress_proxy` | Mirror of the pinned local reference | The pinned index lists arm64 |
| `redis` | Mirror of `redis:7.2-bookworm` with a new digest pin | The pinned index lists arm64 |
| `airflow`, `polaris_admin`, `spark`, `analytics`, `quality` | Built from `services/` on the Mac | Base images list arm64. Built on Apple silicon, so the local images are arm64 once `docker image inspect` confirms |
| `marquez`, `marquez_web` | Built from the Marquez 0.51.1 source commit on the Mac, base images pinned by digest | No arm64 image exists (the pinned images and the 0.51.1 tags are `linux/amd64` only). The base image indexes list arm64. The web build edits the listen call so the server binds `WEB_HOST`, 127.0.0.1 by default |

The foundation repositories are immutable, so every push uses a unique tag: the listed tag plus twelve hex digits of the pinned source digest (mirrors), of the local image ID (builds) or of the source commit (source builds). The printed commands skip the push when ECR already holds the tag and read the digest back from ECR. Run `.venv/bin/python -m scripts.infrastructure.push_images check` to validate the list offline. The `polaris_admin` image is needed by the bootstrap task even though it is not an always-on service.

## Order of the first start

1. The owner applies the foundation. The lead runs the printed `push_images plan` commands and records the digests.
2. Place a checkout of the repository at `dag_code_path` (default `<data>/repo`). Airflow mounts the DAGs, the broker client and the lineage configuration from it read-only.
3. Turn the host on before the first services apply:

   ```sh
   .venv/bin/python -m scripts.infrastructure.host_session start
   ```

   The entry point reads the cluster, host group, capacity provider, service names and task families from the platform and services Terraform outputs, so it runs only once the services stack exists. For the very first registration check before that stack exists, use the CLI fallback below. It sets the desired capacity to 1 and waits up to 10 minutes for one `ACTIVE`, agent-connected container instance. Then it runs step 5 and the service restarts. Any failure, interruption or SIGTERM stops the host and verifies it, unless you pass `--keep-on` to keep the host on for diagnosis. While that stop runs, a further Ctrl-C, SIGTERM or SIGHUP is held and reported ("stopping the host, please wait"), so it cannot cut the stop short. Every wait and every AWS call within it ends at the wait's deadline. Failure messages show a provider reason only as a fixed category. `host_session status` shows the host and service counts without changing anything. `host_session stop` sets the desired capacity to 0 and waits up to 15 minutes for the instance to terminate.

   CLI fallback, with the project profile and the foundation's host group name:

   ```sh
   aws autoscaling set-desired-capacity --profile <PROJECT_PROFILE> --region <REGION> --auto-scaling-group-name <HOST_GROUP_NAME> --desired-capacity 1
   aws ecs list-container-instances --profile <PROJECT_PROFILE> --region <REGION> --cluster <CLUSTER_NAME>
   aws ecs describe-container-instances --profile <PROJECT_PROFILE> --region <REGION> --cluster <CLUSTER_NAME> --container-instances <CONTAINER_INSTANCE_ARN> --query 'containerInstances[].{status:status,agentConnected:agentConnected}'
   ```

   Check registration for up to 10 minutes. Continue only when the instance is `ACTIVE` and `agentConnected` is true. If the deadline expires, read the host boot log from the serial console before applying services. The boot script copies every log line to the console with the prefix `platform_boot:`. Its last line states success or the exit status, line and step of the failure. The project profile may read the console of the project host only (`ec2:GetConsoleOutput`). AWS posts the console output shortly after a state change, not continuously. It lists a terminated instance for about an hour ([GetConsoleOutput](https://docs.aws.amazon.com/AWSEC2/latest/APIReference/API_GetConsoleOutput.html), [TerminateInstances](https://docs.aws.amazon.com/AWSEC2/latest/APIReference/API_TerminateInstances.html)). Observed on Oct 27 2026: the output of an instance read about a minute after it terminated came back empty. Read it while the host runs or at once when a start fails:

   ```sh
   aws ec2 describe-instances --profile <PROJECT_PROFILE> --region <REGION> --filters "Name=tag:Name,Values=<PROJECT_NAME>_host_<ENVIRONMENT>" "Name=tag:Project,Values=<PROJECT_NAME>" --query 'Reservations[].Instances[].[InstanceId,State.Name]' --output text
   aws ec2 get-console-output --profile <PROJECT_PROFILE> --region <REGION> --instance-id <INSTANCE_ID> --latest --output text | grep 'platform_boot:'
   ```

4. From the repository root, create a services plan with the project profile:

   ```sh
   bash scripts/infrastructure/terraform.sh --services plan -out=services.tfplan
   ```

   The wrapper reads the project inputs from `.env` and the nine foundation outputs from `infra/platform/`. It writes
   the ignored `infra/services/deployment.auto.tfvars.json` without printing values. Image digests stay in the separate
   `infra/services/images.auto.tfvars.json` written by `push_images record`. A missing platform output stops rendering.
   Relative plan paths are inside `infra/services/`. The owner reviews the saved plan before applying it:

   ```sh
   bash scripts/infrastructure/terraform.sh --services apply services.tfplan
   ```

   Before apply, the wrapper verifies the project identity and refuses a saved plan whose project inputs, foundation
   outputs, image digests or other variables differ from the current sources or committed defaults.
5. `host_session start` runs this step and the restarts below. It launches `host_prepare` with the tagged RunTask (it first checks for a running task of the same family and never starts a second one; a launch whose reply is lost is retried with the same client token, so ECS returns the same task) and requires exit code 0. Then it forces a new deployment of every project service at once, postgres first, without waiting for the scheduler's backoff. It waits, with a deadline, until each service shows one `COMPLETED` deployment, its running count and no pending task. A `FAILED` rollout stops the wait at once. The manual steps follow as the fallback.

   Run `host_prepare` with the explicit Project tag shown under "Launching a one-shot task". Require an empty `failures` list from RunTask. Save its task ARN, wait for it to stop and inspect its exit status:

   ```sh
   aws ecs wait tasks-stopped --profile <PROJECT_PROFILE> --region <REGION> --cluster <CLUSTER_NAME> --tasks <TASK_ARN>
   aws ecs describe-tasks --profile <PROJECT_PROFILE> --region <REGION> --cluster <CLUSTER_NAME> --tasks <TASK_ARN> --query '{failures:failures,tasks:tasks[].{stopCode:stopCode,stoppedReason:stoppedReason,containers:containers[].{name:name,exitCode:exitCode,reason:reason}}}'
   ```

   The waiter checks every 6 seconds, for at most 100 attempts ([tasks-stopped](https://docs.aws.amazon.com/cli/latest/reference/ecs/wait/tasks-stopped.html)). If it times out, inspect the task before starting another attempt. Continue only with no failures, one stopped task and `host_prepare` exit code 0. The task creates the data directories with the right owners.

   Airflow services retry until Postgres has created the databases and the migration has finished. An initial deployment can reach `FAILED` before the host or directories are ready. With no completed deployment to roll back to, the circuit breaker stops launching tasks ([deployment circuit breaker](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/deployment-circuit-breaker.html)). Turning the host on does not restart that failed deployment.

   After host preparation succeeds, restart each failed service deployment. Use each name in the services stack's `service_names` output, including Marquez when enabled. A service needs the restart when `describe-services` shows its latest deployment with `rolloutState` `FAILED` and no running task. A service whose deployment is `COMPLETED` needs none:

   ```sh
   aws ecs update-service --profile <PROJECT_PROFILE> --region <REGION> --cluster <CLUSTER_NAME> --service <SERVICE_NAME> --force-new-deployment
   aws ecs wait services-stable --profile <PROJECT_PROFILE> --region <REGION> --cluster <CLUSTER_NAME> --services <SERVICE_NAME>
   aws ecs describe-services --profile <PROJECT_PROFILE> --region <REGION> --cluster <CLUSTER_NAME> --services <SERVICE_NAME> --query '{failures:failures,services:services[].{serviceName:serviceName,desired:desiredCount,running:runningCount,pending:pendingCount,deployments:deployments[].{state:rolloutState,reason:rolloutStateReason}}}'
   ```

   The stable waiter checks every 15 seconds, for at most 40 attempts ([services-stable](https://docs.aws.amazon.com/cli/latest/reference/ecs/wait/services-stable.html)). If it times out, inspect the deployment reason before another update. Require no failures, the desired running count, zero pending tasks and a `COMPLETED` deployment. The existing `ManageProjectServices` grant allows this update only for project service ARNs. Keep the circuit breaker and tag propagation enabled.

   Run `.venv/bin/python -m scripts.infrastructure.host_session bootstrap` once. It requires the postgres and polaris services to be stable, runs `polaris_bootstrap` with the same tagged launch and exit-code check and refuses a second run (marker `data/host_session/polaris_bootstrap.launched`) unless you pass `--again`. Once it begins, a failure stops the host as `start` does, unless you pass `--keep-on`. The manual form:

   Run `polaris_bootstrap` once after Postgres is ready: its service passes the stable waiter above with a `COMPLETED` deployment and one running task. Use the same tagged launch, `tasks-stopped` waiter and exit-code checks. It is not restarted on failure: a second run reports that the realm exists. Then create the catalog and principals (see the next section).

## How the workflows run on the host

The silver, gold and maintenance DAGs only call the job broker over HTTP (`orchestration/client.py`). Nothing in the DAGs changes. The scheduler needs `/opt/hai/broker/client.json` and `broker_token`. On the host a broker service writes the endpoint and the token comes from a new Parameter Store secret. That service and the job path are package P7b. This package assumes:

- The broker keeps its HTTP interface and its ledger. It replaces the launcher with an ECS target: one `RunTask` per launcher template (`pipeline_spark`, `pipeline_publish`, `fixture_dbt`, `quality_*`, `pipeline_export`), idempotent through a start token derived from the submission ID.
- Status, cancel and logs come from `DescribeTasks`, `StopTask` and CloudWatch Logs. Light steps run on the host capacity provider. Heavy steps run on Fargate Spot with on-demand fallback.
- Jobs reach Polaris at the host's private address on 8181, read their catalog credentials from Parameter Store through the task definition and reach S3 through the task role.
- Each run needs shared storage for its run folder (the build database, seals and run record), which the Mac keeps on local disk. The interface decision is open (see below).

## What is tied to the Mac

| Where | Tied to | Change |
| --- | --- | --- |
| `orchestration/launcher.py` | The Docker Engine socket (`docker_socket_path`, `DockerApi`). Every container is created through it | An ECS target behind the same template table |
| `orchestration/launcher.py` (`account_home() / ".aws"` mounts) | The Mac user's AWS profile mounted into each container | Task roles. No profile in the container |
| `orchestration/launcher.py`, `scripts/lakehouse/memory_budget.py` | Admission from Docker Desktop and macOS free memory. Docker Desktop restart after heavy runs | ECS capacity and task sizes |
| `orchestration/launcher.py`, `services/orchestration/egress_proxy.json`, `orchestration/network_policy.py` | Closed egress through Docker networks and a squid proxy | Security group rules and, if needed, VPC endpoints (there is no NAT gateway) |
| `docker-compose.yaml` and the other Compose files | Bind mounts of `./data/...` and the checkout, loopback port publishing | Not used on the host. The task definitions replace them |
| `scripts/orchestration/supervisor.py` | Writes secrets to `data/orchestration/airflow/metadata`. Broker and lineage collector run in-process on the Mac, with a `host.docker.internal` broker URL. Foreground start and Ctrl-C teardown | Parameter Store through the task definitions (done here). The broker and the collector become services. ECS restart replaces teardown |
| `scripts/lakehouse/run_lock.py` (`main_checkout`), run folders under `data/analytics/dbt/runs` and `data/e2e/gold` | Local paths and the checkout rule | Per-run storage in S3 or on a shared volume |
| `scripts/lakehouse/catalog.py`, `gold_writer_setup.py`, `silver_writer_setup.py` | Start Polaris through the launcher and read its address as `localhost` | Take the Polaris endpoint as input and run against the host |

## Launching a one-shot task

Services and one-shot tasks must start with the exact `Project` tag and the project value. For RunTask, IAM requires an explicit `--tags key=Project,value=<PROJECT_TAG>` request tag. Tags propagated from a task definition do not supply this request tag ([request tags](https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies_condition-keys.html#condition-keys-requesttag)). The `project_tag` foundation output supplies `<PROJECT_TAG>`.

RunTask has no `ecs:propagate-tags` condition key in its [authorization row](https://docs.aws.amazon.com/service-authorization/latest/reference/list_ecs.html#amazonecs-RunTask). IAM therefore checks the explicit tag, the project cluster and the task definition ARN. A correctly tagged request without propagation is also authorized. Keep `--propagate-tags TASK_DEFINITION` to copy the remaining task definition tags and `--enable-ecs-managed-tags` for ECS-managed tags ([RunTask options](https://docs.aws.amazon.com/AmazonECS/latest/APIReference/API_RunTask.html)).

Every `aws_ecs_service` sets `propagate_tags = "TASK_DEFINITION"` and `enable_ecs_managed_tags = true`. The `one_shot_run_task` output includes the explicit Project tag and both options. Plan tests check those arguments and the service settings.

```sh
aws ecs run-task --profile <PROJECT_PROFILE> --region <REGION> --cluster <cluster_name> --task-definition <task_definition> \
  --capacity-provider-strategy capacityProvider=<capacity_provider>,weight=1 --propagate-tags TASK_DEFINITION --enable-ecs-managed-tags --tags key=Project,value=<PROJECT_TAG> \
  --query '{taskArn:tasks[0].taskArn,failures:failures}'
```

Save the task ARN for the waiter and continue only with an empty `failures` list. The explicit tag and the propagated task definition tag use the same `Project` key. If RunTask rejects the request for a repeated tag key, run it again without `--propagate-tags`: the explicit tag still satisfies IAM and still tags the task.

## Teardown

`terraform destroy` of `infra/services/` removes the services. The task definition revisions stay registered (`skip_destroy`, because the project user may not deregister them) and stay active until they are deregistered separately. The data volume, its snapshots, the secrets (parameters) and the ECR images belong to the foundation and stay until the foundation's own teardown names them. Stop the services before a destroy if the Postgres data must be consistent: scale each service to zero first.

## Open choices

- **How the DAG code reaches the host** (decided): a checkout of the merged `main` commit at `dag_code_path`, mounted read-only, like the local Compose files. Baking the code into the Airflow image by digest is not needed now.
- **Run-folder storage for jobs** (P7b, decided): S3 staging under the lakehouse prefix; no EFS.
- **Airflow sign-in.** All-admins on a loopback-only API server (this package) or the FAB auth manager with a password hash in the database. See the secrets section.
- **Option names that need a running host.** `simple_auth_manager_all_admins`, `scheduler_health_check_server_host` and `--skip-serve-logs` (on the scheduler and triggerer) come from the 3.3.2 sources and documentation. Verify them at the first deploy with `ss -ltn` on the host: only 6379 and 8181 may show a non-loopback address.
- **Host networking and the instance metadata service** (decided): host networking stays, because ECS port mappings have no host address field and bridge networking could not keep a published port on 127.0.0.1. The foundation blocks the metadata service for non-root users with a host firewall rule. Every long-running container runs as a non-root user (see "Users").
