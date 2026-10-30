---
title: Infrastructure controls
description: Environments, the AWS stacks and project policies, bucket lifecycle rules, the lifecycle permission, deferred controls.
last_updated: 2026-10-29
---

# Infrastructure controls

This document describes the environments, the AWS stacks, the project policies, the bucket lifecycle rules and the deferred controls of the project infrastructure. It is for the owner and for developers who change the infrastructure.

**Environments.** All three stacks tag resources with `Environment = dev`. A `prod`
environment and an `infra/modules/` layout are deferred. The platform stack keeps
one file per component in a single root.

**Who applies what.** Three things build the AWS runtime. The owner runs the first two with the administrator profile.
The project profile runs the third.

1. The IAM stack (`infra/iam/`, `bash scripts/infrastructure/terraform.sh --iam --admin-profile <ADMIN_PROFILE>`) creates what
   the project profile must not control. That is every role (the host, task execution, Polaris task, control-plane task,
   and snapshot roles and the host instance profile), the network, the host launch template, the host's Auto
   Scaling group and the cost budget. The network is the VPC, two public subnets in two availability zones with no NAT
   gateway, the internet gateway, the route table and route, the host security group and the run-task security group with
   their rules. The cost budget is the $150 quarterly budget with its 50% and 80% email alerts and its own alert topic. It only
   informs: the owner tracks spend by hand and no action stops spending. The stack attaches no policy to the project user.
2. The policy scripts (`infra/iam/scripts/`, run from the repository root) apply the project profile's policies, one JSON
   file per service in `infra/iam/policies/`. `render_policy` renders the templates with the values of `.env` into a review
   directory. `manage_policies plan` is the dry run: it reads IAM and prints what `apply` would do. `manage_policies apply`
   needs `CONFIRM_IAM_POLICIES=apply-healthcare-hai-prediction-policies` and checks it before any AWS call. The owner
   types it on the command line for one apply and never sets it in `.env`. It creates
   or updates the 13 customer-managed policies (it removes the oldest non-default version before the sixth), creates the
   groups `<PROJECT_NAME>_data_<ENVIRONMENT>` and `<PROJECT_NAME>_platform_<ENVIRONMENT>`, attaches each policy to
   its group, adds the project user to both groups, detaches the policies that were attached to the user directly and
   detaches and deletes the retired grouped policies (`secrets`, `lakehouse`, `platform_network`, `platform_services` and
   `platform_tags`) unless something else holds one. Groups keep the user under the quota of 10 managed policies per user, so the user holds no direct attachment.
   Before any change, `plan` and `apply` read both groups and stop when a group has a member other than the project
   user, a managed policy outside its intended set (compared by ARN, so an AWS-managed policy with an intended name does
   not pass) or any inline policy. A retired policy on a group also stops them, because `apply` attaches the new policies
   before it retires the old ones and the group could pass its quota of 10 in the middle of a run. A missing intended
   attachment or a missing membership of the project user is a planned repair, not a stop, so a run can complete one
   that stopped part way. They also list any other managed policy,
   inline policy or group of the project user as a `NOTE` line. The administrator profile is a required argument,
   is never read from `.env`, must differ from the project profile and must be in the account that `.env` names.
   `export_policies` writes the policies in IAM back as templates with placeholders, to compare with Git.
3. The platform stack (`infra/platform/`, `bash scripts/infrastructure/terraform.sh --platform`) creates what the project profile
   may: the ECS cluster and its capacity providers (the host group, Fargate Spot and Fargate), the ECR repositories, the log
   groups, the run-failure alert topic, the host's gp3 data volume, one Elastic IP and the daily snapshot schedule. It
   reads the VPC, both subnets, both security groups and the host group by their `Name` and `Project` tags. The plan
   fails until the IAM stack is applied. The wrapper renders only the inputs the stack declares
   (`scripts/infrastructure/render_platform_config.py`). Before an apply, the saved plan's `.env` inputs must match
   and every other variable must keep its committed default.

**IAM preparation inputs.** The IAM stack's four preparation inputs are `bronze_writer_prepared`, `job_writer_e2e_prepared`,
`bronze_qualification_collection` and `bronze_live_locations`. They are set only in the committed, reviewed file
`config/aws/iam_preparation.json`, never in `.env` or on the command line. The defaults are `false`, `false`, `""` and `{}`.
The coordinating agent changes a value after the evidence for it exists, so Git history records who prepared what and when. The renderer refuses
a file that is not exactly those four keys, plus two optional inputs with the right types: `athena_mirror_prepared` (a boolean, `false` when absent) and `athena_owner_user` (one IAM user name, empty when absent). The user name belongs only in the owner's local copy of the file, never in a committed one. It also refuses a collection that is not mapped in
`config/lakehouse/bronze_tables.json` and a location map that is neither empty nor exactly the bronze and bronze dictionary
locations of the data bucket. It then writes the ignored `infra/iam/preparation.auto.tfvars.json`. Before an apply, the saved plan
must carry exactly these values, so a file edited between plan and apply is refused. The Terraform preconditions still decide
whether the combination is allowed.

The control-plane services stack (`infra/services/`) runs on the platform's container host after these three. Its inputs,
secrets, images and the Mac launcher changes are in `docs/aws_control_plane.md`.

The order is: the IAM stack, the policy scripts (`plan`, then `apply`), the platform stack, then the project user turns
the host on with `aws autoscaling set-desired-capacity --auto-scaling-group-name <PROJECT_NAME>_host_<ENVIRONMENT>
--desired-capacity 1`, and off again with `0`. The group starts with no instance because the data volume and the Elastic IP
do not exist until the platform stack is applied. The IAM stack's plan also needs the service-linked roles for Auto
Scaling and ECS. The account may already hold them: `aws iam get-role --role-name AWSServiceRoleForECS` shows whether they exist.

**The project profile's policies.** The profile is the IAM user `<PROJECT_NAME>_<ENVIRONMENT>`. Each policy is one file,
`infra/iam/policies/healthcare_hai_prediction_<service>_policy.json`. It holds only what the approved list gives that service.

| Service | What the project profile can do | Scope |
| --- | --- | --- |
| `s3` | Read and write the bucket settings Terraform manages. Read, write and delete objects. Run multipart uploads | The project bucket only. The write-prefix denies and the conditional single-copy create stay. Delete only under `lakehouse/` |
| `sts` | `AssumeRole` | The lakehouse reader and writer roles, plus the job try-writer role only with a `hai_try_*` session name |
| `ssm` | Read the 17 control-plane parameters, write the 16 managed ones (not the Airflow administrator password), read and write the ten AWS Polaris pair names (reader, e2e, bronze, silver and gold client ID and secret) for the AWS catalog setup, read AWS's public `/aws/service/ecs/*` parameters, start a Session Manager port-forwarding session to the project host, resume and end its own sessions | Exact parameter ARNs, taken from the defaults of the secret-name variables in `infra/iam/variables.tf` (the file lists none). No `DescribeParameters`, no path read |
| `secretsmanager` | `GetSecretValue` and `DescribeSecret` | The four shared API keys only, named by the defaults of their variables in `infra/iam/variables.tf` |
| `ecr` | Log in, push and pull images, create, delete, describe and set the lifecycle of repositories | `<PROJECT_NAME>_*_<ENVIRONMENT>` repositories |
| `ecs` | Create, update, delete and describe the cluster, the capacity provider and services. Register and describe task definitions. Run, stop, describe and list tasks. Describe container instances | The project cluster, services, task definition families, tasks and container instances (`container-instance/<cluster>/*`). No deregistering of task definitions |
| `autoscaling` | `SetDesiredCapacity` and describe calls. `CreateOrUpdateTags` with the `AmazonECSManaged` key only, which ECS adds while it creates the capacity provider | The host group only |
| `ec2` | Describe calls. Read the console output of the project host (`ec2:GetConsoleOutput`). Create and delete the data volume. Allocate and release the Elastic IP. Tag them while creating | Volumes and addresses with the exact `Project` tag. Instances that carry both the `Project` tag and the host's `Name` tag, `<PROJECT_NAME>_host_<ENVIRONMENT>`, in the project region and account. No console screenshot, password data, launch, template, network, image, snapshot or copy action |
| `dlm` | Create, update, delete and read the snapshot schedule | Schedules tagged `Project` |
| `logs` | Create, delete and set the retention of log groups. Read log events | `<PROJECT_NAME>_*_<ENVIRONMENT>` log groups |
| `cloudwatch` | Put, delete and describe alarms | `<PROJECT_NAME>_*_<ENVIRONMENT>` alarms |
| `sns` | Create, delete and set the attributes of the topic | The topic `<PROJECT_NAME>_alerts_<ENVIRONMENT>` only, not the budget's topic |
| `iam` | `PassRole` | The task execution and two control-plane task roles, plus the four read-job roles, to `ecs-tasks.amazonaws.com` only. The snapshot role to `dlm.amazonaws.com` |

The profile has no instance launch, launch template, host group create, update or refresh, network, image, snapshot-copy or
budget permission. A load balancer policy waits for the demo package. `*` stays only for describe and list calls and
`ecr:GetAuthorizationToken`, which AWS allows only on `*`, each limited to the project region. No policy grants a KMS
action: Parameter Store uses the AWS-managed key.

**Tags.** The cost budget counts a resource only when it carries the key `Project` with the project name as its value, so
every create that can carry tags needs exactly that. A request-tag condition ignores the case of the key, so a tag written
`project` would satisfy `aws:RequestTag/Project` and miss the budget. Each create therefore requires the value and, in
`aws:TagKeys` the exact key (`ForAnyValue:StringEquals`). The policy repeats both as two Denies
(`StringNotEquals` on the value and `ForAllValues:StringNotEquals` on the key set) that hold even if an allow is weakened
later. Nothing changes or removes `Project` afterwards. EC2 and ECS allow `CreateTags`, `TagResource` and `UntagResource`
only while a resource is created (`ec2:CreateAction`, `ecs:CreateAction`) and deny them otherwise. The other services deny
a request that names `Project` (in any letter case) when it removes the tag or sets it to any value other than the project
name. A Deny that keys on an existing `Project` tag also refuses the tag step of a create for image repositories, the alert
topic and the snapshot schedule, because AWS treats the new tags as `aws:ResourceTag` there. Log groups and alarms need the
same tag permission when they are created with tags. DLM grants management by the `Project` tag, so its tag statement also requires the resource's
`Project` tag, when present, to be the project name. These services allow other tags. An ECS service must carry `ecs:propagate-tags` of `SERVICE` or `TASK_DEFINITION`. A service update must not set `NONE`. RunTask requires the exact Project key and project value in explicit request tags, within the project cluster and task definition families. RunTask does not support the `ecs:propagate-tags` condition key, so IAM cannot require propagation ([ECS authorization reference](https://docs.aws.amazon.com/service-authorization/latest/reference/list_ecs.html#amazonecs-RunTask)). The launch commands retain `--propagate-tags TASK_DEFINITION` and `--enable-ecs-managed-tags`, with an explicit `--tags key=Project,value=<PROJECT_TAG>` ([one-shot launch](aws_control_plane.md#launching-a-one-shot-task)). A correctly tagged RunTask request without propagation is also authorized. A run task, a service create or an ECS tag request that sets its own `Project` tag to any other value is refused (a Deny on `aws:RequestTag/Project` that applies only when the tag is present).

The policy does not restrict which definition a service propagates from. The hardening backlog of the IAM least-privilege review lists that open item. It is a local record that is not in the repository. Service tasks carry the tags of the service or task definition, which carries the tag as well. Standalone tasks carry the explicit Project tag even when propagation is omitted. So Fargate tasks reach the budget.

The policy evaluator (`scripts/infrastructure/policy_evaluator.py`, cases in `scripts/infrastructure/policy_cases.py`, run by
`scripts.infrastructure.run_policy_e2e`) tests all of it offline: it renders the 13 files and the two policies of
`infra/iam/owner_policies/` that the IAM stack renders (the snapshot role and the task execution role) and evaluates requests the way IAM does, with explicit Deny first and key names compared without regard to case. The
inventory compares each policy's allowed actions with the approved list, the cases cover every approved action in its scope
and in another region, account and project, every creating action with seven wrong tag forms, every tag change and
removal and each forbidden action. The policies stay under the 6,144 character limit with the longest values. The run also
plants 58 defects in the policies (one per review finding and per rule above) and 28 in the policy scripts. Each must be
caught. The scripts have their own tests (`tests/test_iam_policy_scripts.py`, in-memory IAM, no AWS call). The report
is written to `data/e2e/policies/` by the local CI. Five request-context cases check the documented RunTask and service keys. Offline checks do not exercise AWS authorization or the requests Terraform sends.

The following rules describe the host and its network:

- The host accepts inbound traffic from the run-task group on the Polaris and
  Redis ports (`host_task_ports`, 8181 and 6379). It also accepts traffic from
  `owner_ip_cidrs` on `host_ingress_ports` (variables of the IAM stack, which owns the rules). Both lists are empty. The service UIs
  stay on loopback: reach them with a Session Manager port-forwarding session,
  which the project user may start only on project-tagged instances. Internet
  egress from the host and the run tasks is HTTPS only.
- The services use host networking, with task roles enabled for host-network
  tasks in the ECS agent. Host-mode containers share the host's network, so the
  boot script installs a rule that drops traffic to the instance metadata
  service from every user except root. The rule has its own nftables table
  (`inet platform_imds`), which Docker, firewalld and reloads of the xtables rule
  sets leave alone. `/etc/sysconfig/nftables.conf` includes it, so a reload of
  `nftables.service` loads it again in the same transaction.
  `platform_imds_guard.service` loads and checks it at every boot. The guard is
  part of `nftables.service`, so stopping or restarting the firewall stops or
  restarts Docker and the ECS agent too. Docker and the ECS agent require the
  guard and load and check the rule before every start, so neither starts
  without it. The check confirms that the table exists and that user 65534
  cannot connect to the metadata service. `platform_imds_watch.timer` runs the
  check every 10 seconds. It loads a missing rule again and stops the ECS agent
  and Docker when it cannot. A rule removed by hand can therefore stay missing
  for up to 10 seconds. The ECS agent, ecs-init and the Session Manager agent
  run as root and keep access. A container running as root would bypass the
  rule, so the control-plane task definitions must use non-root users. Metadata
  tokens are required, the hop limit is 1 and IPv6 metadata is off.
  The boot script also sets `ECS_DISABLE_PRIVILEGED=true` in the ECS agent configuration, so a task definition cannot ask for a
  privileged container, which could remove the rule. The configuration E2E asserts that the last value of the variable is
  `true` and shows that a boot without the line, with `false` or with `false` written after `true` is detected. Added Linux
  capabilities and writable host mounts stay open (the hardening backlog of the IAM least-privilege review lists both, in a local record that is not in the repository).
- Boot sets `kernel.core_pattern = core` and `fs.suid_dumpable = 0` in
  `/etc/sysctl.d/90-platform-core-dumps.conf`, which sorts after the image's
  systemd-coredump pipe handler. A pipe handler receives core dumps even when a
  container's core limit is zero. A plain file pattern does not. Boot reads both
  settings back and stops before joining the cluster when either differs.
- The host role holds only what the ECS agent and the boot script call. It
  covers registration and state reports for the project cluster, the poll
  endpoint and Session Manager registration and channels. It also attaches the
  project's Elastic IP and data volume, found by their `Name` and `Project` tags
  with `ec2:DescribeVolumes` and `ec2:DescribeAddresses` (limited to the project
  region). The boot script requires exactly one of each, because the owner's Auto
  Scaling group cannot carry IDs of resources that the platform stack creates
  later. Image pulls and
  log writes use the execution role, which every task definition names. The boot
  script turns on the agent's awslogs override for that. The execution role
  also reads only the control plane's Parameter Store SecureString
  parameters, `/<PROJECT_NAME>/<name>`, with `ssm:GetParameters` on exactly
  those parameter ARNs. Eight are always granted (postgres_password, airflow_db_password,
  marquez_db_password, airflow_fernet_key, airflow_jwt_secret,
  airflow_api_secret_key, redis_password and polaris_root_secret). The ninth, grafana_admin_password, is a separate inline policy
  that exists only while the IAM stack input `monitoring_enabled` is true, so the stack is unchanged while monitoring is off. It holds no
  Secrets Manager and no KMS permission: the ECS documentation requires
  `kms:Decrypt` only for a customer-managed key. These parameters use the
  AWS-managed key `aws/ssm`. The Airflow administrator password is not
  injected, so it has no grant. The stack holds no parameter value. The Polaris role reaches only `lakehouse/`. The other services' role
  has no permissions. A `Resource` of `*` remains only for actions that the AWS
  Service Authorization Reference lists without a resource type, each limited
  to the project region.
- The project user cannot launch an instance. It has no `ec2:RunInstances`, launch template, host group create, update or
  refresh, network, image or snapshot-copy permission, so it cannot choose what a host boots with. AWS's own scaling service
  launches the host from the template that the IAM stack owns, with the group at a template version that is a number
  (`tostring(latest_version)`, never `$Latest` or `$Default`), a minimum size of 0 and a maximum size of 1. The group also carries the `AmazonECSManaged` tag that ECS adds when it creates the capacity provider. The IAM stack declares it as ECS sets it, so the IAM plan shows no drift. The project
  user has two Auto Scaling permissions on that one group. `autoscaling:SetDesiredCapacity` turns the host on (1) and off
  (0). The group's maximum size keeps the count at 0 or 1, because Auto Scaling has no capacity condition key.
  `autoscaling:CreateOrUpdateTags` allows the `AmazonECSManaged` key only, which ECS adds with the project user's
  permissions while it creates the capacity provider.
  Terraform ignores the desired capacity after creation. A new template version (a resize, a new image or a new boot
  script) is a change the owner applies in the IAM stack. The group then replaces a running host through an instance
  refresh. To resize the host, edit `host_instance_type` in `infra/iam/host.tf`.
- Every new snapshot, by Data Lifecycle Manager, must carry the exact key
  `Project` with the project value in the request (the allow tests `aws:TagKeys`,
  and two Denies repeat it for the key and the value, so a tag written `project`
  is refused). The project user cannot create or copy a snapshot. A Deny holds
  that even if an allow is weakened. The schedule adds `Project` itself in `tags_to_add`, so turning tag
  copying off cannot leave a snapshot untagged: a snapshot without the tag fails
  to be created instead of costing money unseen. The data volume carries the tag
  itself, not only through default tags, so the final snapshot at teardown
  carries it too.
- The project user registers task definitions but cannot deregister them:
  `ecs:DeregisterTaskDefinition` takes no resource or condition, so a grant
  would reach every project's task definitions in the region. An administrator
  retires old revisions. A Terraform stack that replaces task definitions with
  the project profile must set `skip_destroy = true`, which keeps the old
  revision instead of deregistering it.
- Every boot step that can stall runs under a deadline (`run_within` in
  `infra/iam/host_boot.sh.tftpl`). GNU `timeout --kill-after=10` sends TERM
  at the deadline and KILL 10 seconds later. The script reports which one
  ended the step. A stalled step stops the script before the host joins the
  cluster. The configuration E2E runs the script against stateful stand-in
  commands with a stalled service start, format and mount, a format that
  ignores TERM, a firewall that fails to load and core dump settings that do
  not take effect. After a successful boot it reloads and flushes the firewall,
  restarts Docker and the ECS agent and runs the timer's check, with and without
  a working `nft`. It requires root allowed and users 1000 and 50000 denied.
  It also checks that every systemd directive sits in the section systemd reads
  it from (systemd ignores a misplaced one with a warning), restarts the
  firewall service through a model of systemd's dependency rules and requires
  Docker and the ECS agent to stop before the rule goes and start after it
  returns. A boot with `Requires=` moved into `[Service]` runs in the same
  suite and must be reported and shown unsafe, which proves the check fires.
  The boot cases include missing, duplicated, malformed and failed searches for
  the data volume and the Elastic IP by tag. The stand-ins model the kernel and systemd. The E2E exercises neither. The runner
  uses GNU `timeout` when one is on `PATH` and a stand-in with the same TERM,
  KILL and exit-status behavior otherwise.
- Every line the boot script and its commands write goes to
  `/var/log/platform_boot.log` and, with the prefix `platform_boot:`, to the
  serial console (`/dev/console`). The project user reads the console of the
  project host with `ec2:GetConsoleOutput` after the instance ends, because the
  log file ends with the instance. A separate process copies the log, so a
  console that cannot be opened or never accepts a write does not stop or hold
  a boot step. The exit handler creates a stop file, waits up to
  `console_flush_seconds` (4) for the last lines and then ends the copy. Each
  step logs a `starting:` and an `ended:` line, so a hang shows the stalled
  step. The last line is `finished: every step succeeded and ECS_CLUSTER is
  written.` or `FAILED: exit status <N> at line <L> during: <step>`. The AWS CLI
  calls that print JSON (`associate-address`, `attach-volume`) print nothing and
  the metadata token is never logged. An AWS error message can name the
  account, the role and the instance of the project. That text stays in the console,
  which needs `ec2:GetConsoleOutput` to read. The configuration E2E
  runs every boot case with a console file and requires the same lines in the
  log and on the console, each with the prefix, in order. It also boots with a
  console that is a directory and with one that is a named pipe nobody reads,
  and plants eight defects (final lines not flushed, copy left running, no
  prefix, AWS output on the console, a token in the log, an unbounded wait, a
  success line after a failure and no step markers). Each must be reported.
- At boot the host finds its Elastic IP and data volume by tag, associates the
  address, attaches the volume and mounts it at `/data`. It joins the cluster only after the volume is mounted. The data
  volume survives a replaced or resized instance. A new template version or a
  changed tag replaces the instance through an instance refresh.
- Every resource of both stacks carries `Project` set to the project name as it is,
  the value the data bucket carries too. So the $150 budget (an IAM stack resource, which the project user cannot change) filters on that tag
  and on the services the platform adds: Amazon Elastic Compute Cloud - Compute,
  EC2 - Other, Amazon Virtual Private Cloud, Amazon Elastic Container Service,
  Amazon Elastic Load Balancing, AmazonCloudWatch and Amazon EC2 Container
  Registry (ECR). Separate filters combine with AND, so S3 and Secrets Manager
  stay outside the budget. The service names are the Cost Explorer SERVICE
  dimension names. The first deploy must confirm that Budgets accepts them. The
  tag counts only once it is an active cost allocation tag in Billing. The
  budget is quarterly so that one amount covers the whole October to November
  build. The budget's alerts go to its own topic in the IAM stack and the run-failure alerts go to the
  platform stack's topic. Neither has a subscriber until the owner adds one. The project user's sns policy
  names only the second.
- The budget sends alerts only. There is no budget action: nothing stops spending at 100% or at any other percentage. The
  owner tracks spend by hand, turns the host off with `aws autoscaling set-desired-capacity --auto-scaling-group-name
  <PROJECT_NAME>_host_<ENVIRONMENT> --desired-capacity 0` and deletes what is no longer needed.
- `allow_destructive_teardown` lets a destroy delete ECR repositories that still
  hold images. The data volume has no final snapshot, because the project user cannot
  create one. The snapshots that the schedule made remain after a destroy. The
  catalog backups in S3 hold the catalog. Turn the host off before a destroy. The
  project user cannot disassociate the Elastic IP, but an instance that ends releases it. The snapshot role tags a
  snapshot only in the call that creates it and deletes only snapshots that
  carry its schedule's reserved `aws:dlm:lifecycle-schedule-name` tag together
  with the data volume's `Name` and `Project` tags.

**Lifecycle rules.** The bucket-wide rule, applied and read back from S3, aborts incomplete multipart uploads seven days after
initiation, with no expiration or transition of completed objects, historical
versions or delete markers. One exception covers only `lakehouse/`, where
Iceberg replaces and deletes its own table files: replaced versions there expire
30 days after they are replaced and orphaned delete markers are removed. Current
lakehouse files never expire. A second rule, `expire_replaced_dvc_versions`, covers only `dvc/`, the prefix of the DVC remote for the training
baseline. Replaced versions there expire after 30 days and orphaned delete markers are removed. Current DVC objects never expire. A third rule, `expire_athena_results`, covers only `athena/results/`, where Athena writes query results that can hold Confidential rows. Current results expire after 7 days and replaced versions after 1 day. S3 allows either an expiry in days or the delete-marker cleanup in one rule, so the delete markers of expired results stay. The mock-plan test checks sample keys, so no other prefix matches this rule. A fourth rule, `expire_replaced_mlflow_versions`, treats `mlflow/` like `dvc/`. It is the artifact prefix of the MLflow service. It exists only when the owner plans and applies with `terraform.sh --mlflow-retention` (the `mlflow_retention_enabled` input). Everything
outside `lakehouse/`, `dvc/`, `athena/results/` and `mlflow/` never expires. The storage stack and its mock-plan test hold the DVC, Athena and MLflow rules. The owner applies them. The Athena and MLflow rules are not applied yet. Local mock-plan tests, source-shape checks and live
configuration readback pass. The seven-day scheduler itself has not been
observed in a timed test and no deliberately incomplete upload was created for
verification.

The project's IAM policy grants only bucket-scoped
`s3:PutLifecycleConfiguration` for lifecycle changes. The storage stack
holds only the rules above. The owner uses IAM administration only for policy updates.
Storage deployment uses the project profile. This permission can change the
bucket's entire lifecycle configuration, so future changes still require full
plan review and preservation of existing rules. Private plans and live
verification evidence remain untracked under
`data/infrastructure/lifecycle_deployment/`.

**DVC baseline grant.** `infra/iam/owner_policies/job_dvc_baseline.json` is the policy for the training job role: `s3:GetObject`, `s3:PutObject`,
`s3:AbortMultipartUpload` and `s3:ListMultipartUploadParts` on `dvc/*` of the data bucket. It also grants `s3:ListBucket` only with an `s3:prefix` of `dvc/*`, so listing the bare prefix `dvc` (which would also list `dvc-private/`) is denied. It
grants no delete, no version read or listing, no bucket-location read and nothing outside `dvc/`. While `training_prepared` is false no stack attaches it. With the flag true,
`infra/iam/training.tf` attaches it to the `job_train_task` role. `scripts.infrastructure.run_jobs_iam_e2e` evaluates it with
`scripts/infrastructure/dvc_policy_cases.py` and plants six scope defects that each must be caught.

**Training roles and network.** The flag `training_prepared` (default false) adds two task roles and one security group in `infra/iam/training.tf`.
`job_train_task` holds the stage read, the DVC grant above and `job_features_read` (read of `lakehouse/features/*` and `lakehouse/feast/*`). It holds no lakehouse write and no access to `mlflow/`.
`mlflow_task` holds `mlflow_artifacts` only: read and write of `mlflow/*` of the data bucket, with listing limited to that prefix and no delete. The group `job_training` reaches S3, the image and log endpoints, Parameter Store,
the catalog on port 8181 and MLflow on port 5100. The host accepts ports 8181 and 5100 from this group and from no other job group. The shared job execution role gains the policy `train_images` (pull of the training and feature store repositories only) and `train_parameter` (injection of the MLflow administrator password only). The host execution role gains `mlflow_parameter` (injection of the MLflow database password and the administrator password only). All three exist only while the flag is true. The project user's IAM policy file carries a statement `PassTrainingRoles` that passes only `job_train_task` and `mlflow_task` to `ecs-tasks.amazonaws.com`, so the jobs and services stacks can register task definitions that name them. The owner applies that file with the policy scripts under the administrator profile before the services and jobs stacks: `manage_policies plan`, then `manage_policies apply` with the typed confirmation. The outputs `training_roles` and `training_network` are separate from `job_roles` and `job_network`, so the ECS target renderer is unchanged.
`scripts/infrastructure/training_policy_cases.py` evaluates both new policies in the same IAM check and plants scope defects that each must be caught.

**Pipeline roles.** `infra/iam/` also defines two IAM roles for pipeline jobs, a lakehouse reader and a lakehouse writer. Only the project's
own IAM user can assume both through `sts:AssumeRole`. The reader reads the bronze, silver and gold
prefixes of `lakehouse/`. The writer holds the object actions of the lakehouse policy on `lakehouse/silver/`, `lakehouse/silver_e2e/`,
`lakehouse/gold/`, `lakehouse/gold_e2e/` and `lakehouse/catalog_backups/` only. A session lasts at most 7200 seconds. The launcher asks for a
session per job (`docs/lakehouse_usage.md`, Pipeline AWS access), so no long-lived key reaches a container.

**Read jobs.** The project user's four service policy files carry the grants used to prepare and run read jobs. The IAM policy passes only `<PROJECT_NAME>_job_execution_<ENVIRONMENT>`, `<PROJECT_NAME>_job_offline_task_<ENVIRONMENT>`, `<PROJECT_NAME>_job_seal_task_<ENVIRONMENT>` and `<PROJECT_NAME>_job_read_task_<ENVIRONMENT>` to `ecs-tasks.amazonaws.com`. The STS policy allows the job try-writer role only with a session name matching `hai_try_*`. The try-writer role's own policy writes under `lakehouse/run_stage/*/tries/*` and grants nothing else. The S3 policy adds `s3:GetObjectVersion` only under `lakehouse/run_stage/`. Existing acquisition version reads retain their own scopes. The SSM statement `SetUpAwsPolarisPairs` allows `ssm:GetParameter` and `ssm:PutParameter` on exactly the ten AWS Polaris pair names: the reader, `e2e`, `bronze`, `silver` and `gold` client ID and secret, each from its own name variable in `infra/iam/variables.tf`. The AWS catalog setup (`scripts/lakehouse/aws_catalog_setup.py`) uses it to store each issued pair and to read it back, under the project profile. These names are separate from the 14 managed names and from the Mac `polaris_silver_*` and `polaris_gold_*` pairs. The policy grants no `ssm:GetParameters`, no `ssm:DescribeParameters` (it has no resource-level scope: it would list every parameter in the account and Region), no history, delete, label or tag action on them. The jobs execution role separately injects the same pair with `ssm:GetParameters`. The owner renders and applies these service policies with `infra.iam.scripts.manage_policies`. The IAM stack manages the job roles and their own policies without attaching a policy to the project user.

**Single-copy content folders.** The project's S3 policy has four statements for `bronze_objects/sha256/` and
`bronze_evidence/sha256/`, the folders that single-copy storage uses:

- The first statement reads current objects and exact versions (`s3:GetObject` and `s3:GetObjectVersion`).
- The second statement allows `s3:PutObject` only when the request carries the `If-None-Match`
  header and only for a key whose leaf after `sha256/` has exactly 64 characters (64 `?` wildcards), so an empty, a shorter
  and a longer leaf are not created. It grants no deletion, tagging or multipart action there.
- The third statement denies writes, tagging and multipart actions in these two folders for the key shapes that the older wildcard grants (`*/references/*`,
  `*/data/*`, `*/audit/*`, `*/manifests/*` and `*/*/datasets/*`) match.
- The fourth statement denies `s3:PutObject` for every key under `sha256/` that holds a percent sign, so a leaf with a literal encoded separator is not created.

IAM resource patterns
have only `*` and `?`, so no pattern limits the leaf to hexadecimal characters. The length-only pattern is the
strongest form that IAM can express, so the policy does not prove that the 64 characters are hexadecimal digits. The
only writer is the store step. It computes the leaf as the SHA-256 of the file. The index loader checks the leaf. It
refuses every key that is not exactly `sha256/<checksum>`, where the checksum is the SHA-256 recorded for that object.
The policy evaluator cases evaluate the rendered policy for nested keys, empty, short, long and percent leaves and the
older wildcard shapes in both folders. They fail when a write is allowed there. They also fail when the exact key loses its
conditional create or when a statement is missing or wider than these folders. The owner reviews and applies the policy
with the steps in [Switching on single-copy storage](lakehouse_usage.md#switching-on-single-copy-storage). The condition
is untested against real AWS.

**Deferred controls.** A deferral does not mean a control is implemented. The following
acquisition-stage deferrals apply to `aws_s3_bucket.data` only. Checkov
and trivy evidence retain each exception, resource and reason.

| Deferred control | Reason | Review trigger |
| --- | --- | --- |
| Event notifications (`CKV2_AWS_62`) | No event consumer exists. | Implementing an event consumer. |
| Cross-region replication (`CKV_AWS_144`) | Recovery objectives and a destination are not established. | Recovery design, before production. |
| KMS (`CKV_AWS_145`, trivy `AWS-0132`) | Acquisition uses SSE-S3/AES256. | Coordinated ingestion and IAM migration, before production. |
| Access logging (`CKV_AWS_18`, trivy `AWS-0089`) | Acquisition-stage deferral of a separate logging destination. The owner accepts the request-audit gap for this stage. | Before additional user or service access, production or new sensitive-data use. |

The platform stack's exceptions keep the owner-accepted budget design. Each is
recorded beside its resource and reported in the checkov and trivy evidence.

| Exception | Resource | Reason |
| --- | --- | --- |
| Flow logs (`CKV2_AWS_11`, trivy `AWS-0178`) | `aws_vpc.platform` (IAM stack) | Kept within the CloudWatch free allowance. |
| Default security group (`CKV2_AWS_12`) | `aws_vpc.platform` | Nothing uses it. Adopting it needs permission on an untagged group. |
| Internet egress (trivy `AWS-0104`) | host and task HTTPS egress rules (IAM stack) | No NAT gateway. The host and the tasks reach AWS endpoints and registries over HTTPS. |
| Unattached task group (`CKV2_AWS_5`) | `aws_security_group.task` (IAM stack) | Run tasks attach it when they start. |
| Public IP at launch (`CKV_AWS_88`) | `aws_launch_template.host` (IAM stack) | Needed to reach AWS before boot associates the Elastic IP. |
| Unattached Elastic IP (`CKV2_AWS_19`) | `aws_eip.host` | The host associates it at boot. |
| Customer-managed keys (Checkov: `CKV_AWS_189`, `CKV_AWS_136`, `CKV_AWS_158`, `CKV_AWS_26`. Trivy: `AWS-0027`, `AWS-0033`, `AWS-0017`, `AWS-0095`) | data volume, ECR repositories, log groups, alert topic, budget topic (IAM stack) | AWS-managed or default encryption. A customer-managed key adds cost. AWS Budgets cannot publish to a topic encrypted with the AWS-managed key. |
| Container Insights (`CKV_AWS_65`, trivy `AWS-0034`) | `aws_ecs_cluster.platform` | Paid custom metrics. |
| Log retention under a year (`CKV_AWS_338`) | `aws_cloudwatch_log_group.platform` | Seven days keeps logs within the free allowance. |

Acquisition
receipts establish pipeline provenance, not an independent record of every
bucket request. Review CloudTrail data events alongside server access logging
when a review trigger is reached. Server access logs are best-effort, not a
complete audit trail ([AWS documentation](https://docs.aws.amazon.com/AmazonS3/latest/userguide/ServerLogs.html)).

Existing AES256 encryption, private access, versioning and destruction
protection are preserved. Synthetic E2E checks verify that the four exceptions
do not suppress other resources or the versioning control. Passing static
checks with these exceptions does not establish that deferred controls exist.
No AWS changes implementing these four deferred controls have been applied.

## IAM preflight

Before an attended AWS step, the owner runs one read-only check that each project role holds the grants the project needs:

```sh
.venv/bin/python -m scripts.infrastructure.iam_preflight
```

The command reads the profile, region and account from `.env` and confirms that the profile authenticates as its same-named user. Then it calls `iam simulate-principal-policy` once for each row of `config/aws/iam_preflight_checks.json`. Each row names a role, an action, a resource and, where the policy has a condition, the condition keys. It prints one `PASS` or `FAIL` line per row and exits with 1 if any row fails. Only an `allowed` decision passes. A denied simulation call, a missing role, an answer that is not understood and a call that outlives its deadline (30 seconds for a call, 600 for the run) are failures with a message, never passes. Exit status 2 means the setup is wrong. The check makes no change in AWS.

The project profile needs `iam:SimulatePrincipalPolicy` on the project roles. The statement `SimulateProjectRoles` in `infra/iam/policies/healthcare_hai_prediction_iam_policy.json` grants it. It takes effect after the owner applies the policies with `infra/iam/scripts/manage_policies.py`.

`.venv/bin/python -m scripts.infrastructure.iam_preflight_policies` checks the table offline. It renders the policies that the IAM stack attaches to each role and evaluates every row, so a role change that removes a listed grant fails without AWS. The table covers the calls of a first layer publish: the run claim and run folder upload, the input read and its version listing, the code stage read, the Feast registry and Redis password of the features role and the drift role with its DVC read and record upload. A policy that a table row depends on must be attached by a real `aws_iam_role_policy` resource that uses it and that exists only while its preparation flag is on. A resource in a comment or a flag that was removed fails the check. `scripts.infrastructure.run_iam_preflight_e2e` runs both commands against a stand-in `aws` that answers from the rendered policies, with planted defects. The stand-in models the policy files and not IAM itself.
