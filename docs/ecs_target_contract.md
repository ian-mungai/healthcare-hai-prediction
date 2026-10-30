---
title: ECS read target contract
description: Proposed deployment captures, private target fields and jobs-wrapper interface for read tasks.
last_updated: 2026-10-29
audience: [developer, operator]
status: Proposed
---

# ECS read target contract

Review this proposal before connecting the selector or jobs infrastructure. The [schema](../data_contracts/orchestration/ecs_target.schema.json) defines the private target. The [example](../data_contracts/orchestration/examples/ecs_target.json) uses synthetic deployment values and keeps execution disabled.

## Capture interface

The renderer accepts explicit JSON files. It executes no cloud command. Each Terraform capture uses the `terraform output -json` envelope: `sensitive`, `type` and `value`. Required outputs must exist, contain known values and have `sensitive: false`.

| Capture flag | Required outputs or fields | Authority |
| --- | --- | --- |
| `--storage-outputs` | `bucket_name` | Storage root |
| `--platform-outputs` | `cluster_arn`, `project_tag`, `public_subnet_ids`, `ecr_repository_urls`, `log_group_names.jobs` | Platform root |
| `--iam-outputs` | `job_roles`, `job_network` | IAM job outputs |
| `--jobs-outputs` | `task_definition_arns`, `container_definitions`, `task_definition_settings`, `execution_digests`, `dispatch_contract`. Writer captures also require `writer_dispatch_contracts` | Jobs export |
| `--image-measurements` | `schema_version`, `platform`, `images` | Recorded image measurements |

IAM role keys require `execution`, `offline_task`, `seal_task`, `read_task` and `try_writer`. Optional writer keys are `write_e2e_task`, `write_silver_task`, `write_gold_task`, `write_features_task` and `write_bronze_task`. Each record retains its `name`, `arn` and `unique_id`. The network retains `subnet_id`, `security_group_ids.offline`, `security_group_ids.lakehouse` and `endpoints_enabled`. These output names are consumed unchanged.

The jobs export follows the control-plane conventions. Its first three maps require `analytics_offline`, `quality_offline`, `seal`, `analytics_read` and `spark_read`. They permit only five additional definitions: `spark_write_e2e`, `spark_write_silver`, `spark_write_gold`, `spark_write_features` and `spark_write_bronze`. Each selected writer requires its matching role. Prepared writer roles can exist before their definitions. `container_definitions` contains Terraform's JSON string for each task. `task_definition_settings` contains the complete configured task attributes used by the existing execution-pin module. `execution_digests` contains that module's unchanged `task_name/container_name` keys. The renderer consumes those pins. It does not create another execution-pin algorithm.

The jobs export and its selected logical keys require infrastructure review. The interface does not assert that the jobs root exists or that a definition is active. The dispatch caller must describe the active fixed revision and compare its execution pin before `RunTask`.

## Optional writer definitions

Writer definitions use the digest-pinned Spark image and the same fixed bootstrap, execution role and execution-pin module as the readers. Selection stays empty by default. Registration creates no task, schedule, catalog grant or layer activation.

`writer_parameter_names` maps each `write_<layer>_task` role key to a two-name list. The first name is `aws_polaris_<layer>_client_id`. The second is `aws_polaris_<layer>_client_secret`. Each name uses its reviewed Parameter Store path. The values never enter Terraform. Obtain the test pair from `job_writer_e2e_contract` and production pairs from `job_writer_layer_contract.parameter_names`. Consume role ARNs from `job_roles`.

Each container injects only its own pair as `POLARIS_CLIENT_ID` and `POLARIS_CLIENT_SECRET`. The test definition binds `write_e2e_task` and the e2e pair. Reader definitions retain the reader pair. The renderer refuses role and parameter swaps between layers.

The jobs export adds one `writer_dispatch_contracts` record for each selected writer. Each record fixes `job_class`, `task_role_key`, `parameter_names`, `capacity_providers`, `network` and `assign_public_ip`. The capacity list is exactly `["FARGATE"]`. The network is `lakehouse` and public IPs are `DISABLED`. The target renderer checks the complete contract against the actual container bindings before writing. The general `dispatch_contract` carries the exact selected `definition_classes`, `class_networks` and `class_capacity_providers` maps. The renderer validates them and retains both class placement maps in the target. The general `capacity_providers` list describes readers only.

The target requires `class_capacity_providers`. Offline, seal and read classes retain `["FARGATE_SPOT", "FARGATE"]` in that order. Every selected writer requires `lakehouse_write: ["FARGATE"]`. A missing writer strategy or any writer Spot entry invalidates the target. The target also requires `assign_public_ip: "DISABLED"`. Refresh captures and render the target before use when either required field is absent.

The dispatcher must validate the target before submitting a task. For `lakehouse_write`, it must submit `capacityProviderStrategy = [{ capacityProvider = "FARGATE", weight = 1 }]` explicitly and disable public IP assignment. It must refuse an omitted strategy or any Spot entry. The cluster default is not a writer strategy. Writer dispatch must remain disabled until that request enforcement is integrated and verified.

Test admission remains limited to the closed `aws_qualify` binding. Production writers need accepted test qualification, separate layer activation and exact run approval. Task registration proves none of those runtime conditions. The target retains all five read definitions when any writer is added.

## Image measurements

Record `analytics`, `quality` and `spark` under `images`. Each record contains its full ECR `image` reference, `compressed_bytes` and `uncompressed_bytes`. The platform is `linux/arm64`. Both byte counts must be positive integers. Bind measurements to the pushed digest and its platform repository. Reuse the existing image publication tool.

Measure compressed bytes from the registry manifest layers. Measure unpacked bytes from the same locally built ARM64 image. Keep the measurement commands and digest readback in the image artifact. The renderer rounds each byte count up independently to GiB. A missing measurement or digest mismatch causes refusal. The selector adds both figures to its own storage formula.

## Catalog binding and networks

`--catalog-binding` supplies verified host metadata, the private HTTP endpoint on port 8181, realm, authority epoch, registration inventory digest and readiness evidence digest. It supplies parameter ARNs only. Parameter values never belong in this record.

The read proposal permits the project-prefixed `aws_polaris_reader_client_id` and `aws_polaris_reader_client_secret` pair. Reader definitions contain exactly that pair. Offline and seal definitions contain no injected secret. A deployment with different reader parameter names needs a reviewed contract update.

Without a binding, `catalog` is null. The selector must refuse every lakehouse job with a null catalog. A non-null binding still requires the registration and native network checks. A record's presence does not prove service readiness.

`class_networks` maps offline and seal jobs to the exported offline group. It maps read jobs to the exported lakehouse group. A target with writer definitions also maps `lakehouse_write` to that group. The groups must differ. The endpoint subnet must belong to the platform output. Every application request disables public IPs. The definition checks require ARM64 Linux, Fargate compatibility, the fixed bootstrap, private scratch volumes, a non-root user, dropped capabilities and a 90-second stop timeout.

The bronze cutover (`scripts/lakehouse/bronze_cutover.py`) enables the approved Session Manager route with an owned relay for its `register` and `verify` steps only. The host is the one in-service instance of the approved host group from the platform outputs. Both steps refuse to open the route without a passing route check of that host for the same cutover (`route-check`). For this one attended cutover the owner approved a Mac-side check. The check requires three conditions. The tunnel and relay reach only the catalog port of the bound host. The host's security groups admit only project security groups (same account and VPC, with the project tag). No Mac Docker network reaches the tunnel or relay. The check does not observe the class-network matrix. The first dry runs and real publishes give that evidence. The network acceptance check runs before the tunnel starts and requires the local tunnel and relay ports to differ from the Mac Polaris port 8181. The host must be a running project instance. Every security group on the host or its interfaces admits inbound traffic only from project security groups (same account and VPC, with the project tag), never from an IP range or prefix list. Every other catalog-dependent Mac preparation remains disabled. No public catalog route is introduced.

## Private target writer

Run from the repository root after obtaining reviewed captures. The paths below hold exported metadata, not state or variable files.

```bash
.venv/bin/python -m scripts.infrastructure.render_ecs_target \
  --storage-outputs data/orchestration/storage_outputs.json \
  --platform-outputs data/orchestration/platform_outputs.json \
  --iam-outputs data/orchestration/iam_job_outputs.json \
  --jobs-outputs data/orchestration/jobs_outputs.json \
  --image-measurements data/orchestration/image_measurements.json \
  --catalog-binding data/orchestration/catalog_binding.json
```

The default destination is ignored `data/orchestration/ecs_target.json`. The writer validates before replacing it. It writes deterministic JSON atomically with mode 0600. Invalid captures preserve the existing destination. `--check` compares bytes and mode without writing. `--enable` explicitly sets the execution switch after deployment acceptance. Schema validation never supplies run approval.

The writer rejects protected deployment paths, duplicate JSON fields and records above 32 MiB. Errors identify the failed contract without repeating supplied values. The file fixes three tasks, 16 vCPU, 120 GiB and the 32 MiB result-file limit. Hard stops remain required. A budget alert cannot authorize admission or replace a stop.

The selector can call `validate_target` after reading the target. It must preserve runtime checks for catalog authority, admission, actual task identity and active revisions. The infrastructure writer cannot prove those changing conditions from exported files.

## Jobs wrapper

The wrapper selects the jobs root with `--jobs`. It preserves project-profile verification, saved-plan checks, explicit input arguments and refusal of injected Terraform CLI arguments.

```bash
bash scripts/infrastructure/terraform.sh --jobs plan -out=review.tfplan
bash scripts/infrastructure/terraform.sh --jobs apply review.tfplan
bash scripts/infrastructure/terraform.sh --jobs validate
```

An apply requires approval for that exact saved plan. The renderer supplies six environment-derived base inputs: `aws_profile`, `aws_region`, `data_bucket_name`, `environment`, `expected_account_id` and `project_name`.

The `infra/jobs/` root registers the five read definitions and any explicitly selected writer definitions. Its default plan creates no resources. Registration requires `jobs_enabled = true`, selected `enabled_definitions`, digest-pinned images, merged job role ARNs, reviewed task resources, the existing jobs log group and the fixed cloud cap. The root creates no services or running tasks. Its dispatch output requires private class networks and hard stops. The dispatcher must enforce admission and confirm task stops.

Prepare a nonsecret JSON file that follows the [preparation schema](../data_contracts/infrastructure/jobs_preparation.schema.json). Its envelope contains `schema_version: 1`, `root: "jobs"` and `variables`. The schema lists the allowed prepared variables. Base identity settings cannot appear in that map. Read registration accepts `execution`, `offline_task`, `seal_task` and `read_task` under `job_role_arns`. Writer registration also accepts the five dedicated writer keys and requires their matching `writer_parameter_names` pairs. The preparation schema permits only the same closed definition, role and resource inventories. The IAM capture retains `try_writer` for the runtime grant contract, but registration inputs refuse that unused key. Record the file's SHA-256 during review. Supply the same path and reviewed digest for planning and saved-plan inspection:

```bash
bash scripts/infrastructure/terraform.sh --jobs \
  --preparation-inputs <REVIEWED_JSON_PATH> \
  --preparation-sha256 <REVIEWED_SHA256> plan -out=review.tfplan
bash scripts/infrastructure/terraform.sh --jobs \
  --preparation-inputs <REVIEWED_JSON_PATH> \
  --preparation-sha256 <REVIEWED_SHA256> apply review.tfplan
```

The renderer checks the file bytes before writing private generated inputs. It refuses duplicate JSON fields, unknown variables, the wrong root and a changed digest. The renderer sorts selected definition names to match Terraform set ordering. Apply compares every prepared value with the saved plan. Remaining variables must retain their committed defaults. Direct `-var` arguments, `-var-file` arguments, `TF_VAR_*` settings and injected Terraform CLI arguments remain refused. The environment-variable refusal applies to the storage, IAM, platform and jobs stacks, including the separate probe root.

The separate `infra/jobs/identity_probe/` root creates nothing by default. Select it with `--jobs --identity-probe`. Its preparation envelope uses `root: "identity_probe"`. It needs only the approved existing Spark seed digest, merged execution and offline role identities, the jobs log group and the reviewed foundation network. It has no dependency on application definitions or rebuilt worker images.

The fixed probe command uses a 60-second timeout. It compares task metadata with one regional STS identity call and exports only hashed identifiers. Its launch contract permits a public IP on the foundation network within a 30-minute outer slot. This exception belongs only to the separately approved diagnostic task. Registration does not launch it or establish the task-session premise.

`validate` renders selected base and preparation inputs without an identity call. `plan` verifies the project identity. `apply` also inspects the saved plan and rejects secret resources, incomplete variable sets and mismatched inputs. The saved-plan inspection has a 120-second limit.

## Local verification

```bash
.venv/bin/python -m scripts.infrastructure.run_ecs_render_e2e
.venv/bin/python -m scripts.infrastructure.run_jobs_preparation_e2e --output <NEW_DIRECTORY>
```

The `lakehouse_bronze` check group runs both suites. The `terraform_checks` group validates and tests the `infra/jobs` and `infra/jobs/identity_probe` roots. The artifact under `data/e2e/ecs_target/` records source hashes and every command case. The suites run the native renderer commands. Writer cases cover each definition, role and pair swaps, Spot refusal, exported class rules, private placement, pins and preservation of the five required readers. The wrapper and plan-reader executables are inert captures. It contacts no cloud service. AWS authorization, Terraform plans, images, catalog readiness and integrated launcher behavior require their separate lead-run checks.
