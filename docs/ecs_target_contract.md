---
title: ECS read target contract
description: Proposed deployment captures, private target fields and jobs-wrapper interface for read tasks.
last_updated: 2026-10-25
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
| `--jobs-outputs` | `task_definition_arns`, `container_definitions`, `task_definition_settings`, `execution_digests` | Proposed jobs export |
| `--image-measurements` | `schema_version`, `platform`, `images` | Recorded image measurements |

IAM role keys stay `execution`, `offline_task`, `seal_task`, `read_task` and `try_writer`. Each record retains its `name`, `arn` and `unique_id`. The network retains `subnet_id`, `security_group_ids.offline`, `security_group_ids.lakehouse` and `endpoints_enabled`. These output names are consumed unchanged.

The jobs export follows the control-plane conventions. Its first three maps contain exactly `analytics_offline`, `quality_offline`, `seal`, `analytics_read` and `spark_read`. `container_definitions` contains Terraform's JSON string for each task. `task_definition_settings` contains the complete configured task attributes used by the existing execution-pin module. `execution_digests` contains that module's unchanged `task_name/container_name` keys. The renderer consumes those pins. It does not create another execution-pin algorithm.

The jobs export and its five logical keys require infrastructure review. The interface does not assert that the jobs root exists or that a definition is active. The dispatch caller must describe the active fixed revision and compare its execution pin before `RunTask`.

## Image measurements

Record `analytics`, `quality` and `spark` under `images`. Each record contains its full ECR `image` reference, `compressed_bytes` and `uncompressed_bytes`. The platform is `linux/arm64`. Both byte counts must be positive integers. Bind measurements to the pushed digest and its platform repository. Reuse the existing image publication tool.

Measure compressed bytes from the registry manifest layers. Measure unpacked bytes from the same locally built ARM64 image. Keep the measurement commands and digest readback in the image artifact. The renderer rounds each byte count up independently to GiB. A missing measurement or digest mismatch causes refusal. The selector adds both figures to its own storage formula.

## Catalog binding and networks

`--catalog-binding` supplies verified host metadata, the private HTTP endpoint on port 8181, realm, authority epoch, registration inventory digest and readiness evidence digest. It supplies parameter ARNs only. Parameter values never belong in this record.

The read proposal permits the project-prefixed `aws_polaris_reader_client_id` and `aws_polaris_reader_client_secret` pair. Reader definitions contain exactly that pair. Offline and seal definitions contain no injected secret. A deployment with different reader parameter names needs a reviewed contract update.

Without a binding, `catalog` is null. The selector must refuse every lakehouse job with a null catalog. A non-null binding still requires the registration and native network checks in the [real-run procedure](ecs_real_runs.md). A record's presence does not prove service readiness.

`class_networks` maps offline and seal jobs to the exported offline group. It maps read jobs to the exported lakehouse group. The groups must differ. The endpoint subnet must belong to the platform output. Every application request disables public IPs. The definition checks require ARM64 Linux, Fargate compatibility, the fixed bootstrap, private scratch volumes, a non-root user, dropped capabilities and a 90-second stop timeout.

Catalog-dependent Mac preparation remains disabled in this proposal. Its transport label records the approved Session Manager route with an owned relay. The lead must supply native class-network acceptance before a separate change enables that route. No public catalog route is introduced.

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

The `infra/jobs/` root registers the five read definitions. Its default plan creates no resources. Registration requires `jobs_enabled = true`, selected `enabled_definitions`, digest-pinned images, merged job role ARNs, reviewed task resources, the existing jobs log group and the fixed cloud cap. The root creates no services or running tasks. Its dispatch output requires private class networks and hard stops. The dispatcher must enforce admission and confirm task stops.

Prepare a nonsecret JSON file that follows the [preparation schema](../data_contracts/infrastructure/jobs_preparation.schema.json). Its envelope contains `schema_version: 1`, `root: "jobs"` and `variables`. The schema lists the allowed prepared variables. Base identity settings cannot appear in that map. Read registration accepts only `execution`, `offline_task`, `seal_task` and `read_task` under `job_role_arns`. The IAM capture retains `try_writer` for the runtime grant contract, but registration inputs refuse that unused key. Record the file's SHA-256 during review. Supply the same path and reviewed digest for planning and saved-plan inspection:

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

The artifact under `data/e2e/ecs_target/` records source hashes and every command case. The suite runs the native renderer commands. Its wrapper and plan-reader executables are inert captures. It contacts no cloud service. AWS authorization, Terraform plans, images, catalog readiness and integrated launcher behavior require their separate lead-run checks.
