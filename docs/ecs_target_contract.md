---
title: ECS read target contract
description: Proposed deployment captures, private target fields and jobs-wrapper interface for read tasks.
last_updated: 2026-10-28
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

The bronze cutover (`scripts/lakehouse/bronze_cutover.py`) enables the approved Session Manager route with an owned relay for its `register` and `verify` steps only. Its network acceptance check runs before the tunnel starts: the local tunnel and relay ports differ from the Mac Polaris port 8181. The host must be one running instance without a public address on the instance or any interface. Every other catalog-dependent Mac preparation remains disabled. No public catalog route is introduced.

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

An apply still requires approval for that exact saved plan. The renderer supplies six environment-derived base inputs: `aws_profile`, `aws_region`, `data_bucket_name`, `environment`, `expected_account_id` and `project_name`. Other declared variables must retain their committed defaults. A nondefault image or deployment input needs an explicit reviewed extension to this renderer before apply. Arbitrary variable injection remains refused.

`validate` renders local base inputs without an identity call. `plan` verifies the project identity. `apply` also inspects the saved plan and rejects secret resources, incomplete variable sets and mismatched inputs. The saved-plan inspection has a 120-second limit.

## Local verification

```bash
.venv/bin/python -m scripts.infrastructure.run_ecs_render_e2e
```

The artifact under `data/e2e/ecs_target/` records source hashes and every command case. The suite runs the native renderer commands. Its wrapper and plan-reader executables are inert captures. It contacts no cloud service. AWS authorization, Terraform plans, images, catalog readiness and integrated launcher behavior require their separate lead-run checks.
