---
title: Real AWS read verification
description: Bounded procedures and evidence requirements for cloud identity, admission and actual gold spill runs.
last_updated: 2026-10-28
audience: [developer, operator]
status: Proposed
---

# Real AWS read verification

Prepare each approved cloud run with the [procedure data](../config/orchestration/ecs_real_runs.json). The shared runner consumes that data and `prepare_run` from `scripts.infrastructure.ecs_real_runs`. This module prepares a checklist. Cloud execution and acceptance remain separate operations.

## Prerequisites

1. Accept the integrated read code and exact CI results. Verify the five fixed application definitions and digest-pinned ARM64 images.
2. Apply the approved endpoint and role plans. Verify the catalog host identity, private IP and native read-class network.
3. Complete both prerequisite real publishes and read-only registration. Retain their source snapshots and reader reservations.
4. Record current regional price evidence and the approved run's cost ceiling, code hash and absolute UTC window.
5. Render and check the [target](ecs_target_contract.md). Keep catalog-dependent Mac preparation disabled except the bronze cutover's route, which runs only after a passing Mac-side route check of the bound host and its network acceptance check.

The early one-task identity check must pass before grant delivery relies on task-session naming. The two-task identity check below remains required. A failed premise stops the run. Return it to the lead without broadening grant access.

No dataset bytes move from the Mac to AWS. These runs use existing S3 lakehouse objects and registered snapshots. Only the verified code archive, job descriptor and per-try grants are uploaded. Source rows stay in the approved task workspace or run prefix. Controller records contain permitted counts, identities, digests and measurements only.

## Prepare a run card

The [request schema](../data_contracts/orchestration/ecs_real_run_request.schema.json) requires the complete registration inventory. Its fields match the registration tool's portable inventory. The digest uses that tool's sorted compact JSON with a final newline. The request, target catalog binding and calculated inventory digest must agree.

The [request example](../data_contracts/orchestration/examples/ecs_real_run_request.json) contains synthetic references and supplies no approval. Replace it with the accepted registration inventory and exact approved run inputs. Run the registration tool's inventory verification first. The preparation module checks identity bindings and record structure. It cannot verify that the cloud tables exist.

```bash
.venv/bin/python -m scripts.infrastructure.ecs_real_runs \
  --case R1 \
  --target data/orchestration/ecs_target.json \
  --run-inputs data/orchestration/approved_read_run.json \
  --output data/e2e/ecs_target/approved_read_run/run_card.json
```

Use `R2` for capacity and stopping. Use `R3` for gold spill. Every card sets `proof: A`, `requested_target: ecs`, `preparation_only: true` and `execution_verified: false`. Every observation starts pending. Preparing a card never dispatches a task or turns pending observations into passes.

The shared runner must bind the card's target digest and code hash before execution. It must preserve the absolute window across setup, queue admission, retries, reconciliation and cleanup. Task deadlines leave 90 seconds for stopping and 120 seconds for cleanup. Both periods remain inside the outer window. Expiry causes a visible failure with remaining ownership recorded.

The runner saves observations and their sanitized evidence hashes under the card's run folder. It must refuse a cloud proof request when its cloud execution adapter or approved inputs are absent. It must preserve the requested proof level.

## Identity and actual reads

The outer limit is two hours. Begin with two tasks and their separate grants. Keep both tasks counted against the cloud cap.

1. Compare each authenticated session suffix with its task ID and exported role ID. Prove own-grant access and denial of the other task's grant. Include the known upstream grant denial.
2. Prove the read role cannot write. Prove the try grant cannot write outside its scope. Test conditional-write denial only in approved staging locations.
3. Read the registered actual source snapshots and pinned S3 versions. Record reader bindings, object hashes, task revisions and image digests. Exercise the complete remote acceptance and release path.
4. Record the class-network matrix, hard-stop settings, cloud cap and each claimed cloud CI suite's native evidence. Record starting disk use and task metadata storage.
5. Release the owned run objects and tasks. Record multipart cleanup, delete markers and the versions that remain.

Lakehouse tasks must reach their approved catalog and S3. Offline tasks must not reach the catalog. Both classes must fail to reach Redis, unrelated hosts and the internet. The native Mac-container route remains a separate required observation. Mark it blocked when acceptance is absent. Keep catalog-dependent Mac preparation disabled.

The one attended bronze cutover uses Mac-side route acceptance instead (owner decision, Oct 28 2026, described in [lakehouse usage](lakehouse_usage.md#bronze-cutover-to-the-aws-catalog)). Its `route-check` records that the tunnel and relay reach only the catalog port of the bound host, that the host's security groups admit inbound traffic only from project security groups (same account and VPC, with the project tag) and that no Mac Docker network reaches the tunnel or relay. That record does not replace this class-network matrix. The matrix stays required before any ECS job uses the catalog.

The cap includes pending, running, stopping and unknown tasks. An unconfirmed stop keeps its reservation. The budget alert is a warning to the owner. Lifecycle expiration remains a configuration assertion. A delete marker does not prove old versions disappeared.

## Capacity and stopping

The outer limit is one hour. Use approved effect-free tasks and isolated run prefixes.

1. Run the approved on-demand diagnostic. Verify denial when `propagateTags` is missing. Use only an approved task-level failure for catalog error handling.
2. Exercise the fixed deadline and cancellation during provisioning or running. Confirm `STOPPED`. Record a stop failure only if one occurs.
3. Run one Mac heavy job beside multiple approved AWS tasks. Prove excess cloud admission is refused before dispatch. Keep the Mac lock until its child is absent.
4. Verify permitted non-write fallback from measured AWS capacity evidence. Fence the pending cloud try before Mac admission. Preserve the original deadline when the Mac is busy.
5. Confirm each job's cleanup preserves the other jobs. Record every unresolved task and process before returning control.

Do not exhaust a real quota, stop a shared catalog, weaken IAM or manufacture an untagged task. Application, integrity and credential failures cannot trigger fallback. A fallback result cannot satisfy cloud acceptance. Write jobs never move to the Mac.

## Actual gold spill

The outer limit is four hours. Use the existing published silver snapshots, the actual cloud extract and the actual gold import and dbt build.

1. Record the approved controlled storage refusal. Preserve its intended failure cause. Use no generated large input.
2. Run the actual workload with the approved memory and temp limits. Measure transfer, hashing and scan rates in the task.
3. Sample `duckdb_temporary_files()` through the build connection. Record its nonzero peak, minimum free disk and storage left after image, input and output footprints.
4. Check staging at most 25 percent of the step, each chain at most 30 minutes and spill at most half the remaining storage. Preserve the 5 GiB disk margin and record all units.
5. Verify the approved portable input path and release the run. Supply the inventory and measurements for the independent workload audit.

Count compressed and unpacked images, copied inputs, declared outputs, scratch and buffers. Report missing spill as unproven. A failed threshold holds acceptance. Return a bounded sizing or chain-task proposal to the lead. Preserve the fixed deadline and actual workload.

## Artifact and cleanup

Save the code and target hashes, image digests, immutable source inventory, approved window and cost ceiling. Save expected and observed results with sanitized evidence for every observation. Record substitutions and blocked checks. Keep each controller result file at or below 32 MiB. Store no source rows, secret values, raw service errors or private worker witness values.

Stop and confirm each owned task and local process before marking cleanup done. Keep unresolved tasks counted and preserve their records. List current delete markers, remaining versions and incomplete upload status. Shared endpoints remain enabled while another admitted run needs them. Record a failed teardown and its remaining cost.

The full runner must collect native evidence. A prepared card, exported target, mocked plan or passing file-rendering suite cannot supply cloud acceptance.
