# Orchestration Contracts

The frozen contracts that the launchers, the host job broker and the Airflow DAGs share. Packages code against them
and do not change them; a needed change goes to the repository owner first. Check them with
`.venv/bin/python -m scripts.orchestration.run_contracts_e2e`, whose report under
`data/e2e/orchestration_contracts/` lists the SHA-256 of every file here.

| File | Contract |
| --- | --- |
| `submission_request.schema.json` | What a caller may send: template, run ID, mode and attempt, nothing else |
| `launch_spec.schema.json` | The launch specification of every container template (`config/orchestration/job_templates.json`) |
| `ledger_line.schema.json` | One line of a run's ledger |
| `examples/job_templates.json` | One complete specification per template (13 templates, placeholder image digests) |
| `examples/ledger_states.json` | Every ledger state a submission can be in |
| `examples/submission_requests.json` | Accepted requests and one refused request per caller override |

The submission interface itself is `orchestration/interface.py`. `orchestration/fake.py` implements it in memory for
package tests.

## Run Identity

- **Run ID:** `<UTC start as YYYYMMDDTHHMMSSZ>-<8 hex characters>`, for example `20261011T090000Z-0a1b2c3d`. A retry
  with different code, specification or inputs is a new run with a new run ID.
- **Submission ID:** `<run_id>:<template>:<mode>:<attempt>`. A repeated request with the same ID returns the same
  handle and starts nothing.
- **Container name:** `hai-<run_id>-<template>-<attempt>`, so a second create of one submission fails instead of
  starting a duplicate. Containers carry the labels `hai.run_id`, `hai.template`, `hai.attempt`,
  `hai.submission_id` and `hai.spec_sha256`; the run lock fences a run by its `hai.run_id` label.

## Run Directory and Build File

- **Run directory:** `data/analytics/dbt/runs/<run_id>/`, the only folder a job may write. It holds the build file
  `build.duckdb`, dbt's `target/silver/` and `target/gold/` and the logs `logs/silver/` and `logs/gold/`, plus
  `pins.json` (the pinned bronze snapshot IDs).
- **Build file:** silver and gold are built into the same `build.duckdb`. Silver validation records its SHA-256 and the
  gold build checks it again before it starts.
- **Run record:** `data/e2e/gold/<run_id>/run.json`, written by the run's record step; the lock is released only after
  it exists. The build file is deleted only after an accepted run.

## Ledger

- **Location:** `data/orchestration/ledger/<run_id>.jsonl` in the main checkout, beside the run registry.
- **Durability:** the broker appends one line, then fsyncs the file before the action the line announces; the folder is
  fsynced when a ledger is created. Lines are never rewritten.
- **Order per submission:** `intent` (before any container exists), then `started` (after create) or `adopted` (after a
  broker restart finds the named container), then `finished` (before the container is removed). `failed_closed` stops
  the submission for the owner when reconciliation finds two containers or a different specification hash.
- **Recovery:** before any dispatch the broker reconciles every unfinished intent with Docker by container name: a
  match is adopted, no match is dispatched once and an ambiguous match fails closed.

## Modes and Classes

`fixture` runs offline on synthetic data; `pin_bronze` and `verify_bronze` are recorded no-ops in that mode.
`publish_fixture` writes only the `gold_e2e` namespace; `real` writes `gold`. Each template names the network and
secret class of each mode it supports (`offline`, `lakehouse_read`, `lakehouse_write`, `collector`, `training`,
`serving`). Only the publish, backup, commit and maintenance templates write. None of them supports `fixture`.
