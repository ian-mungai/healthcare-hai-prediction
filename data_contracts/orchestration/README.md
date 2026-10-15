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
- **Container name:** the launcher names every container `hai-<registry ID prefix>-<owner>-<launch ID>-a<attempt>`.
  For a broker submission the launch ID is `sub-` with 24 hexadecimal characters of the submission ID's SHA-256. The
  launcher request ID is `brk-submit-` with 40 of them, so a repeated submit returns the launcher's first answer
  instead of starting a duplicate. Containers carry the launcher's labels, among them `hai.registry_id`, `hai.launch`
  and `hai.attempt`. A container that still carries the earlier `hai.run_id` label blocks a registry migration until
  it is removed.

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
- **Order per submission:** `intent` (before the launcher is asked), then `started` once the launcher's attempt has a
  container, then `finished` when the attempt ended. `failed_closed` stops the submission for the owner when the broker
  template row changed since the intent was written or the created container differs from its specification. The
  ledger format still accepts `adopted`; the broker does not write it.
- **Recovery:** before any dispatch the broker reconciles every unfinished intent through the launcher's authenticated
  `submission` lookup by the request ID it sent. A found attempt is reconciled before the row can end the submission.
  Not found means the launcher never accepted it: the row decides, then the submission is sent once. A launcher that
  cannot answer leaves the submission pending: status and cancel answer 503 and nothing terminal is written. No
  absence on disk, such as a missing marker or folder, ever counts as "never sent".

## Modes and Classes

`fixture` runs offline on synthetic data; `pin_bronze` and `verify_bronze` are recorded no-ops in that mode.
`publish_fixture` writes only the `gold_e2e` namespace; `real` writes `gold`. Each template names the network and
secret class of each mode it supports (`offline`, `lakehouse_read`, `lakehouse_write`, `collector`, `training`,
`serving`). Only the publish, backup, commit and maintenance templates write. None of them supports `fixture`.
