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
- **Fixture mode:** the run directory is also the dbt case folder, mounted at `/workspace/out/case`. The `acquire_run`
  step copies the seed folder `data/orchestration/fixture_seed` into it once (built by
  `.venv/bin/python -m scripts.orchestration.transform_fixtures`) and dbt's `fixture` target writes the build file
  `staging.duckdb` there. The release step deletes `staging.duckdb` and `staging.duckdb.wal` after an accepted run.

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

## Run control rows

A broker template row can name an action the broker performs on the launcher's run methods instead of forwarding a
container. The request is the same four fields as for any other template. The actions are idempotent, so a repeated
request or a task retry ends in the same state. The rows exist for mode `fixture` only.

| Template | Action | What it does |
| --- | --- | --- |
| `acquire_run` | `acquire` | Takes the `transform` lock (a refusal ends the submission with exit code 125), makes the run directory and copies the seed |
| `run_record` | `record` | Writes the run record and moves the run to `finalizing` |
| `final_gate_accepted` | `gate_accepted` | Writes an accepted result into the run record, only for a finalizing run. When a predecessor task did not end as expected, it writes a rejected result instead and refuses |
| `final_gate_rejected` | `gate_rejected` | Writes a rejected result with the failed submissions and the predecessor task states, also when no lock was taken |
| `release_accepted` | `release_accepted` | After an accepted result: stops open attempts, deletes the build files, releases the lock |
| `release_rejected` | `release_rejected` | Stops open attempts, keeps the build files, moves a held run to `failed_retained` |

The owner's release remains the only way out of `failed_retained`.

The gate rows record the Airflow DAG run's task states in `gate.task_states`. The supervisor reads them once from the
Airflow API in the one DAG run of `transform_dag` whose configuration names the run and mode. It reads every page of the
DAG run listing and every task instance of the run. A listing that cannot be shown to be complete counts as a listing
that cannot be read. So does a read that does not finish within 60 seconds in total. It hands them to the
broker, which holds no Airflow credential. For each predecessor task the record has its name, its expected state (the
row's `expected_states`) and its observed state. `failed_tasks` names the tasks whose states differ. A DAG run that
cannot be read or that is not the only one for the run and mode is recorded as `unavailable` with its reason and no
task list. A rejection stands either way. After a DAG run is marked failed or cancelled by hand, the supervisor leaves
a finalizing run for its release task only when the record holds an accepted result and the `final_gate` task of that
DAG run succeeded.

## Build file checkpoint

A launcher row can name the build file it validates (`records_build_file`), the result file of the validator
(`build_file_result`) and the step whose record a later row requires (`requires_build_file`). The broker writes that
checkpoint into the intent line when it accepts the submission. Recovery follows the intent, not the present inventory.

The validator binds the SHA-256 to the bytes it reads. Inside its container it copies the build file to a private
temporary copy, hashes the copy and runs every suite on that copy. It reports the SHA-256 as `build_file_sha256` in its
result file. In mode `fixture`, `silver_validate` ends with exit code 1 and records nothing when that result is
missing, unreadable or holds no SHA-256. Otherwise the broker checks that the file on the host has the reported
SHA-256 once the job has exited. A file that differs ends the step with exit code 1 and records nothing, also when it
changed during the validation and returned to its earlier bytes. A file that matches is recorded in the `finished`
line as the validated identity: the file, the reported SHA-256, the run and the launcher attempt.

For a step that needs the validated file, the intent holds the file, the validating step and the SHA-256 that step
recorded. `gold_build` compares the file with that SHA-256 before it is sent to the launcher. A missing record or a
different SHA-256 ends the submission with exit code 125 and no container starts. `cross_layer_checks` runs
`quality_reconcile`: the silver and gold suites plus the per-fact row and content reconciliation of every gold fact
with its silver model, on the same private copy.

## Modes and Classes

`fixture` runs offline on synthetic data; `pin_bronze` and `verify_bronze` are recorded no-ops in that mode.
`publish_fixture` writes only the `gold_e2e` namespace; `real` writes `gold`. Each template names the network and
secret class of each mode it supports (`offline`, `lakehouse_read`, `lakehouse_write`, `collector`, `training`,
`serving`). Only the publish, backup, commit and maintenance templates write. None of them supports `fixture`.
