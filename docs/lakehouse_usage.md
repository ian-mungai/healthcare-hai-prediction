---
title: Lakehouse usage
description: Commands and checks for the launcher, bronze, staging, gold, silver validation, the run lock and the fixture kit.
last_updated: 2026-10-22
---

# Lakehouse usage

## Main commands

Every container starts through the launcher (`orchestration/launcher.py`), the only process allowed to call Docker. A real
dbt build runs only with its provenance recorded; a complete successful build is sealed into a read-only copy that the
gold, quality and query readers check before they read it. The main lakehouse commands:

```sh
.venv/bin/python -m orchestration.launcher status                  # the launcher; --what registry or catalog
.venv/bin/python -m scripts.lakehouse.catalog up                   # start the catalog (from the main checkout only)
scripts/lakehouse/query.sh                                         # DuckDB shell with bronze attached read-only
.venv/bin/python -m scripts.lakehouse.catalog job bronze -- --group hai   # load a bronze group; report: data/e2e/bronze_load/
.venv/bin/python -m scripts.lakehouse.run_silver_e2e               # silver and gold quality checks on the fixture; report: data/e2e/silver_quality/
.venv/bin/python -m scripts.lakehouse.run_gold_e2e                 # gold on the fixture; report: data/e2e/gold/
.venv/bin/python -m scripts.lakehouse.run_launcher_e2e             # launcher scenarios with a fake Docker; report: data/e2e/launcher/
.venv/bin/python -m scripts.orchestration.trigger_dag --dag silver_transform_dag --mode real --wait-seconds 7200   # one workflow run on the running stack
```

`trigger_dag` starts one silver, gold or maintenance workflow run on the stack that `scripts.orchestration.supervisor up` holds
and waits for its end within `--wait-seconds` (at most 4 hours). Workflows start paused, so it unpauses the selected one first.
One deadline covers the whole command: every request and every sleep stops at it. A run that ends after it is reported as
not ended (exit 1). A gold run names the committed silver run it reads with `--silver-snapshot-run` (default `latest`). It reads
the session's Airflow token secret from Secrets Manager into memory and prints one line: the run ID, the end state, the seconds
and the task states. It exits 0 for a successful run. An error names the workflow, the run ID and the HTTP status or the cause,
never a response body. The native check is the `trigger_command` case of `scripts.orchestration.run_transform_e2e`; its offline
refusal and deadline checks run with `--trigger-check`.

## Secrets

No secret is saved in a file on the host. The catalog database password, the root client secret and each principal's client
ID and secret live in AWS Systems Manager Parameter Store as SecureString parameters under `/healthcare_hai_prediction/` (the names are in `docs/install_details.md`). The
catalog commands and the writer principal setup generate or store them there, the launcher reads them into memory when it
creates a container and sets them in that container's environment. The workflow supervisor stores its session secrets
the same way. Reading or storing a secret needs the IAM grant of the project profile; a missing grant fails and names the secret.

The launcher fetches the secrets of a container in a worker, never on its main loop. One deadline of 30 s covers the client setup,
every read and every retry (5 s to connect, 10 s per read, 2 requests in all). A fetch that does not finish fails the attempt and
names the secret.

The launcher saves a container's output only after it replaces every credential the container holds with `[redacted]`. It takes the
credentials from the container's own configuration, so this also holds after a launcher restart. The launcher also keeps no text that the
Docker daemon returns with an error, because that text can quote a credential. A record holds the HTTP status and a fixed reason, such as
`Docker 400: request refused`.

A secret that exists with no value is refused until you fill it by hand. A secret that does not exist is created once. A caller that
loses a create race reads the winner's value. A stored principal pair is kept only when Polaris accepts it. Otherwise the next run
resets the principal and stores a new pair, one run at a time.

## Writer principals

Each publish layer writes the catalog as its own principal. Run each set-up once with the catalog up, before that layer's first real
publish. `--dry-run` prints the requests and sends none. A rerun changes nothing and keeps the stored credentials.

```sh
.venv/bin/python -m scripts.lakehouse.gold_writer_setup --dry-run     # plan of the gold_writer principal (gold and gold_e2e)
.venv/bin/python -m scripts.lakehouse.gold_writer_setup               # create it; credentials go to AWS Parameter Store
.venv/bin/python -m scripts.lakehouse.silver_writer_setup --dry-run   # plan of the silver_writer principal (silver and silver_e2e)
.venv/bin/python -m scripts.lakehouse.silver_writer_setup             # create it; credentials go to AWS Parameter Store
.venv/bin/python -m scripts.lakehouse.run_gold_publish_e2e --mode writer_setup   # both set-ups against a stand-in Polaris; no Docker
```

A writer may create, read and write the tables of its own two namespaces and read the catalog's properties. It cannot touch the other
layer's namespaces or bronze, manage the catalog, create or drop a namespace or drop a table. The root credentials make the set-up
requests only. Each writer's client ID and secret are stored in AWS Parameter Store (see Secrets) and never printed. A job receives them
only from the launcher (`POLARIS_GOLD_WRITER_CLIENT_ID` and `_SECRET`, `POLARIS_SILVER_WRITER_CLIENT_ID` and `_SECRET`). The shared
requests and the Secrets Manager steps are in `scripts/lakehouse/polaris_writer.py`.

## Pipeline AWS access

A pipeline job whose class needs AWS (`lakehouse_read` or `lakehouse_write`) gets a role session, not a key. When the launcher renders the job's
container, a worker asks AWS Security Token Service (STS) for a session of one role through the project's AWS command wrapper and the project
profile (`orchestration/pipeline_session.py`): the lakehouse reader role for a read job and the lakehouse writer role for a write job. The session
lasts until the job's deadline plus 300 s. The roles give at most 7200 s, so the launcher refuses a pipeline job with an AWS class at submit when its
timeout plus 300 s is over 7200 s. The request has the same 30 s deadline as a secret fetch and fails the attempt with the role and the error code.

The AWS command saves the answers of its calls in `~/.aws/cli/history/history.db` when the profile sets `cli_history` to `enabled`. The session
answer holds all three session values. Before the request, the worker asks the same AWS command for that setting of the project profile with the
same environment. It makes no request unless the setting is `disabled` or absent. A profile with history on fails the attempt with a message that
names the profile. Run `aws configure set cli_history disabled --profile <profile>` and submit again. The check and the request share the 30 s deadline.

The launcher passes `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN` and `AWS_REGION` to the container as environment values.
No key or AWS file is stored on the host or mounted into the container. The values are hidden in saved output and kept out of the operation record
like every other credential. Each job asks for a new session, so a retry never reuses an earlier one. A session that no longer reaches the job's
deadline is refused. Fixture runs and jobs with no AWS access get no session. A silver build in a real run reads S3 through the dbt target
`lakehouse_session`, which takes its S3 access from these environment values.

Terraform in `infra/iam/` defines the two roles. Only the project's IAM user can assume them. A new policy of that user allows
`sts:AssumeRole` on those two roles and no other. The reader reads `lakehouse/bronze/`, `lakehouse/silver/` and `lakehouse/gold/`. The writer
writes `lakehouse/silver/`, `lakehouse/silver_e2e/`, `lakehouse/gold/`, `lakehouse/gold_e2e/` and `lakehouse/catalog_backups/` with the object
actions the lakehouse policy already allows. The role names are the variables `lakehouse_reader_role_name` and `lakehouse_writer_role_name`. The
longest session is the variable `lakehouse_session_max_seconds`, 7200 by default. Both roles must exist (apply `infra/iam/`) before a real pipeline job
with an AWS class can start.

## Launcher

`orchestration/launcher.py` is the only process that starts containers; the Docker-call check
(`.venv/bin/python -m scripts.quality.docker_calls`) fails on a Docker call anywhere else, except the reviewed sites in
`config/quality/docker_calls_reviewed.json`. It starts on demand, holds the registry lock for its whole life and serves
its clients over a local socket. Every job is an attempt of a fixed template (`dbt_deps`, `real_dbt`, `fixture_dbt`,
`fixture_init`, `duckdb_helper`, `quality_validate`, `query_reader`, `ui_reader`, `spark_read`, `spark_write`,
`spark_e2e`) with a closed argument schema, a memory plan and a deadline entrypoint inside the image. `dbt.sh`,
`query.sh`, `ui.sh`, the catalog script and the fixture kit are its clients.

The registry keeps live state and the last 24 hours of terminal records. Every 10 minutes the launcher moves older
records into the append-only archive `history/<UTC date>.jsonl` beside `registry.json` (owner-only): ended attempts of
released or failed allocations with their settled operations and container records, done requests and reservation
events. A transaction then no longer rewrites the whole history. A record leaves the registry only after its archive
line is complete on disk: the launcher writes every byte, syncs the file and its folders, reads the lines back and cuts an
incomplete last line left by a crash. A failed append keeps the records in the registry and the next pass tries again; a
crash can repeat a line but never lose one. An allocation that an attempt in the registry, a registration, a reader or a
reservation still names stays. A test registry's retention must exceed the client's retry patience, so a retried request
always finds its first result.

The launcher also watches the file's size. On each maintenance tick (every 0.5 s) it reads `registry.json` once and takes
the size and SHA-256 of those bytes. Above 1,000,000 bytes (`compact_threshold`) it moves finished records older than 1 hour
(`compact_floor`) under the same selection and archive rules, instead of waiting for the 24-hour retention. The hour is 20
times the client's 180 s `poll_patience`, so a retried request still finds its first result. The move is tried once per
change of the file. The periodic pass (every `compact_every`, 600 s) chooses the age by the current size, so records
that become old with no write still move. Under the threshold nothing younger than 24 hours moves. A test registry may
lower the threshold and the floor. The floor must stay above `poll_patience`. A threshold or floor of zero or less is
refused at start. The maintenance tick parses the registry only when the file changed, so an idle launcher with a large
registry no longer parses it every half second. A change is a different size or a different SHA-256 of the content, so
every outside write counts, including an edit in place that keeps the size and restores the modification time. A tick that
sees a different signature parses those same bytes and no others, so the cached open operations and their signature
always describe the same content. Before caching or acting on those bytes, the launcher applies the transaction's
schema and shape checks and verifies the registry ID against the ID it holds. A foreign or malformed snapshot raises
a registry error before any orphan stop or removal. The disposal worker also inspects the container and requires
its `hai.registry_id` label to match before stopping or removing it. A replacement that lands while a tick runs is read on the next tick. So is one that
restores earlier content after the cache took a later state. A file that is not valid JSON is refused with a registry
error and its signature is not stored, so the next tick reads it again. The read and the hash cost about 0.4 ms for a 1 MB
file and 3 ms for an 8 MB file (one machine, warm cache). The time-based work (create deadlines, orphan retries and the
periodic pass) runs on every tick with no parse. The state machine pass (every `tick`,
0.2 s) and the session timers still read the registry on each tick.

A client that waits for the launcher (`run_attempt`) tells a gone launcher from a slow one and from a stuck one:

- **Gone:** the socket is missing or refuses connections for `gone_after` (1 s). The client fails at once with code
  `unavailable` and the message "the launcher is not running".
- **Alive:** after a timed-out exchange the client sends a `live` request, which the launcher answers without reading its
  registry. An answer means the loop is alive. The client repeats the same request ID with a longer budget (1 s, doubled
  after each timeout up to 4 s, reset by a quick exchange) and waits up to `poll_patience` (180 s). A request that stays
  stuck while the loop keeps answering falls back to that `poll_patience`.
- **Not answering:** no `live` answer for `stuck_after` (30 s). The client fails with code `unresponsive`.
- **Interrupted pool:** once a pool is cancelling, none of this applies. The bounded unresolved path and `cancel_settle`
  decide, as before.
- **Slow exchange:** an exchange that takes more than `slow_exchange` (0.5 s) beyond the wait it asked for writes one line,
  `launcher exchange <method> took <seconds> s` (or `timed out after`), to the `launcher.client` logger, which prints on
  standard error unless the program configures logging. The liveness probe is timed and logged the same way, as method
  `live` with the outcome `took`, `got no reply after` or `found no listener after`. A line holds the method, the outcome,
  the seconds and the requested wait, never a request ID, a principal or a payload.

The single loop answers `live` between its steps, not during a registry write. Liveness therefore means that the loop
returns to the socket within `stuck_after`. It does not show that the outstanding request progresses: a request that the
loop never finishes while it keeps answering `live` is not detected before `poll_patience`.

After `unavailable` or `unresponsive` the client's cleanup never starts a launcher and has `cleanup_budget` (20 s) in all for each
principal (an allocation or a run), counted from that principal's first cleanup step. The record of the lost connection and
its deadline belong to that principal. A successful exchange of another worker on the same client does not clear them or
move the deadline, even when that worker uses the same allocation, because the failed worker's ambiguous submit may still
have created an attempt or its accepted attempt may still be open. A terminal wait failure records its accepted attempt's
cleanup obligation together with the failure, before the exception propagates. The sibling's clearing decision uses the
same lock interval, so it cannot observe the failure without that obligation. The record ends when the principal's own work is settled. A failure of an exchange that names no
principal ends with the next successful exchange.

`release_allocation` replays each ambiguous submit of the allocation under its own request ID: a refusal by the launcher
drops it, a reply names the attempt. It cancels the open attempts, waits for them to end and then makes one bounded release
request. It returns a `ReleaseOutcome`:

- **`released`:** the launcher confirmed the release.
- **`retained`:** the work of the allocation was not settled within the budget, so no release was requested. One
  `launcher.client` line names the allocation and the launcher releases it once its holder process is gone.
- **`unresolved`:** the work was settled and the release request got no answer in time, so the client does not know
  whether the allocation is released. One `launcher.client` line names it.

Without a lost connection the call is an ordinary one and a refusal raises `ClientError`. After a lost connection it never
raises, so the original failure reaches the caller: a release or deregistration that fails after such a failure is logged
and does not replace it. `unavailable` and `unresponsive` both end `launcher run` and `launcher dbt` with exit code 5
(degraded), not 3 (refused). The catalog and silver scripts, the package fixture and the staging runner report that the
launcher did not answer, with the code, instead of that it refused. A caller that reports cleanup reads the outcome: the
silver runner's cleanup summary holds `allocation_release` (the outcome) beside `allocation_released` (true only for
`released`). A retained or unresolved release makes its `ok` false.

`scripts/lakehouse/query.sh -c "select 1" -csv` runs one query and needs no terminal; every word after `query` goes to
DuckDB unchanged. `query --help` prints the supported arguments (`-c SQL` and the output flags) and exits 0 without
starting the catalog or taking an allocation. A file or pipe on standard input is sent in chunks of 16 KiB while the
shell's output is printed, so a large file neither fills memory nor stops the output. `.venv/bin/python -m
orchestration.launcher run --template query_reader --param tty=false -- -csv -c "select 1"` starts the catalog its
template needs and takes and returns the reader registration.

A real build (`real_dbt`) gets its provenance from `scripts/lakehouse/provenance.py`: the run ID, the SHA-256 of the dbt
project and package snapshot and the pinned bronze snapshot set. The launch is refused unless every value is recorded.
Launches that may write the build file run one at a time in submit order. A hook drops the build marker before each
mutating launch. After a complete build whose results are all `success` or `pass`, the end hook writes the marker and
the launcher seals the build: a read-only copy `sealed/<sha256>.duckdb` in the run folder, recorded with its digest in the
run's completion. A build with any warning is not sealed. The release-copy check fails the build unless each selected
file has exactly one live physical copy and one valid canonical copy. After sealing, the
run refuses every further mutating launch. Each reader (gold, quality, query) attaches the sealed copy and checks its
digest inside its container before reading. A graceful launcher stop waits for a running seal to finish. The sealed copy
is a full byte copy, so it needs free disk equal to the build file. `scripts.lakehouse.run_provenance_e2e` covers these
rules with a fake Docker and a test registry.

```sh
.venv/bin/python -m orchestration.launcher status                # the launcher; --what registry, catalog or protected_sets
.venv/bin/python -m orchestration.launcher plan                  # the memory plan the launcher computes now
.venv/bin/python -m orchestration.launcher shutdown --owner      # stop the launcher
.venv/bin/python -m scripts.lakehouse.run_launcher_e2e           # launcher scenarios with a fake Docker; report: data/e2e/launcher/
```

## Bronze

The lakehouse jobs run in the Spark container. The bronze E2E uses a throwaway local catalog and synthetic files, never
S3. Discovery reads the S3 manifests, the committed table map (`config/lakehouse/bronze_tables.json`) and the previous
complete metadata snapshot. The first bootstrap selects the smallest logical key for each file. Later loads retain
that canonical lineage, list every observation in `bronze.stored_copies`, replace each loaded object in its own commit,
prune objects a table no longer selects, rebuild the table's data dictionary and write a counts-only report.
The initial `--bootstrap-all` path requires coverage of the full map and publishes the copies ledger and one complete
metadata generation. It verifies the existing data without loading, pruning or rebuilding dictionaries.
A corrected manifest replaces the manifest it names as superseded. Tables taken out of the map are listed in
`config/lakehouse/removed_tables.json` and dropped with `--drop-removed`:

```sh
.venv/bin/python -m scripts.lakehouse.catalog job bronze_e2e        # report: data/e2e/bronze/
.venv/bin/python -m scripts.lakehouse.catalog job bronze -- --bootstrap-all    # full-map metadata bootstrap, no data reload
.venv/bin/python -m scripts.lakehouse.catalog job bronze -- --group hai           # or --tables a,b; report: data/e2e/bronze_load/
.venv/bin/python -m scripts.lakehouse.catalog job bronze -- --tables a,b --copies-only   # list copies, prune, verify
.venv/bin/python -m scripts.lakehouse.catalog job bronze -- --drop-removed       # drop tables in removed_tables.json
.venv/bin/python -m scripts.lakehouse.catalog job dictionary -- --tables a,b      # rebuild bronze_dictionary tables
.venv/bin/python -m scripts.lakehouse.care_compare_tables           # regenerate the care_compare group (read-only S3)
.venv/bin/python -m scripts.lakehouse.retired_objects --collection <publisher/collection>   # rebuild the retired list
scripts/lakehouse/query.sh                                          # DuckDB shell, bronze attached read-only
scripts/lakehouse/ui.sh                                             # DuckDB UI at http://localhost:4213
```

Bronze loads one copy of every stored data file as published, every value as text:

- CSV files keep their columns.
- Text files keep one row per line.
- Excel files keep one row per sheet row.
- PDFs and Word documents keep their table rows and text.

Every row carries its source, snapshot, S3 key, version and SHA-256. `bronze.stored_copies` lists every stored copy of
each file with its lineage and whether it is loaded or retired. Each table's `bronze_dictionary` entry lists its
columns with counts and the publisher's description or published type where a stored dictionary gives one. CMS Care
Compare dictionaries give types only. Bronze does not load:

- objects removed from S3, which the manifests still list (`config/lakehouse/retired_objects.json`, keys, versions and
  checksums only): the privacy deletion and 2,193 byte-identical duplicate versions, each with a kept
  twin verified first;
- other copies of a file already loaded, which `bronze.stored_copies` lists;
- a Care Compare contacts file that names state staff;
- audit copies, receipts and Office package parts.

Two HRSA detail files, stored under the dictionary role, have corrected manifests in S3 that give them the data role
(`config/acquisition/manifest_corrections.json`, written by `scripts.acquisition.correct_manifest_roles`); the original
manifests stay unchanged. Open gaps are tracked in the local issue register.

## Staging, gold and pins

The staging models and their checks are described in [staging_models.md](staging_models.md).

The dbt packages
(dbt-project-evaluator 1.4.0 and its dbt_utils 1.4.1) are declared in `dbt/packages.yml` and pinned by version in `dbt/package-lock.yml`; dbt Hub
publishes no content checksum. They install outside the read-only project: `scripts/lakehouse/dbt.sh deps` installs them in the
container. The staging E2E installs them before it builds. dbt-project-evaluator is off in every other run and runs
only on request, on the in-memory `lint` target with no catalog; its findings are warnings:

```sh
.venv/bin/python -m scripts.lakehouse.run_staging_e2e              # fixtures; --real adds the real tables; report: data/e2e/staging/
scripts/lakehouse/dbt.sh deps                                       # install the locked dbt packages (once)
scripts/lakehouse/dbt.sh build                                      # dbt on the real bronze tables
.venv/bin/python -m scripts.lakehouse.run_project_evaluator         # dbt-project-evaluator; report: data/e2e/dbt_evaluator/
.venv/bin/python -m scripts.lakehouse.ipps_file_labels --check      # seeds match the S3 manifests (read-only S3)
.venv/bin/python -m scripts.lakehouse.pos_file_periods --check      # POS periods match the manifests and job plans
.venv/bin/python -m scripts.lakehouse.ownership_release_periods --check   # ownership periods match the manifests and receipts
.venv/bin/python -m scripts.lakehouse.geography_file_periods --check      # geography periods match the manifests and records
.venv/bin/python -m scripts.lakehouse.mmd_conditions --check             # MMD units seed and bridge match the pinned plans
.venv/bin/python -m scripts.lakehouse.run_mmd_conditions_e2e           # the check catches drift; report: data/e2e/mmd_conditions/
.venv/bin/python -m scripts.lakehouse.bridge_references --check          # bridges and recipes match the gold contract and the registry
.venv/bin/python -m scripts.lakehouse.run_gold_e2e                     # gold on fixtures; --real adds two pinned real builds; report: data/e2e/gold/
.venv/bin/python -m scripts.lakehouse.gold_checks check-suites          # the gold suites match the gold contract
.venv/bin/python -m scripts.lakehouse.catalog job pins -- pin --run <run_id>      # pin bronze in data/analytics/dbt/runs/<run_id>/pins.json
.venv/bin/python -m scripts.lakehouse.catalog job pins -- verify --run <run_id>   # fails when a pinned table moved
.venv/bin/python -m scripts.lakehouse.catalog job pins_e2e                # the pin's scenarios on a throwaway catalog; report: data/e2e/pins/
```

Before it starts the catalog or a build, `run_staging_e2e` sends one ping to a running launcher and decides on that
exchange: no retry, no warm-up and no launcher start, so it creates no attempt, allocation or run. It stops with exit
code 2 when the ping takes more than 0.5 s, fails or has no answer within 5 s. The message gives the measured time and the
size of `registry.json`, then names compaction as one fix to try (the launcher moves finished records older than 1 hour
once the file passes 1 MB); it does not say the size caused the delay. A launcher that is not running is refused with its
own message and the command that starts it. The check changes no state.

**Gold checks.** `scripts/lakehouse/run_gold_e2e.py` builds silver and gold on the staging fixtures and, with `--real`, twice
from the real bronze tables, pinning the bronze snapshots around each build. It reads the fact list, grains, columns,
surrogate keys and source dates from `data_contracts/lakehouse/gold_contract.json` and checks that each fact keeps its
silver model's rows and values, that every key resolves to its dimension or the unknown member, that each dimension
holds its unknown member once, that gold reads only its declared inputs and that two real builds give identical gold
fingerprints. Every fact needs rows in the fixture stage; in the real stage every fact except the hold facts needs rows.
A hold fact may be empty when nothing is held; at least one non-empty hold fact must keep its rows. Every expression
finding on the compiled gold SQL (`scripts/lakehouse/gold_sql.py`) counts toward the verdict. An analysis that could not
visit every relation a model reads fails the run. The check's own corpus E2E is
`.venv/bin/python -m scripts.lakehouse.run_gold_sql_e2e --out <report.json>` (`--g4` runs only the six allowlist entries that
name a model and the models that use them). The keys and the identity text of the split dimensions end with the edition
part (`edition_start`). The check accepts it in `dim_rurality_code`, `dim_measure` and `bridge_measure_control` only. Each
allowlist entry that names a model admits one plain expression over the relation and column that model reads, in the clause and
output column the entry names. The entries that rest on a model's whole query (the label bounds of `dim_rurality_code` and
`dim_measure`, the county-list bounds and the membership test of `dim_county`, the edition years of `bridge_measure_control`,
the state prefix of `dim_state`, the tract count and the county prefix of `bridge_county_planning_region`)
are granted only to the delivered query: its normalized parse must equal the fingerprint pinned in `PINS` in
`scripts/lakehouse/gold_sql.py`. A change to one of these six models changes its pin and its probes in the same commit.
A function or window call written with a schema or catalog (`main.left(...)`, `system.main.count_star()`) is a finding in every model.
The permissions are for the unqualified built-in names.
The gold Great Expectations suites in `data_contracts/great_expectations/gold/` are generated from the contract (`gold_checks write-suites`) and
`gold_checks check-suites` in local CI fails when they drift. The test `release_copy_consistency` fails a build unless
each selected file has exactly one live physical copy and one valid canonical copy. Release-label aliases remain in
the observation history and do not increase the physical-copy count.

**Silver validation (Great Expectations).** After a build, Great Expectations 1.23.2 checks the kept silver models and the
gold tables as a second gate independent of the dbt tests. `scripts/lakehouse/silver.py validate` runs
`scripts/lakehouse/silver_quality.py` in the `quality` image (`services/quality/`, no network, memory limit from the launch
plan) over the silver suites in `data_contracts/great_expectations/suites/` and the gold suites in
`data_contracts/great_expectations/gold/` in one run. It opens the build file read-only and stops when an `analytics-dbt`
container is running, when `int_silver_build_marker` is missing or when the marker's DuckDB version differs from the
reader's. The silver suites are generated from `data_contracts/lakehouse/silver_columns.json` and the baselines
(`scripts/lakehouse/run_silver_e2e.py --write-suites`; `--check-suites` in local CI fails when they drift):

- for every kept model, its column set and the type of each column; a row-count floor of one for every model except the
  hold models (a real build may hold nothing for a source) and the marker, which needs exactly one row;
- five-digit county codes and the self-link flag in the county adjacency edges;
- a row-count band for each kept model that has a real count and no documented grain change.

A table the build lacks fails the expectations that name it. The 102 checks that read the spine, alignment, outcome and
history tables are saved with the feature files in `features/great_expectations/` and do not run in silver validation; the
ownership-history assertion that every derived relationship comes from organization-owner rows stays there for the
history build.

Row-count bands come from `data_contracts/great_expectations/baselines.json`. The values under `proposed_values` only
warn until the owner moves them to `values`; the older approved values belong to the saved checks. No expectation
repeats a dbt test on the same column; the run fails if one does. Results keep counts only. Great Expectations keeps no
open connection for DuckDB, so every metric came back empty. The validator registers DuckDB with it as a single-connection
dialect, only for the pinned version.

```sh
.venv/bin/python -m scripts.lakehouse.silver validate              # the last build; record: data/e2e/silver_quality/<UTC>/run.json
.venv/bin/python -m scripts.lakehouse.run_silver_e2e               # fixture build of silver and gold, then one mutation per case through the quality image; report data/e2e/silver_quality/fixture_<run>/report.json
.venv/bin/python -m scripts.lakehouse.run_silver_e2e --check-suites # static check of the suites; no Docker
```

Load large groups in batches of about 20 to 40 tables: each job stops at its launcher template's time limit
(`default_seconds` in `orchestration/launcher.py`). A rerun of the same tables ends in the same state.

## Run lock, registry and fixture kit

**Run lock and registry.** `scripts/lakehouse/run_lock.py` keeps one lock per domain (`acquisition`, `transform`,
`training`, `serving`) for the whole machine, in `data/orchestration/registry/` of the main checkout. Every worktree
finds it through Git's common directory; there is no environment override. `acquire` prints the run's token once and
the registry keeps only its SHA-256; nested launchers join the run with `RUN_LOCK_TOKEN`. A run moves from `held` to
`finalizing` and `released`. A failed or stale run stays `failed_retained` until the owner releases it with its token
after its labelled containers are stopped; nothing is released automatically. An acquisition run cannot write a
prefix a transform run pinned. Readers reserve a publish run, then register its snapshot set, so maintenance can keep
it (`protected-sets`). Resource admission is the launcher's: a client takes an allocation, which admits one heavy launch at a time across
worktrees. A child process that carries `HAI_ALLOCATION` reuses it (see [Launcher](#launcher)). Each registered
service's unused limit is held back as a growth reserve.

**Package fixture kit.** `scripts/lakehouse/package_fixture.py` builds the synthetic fixture with the pinned analytics
image and no network, AWS profile or secret (`prepare`), fingerprints a build file read-only (`baseline`) and checks a
work package's patched project against its frozen selector: required tests with their ancestors, a recorded and
approved effective selection, the `beab859` baseline and expected-failure mutations (`check`). Its launches are launcher
attempts under one allocation. Package checks and silver and gold E2E fixture builds enforce the same unconditional
physical release-copy invariant. Every selected file must have exactly one live physical copy and one valid canonical copy.

The backing `bronze.file_metadata` table covers all 330 committed bronze tables. Its physical index covers every live
checksum across that map. A checksum with only retired versions or failed attempts has observation rows and no physical
index row. Observations retain manifest entries, receipt and transport references and retirement evidence.
Failed attempts remain in this audit ledger but do not contribute publication labels, publication-date lists or the
latest publication date in `stg_bronze__files` and `dim_release`. Successful retired observations still contribute them.
Published observations from superseded manifests retain their labels and the replacing manifest's exact key and version.
They cannot become canonical. A canonical transfer changes only the observation identity, to a validated replacement with
the same table, logical key, checksum and exact physical version. The physical index records both observation identities
and the superseding manifest for pinned readers. A missing eligible replacement fails closed with the table and key.

Single-copy acquisition is off by default. When enabled, publisher payloads in the `datasets` and `references`
categories use `bronze_objects/sha256/<sha256>`. Audit payloads and evidence-only captures use
`bronze_evidence/sha256/<sha256>`. Each prefix has its own conditional create and checksum index, so identical publisher
and audit bytes remain one physical object in each namespace. The immutable bootstrap format has separate `objects`
and `evidence_objects` maps; a reference cannot cross between them. Category determines routing. An unknown category
fails closed. Manifests retain their collection paths, including evidence-only manifests under `audit`.
Both content prefixes stay inactive until the owner applies the access rules and changes the setting (see
[Switching on single-copy storage](#switching-on-single-copy-storage)).

### Switching on single-copy storage

Two parts must be ready before `single_copy_enabled` is `true`: the access rules for the two content folders and a
starting index that names the objects already in the bucket. The setting stays `false` in Git. The owner changes it in
the working tree only.

**Part 1: access rules.** The change is in `infra/iam/policies/healthcare_hai_prediction_s3_policy.json`. It adds four statements for
`bronze_objects/sha256/` and `bronze_evidence/sha256/` in the project bucket. One reads current objects and exact
versions (`s3:GetObject` and `s3:GetObjectVersion`). One creates objects (`s3:PutObject`) only when the request carries
the `If-None-Match` header, so an existing key cannot be overwritten. It allows only a leaf of exactly 64 characters.
Deletion, tagging and multipart actions are not granted. One denies writes, tagging and multipart actions in these two
folders for every key shape that the older wildcard grants match, such as `bronze_objects/sha256/<hash>/references/x`.
One denies `s3:PutObject` for every key under `sha256/` that holds a percent sign. IAM resource patterns have only `*`
and `?`, so the policy cannot limit the 64 characters to hexadecimal digits. The length-only pattern is the strongest
form that IAM can express. The only writer is the store step, which computes the leaf as the SHA-256 of the file. The
index loader refuses any other key. The policy evaluator cases (`scripts/infrastructure/policy_cases.py`, run by
`scripts.infrastructure.run_policy_e2e`) fail when a statement is missing, reaches past these two folders, has no
condition or leaves an empty, short, long, percent or nested key writable.

1. Read the change: `git diff main -- infra/iam`
2. Plan with the administrator profile; the plan changes nothing:
   `.venv/bin/python -m infra.iam.scripts.manage_policies plan --admin-profile <ADMIN_PROFILE> --policy s3`
3. Read the plan. When the policy set of the README (Install) is already applied, it must list only `UPDATE POLICY` for `healthcare_hai_prediction_s3_policy` and end with
   `Summary: 1 change(s), 0 unchanged.` Stop on any other line.
4. Apply it, with the confirmation word on the same command line:
   `CONFIRM_IAM_POLICIES=apply-healthcare-hai-prediction-policies .venv/bin/python -m infra.iam.scripts.manage_policies apply --admin-profile <ADMIN_PROFILE> --policy s3`
5. Plan again with the command from step 2. It must end with `Summary: 0 change(s), 1 unchanged.`

The create-once condition (`Null` on `s3:if-none-match`) follows the form that AWS documents for conditional writes. No
test against real AWS covers it. If the first single-copy write returns `AccessDenied`, stop and review the condition
before any other change.

**Part 2: starting index.** Run these steps from the repository root. The builder checks the project identity and lists
every object version of the bucket with read-only requests under the project profile from `.env`. It writes one new
local file and nothing to S3.

1. Build the index: `.venv/bin/python -m scripts.acquisition.single_copy_index --output data/single_copy/index_<DATE>.json`
   One deadline (`--deadline`, 1800 seconds) covers the identity check and all listing requests. The local download
   records in `data/datasets` are the cross-check: pass `--records <FOLDER>` when they are elsewhere. It finds the
   privacy, duplicate-removal and license-removal retirement records by default. Pass `--retirements <FILE>...` to name
   them yourself (the list replaces the default). Pass `--dispositions <FILE>` for acquired captures that have no storage
   record on purpose. The file is JSON with `"kind": "unstored_capture_dispositions"` and a `captures` list. Each line
   has the capture `path` (relative to the records folder), the `receipt_sha256` (the SHA-256 of that capture's
   `receipt.json`) and a `reason`.
   The upload records under `--evidence-root` (default `data`) also name objects, by exact bucket, key and version. Pass
   `--empty-capture-dispositions <FILE>` when that folder holds reviewed empty publisher captures, as for
   `verify_storage_records`. Saved bucket listings are not names. A recorded size must be a whole number equal to the
   listing size. The reader skips every single-copy index by its format, wherever the file is stored, so an earlier index
   is never a name. For each stored object that no record names, pass `--object-dispositions <FILE>`. The
   file is JSON with `"kind": "unnamed_object_dispositions"` and an `objects` list. Each line has the `key`, `version_id`,
   `byte_count` and `sha256` of the stored object and a `reason`. The builder refuses a line that matches no stored
   object, a second line for one object, a line without a reason and a line for an object that a record already names.
   When identical bytes sit under more than one key, the index names the copy stored first and the report lists every
   other copy under `aliases`. Read that list before you switch the setting on.
2. If the builder refuses, it writes nothing and lists each reason with a count and examples. The reasons and their
   fixes are in [Single-Copy Starting Index](acquisition_design.md#single-copy-starting-index). Fix the cause and build
   again into a new file name.
3. Switch the setting on with the command the builder printed. It writes `config/acquisition/storage_policy.json` with
   the index path and its SHA-256.
4. Check the policy loads:
   `.venv/bin/python -c "from scripts.acquisition.single_copy import load_policy; p = load_policy(); print(p['enabled'], len(p['objects']), len(p['evidence_objects']))"`

Do not commit the changed policy file. The index is a local file, so a clone or a CI run cannot read it. To switch the
setting off again, run `git checkout -- config/acquisition/storage_policy.json`.

`stg_bronze__release_observations` covers the 70 analytical tables in the `copy_tables` family map, including aliases
and retired observations. Adding a table to that map includes its observations. Readers needing the full set use
the source-layer `bronze.file_metadata` relation with `kind = 'observation'` through the pinned metadata source,
or filter the unfiltered `stg_bronze__metadata` model by that kind.

```sh
.venv/bin/python -m scripts.lakehouse.run_lock status       # holders, runs and readers; never a token
.venv/bin/python -m scripts.orchestration.run_contracts_e2e # orchestration contracts; report: data/e2e/orchestration_contracts/
.venv/bin/python -m scripts.lakehouse.package_fixture prepare  # credential-free fixture build in data/package_fixture/
.venv/bin/python -m scripts.lakehouse.run_package_fixture_e2e # the kit's checks, with Docker; report: data/e2e/package_fixture/
.venv/bin/python -m scripts.lakehouse.run_lock_e2e          # CLI scenarios in test registries; report: data/e2e/run_lock/
```
