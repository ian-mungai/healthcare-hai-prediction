"""Register the tables of a committed bronze load in the bronze Glue mirror database, the way the silver, gold and features publishes do.

``bronze.main`` calls ``mirror_after_load`` once, after the load and its metadata generation are committed. The switch, the time budget, the Glue
registration and the failure report are those of ``glue_mirror`` (``tables_to_mirror``, ``Budget``, ``mirror``, ``explain``); this module only names
what a bronze load mirrors. Only the real ``bronze`` namespace with its real ``bronze_dictionary`` namespace is mirrored: a qualification run (``bronze_e2e``
with ``bronze_dictionary_e2e``) and the other test namespaces mirror nothing, and nothing runs while ``enabled`` is false in
``config/lakehouse/athena_mirror.json``.

The generated data dictionaries (``bronze_dictionary.<table>``, one Iceberg table per bronze table, rebuilt by every load that loads data) are registered
right after the bronze tables, in the ``bronze_dictionary`` Glue database, by the same ``glue_mirror.mirror`` with the same marker, run ID, failure rule and
deadline: the hook starts one deadline of ``mirror_seconds`` and the dictionaries use what the tables left of it, so they never extend the hold on the load
and the tables are always registered first. Each dictionary of a loaded table is registered at its newest snapshot, read in the load's own Spark session
before the dictionaries are registered; a registration points at the metadata file of that snapshot, so a commit that lands later is not registered. Every
load registers every dictionary that exists, also a load that rebuilt none (an unchanged rerun), so a rerun repairs a registration that failed. A dictionary
that does not exist, has no snapshot, cannot be read or cannot be registered is named under ``bronze_dictionary.<table>`` and stops no other, whatever
error it raised. Every dictionary of a loaded table has exactly one outcome in the result, also when the deadline or an error stops the hook before the
dictionaries: those not reached are ``failed: not attempted`` with the cause. No read and no registration starts after the deadline: every call checks it as
its worker starts. A dictionary registration that fails is repaired by a rerun of the load, which registers every dictionary again.

The mirrored set is what the load committed: the snapshot of each table the load names (a table that the load left out is simply not mirrored) and the
snapshot of each loader side table that has one (``file_metadata``, ``stored_copies``, ``column_map`` and ``file_preambles``), read after the metadata
generation is published. A side table is absent (no table yet: not named), waiting (the table exists and no commit wrote a row to it, as
``column_map`` after a first load of headerless formats: reported as not mirrored yet, and it blocks nothing) or committed (mirrored).
``glue_mirror.bronze_table_set`` is the one function that names the set, and the by-hand ``register`` takes it from there too. A rerun of the load
registers whatever is missing or stale.

A table the load left out for lack of a source object (``--unsourced-tables``, named by the stage and checked by ``bronze.check_unsourced``) is left out
of the hook's registration by the rule ``glue_mirror register --layer bronze`` uses (``glue_mirror.omission_decision``, one function): it stays out only when
no silver model reads it (``config/lakehouse/bronze_silver_readers.json``, derived from the dbt sources because the Spark image has no dbt), it has no
metadata file in S3, no Glue entry and no source history (``bronze_source_history.source_evidence``, a delete-marked manifest included). A table that stays
out is reported ``left out``. A table that fails any condition is required: it is registered from its committed snapshot when the catalog has one, and
otherwise the mirror is ``failed`` and names the table and the reason (the other tables are still registered). A check that cannot be read fails the
mirror too. Nothing is read when the mirror is off, in a test namespace or when the load left nothing out. For the bronze tables the hook and the
register can differ only in the inputs they read. The register does not take the dictionaries: only the hook registers them.

Before the IAM grant exists, Glue refuses every call with AccessDeniedException. The mirror is then ``unavailable`` (a status of its own: the refusal is
named, nothing that was refused is registered and nothing is reported ok). A grant can be applied for the bronze tables and not yet for the dictionaries:
the tables are registered and the dictionaries are refused, so the mirror is ``unavailable`` while the tables are registered. When the attempt record is
refused with AccessDenied too and the run has no recorded attempt (``bronze_run.read_attempt``: a record that exists, or a history that cannot be read other
than as a 403 or 404, is history), no attempt fails its check phase, first run or retry. A run with any recorded attempt gets no exception: its retry fails
by name. The task role cannot list the attempt folder and reads a key only through the read grant, so a full withdrawal of both mirror grants after a
recorded ok answers 403 exactly as a grant never applied does: that retry exits 0 as unavailable while ``show`` still reads the old ok (the owner's profile
lists the folder), so the sign-off needs the latest task's result and loader report as well (its digest must be the last stored attempt's). Any other
failure is a failure. After the grant, a rerun registers the tables.

The load is already committed, so the hook never raises and never undoes it. The report (status, reason, one outcome per table) goes into the loader
report and standard error when it failed. ``check.json`` is the immutable receipt of the load and a retry runs the hook again, so the check phase
writes each attempt as its own create-once record ``lakehouse/bronze_runs/<run>/mirror_<NNN>.json``. The number is the order: it is the first free
number of a conditional create, so no clock and no digest decides which attempt is the last. The record carries the run, the number, the metadata
generation, the loader report's digest and its time (for display) and the outcome. ``acceptance`` joins the chain to the receipt: the first record
must be the receipt's own report, every record must carry the receipt's generation, the numbers must run from 001 without a gap and no report may
appear twice. Anything else is ``unavailable``, never a pass that another record lends. ``python -m scripts.lakehouse.bronze_mirror show --run <bronze
run> [--bucket NAME]`` prints the attempts and the verdict and exits 1 unless it is ok, unchanged or skipped.

Known limit (owner decision, Nov 18 2026): ``show`` reads only the records that exist. If the role loses its write access to the attempt records after
an earlier ``ok`` record (a policy change, or a task that stops between the hook and the record), the retry leaves no record and ``show`` still reads
that earlier ``ok``. The check phase of such a retry fails (exit 1, the key, report digest and generation are named), but nothing in S3 says so. A
mirror is therefore signed off only by ``python -m scripts.lakehouse.bronze_mirror_signoff --run <bronze run> --describe <describe-tasks output of the
run's latest task> --report <that task's loader report>``, which needs this verdict, a stopped task of the bronze job, for this run, with every container
at exit 0, and the task's loader report, whose SHA-256 must be the report digest of the last stored attempt (a retry that stored nothing has a report that
matches no attempt, so its sign-off is refused, also after both mirror grants were withdrawn in full). The owner pairs the task with its report by hand: the
launch-evidence join that would do it by itself is not built."""

from __future__ import annotations

import argparse
import contextlib
import json
import os
import re
import sys
import time
from collections.abc import Callable, Collection, Mapping, Sequence
from datetime import datetime
from typing import Any

from scripts.lakehouse import glue_mirror

REAL_NAMESPACE = "bronze"
# The run that a registration names when the loader was started by hand, with no bronze run ID.
HAND_RUN = "bronze_by_hand"
MIRROR_STATUSES = ("ok", "unchanged", "failed", "skipped", "unavailable")
# The statuses that name a cause. ``unavailable``: Glue refused every failed call with AccessDeniedException, so the IAM grant is not applied yet.
REASON_STATUSES = ("failed", "skipped", "unavailable")
ACCESS_DENIED = "AccessDeniedException"
ATTEMPT_KEY = "lakehouse/bronze_runs/{run}/mirror_{sequence:03d}.json"
ATTEMPT_NAME = re.compile(r"mirror_(?P<sequence>[0-9]{3})\.json")
RECEIPT_KEY = "lakehouse/bronze_runs/{run}/check.json"
MAX_ATTEMPTS = 99
ATTEMPT_KEYS = {"schema", "bronze_run", "sequence", "metadata_generation", "report_sha256", "report_run_at_utc", "mirror"}
SHA256 = re.compile(r"[a-f0-9]{64}")
RUN_ID = re.compile(r"bronze_[a-f0-9]{8}(?:_[0-9]{1,2}){0,2}")
WAITING = "not mirrored: no committed snapshot yet"
DICTIONARY_PREFIX = f"{glue_mirror.DICTIONARY_LAYER}."
DICTIONARY_MISSING = "failed: no dictionary exists for it in the catalog"
NOT_ATTEMPTED = "failed: not attempted: {why}"
LEFT_OUT = "left out: the load left it out for lack of a source object, and its source collection holds no source object or observation history"


class AttemptError(ValueError):
    """A loader report cannot be bound to an attempt record, or a stored attempt record is not this run's own."""


def loader_snapshots(spark: Any, namespace: str, tables: Sequence[str]) -> tuple[dict[str, int], list[str]]:
    """The committed snapshot of each of ``tables`` that has one, and the tables that exist with no commit yet. A table not in the catalog is in neither."""
    from scripts.lakehouse import bronze

    found: dict[str, int] = {}
    waiting: list[str] = []
    for table in tables:
        if not spark.catalog.tableExists(f"{namespace}.{table}"):
            continue
        snapshot = bronze.latest_snapshot(spark, namespace, table)
        if snapshot is None:
            waiting.append(table)
        else:
            found[table] = snapshot
    return found, waiting


def required_outcome(why: str) -> str:
    """A table the load left out that the rule keeps required, with no snapshot to register: the mirror is failed and the outcome names the reason."""
    return f"failed: required ({why}), but the load committed no snapshot for it and the catalog holds none"


def mirror_after_load(
    spark: Any,
    namespace: str,
    names: Sequence[str],
    snapshots: Mapping[str, int],
    run_id: str | None = None,
    source: Any = None,
    glue: Any = None,
    config: Any = None,
    read_loader: Callable[[Any, str, Sequence[str]], tuple[Mapping[str, int], Sequence[str]]] | None = None,
    unsourced: Sequence[str] = (),
    history: Any = None,
    bucket: str = "",
    table_map: Mapping[str, Any] | None = None,
    s3: Any = None,
    readers: Collection[str] | None = None,
    dictionary_namespace: str | None = None,
    dictionary_source: Any = None,
) -> dict[str, Any]:
    """Register the committed snapshot of each loaded table of ``names``, and of each loader side table that exists, in the bronze Glue mirror database.

    Then the dictionary of each loaded table, in the ``bronze_dictionary`` database, when ``namespace`` is ``bronze`` and ``dictionary_namespace`` is
    ``bronze_dictionary`` (``dictionary_source`` is its Polaris side). ``read_loader`` reads their snapshots too.

    ``snapshots`` maps each loaded table to the snapshot that the load committed; a table of ``names`` without one is not mirrored. One absolute deadline
    starts here, before the first Spark read, and bounds every Spark and Glue call, so no step blocks the job past it and no write is issued after it.
    Never raises.
    """
    entered = time.monotonic()
    try:
        config = config or glue_mirror.load_config()
        mode = "real" if namespace == REAL_NAMESPACE else "qualify"
        loaded = {name: int(snapshots[name]) for name in names if name in snapshots}
        wanted, reason = glue_mirror.tables_to_mirror(config, namespace, mode, glue_mirror.bronze_table_set(loaded))
        if not wanted:
            return {"status": "skipped", "reason": reason, "tables": {}}
        budget = glue_mirror.Budget(config.mirror_seconds, started=entered)
        side, waiting = budget.run(
            "reading the snapshots of the loader tables", read_loader or loader_snapshots, spark, namespace, glue_mirror.bronze_loader_tables()
        )
        committed = {**loaded, **{name: int(snapshot) for name, snapshot in side.items()}}
        if source is None:
            from scripts.lakehouse.session import CATALOG

            source = glue_mirror.SparkSource(spark, CATALOG, namespace)
        if glue is None:
            glue = budget.run("creating the Glue client", glue_mirror.glue_client)
        extra: dict[str, str] = {}
        if unsourced:
            try:
                if table_map is None:
                    raise glue_mirror.MirrorError("the table map is not available to read the source history with")
                left_out, required = glue_mirror.omission_decision(
                    list(unsourced),
                    s3=s3 if s3 is not None else getattr(history, "client", None),
                    bucket=bucket,
                    glue=glue,
                    database=config.layers[REAL_NAMESPACE].database,
                    readers=glue_mirror.silver_readers_file() if readers is None else readers,
                    storage=history,
                    table_map=table_map,
                    budget=budget,
                )
                extra.update(dict.fromkeys(left_out, LEFT_OUT))
                if required:
                    found, _ = budget.run(
                        "reading the committed snapshots of the required tables", read_loader or loader_snapshots, spark, namespace, sorted(required)
                    )
                    committed.update({name: int(snapshot) for name, snapshot in found.items()})
                    extra.update({name: required_outcome(why) for name, why in required.items() if name not in found})
            except Exception as error:
                text = str(error) if isinstance(error, glue_mirror.MirrorError) else glue_mirror.explain(error)
                extra = {name: f"failed: whether it may be left out could not be decided: {text}" for name in unsourced}
        report = glue_mirror.mirror(config, namespace, mode, committed, source, glue, run_id or HAND_RUN, budget)
        registered = register_dictionaries(
            spark, config, dictionary_namespace, sorted(loaded), run_id or HAND_RUN, glue, read_loader or loader_snapshots, budget, dictionary_source
        )
        tables = {**report["tables"], **dict.fromkeys(waiting, WAITING), **extra, **{f"{DICTIONARY_PREFIX}{name}": text for name, text in registered.items()}}
        failures = {name: text for name, text in tables.items() if text.startswith("failed")}
        if failures:
            denied = all(ACCESS_DENIED in text for text in failures.values())
            report = {
                "status": "unavailable" if denied else "failed",
                "reason": "; ".join(f"{name} {text}" for name, text in failures.items()),
                "tables": tables,
            }
        else:
            moved = any(text in ("created", "updated") for text in registered.values())
            report = {**report, "status": "ok" if moved else report["status"], "tables": tables}
    except Exception as error:
        reason = glue_mirror.explain(error)
        unreached = []
        if namespace == REAL_NAMESPACE and dictionary_enabled(config, dictionary_namespace):
            unreached = [name for name in names if name in snapshots]
        report = {"status": "failed", "reason": reason, "tables": {f"{DICTIONARY_PREFIX}{name}": NOT_ATTEMPTED.format(why=reason) for name in unreached}}
    if report["status"] == "failed":
        with contextlib.suppress(OSError):
            sys.stderr.write(f"bronze load: the Glue mirror FAILED and the load stays committed: {report['reason']}. Rerun the load to retry the mirror.\n")
    elif report["status"] == "unavailable":
        with contextlib.suppress(OSError):
            sys.stderr.write(
                f"bronze load: the Glue mirror is UNAVAILABLE (Glue refused the calls: the IAM grant is not applied yet) and the load stays committed: "
                f"{report['reason']}. Rerun the load after the IAM apply to register the tables.\n"
            )
    return report


def dictionary_enabled(config: glue_mirror.MirrorConfig | None, dictionary_namespace: str | None) -> bool:
    """Whether the dictionaries are registered: the real dictionary namespace and a config that has the layer."""
    return config is not None and dictionary_namespace == glue_mirror.DICTIONARY_LAYER and glue_mirror.DICTIONARY_LAYER in config.layers


def register_dictionaries(
    spark: Any,
    config: glue_mirror.MirrorConfig,
    dictionary_namespace: str | None,
    names: Sequence[str],
    run_id: str,
    glue: Any,
    read_snapshots: Callable[[Any, str, Sequence[str]], tuple[Mapping[str, int], Sequence[str]]],
    budget: glue_mirror.Budget,
    source: Any = None,
) -> dict[str, str]:
    """The outcome of the dictionary of each of ``names``, registered in the ``bronze_dictionary`` database under the hook's own deadline (``budget``).

    Nothing for another dictionary namespace than the real one or a config without the layer. A snapshot is read one dictionary at a time, so one that fails
    to read fails alone. Never raises, and every one of ``names`` has exactly one outcome: whatever was not reached when an error or the deadline came is
    reported failed with its cause.
    """
    if dictionary_namespace is None or not dictionary_enabled(config, dictionary_namespace):
        return {}
    outcomes: dict[str, str] = {}

    def failed(error: Exception) -> str:
        return "failed: " + (str(error) if isinstance(error, glue_mirror.MirrorError) else glue_mirror.explain(error))

    committed: dict[str, int] = {}
    for name in names:
        try:
            found, waiting = budget.run(f"reading the snapshot of the dictionary of {name}", read_snapshots, spark, dictionary_namespace, [name])
        except Exception as error:
            outcomes[name] = failed(error)
            continue
        if name in found:
            committed[name] = int(found[name])
        else:
            outcomes[name] = WAITING if name in waiting else DICTIONARY_MISSING
    try:
        if committed:
            if source is None:
                from scripts.lakehouse.session import CATALOG

                source = glue_mirror.SparkSource(spark, CATALOG, dictionary_namespace)
            outcomes.update(glue_mirror.mirror(config, dictionary_namespace, "real", committed, source, glue, run_id, budget)["tables"])
    except Exception as error:
        outcomes.update({name: failed(error) for name in committed if name not in outcomes})
    return {name: outcomes.get(name) or NOT_ATTEMPTED.format(why="the registration returned no outcome for it") for name in names}


def outcome_shaped(outcome: object) -> bool:
    """The Glue mirror outcome of a loader report: a status, one outcome text per table and, for a failure or a skip, the reason."""
    if not isinstance(outcome, dict) or not {"status", "tables"} <= set(outcome) <= {"status", "tables", "reason"}:
        return False
    tables = outcome["tables"]
    if outcome["status"] not in MIRROR_STATUSES or not isinstance(tables, dict):
        return False
    if not all(isinstance(name, str) and isinstance(text, str) for name, text in tables.items()):
        return False
    return isinstance(outcome.get("reason"), str) if outcome["status"] in REASON_STATUSES else "reason" not in outcome


def attempt_key(run: str, sequence: int) -> str:
    return ATTEMPT_KEY.format(run=run, sequence=sequence)


def report_time(text: object) -> bool:
    """A loader report's time, for display: a real UTC date and time in the form of the report's file name."""
    try:
        return isinstance(text, str) and bool(datetime.strptime(text, "%Y%m%dT%H%M%SZ"))
    except ValueError:
        return False


def attempt_record(run: str, report: Mapping[str, Any], report_sha256: str, sequence: int) -> dict[str, Any]:
    """One mirror attempt as durable evidence: the outcome of the loader report, bound to the run, its number, the metadata generation and the report."""
    index, stamp = report.get("metadata_index"), report.get("run_at_utc")
    if not isinstance(index, dict) or not isinstance(index.get("generation"), str) or not index["generation"]:
        raise AttemptError("the loader report names no metadata generation to bind the mirror attempt to")
    if not report_time(stamp):
        raise AttemptError("the loader report has no valid run time to show the mirror attempt by")
    if not outcome_shaped(report.get("mirror")):
        raise AttemptError("the loader report holds no well-formed mirror outcome")
    return {
        "schema": 2,
        "bronze_run": run,
        "sequence": sequence,
        "metadata_generation": index["generation"],
        "report_sha256": report_sha256,
        "report_run_at_utc": stamp,
        "mirror": report["mirror"],
    }


def attempt_shaped(record: object, run: str, sequence: int) -> str | None:
    """Why a stored attempt record is not the run's own record of that number, or None."""
    if not isinstance(record, dict) or set(record) != ATTEMPT_KEYS or record["schema"] != 2:
        return "does not have exactly the keys of a schema 2 attempt record"
    if record["bronze_run"] != run or record["sequence"] != sequence or isinstance(record["sequence"], bool):
        return "belongs to another run or carries another number"
    if not isinstance(record["report_sha256"], str) or not SHA256.fullmatch(record["report_sha256"]):
        return "has no valid report digest"
    if not isinstance(record["metadata_generation"], str) or not record["metadata_generation"]:
        return "has no metadata generation"
    if not report_time(record["report_run_at_utc"]) or not outcome_shaped(record["mirror"]):
        return "has no valid report time or mirror outcome"
    return None


def attempts(client: Any, bucket: str, run: str) -> list[dict[str, Any]]:
    """Every attempt record of the run in number order. The numbers must run from 001 without a gap, every record is checked in full and no report may be
    recorded twice: anything else is refused, so an order is never guessed."""
    prefix = attempt_key(run, 0)[: -len("000.json")]
    numbers: dict[int, str] = {}
    token: str | None = None
    while True:
        page = client.list_objects_v2(Bucket=bucket, Prefix=prefix, **({"ContinuationToken": token} if token else {}))
        for item in page.get("Contents", []):
            name = ATTEMPT_NAME.fullmatch(item["Key"][len(prefix) - len("mirror_") :])
            if name is not None and item["Key"] == attempt_key(run, int(name["sequence"])):
                numbers[int(name["sequence"])] = item["Key"]
        token = page.get("NextContinuationToken") if page.get("IsTruncated") else None
        if token is None:
            break
    found: list[dict[str, Any]] = []
    for sequence in range(1, max(numbers, default=0) + 1):
        if sequence not in numbers:
            raise AttemptError(f"{attempt_key(run, sequence)} is missing but a later attempt record exists: the order cannot be established")
        record = json.loads(client.get_object(Bucket=bucket, Key=numbers[sequence])["Body"].read())
        problem = attempt_shaped(record, run, sequence)
        if problem is not None:
            raise AttemptError(f"{numbers[sequence]} {problem}")
        found.append(record)
    digests = [record["report_sha256"] for record in found]
    if len(set(digests)) != len(digests):
        raise AttemptError("the same report is recorded under two numbers: the order cannot be established")
    return found


def latest_attempt(client: Any, bucket: str, run: str) -> dict[str, Any] | None:
    found = attempts(client, bucket, run)
    return found[-1] if found else None


def read_receipt(client: Any, bucket: str, run: str) -> dict[str, Any] | None:
    """The immutable load receipt of the run (``check.json``), or None when it does not exist."""
    from botocore.exceptions import ClientError

    try:
        document = json.loads(client.get_object(Bucket=bucket, Key=RECEIPT_KEY.format(run=run))["Body"].read())
    except ClientError as error:
        if str(error.response.get("Error", {}).get("Code")) in ("NoSuchKey", "404"):
            return None
        raise
    return document if isinstance(document, dict) else None


def acceptance(client: Any, bucket: str, run: str) -> dict[str, Any]:
    """The state of the mirror of a run: the outcome of its last attempt, but only when the chain is joined to the load receipt.

    ``unavailable`` when there is no receipt (no load is recorded), no attempt record, the first record is not the receipt's own report (the first load's
    attempt was never recorded, so a later report's record must not stand in for it) or a record carries another metadata generation than the receipt.
    """
    found = attempts(client, bucket, run)
    receipt = read_receipt(client, bucket, run)
    first = attempt_key(run, 1)
    if receipt is None:
        return {"state": "unavailable", "reason": "the run has no load receipt (check.json)", "attempts": found}
    if not found:
        return {"state": "unavailable", "reason": f"no mirror attempt record exists; the first load's record would be {first}", "attempts": found}
    if found[0]["report_sha256"] != receipt.get("report_sha256"):
        return {"state": "unavailable", "reason": f"the first load's own attempt record is missing: {first} is the record of a later report", "attempts": found}
    if any(record["metadata_generation"] != receipt.get("metadata_generation") for record in found):
        return {"state": "unavailable", "reason": "an attempt record carries another metadata generation than the load receipt", "attempts": found}
    outcome = found[-1]["mirror"]
    return {"state": outcome["status"], "reason": outcome.get("reason", ""), "attempts": found}


def render(verdict: Mapping[str, Any]) -> str:
    """The attempts of a verdict in number order, each with its cause, then the verdict line."""
    lines = []
    for record in verdict["attempts"]:
        outcome = record["mirror"]
        cause = f": {outcome['reason']}" if "reason" in outcome else ""
        lines.append(
            f"{record['sequence']:03d} {record['report_run_at_utc']} {outcome['status']} generation {record['metadata_generation']} "
            f"report {record['report_sha256'][:12]}{cause}"
        )
    detail = f": {verdict['reason']}" if verdict["reason"] else ""
    return "\n".join([*lines, f"acceptance: {verdict['state']}{detail}"]) + "\n"


def main(argv: Sequence[str] | None = None, client: Any = None) -> int:
    """``show``: every mirror attempt of a bronze run in number order with its cause, then the verdict. Exit 1 unless it is ok, unchanged or skipped."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("command", choices=("show",))
    parser.add_argument("--run", required=True)
    parser.add_argument("--bucket", help="the data bucket (default: DATA_BUCKET_NAME)")
    args = parser.parse_args(list(sys.argv[1:] if argv is None else argv))
    if not RUN_ID.fullmatch(args.run):
        parser.error("the run ID must look like bronze_0a1b2c3d")
    bucket = args.bucket or os.environ.get("DATA_BUCKET_NAME", "")
    if not bucket:
        parser.error("show needs the data bucket: pass --bucket or set DATA_BUCKET_NAME")
    if client is None:
        client = glue_mirror.s3_client()
    try:
        verdict = acceptance(client, bucket, args.run)
    except AttemptError as error:
        sys.stdout.write(f"acceptance: unavailable: {error}\n")
        return 1
    sys.stderr.write(
        "the recorded outcome only, not a sign-off: the bronze_mirror_signoff command (see its docstring) also needs the matching task's result and report\n"
    )
    sys.stdout.write(render(verdict))
    return 0 if verdict["state"] in ("ok", "unchanged", "skipped") else 1


if __name__ == "__main__":
    sys.exit(main())
