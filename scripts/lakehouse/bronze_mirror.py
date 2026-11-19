"""Register the tables of a committed bronze load in the bronze Glue mirror database, the way the silver, gold and features publishes do.

``bronze.main`` calls ``mirror_after_load`` once, after the load and its metadata generation are committed. The switch, the time budget, the Glue
registration and the failure report are those of ``glue_mirror`` (``tables_to_mirror``, ``Budget``, ``mirror``, ``explain``); this module only names
what a bronze load mirrors. Only the real ``bronze`` namespace is mirrored: a qualification run (``bronze_e2e``), the dictionary namespaces and the
other test namespaces mirror nothing, and nothing runs while ``enabled`` is false in ``config/lakehouse/athena_mirror.json``.

The mirrored set is what the load committed: the snapshot of each table the load names (a table that the load left out is simply not mirrored) and the
snapshot of each loader side table that exists (``file_metadata``, ``stored_copies``, ``column_map`` and ``file_preambles``), read after the metadata
generation is published. ``glue_mirror.bronze_table_set`` is the one function that names the set, and the by-hand ``register`` takes it from there too.
A rerun of the load registers whatever is missing or stale.

The load is already committed, so the hook never raises and never undoes it. The report (status, reason, one outcome per table) goes into the loader report
and standard error when it failed. The check phase writes each attempt as its own create-once record,
``lakehouse/bronze_runs/<run>/mirror_<report digest>.json``, bound to the run, the metadata generation and the loader report, because ``check.json`` is the
immutable receipt of the load and a retry runs the hook again.
The latest attempt by the report's time is the state of the mirror: ``python -m scripts.lakehouse.bronze_mirror show --run <bronze run> [--bucket NAME]``
lists every attempt with its cause and exits 1 when the latest one failed.
"""

from __future__ import annotations

import argparse
import contextlib
import json
import os
import re
import sys
import time
from collections.abc import Callable, Mapping, Sequence
from typing import Any

from scripts.lakehouse import glue_mirror

REAL_NAMESPACE = "bronze"
# The run that a registration names when the loader was started by hand, with no bronze run ID.
HAND_RUN = "bronze_by_hand"
MIRROR_STATUSES = ("ok", "unchanged", "failed", "skipped")
ATTEMPT_KEY = "lakehouse/bronze_runs/{run}/mirror_{sha256}.json"
ATTEMPT_NAME = re.compile(r"mirror_(?P<sha256>[a-f0-9]{64})\.json")
REPORT_TIME = re.compile(r"[0-9]{8}T[0-9]{6}Z")
RUN_ID = re.compile(r"bronze_[a-f0-9]{8}(?:_[0-9]{1,2}){0,2}")


class AttemptError(ValueError):
    """A loader report cannot be bound to an attempt record, or a stored attempt record is not this run's own."""


def loader_snapshots(spark: Any, namespace: str, tables: Sequence[str]) -> dict[str, int]:
    """The committed snapshot of each of ``tables`` that exists in the catalog: a side table that no load has written yet is not there and is not an error."""
    from scripts.lakehouse import bronze

    present = [table for table in tables if spark.catalog.tableExists(f"{namespace}.{table}")]
    return bronze.table_snapshots(spark, namespace, present) if present else {}


def mirror_after_load(
    spark: Any,
    namespace: str,
    names: Sequence[str],
    snapshots: Mapping[str, int],
    run_id: str | None = None,
    source: Any = None,
    glue: Any = None,
    config: Any = None,
    read_loader: Callable[[Any, str, Sequence[str]], Mapping[str, int]] | None = None,
) -> dict[str, Any]:
    """Register the committed snapshot of each loaded table of ``names``, and of each loader side table that exists, in the bronze Glue mirror database.

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
        side = budget.run("reading the snapshots of the loader tables", read_loader or loader_snapshots, spark, namespace, glue_mirror.bronze_loader_tables())
        committed = {**loaded, **{name: int(snapshot) for name, snapshot in side.items()}}
        if source is None:
            from scripts.lakehouse.session import CATALOG

            source = glue_mirror.SparkSource(spark, CATALOG, namespace)
        if glue is None:
            glue = budget.run("creating the Glue client", glue_mirror.glue_client)
        report = glue_mirror.mirror(config, namespace, mode, committed, source, glue, run_id or HAND_RUN, budget)
    except Exception as error:
        report = {"status": "failed", "reason": glue_mirror.explain(error), "tables": {}}
    if report["status"] == "failed":
        with contextlib.suppress(OSError):
            sys.stderr.write(f"bronze load: the Glue mirror FAILED and the load stays committed: {report['reason']}. Rerun the load to retry the mirror.\n")
    return report


def outcome_shaped(outcome: object) -> bool:
    """The Glue mirror outcome of a loader report: a status, one outcome text per table and, for a failure or a skip, the reason."""
    if not isinstance(outcome, dict) or not {"status", "tables"} <= set(outcome) <= {"status", "tables", "reason"}:
        return False
    tables = outcome["tables"]
    if outcome["status"] not in MIRROR_STATUSES or not isinstance(tables, dict):
        return False
    if not all(isinstance(name, str) and isinstance(text, str) for name, text in tables.items()):
        return False
    return isinstance(outcome.get("reason"), str) if outcome["status"] in ("failed", "skipped") else "reason" not in outcome


def attempt_key(run: str, report_sha256: str) -> str:
    return ATTEMPT_KEY.format(run=run, sha256=report_sha256)


def attempt_record(run: str, report: Mapping[str, Any], report_sha256: str) -> dict[str, Any]:
    """One mirror attempt as durable evidence: the outcome of the loader report, bound to the run, the committed metadata generation and the report."""
    index, stamp = report.get("metadata_index"), report.get("run_at_utc")
    if not isinstance(index, dict) or not isinstance(index.get("generation"), str) or not index["generation"]:
        raise AttemptError("the loader report names no metadata generation to bind the mirror attempt to")
    if not isinstance(stamp, str) or not REPORT_TIME.fullmatch(stamp):
        raise AttemptError("the loader report has no run time to order the mirror attempts by")
    if not outcome_shaped(report.get("mirror")):
        raise AttemptError("the loader report holds no well-formed mirror outcome")
    return {
        "schema": 1,
        "bronze_run": run,
        "metadata_generation": index["generation"],
        "report_sha256": report_sha256,
        "report_run_at_utc": stamp,
        "mirror": report["mirror"],
    }


def attempts(client: Any, bucket: str, run: str) -> list[dict[str, Any]]:
    """Every attempt record of the run, oldest first by the time of its loader report. A record that is not this run's own is refused."""
    prefix = ATTEMPT_KEY.format(run=run, sha256="")[: -len(".json")]
    found: list[dict[str, Any]] = []
    token: str | None = None
    while True:
        page = client.list_objects_v2(Bucket=bucket, Prefix=prefix, **({"ContinuationToken": token} if token else {}))
        for item in page.get("Contents", []):
            name = ATTEMPT_NAME.fullmatch(item["Key"].rsplit("/", 1)[-1])
            if name is None:
                continue
            record = json.loads(client.get_object(Bucket=bucket, Key=item["Key"])["Body"].read())
            if not isinstance(record, dict) or record.get("bronze_run") != run:
                raise AttemptError(f"{item['Key']} belongs to another run")
            if record.get("report_sha256") != name["sha256"] or record.get("schema") != 1 or not outcome_shaped(record.get("mirror")):
                raise AttemptError(f"{item['Key']} is not a well-formed mirror attempt record")
            found.append(record)
        token = page.get("NextContinuationToken") if page.get("IsTruncated") else None
        if token is None:
            break
    return sorted(found, key=lambda record: (record["report_run_at_utc"], record["report_sha256"]))


def latest_attempt(client: Any, bucket: str, run: str) -> dict[str, Any] | None:
    found = attempts(client, bucket, run)
    return found[-1] if found else None


def main(argv: Sequence[str] | None = None, client: Any = None) -> int:
    """``show``: every mirror attempt of a bronze run with its cause, oldest first, then the latest status. Exit 1 when none exists or the latest failed."""
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
    found = attempts(client, bucket, args.run)
    for record in found:
        outcome = record["mirror"]
        cause = f": {outcome['reason']}" if "reason" in outcome else ""
        sys.stdout.write(
            f"{record['report_run_at_utc']} {outcome['status']} generation {record['metadata_generation']} report {record['report_sha256'][:12]}{cause}\n"
        )
    if not found:
        sys.stdout.write(f"no mirror attempt is recorded for {args.run}\n")
        return 1
    status = found[-1]["mirror"]["status"]
    sys.stdout.write(f"latest: {status}\n")
    return 1 if status == "failed" else 0


if __name__ == "__main__":
    sys.exit(main())
