"""Phase driver of the bronze ECS job that the direct launch starts.

    python -m scripts.lakehouse.bronze_run <phase> --run <run ID> [--tables a,b] [--target real|qualify] [--work /workspace] [--dry-run]

Each ECS container of the job runs one phase on the shared ``/workspace`` (the container names of ``config/orchestration/ecs_broker_jobs.json``):
``pin``, ``load`` and ``check``. The first container of the task (``fetch``) has already downloaded and unpacked ``code.tar.gz``.

The ``stage`` phase belongs to the small ``bronze_stage`` job that runs before the bronze job under the same run ID. It lists the raw sources with the task
role and writes the run's ``bronze_inventory.json`` and then its ``bronze_request.json`` (once each, for every table of the table map that has a source object,
as a real load) to ``lakehouse/run_stage/<run>/inputs/``. The launch preflight of the bronze job reads and checks that request and the pin phase checks both.
The request also names the tables it leaves out (``skipped_tables``) under the digest that every phase checks. The pin lists the sources of those tables
again with the task identity and refuses the run when one has a source object, so a request and a stage report that both drop such a table do not run. The
load and check phases read the list from the request, and the stage report must name the same tables.
A table with stored objects that only have rejected or retired observations is not absent: it has no loadable object, but its observations belong in the
metadata generation, so the stage and the pin refuse it by name instead of leaving it out. The owner ``inventory`` phase of a partial real run names, in the
same way, the unrequested tables that have no source object at all, and writes the stage report beside the request (upload both).

Two values change from run to run and arrive at dispatch: the tables and the target (``BRONZE_TARGET``). The preflight checks them before it starts the task.
The tables are not in the container environment, because a full run names 330 of them and a RunTask call takes at most 8192 characters of overrides. With no
``--tables`` and no ``BRONZE_TABLES`` (a hand run may set either) each phase reads them from the staged request, after it checks the request's bytes against
``BRONZE_REQUEST_SHA256``. ``pin`` checks them again against the table map of the unpacked code, and ``load`` and ``check`` refuse values that differ from the
pin.
The target ``real`` writes the namespaces ``bronze`` and ``bronze_dictionary``. The target ``qualify`` writes ``bronze_e2e`` and ``bronze_dictionary_e2e``
and takes the tables of one collection. No other namespace can be named.

``load`` runs the loader ``scripts.lakehouse.bronze``, which reads the versioned manifests of the acquisition bucket, checks every object against its recorded
version, SHA-256 and size, replaces each object's rows in place and publishes one metadata generation, so a retry of one run writes the same rows and no
second generation. A table the stage left out (no source object yet) is named to the loader with ``--unsourced-tables``: the loader reads the pinned manifests
for it again, publishes a generation without its observations and lists it in its report, and ``check`` compares that list with the stage report. ``check``
accepts the loader report: one report, a pass, the requested tables only. Any failed check exits 1 and names the phase.

With ``--dry-run`` or ``BRONZE_DRY_RUN=1`` ``pin`` runs as usual, because it writes only the task's own folder. ``load`` then reads the manifests and
every object version, checks each hash and size, prints and saves the plan and starts no loader: it needs no catalog and writes nothing shared. ``check``
does nothing. The first dry phase marks the run folder, so a later phase without the flag stays dry. A new ``pin`` clears the mark."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
import tarfile
import tempfile
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from orchestration import ingestion_schedule
from orchestration.training_contract import PIPELINE_DAG, PIPELINE_RUN, compose_run_id, run_id_parts, run_id_pattern
from scripts import metrics_push, phase_failure, resource_peaks
from scripts.infrastructure import startup_check
from scripts.lakehouse import bronze, bronze_mirror, bronze_notice, bronze_pin, provenance, task_capacity
from scripts.lakehouse.bronze_dispatch import NAMESPACES, SKIPPED_REASON, DispatchError, check_dispatch, check_skipped
from scripts.process import TimeoutExpired, run_command

PHASES = ("pin", "load", "check")
# The phases that start an engine (the loader runs Spark). Only they derive the engine limits and meet the floor of ``task_capacity``: the stage job holds
# 2 GiB and its stage and inventory phases only list sources and write JSON, and pin and check read and compare files.
ENGINE_PHASES = ("load",)
REQUEST_FILE = "bronze_request.json"
INVENTORY_FILE = "bronze_inventory.json"
STAGE_REPORT_FILE = "bronze_stage_report.json"
ABSENT_CODES = ("NoSuchKey", "404", "AccessDenied", "403")
STAGE_KEY = "lakehouse/run_stage/{run}/inputs/{name}"
REQUEST_KEYS = {"tables", "target", "dry_run", "inventory_sha256", "skipped_tables"}
TABLE_MAP = Path("config/lakehouse/bronze_tables.json")
REPORT_FOLDER = bronze.REPORT_ROOT
STEP_SECONDS = 4 * 3600
RECORD = "bronze_run.json"
DRY_MARKER = "dry_run"
PLAN_FILE = "dry_run_plan.json"
PLAN_PREFIX = "DRY_RUN_PLAN "
RUN_ID = re.compile(run_id_pattern("bronze"))
SHA256 = re.compile(r"[a-f0-9]{64}")
DRY_VALUES = ("1", "true", "yes")


class StepError(RuntimeError):
    """A phase cannot prove its input or the loader failed."""


@dataclass(frozen=True)
class Job:
    run: str
    work: Path
    bucket: str
    tables: tuple[str, ...]
    target: str
    dry_run: bool = False
    request_sha256: str = ""
    inventory_sha256: str = ""

    @property
    def code(self) -> Path:
        return self.work / "code"

    @property
    def folder(self) -> Path:
        return self.work / "state" / self.run

    @property
    def namespaces(self) -> tuple[str, str]:
        return NAMESPACES[self.target]


def run_module(job: Job, module: str, arguments: Sequence[str | Path], environment: dict[str, str] | None = None) -> None:
    """Run ``python -m module`` in the code folder; a nonzero exit or a deadline names the module."""
    try:
        done = run_command(sys.executable, ["-m", module, *map(str, arguments)], cwd=job.code, env={**os.environ, **(environment or {})}, timeout=STEP_SECONDS)
    except TimeoutExpired:
        raise StepError(f"{module} ran longer than {STEP_SECONDS} seconds") from None
    sys.stdout.write(done.stdout)
    sys.stderr.write(done.stderr)
    if done.returncode:
        raise StepError(f"{module} exited {done.returncode}")


def storage() -> Any:
    """The versioned reader of the acquisition bucket, with the task identity."""
    return bronze.S3Storage(task_role=True)


def read_staged(run: str, bucket: str, name: str) -> bytes:
    """One staged input of the run, read with the task identity."""
    import boto3

    return bytes(boto3.client("s3").get_object(Bucket=bucket, Key=STAGE_KEY.format(run=run, name=name))["Body"].read())


def request_document(run: str, bucket: str, request_sha256: str) -> dict[str, Any]:
    """The run's staged request, read with the task identity once its bytes match the digest the preflight checked."""
    body = read_staged(run, bucket, REQUEST_FILE)
    if not SHA256.fullmatch(request_sha256) or hashlib.sha256(body).hexdigest() != request_sha256:
        raise StepError("the staged request differs from the one the preflight checked")
    try:
        document = json.loads(body)
    except ValueError:
        raise StepError("the staged request is not JSON") from None
    if not isinstance(document, dict):
        raise StepError("the staged request holds no table list")
    return document


def staged_tables(run: str, bucket: str, request_sha256: str) -> list[str]:
    """The tables of the run's staged request, read with the task identity once its bytes match the digest the preflight checked. The pin checks the rest."""
    tables = request_document(run, bucket, request_sha256).get("tables")
    if not isinstance(tables, list):
        raise StepError("the staged request holds no table list")
    return tables


def owner_storage() -> Any:
    """The versioned reader of the acquisition bucket, with the owner's project profile."""
    return bronze.S3Storage(task_role=False)


def table_map(code: Path) -> dict[str, Any]:
    return bronze.load_table_map(code / TABLE_MAP)


def collections_of(config: dict[str, Any]) -> dict[str, str]:
    return {entry["table"]: entry["collection"] for entry in config["tables"]}


def archive_revision(archive: Path, code_sha256: str) -> str:
    """Check the archive digest again and return the commit the archive records."""
    if not SHA256.fullmatch(code_sha256) or bronze.sha256_file(archive)[0] != code_sha256:
        raise StepError("code.tar.gz does not match the expected SHA-256")
    with tarfile.open(archive) as tar:
        revision = str(tar.pax_headers.get("comment", ""))
    if not provenance.REVISION.fullmatch(revision):
        raise StepError("code.tar.gz carries no commit ID (it must come from git archive)")
    return revision


def recorded(job: Job) -> dict[str, Any]:
    path = job.folder / RECORD
    if not path.is_file():
        raise StepError("the pin phase has not run for this run")
    record: dict[str, Any] = json.loads(path.read_text())
    if record["run_id"] != job.run or tuple(record["tables"]) != job.tables or record["target"] != job.target:
        raise StepError("the dispatch values changed after the pin phase")
    if job.request_sha256 and job.request_sha256 != record["request_sha256"]:
        raise StepError("the request digest changed after the pin phase")
    return record


def pin(job: Job, archive: Path, code_sha256: str) -> None:
    """Bind the code, the request the launch preflight checked and the source inventory of the run."""
    revision = archive_revision(archive, code_sha256)
    request_raw = read_staged(job.run, job.bucket, REQUEST_FILE)
    if not SHA256.fullmatch(job.request_sha256) or hashlib.sha256(request_raw).hexdigest() != job.request_sha256:
        raise StepError("the staged request differs from the one the preflight checked")
    request = json.loads(request_raw)
    if not isinstance(request, dict) or set(request) != REQUEST_KEYS or type(request["dry_run"]) is not bool:
        raise StepError("the staged request needs exactly tables, target, dry_run, inventory_sha256 and skipped_tables")
    if tuple(request["tables"]) != job.tables or request["target"] != job.target or request["inventory_sha256"] != job.inventory_sha256:
        raise StepError("the staged request differs from the dispatch values")
    config = table_map(job.code)
    skipped = check_skipped(request["skipped_tables"], job.tables, job.target, collections_of(config))
    if [item["table"] for item in staged_skipped(job)] != list(skipped):
        raise StepError("the stage report names other skipped tables than the staged request")
    # The request and the report are written by the same stage, so the pin lists the sources again: a table with source objects cannot be left out.
    bronze.check_unsourced(storage(), job.bucket, config, list(job.tables), list(skipped), [], bronze.load_retired(job.code / bronze.RETIRED))
    if request["dry_run"] and not job.dry_run:
        raise StepError("the request is a dry run and the task is not: a dry run request cannot run live")
    inventory_raw = read_staged(job.run, job.bucket, INVENTORY_FILE)
    if not SHA256.fullmatch(job.inventory_sha256) or hashlib.sha256(inventory_raw).hexdigest() != job.inventory_sha256:
        raise StepError("the staged inventory differs from the digest in the request")
    inventory = bronze_pin.read_inventory(inventory_raw)
    if inventory["tables"] != sorted(job.tables):
        raise StepError("the staged inventory covers other tables than the dispatch")
    bronze_pin.require_objects(inventory)
    job.folder.mkdir(parents=True, exist_ok=True)
    (job.folder / INVENTORY_FILE).write_bytes(inventory_raw)
    record = {"run_id": job.run, "code_sha256": code_sha256, "git_revision": revision, "tables": list(job.tables), "target": job.target}
    record |= {"request_sha256": job.request_sha256, "inventory_sha256": job.inventory_sha256, "skipped_tables": list(skipped)}
    (job.folder / RECORD).write_text(json.dumps(record, indent=2, sort_keys=True) + "\n")
    sys.stdout.write(
        f"pinned bronze run {job.run}: target {job.target}, tables {','.join(job.tables)}, code {revision[:12]}, inventory {job.inventory_sha256[:12]}\n"
    )


def stage_inputs(code: Path, bucket: str, tables: str, target: str, dry_run: bool, out: Path, run: str) -> tuple[str, str]:
    """Owner side: list the current sources once and write the inventory and the request that names its digest. Stage both beside the code archive.

    A real run that requests only some tables also names the other tables that have no source object at all (no loadable object and no rejected or
    retired observation), as the stage job does for a full run: after a generation that left such a table out, a partial load needs that absence in its
    bound request. The tables that have sources are not named, and the stage report for ``run`` is written beside the request when any table is named.
    """
    config = table_map(code)
    names = check_dispatch(tables, target, collections_of(config))
    retired = bronze.load_retired(code / bronze.RETIRED)
    inventory = bronze_pin.build_inventory(owner_storage(), bucket, config, list(names), retired)
    bronze_pin.require_objects(inventory)
    raw = bronze_pin.inventory_bytes(inventory)
    digest = hashlib.sha256(raw).hexdigest()
    others = [name for name in collections_of(config) if name not in names] if target == "real" else []
    skipped: list[str] = []
    if others:
        found, excluded = bronze.discover(owner_storage(), bucket, config, others, retired)
        known = {item["table"] for item in found} | {entry["copy"]["table"] for entry in excluded if "copy" in entry}
        skipped = [name for name in others if name not in known]
    request = json.dumps(
        {"tables": list(names), "target": target, "dry_run": dry_run, "inventory_sha256": digest, "skipped_tables": skipped}, sort_keys=True
    ).encode()
    out.mkdir(parents=True, exist_ok=True)
    (out / INVENTORY_FILE).write_bytes(raw)
    if skipped:
        items = [{"table": name, "reason": SKIPPED_REASON} for name in skipped]
        report = {"schema": 1, "bronze_run": run, "requested_tables": list(names), "skipped_tables": items, "skipped_count": len(items)}
        (out / STAGE_REPORT_FILE).write_bytes(json_bytes(report))
    (out / REQUEST_FILE).write_bytes(request)
    sys.stdout.write(f"staged {len(inventory['objects'])} objects from {len(inventory['manifests'])} manifests: inventory {digest}\n")
    if skipped:
        sys.stdout.write(f"skipped_tables {len(skipped)}: {','.join(skipped)}\n")
    return hashlib.sha256(request).hexdigest(), digest


def put_stage_object(client: Any, bucket: str, key: str, body: bytes) -> bytes:
    """Write one staged object once and return the staged bytes: the body when this call wrote it, the object already there when it is a retry.

    The task role cannot list the stage folder, so S3 answers 403 for a key that does not exist. The write therefore comes first and only a key that is
    there is read.
    """
    if put_once(client, bucket, key, body):
        return body
    return bytes(client.get_object(Bucket=bucket, Key=key)["Body"].read())


def stage_run(code: Path, bucket: str, run: str) -> dict[str, Any]:
    """Job side: stage the request and the source inventory of a pipeline run, for every table of the table map that has a source object, as a real load.

    A table with no source object cannot load (the pin refuses an inventory with an empty table), so it is left out and named in the output; with no
    object for any table the stage fails.

    The sources are read with the task identity (the bronze role). The inventory is written first and the request last, each once, so the request only
    ever names an inventory that is staged. A retry keeps what is staged: it lists the sources again, but the staged inventory wins over the new one (and is
    checked as the pin checks it), and a staged request that differs from the one this run would write is refused and never replaced.
    """
    config = table_map(code)
    names = check_dispatch(list(collections_of(config)), "real", collections_of(config))
    client = storage().client
    inventory_key, request_key = (STAGE_KEY.format(run=run, name=name) for name in (INVENTORY_FILE, REQUEST_FILE))
    listed = bronze_pin.build_inventory(storage(), bucket, config, list(names), bronze.load_retired(code / bronze.RETIRED))
    present = {row[0] for row in listed["objects"]}
    left_out = [name for name in names if name not in present]
    if left_out and present:
        # A table with stored objects that only have rejected or retired observations has no loadable object but is not absent: it is never left out.
        bronze.check_unsourced(storage(), bucket, config, [name for name in names if name in present], left_out, [], bronze.load_retired(code / bronze.RETIRED))
        listed = {**listed, "tables": [name for name in listed["tables"] if name in present]}
    bronze_pin.require_objects(listed)
    raw = put_stage_object(client, bucket, inventory_key, bronze_pin.inventory_bytes(listed))
    inventory = bronze_pin.read_inventory(raw)
    names = tuple(name for name in names if name in inventory["tables"])
    if not names or inventory["tables"] != sorted(names):
        raise StepError("the staged inventory covers other tables than the table map")
    bronze_pin.require_objects(inventory)
    digest = hashlib.sha256(raw).hexdigest()
    skipped = [{"table": name, "reason": SKIPPED_REASON} for name in collections_of(config) if name not in names]
    report = {"schema": 1, "bronze_run": run, "requested_tables": list(names), "skipped_tables": skipped, "skipped_count": len(skipped)}
    report_bytes = json_bytes(report)
    if put_stage_object(client, bucket, STAGE_KEY.format(run=run, name=STAGE_REPORT_FILE), report_bytes) != report_bytes:
        raise StepError(f"{STAGE_REPORT_FILE} is staged and differs from the report of the staged inventory: it is not replaced")
    document = {"tables": list(names), "target": "real", "dry_run": False, "inventory_sha256": digest, "skipped_tables": [item["table"] for item in skipped]}
    request = json.dumps(document, sort_keys=True).encode()
    if put_stage_object(client, bucket, request_key, request) != request:
        raise StepError(f"{request_key} is staged and differs from the request this stage would write: it is not replaced")
    request_digest = hashlib.sha256(request).hexdigest()
    sys.stdout.write(f"skipped_tables {len(skipped)}{': ' + ','.join(item['table'] for item in skipped) if skipped else ''}\n")
    sys.stdout.write(
        f"staged for {run}: {len(inventory['objects'])} objects from {len(inventory['manifests'])} manifests, inventory {digest}, request {request_digest}\n"
    )
    metrics_push.add("hai_bronze_stage_skipped_tables", len(skipped))
    return {"request_sha256": request_digest, "inventory_sha256": digest, "skipped_count": len(skipped)}


def pinned_inventory(job: Job) -> dict[str, Any]:
    """The inventory the pin bound, read again from the run folder and checked against its recorded digest."""
    raw = (job.folder / INVENTORY_FILE).read_bytes()
    if hashlib.sha256(raw).hexdigest() != recorded(job)["inventory_sha256"]:
        raise StepError("the pinned inventory changed after the pin phase")
    inventory = bronze_pin.read_inventory(raw)
    bronze_pin.require_objects(inventory)
    return inventory


def plan_load(job: Job) -> dict[str, Any]:
    """Dry-run ``load``: read the manifests and every object version, check each hash and size, then print and save the plan. Nothing shared is written."""
    config = table_map(job.code)
    inventory = pinned_inventory(job)
    source = bronze_pin.PinnedStorage(storage(), inventory)
    inputs, _ = bronze.discover(source, job.bucket, config, job.tables, bronze.load_retired(job.code / bronze.RETIRED))
    bronze_pin.verify_replay(inventory, inputs)
    objects = []
    scratch = job.folder / "scratch"
    scratch.mkdir(parents=True, exist_ok=True)
    for item in inputs:
        with tempfile.TemporaryDirectory(dir=scratch) as folder:
            path = Path(folder) / "object"
            source.download(item["bucket"], item["key"], item["version_id"], path)
            if bronze.sha256_file(path) != (item["sha256"], item["byte_count"]):
                raise StepError(f"{item['key']}: the bytes read do not match the recorded SHA-256 and size")
        keys = ("table", "key", "version_id", "sha256", "byte_count", "manifest_key", "manifest_version_id")
        objects.append({name: item[name] for name in keys} | {"sha256_verified": True})
    per_table = {name: sum(1 for item in objects if item["table"] == name) for name in job.tables}
    summary = {"objects": len(objects), "bytes": sum(item["byte_count"] for item in objects), "objects_per_table": per_table}
    plan = {"dry_run": True, "run": job.run, "target": job.target, "namespaces": list(job.namespaces), "tables": list(job.tables)}
    plan |= {"summary": summary, "objects": objects, "catalog_writes": "none"}
    (job.folder / PLAN_FILE).write_text(json.dumps(plan, indent=2, sort_keys=True) + "\n")
    sys.stdout.write(f"dry run plan for bronze run {job.run} (target {job.target}, namespaces {', '.join(job.namespaces)}): nothing was written\n")
    sys.stdout.write(f"objects {summary['objects']}, bytes {summary['bytes']}, every hash and size checked against the manifest\n")
    sys.stdout.write(PLAN_PREFIX + json.dumps(plan, sort_keys=True, separators=(",", ":")) + "\n")
    return plan


def load(job: Job) -> None:
    namespace, dictionary = job.namespaces
    pinned_inventory(job)
    arguments = ["--tables", ",".join(job.tables), "--namespace", namespace, "--dictionary-namespace", dictionary, "--bucket", job.bucket, "--task-role"]
    arguments += ["--inventory", str(job.folder / INVENTORY_FILE), "--run-id", job.run]
    if job.target == "qualify":
        arguments.append("--selected-metadata")
    else:
        left_out = [item["table"] for item in staged_skipped(job)]
        if left_out:
            arguments += ["--unsourced-tables", ",".join(left_out)]
    run_module(job, "scripts.lakehouse.bronze", arguments)


def table_check_passed(check: object) -> bool:
    if not isinstance(check, dict):
        return False
    optional = ("one_loaded_copy_per_file", "loaded_copies_match_rows", "distinct_files_match_expected", "distinct_rows_match_expected")
    return check.get("passed") is True and check.get("objects_match_inputs") is True and all(check[name] is True for name in optional if name in check)


def accept_report(job: Job) -> tuple[dict[str, Any], bytes]:
    """The loader wrote exactly one report, it passed, and it names the requested tables and no others. Return the report and its bytes."""
    reports = sorted((job.code / REPORT_FOLDER).glob("report_*.json"))
    if len(reports) != 1:
        raise StepError(f"expected one loader report, found {len(reports)}")
    body = reports[0].read_bytes()
    report = json.loads(body)
    if report.get("mode") != "load" or report.get("passed") is not True:
        raise StepError("the loader report did not pass")
    if sorted(report.get("tables", [])) != sorted(job.tables):
        raise StepError("the loader report names other tables than the dispatch")
    checks = report.get("checks", {})
    if not isinstance(checks, dict) or set(checks) != set(job.tables) or not all(table_check_passed(check) for check in checks.values()):
        raise StepError("a table check of the loader report did not pass")
    if "mirror" in report and not bronze_mirror.outcome_shaped(report["mirror"]):
        raise StepError("the loader report holds a malformed Glue mirror outcome")
    return report, body


def accepted_unsourced(report: dict[str, Any], skipped: Sequence[dict[str, str]]) -> None:
    """The loader left out exactly the tables the stage report names: its report lists them, and a run that left none out lists none."""
    if sorted(report.get("unsourced_tables", [])) != sorted(item["table"] for item in skipped):
        raise StepError("the loader report left out other tables than the stage report names")


def staged_skipped(job: Job) -> list[dict[str, str]]:
    """The tables the stage job left out of this run's request, each with its reason: the names come from the staged request, checked against the digest the
    preflight bound, and the reasons from the stage report that the stage job wrote before it, which must name the same tables.

    A hand run with no bound request skipped nothing, and a request that skips nothing needs no report (a hand-staged run has none). A request that skips a
    table with no report, a report of another run or with a wrong count, and a report that names other tables than the request or a requested table are refused.
    """
    from botocore.exceptions import ClientError

    if not job.request_sha256:
        return []
    names = request_document(job.run, job.bucket, job.request_sha256).get("skipped_tables")
    if not isinstance(names, list) or not all(isinstance(name, str) for name in names):
        raise StepError("the staged request holds no skipped table list")
    try:
        raw = read_staged(job.run, job.bucket, STAGE_REPORT_FILE)
    except ClientError as error:
        if str(error.response.get("Error", {}).get("Code")) in ABSENT_CODES:
            if names:
                raise StepError("the staged request skips tables but the stage report is missing") from None
            return []
        raise
    try:
        report = json.loads(raw)
    except ValueError:
        raise StepError("the stage report is not JSON") from None
    keys = {"schema", "bronze_run", "requested_tables", "skipped_tables", "skipped_count"}
    skipped = report.get("skipped_tables") if isinstance(report, dict) else None
    if not isinstance(report, dict) or set(report) != keys or report["schema"] != 1 or report["bronze_run"] != job.run or not isinstance(skipped, list):
        raise StepError("the stage report is not the report of this run")
    listed = [
        item.get("table") if isinstance(item, dict) and set(item) == {"table", "reason"} and isinstance(item["reason"], str) else None for item in skipped
    ]
    if None in listed or len(set(listed)) != len(listed) or report["skipped_count"] != len(skipped) or set(listed) & set(job.tables):
        raise StepError("the stage report names a table twice, a table of the request, or the wrong number of tables")
    if report["requested_tables"] != list(job.tables):
        raise StepError("the stage report describes another request than the one this run loads")
    if sorted(name for name in listed if name is not None) != sorted(names):
        raise StepError("the stage report names other skipped tables than the staged request")
    if any(item["reason"] != SKIPPED_REASON for item in skipped):
        raise StepError(f"the stage report gives a reason other than {SKIPPED_REASON!r} for a skipped table")
    reasons = {item["table"]: item["reason"] for item in skipped}
    return [{"table": name, "reason": reasons[name]} for name in names]


def run_record(job: Job, report: dict[str, Any], report_bytes: bytes, inventory: dict[str, Any], skipped: Sequence[dict[str, str]] = ()) -> dict[str, Any]:
    """The run record ``lakehouse/bronze_runs/<run>/check.json`` of a passed real run, in the shape ``orchestration.raw_lineage.loaded_objects`` reads: the
    tables, the snapshot of each (which the loader read from the catalog), the generation of the metadata it published, the digest of the loader report
    and every object the run loaded with its version, SHA-256 and size (from the pinned inventory). A report without them gives no record."""
    snapshots, index = report.get("snapshots"), report.get("metadata_index")
    if not isinstance(snapshots, dict) or set(snapshots) != set(job.tables) or any(type(value) is not int or value <= 0 for value in snapshots.values()):
        raise StepError("the loader report holds no snapshot of each requested table")
    if not isinstance(index, dict) or not isinstance(index.get("generation"), str) or not index["generation"]:
        raise StepError("the loader report names no metadata generation")
    objects = [
        dict(zip(("table", "bucket", "key", "version_id", "sha256", "byte_count"), (row[0], job.bucket, row[1], row[2], row[3], row[4]), strict=True))
        for row in inventory["objects"]
        if row[0] in job.tables
    ]
    if not objects:
        raise StepError("the pinned inventory lists no object of the requested tables")
    return {
        "schema_version": 1,
        "run_id": job.run,
        "target": job.target,
        "namespaces": list(job.namespaces),
        "tables": list(job.tables),
        "report_sha256": hashlib.sha256(report_bytes).hexdigest(),
        "metadata_generation": index["generation"],
        "snapshots": snapshots,
        "objects": objects,
        "skipped_tables": list(skipped),
    }


def store_run_record(client: Any, bucket: str, run: str, record: dict[str, Any]) -> str:
    """Write the run record once. A retry of the run finds it and keeps the first one, so the record names the load that made the tables."""
    created = put_once(client, bucket, f"lakehouse/bronze_runs/{run}/check.json", json_bytes(record))
    return "written" if created else "kept"


def read_run_record(client: Any, bucket: str, key: str) -> dict[str, Any] | None:
    """One stored run record of an ingestion run (the current version), or None when the key holds none. Any other failure is raised, never read as missing."""
    latest = [
        item["VersionId"] for item in client.list_object_versions(Bucket=bucket, Prefix=key).get("Versions", []) if item["Key"] == key and item["IsLatest"]
    ]
    if not latest:
        return None
    record = json.loads(client.get_object(Bucket=bucket, Key=key, VersionId=latest[0])["Body"].read())
    if not isinstance(record, dict):
        raise StepError(f"{key} is not a run record")
    return record


def put_once(client: Any, bucket: str, key: str, body: bytes) -> bool:
    """Create ``key`` only if it does not exist: True when this call created it, False when an earlier call had (the bucket refuses the write)."""
    from botocore.exceptions import ClientError

    try:
        client.put_object(Bucket=bucket, Key=key, Body=body, IfNoneMatch="*", ServerSideEncryption="AES256")
    except ClientError as error:
        if str(error.response.get("Error", {}).get("Code")) != "PreconditionFailed":
            raise
        return False
    return True


def read_own_record(client: Any, bucket: str, key: str) -> dict[str, Any] | None:
    """One record this run wrote, or None when the key cannot be read. The role cannot list its records folder, so a key that does not exist answers 403
    as well as 404: both read as missing."""
    from botocore.exceptions import ClientError

    try:
        record = json.loads(client.get_object(Bucket=bucket, Key=key)["Body"].read())
    except ClientError as error:
        if str(error.response.get("Error", {}).get("Code")) in ("NoSuchKey", "404", "AccessDenied", "403"):
            return None
        raise
    return record if isinstance(record, dict) else None


def json_bytes(document: dict[str, Any]) -> bytes:
    return (json.dumps(document, indent=2, sort_keys=True) + "\n").encode()


CLAIMED = "claimed"
NO_OUTCOME = "unknown: an earlier attempt of this run claimed the notice and recorded no outcome, so it is not sent again"


def recorded_carried_over(client: Any, bucket: str, run: str) -> dict[str, Any]:
    """The carried-over record a first attempt of the run wrote, with the outcome of its notice. A repeated attempt reports this and sends nothing."""
    document = read_own_record(client, bucket, f"lakehouse/bronze_runs/{run}/carried_over.json")
    if document is None:
        return {"schema": 1, "bronze_run": run, "carried_over": None, "notice": "kept: an earlier attempt recorded it and the record cannot be read"}
    if document.get("notice") == CLAIMED:
        outcome = read_own_record(client, bucket, f"lakehouse/bronze_runs/{run}/carried_over_notice.json")
        document["notice"] = str(outcome["notice"]) if outcome is not None and "notice" in outcome else NO_OUTCOME
    return document


def read_counted_record(client: Any, bucket: str, key: str) -> tuple[dict[str, Any] | None, str]:
    """A run record the notice counts, or None with the reason it is not counted. A record that is missing, not JSON, not an object or without its unit lists
    never raises: the notice names it."""
    try:
        record = read_run_record(client, bucket, key)
        if record is not None:
            ingestion_schedule.capture_counts(record, [])
        return record, "is missing"
    except Exception:
        return None, "cannot be read"


def record_carried_over(client: Any, bucket: str, run: str, root: str, *, topic: str, notify: Any, skipped: Sequence[dict[str, str]] = ()) -> dict[str, Any]:
    """Count the source files the ingestion run's capture captured, failed and held, name the files that no run of its retry period captured and the tables
    the stage left out (``skipped``, each with its reason), in the bronze run record and in the owner notice.

    Bronze loads the latest capture of every file, so a file still missing after the retry period keeps its earlier capture. The counts are those of the
    capture's own check (``ingestion_schedule.capture_counts``). A run record that is missing or cannot be read is named in the notice and its files are not
    counted. The notice is sent for every run that has a capture root, also when nothing is missing. A run with no capture root (a hand-staged bronze run)
    sends it only for a skipped table, and the record names the skipped tables whether or not it is sent.

    The record ``lakehouse/bronze_runs/<run>/carried_over.json`` is created once and is the claim on the notice: the attempt that creates it sends the notice,
    and any later attempt reads the record, sends nothing and reports it. The outcome of the notice goes to ``carried_over_notice.json``, created once after the
    send. So a notice is sent at most once per run. An attempt that is interrupted between the claim and the outcome leaves a record that says the outcome is
    unknown: the notice may or may not have gone, and it is not sent again. A notice that fails is recorded and never fails the run.
    """
    original, retries = ingestion_schedule.ingestion_runs_of(run)
    folder = f"{root}ingestion_runs/"
    document: dict[str, Any] = {"schema": 1, "bronze_run": run, "ingestion_run": original, "retry_runs": list(retries), "skipped_tables": list(skipped)}
    files: list[str] | None = None
    counts: dict[str, int] | None = None
    unread: dict[str, str] = {}
    if not root:
        document |= {"carried_over": None, "note": "no capture root is configured (HAI_CAPTURE_ROOT): the files carried over are not known"}
    else:
        found: dict[str, dict[str, Any]] = {}
        for name in (original, *retries):
            record, why = read_counted_record(client, bucket, f"{folder}{name}/run.json")
            if record is None:
                unread[name] = why
            else:
                found[name] = record
        if original not in found:
            what = f"no run record of {original} under {folder}" if unread[original] == "is missing" else f"the run record of {original} cannot be read"
            document |= {"carried_over": None, "note": f"{what}: the files carried over are not known"}
        else:
            captured, failed, held = ingestion_schedule.capture_counts(found[original], [found[name] for name in retries if name in found])
            files, counts = sorted(failed | held), {"captured": len(captured), "failed": len(failed), "held": len(held)}
            document |= {"carried_over": files, "counts": counts, "retry_records_read": len(found) - 1}
        document["unread_records"] = unread
    message: tuple[str, str] | None = None
    if not root and not skipped:
        document["notice"] = "not sent"
    elif not topic:
        document["notice"] = "not sent: no alerts topic"
    else:
        document["notice"] = CLAIMED
        message = bronze_notice.carried_over_notice(run, original, counts, files or [], skipped, unread)
    if not put_once(client, bucket, f"lakehouse/bronze_runs/{run}/carried_over.json", json_bytes(document)):
        return recorded_carried_over(client, bucket, run)
    if message is not None:
        try:
            notify.publish(TopicArn=topic, Subject=message[0], Message=message[1])
            outcome = "sent"
        except Exception as error:
            outcome = f"failed: {type(error).__name__}"
        put_once(client, bucket, f"lakehouse/bronze_runs/{run}/carried_over_notice.json", json_bytes({"schema": 1, "bronze_run": run, "notice": outcome}))
        document["notice"] = outcome
    return document


def show_mirror(report: dict[str, Any]) -> None:
    """Name the Glue mirror outcome of the load. A failure fails no phase, because the load is committed, but it is shown on standard error."""
    outcome = report.get("mirror")
    if outcome is None:
        return
    detail = f": {outcome['reason']}" if "reason" in outcome else ""
    sys.stdout.write(f"glue mirror {outcome['status']}{detail}\n")
    if outcome["status"] == "failed":
        sys.stderr.write(
            f"bronze_run: glue mirror FAILED, the run stays committed{detail}. Rerun the load phase to register the tables, or run glue_mirror register.\n"
        )
    elif outcome["status"] == "unavailable":
        sys.stderr.write(
            f"bronze_run: glue mirror UNAVAILABLE (the IAM grant is not applied yet), the run stays committed and nothing is recorded as ok{detail}. "
            "Rerun the load phase after the IAM apply, or run glue_mirror register.\n"
        )


class AttemptWriteError(StepError):
    """The mirror attempt record of a loader report could not be written. The message names the exact key, the report and the metadata generation.

    ``denied`` is true when the cause is a missing grant (AccessDenied). ``history`` is true when the run has a recorded mirror attempt or when its attempt
    records cannot be read: only a run with no history may tolerate an unavailable mirror."""

    def __init__(self, message: str, denied: bool = False, history: bool = False) -> None:
        super().__init__(message)
        self.denied = denied
        self.history = history


def read_attempt(client: Any, bucket: str, key: str) -> dict[str, Any] | None:
    """The attempt record at ``key``, or None when the key reads as absent.

    The task role cannot list its records folder, and it reads a key only through the read grant: an absent key answers 403 or 404 and so does a key that
    exists but may not be read, so both read as absent (before the IAM apply no record can exist; the owner's profile lists the folder). Every other read error
    and a body that is not an attempt record object raise ``AttemptWriteError`` with ``history`` set: a record that exists, or a history that cannot be
    read, is never read as no history.
    """
    from botocore.exceptions import ClientError

    try:
        body = client.get_object(Bucket=bucket, Key=key)["Body"].read()
    except ClientError as error:
        if str(error.response.get("Error", {}).get("Code")) in ABSENT_CODES:
            return None
        raise AttemptWriteError(
            f"the mirror attempt record {key} cannot be read ({error}), so the history of this run's mirror attempts is unknown", history=True
        ) from None
    try:
        record = json.loads(body)
    except ValueError:
        record = None
    if not isinstance(record, dict):
        raise AttemptWriteError(f"{key} exists and is not an attempt record object, so the history of this run's mirror attempts cannot be read", history=True)
    return record


def store_mirror_attempt(client: Any, bucket: str, run: str, report: dict[str, Any], report_sha256: str) -> tuple[str, str]:
    """Create the attempt record of this loader report under the first free number and return ``(written or kept, key)``.

    The number is the order of the chain: the attempt reads the numbers from 001 upward and takes the first free one with a conditional create, so no two
    attempts share a number. A record of the same report that is already there is kept, so a repeat of the same check phase writes nothing new. A lost race
    (another attempt created the number between the read and the write) reads that number again. The role cannot list its folder, so a number that does not
    exist reads as 403 and as missing.
    """
    from botocore.exceptions import ClientError

    sequence, key, refused, history = 1, bronze_mirror.attempt_key(run, 1), "", False
    for _ in range(2 * bronze_mirror.MAX_ATTEMPTS):
        key = bronze_mirror.attempt_key(run, sequence)
        try:
            existing = read_attempt(client, bucket, key)
            if existing is None and refused == key:
                raise AttemptWriteError(
                    f"the mirror attempt record {key} exists and cannot be read, so the next number cannot be found (report {report_sha256[:12]})", history=True
                )
            if existing is not None:
                history = True
                if existing.get("report_sha256") == report_sha256:
                    return "kept", key
                sequence += 1
                if sequence > bronze_mirror.MAX_ATTEMPTS:
                    break
                continue
            if put_once(client, bucket, key, json_bytes(bronze_mirror.attempt_record(run, report, report_sha256, sequence))):
                return "written", key
            refused = key
        except ClientError as error:
            generation = report.get("metadata_index", {}).get("generation", "unknown") if isinstance(report.get("metadata_index"), dict) else "unknown"
            raise AttemptWriteError(
                f"the mirror attempt record {key} was not written (report {report_sha256[:12]}, generation {generation}): {error}",
                denied=str(error.response.get("Error", {}).get("Code")) in ("AccessDenied", "403"),
                history=history,
            ) from None
        except bronze_mirror.AttemptError as error:
            raise StepError(str(error)) from None
    raise AttemptWriteError(f"the run has more than {bronze_mirror.MAX_ATTEMPTS} mirror attempt records: {key} cannot be numbered", history=True)


def record_mirror_attempt(job: Job, report: dict[str, Any], report_bytes: bytes, retry: bool) -> None:
    """Keep this attempt's mirror outcome as its own numbered create-once record beside the run record, which stays the immutable receipt of the load.

    A first check whose record cannot be written (the code refresh can come before the IAM apply that grants the key) fails no phase: the committed run goes
    on, the missing record is named with its exact key, and the acceptance of the mirror (``python -m scripts.lakehouse.bronze_mirror show``) says
    ``unavailable``. A retry (the receipt of the run already exists) whose record cannot be written fails its check phase: the durable history would
    otherwise end on an older attempt, and an acceptance would read a pass that this attempt did not earn. The load and its receipt stay as they are.

    The one exception is a mirror that is ``unavailable`` (Glue refused every call: the IAM grant is not applied yet) whose record is refused with AccessDenied
    too, for a run with no recorded mirror attempt: the grant has never worked for this run, so no attempt fails its check phase for it, first run or retry,
    and nothing is recorded. A run with any recorded attempt, or whose attempt records cannot be read (a read error, a body that is not a record), gets no
    exception: its retry fails by name. The role cannot tell a grant that was never applied from one withdrawn in full (both answer 403, and it cannot list
    the folder), so a full withdrawal after a recorded ok exits 0 as unavailable; the sign-off needs the owner's listing, the task result and the task's
    loader report (matched to the last stored attempt) for that.
    """
    if "mirror" not in report:
        return
    try:
        outcome, key = store_mirror_attempt(storage().client, job.bucket, job.run, report, hashlib.sha256(report_bytes).hexdigest())
    except AttemptWriteError as error:
        if retry and not (error.denied and not error.history and report["mirror"]["status"] == "unavailable"):
            raise StepError(f"{error}; the run already has a recorded load, so the history of its mirror cannot end on an older attempt") from None
        sys.stderr.write(f"bronze_run: {error}\n")
        return
    sys.stdout.write(f"mirror attempt record {outcome}: {key}\n")


def check(job: Job) -> None:
    report, body = accept_report(job)
    sys.stdout.write(f"bronze run {job.run} passed: {','.join(job.tables)} in {' and '.join(job.namespaces)}\n")
    show_mirror(report)
    if job.target == "real":
        skipped = staged_skipped(job)
        accepted_unsourced(report, skipped)
        stored = store_run_record(storage().client, job.bucket, job.run, run_record(job, report, body, pinned_inventory(job), skipped))
        sys.stdout.write(f"run record {stored}; skipped_tables {len(skipped)}\n")
        topic = os.environ.get("ALERTS_TOPIC_ARN", "")
        notify = None
        if topic:
            import boto3

            notify = boto3.client("sns")
        done = record_carried_over(storage().client, job.bucket, job.run, os.environ.get("HAI_CAPTURE_ROOT", ""), topic=topic, notify=notify, skipped=skipped)
        files = done["carried_over"]
        sys.stdout.write(f"carried over: {'unknown' if files is None else len(files)} files; notice {done['notice']}\n")
        record_mirror_attempt(job, report, body, retry=stored == "kept")
    if job.target == "real":
        parts = run_id_parts(job.run)
        conf = json.dumps({"run_id": compose_run_id(PIPELINE_RUN, parts.hex, parts.attempt), "mode": "real", "start_from": "silver"})
        sys.stdout.write(f"the later layers run from this bronze run; to start them by hand: airflow dags trigger {PIPELINE_DAG} --conf '{conf}'\n")
    else:
        sys.stdout.write("this was a qualification run in the test twins: the later layers read production bronze, so do not start them\n")


def dispatch(job: Job, phase: str, args: argparse.Namespace) -> None:
    if phase == "pin":
        pin(job, args.archive or job.work / "code.tar.gz", args.code_sha256)
        return
    recorded(job)
    if phase == "load":
        plan_load(job) if job.dry_run else load(job)
    elif job.dry_run:
        sys.stdout.write(json.dumps({"dry_run": True, "checked": 0}) + "\n")
    else:
        check(job)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("phase", choices=(*PHASES, "inventory", "stage"))
    parser.add_argument("--run", required=True)
    parser.add_argument("--tables", default="", help="comma-separated tables (also BRONZE_TABLES; with neither, the staged request names them)")
    parser.add_argument("--target", default="", help="real or qualify (also BRONZE_TARGET)")
    parser.add_argument("--work", type=Path, default=Path("/workspace"))
    parser.add_argument("--archive", type=Path)
    parser.add_argument("--code", type=Path, default=Path("."), help="inventory: the unpacked repository that holds the table map")
    parser.add_argument("--out", type=Path, help="inventory: the folder that receives bronze_request.json and bronze_inventory.json")
    parser.add_argument("--code-sha256", default=os.environ.get("CODE_SHA256", ""))
    parser.add_argument("--bucket", default=os.environ.get("DATA_BUCKET_NAME", ""))
    parser.add_argument("--request-sha256", default=os.environ.get("BRONZE_REQUEST_SHA256", ""), help="digest of the request the preflight checked")
    parser.add_argument("--inventory-sha256", default=os.environ.get("BRONZE_INVENTORY_SHA256", ""), help="digest of the staged source inventory")
    parser.add_argument("--dry-run", action="store_true", help="load: print and save the checked plan and write nothing (also set by BRONZE_DRY_RUN=1)")
    args = parser.parse_args(argv)
    if not RUN_ID.fullmatch(args.run):
        parser.error("the run ID must look like bronze_0a1b2c3d")
    work = args.work
    folder = work / "state" / args.run
    dry = args.dry_run or os.environ.get("BRONZE_DRY_RUN", "") in DRY_VALUES
    try:
        startup_check.check_phase("bronze_stage" if args.phase == "stage" else "bronze", args.phase)
        if args.phase in ENGINE_PHASES:
            task_capacity.apply()
        if not args.bucket:
            raise StepError("DATA_BUCKET_NAME (or --bucket) is not set")
        if args.phase == "inventory":
            if args.out is None:
                raise StepError("the inventory phase needs --out")
            stage_inputs(args.code, args.bucket, args.tables, args.target, dry, args.out, args.run)
            return 0
        if args.phase == "stage":
            stage_run(work / "code", args.bucket, args.run)
            return 0
        if args.phase == "pin":
            (folder / DRY_MARKER).unlink(missing_ok=True)
        elif (folder / DRY_MARKER).exists():
            dry = True
        target = args.target or os.environ.get("BRONZE_TARGET", "")
        requested = args.tables or os.environ.get("BRONZE_TABLES", "") or staged_tables(args.run, args.bucket, args.request_sha256)
        names = check_dispatch(requested, target, collections_of(table_map(work / "code")))
        job = Job(args.run, work, args.bucket, names, target, dry, args.request_sha256, args.inventory_sha256)
        if dry:
            job.folder.mkdir(parents=True, exist_ok=True)
            (job.folder / DRY_MARKER).write_text("this task is a dry run: no phase may write to the catalog or the bucket\n")
        dispatch(job, args.phase, args)
    except (
        StepError,
        DispatchError,
        bronze.BronzeError,
        task_capacity.CapacityError,
        startup_check.StartupCheckError,
        OSError,
        ValueError,
        KeyError,
        tarfile.TarError,
    ) as error:
        sys.stderr.write(f"bronze_run: step {args.phase} (run {args.run}) failed: {error}\n")
        phase_failure.emit_error(args.phase, error)
        return 1
    return 0


if __name__ == "__main__":
    resource_peaks.recorded(main)
