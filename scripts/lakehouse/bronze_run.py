"""Phase driver of the bronze ECS job that the job broker starts.

    python -m scripts.lakehouse.bronze_run <phase> --run <run ID> [--tables a,b] [--target real|qualify] [--work /workspace] [--dry-run]

Each ECS container of the job runs one phase on the shared ``/workspace`` (the container names of ``config/orchestration/ecs_broker_jobs.json``):
``pin``, ``load`` and ``check``. The first container of the task (``fetch``) has already downloaded and unpacked ``code.tar.gz``.

Two values change from run to run and arrive at dispatch: the tables (``BRONZE_TABLES``) and the target (``BRONZE_TARGET``). The broker checks them before it
starts the task. ``pin`` checks them again against the table map of the unpacked code, and ``load`` and ``check`` refuse values that differ from the pin.
The target ``real`` writes the namespaces ``bronze`` and ``bronze_dictionary``. The target ``qualify`` writes ``bronze_e2e`` and ``bronze_dictionary_e2e``
and takes the tables of one collection. No other namespace can be named.

``load`` runs the loader ``scripts.lakehouse.bronze``, which reads the versioned manifests of the acquisition bucket, checks every object against its recorded
version, SHA-256 and size, replaces each object's rows in place and publishes one metadata generation, so a retry of one run writes the same rows and no
second generation. ``check`` accepts the loader report: one report, a pass, the requested tables only. Any failed check exits 1 and names the phase.

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

from scripts.lakehouse import bronze, bronze_pin, provenance
from scripts.lakehouse.bronze_dispatch import NAMESPACES, DispatchError, check_dispatch
from scripts.process import TimeoutExpired, run_command

PHASES = ("pin", "load", "check")
REQUEST_FILE = "bronze_request.json"
INVENTORY_FILE = "bronze_inventory.json"
STAGE_KEY = "lakehouse/run_stage/{run}/inputs/{name}"
REQUEST_KEYS = {"tables", "target", "dry_run", "inventory_sha256"}
TABLE_MAP = Path("config/lakehouse/bronze_tables.json")
REPORT_FOLDER = bronze.REPORT_ROOT
STEP_SECONDS = 4 * 3600
RECORD = "bronze_run.json"
DRY_MARKER = "dry_run"
PLAN_FILE = "dry_run_plan.json"
PLAN_PREFIX = "DRY_RUN_PLAN "
RUN_ID = re.compile(r"[0-9]{8}T[0-9]{6}Z-[a-f0-9]{8}")
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
    """Bind the code, the request the broker checked and the source inventory of the run."""
    revision = archive_revision(archive, code_sha256)
    request_raw = read_staged(job.run, job.bucket, REQUEST_FILE)
    if not SHA256.fullmatch(job.request_sha256) or hashlib.sha256(request_raw).hexdigest() != job.request_sha256:
        raise StepError("the staged request differs from the one the broker checked")
    request = json.loads(request_raw)
    if not isinstance(request, dict) or set(request) != REQUEST_KEYS or type(request["dry_run"]) is not bool:
        raise StepError("the staged request needs exactly tables, target, dry_run and inventory_sha256")
    if tuple(request["tables"]) != job.tables or request["target"] != job.target or request["inventory_sha256"] != job.inventory_sha256:
        raise StepError("the staged request differs from the dispatch values")
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
    record |= {"request_sha256": job.request_sha256, "inventory_sha256": job.inventory_sha256}
    (job.folder / RECORD).write_text(json.dumps(record, indent=2, sort_keys=True) + "\n")
    sys.stdout.write(
        f"pinned bronze run {job.run}: target {job.target}, tables {','.join(job.tables)}, code {revision[:12]}, inventory {job.inventory_sha256[:12]}\n"
    )


def stage_inputs(code: Path, bucket: str, tables: str, target: str, dry_run: bool, out: Path) -> tuple[str, str]:
    """Owner side: list the current sources once and write the inventory and the request that names its digest. Stage both beside the code archive."""
    config = table_map(code)
    names = check_dispatch(tables, target, collections_of(config))
    inventory = bronze_pin.build_inventory(owner_storage(), bucket, config, list(names), bronze.load_retired(code / bronze.RETIRED))
    bronze_pin.require_objects(inventory)
    raw = bronze_pin.inventory_bytes(inventory)
    digest = hashlib.sha256(raw).hexdigest()
    request = json.dumps({"tables": list(names), "target": target, "dry_run": dry_run, "inventory_sha256": digest}, sort_keys=True).encode()
    out.mkdir(parents=True, exist_ok=True)
    (out / INVENTORY_FILE).write_bytes(raw)
    (out / REQUEST_FILE).write_bytes(request)
    sys.stdout.write(f"staged {len(inventory['objects'])} objects from {len(inventory['manifests'])} manifests: inventory {digest}\n")
    return hashlib.sha256(request).hexdigest(), digest


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
    arguments += ["--inventory", str(job.folder / INVENTORY_FILE)]
    if job.target == "qualify":
        arguments.append("--selected-metadata")
    run_module(job, "scripts.lakehouse.bronze", arguments)


def accept_report(job: Job) -> None:
    """The loader wrote exactly one report, it passed, and it names the requested tables and no others."""
    reports = sorted((job.code / REPORT_FOLDER).glob("report_*.json"))
    if len(reports) != 1:
        raise StepError(f"expected one loader report, found {len(reports)}")
    report = json.loads(reports[0].read_text())
    if report.get("mode") != "load" or report.get("passed") is not True:
        raise StepError("the loader report did not pass")
    if sorted(report.get("tables", [])) != sorted(job.tables):
        raise StepError("the loader report names other tables than the dispatch")
    checks = report.get("checks", {})
    if not set(checks) <= set(job.tables) or any(check.get("passed") is not True for check in checks.values()):
        raise StepError("a table check of the loader report did not pass")


def check(job: Job) -> None:
    accept_report(job)
    sys.stdout.write(f"bronze run {job.run} passed: {','.join(job.tables)} in {' and '.join(job.namespaces)}\n")
    if job.target == "real":
        conf = json.dumps({"run_id": job.run, "mode": "real"})
        sys.stdout.write(f"to start the chain by hand: airflow dags trigger bronze_chain_dag --conf '{conf}'\n")
    else:
        sys.stdout.write("this was a qualification run in the test twins: the chain reads production bronze, so do not start it\n")


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
    parser.add_argument("phase", choices=(*PHASES, "inventory"))
    parser.add_argument("--run", required=True)
    parser.add_argument("--tables", default="", help="comma-separated tables (also BRONZE_TABLES)")
    parser.add_argument("--target", default="", help="real or qualify (also BRONZE_TARGET)")
    parser.add_argument("--work", type=Path, default=Path("/workspace"))
    parser.add_argument("--archive", type=Path)
    parser.add_argument("--code", type=Path, default=Path("."), help="inventory: the unpacked repository that holds the table map")
    parser.add_argument("--out", type=Path, help="inventory: the folder that receives bronze_request.json and bronze_inventory.json")
    parser.add_argument("--code-sha256", default=os.environ.get("CODE_SHA256", ""))
    parser.add_argument("--bucket", default=os.environ.get("DATA_BUCKET_NAME", ""))
    parser.add_argument("--request-sha256", default=os.environ.get("BRONZE_REQUEST_SHA256", ""), help="digest of the request the broker checked")
    parser.add_argument("--inventory-sha256", default=os.environ.get("BRONZE_INVENTORY_SHA256", ""), help="digest of the staged source inventory")
    parser.add_argument("--dry-run", action="store_true", help="load: print and save the checked plan and write nothing (also set by BRONZE_DRY_RUN=1)")
    args = parser.parse_args(argv)
    if not RUN_ID.fullmatch(args.run):
        parser.error("the run ID must look like 20261030T010203Z-0a1b2c3d")
    work = args.work
    folder = work / "state" / args.run
    dry = args.dry_run or os.environ.get("BRONZE_DRY_RUN", "") in DRY_VALUES
    try:
        if not args.bucket:
            raise StepError("DATA_BUCKET_NAME (or --bucket) is not set")
        if args.phase == "inventory":
            if args.out is None:
                raise StepError("the inventory phase needs --out")
            stage_inputs(args.code, args.bucket, args.tables, args.target, dry, args.out)
            return 0
        if args.phase == "pin":
            (folder / DRY_MARKER).unlink(missing_ok=True)
        elif (folder / DRY_MARKER).exists():
            dry = True
        target = args.target or os.environ.get("BRONZE_TARGET", "")
        names = check_dispatch(args.tables or os.environ.get("BRONZE_TABLES", ""), target, collections_of(table_map(work / "code")))
        job = Job(args.run, work, args.bucket, names, target, dry, args.request_sha256, args.inventory_sha256)
        if dry:
            job.folder.mkdir(parents=True, exist_ok=True)
            (job.folder / DRY_MARKER).write_text("this task is a dry run: no phase may write to the catalog or the bucket\n")
        dispatch(job, args.phase, args)
    except (StepError, DispatchError, bronze.BronzeError, OSError, ValueError, KeyError, tarfile.TarError) as error:
        sys.stderr.write(f"bronze_run: step {args.phase} (run {args.run}) failed: {error}\n")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
