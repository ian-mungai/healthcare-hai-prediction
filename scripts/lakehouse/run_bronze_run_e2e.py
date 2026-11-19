"""E2E of the bronze job driver with stand-ins for the S3 bucket, the Spark loader and the catalog (no Docker, no AWS).

    .venv/bin/python -m scripts.lakehouse.run_bronze_run_e2e [--report data/e2e/bronze_run/<stamp>/report.json]

The driver (``scripts/lakehouse/bronze_run.py``) runs for real through ``main``: the dispatch checks, the code archive digest, the real discovery of the
versioned manifests, the hash and size checks of the dry run, the loader command and the acceptance check of the loader report. The stand-ins replace
only the S3 client and the loader module (which needs Spark and the catalog). Every write to the shared catalog goes through ``shared_write``, so a
case can prove what a run wrote, and where.
"""

from __future__ import annotations

import argparse
import ast
import contextlib
import hashlib
import io
import json
import os
import sys
import tarfile
import tempfile
import traceback
from collections.abc import Callable, Iterator, Sequence
from datetime import UTC, datetime
from functools import partial
from pathlib import Path
from typing import Any
from unittest import mock

from botocore.exceptions import ClientError

from orchestration import raw_lineage
from scripts.lakehouse import bronze, bronze_metadata, bronze_mirror, bronze_pin, bronze_run
from scripts.quality import lock_env

REVISION = "0123456789abcdef0123456789abcdef01234567"
RUN = "bronze_0a1b2c3d"
BUCKET = "hai-test-bucket"
TABLES = [
    {"table": "alpha_data", "collection": "org_a/set_one", "format": "csv", "dataset_ids": ["alpha"]},
    {"table": "beta_data", "collection": "org_a/set_one", "format": "csv", "dataset_ids": ["beta"]},
    {"table": "gamma_data", "collection": "org_b/set_two", "format": "csv", "dataset_ids": ["gamma"]},
    {"table": "empty_data", "collection": "org_c/set_three", "format": "csv", "dataset_ids": ["empty"]},
]
CATALOG: list[Path] = []
DEFECTS: set[str] = set()
LOADS: list[list[str]] = []
CLOCK = [0]
STORAGE: list[FakeStorage] = []
STAGED: dict[str, bytes] = {}


class FakeClient:
    """The S3 client of the check phase's records: no ingestion run record exists in these cases. A write that carries ``IfNoneMatch`` is refused when the key
    exists, as the bucket does, and a key that was never written answers 403, because the task role cannot list its folders."""

    def __init__(self) -> None:
        self.puts: dict[str, bytes] = {}

    def list_object_versions(self, **arguments: str) -> dict[str, Any]:
        return {"Versions": []}

    def put_object(self, **arguments: Any) -> dict[str, Any]:
        if arguments.get("IfNoneMatch") == "*" and arguments["Key"] in self.puts:
            raise ClientError({"Error": {"Code": "PreconditionFailed", "Message": "exists"}}, "PutObject")
        self.puts[arguments["Key"]] = arguments["Body"]
        return {}

    def get_object(self, **arguments: Any) -> dict[str, Any]:
        if arguments["Key"] not in self.puts:
            raise ClientError({"Error": {"Code": "AccessDenied", "Message": "no such key"}}, "GetObject")
        return {"Body": io.BytesIO(self.puts[arguments["Key"]])}

    def list_objects_v2(self, **arguments: Any) -> dict[str, Any]:
        """The owner's listing of one run's attempt records (the task role cannot list)."""
        return {"Contents": [{"Key": key} for key in sorted(self.puts) if key.startswith(arguments["Prefix"])], "IsTruncated": False}


class Publisher:
    """The notice service: every publish call, kept."""

    def __init__(self) -> None:
        self.calls: list[dict[str, str]] = []

    def publish(self, **arguments: str) -> dict[str, str]:
        self.calls.append(arguments)
        return {"MessageId": f"m{len(self.calls)}"}


class FakeStorage:
    """Versioned objects in memory: list current keys, get or download one exact version."""

    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], bytes] = {}
        self.client = FakeClient()

    def put(self, key: str, data: bytes) -> str:
        version = f"v-{hashlib.sha256(key.encode() + data).hexdigest()[:12]}"
        self.objects[(key, version)] = data
        return version

    def list_keys(self, bucket: str, prefix: str) -> list[tuple[str, str]]:
        require(bucket == BUCKET, bucket)
        return sorted((key, version) for key, version in self.objects if key.startswith(prefix))

    def list_current(self, bucket: str, prefix: str) -> list[tuple[str, str]]:
        return self.list_keys(bucket, prefix)

    def get(self, bucket: str, key: str, version_id: str) -> bytes:
        return self.objects[(key, version_id)]

    def download(self, bucket: str, key: str, version_id: str, path: Path) -> None:
        path.write_bytes(self.objects[(key, version_id)])


def require(condition: bool, evidence: object = "") -> None:
    if not condition:
        raise AssertionError(evidence)


def sha_of(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def shared_write(kind: str, **detail: Any) -> None:
    with (CATALOG[0] / "writes.log").open("a") as handle:
        handle.write(json.dumps({"kind": kind, **detail}, sort_keys=True) + "\n")


def writes() -> list[dict[str, Any]]:
    file = CATALOG[0] / "writes.log"
    return [json.loads(line) for line in file.read_text().splitlines()] if file.exists() else []


def catalog() -> dict[str, dict[str, str]]:
    file = CATALOG[0] / "tables.json"
    return json.loads(file.read_text()) if file.exists() else {}


def store(storage: FakeStorage, collection: str, dataset: str, snapshot: str, data: bytes) -> tuple[str, str]:
    """Store one data object and its manifest the way the acquisition storage step does; return the object key and version."""
    digest = hashlib.sha256(data).hexdigest()
    key = f"{collection}/datasets/{dataset}/release_date=2026-01-01/{digest}/{dataset}.csv"
    version = storage.put(key, data)
    entry = {"role": "data", "dataset_id": dataset, "object": {"bucket": BUCKET, "key": key, "version_id": version, "sha256": digest, "byte_count": len(data)}}
    receipt = {"role": "capture_receipt", "dataset_id": dataset, "object": {"bucket": BUCKET, "key": f"{collection}/manifests/{snapshot}/receipt.json"}}
    manifest = {"snapshot_id": snapshot, "source_id": dataset.upper(), "dataset_id": dataset, "release_id": "2026-01-01", "objects": [entry, receipt]}
    storage.put(f"{collection}/manifests/release_date=2026-01-01/{snapshot}/{dataset}/manifest.json", json.dumps(manifest).encode())
    return key, version


def archive(folder: Path) -> tuple[Path, str]:
    """A ``git archive`` style tarball with the table map, and the same files unpacked in ``code`` as the fetch container leaves them."""
    source = folder / "src"
    (source / "config/lakehouse").mkdir(parents=True)
    (source / "config/lakehouse/bronze_tables.json").write_text(json.dumps({"version": 1, "namespace": "bronze", "tables": TABLES}))
    path = folder / "code.tar.gz"
    with tarfile.open(path, "w:gz", format=tarfile.PAX_FORMAT, pax_headers={"comment": REVISION}) as tar:
        tar.add(source / "config", arcname="config")
    return path, sha_of(path)


def stand_in_mirror(namespace: str, names: list[str]) -> dict[str, Any]:
    """The ``mirror`` section of the loader report: a real run's tables are registered, the qualification twin mirrors nothing, a failure is reported."""
    if "mirror_malformed" in DEFECTS:
        return {"status": "great", "tables": names}
    if namespace != "bronze":
        return {"status": "skipped", "reason": "mode qualify is a test mode", "tables": {}}
    if "mirror_failed" in DEFECTS:
        reason = "alpha_data failed: AccessDeniedException from Glue"
        return {"status": "failed", "reason": reason, "tables": {names[0]: "failed: AccessDeniedException from Glue", **dict.fromkeys(names[1:], "created")}}
    return {"status": "ok", "tables": dict.fromkeys(names, "created")}


def loader(job: bronze_run.Job, module: str, arguments: Sequence[str | Path], environment: dict[str, str] | None = None) -> None:
    """Stand-in for ``scripts.lakehouse.bronze``: it discovers the real inputs, replaces each object's rows in place and writes the counts-only report."""
    require(module == "scripts.lakehouse.bronze", module)
    values = [str(item) for item in arguments]
    LOADS.append(values)
    if "loader_fails" in DEFECTS:
        raise bronze_run.StepError("scripts.lakehouse.bronze exited 1")
    namespace, dictionary = values[values.index("--namespace") + 1], values[values.index("--dictionary-namespace") + 1]
    require("--run-id" in values and values[values.index("--run-id") + 1] == job.run, f"the loader is not told the bronze run: {values}")
    names = values[values.index("--tables") + 1].split(",")
    config = bronze.load_table_map(job.code / "config/lakehouse/bronze_tables.json")
    source: Any = STORAGE[0]
    if "--inventory" in values:
        inventory = bronze_pin.read_inventory(Path(values[values.index("--inventory") + 1]).read_bytes())
        source = bronze_pin.PinnedStorage(source, inventory)
    inputs, _ = bronze.discover(source, BUCKET, config, names, bronze.RetiredObjects())
    if "--inventory" in values:
        bronze_pin.verify_replay(inventory, inputs)
    unsourced = values[values.index("--unsourced-tables") + 1].split(",") if "--unsourced-tables" in values else []
    bronze.check_unsourced(source, BUCKET, config, names, unsourced, [], bronze.RetiredObjects())
    tables = catalog()
    for item in inputs:
        tables.setdefault(f"{namespace}.{item['table']}", {})[item["object_key"]] = item["sha256"]
        shared_write("table", table=f"{namespace}.{item['table']}", object_key=item["object_key"])
    for name in names:
        tables.setdefault(f"{dictionary}.{name}", {"dictionary": "rows"})
        shared_write("dictionary", table=f"{dictionary}.{name}")
    (CATALOG[0] / "tables.json").write_text(json.dumps(tables, sort_keys=True))
    checks = {name: {"passed": "report_failed" not in DEFECTS, "objects_match_inputs": True} for name in names}
    shown = [name for name in names if name != "beta_data"] if "report_missing_table" in DEFECTS else names
    snapshots = {name: 100 + position for position, name in enumerate(names)}
    CLOCK[0] += 1
    stamp = f"20261118T23{CLOCK[0]:02d}00Z"
    report = {"run_at_utc": stamp, "mode": "load", "tables": shown, "inputs": len(inputs), "checks": checks, "passed": "report_failed" not in DEFECTS}
    report["unsourced_tables"] = sorted(unsourced)
    report |= {"snapshots": snapshots, "metadata_index": {"generation": "1", "path": "stand_in", "sha256": "0" * 64}}
    report["mirror"] = stand_in_mirror(namespace, names)
    folder = job.code / bronze_run.REPORT_FOLDER
    folder.mkdir(parents=True, exist_ok=True)
    if "no_report" not in DEFECTS:
        (folder / f"report_{stamp}.json").write_text(json.dumps(report, sort_keys=True))
    sys.stdout.write(json.dumps(report, sort_keys=True) + "\n")


class Case:
    """One workspace: its own catalog model, bucket and code archive."""

    def __init__(self, root: Path) -> None:
        self.root = root
        CATALOG[:] = [root / "catalog"]
        CATALOG[0].mkdir()
        DEFECTS.clear()
        LOADS.clear()
        STAGED.clear()  # what an earlier case staged is not the staging of this one
        self.storage = FakeStorage()
        STORAGE[:] = [self.storage]
        self.archive, self.digest = archive(root)
        self.objects = {
            "alpha_data": store(self.storage, "org_a/set_one", "alpha", "snap_a", b"id,value\n1,a\n2,b\n"),
            "beta_data": store(self.storage, "org_a/set_one", "beta", "snap_b", b"id,value\n3,c\n"),
            "gamma_data": store(self.storage, "org_b/set_two", "gamma", "snap_c", b"id,value\n4,d\n5,e\n6,f\n"),
        }
        self.out = ""
        self.err = ""
        self.shas: tuple[str, str] | None = None
        self.key: tuple[str, str, bool] | None = None

    def stage(self, tables: str, target: str, dry: bool) -> tuple[str, str]:
        """What the owner stages and the launch preflight checks: the inventory of the manifest versions and the request that names its digest."""
        self.key = (tables, target, dry)
        names = [name for name in tables.split(",") if name]
        config = bronze.load_table_map(self.root / "src/config/lakehouse/bronze_tables.json")
        try:
            inventory = bronze_pin.build_inventory(STORAGE[0], BUCKET, config, names)
        except (bronze.BronzeError, KeyError):
            inventory = {"version": 1, "tables": [], "manifests": [], "objects": [], "documents": []}
        raw = bronze_pin.inventory_bytes(inventory)
        STAGED["bronze_inventory.json"] = raw
        request: dict[str, Any] = {"tables": names, "target": target, "dry_run": dry, "inventory_sha256": hashlib.sha256(raw).hexdigest(), "skipped_tables": []}
        STAGED["bronze_request.json"] = json.dumps(request).encode()
        return hashlib.sha256(STAGED["bronze_request.json"]).hexdigest(), request["inventory_sha256"]

    def work(self, label: str = "w") -> Path:
        path = self.root / label
        (path / "code").mkdir(parents=True, exist_ok=True)
        if not (path / "code.tar.gz").exists():
            (path / "code.tar.gz").write_bytes(self.archive.read_bytes())
            with tarfile.open(path / "code.tar.gz") as tar:
                tar.extractall(path / "code", filter="data")
        return path

    def phase(
        self,
        phase: str,
        *,
        label: str = "w",
        tables: str = "alpha_data,beta_data",
        target: str = "real",
        digest: str | None = None,
        extra: Sequence[str] = (),
        environment: dict[str, str] | None = None,
        staged: bool = False,
    ) -> int:
        environment = environment or {}
        dry = "--dry-run" in extra or environment.get("BRONZE_DRY_RUN", "") in ("1", "true")
        key = (tables or environment.get("BRONZE_TABLES", ""), target or environment.get("BRONZE_TARGET", ""), dry)
        if not staged and (self.shas is None or (phase == "pin" and self.key != key)):
            self.shas = self.stage(*key)
        request_sha, inventory_sha = self.shas or ("", "")
        arguments = [phase, "--run", RUN, "--work", str(self.work(label)), "--code-sha256", digest or self.digest, "--bucket", BUCKET]
        arguments += ["--tables", tables, "--target", target, "--request-sha256", request_sha, "--inventory-sha256", inventory_sha, *extra]
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err), mock.patch.dict(os.environ, environment):
            try:
                code = bronze_run.main(arguments)
            except SystemExit as stop:
                code = int(stop.code or 0)
        self.out += out.getvalue()
        self.err += err.getvalue()
        return code

    def chain(self, *, label: str = "w", **options: Any) -> list[int]:
        codes = []
        for phase in bronze_run.PHASES:
            codes.append(self.phase(phase, label=label, **options))
            if codes[-1]:
                break
        return codes


def staged_object(run: str, bucket: str, name: str) -> bytes:
    """One staged input as the task role reads it: a key that was never staged answers 403, because the role cannot list the folder."""
    if name not in STAGED:
        raise ClientError({"Error": {"Code": "AccessDenied", "Message": "no such staged input"}}, "GetObject")
    return STAGED[name]


@contextlib.contextmanager
def patched(root: Path) -> Iterator[Case]:
    with (
        mock.patch.object(bronze_run, "run_module", loader),
        mock.patch.object(bronze_run, "storage", lambda: STORAGE[0]),
        mock.patch.object(bronze_run, "owner_storage", lambda: STORAGE[0]),
        mock.patch.object(bronze_run, "read_staged", staged_object),
        mock.patch.dict(
            os.environ, {"BRONZE_DRY_RUN": "", "BRONZE_TABLES": "", "BRONZE_TARGET": "", "BRONZE_REQUEST_SHA256": "", "BRONZE_INVENTORY_SHA256": ""}
        ),
    ):
        yield Case(root)


def case_real_run_writes_only_bronze(case: Case) -> None:
    require(case.chain() == [0, 0, 0], case.err)
    require(len(LOADS) == 1, LOADS)
    arguments = LOADS[0]
    require(arguments[arguments.index("--namespace") + 1] == "bronze", arguments)
    require(arguments[arguments.index("--dictionary-namespace") + 1] == "bronze_dictionary", arguments)
    require(arguments[arguments.index("--tables") + 1] == "alpha_data,beta_data" and "--task-role" in arguments, arguments)
    require(arguments[arguments.index("--bucket") + 1] == BUCKET, arguments)
    require(set(catalog()) == {"bronze.alpha_data", "bronze.beta_data", "bronze_dictionary.alpha_data", "bronze_dictionary.beta_data"}, sorted(catalog()))
    require("bronze_chain_dag" in case.out and RUN in case.out, "the success line names the chain trigger for this run")
    record = json.loads(STORAGE[0].client.puts[f"lakehouse/bronze_runs/{RUN}/check.json"])
    require("mirror" not in record, f"the load receipt carries the mirror outcome: {record}")
    raw_lineage.loaded_objects(record, "check.json", RUN)
    latest = attempts()[-1]
    require(
        latest["mirror"] == {"status": "ok", "tables": {"alpha_data": "created", "beta_data": "created"}}, f"the attempt record lacks the outcome: {latest}"
    )
    require(latest["bronze_run"] == RUN and latest["metadata_generation"] == "1", f"the attempt is not bound to the run and the generation: {latest}")
    require("glue mirror ok" in case.out, "the check phase does not show the mirror outcome")


def attempts() -> list[dict[str, Any]]:
    """The attempt records of the run as the owner reads them, oldest first."""
    return bronze_mirror.attempts(STORAGE[0].client, BUCKET, RUN)


def case_a_failed_glue_mirror_leaves_the_run_passed_and_is_recorded_and_shown(case: Case) -> None:
    """The load is committed before the mirror runs: a Glue failure fails no phase, is kept as an attempt record and is shown by name."""
    DEFECTS.add("mirror_failed")
    require(case.chain() == [0, 0, 0], case.err)
    record = json.loads(STORAGE[0].client.puts[f"lakehouse/bronze_runs/{RUN}/check.json"])
    latest = attempts()[-1]["mirror"]
    require(latest["status"] == "failed" and "AccessDeniedException" in latest["reason"], f"{latest}")
    require(record["snapshots"] == {"alpha_data": 100, "beta_data": 101} and "bronze_chain_dag" in case.out, "the failed mirror held back the committed run")
    require("glue mirror FAILED" in case.err and "alpha_data" in case.err, f"the failed mirror is not shown: {case.err}")
    raw_lineage.loaded_objects(record, "check.json", RUN)


def retry(case: Case, defect: str | None) -> list[int]:
    """The same run again: a fresh workspace, the same bucket and the same run ID. The bucket keeps the load receipt of the first attempt."""
    DEFECTS.clear()
    if defect:
        DEFECTS.add(defect)
    case.shas = None
    return case.chain(label="retry")


def case_a_retry_failing_the_mirror_after_a_success_keeps_the_receipt(case: Case) -> None:
    require(case.chain() == [0, 0, 0], case.err)
    receipt = STORAGE[0].client.puts[f"lakehouse/bronze_runs/{RUN}/check.json"]
    require(retry(case, "mirror_failed") == [0, 0, 0], case.err)
    require(STORAGE[0].client.puts[f"lakehouse/bronze_runs/{RUN}/check.json"] == receipt, "the retry replaced the load receipt")
    found = attempts()
    require([item["mirror"]["status"] for item in found] == ["ok", "failed"], f"{[item['mirror']['status'] for item in found]}")
    latest = bronze_mirror.latest_attempt(STORAGE[0].client, BUCKET, RUN)
    require(latest is not None and latest["mirror"]["status"] == "failed", "the latest attempt is not the failed retry")
    require("glue mirror FAILED" in case.err, "the failed retry is not shown")


def case_a_retry_repairing_the_mirror_keeps_the_receipt(case: Case) -> None:
    DEFECTS.add("mirror_failed")
    require(case.chain() == [0, 0, 0], case.err)
    receipt = STORAGE[0].client.puts[f"lakehouse/bronze_runs/{RUN}/check.json"]
    require(retry(case, None) == [0, 0, 0], case.err)
    require(STORAGE[0].client.puts[f"lakehouse/bronze_runs/{RUN}/check.json"] == receipt, "the retry replaced the load receipt")
    found = attempts()
    require([item["mirror"]["status"] for item in found] == ["failed", "ok"], f"{[item['mirror']['status'] for item in found]}")
    latest = bronze_mirror.latest_attempt(STORAGE[0].client, BUCKET, RUN)
    require(latest is not None and latest["mirror"]["tables"] == {"alpha_data": "created", "beta_data": "created"}, "the repair is not the latest")
    require(found[0]["report_sha256"] != found[1]["report_sha256"], "two attempts share one record")


def case_a_repeated_check_of_the_same_report_makes_no_second_attempt_record(case: Case) -> None:
    require(case.chain() == [0, 0, 0], case.err)
    require(case.phase("check") == 0, case.err)
    require(len(attempts()) == 1, f"a repeated check wrote {len(attempts())} attempt records")


def case_an_attempt_record_that_cannot_be_written_fails_no_phase_and_is_named(case: Case) -> None:
    """A code refresh can come before the IAM apply that grants the new key: the run stays committed and the missing record is shown."""
    client = STORAGE[0].client
    original = client.put_object

    def refuse(**arguments: Any) -> dict[str, Any]:
        if "/mirror_" in arguments["Key"]:
            raise ClientError({"Error": {"Code": "AccessDenied", "Message": "no grant"}}, "PutObject")
        return original(**arguments)

    with mock.patch.object(client, "put_object", refuse):
        require(case.chain() == [0, 0, 0], case.err)
    require(f"lakehouse/bronze_runs/{RUN}/check.json" in client.puts and "bronze_chain_dag" in case.out, "the run was held back")
    require("mirror attempt record was not written" in case.err and "AccessDenied" in case.err, f"the missing record is not named: {case.err}")


def case_a_loader_report_with_a_malformed_mirror_outcome_is_refused(case: Case) -> None:
    DEFECTS.add("mirror_malformed")
    require(case.chain() == [0, 0, 1], case.err)
    require(
        "mirror" in case.err and f"lakehouse/bronze_runs/{RUN}/check.json" not in STORAGE[0].client.puts,
        f"a record was written for a report with a bad mirror: {case.err}",
    )


def case_the_stage_phase_writes_the_inputs_the_chain_then_runs_on(case: Case) -> None:
    """The stage job writes the request and the inventory under the task role, as a real load: pin, load and check accept them."""
    require(case.phase("stage", staged=True) == 0, case.err)
    require("empty_data" in case.out, "the stage did not name the table it left out")
    stem = f"lakehouse/run_stage/{RUN}/inputs/"
    puts = STORAGE[0].client.puts
    wanted = [f"{stem}{name}" for name in (bronze_run.INVENTORY_FILE, bronze_run.REQUEST_FILE, bronze_run.STAGE_REPORT_FILE)]
    require(sorted(puts) == sorted(wanted), f"the stage wrote {sorted(puts)}")
    report = json.loads(puts[f"{stem}{bronze_run.STAGE_REPORT_FILE}"])
    require(report["skipped_tables"] == [{"table": "empty_data", "reason": bronze_run.SKIPPED_REASON}] and report["skipped_count"] == 1, f"{report}")
    require("skipped_tables 1: empty_data" in case.out, "the stage output does not count the skipped table")
    request = json.loads(puts[f"{stem}{bronze_run.REQUEST_FILE}"])
    tables = [entry["table"] for entry in TABLES if entry["table"] != "empty_data"]  # a table with no source object is left out
    require(request["tables"] == tables and request["target"] == "real" and request["dry_run"] is False, f"{request}")
    STAGED[bronze_run.INVENTORY_FILE], STAGED[bronze_run.REQUEST_FILE] = puts[f"{stem}{bronze_run.INVENTORY_FILE}"], puts[f"{stem}{bronze_run.REQUEST_FILE}"]
    STAGED[bronze_run.STAGE_REPORT_FILE] = puts[f"{stem}{bronze_run.STAGE_REPORT_FILE}"]
    case.shas = (hashlib.sha256(STAGED[bronze_run.REQUEST_FILE]).hexdigest(), hashlib.sha256(STAGED[bronze_run.INVENTORY_FILE]).hexdigest())
    case.key = (",".join(tables), "real", False)
    topic = "arn:aws:sns:us-west-2:" + "1" * 12 + ":alerts"
    notices = Publisher()
    with mock.patch.dict(os.environ, {"ALERTS_TOPIC_ARN": topic}), mock.patch("boto3.client", lambda *_, **__: notices):
        require(case.chain(tables=",".join(tables)) == [0, 0, 0], case.err)
        require(case.phase("check", tables=",".join(tables)) == 0, f"a repeated check failed: {case.err}")
    require(len(LOADS) == 1 and LOADS[0][LOADS[0].index("--tables") + 1] == ",".join(tables), LOADS)
    require(LOADS[0][LOADS[0].index("--unsourced-tables") + 1] == "empty_data", f"the loader was not told which table the stage left out: {LOADS[0]}")
    record = json.loads(STORAGE[0].client.puts[f"lakehouse/bronze_runs/{RUN}/check.json"])
    require(record["skipped_tables"] == report["skipped_tables"], f"the check record does not name the skipped table: {record['skipped_tables']}")
    claim = json.loads(STORAGE[0].client.puts[f"lakehouse/bronze_runs/{RUN}/carried_over.json"])
    require(claim["skipped_tables"] == report["skipped_tables"], f"the carried-over record does not name the skipped table: {claim}")
    require(len(notices.calls) == 1 and "empty_data" in notices.calls[0]["Message"], f"the owner notice: {notices.calls}")
    outcome = json.loads(STORAGE[0].client.puts[f"lakehouse/bronze_runs/{RUN}/carried_over_notice.json"])
    require(outcome["notice"] == "sent", f"{outcome}")


def case_a_dropped_table_with_sources_is_refused(case: Case) -> None:
    """The stage report sits beside the request and is written by the same stage, so a request and report that agree on dropping ``beta_data`` (whose
    manifests the pinned inventory holds through ``alpha_data``'s collection) must not run. The request names the skipped table under its digest, and the
    pin lists the sources of the skipped tables again with the task identity: it finds ``beta_data``'s object and refuses, before the load."""
    case.shas = case.stage("alpha_data", "real", False)
    request = json.loads(STAGED["bronze_request.json"]) | {"skipped_tables": ["beta_data"]}
    STAGED["bronze_request.json"] = json.dumps(request, sort_keys=True).encode()
    case.shas = (hashlib.sha256(STAGED["bronze_request.json"]).hexdigest(), case.shas[1])
    report = {"schema": 1, "bronze_run": RUN, "requested_tables": ["alpha_data"], "skipped_count": 1}
    STAGED[bronze_run.STAGE_REPORT_FILE] = json.dumps(report | {"skipped_tables": [{"table": "beta_data", "reason": bronze_run.SKIPPED_REASON}]}).encode()
    codes = case.chain(tables="alpha_data", staged=True)
    require(codes == [1], f"{codes} {case.err}")
    require("beta_data" in case.err and "cannot be left out" in case.err, case.err)
    require(not (case.work() / "state" / RUN / bronze_run.RECORD).exists() and LOADS == [], "a refused pin bound the run or a loader ran")
    require(not any(name.startswith("bronze.") for name in catalog()), f"the refused run wrote bronze tables: {sorted(catalog())}")


def case_a_changed_request_or_report_is_refused(case: Case) -> None:
    """The stage skips ``empty_data``. A request that changes after the preflight bound its digest stops the pin, and a report that is changed alone (to
    hide the skipped table) stops the load and the check phases, which compare it with the request."""
    require(case.phase("stage", staged=True) == 0, case.err)
    stem = f"lakehouse/run_stage/{RUN}/inputs/"
    puts = STORAGE[0].client.puts
    for name in (bronze_run.INVENTORY_FILE, bronze_run.REQUEST_FILE, bronze_run.STAGE_REPORT_FILE):
        STAGED[name] = puts[f"{stem}{name}"]
    tables = ",".join(entry["table"] for entry in TABLES if entry["table"] != "empty_data")
    case.shas = (hashlib.sha256(STAGED[bronze_run.REQUEST_FILE]).hexdigest(), hashlib.sha256(STAGED[bronze_run.INVENTORY_FILE]).hexdigest())
    case.key = (tables, "real", False)
    request = json.loads(STAGED[bronze_run.REQUEST_FILE])
    STAGED[bronze_run.REQUEST_FILE] = json.dumps(request | {"skipped_tables": []}, sort_keys=True).encode()
    require(case.phase("pin", tables=tables, staged=True) == 1 and "differs from the one the preflight checked" in case.err, case.err)
    STAGED[bronze_run.REQUEST_FILE] = puts[f"{stem}{bronze_run.REQUEST_FILE}"]
    case.err = ""
    require(case.phase("pin", tables=tables, staged=True) == 0, case.err)
    report = json.loads(STAGED[bronze_run.STAGE_REPORT_FILE]) | {"skipped_tables": [], "skipped_count": 0}
    STAGED[bronze_run.STAGE_REPORT_FILE] = json.dumps(report).encode()
    case.err = ""
    require(case.phase("load", tables=tables, staged=True) == 1 and "stage report" in case.err and LOADS == [], f"{case.err} {LOADS}")


def case_a_table_with_only_a_rejected_capture_is_not_left_out(case: Case) -> None:
    """Review round 2 (Medium). ``empty_data`` has a stored object whose manifest was rejected: no load takes it, but its failure observation belongs in the
    generation. The stage must refuse by name instead of leaving the table out, and stage nothing."""
    key, version = store(case.storage, "org_c/set_three", "empty", "snap_empty", b"id,value\n7,failed\n")
    manifest_key, manifest_version = next((name, found) for name, found in case.storage.objects if name.startswith("org_c/set_three/manifests/"))
    manifest = json.loads(case.storage.objects.pop((manifest_key, manifest_version)))
    manifest["snapshot_status"] = "rejected"
    case.storage.put(manifest_key, json.dumps(manifest).encode())
    config = bronze.load_table_map(case.root / "src/config/lakehouse/bronze_tables.json")
    names = [entry["table"] for entry in config["tables"]]
    inputs, excluded = bronze.discover(case.storage, BUCKET, config, names, bronze.RetiredObjects())
    kept = bronze_metadata.build(names, inputs, excluded, [], names)
    require(any(row["table_name"] == "empty_data" and row["failed_attempt"] for row in kept), "the complete generation does not keep the failed observation")
    require(case.phase("stage", staged=True) != 0 and "empty_data" in case.err and "rejected, retired or superseded" in case.err, case.err)
    require(not STORAGE[0].client.puts and (key, version) in case.storage.objects, f"a refused stage wrote {sorted(STORAGE[0].client.puts)}")


def case_the_owner_stages_a_partial_run_that_names_the_absent_table_and_every_phase_accepts_it(case: Case) -> None:
    """Review round 2 (Medium). After a generation that skipped ``empty_data``, a hand-started partial reload of ``alpha_data`` needs the absence in its bound
    request and report. The owner ``inventory`` phase finds it (it has no source object) and leaves ``beta_data`` and ``gamma_data`` (which have) alone."""
    out = case.root / "staged_partial"
    arguments = ["inventory", "--run", RUN, "--code", str(case.root / "src"), "--bucket", BUCKET, "--tables", "alpha_data", "--target", "real"]
    require(bronze_run.main([*arguments, "--out", str(out)]) == 0, "the inventory phase failed")
    request = json.loads((out / bronze_run.REQUEST_FILE).read_text())
    require(request["tables"] == ["alpha_data"] and request["skipped_tables"] == ["empty_data"], f"{request}")
    for name in (bronze_run.REQUEST_FILE, bronze_run.INVENTORY_FILE, bronze_run.STAGE_REPORT_FILE):
        STAGED[name] = (out / name).read_bytes()
    case.shas = (hashlib.sha256(STAGED[bronze_run.REQUEST_FILE]).hexdigest(), hashlib.sha256(STAGED[bronze_run.INVENTORY_FILE]).hexdigest())
    case.key = ("alpha_data", "real", False)
    require(case.chain(tables="alpha_data", staged=True) == [0, 0, 0], case.err)
    require(LOADS[0][LOADS[0].index("--unsourced-tables") + 1] == "empty_data", f"the loader was not told the absence: {LOADS[0]}")


def case_the_phases_read_their_tables_from_the_staged_request(case: Case) -> None:
    """The launch passes no table list (it would not fit the RunTask overrides of a full run). With no ``--tables`` and no ``BRONZE_TABLES`` each phase takes
    the tables from the staged request, after checking its bytes against ``BRONZE_REQUEST_SHA256``."""
    case.shas = case.stage("alpha_data,beta_data", "real", False)
    require(case.chain(tables="", staged=True) == [0, 0, 0], case.err)
    require(len(LOADS) == 1 and LOADS[0][LOADS[0].index("--tables") + 1] == "alpha_data,beta_data", LOADS)
    recorded = json.loads((case.work() / "state" / RUN / bronze_run.RECORD).read_text())
    require(recorded["tables"] == ["alpha_data", "beta_data"], recorded)
    LOADS.clear()
    require(case.phase("pin", label="again", tables="", staged=True) == 0, case.err)
    STAGED["bronze_request.json"] = json.dumps(
        {"tables": ["alpha_data"], "target": "real", "dry_run": False, "inventory_sha256": case.shas[1], "skipped_tables": []}
    ).encode()
    case.err = ""
    require(case.phase("load", label="again", tables="", staged=True) != 0 and "differs from the one the preflight checked" in case.err, case.err)
    require(LOADS == [], "a load ran on a request that is not the one the preflight checked")
    case.err = ""
    case.shas = ("", case.shas[1])
    require(case.phase("pin", label="third", tables="", staged=True) != 0 and "differs from the one the preflight checked" in case.err, case.err)


def case_qualification_writes_only_the_twins(case: Case) -> None:
    require(case.chain(target="qualify") == [0, 0, 0], case.err)
    arguments = LOADS[0]
    require(arguments[arguments.index("--namespace") + 1] == "bronze_e2e", arguments)
    require(arguments[arguments.index("--dictionary-namespace") + 1] == "bronze_dictionary_e2e", arguments)
    require(all(name.startswith(("bronze_e2e.", "bronze_dictionary_e2e.")) for name in catalog()), sorted(catalog()))
    require("bronze_chain_dag" not in case.out, "a qualification run never offers the chain")


def case_a_real_run_records_the_files_it_carried_over(case: Case) -> None:
    """The check phase of a real run writes ``carried_over.json`` once: with no ingestion run record it says the files are not known and names none, and a
    qualification run (the test twins) writes none."""
    with mock.patch.dict(os.environ, {"HAI_CAPTURE_ROOT": "raw/"}):
        require(case.chain() == [0, 0, 0], case.err)
    stored = STORAGE[0].client.puts
    key = f"lakehouse/bronze_runs/{RUN}/carried_over.json"
    record_key = f"lakehouse/bronze_runs/{RUN}/check.json"
    attempt = [name for name in stored if "/mirror_" in name]
    require(len(attempt) == 1 and attempt[0].startswith(f"lakehouse/bronze_runs/{RUN}/mirror_"), sorted(stored))
    require(sorted(stored) == sorted([key, record_key, *attempt]), sorted(stored))
    objects, snapshots = raw_lineage.loaded_objects(json.loads(stored[record_key]), record_key, RUN)
    require({item["table"] for item in objects} == {"alpha_data", "beta_data"} and set(snapshots) == {"alpha_data", "beta_data"}, (objects, snapshots))
    document = json.loads(stored[key])
    require(document["carried_over"] is None and "no run record" in document["note"] and document["bronze_run"] == RUN, document)
    require("carried over: unknown files" in case.out, case.out)


def case_retry_has_no_second_effect(case: Case) -> None:
    require(case.chain(label="first") == [0, 0, 0], case.err)
    first_catalog, first_keys = catalog(), {item["object_key"] for item in writes() if item["kind"] == "table"}
    require(case.chain(label="second") == [0, 0, 0], case.err)
    require(catalog() == first_catalog, "the second task leaves the same tables")
    require({item["object_key"] for item in writes() if item["kind"] == "table"} == first_keys, "the second task writes the same object keys")
    require(sum(len(rows) for name, rows in catalog().items() if name.startswith("bronze.")) == 2, "one row set per object, not per task")


def case_dry_run_writes_nothing(case: Case) -> None:
    require(case.chain(extra=["--dry-run"]) == [0, 0, 0], case.err)
    require(writes() == [] and catalog() == {} and LOADS == [], "a dry run starts no loader and writes nothing shared")
    plan = json.loads(case.out.split("DRY_RUN_PLAN ", 1)[1].splitlines()[0])
    require(plan["dry_run"] is True and plan["target"] == "real" and plan["namespaces"] == ["bronze", "bronze_dictionary"], plan)
    require(plan["catalog_writes"] == "none" and plan["tables"] == ["alpha_data", "beta_data"], plan)
    alpha_key, alpha_version = case.objects["alpha_data"]
    found = [item for item in plan["objects"] if item["key"] == alpha_key]
    require(len(found) == 1 and found[0]["version_id"] == alpha_version and found[0]["sha256_verified"] is True, plan["objects"])
    require(plan["summary"]["objects"] == 2 and plan["summary"]["bytes"] == len(b"id,value\n1,a\n2,b\n") + len(b"id,value\n3,c\n"), plan["summary"])
    require("bronze_chain_dag" not in case.out, "a dry run never offers the chain")


def case_dry_run_from_the_environment(case: Case) -> None:
    environment = {"BRONZE_DRY_RUN": "1", "BRONZE_TABLES": "gamma_data", "BRONZE_TARGET": "real"}
    codes = [case.phase(phase, tables="", target="", environment=environment) for phase in bronze_run.PHASES]
    require(codes == [0, 0, 0], (codes, case.err))
    require(writes() == [] and LOADS == [], "BRONZE_DRY_RUN=1 behaves as --dry-run")
    require("gamma_data" in case.out, case.out)


def case_dry_run_stays_dry_after_pin(case: Case) -> None:
    require(case.phase("pin", extra=["--dry-run"]) == 0, case.err)
    require(case.phase("load") == 0 and case.phase("check") == 0, case.err)
    require(writes() == [] and LOADS == [], "a later phase of a dry task stays dry")
    require(case.phase("pin") == 0, "a new pin starts a new task")
    require(case.phase("load") == 0 and len(LOADS) == 1, "the new task is not dry")


def case_dry_run_checks_the_bytes(case: Case) -> None:
    key, version = case.objects["alpha_data"]
    case.storage.objects[(key, version)] = b"id,value\n9,tampered\n"
    require(case.phase("pin", extra=["--dry-run"]) == 0, case.err)
    require(case.phase("load") != 0 and key in case.err and "SHA-256" in case.err, case.err)
    require(writes() == [] and LOADS == [], "nothing is written")


def case_an_object_without_a_version_cannot_enter_the_inventory(case: Case) -> None:
    manifest_key = next(name for name, _ in case.storage.objects if name.endswith("beta/manifest.json"))
    version = next(v for k, v in case.storage.objects if k == manifest_key)
    manifest = json.loads(case.storage.objects.pop((manifest_key, version)))
    manifest["objects"][0]["object"]["version_id"] = ""
    case.storage.put(manifest_key, json.dumps(manifest).encode())
    config = bronze.load_table_map(case.root / "src/config/lakehouse/bronze_tables.json")
    try:
        bronze_pin.build_inventory(case.storage, BUCKET, config, ["beta_data"])
    except bronze.BronzeError as error:
        require("no S3 version" in str(error), str(error))
    else:
        raise AssertionError("an object without a version entered the inventory")


def case_unknown_table_is_refused(case: Case) -> None:
    require(case.phase("pin", tables="alpha_data,not_a_table") != 0 and "not_a_table" in case.err, case.err)
    require(writes() == [] and LOADS == [], "nothing runs")


def case_bad_dispatch_values_are_refused(case: Case) -> None:
    for tables, target, fragment in (
        ("", "real", "at least one table"),
        ("alpha_data,alpha_data", "real", "repeats"),
        ("alpha_data;drop", "real", "table names"),
        ("alpha_data", "production", "target"),
        ("alpha_data", "", "target"),
        ("alpha_data,gamma_data", "qualify", "one collection"),
    ):
        case.err = ""
        require(case.phase("pin", tables=tables, target=target) != 0 and fragment in case.err, (tables, target, case.err))
    require(writes() == [] and LOADS == [], "nothing runs")


def case_changed_dispatch_values_are_refused(case: Case) -> None:
    require(case.phase("pin") == 0, case.err)
    require(case.phase("load", tables="alpha_data,gamma_data") != 0 and "after the pin" in case.err, case.err)
    require(case.phase("load", target="qualify") != 0 and "after the pin" in case.err, case.err)
    require(writes() == [] and LOADS == [], "nothing runs")


def case_wrong_code_digest(case: Case) -> None:
    require(case.phase("pin", digest="0" * 64) != 0 and "SHA-256" in case.err, case.err)
    require(case.phase("load") != 0, "a load without a pin is refused")
    require(writes() == [] and LOADS == [], "nothing runs")


def case_missing_bucket(case: Case) -> None:
    out, err = io.StringIO(), io.StringIO()
    with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err), mock.patch.dict(os.environ, {"DATA_BUCKET_NAME": ""}):
        arguments = ["pin", "--run", RUN, "--work", str(case.work()), "--code-sha256", case.digest, "--tables", "alpha_data", "--target", "real"]
        code = bronze_run.main(arguments)
    require(code != 0 and "DATA_BUCKET_NAME" in err.getvalue(), err.getvalue())


def case_loader_failure_fails_the_task(case: Case) -> None:
    DEFECTS.add("loader_fails")
    codes = case.chain()
    require(codes == [0, 1], codes)
    require("exited 1" in case.err, case.err)
    require("bronze_chain_dag" not in case.out, "no chain offer after a failed load")


def case_check_refuses_a_bad_report(case: Case) -> None:
    for defect, fragment in (("report_failed", "did not pass"), ("no_report", "one loader report"), ("report_missing_table", "tables")):
        DEFECTS.clear()
        DEFECTS.add(defect)
        case.err, case.out = "", ""
        label = f"w_{defect}"
        require(case.phase("pin", label=label) == 0 and case.phase("load", label=label) == 0, case.err)
        require(case.phase("check", label=label) != 0 and fragment in case.err, (defect, case.err))
        require("bronze_chain_dag" not in case.out, defect)


def contradict_table_check(report: dict[str, Any], field: str) -> None:
    report["checks"]["beta_data"][field] = False


def case_report_table_checks(case: Case, change: Callable[[dict[str, Any]], object] | None = None, tables: str = "alpha_data,beta_data") -> None:
    label = "report_checks"
    require(case.phase("pin", label=label, tables=tables) == 0, case.err)
    require(case.phase("load", label=label, tables=tables) == 0, case.err)
    require(LOADS[-1][LOADS[-1].index("--tables") + 1] == tables, LOADS)
    reports = sorted((case.work(label) / "code" / bronze_run.REPORT_FOLDER).glob("report_*.json"))
    require(len(reports) == 1, reports)
    report = json.loads(reports[0].read_text())
    names = tables.split(",")
    require(sorted(report["tables"]) == sorted(names) and set(report["checks"]) == set(names), report)
    for name in names:
        report["checks"][name] |= {
            "objects": 1,
            "rows": 0,
            "distinct_files": 1,
            "distinct_rows": 0,
            "copies": 1,
            "one_loaded_copy_per_file": True,
            "loaded_copies_match_rows": True,
            "distinct_files_match_expected": True,
            "distinct_rows_match_expected": True,
        }
    if change is not None:
        change(report)
    reports[0].write_text(json.dumps(report, sort_keys=True) + "\n")
    before, loads = writes(), len(LOADS)
    case.err, case.out = "", ""
    code = case.phase("check", label=label, tables=tables)
    require(writes() == before and len(LOADS) == loads, "the report check must not load or write tables")
    if change is None:
        require(code == 0 and "bronze_chain_dag" in case.out, (code, case.err, case.out))
        return
    require(code == 1 and "a table check of the loader report did not pass" in case.err, (code, case.err))
    require("bronze_chain_dag" not in case.out, case.out)


def case_phase_outside_the_job(case: Case) -> None:
    require(case.phase("publish") != 0, case.err)
    require(writes() == [] and LOADS == [], "nothing runs")


def new_capture(case: Case) -> None:
    store(case.storage, "org_a/set_one", "alpha", "snap_a2", b"id,value\n7,z\n")


def case_the_pin_binds_the_source_inventory(case: Case) -> None:
    require(case.phase("pin") == 0, case.err)
    config = bronze.load_table_map(case.root / "src/config/lakehouse/bronze_tables.json")
    before, _ = bronze.discover(case.storage, BUCKET, config, ["alpha_data", "beta_data"], bronze.RetiredObjects())
    new_capture(case)
    after, _ = bronze.discover(case.storage, BUCKET, config, ["alpha_data", "beta_data"], bronze.RetiredObjects())
    require(len(before) == 2 and len(after) == 3, "an unpinned discovery sees the new capture")
    require(case.phase("load") == 0 and case.phase("check") == 0, case.err)
    require(sum(len(rows) for name, rows in catalog().items() if name.startswith("bronze.")) == 2, "the load replays the pinned inventory only")
    case.chain(label="retry")
    require(
        sum(len(rows) for name, rows in catalog().items() if name.startswith("bronze.")) == 2, "a retry after the capture arrived loads the same two objects"
    )


def case_a_dry_run_replays_the_pinned_inventory(case: Case) -> None:
    require(case.phase("pin", extra=["--dry-run"]) == 0, case.err)
    new_capture(case)
    require(case.phase("load") == 0, case.err)
    plan = json.loads(case.out.split("DRY_RUN_PLAN ", 1)[1].splitlines()[0])
    require(plan["summary"]["objects"] == 2, plan["summary"])


def case_a_tampered_inventory_is_refused(case: Case) -> None:
    case.phase("pin", extra=["--dry-run"])
    case.err = ""
    STAGED["bronze_inventory.json"] = STAGED["bronze_inventory.json"] + b" "
    require(case.phase("pin", extra=["--dry-run"]) != 0 and "inventory" in case.err, case.err)


def case_an_inventory_object_missing_from_the_bucket_is_refused(case: Case) -> None:
    require(case.phase("pin", extra=["--dry-run"]) == 0, case.err)
    key, version = case.objects["alpha_data"]
    del case.storage.objects[(key, version)]
    require(case.phase("load") != 0 and key in case.err, case.err)
    require(writes() == [] and LOADS == [], "nothing is written")


def case_a_request_replaced_after_the_preflight_checked_it_is_refused(case: Case) -> None:
    case.shas = case.stage("alpha_data,beta_data", "qualify", True)
    live = {"tables": ["alpha_data", "beta_data"], "target": "real", "dry_run": False, "inventory_sha256": case.shas[1], "skipped_tables": []}
    STAGED["bronze_request.json"] = json.dumps(live).encode()
    require(case.phase("pin", target="qualify", extra=["--dry-run"], staged=True) != 0 and "preflight checked" in case.err, case.err)
    case.err = ""
    case.shas = case.stage("alpha_data,beta_data", "qualify", True)
    STAGED["bronze_request.json"] = json.dumps(live).encode()
    require(case.phase("pin", target="real", staged=True) != 0 and "preflight checked" in case.err, case.err)
    require(writes() == [] and LOADS == [], "nothing runs")


def case_a_dry_request_cannot_run_live(case: Case) -> None:
    case.shas = case.stage("alpha_data,beta_data", "qualify", True)
    arguments = ["pin", "--run", RUN, "--work", str(case.work()), "--code-sha256", case.digest, "--bucket", BUCKET, "--tables", "alpha_data,beta_data"]
    arguments += ["--target", "qualify", "--request-sha256", case.shas[0], "--inventory-sha256", case.shas[1]]
    err = io.StringIO()
    with contextlib.redirect_stderr(err), contextlib.redirect_stdout(io.StringIO()):
        code = bronze_run.main(arguments)
    require(code != 0 and "dry run" in err.getvalue(), err.getvalue())


def case_qualification_metadata_is_scoped_to_the_selected_tables(case: Case) -> None:
    config = bronze.load_table_map(case.root / "src/config/lakehouse/bronze_tables.json")
    names = ["alpha_data", "beta_data"]
    inputs, unselected = bronze.discover(case.storage, BUCKET, config, names, bronze.RetiredObjects())
    everything = [entry["table"] for entry in config["tables"]]
    try:
        bronze_metadata.build(names, inputs, unselected, [], everything)
    except bronze_metadata.MetadataError as error:
        require("full committed" in str(error), str(error))
    else:
        raise AssertionError("the first metadata publication must need the full map")
    require(bronze.metadata_tables(config, names, "bronze_e2e") == names, "a qualification namespace is scoped")
    require(bronze.metadata_tables(config, names, "bronze") == everything, "the production namespace keeps the full map")
    require(len(bronze_metadata.build(names, inputs, unselected, [], bronze.metadata_tables(config, names, "bronze_e2e"))) > 0, "the scoped build works")
    try:
        bronze.metadata_tables(config, names, "bronze_dictionary")
    except bronze.BronzeError:
        pass
    else:
        raise AssertionError("scoped metadata is for the qualification namespace only")
    case.phase("pin", target="qualify")
    case.phase("load", target="qualify")
    require("--selected-metadata" in LOADS[-1], LOADS[-1])
    case.phase("pin", label="p")
    case.phase("load", label="p")
    require("--selected-metadata" not in LOADS[-1], LOADS[-1])


def case_the_owner_stages_an_inventory_the_pin_accepts(case: Case) -> None:
    out = case.root / "staged"
    arguments = ["inventory", "--run", RUN, "--code", str(case.root / "src"), "--bucket", BUCKET, "--tables", "alpha_data,beta_data", "--target", "qualify"]
    require(bronze_run.main([*arguments, "--out", str(out), "--dry-run"]) == 0, "the inventory phase failed")
    STAGED["bronze_request.json"], STAGED["bronze_inventory.json"] = (out / "bronze_request.json").read_bytes(), (out / "bronze_inventory.json").read_bytes()
    case.shas = (hashlib.sha256(STAGED["bronze_request.json"]).hexdigest(), hashlib.sha256(STAGED["bronze_inventory.json"]).hexdigest())
    new_capture(case)
    require(case.phase("pin", target="qualify", extra=["--dry-run"], staged=True) == 0, case.err)
    require(case.phase("load", target="qualify", staged=True) == 0 and '"objects":2' in case.out.replace(" ", ""), case.err + case.out[-300:])


ROOT = Path(__file__).resolve().parents[2]
IMAGE = ROOT / "services/lakehouse/spark"
LOCAL_PACKAGES = ("scripts", "orchestration", "services", "features", "config")


def third_party_imports(module: str) -> dict[str, set[str]]:
    """Every third-party package that the module and the repository modules it imports can import, with their importers (static, lazy imports included)."""
    seen: set[str] = set()
    found: dict[str, set[str]] = {}

    def source(name: str) -> Path | None:
        base = ROOT / name.replace(".", "/")
        return base.with_suffix(".py") if base.with_suffix(".py").exists() else (base / "__init__.py" if (base / "__init__.py").exists() else None)

    def walk(name: str) -> None:
        path = source(name)
        if name in seen or path is None:
            return
        seen.add(name)
        for node in ast.walk(ast.parse(path.read_text())):
            names = [alias.name for alias in node.names] if isinstance(node, ast.Import) else []
            if isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
                names = [node.module, *(f"{node.module}.{alias.name}" for alias in node.names)]
            for imported in names:
                top = imported.split(".")[0]
                if top in LOCAL_PACKAGES:
                    walk(imported)
                elif top not in sys.stdlib_module_names:
                    found.setdefault(top, set()).add(name)

    walk(module)
    return found


def image_packages(requirements: str) -> set[str]:
    """The distributions that the Spark image installs, plus pyspark, which the image puts on PYTHONPATH from the Spark distribution."""
    pinned = {line.split("==")[0].strip().lower().replace("-", "_") for line in requirements.splitlines() if "==" in line and not line.startswith((" ", "#"))}
    return pinned | {"pyspark"}


def case_the_spark_image_has_every_package_the_driver_imports(case: Case) -> None:
    needed = third_party_imports("scripts.lakehouse.bronze_run")
    require({"boto3", "pdfplumber", "openpyxl", "xlrd", "pyreadstat", "pyspark"} <= set(needed), sorted(needed))
    requirements = lock_env.export("spark")
    require("/opt/spark/python" in (IMAGE / "Dockerfile").read_text(), "the image must put pyspark on PYTHONPATH")
    missing = sorted(set(needed) - image_packages(requirements))
    require(not missing, f"the Spark image does not install {missing}")
    without = "\n".join(line for line in requirements.splitlines() if not line.startswith("pdfplumber=="))
    require(sorted(set(needed) - image_packages(without)) == ["pdfplumber"], "a requirements file without pdfplumber must be caught")


def case_a_selected_table_without_objects_is_refused(case: Case) -> None:
    for label, extra in (("dry", ["--dry-run"]), ("real", [])):
        case.err = ""
        code = case.phase("pin", label=label, tables="alpha_data,empty_data", extra=extra)
        require(code != 0 and "empty_data" in case.err and "no objects" in case.err, f"{label}: {case.err}")
        require("alpha_data" not in case.err, f"{label}: only the empty table is named: {case.err}")
    require(case.phase("load", tables="alpha_data,empty_data") != 0, "a load without a pin is refused")
    require(writes() == [] and LOADS == [], "nothing runs")


def case_a_raw_only_capture_is_refused_by_name(case: Case) -> None:
    store(case.storage, "raw/org_c/set_three", "empty", "snap_raw", b"id,value\n1,r\n")
    require(case.phase("pin", tables="empty_data", extra=["--dry-run"]) != 0 and "empty_data" in case.err and "no objects" in case.err, case.err)
    require(writes() == [] and LOADS == [], "nothing runs")


def case_the_raw_location_is_one_function_and_stays_off(case: Case) -> None:
    """The raw prefix is read only after the redownload route lands. Until then every place that lists a collection asks one function for its roots, which names
    the bucket root alone. A raw-only capture is refused. Adding the raw root to that one function finds the capture, and the inventory records its raw key."""
    require(bronze.source_roots("org_c/set_three") == ("org_c/set_three/",), f"the default roots are {bronze.source_roots('org_c/set_three')}")
    store(case.storage, "raw/org_c/set_three", "empty", "snap_raw", b"id,value\n1,r\n")
    require(case.phase("pin", tables="empty_data", extra=["--dry-run"]) != 0 and "no objects" in case.err, "the default must not read raw")
    case.err = ""

    def both(collection: str) -> tuple[str, ...]:
        return (f"{collection}/", f"raw/{collection}/")

    case.shas = None
    with mock.patch.object(bronze, "source_roots", both):
        code = case.phase("pin", label="flipped", tables="empty_data", extra=["--dry-run"])
        require(code == 0, f"with the raw root the capture must be found: {case.err}")
        keys = [row[1] for row in json.loads(STAGED["bronze_inventory.json"])["objects"]]
        require(len(keys) == 1 and keys[0].startswith("raw/org_c/set_three/"), f"the inventory records {keys}")


def case_the_owner_cannot_stage_an_inventory_with_an_empty_table(case: Case) -> None:
    out = case.root / "staged_empty"
    arguments = ["inventory", "--run", RUN, "--code", str(case.root / "src"), "--bucket", BUCKET, "--tables", "alpha_data,empty_data", "--target", "real"]
    err = io.StringIO()
    with contextlib.redirect_stderr(err), contextlib.redirect_stdout(io.StringIO()):
        code = bronze_run.main([*arguments, "--out", str(out)])
    require(code != 0 and "empty_data" in err.getvalue() and "no objects" in err.getvalue(), err.getvalue())
    require(not (out / "bronze_request.json").exists(), "no request is written for an empty table")


CASES: dict[str, Callable[[Case], None]] = {
    "a real run writes only bronze and its dictionary and names the chain trigger": case_real_run_writes_only_bronze,
    "a failed Glue mirror leaves the run passed and is recorded and shown": case_a_failed_glue_mirror_leaves_the_run_passed_and_is_recorded_and_shown,
    "a loader report with a malformed mirror outcome is refused": case_a_loader_report_with_a_malformed_mirror_outcome_is_refused,
    "a retry that fails the mirror after a success shows the failure and keeps the receipt": case_a_retry_failing_the_mirror_after_a_success_keeps_the_receipt,
    "a retry that repairs a failed mirror shows the repair and keeps the receipt": case_a_retry_repairing_the_mirror_keeps_the_receipt,
    "a repeated check of the same report makes no second attempt record": case_a_repeated_check_of_the_same_report_makes_no_second_attempt_record,
    "an attempt record that cannot be written fails no phase and is named": case_an_attempt_record_that_cannot_be_written_fails_no_phase_and_is_named,
    "a real run records the files it carried over": case_a_real_run_records_the_files_it_carried_over,
    "the stage phase writes the inputs the chain then runs on": case_the_stage_phase_writes_the_inputs_the_chain_then_runs_on,
    "a request that drops a table with sources is refused at the pin even when the stage report names it": case_a_dropped_table_with_sources_is_refused,
    "a changed request or a report changed alone is refused by every phase": case_a_changed_request_or_report_is_refused,
    "a table with only a rejected capture is not left out": case_a_table_with_only_a_rejected_capture_is_not_left_out,
    "the owner stages a partial run that names the absent table": case_the_owner_stages_a_partial_run_that_names_the_absent_table_and_every_phase_accepts_it,
    "the phases read their tables from the staged request when none is passed": case_the_phases_read_their_tables_from_the_staged_request,
    "a qualification run writes only the bronze_e2e twins": case_qualification_writes_only_the_twins,
    "a retry of one run has no second effect": case_retry_has_no_second_effect,
    "a dry run writes nothing shared and prints the checked plan": case_dry_run_writes_nothing,
    "BRONZE_DRY_RUN=1 and the dispatch variables start a dry run": case_dry_run_from_the_environment,
    "a dry task stays dry and a new pin starts a new task": case_dry_run_stays_dry_after_pin,
    "a dry run stops on bytes that differ from the manifest hash": case_dry_run_checks_the_bytes,
    "an object without a version cannot enter the inventory": case_an_object_without_a_version_cannot_enter_the_inventory,
    "a table outside the table map is refused at the pin": case_unknown_table_is_refused,
    "bad table lists and targets are refused at the pin": case_bad_dispatch_values_are_refused,
    "dispatch values that changed after the pin are refused": case_changed_dispatch_values_are_refused,
    "a wrong code digest stops the pin": case_wrong_code_digest,
    "a missing bucket name is refused": case_missing_bucket,
    "a loader failure fails the task": case_loader_failure_fails_the_task,
    "the check phase refuses a failed, missing or partial report": case_check_refuses_a_bad_report,
    "a phase outside the job exits non-zero": case_phase_outside_the_job,
    "the pin binds the source inventory and a retry replays it": case_the_pin_binds_the_source_inventory,
    "a dry run replays the pinned inventory": case_a_dry_run_replays_the_pinned_inventory,
    "a tampered inventory is refused at the pin": case_a_tampered_inventory_is_refused,
    "an inventory object missing from the bucket is refused": case_an_inventory_object_missing_from_the_bucket_is_refused,
    "a request replaced after the preflight checked it is refused": case_a_request_replaced_after_the_preflight_checked_it_is_refused,
    "a dry request cannot run live": case_a_dry_request_cannot_run_live,
    "qualification metadata is scoped to the selected tables": case_qualification_metadata_is_scoped_to_the_selected_tables,
    "the owner stages an inventory that the pin accepts": case_the_owner_stages_an_inventory_the_pin_accepts,
    "the Spark image installs every package the driver imports": case_the_spark_image_has_every_package_the_driver_imports,
    "a selected table without objects is refused by name in dry and real runs": case_a_selected_table_without_objects_is_refused,
    "a raw-only capture gives no objects and is refused by name": case_a_raw_only_capture_is_refused_by_name,
    "the raw location is one function and stays off": case_the_raw_location_is_one_function_and_stays_off,
    "the owner cannot stage an inventory with an empty table": case_the_owner_cannot_stage_an_inventory_with_an_empty_table,
    "the check phase accepts complete table checks with numeric rows zero": case_report_table_checks,
}

for label, tables, change in (
    ("missing table checks", "alpha_data,beta_data", lambda d: d.pop("checks")),
    ("empty table checks", "alpha_data,beta_data", lambda d: d.update(checks={})),
    ("partial table checks", "alpha_data,beta_data", lambda d: d["checks"].pop("beta_data")),
    ("partial checks of another pinned table set", "beta_data,gamma_data", lambda d: d["checks"].pop("gamma_data")),
    ("an unrequested table check", "alpha_data,beta_data", lambda d: d["checks"].update(gamma_data={"passed": True, "objects_match_inputs": True})),
    ("a failed table under a passing summary", "alpha_data,beta_data", lambda d: d["checks"]["beta_data"].update(passed=False)),
    ("a missing objects_match_inputs assertion", "alpha_data,beta_data", lambda d: d["checks"]["beta_data"].pop("objects_match_inputs")),
    ("a nonboolean objects_match_inputs assertion", "alpha_data,beta_data", lambda d: d["checks"]["beta_data"].update(objects_match_inputs=1)),
):
    CASES[f"the check phase refuses {label}"] = partial(case_report_table_checks, change=change, tables=tables)

for assertion in (
    "objects_match_inputs",
    "one_loaded_copy_per_file",
    "loaded_copies_match_rows",
    "distinct_files_match_expected",
    "distinct_rows_match_expected",
):
    CASES[f"the check phase refuses passed true with {assertion} false"] = partial(
        case_report_table_checks, change=partial(contradict_table_check, field=assertion)
    )


def run_all(selected: Sequence[str]) -> list[dict[str, Any]]:
    results = []
    for name, case in CASES.items():
        if selected and not any(part in name for part in selected):
            continue
        with tempfile.TemporaryDirectory() as folder, patched(Path(folder)) as workspace:
            try:
                case(workspace)
                outcome, detail = "PASS", ""
            except Exception as error:
                outcome, detail = "FAIL", f"{type(error).__name__}: {error}\n{traceback.format_exc(limit=3)}"
        sys.stdout.write(f"{outcome} {name}\n")
        if detail:
            sys.stdout.write(detail + "\n")
        results.append({"case": name, "outcome": outcome, "detail": detail})
    return results


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--report", type=Path)
    parser.add_argument("--only", action="append", default=[], help="run the cases whose name contains this text")
    args = parser.parse_args(argv)
    results = run_all(args.only)
    failed = [item for item in results if item["outcome"] != "PASS"]
    sys.stdout.write(f"{len(results) - len(failed)} of {len(results)} cases passed\n")
    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(json.dumps({"at": datetime.now(UTC).isoformat(), "revision": REVISION, "results": results}, indent=2) + "\n")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
