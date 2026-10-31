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
from pathlib import Path
from typing import Any
from unittest import mock

from scripts.lakehouse import bronze, bronze_metadata, bronze_pin, bronze_run

REVISION = "0123456789abcdef0123456789abcdef01234567"
RUN = "20261030T010203Z-0a1b2c3d"
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
STORAGE: list[FakeStorage] = []
STAGED: dict[str, bytes] = {}


class FakeStorage:
    """Versioned objects in memory: list current keys, get or download one exact version."""

    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], bytes] = {}

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


def loader(job: bronze_run.Job, module: str, arguments: Sequence[str | Path], environment: dict[str, str] | None = None) -> None:
    """Stand-in for ``scripts.lakehouse.bronze``: it discovers the real inputs, replaces each object's rows in place and writes the counts-only report."""
    require(module == "scripts.lakehouse.bronze", module)
    values = [str(item) for item in arguments]
    LOADS.append(values)
    if "loader_fails" in DEFECTS:
        raise bronze_run.StepError("scripts.lakehouse.bronze exited 1")
    namespace, dictionary = values[values.index("--namespace") + 1], values[values.index("--dictionary-namespace") + 1]
    names = values[values.index("--tables") + 1].split(",")
    config = bronze.load_table_map(job.code / "config/lakehouse/bronze_tables.json")
    source: Any = STORAGE[0]
    if "--inventory" in values:
        inventory = bronze_pin.read_inventory(Path(values[values.index("--inventory") + 1]).read_bytes())
        source = bronze_pin.PinnedStorage(source, inventory)
    inputs, _ = bronze.discover(source, BUCKET, config, names, bronze.RetiredObjects())
    if "--inventory" in values:
        bronze_pin.verify_replay(inventory, inputs)
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
    report = {"mode": "load", "tables": shown, "inputs": len(inputs), "checks": checks, "passed": "report_failed" not in DEFECTS}
    folder = job.code / bronze_run.REPORT_FOLDER
    folder.mkdir(parents=True, exist_ok=True)
    if "no_report" not in DEFECTS:
        (folder / f"report_{datetime.now(UTC).strftime('%Y%m%dT%H%M%SZ')}.json").write_text(json.dumps(report, sort_keys=True))
    sys.stdout.write(json.dumps(report, sort_keys=True) + "\n")


class Case:
    """One workspace: its own catalog model, bucket and code archive."""

    def __init__(self, root: Path) -> None:
        self.root = root
        CATALOG[:] = [root / "catalog"]
        CATALOG[0].mkdir()
        DEFECTS.clear()
        LOADS.clear()
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
        """What the owner stages and the broker checks: the inventory of the manifest versions and the request that names its digest."""
        self.key = (tables, target, dry)
        names = [name for name in tables.split(",") if name]
        config = bronze.load_table_map(self.root / "src/config/lakehouse/bronze_tables.json")
        try:
            inventory = bronze_pin.build_inventory(STORAGE[0], BUCKET, config, names)
        except (bronze.BronzeError, KeyError):
            inventory = {"version": 1, "tables": [], "manifests": [], "objects": [], "documents": []}
        raw = bronze_pin.inventory_bytes(inventory)
        STAGED["bronze_inventory.json"] = raw
        request = {"tables": names, "target": target, "dry_run": dry, "inventory_sha256": hashlib.sha256(raw).hexdigest()}
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
        arguments = [phase, "--run", RUN, "--work", str(self.work(label)), "--code-sha256", digest or self.digest, "--bucket", BUCKET]
        arguments += ["--tables", tables, "--target", target, "--request-sha256", self.shas[0], "--inventory-sha256", self.shas[1], *extra]
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


@contextlib.contextmanager
def patched(root: Path) -> Iterator[Case]:
    with (
        mock.patch.object(bronze_run, "run_module", loader),
        mock.patch.object(bronze_run, "storage", lambda: STORAGE[0]),
        mock.patch.object(bronze_run, "owner_storage", lambda: STORAGE[0]),
        mock.patch.object(bronze_run, "read_staged", lambda run, bucket, name: STAGED[name]),
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


def case_qualification_writes_only_the_twins(case: Case) -> None:
    require(case.chain(target="qualify") == [0, 0, 0], case.err)
    arguments = LOADS[0]
    require(arguments[arguments.index("--namespace") + 1] == "bronze_e2e", arguments)
    require(arguments[arguments.index("--dictionary-namespace") + 1] == "bronze_dictionary_e2e", arguments)
    require(all(name.startswith(("bronze_e2e.", "bronze_dictionary_e2e.")) for name in catalog()), sorted(catalog()))
    require("bronze_chain_dag" not in case.out, "a qualification run never offers the chain")


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


def case_a_request_replaced_after_the_broker_checked_it_is_refused(case: Case) -> None:
    case.shas = case.stage("alpha_data,beta_data", "qualify", True)
    live = {"tables": ["alpha_data", "beta_data"], "target": "real", "dry_run": False, "inventory_sha256": case.shas[1]}
    STAGED["bronze_request.json"] = json.dumps(live).encode()
    require(case.phase("pin", target="qualify", extra=["--dry-run"], staged=True) != 0 and "broker checked" in case.err, case.err)
    case.err = ""
    case.shas = case.stage("alpha_data,beta_data", "qualify", True)
    STAGED["bronze_request.json"] = json.dumps(live).encode()
    require(case.phase("pin", target="real", staged=True) != 0 and "broker checked" in case.err, case.err)
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
    requirements = (IMAGE / "requirements.txt").read_text()
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
    "a request replaced after the broker checked it is refused": case_a_request_replaced_after_the_broker_checked_it_is_refused,
    "a dry request cannot run live": case_a_dry_request_cannot_run_live,
    "qualification metadata is scoped to the selected tables": case_qualification_metadata_is_scoped_to_the_selected_tables,
    "the owner stages an inventory that the pin accepts": case_the_owner_stages_an_inventory_the_pin_accepts,
    "the Spark image installs every package the driver imports": case_the_spark_image_has_every_package_the_driver_imports,
    "a selected table without objects is refused by name in dry and real runs": case_a_selected_table_without_objects_is_refused,
    "a raw-only capture gives no objects and is refused by name": case_a_raw_only_capture_is_refused_by_name,
    "the raw location is one function and stays off": case_the_raw_location_is_one_function_and_stays_off,
    "the owner cannot stage an inventory with an empty table": case_the_owner_cannot_stage_an_inventory_with_an_empty_table,
}


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
