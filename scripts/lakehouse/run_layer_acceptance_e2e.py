"""E2E of the owner's layer acceptance command: two real-shaped runs per layer, a good record that passes and each planted defect refused by name.

    .venv/bin/python -m scripts.lakehouse.run_layer_acceptance_e2e --out <NEW_DIRECTORY>

The records have the shapes the driver writes (``layer_run.bundle``, ``gold_export.export_sealed``, ``gold_publish.publish_candidates``,
``reconcile_candidates``, ``catalog_backup``, ``commit_receipt`` and ``silver_publish.record_committed``) with the tables of the frozen publish contract.
The input listing digest comes from the driver's own ``layer_run.input_listing`` over a fake bucket. The describe output holds the overrides that
``job_overrides`` writes. Every defect case copies a good run, breaks one thing and requires the report to fail the check that names it."""

from __future__ import annotations

import argparse
import contextlib
import functools
import hashlib
import io
import json
import os
import re
import shutil
import sys
import tempfile
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from unittest import mock

from scripts.lakehouse import layer_acceptance as acceptance
from scripts.lakehouse import layer_run
from scripts.process import run_command

ROOT = Path(__file__).resolve().parents[2]
GIT_IGNORED = acceptance.ignored  # the real check, before the cases replace it with a rule about the scratch folder
SILVER_RUN, OTHER_SILVER_RUN, GOLD_RUN, OTHER_GOLD_RUN = (
    "20261031T120000Z-abcdef12",
    "20261031T130000Z-fedcba98",
    "20261101T090000Z-0a1b2c3d",
    "20261101T100000Z-3d2c1b0a",
)
ACCOUNT = "111111111111"
INPUTS = 1500  # more than one page of 1000, so the page structure of the digest is exercised
MARKER = "9x<bad>-fingerprint"


@functools.cache
def job_overrides_for(layer: str, run: str, code_sha256: str, input_run: str) -> dict[str, Any]:
    """The container overrides that ``scripts.orchestration.job_overrides`` writes for a real task of the layer."""
    arguments = ["-m", "scripts.orchestration.job_overrides", "--job", layer, "--run", run, "--code-run", run, "--code-sha256", code_sha256]
    arguments += ["--polaris-uri", "http://10.0.1.20:8181/api/catalog"] + (["--input-run", input_run] if input_run else [])
    result = run_command(sys.executable, arguments, cwd=ROOT, env={**os.environ, "PYTHONPATH": str(ROOT)}, timeout=60)
    if result.returncode != 0:
        raise RuntimeError("the override generator failed")
    return json.loads(result.stdout)


def h(*parts: str) -> str:
    return hashlib.sha256(":".join(parts).encode()).hexdigest()


def revision() -> str:
    return run_command("git", ["rev-parse", "HEAD"], cwd=ROOT, timeout=30).stdout.strip()


REVISION = revision()


def frozen(path: str) -> Any:
    return json.loads(run_command("git", ["show", f"{REVISION}:{path}"], cwd=ROOT, timeout=30).stdout)


CONTRACT_TABLES = {
    "silver": sorted(frozen("data_contracts/lakehouse/silver_publish.json")["publish"]["tables"]),
    "gold": sorted(frozen("data_contracts/lakehouse/gold_contract.json")["publish"]["tables"]),
}
BRONZE_TABLES = sorted({entry["table"] for entry in frozen("config/lakehouse/bronze_tables.json")["tables"]} | {"file_metadata"})


@dataclass(frozen=True)
class Spec:
    """One run: its layer, its ID, a tag that makes its digests differ from another run's and the tables it publishes."""

    layer: str
    run: str
    tag: str
    tables: tuple[str, ...]
    code: str = ""

    @property
    def code_sha256(self) -> str:
        return self.code or h(self.tag, "code")

    @property
    def sealed(self) -> str:
        return h(self.tag, "sealed")


@dataclass
class Env:
    root: Path
    spec: Spec
    producer: Env | None = None

    @property
    def evidence(self) -> Path:
        return self.root / "evidence"


def spec(layer: str, run: str, tag: str, tables: tuple[str, ...] | None = None) -> Spec:
    return Spec(layer, run, tag, tuple(tables if tables is not None else CONTRACT_TABLES[layer]))


def write(path: Path, document: Any) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    raw = (json.dumps(document, indent=2, sort_keys=True) + "\n").encode()
    path.write_bytes(raw)
    return hashlib.sha256(raw).hexdigest()


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


class FakeBucket:
    """The version listing of a bucket as ``list_object_versions`` pages it: 1000 entries a page in key order."""

    def __init__(self, entries: list[dict[str, Any]]) -> None:
        self.entries = entries

    def list_object_versions(self, Bucket: str, Prefix: str, KeyMarker: str | None = None, VersionIdMarker: str | None = None) -> dict[str, Any]:
        held = [item for item in self.entries if item["Key"].startswith(Prefix)]
        start = 0 if KeyMarker is None else next(i for i, item in enumerate(held) if (item["Key"], item["VersionId"]) == (KeyMarker, VersionIdMarker)) + 1
        page = held[start : start + 1000]
        truncated = start + 1000 < len(held)
        result: dict[str, Any] = {
            "Versions": [entry for entry in page if not entry.get("Marker")],
            "DeleteMarkers": [{"Key": entry["Key"], "VersionId": entry["VersionId"]} for entry in page if entry.get("Marker")],
            "IsTruncated": truncated,
        }
        if truncated:
            result |= {"NextKeyMarker": page[-1]["Key"], "NextVersionIdMarker": page[-1]["VersionId"]}
        return result


def input_entries(item: Spec) -> list[dict[str, Any]]:
    """The input prefix in listing order: INPUTS versions and three historical delete markers of keys that were deleted before the run."""
    prefix = acceptance.INPUT_PREFIX[item.layer]
    versions = [
        {"Key": f"{prefix}f{index:05d}", "VersionId": f"v{h(item.tag, str(index))[:8]}", "ETag": f'"{h(item.tag, str(index))[:32]}"', "Size": 100 + index}
        for index in range(INPUTS)
    ]
    markers = [{"Key": f"{prefix}f{index:05d}x", "VersionId": f"d{h(item.tag, 'marker', str(index))[:8]}", "Marker": True} for index in (10, 1200, 1499)]
    return sorted([*versions, *markers], key=lambda entry: entry["Key"])


def cli_listing(entries: list[dict[str, Any]]) -> dict[str, Any]:
    """The ``--query`` shape of ``aws s3api list-object-versions`` that the doc saves: versions and delete markers apart."""
    versions = [{"key": i["Key"], "version": i["VersionId"], "etag": i["ETag"], "bytes": i["Size"]} for i in entries if not i.get("Marker")]
    deleted = [{"key": i["Key"], "version": i["VersionId"]} for i in entries if i.get("Marker")]
    return {"versions": versions, "deleted": deleted or None}


def build_records(env: Env) -> None:
    """The run folder the layer upload leaves: every record has the shape the driver writes and every digest link holds."""
    item, folder = env.spec, env.evidence
    namespace = "silver" if item.layer == "silver" else "gold"
    run, sealed = item.run, item.sealed
    entries = input_entries(item)
    listing = layer_run.input_listing(FakeBucket(entries), "bucket", acceptance.INPUT_PREFIX[item.layer])
    write(
        folder / "layer_run.json",
        {  # layer_run.bundle
            "run_id": run,
            "layer": item.layer,
            "mode": "real",
            "code_run": run,
            "code_sha256": item.code_sha256,
            "git_revision": REVISION,
            "source_sha256": h(item.tag, "source"),
            "input_objects": {"code.tar.gz": {"version_id": "v1", "etag": '"e"', "bytes": 10}},
            "input_listing": listing,
        },
    )
    tables = {
        name: {"rows": 10 + i, "columns": [["id", "BIGINT"]], "merge_key": ["id"], "file": f"{name}.parquet", "file_sha256": h(item.tag, name), "bytes": 99}
        for i, name in enumerate(item.tables)
    }
    manifest = write(
        folder / "publish/export/manifest.json", {"schema": 1, "run_id": run, "sealed_sha256": sealed, "in_container_sha256": sealed, "tables": tables}
    )
    write(  # gold_export.export_sealed
        folder / "publish/consumer_result.json",
        {"schema": 1, "run_id": run, "expected_sha256": sealed, "in_container_sha256": sealed, "exported": True, "manifest_sha256": manifest},
    )
    snapshots = {name: 4000 + i + ord(item.tag) * 100 for i, name in enumerate(item.tables)}
    candidates = write(
        folder / "publish/candidates.json",
        {  # gold_publish.publish_candidates
            "schema": 1,
            "run_id": run,
            "mode": "real",
            "namespace": namespace,
            "branch": f"run_{run}",
            "sealed_sha256": sealed,
            "export_manifest_sha256": manifest,
            "bases": {name: 1 for name in item.tables},
            "tables": {name: {"candidate_snapshot_id": snapshots[name], "rows": 10 + i, "base_snapshot_id": 1} for i, name in enumerate(item.tables)},
            "complete": True,
        },
    )
    reconciled = {
        name: {  # gold_publish.reconcile_candidates
            "candidate_snapshot_id": snapshots[name],
            "rows": 10 + i,
            "export_rows": 10 + i,
            "manifest_rows": 10 + i,
            "fingerprint": str(7000 + i),
            "export_fingerprint": str(7000 + i),
            "main_snapshot_id": 1,
            "main_unchanged": True,
            "equal": True,
        }
        for i, name in enumerate(item.tables)
    }
    reconcile = write(folder / "publish/reconcile.json", {"schema": 1, "run_id": run, "candidates_sha256": candidates, "tables": reconciled, "pass": True})
    backup = write(  # gold_publish.catalog_backup
        folder / "publish/backup.json",
        {
            "schema": 1,
            "run_id": run,
            "reconcile_sha256": reconcile,
            "name": f"catalog_backup_{run}.json",
            "backup_sha256": h(item.tag, "backup"),
            "read_back": True,
        },
    )
    write(
        folder / "publish/commit.json", {"schema": 1, "run_id": run, "backup_sha256": backup, "snapshot_set": snapshots, "state": "committed"}
    )  # commit_receipt
    if item.layer == "silver":
        write(folder / "pins.json", {name: str(900 + i) for i, name in enumerate(BRONZE_TABLES)})
        write(
            folder / "silver_snapshot.json", {"schema": 1, "run_id": run, "mode": "real", "namespace": "silver", "snapshot_set": snapshots}
        )  # record_committed
    else:
        producer = json.loads((env.producer.evidence / "silver_snapshot.json").read_text()) if env.producer else {"run_id": "", "snapshot_set": {}}
        write(
            folder / "silver_pins.json",
            {
                "schema": 1,
                "run_id": run,
                "mode": "real",
                "namespace": "silver",
                "source_run_id": producer["run_id"],
                "snapshot_set": producer["snapshot_set"],
            },  # silver_publish.pin
        )
    root = env.root
    write(root / "before.json", cli_listing(entries + [{"Key": f"lakehouse/run_stage/{run}/inputs/code.tar.gz", "VersionId": "s1", "ETag": '"s"', "Size": 5}]))
    added = [
        {"Key": f"{acceptance.WRITE_PREFIXES[item.layer][0]}table/data.parquet", "VersionId": "n1", "ETag": '"n"', "Size": 9},
        {"Key": f"lakehouse/layer_runs/{item.layer}/{run}/layer_run.json", "VersionId": "n2", "ETag": '"n"', "Size": 9},
    ]
    after = json.loads((root / "before.json").read_text())
    after["versions"] += cli_listing(added)["versions"]
    write(root / "after.json", after)
    overrides = job_overrides_for(item.layer, run, item.code_sha256, env.producer.spec.run if env.producer else "")
    write_describe(env, {"containerOverrides": [{"name": "init"}, *overrides["containerOverrides"]]})  # describe-tasks lists every container, init by name only
    write(root / "approval.json", {"schema": 1, "layer": item.layer, "run_id": run, "code_sha256": item.code_sha256, "approved_at": "2026-10-31T16:50:00Z"})
    write(root / "running.json", {"taskArns": []})
    write(root / "host.json", {"desired_capacity": 0, "group_instances": 0, "active_container_instances": 0, "services": {}})


def write_describe(env: Env, overrides: dict[str, Any], definition: str | None = None) -> None:
    layer = env.spec.layer
    containers = [{"name": name, "lastStatus": "STOPPED", "exitCode": 0} for name in acceptance.expected_containers(layer)]
    task = h(env.spec.tag, "task")[:32]
    write(
        env.root / "describe.json",
        {
            "failures": [],
            "tasks": [
                {
                    "taskArn": f"arn:aws:ecs:us-west-2:{ACCOUNT}:task/example_project_cluster_dev/{task}",
                    "taskDefinitionArn": definition or f"arn:aws:ecs:us-west-2:{ACCOUNT}:task-definition/example_project_dev_job_{layer}:7",
                    "createdAt": "2026-10-31T10:00:00.000000-07:00",
                    "lastStatus": "STOPPED",
                    "stopCode": "EssentialContainerExited",
                    "stoppedReason": "Essential container in task exited",
                    "containers": containers,
                    "overrides": overrides,
                }
            ],
        },
    )


def run_cli(arguments: list[str], variables: Mapping[str, str] | None = None) -> tuple[int, str, str]:
    out, err = io.StringIO(), io.StringIO()
    old = {key: os.environ.pop(key, None) for key in ("CONFIRM_APPROVE", "CONFIRM_SIGN")}
    os.environ.update(variables or {})
    try:
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            try:
                code = acceptance.main(arguments)
            except SystemExit as stop:
                code = int(stop.code or 0)
            except Exception as crash:  # a crash is not a refusal: it is one failed check with the exit code 70
                code = 70
                err.write(f"crashed: {type(crash).__name__}\n")
    finally:
        for key in old:
            os.environ.pop(key, None)
        os.environ.update({key: value for key, value in old.items() if value is not None})
    return code, out.getvalue(), err.getvalue()


def report_arguments(env: Env, out: Path) -> list[str]:
    files = {"approval": "approval.json", "describe": "describe.json", "evidence": "evidence", "before": "before.json", "after": "after.json"}
    files |= {"running-tasks": "running.json", "host-status": "host.json"}
    arguments = ["report", "--layer", env.spec.layer, "--run", env.spec.run, "--out", str(out)]
    for flag, name in files.items():
        arguments += [f"--{flag}", str(env.root / name)]
    if env.producer:
        arguments += ["--input-evidence", str(env.producer.evidence), "--input-report", str(env.producer.root / "qualification.json")]
    return arguments


def accept(env: Env) -> tuple[int, list[str], str]:
    """Run the report; returns its exit code, the failed check names and everything the command printed."""
    out = env.root / "qualification.json"
    code, printed, error = run_cli(report_arguments(env, out))
    document = json.loads(out.read_text()) if out.is_file() else {}
    return code, sorted(name for name, ok in document.get("checks", {}).items() if not ok), printed + error + (out.read_text() if out.is_file() else "")


def sign(env: Env) -> int:
    out = env.root / "qualification.json"
    text = f"sign-{env.spec.layer}-{env.spec.run}-{digest(out)[:12]}"
    return run_cli(["sign", "--report", str(out), "--owner", "Example Owner"], {"CONFIRM_SIGN": text})[0]


def make(work: Path, name: str, item: Spec, producer: Env | None = None, signed: bool = True) -> Env:
    """Build a run, check it and sign it, as the owner does for the silver run before gold."""
    env = Env(work / name, item, producer)
    build_records(env)
    if signed:
        code, failed, _ = accept(env)
        if code != 0 or failed or sign(env) != 0:
            raise RuntimeError(f"the good {name} run does not pass: {failed}")
    return env


def edit(path: Path, change: Callable[[Any], None]) -> None:
    document = json.loads(path.read_text())
    change(document)
    path.write_text(json.dumps(document, indent=2, sort_keys=True) + "\n")


def relink(env: Env) -> None:
    """Rewrite every digest link of the run in order, so a case breaks one rule and not the chain."""
    publish = env.evidence / "publish"
    manifest = digest(publish / "export/manifest.json")
    edit(publish / "consumer_result.json", lambda d: d.update(manifest_sha256=manifest))
    edit(publish / "candidates.json", lambda d: d.update(export_manifest_sha256=manifest))
    edit(publish / "reconcile.json", lambda d: d.update(candidates_sha256=digest(publish / "candidates.json")))
    edit(publish / "backup.json", lambda d: d.update(reconcile_sha256=digest(publish / "reconcile.json")))
    edit(publish / "commit.json", lambda d: d.update(backup_sha256=digest(publish / "backup.json")))


def rewrite(env: Env, name: str, change: Callable[[Any], None]) -> None:
    edit(env.evidence / name, change)
    relink(env)


def copy_from(env: Env, other: Env, name: str) -> None:
    shutil.copyfile(other.evidence / name, env.evidence / name)


def scenario(work: Path, layer: str, label: str, tables: tuple[str, ...] | None = None) -> tuple[Env, Env]:
    """A good unsigned run of the layer and a second real-shaped run of the same layer, for the cases that mix them."""
    slug = re.sub("[^a-z]+", "_", f"{layer}_{label}")
    producer = None
    if layer == "gold":
        producer = make(work, f"{slug}_producer", spec("silver", SILVER_RUN, "a"))
    runs = {"silver": (SILVER_RUN, OTHER_SILVER_RUN), "gold": (GOLD_RUN, OTHER_GOLD_RUN)}[layer]
    env = Env(work / slug, spec(layer, runs[0], "a" if layer == "silver" else "c", tables), producer)
    build_records(env)
    other = Env(work / f"{slug}_other", spec(layer, runs[1], "b" if layer == "silver" else "d", tables), producer)
    build_records(other)
    return env, other


NAMES = {
    "approved": "the owner approved this run ID before the task was created",
    "task": "the task is this run's: its definition, its overrides and the approved code digest",
    "exits": "every container of the task exited 0 and the task stopped",
    "identity": "the run records belong to this run and layer in real mode, with the approved code",
    "sets": "every table set equals the frozen publish contract",
    "namespace": "the records name the layer's own namespace in real mode",
    "export": "the export is this run's: run, sealed identity, consumer result and manifest hashes agree",
    "reconcile": "the candidates are complete and the reconcile record is a pass for them",
    "rows": "the row count of every table equals its candidate and its export",
    "fingerprints": "the content fingerprint of every table is valid and equals its export",
    "backup": "the backup was read back after the reconcile",
    "commit": "publish_runs was written last: the commit record follows the backup and is committed",
    "snapshots": "the commit names every candidate snapshot ID",
    "record": "the silver record equals the commit",
    "bronze": "H3: the pinned table set is the bronze table map of the run's revision",
    "pin": "H3: the gold pin is complete: its run, mode, namespace and the silver contract's tables",
    "producer_set": "H3: the pin names the silver run and its complete snapshot set",
    "signed": "the silver producer is signed: its report passed, covers its record and matches its signature",
    "same": "H2: the input object versions are the same before and after the task",
    "listing": "H2: the pin recorded the digest and count of the listing taken before the task",
    "scope": "the task added versions only under the layer's prefixes and its own run prefix",
    "removed": "the task removed no version",
    "running": "no task of the job is left running",
    "host": "the host is stopped and verified: no capacity, group instance or container instance",
}


Case = tuple[str, Callable[[Env, Env], None], tuple[str, ...]]


def common_cases() -> list[Case]:
    """Defects of either layer: (label, change given the run and another real run, the checks that must fail)."""

    def overrides_with(change: Callable[[dict[str, Any]], None]) -> Callable[[Env, Env], None]:
        def apply(env: Env, other: Env) -> None:
            edit(env.root / "describe.json", lambda d: change(d["tasks"][0]["overrides"]))

        return apply

    def set_env(name: str, value: str) -> Callable[[dict[str, Any]], None]:
        def change(overrides: dict[str, Any]) -> None:
            for container in overrides["containerOverrides"]:
                for item in container.get("environment", []):
                    if item["name"] == name:
                        item["value"] = value

        return change

    return [
        (
            "the dry run task of the same run",
            overrides_with(lambda o: [c["environment"].append({"name": "LAYER_DRY_RUN", "value": "1"}) for c in o["containerOverrides"] if "environment" in c]),
            (NAMES["task"],),
        ),
        ("a task started for another run", overrides_with(set_env("RUN_ID", "20261030T000000Z-00000000")), (NAMES["task"],)),
        ("a task that fetched unapproved code", overrides_with(set_env("CODE_SHA256", "f" * 64)), (NAMES["task"],)),
        (
            "a task of another layer's definition",
            lambda e, o: edit(
                e.root / "describe.json", lambda d: d["tasks"][0].update(taskDefinitionArn=d["tasks"][0]["taskDefinitionArn"].replace("_job_", "_job_x"))
            ),
            (NAMES["task"],),
        ),
        (
            "approved code that is not the code the run unpacked",
            lambda e, o: edit(e.root / "approval.json", lambda d: d.update(code_sha256="e" * 64)),
            (NAMES["identity"], NAMES["task"]),
        ),
        (
            "a run approved after its task was created",
            lambda e, o: edit(e.root / "approval.json", lambda d: d.update(approved_at="2026-10-31T18:00:00Z")),
            (NAMES["approved"],),
        ),
        (
            "a container exited nonzero",
            lambda e, o: edit(e.root / "describe.json", lambda d: d["tasks"][0]["containers"][3].update(exitCode=1)),
            (NAMES["exits"],),
        ),
        ("a container has no result", lambda e, o: edit(e.root / "describe.json", lambda d: d["tasks"][0]["containers"].pop()), (NAMES["exits"],)),
        ("the task is still running", lambda e, o: edit(e.root / "describe.json", lambda d: d["tasks"][0].update(lastStatus="RUNNING")), (NAMES["exits"],)),
        ("only one table of the contract", lambda e, o: None, (NAMES["sets"],)),
        (
            "the commit misses a snapshot ID",
            lambda e, o: edit(e.evidence / "publish/commit.json", lambda d: d["snapshot_set"].pop(sorted(d["snapshot_set"])[0])),
            (NAMES["snapshots"], NAMES["sets"]),
        ),
        (
            "another namespace in the candidates",
            lambda e, o: rewrite(e, "publish/candidates.json", lambda d: d.update(namespace="silver_e2e")),
            (NAMES["namespace"],),
        ),
        (
            "a fixture mode in the candidates",
            lambda e, o: rewrite(e, "publish/candidates.json", lambda d: d.update(mode="publish_fixture")),
            (NAMES["namespace"],),
        ),
        ("the manifest of another run", lambda e, o: copy_from(e, o, "publish/export/manifest.json"), (NAMES["export"],)),
        ("the consumer result of another run", lambda e, o: copy_from(e, o, "publish/consumer_result.json"), (NAMES["export"],)),
        (
            "a stale export digest in the candidates",
            lambda e, o: edit(e.evidence / "publish/candidates.json", lambda d: d.update(export_manifest_sha256="0" * 64)),
            (NAMES["export"],),
        ),
        ("a sealed identity that differs", lambda e, o: rewrite(e, "publish/candidates.json", lambda d: d.update(sealed_sha256="0" * 64)), (NAMES["export"],)),
        ("the commit record of another run", lambda e, o: copy_from(e, o, "publish/commit.json"), (NAMES["commit"], NAMES["snapshots"])),
        ("the reconcile record is not a pass", lambda e, o: rewrite(e, "publish/reconcile.json", lambda d: d.update({"pass": False})), (NAMES["reconcile"],)),
        (
            "a row count differs from its export",
            lambda e, o: rewrite(e, "publish/reconcile.json", lambda d: d["tables"][sorted(d["tables"])[1]].update(rows=99)),
            (NAMES["rows"],),
        ),
        (
            "a fingerprint differs from its export",
            lambda e, o: rewrite(e, "publish/reconcile.json", lambda d: d["tables"][sorted(d["tables"])[1]].update(fingerprint="5")),
            (NAMES["fingerprints"],),
        ),
        (
            "an invalid fingerprint",
            lambda e, o: rewrite(
                e, "publish/reconcile.json", lambda d: d["tables"][sorted(d["tables"])[0]].update(fingerprint=MARKER, export_fingerprint=MARKER)
            ),
            (NAMES["fingerprints"],),
        ),
        ("the backup was not read back", lambda e, o: rewrite(e, "publish/backup.json", lambda d: d.update(read_back=False)), (NAMES["backup"],)),
        ("the commit is not committed", lambda e, o: edit(e.evidence / "publish/commit.json", lambda d: d.update(state="pending")), (NAMES["commit"],)),
        (
            "a listing of the same size but other versions",
            lambda e, o: [shutil.copyfile(o.root / name, e.root / name) for name in ("before.json", "after.json")],
            (NAMES["listing"], NAMES["scope"]),
        ),
        (
            "a delete marker added to the input prefix during the task",
            lambda e, o: edit(
                e.root / "after.json",
                lambda d: d.update(deleted=[*(d.get("deleted") or []), {"key": f"{acceptance.INPUT_PREFIX[e.spec.layer]}f00001", "version": "d1"}]),
            ),
            (NAMES["same"], NAMES["scope"]),
        ),
        (
            "a historical delete marker that is another one than the pin saw",
            lambda e, o: [edit(e.root / name, lambda d: d["deleted"][0].update(version="other")) for name in ("before.json", "after.json")],
            (NAMES["listing"],),
        ),
        (
            "an init override that carries a variable",
            lambda e, o: edit(
                e.root / "describe.json",
                lambda d: d["tasks"][0]["overrides"]["containerOverrides"][0].update(environment=[{"name": "RUN_ID", "value": e.spec.run}]),
            ),
            (NAMES["task"],),
        ),
        (
            "an input version changed during the task",
            lambda e, o: edit(e.root / "after.json", lambda d: d["versions"][0].update(version="changed")),
            (NAMES["same"], NAMES["scope"], NAMES["removed"]),
        ),
        (
            "the task wrote outside its prefixes",
            lambda e, o: edit(e.root / "after.json", lambda d: d["versions"].append({"key": "lakehouse/other/x", "version": "z", "etag": '"z"', "bytes": 1})),
            (NAMES["scope"],),
        ),
        ("a version disappeared", lambda e, o: edit(e.root / "after.json", lambda d: d["versions"].pop(-3)), (NAMES["removed"],)),
        ("a task of the job is still running", lambda e, o: write(e.root / "running.json", {"taskArns": ["arn:other"]}), (NAMES["running"],)),
        ("the host is still on", lambda e, o: edit(e.root / "host.json", lambda d: d.update(group_instances=1)), (NAMES["host"],)),
    ]


def layer_cases(layer: str) -> list[Case]:
    if layer == "silver":
        return [
            ("the pin misses a bronze table", lambda e, o: edit(e.evidence / "pins.json", lambda d: d.pop(sorted(d)[0])), (NAMES["bronze"],)),
            (
                "the silver record names another namespace",
                lambda e, o: edit(e.evidence / "silver_snapshot.json", lambda d: d.update(namespace="silver_e2e")),
                (NAMES["namespace"], NAMES["record"]),
            ),
            ("the silver record of another run", lambda e, o: copy_from(e, o, "silver_snapshot.json"), (NAMES["record"],)),
        ]
    gold_input = "silver_pins.json"
    return [
        ("a gold pin of another run", lambda e, o: edit(e.evidence / gold_input, lambda d: d.update(run_id="20261030T000000Z-00000000")), (NAMES["pin"],)),
        ("a gold pin with a fixture namespace", lambda e, o: edit(e.evidence / gold_input, lambda d: d.update(namespace="silver_e2e")), (NAMES["pin"],)),
        (
            "a gold pin that names another silver run",
            lambda e, o: edit(e.evidence / gold_input, lambda d: d.update(source_run_id=OTHER_SILVER_RUN)),
            (NAMES["producer_set"], NAMES["signed"], NAMES["task"]),
        ),
        (
            "a gold task that names another input run",
            lambda e, o: edit(
                e.root / "describe.json",
                lambda d: [
                    i.update(value=OTHER_SILVER_RUN)
                    for c in d["tasks"][0]["overrides"]["containerOverrides"]
                    for i in c.get("environment", [])
                    if i["name"] == "INPUT_RUN_ID"
                ],
            ),
            (NAMES["task"],),
        ),
        ("an unsigned silver producer", lambda e, o: e.producer.root.joinpath("qualification.signed.json").unlink(), (NAMES["signed"],)),
        (
            "a silver report changed after it was signed",
            lambda e, o: e.producer.root.joinpath("qualification.json").write_text(e.producer.root.joinpath("qualification.json").read_text() + " "),
            (NAMES["signed"],),
        ),
        (
            "a silver record changed after it was signed",
            lambda e, o: edit(e.producer.evidence / "silver_snapshot.json", lambda d: d.update(note="x")),
            (NAMES["signed"],),
        ),
        (
            "matching empty snapshot sets",
            lambda e, o: [
                edit(e.evidence / gold_input, lambda d: d.update(snapshot_set={})),
                edit(e.producer.evidence / "silver_snapshot.json", lambda d: d.update(snapshot_set={})),
            ],
            (NAMES["pin"], NAMES["signed"]),
        ),
    ]


def cases(work: Path, layer: str) -> dict[str, bool]:
    checks: dict[str, bool] = {}
    for label, change, expected in [*common_cases(), *layer_cases(layer)]:
        tables = (CONTRACT_TABLES[layer][0],) if label == "only one table of the contract" else None
        env, other = scenario(work, layer, label, tables)
        change(env, other)
        code, failed, printed = accept(env)
        refused = code == 1 and not (env.root / "qualification.json").exists()  # an input that cannot be read ends the command with a fixed error and no report
        checks[f"{layer}: {label}"] = (set(expected) <= set(failed) or refused) and code == 1 and MARKER not in printed
        checks[f"{layer}: {label}: nothing is signed"] = (sign(env) == 1) if (env.root / "qualification.json").exists() else True
    return checks


def good_cases(work: Path) -> dict[str, bool]:
    checks: dict[str, bool] = {}
    silver = make(work, "good_silver", spec("silver", SILVER_RUN, "a"))
    gold = make(work, "good_gold", spec("gold", GOLD_RUN, "c"), silver)
    for env in (silver, gold):
        layer = env.spec.layer
        report = json.loads((env.root / "qualification.json").read_text())
        signature = json.loads((env.root / "qualification.signed.json").read_text())
        consumed = set(report["file_sha256"])
        wanted = {f"publish/{name}.json" for name in ("candidates", "reconcile", "backup", "commit", "consumer_result")} | {
            "layer_run.json",
            "publish/export/manifest.json",
            "approval",
            "describe",
            "before",
            "after",
        }
        wanted |= (
            {"pins.json", "silver_snapshot.json"}
            if layer == "silver"
            else {"silver_pins.json", "input/silver_snapshot.json", "input/report", "input/signature"}
        )
        checks[f"{layer}: the good run passes every check and is signed"] = report["passed"] is True and signature["report_sha256"] == digest(
            env.root / "qualification.json"
        )
        checks[f"{layer}: the report hashes every consumed record ({sorted(wanted - consumed)})"] = wanted <= consumed
        checks[f"{layer}: each hash is the hash of the file's bytes"] = all(
            report["file_sha256"][name] == digest(env.evidence / name) for name in wanted if (env.evidence / name).is_file()
        )
        code, _, _ = run_cli(["sign", "--report", str(env.root / "qualification.json"), "--owner", "Owner"], {"CONFIRM_SIGN": signature["confirmation"]})
        checks[f"{layer}: a second signature is refused"] = code == 1
        wrong = env.root / "wrong.json"
        wrong.write_text((env.root / "qualification.json").read_text())
        checks[f"{layer}: a wrong confirmation does not sign"] = (
            run_cli(["sign", "--report", str(wrong), "--owner", "Owner"], {"CONFIRM_SIGN": "sign-other"})[0] == 1
        )
    return checks


def input_cases(work: Path) -> dict[str, bool]:
    """Inputs that are links, not records or full of text: refused with a fixed error that echoes nothing from them."""
    checks: dict[str, bool] = {}
    env, other = scenario(work, "silver", "inputs")
    link = env.evidence / "publish/commit.json"
    target = other.evidence / "publish/commit.json"
    link.unlink()
    link.symlink_to(target)
    code, failed, printed = accept(env)
    checks["a linked evidence file is refused and no report is written"] = code == 1 and not (env.root / "qualification.json").exists() and "link" in printed
    env, other = scenario(work, "silver", "folder link")
    moved = env.root / "evidence_real"
    env.evidence.rename(moved)
    env.evidence.symlink_to(moved)
    code, failed, printed = accept(env)
    checks["a linked evidence folder is refused"] = code == 1 and not (env.root / "qualification.json").exists()
    env, other = scenario(work, "silver", "array")
    (env.evidence / "publish/commit.json").write_text("[1]")
    code, failed, printed = accept(env)
    checks["a record that is not an object is refused with a fixed error"] = (
        code == 1 and not (env.root / "qualification.json").exists() and "[1]" not in printed
    )
    env, other = scenario(work, "silver", "fingerprint text")
    rewrite(env, "publish/reconcile.json", lambda d: d["tables"][sorted(d["tables"])[0]].update(fingerprint=MARKER, export_fingerprint=MARKER))
    code, failed, printed = accept(env)
    checks["an invalid fingerprint fails the report and is not echoed into it"] = code == 1 and NAMES["fingerprints"] in failed and MARKER not in printed
    env, other = scenario(work, "silver", "reason text")
    edit(env.root / "describe.json", lambda d: d["tasks"][0]["containers"][2].update(exitCode=1, reason="SECRET-REASON-TEXT"))
    path = env.root / "describe.json"
    code, printed, error = run_cli(["task", "--layer", "silver", "--describe", str(path)])
    checks["the task check does not echo a container reason"] = code == 1 and "SECRET-REASON-TEXT" not in printed + error
    return checks


def command_cases(work: Path) -> dict[str, bool]:
    env, _ = scenario(work, "silver", "commands")
    code = "a" * 64
    approval = env.root / "new_approval.json"
    approve = ["approve", "--layer", "silver", "--run", SILVER_RUN, "--code-sha256", code, "--out", str(approval)]
    checks = {"approve: without the typed text nothing is written": run_cli(approve)[0] == 1 and not approval.exists()}
    text = f"approve-silver-{SILVER_RUN}-{code[:12]}"
    checks["approve: the typed text writes the approval"] = run_cli(approve, {"CONFIRM_APPROVE": text})[0] == 0 and approval.exists()
    checks["approve: a second approval does not overwrite"] = run_cli(approve, {"CONFIRM_APPROVE": text})[0] == 1
    tracked, outside = ROOT / "docs/layer_acceptance_report_must_not_exist.json", Path(tempfile.gettempdir()) / "layer_acceptance_outside_the_repository.json"
    checks["report: a path that Git tracks is refused"] = run_cli(report_arguments(env, tracked))[0] == 1 and not tracked.exists()
    checks["report: a path outside the repository is refused"] = run_cli(report_arguments(env, outside))[0] == 1 and not outside.exists()
    checks["report: Git ignores the data and plans folders"] = GIT_IGNORED(ROOT / "data/e2e/x.json") and GIT_IGNORED(ROOT / "plans/x/signed.json")
    return checks


def plan_events(layer: str, **changes: Any) -> list[dict[str, str]]:
    """The log events of a dry run publish stream: the lines the task prints, then the whole plan as one JSON line."""
    plan: dict[str, Any] = {
        "dry_run": True,
        "layer": layer,
        "run": SILVER_RUN,
        "mode": "real",
        "namespace": layer,
        "summary": {"tables": 2},
        "tables": [],
        "blocked": [],
        "unknown": [],
        "catalog_error": "",
    } | changes
    lines = [
        f"dry run plan for {layer} run {SILVER_RUN} (mode real, namespace {layer}): nothing was written",
        "CREATE    silver.hospital_measures: write 10 rows",
    ]
    lines += [f"UNKNOWN   {item}" for item in plan["unknown"]] + [f"BLOCKED   {item}" for item in plan["blocked"]]
    lines += [f"CATALOG   the catalog could not be read: {plan['catalog_error']}"] if plan["catalog_error"] else []
    return [{"timestamp": "1", "message": line} for line in [*lines, "DRY_RUN_PLAN " + json.dumps(plan, sort_keys=True, separators=(",", ":"))]]


def plan_cases(work: Path) -> dict[str, bool]:
    """The plan check reads this run's plan from the saved log events and refuses an unknown, a catalog error, a blocked plan and another run."""
    folder = work / "plans"
    folder.mkdir()
    checks: dict[str, bool] = {}
    cases_ = {
        "a complete plan": (plan_events("silver"), 0),
        "an unknown table": (plan_events("silver", unknown=["silver.state_measures"]), 1),
        "a catalog error": (plan_events("silver", catalog_error="timeout"), 1),
        "a blocked publish": (plan_events("silver", blocked=["this run is already committed"]), 1),
        "another run's plan": (plan_events("silver", run="20261030T000000Z-00000000"), 1),
        "a plan for another layer": (plan_events("gold"), 1),
        "no plan line": (plan_events("silver")[:-1], 1),
    }
    for label, (events, expected) in cases_.items():
        path = folder / (re.sub("[^a-z]+", "_", label) + ".json")
        path.write_text(json.dumps({"events": events}))
        code, _, _ = run_cli(["plan", "--layer", "silver", "--run", SILVER_RUN, "--log", str(path)])
        checks[f"plan: {label}"] = code == expected
    return checks


def static_cases() -> dict[str, bool]:
    """The command's constants equal the files they restate."""
    writer = (ROOT / "infra/iam/job_writer.tf").read_text()
    prefixes = {
        layer: {f"lakehouse/{name}/" for name in re.findall(r'"([a-z_]+)"', body)}
        for layer, body in re.findall(r"(silver|gold) = \{\s*prefixes\s*=\s*\[([^\]]*)\]", writer)
    }
    broker = json.loads(acceptance.JOBS_FILE.read_text())["jobs"]
    return {
        "the write prefixes equal the layer writer roles": all(
            set(acceptance.WRITE_PREFIXES[layer]) - {"metadata/dbt/"} == prefixes[layer] for layer in acceptance.LAYERS
        ),
        "the gold role may also write the dbt metadata": '"${local.bronze_bucket_arn}/metadata/dbt/*"' in writer,
        "the expected containers are init and the broker table's containers": all(
            acceptance.expected_containers(layer) == ["init", *broker[layer]["containers"]] for layer in acceptance.LAYERS
        ),
        "the contract tables are read from the frozen revision": len(CONTRACT_TABLES["silver"]) > 1 and len(CONTRACT_TABLES["gold"]) > 1,
    }


def group(name: str, function: Callable[..., dict[str, bool]], *arguments: Any) -> dict[str, bool]:
    """Run one group of cases. A good run that fails to build is one failed check and not a crash."""
    try:
        return function(*arguments)
    except RuntimeError as error:
        return {f"{name}: {error}": False}


def main() -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    parser.add_argument("--out", type=Path, required=True, help="a new directory for the report")
    args = parser.parse_args()
    out = args.out.expanduser().absolute()
    out.mkdir(mode=0o700, parents=True, exist_ok=False)
    work = Path(tempfile.mkdtemp(prefix="layer_acceptance_", dir=out))
    results: dict[str, dict[str, bool]] = {"static": static_cases()}
    try:
        with mock.patch.object(acceptance, "ignored", lambda path: str(path).startswith(str(work))):
            results["good"] = group("good", good_cases, work)
            for layer in acceptance.LAYERS:
                results[f"defects {layer}"] = group(f"defects {layer}", cases, work, layer)
            results["inputs"] = group("inputs", input_cases, work)
            results["commands"] = command_cases(work)
            results["plan"] = plan_cases(work)
    finally:
        shutil.rmtree(work, ignore_errors=True)
    passed = all(all(checks.values()) for checks in results.values())
    (out / "report.json").write_text(json.dumps({"passed": passed, "results": results}, indent=2, sort_keys=True) + "\n")
    for name, checks in results.items():
        for check, ok in checks.items():
            sys.stdout.write(f"{'PASS' if ok else 'FAIL'} {name}: {check}\n")
    total = sum(len(checks) for checks in results.values())
    sys.stdout.write(f"layer acceptance E2E: {sum(ok for checks in results.values() for ok in checks.values())} of {total} checks passed\n")
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
