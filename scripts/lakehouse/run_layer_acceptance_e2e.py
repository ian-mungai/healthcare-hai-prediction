"""E2E of the owner's layer acceptance command: two real-shaped runs per layer, a good record that passes and each planted defect refused by name.

    .venv/bin/python -m scripts.lakehouse.run_layer_acceptance_e2e --out <NEW_DIRECTORY>

Every run, code digest, launch record and instance ID in this suite is SYNTHETIC and stays in its isolated SYNTHETIC fixture folder.
The records have the shapes the driver writes (``layer_run.bundle``, ``gold_export.export_sealed``, ``gold_publish.publish_candidates``,
``reconcile_candidates``, ``catalog_backup``, ``commit_receipt``, ``silver_publish.record_committed``, ``feast_job.apply`` and
``feast_materialize.run_materialize``) with the tables of the frozen publish contract.
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
import tarfile
import tempfile
from collections.abc import Callable
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any
from unittest import mock

from scripts.features.publication_reads import Publication
from scripts.lakehouse import layer_acceptance as acceptance
from scripts.lakehouse import layer_run, pins
from scripts.process import run_command

ROOT = Path(__file__).resolve().parents[2]
GIT_IGNORED = acceptance.ignored  # the real check, before the cases replace it with a rule about the scratch folder
SILVER_RUN, OTHER_SILVER_RUN, GOLD_RUN, OTHER_GOLD_RUN, FEATURES_RUN, OTHER_FEATURES_RUN = (
    "20261031T120000Z-abcdef12",
    "20261031T130000Z-fedcba98",
    "20261101T090000Z-0a1b2c3d",
    "20261101T100000Z-3d2c1b0a",
    "20261102T090000Z-1a2b3c4d",
    "20261102T100000Z-4d3c2b1a",
)
ACCOUNT = "111111111111"
INPUTS = 1500  # more than one page of 1000, so the page structure of the digest is exercised
MARKER = "9x<bad>-fingerprint"
HOST = "i-0123456789abcdef0"
ReportChange = Callable[[dict[str, Any]], object]


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


def frozen_text(path: str) -> str:
    return run_command("git", ["show", f"{REVISION}:{path}"], cwd=ROOT, timeout=30).stdout


def frozen(path: str) -> Any:
    return json.loads(frozen_text(path))


ARCHIVE_PATHS = (*acceptance.CONTRACT.values(), acceptance.BRONZE_MAP, str(pins.SOURCES))


@functools.cache
def archive_for(tag: str, commit: str, edited: str = "") -> bytes:
    """The staged ``code.tar.gz`` of a run as ``scripts.lakehouse.code_archive`` writes it for the contract files: the commit ID is the global pax comment and
    a member named for the tag makes the digest differ between runs. ``edited`` names a contract file whose text the archive changes."""
    stream = io.BytesIO()
    with tarfile.open(fileobj=stream, mode="w:gz", format=tarfile.PAX_FORMAT, pax_headers={"comment": commit}) as tar:
        for path in (*ARCHIVE_PATHS, f"run_tag/{tag}"):
            body = (frozen_text(path) if path in ARCHIVE_PATHS else tag).encode()
            if path == edited:
                document = json.loads(body)
                document["publish"]["tables"] = dict(list(document["publish"]["tables"].items())[:-1])
                body = json.dumps(document).encode()
            info = tarfile.TarInfo(path)
            info.size = len(body)
            tar.addfile(info, io.BytesIO(body))
    return stream.getvalue()


CONTRACT_TABLES = {
    "silver": sorted(frozen("data_contracts/lakehouse/silver_publish.json")["publish"]["tables"]),
    "gold": sorted(frozen("data_contracts/lakehouse/gold_contract.json")["publish"]["tables"]),
    "features": sorted(frozen("data_contracts/lakehouse/features_publish.json")["publish"]["tables"]),
}
BRONZE_TABLES = pins.pinned_tables(
    {entry["table"] for entry in frozen("config/lakehouse/bronze_tables.json")["tables"]}, pins.declared_sources(frozen_text(str(pins.SOURCES)))
)


@dataclass(frozen=True)
class Spec:
    """One run: its layer, its ID, a tag that makes its digests differ from another run's and the tables it publishes."""

    layer: str
    run: str
    tag: str
    tables: tuple[str, ...]
    code: str = ""
    revision: str = REVISION
    edited: str = ""

    @property
    def archive(self) -> bytes:
        return archive_for(self.tag, self.revision, self.edited)

    @property
    def code_sha256(self) -> str:
        return self.code or hashlib.sha256(self.archive).hexdigest()

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

    @property
    def source(self) -> Env:
        """The run of the layer below that this run reads. A silver run has none."""
        if self.producer is None:
            raise RuntimeError("this run has no producer")
        return self.producer


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
    namespace = {"silver": "silver", "gold": "gold", "features": "features"}[item.layer]
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
            "git_revision": item.revision,
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
        source = {"gold": "silver_snapshot.json", "features": "publish/commit.json"}[item.layer]
        producer = json.loads((env.producer.evidence / source).read_text()) if env.producer else {"run_id": "", "snapshot_set": {}}
        write(
            folder / acceptance.PIN_FILE[item.layer],
            {
                "schema": 1,
                "run_id": run,
                "mode": "real",
                "namespace": "silver" if item.layer == "gold" else "gold",
                "source_run_id": producer["run_id"],
                "snapshot_set": producer["snapshot_set"],
            },  # silver_publish.pin and features_input.pin
        )
    if item.layer == "features":
        write(folder / "features_snapshot.json", {"schema": 1, "run_id": run, "mode": "real", "namespace": "features", "snapshot_set": snapshots})
        publication = Publication("features", run, "2026-11-02T09:00:00+00:00", snapshots)
        write(
            folder / "feast_apply.json",
            {  # feast_job.apply
                "schema": 1,
                "step": "apply",
                "run_id": run,
                "mode": "real",
                "namespace": "features",
                "publication": {
                    "run_id": run,
                    "committed_at": publication.committed_at,
                    "tables": len(snapshots),
                    "snapshot_set_sha256": publication.snapshot_set_sha256,
                },
                "feature_schema_sha256": h(item.tag, "schema"),
                "feast_version": "0.66.0",
                "objects": {"entity": 1, "feature_view": 9, "data_source": 9},
                "object_list_sha256": h(item.tag, "objects"),
            },
        )
        write(
            folder / "feast_materialize.json",
            {  # feast_materialize.run_materialize
                "schema": 1,
                "step": "materialize",
                "run_id": run,
                "mode": "real",
                "publication": {"run_id": run, "snapshot_set_sha256": publication.snapshot_set_sha256},
                "feature_schema_sha256": h(item.tag, "schema"),
                "load_id": h(item.tag, "load")[:16],
                "entities": 12,
            },
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
    # describe-tasks lists every container of the task definition: init and the guard have no override and appear by name only
    write_describe(env, {"containerOverrides": [{"name": "init"}, *overrides["containerOverrides"], {"name": "guard"}]})
    write_launch_records(env, task_arn(env))
    (root / "code.sha256").write_text(item.code_sha256 + "\n")
    if hashlib.sha256(item.archive).hexdigest() == item.code_sha256:
        (root / "code.tar.gz").write_bytes(item.archive)
    write(root / "running.json", {"taskArns": []})
    write(root / "host.json", {"desired_capacity": 1, "group_instances": [HOST], "container_instances": [HOST]})  # the host runs while the evidence step does


def task_arn(env: Env) -> str:
    return f"arn:aws:ecs:us-west-2:{ACCOUNT}:task/example_project_cluster_dev/{h(env.spec.tag, 'task')[:32]}"


def write_launch_records(env: Env, arn: str) -> None:
    """The write-once records of the direct ECS launch as ``orchestration.ecs_launch.parse_record`` validates them: the claim of the run and job, then the
    started and end records of its one attempt."""
    run, layer, folder = env.spec.run, env.spec.layer, env.root / "launch"
    common = {"schema_version": 1, "run_id": run, "job": layer, "attempt": 1, "at": "2026-10-31T10:00:00Z"}
    bound = {"code_digest": env.spec.code_sha256, "request_hash": "0" * 64, "input_run": env.producer.spec.run if env.producer else "latest"}
    write(folder / "claim.json", {**common, "event": "claim", "bound": bound, "deadline_seconds": 21600, "acceptance": ["publish"], "need": {}})
    write(folder / "01/started.json", {**common, "event": "started", "task_arn": arn, "job_started_at": "2026-10-31T10:01:00Z"})
    write(folder / "01/end.json", {**common, "event": "end", "outcome": "succeeded", "exit_code": 0, "reason": "the task finished"})


def write_describe(env: Env, overrides: dict[str, Any], definition: str | None = None) -> None:
    layer = env.spec.layer
    containers = [{"name": name, "lastStatus": "STOPPED", "exitCode": 0} for name in acceptance.expected_containers(layer)]
    containers.append(
        {"name": "guard", "lastStatus": "STOPPED", "exitCode": 0}
    )  # the guard container of the task definition, stopped by ECS when the last phase ended
    write(
        env.root / "describe.json",
        {
            "failures": [],
            "tasks": [
                {
                    "taskArn": task_arn(env),
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


def run_cli(arguments: list[str]) -> tuple[int, str, str]:
    out, err = io.StringIO(), io.StringIO()
    with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
        try:
            code = acceptance.main(arguments)
        except SystemExit as stop:
            code = int(stop.code or 0)
        except Exception as crash:  # a crash is not a refusal: it is one failed check with the exit code 70
            code = 70
            err.write(f"crashed: {type(crash).__name__}\n")
    return code, out.getvalue(), err.getvalue()


def report_arguments(env: Env, out: Path) -> list[str]:
    files = {
        "code-digest": "code.sha256",
        "describe": "describe.json",
        "evidence": "evidence",
        "launch-records": "launch",
        "before": "before.json",
        "after": "after.json",
    }
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


def make(work: Path, name: str, item: Spec, producer: Env | None = None, checked: bool = True) -> Env:
    """Build a run and check it, as the evidence step does for the silver run before gold."""
    env = Env(work / name, item, producer)
    build_records(env)
    if checked:
        code, failed, _ = accept(env)
        if code != 0 or failed:
            raise RuntimeError(f"the good {name} run does not pass: {failed}")
    return env


def edit(path: Path, change: Callable[[Any], object]) -> None:
    document = json.loads(path.read_text())
    change(document)
    path.write_text(json.dumps(document, indent=2, sort_keys=True) + "\n")


def edit_each(root: Path, names: tuple[str, ...], change: Callable[[Any], object]) -> None:
    for name in names:
        edit(root / name, change)


def empty_sets(env: Env, pin: str) -> None:
    """The gold pin and the silver record it names both hold an empty snapshot set, so the two sets match."""
    edit(env.evidence / pin, lambda d: d.update(snapshot_set={}))
    edit(env.source.evidence / "silver_snapshot.json", lambda d: d.update(snapshot_set={}))


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
    elif layer == "features":
        source = make(work, f"{slug}_source", spec("silver", SILVER_RUN, "a"))
        producer = make(work, f"{slug}_producer", spec("gold", GOLD_RUN, "c"), source)
    runs = {"silver": (SILVER_RUN, OTHER_SILVER_RUN), "gold": (GOLD_RUN, OTHER_GOLD_RUN), "features": (FEATURES_RUN, OTHER_FEATURES_RUN)}[layer]
    tags = {"silver": "ab", "gold": "cd", "features": "ef"}[layer]
    env = Env(work / slug, spec(layer, runs[0], tags[0], tables), producer)
    build_records(env)
    other = Env(work / f"{slug}_other", spec(layer, runs[1], tags[1], tables), producer)
    build_records(other)
    return env, other


NAMES = {
    "digest": "the staged code digest is the run's: its run records, its launch claim and its task carry it",
    "launch": "the launch records are this run's one launch: the claim binds the input run, the started record names the described task and the end succeeded",
    "task": "the task is this run's: its definition, its overrides and the staged code digest",
    "exits": "every container of the task exited 0 and the task stopped",
    "identity": "the run records belong to this run and layer in real mode, with the staged code",
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
    "bronze": "H3: the pinned table set is the silver-declared bronze tables of the run's revision",
    "pin": "H3: the gold pin is complete: its run, mode, namespace and the silver contract's tables",
    "producer_set": "H3: the pin names the silver run and its complete snapshot set",
    "passed": "the silver producer passed: its report passed and covers its record",
    "features_record": "the features record equals the commit",
    "features_pin": "H3: the features pin is complete: its run, mode, namespace and the gold contract's tables",
    "features_producer_set": "H3: the pin names the gold run and its complete snapshot set",
    "features_passed": "the gold producer passed: its report passed and covers its record",
    "feast_apply": "Feast applied this run's publication: its run, namespace, table count and snapshot set",
    "feast_materialize": "Feast loaded this run's publication into the online store: its run, snapshot set and a positive entity count",
    "same": "H2: the input object versions are the same before and after the task",
    "listing": "H2: the pin recorded the digest and count of the listing taken before the task",
    "scope": "the task added versions only under the layer's prefixes and its own run prefix",
    "removed": "the task removed no version",
    "running": "no task of the job is left running",
    "host": "the host runs alone: its group desired capacity is at most 1 and its one instance is the only active container instance",
}


Case = tuple[str, Callable[[Env, Env], object], tuple[str, ...]]  # the callback's result is ignored: a case may return what its last call returns


def common_cases() -> list[Case]:
    """Defects of either layer: (label, change given the run and another real run, the checks that must fail)."""

    def overrides_with(change: Callable[[dict[str, Any]], object]) -> Callable[[Env, Env], None]:
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
            "a staged code digest that is not the code the run unpacked",
            lambda e, o: e.root.joinpath("code.sha256").write_text("e" * 64 + "\n"),
            (NAMES["identity"], NAMES["task"], NAMES["digest"]),
        ),
        (
            "a staged code digest that is not a digest",
            lambda e, o: e.root.joinpath("code.sha256").write_text("not a digest\n"),
            (NAMES["identity"], NAMES["digest"]),
        ),
        (
            "a launch claim bound to other code",
            lambda e, o: edit(e.root / "launch/claim.json", lambda d: d["bound"].update(code_digest="d" * 64)),
            (NAMES["digest"],),
        ),
        (
            "a launch claim bound to another input run",
            lambda e, o: edit(e.root / "launch/claim.json", lambda d: d["bound"].update(input_run="20261030T000000Z-00000000")),
            (NAMES["launch"],),
        ),
        ("a launch claim of another run", lambda e, o: edit(e.root / "launch/claim.json", lambda d: d.update(run_id=o.spec.run)), (NAMES["launch"],)),
        ("a launch claim of another job", lambda e, o: edit(e.root / "launch/claim.json", lambda d: d.update(job="train")), (NAMES["launch"],)),
        (
            "a launch that started another task",
            lambda e, o: edit(e.root / "launch/01/started.json", lambda d: d.update(task_arn=d["task_arn"][:-1] + "0")),
            (NAMES["launch"],),
        ),
        (
            "a launch that did not succeed",
            lambda e, o: edit(e.root / "launch/01/end.json", lambda d: d.update(outcome="failed", exit_code=1)),
            (NAMES["launch"],),
        ),
        ("an end record of another attempt", lambda e, o: edit(e.root / "launch/01/end.json", lambda d: d.update(attempt=2)), (NAMES["launch"],)),
        ("an end record of another run", lambda e, o: edit(e.root / "launch/01/end.json", lambda d: d.update(run_id=o.spec.run)), (NAMES["launch"],)),
        (
            "a container exited nonzero",
            lambda e, o: edit(e.root / "describe.json", lambda d: d["tasks"][0]["containers"][3].update(exitCode=1)),
            (NAMES["exits"],),
        ),
        ("a container has no result", lambda e, o: edit(e.root / "describe.json", lambda d: d["tasks"][0]["containers"].pop(-2)), (NAMES["exits"],)),
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
            lambda e, o: edit_each(e.root, ("before.json", "after.json"), lambda d: d["deleted"][0].update(version="other")),
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
            "a guard override that carries a variable",
            lambda e, o: edit(
                e.root / "describe.json",
                lambda d: d["tasks"][0]["overrides"]["containerOverrides"][-1].update(environment=[{"name": "RUN_ID", "value": e.spec.run}]),
            ),
            (NAMES["task"],),
        ),
        (
            "a container that is not part of the job",
            lambda e, o: edit(
                e.root / "describe.json", lambda d: d["tasks"][0]["containers"].append({"name": "extra", "lastStatus": "STOPPED", "exitCode": 0})
            ),
            (NAMES["exits"],),
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
        ("a group desired capacity of two", lambda e, o: edit(e.root / "host.json", lambda d: d.update(desired_capacity=2)), (NAMES["host"],)),
        (
            "a second container instance",
            lambda e, o: edit(e.root / "host.json", lambda d: d["container_instances"].append("i-0fedcba9876543210")),
            (NAMES["host"],),
        ),
        (
            "a container instance that is not the host",
            lambda e, o: edit(e.root / "host.json", lambda d: d.update(container_instances=["i-0fedcba9876543210"])),
            (NAMES["host"],),
        ),
        ("no container instance", lambda e, o: edit(e.root / "host.json", lambda d: d.update(container_instances=[])), (NAMES["host"],)),
        ("a second group instance", lambda e, o: edit(e.root / "host.json", lambda d: d["group_instances"].append("i-0fedcba9876543210")), (NAMES["host"],)),
        ("a host status without its instance lists", lambda e, o: edit(e.root / "host.json", lambda d: d.pop("container_instances")), (NAMES["host"],)),
        ("a host status with a text capacity", lambda e, o: edit(e.root / "host.json", lambda d: d.update(desired_capacity="1")), (NAMES["host"],)),
    ]


def features_cases() -> list[Case]:
    """The input check against the signed gold run, the features record and the two Feast records."""
    pin, commit = "gold_pins.json", "publish/commit.json"

    def another_digest(d: dict[str, Any]) -> None:
        d["publication"]["snapshot_set_sha256"] = "0" * 64

    def another_run(d: dict[str, Any]) -> None:
        d["publication"]["run_id"] = OTHER_FEATURES_RUN

    def empty_gold_sets(e: Env, o: Env) -> None:
        edit(e.evidence / pin, lambda d: d.update(snapshot_set={}))
        edit(e.source.evidence / commit, lambda d: d.update(snapshot_set={}))

    return [
        (
            "a features pin of another run",
            lambda e, o: edit(e.evidence / pin, lambda d: d.update(run_id="20261030T000000Z-00000000")),
            (NAMES["features_pin"],),
        ),
        ("a features pin with a fixture namespace", lambda e, o: edit(e.evidence / pin, lambda d: d.update(namespace="gold_e2e")), (NAMES["features_pin"],)),
        (
            "a features pin that misses a gold table",
            lambda e, o: edit(e.evidence / pin, lambda d: d["snapshot_set"].pop(sorted(d["snapshot_set"])[0])),
            (NAMES["features_pin"], NAMES["features_producer_set"]),
        ),
        (
            "a features pin with another snapshot of a gold table",
            lambda e, o: edit(e.evidence / pin, lambda d: d["snapshot_set"].update({sorted(d["snapshot_set"])[0]: 987654})),
            (NAMES["features_producer_set"],),
        ),
        (
            "a features pin that names another gold run",
            lambda e, o: edit(e.evidence / pin, lambda d: d.update(source_run_id=OTHER_GOLD_RUN)),
            (NAMES["features_producer_set"], NAMES["features_passed"], NAMES["task"]),
        ),
        (
            "a features task that names another input run",
            lambda e, o: edit(
                e.root / "describe.json",
                lambda d: [
                    i.update(value=OTHER_GOLD_RUN)
                    for c in d["tasks"][0]["overrides"]["containerOverrides"]
                    for i in c.get("environment", [])
                    if i["name"] == "INPUT_RUN_ID"
                ],
            ),
            (NAMES["task"],),
        ),
        (
            "a gold producer report that failed",
            lambda e, o: edit(e.source.root / "qualification.json", lambda d: d.update(passed=False)),
            (NAMES["features_passed"],),
        ),
        (
            "a gold commit record changed after the gold report",
            lambda e, o: edit(e.source.evidence / commit, lambda d: d.update(note="x")),
            (NAMES["features_passed"],),
        ),
        (
            "a gold candidates record changed after the gold report",
            lambda e, o: edit(e.source.evidence / "publish/candidates.json", lambda d: d.update(note="x")),
            (NAMES["features_passed"],),
        ),
        (
            "a gold commit that is not committed",
            lambda e, o: edit(e.source.evidence / commit, lambda d: d.update(state="pending")),
            (NAMES["features_producer_set"],),
        ),
        ("matching empty gold snapshot sets", empty_gold_sets, (NAMES["features_pin"], NAMES["features_passed"])),
        ("the features record of another run", lambda e, o: copy_from(e, o, "features_snapshot.json"), (NAMES["features_record"],)),
        (
            "the features record names another namespace",
            lambda e, o: edit(e.evidence / "features_snapshot.json", lambda d: d.update(namespace="features_e2e")),
            (NAMES["features_record"],),
        ),
        ("Feast applied another run's publication", lambda e, o: edit(e.evidence / "feast_apply.json", another_run), (NAMES["feast_apply"],)),
        ("Feast applied another snapshot set", lambda e, o: edit(e.evidence / "feast_apply.json", another_digest), (NAMES["feast_apply"],)),
        (
            "Feast applied a fixture namespace",
            lambda e, o: edit(e.evidence / "feast_apply.json", lambda d: d.update(namespace="features_e2e")),
            (NAMES["feast_apply"],),
        ),
        (
            "the apply record has no views",
            lambda e, o: edit(e.evidence / "feast_apply.json", lambda d: d["objects"].update(feature_view=0)),
            (NAMES["feast_apply"],),
        ),
        ("the apply record is missing", lambda e, o: e.evidence.joinpath("feast_apply.json").unlink(), ()),
        ("the materialize record is missing", lambda e, o: e.evidence.joinpath("feast_materialize.json").unlink(), ()),
        ("Feast loaded another snapshot set", lambda e, o: edit(e.evidence / "feast_materialize.json", another_digest), (NAMES["feast_materialize"],)),
        ("Feast loaded another run's publication", lambda e, o: edit(e.evidence / "feast_materialize.json", another_run), (NAMES["feast_materialize"],)),
        ("Feast loaded no entities", lambda e, o: edit(e.evidence / "feast_materialize.json", lambda d: d.update(entities=0)), (NAMES["feast_materialize"],)),
    ]


def layer_cases(layer: str) -> list[Case]:
    if layer == "features":
        return features_cases()
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
            (NAMES["producer_set"], NAMES["passed"], NAMES["task"]),
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
        ("a silver producer report that failed", lambda e, o: edit(e.source.root / "qualification.json", lambda d: d.update(passed=False)), (NAMES["passed"],)),
        (
            "a silver record changed after the silver report",
            lambda e, o: edit(e.source.evidence / "silver_snapshot.json", lambda d: d.update(note="x")),
            (NAMES["passed"],),
        ),
        (
            "matching empty snapshot sets",
            lambda e, o: empty_sets(e, gold_input),
            (NAMES["pin"], NAMES["passed"]),
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
    return checks


def host_twin_cases(work: Path) -> dict[str, bool]:
    """A run of the host twin (``<project>_<env>_job_<layer>_host``) that the direct ECS launch started from a DAG step: its definition and its launch
    records under ``lakehouse/launch_records/<run>/`` pass, while any other family suffix and any write outside the run's prefixes stay refused."""

    def definition(suffix: str) -> Callable[[Env, Env], None]:
        def apply(env: Env, other: Env) -> None:
            edit(
                env.root / "describe.json",
                lambda d: d["tasks"][0].update(taskDefinitionArn=re.sub(r"_job_([a-z]+):[0-9]+$", rf"_job_\1{suffix}:3", d["tasks"][0]["taskDefinitionArn"])),
            )

        return apply

    def written(*keys: str) -> Callable[[Env, Env], None]:
        def apply(env: Env, other: Env) -> None:
            run, layer = env.spec.run, env.spec.layer
            names = [key.format(run=run, other=other.spec.run, layer=layer) for key in keys]
            edit(env.root / "after.json", lambda d: d["versions"].extend({"key": key, "version": "lr", "etag": '"l"', "bytes": 1} for key in names))

        return apply

    def both(*changes: Callable[[Env, Env], None]) -> Callable[[Env, Env], None]:
        def apply(env: Env, other: Env) -> None:
            for change in changes:
                change(env, other)

        return apply

    records = ("lakehouse/launch_records/{run}/{layer}/claim.json", "lakehouse/launch_records/{run}/{layer}/01/end.json")
    good = (
        ("a host twin definition", definition("_host")),
        ("the launch records of the run", written(*records)),
        ("a host twin run with its launch records", both(definition("_host"), written(*records))),
        ("the family index key of the run and job", written("lakehouse/launch_records/families/{layer}/{run}.json")),
    )
    planted = (
        ("a definition with another suffix", definition("_hosted"), NAMES["task"]),
        ("a definition with a suffix after host", definition("_host_x"), NAMES["task"]),
        ("a definition of another family suffix", definition("_fargate"), NAMES["task"]),
        ("the launch records of another run without its claim", written("lakehouse/launch_records/{other}/{layer}/01/end.json"), NAMES["scope"]),
        ("a file at the top of the launch records", written("lakehouse/launch_records/claim.json"), NAMES["scope"]),
        ("the family index key of another run", written("lakehouse/launch_records/families/{layer}/{other}.json"), NAMES["scope"]),
        ("the family index key of another job", written("lakehouse/launch_records/families/ingestion/{run}.json"), NAMES["scope"]),
        ("a file beside the family index key of the run", written("lakehouse/launch_records/families/{layer}/{run}.json.x"), NAMES["scope"]),
        ("a prefix that only starts like the launch records", written("lakehouse/launch_records_x/{run}/{layer}/claim.json"), NAMES["scope"]),
    )
    checks: dict[str, bool] = {}
    for layer in ("silver", "gold"):
        for label, change in good:
            env, other = scenario(work, layer, f"host {label}")
            change(env, other)
            code, failed, _ = accept(env)
            checks[f"{layer}: {label} passes every check ({failed})"] = code == 0 and not failed
        for label, change, name in planted:
            env, other = scenario(work, layer, f"host {label}")
            change(env, other)
            code, failed, _ = accept(env)
            checks[f"{layer}: planted {label} is refused by its check"] = code == 1 and name in failed
    return checks


OTHER_RUN_JOB = "ingestion"
OTHER_RUN_ID = "20261112T001500Z-c0ffee01"
OTHER_RUN_ATTRIBUTED = 10  # the Nov 11 shape: 4 staged inputs, 5 launch records with the claim and 1 family index key


def other_run_keys(run: str = OTHER_RUN_ID, job: str = OTHER_RUN_JOB, claim: bool = True) -> list[str]:
    """The keys that a queued run of another family wrote while a layer task ran: its staged inputs, its launch records and its family index key."""
    stage = [f"lakehouse/run_stage/{run}/inputs/{name}" for name in ("code.tar.gz", "code.sha256", "bronze_request.json", "approval.json")]
    records = [f"lakehouse/launch_records/{run}/{job}/{name}" for name in ("claim.json", "01/started.json", "01/end.json", "02/started.json", "02/end.json")]
    keys = [*stage, *records, f"lakehouse/launch_records/families/{job}/{run}.json"]
    return keys if claim else [key for key in keys if not key.endswith("/claim.json")]


def add_versions(env: Env, keys: list[str], names: tuple[str, ...] = ("after.json",)) -> None:
    edit_each(env.root, names, lambda d: d["versions"].extend({"key": key, "version": "or", "etag": '"o"', "bytes": 1} for key in keys))


def other_run_cases(work: Path) -> dict[str, bool]:
    """Another run's own writes in the window are listed apart and do not fail the write scope, only when that run's claim record is in the listings. Every
    other key outside the layer's scope still fails, and a run ID that is not the project's grammar never counts."""
    checks: dict[str, bool] = {}

    def run(layer: str, label: str, keys: list[str], before: list[str] | None = None) -> tuple[int, list[str], dict[str, Any]]:
        env, _ = scenario(work, layer, f"other run {label}")
        if before:
            add_versions(env, before, ("before.json", "after.json"))
        add_versions(env, keys)
        code, failed, _ = accept(env)
        written = env.root / "qualification.json"
        return code, failed, json.loads(written.read_text()) if written.is_file() else {}

    def listed(document: dict[str, Any], name: str) -> tuple[int, list[str]]:
        found = document.get(name) or {}
        return found.get("count", -1), found.get("keys", [])

    for layer in ("silver", "gold"):
        keys = other_run_keys()
        code, failed, document = run(layer, "with its claim", keys)
        checks[f"{layer}: the keys of a queued run with its claim pass and are listed as its writes"] = (
            code == 0
            and not failed
            and listed(document, "other_runs_writes") == (OTHER_RUN_ATTRIBUTED, sorted(keys))
            and listed(document, "out_of_scope") == (0, [])
        )
        code, failed, document = run(layer, "claim before", [key for key in keys if "/run_stage/" in key], before=[keys[4]])
        checks[f"{layer}: a claim that the before listing holds attributes the run's staged inputs"] = (
            code == 0 and not failed and listed(document, "other_runs_writes")[0] == 4 and listed(document, "out_of_scope") == (0, [])
        )
        code, failed, document = run(layer, "no claim", other_run_keys(claim=False))
        checks[f"{layer}: the same keys without the run's claim fail and stay out of scope"] = (
            code == 1
            and failed == [NAMES["scope"]]
            and listed(document, "out_of_scope")[0] == OTHER_RUN_ATTRIBUTED - 1
            and listed(document, "other_runs_writes") == (0, [])
        )
        sibling = f"lakehouse/layer_runs/{layer}/{OTHER_RUN_ID}/layer_run.json"
        code, failed, document = run(layer, "outside the run prefixes", [*keys, sibling])
        checks[f"{layer}: a key under another run's name outside run_stage and launch_records fails and is the only out-of-scope key"] = (
            code == 1
            and failed == [NAMES["scope"]]
            and listed(document, "out_of_scope") == (1, [sibling])
            and listed(document, "other_runs_writes")[0] == OTHER_RUN_ATTRIBUTED
        )
        for label, bad in (
            ("an uppercase hex suffix", "20261112T001500Z-C0FFEE01"),
            ("a short suffix", "20261112T001500Z-c0ffee0"),
            ("no suffix", "20261112T001500Z"),
            ("a path name", "x"),
        ):
            code, failed, document = run(layer, f"malformed {label}", other_run_keys(bad))
            checks[f"{layer}: a run ID with {label} fails even with its claim"] = (
                code == 1
                and failed == [NAMES["scope"]]
                and listed(document, "out_of_scope")[0] == OTHER_RUN_ATTRIBUTED
                and listed(document, "other_runs_writes") == (0, [])
            )
        own = "lakehouse/other/x"
        code, failed, document = run(layer, "with an own out-of-scope write", [*keys, own])
        checks[f"{layer}: the layer's own out-of-scope write still fails beside an attributed run"] = (
            code == 1
            and failed == [NAMES["scope"]]
            and listed(document, "out_of_scope") == (1, [own])
            and listed(document, "other_runs_writes")[0] == OTHER_RUN_ATTRIBUTED
        )
        other_claim = other_run_keys("20261112T002500Z-c0ffee02", "ingestion", claim=False)
        code, failed, document = run(layer, "claim of another run", [*keys, *other_claim])
        checks[f"{layer}: a claim attributes only its own run's keys"] = (
            code == 1
            and failed == [NAMES["scope"]]
            and listed(document, "out_of_scope") == (len(other_claim), sorted(other_claim))
            and listed(document, "other_runs_writes")[0] == OTHER_RUN_ATTRIBUTED
        )
        family = f"lakehouse/launch_records/families/{layer}/{OTHER_RUN_ID}.json"
        code, failed, document = run(layer, "family of another job", [*keys, family])
        checks[f"{layer}: a family index key of a job that the run's claim does not name fails"] = (
            code == 1 and failed == [NAMES["scope"]] and listed(document, "out_of_scope") == (1, [family])
        )
        many = [f"lakehouse/run_stage/{OTHER_RUN_ID}/inputs/part_{index:03d}" for index in range(150)] + [keys[4]]
        code, failed, document = run(layer, "many keys", many)
        count, named = listed(document, "other_runs_writes")
        checks[f"{layer}: the list of another run's writes is bounded and its count is whole"] = (
            code == 0 and count == 151 and len(named) == acceptance.OUT_OF_SCOPE_KEYS == 100 and named == sorted(many)[:100]
        )
        scattered = [f"lakehouse/other/part_{index:03d}" for index in range(150)]
        code, failed, document = run(layer, "many own keys", [*keys, *scattered])
        count, named = listed(document, "out_of_scope")
        checks[f"{layer}: the list of out-of-scope keys stays bounded beside an attributed run"] = code == 1 and count == 150 and len(named) == 100
    return checks


def report_check_defects() -> list[tuple[str, ReportChange]]:
    return [
        ("missing checks", lambda d: d.pop("checks")),
        ("empty checks", lambda d: d.update(checks={})),
        ("nonmapping checks", lambda d: d.update(checks=[])),
        ("null checks", lambda d: d.update(checks=None)),
        ("a false check", lambda d: d["checks"].update({"SYNTHETIC additional assertion": False})),
        ("an integer check", lambda d: d["checks"].update({"SYNTHETIC additional assertion": 1})),
        ("a string check", lambda d: d["checks"].update({"SYNTHETIC additional assertion": "true"})),
        ("a null check", lambda d: d["checks"].update({"SYNTHETIC additional assertion": None})),
    ]


def dynamic_report_checks(document: dict[str, Any]) -> None:
    document["checks"] = {f"SYNTHETIC dynamic assertion {index}: {name}": value for index, (name, value) in enumerate(document["checks"].items())}


def passed_dependency_cases(work: Path) -> dict[str, bool]:
    """The report of the layer below must have passed and must cover the records that this layer reads from it. No signature is read."""
    checks: dict[str, bool] = {}
    for layer in ("gold", "features"):
        below = "silver" if layer == "gold" else "gold"
        expected = NAMES["passed" if layer == "gold" else "features_passed"]
        covered = ("silver_snapshot.json",) if layer == "gold" else ("publish/commit.json", "publish/candidates.json")
        variants: list[tuple[str, ReportChange | None]] = [
            ("complete checks", None),
            ("valid dynamic check labels", dynamic_report_checks),
            *report_check_defects(),
            ("a failed summary", lambda d: d.update(passed=False)),
            ("another report layer", lambda d: d.update(layer="features")),
            ("another report run", lambda d: d.update(run_id="20261030T000000Z-00000000")),
            *[(f"a wrong covered hash for {name}", lambda d, key=name: d["file_sha256"].update({key: "0" * 64})) for name in covered],
        ]
        for label, change in variants:
            env, _ = scenario(work, layer, f"{below} dependency {label}")
            report_path = env.source.root / "qualification.json"
            if change is not None:
                edit(report_path, change)
            raw = report_path.read_bytes()
            code, failed, _ = accept(env)
            result_path = env.root / "qualification.json"
            result = json.loads(result_path.read_text()) if result_path.is_file() else {}
            positive = label in ("complete checks", "valid dynamic check labels")
            checks[f"{layer}: {below} dependency with {label}"] = (
                code == (0 if positive else 1) and failed == ([] if positive else [expected]) and result.get("passed") is positive
            )
            checks[f"{layer}: {label} reaches the dependency reader"] = expected in result.get("checks", {}) and result.get("file_sha256", {}).get(
                "input/report"
            ) == digest(report_path)
            checks[f"{layer}: {label} preserves the {below} report bytes"] = raw == report_path.read_bytes()
        env, _ = scenario(work, layer, f"{below} dependency without a report")
        env.source.root.joinpath("qualification.json").unlink()
        code, _, _ = accept(env)
        checks[f"{layer}: a missing {below} report is refused and no report is written"] = code == 1 and not (env.root / "qualification.json").exists()
    return checks


def good_cases(work: Path) -> dict[str, bool]:
    checks: dict[str, bool] = {}
    silver = make(work, "good_silver", spec("silver", SILVER_RUN, "a"))
    gold = make(work, "good_gold", spec("gold", GOLD_RUN, "c"), silver)
    features = make(work, "good_features", spec("features", FEATURES_RUN, "e"), gold)
    for env in (silver, gold, features):
        layer = env.spec.layer
        report = json.loads((env.root / "qualification.json").read_text())
        consumed = set(report["file_sha256"])
        wanted = {f"publish/{name}.json" for name in ("candidates", "reconcile", "backup", "commit", "consumer_result")} | {
            "layer_run.json",
            "publish/export/manifest.json",
            "code_digest",
            "describe",
            "launch/claim.json",
            "launch/01/started.json",
            "launch/01/end.json",
            "before",
            "after",
            "running_tasks",
            "host_status",
        }
        wanted |= {
            "silver": {"pins.json", "silver_snapshot.json"},
            "gold": {"silver_pins.json", "input/silver_snapshot.json", "input/report"},
            "features": {
                "gold_pins.json",
                "features_snapshot.json",
                "feast_apply.json",
                "feast_materialize.json",
                "input/publish/commit.json",
                "input/publish/candidates.json",
                "input/report",
            },
        }[layer]
        checks[f"{layer}: the good run passes every check with no approval and no signature"] = (
            report["passed"] is True and "approval" not in consumed and not env.root.joinpath("qualification.signed.json").exists()
        )
        checks[f"{layer}: the report hashes every consumed record ({sorted(wanted - consumed)})"] = wanted <= consumed
        checks[f"{layer}: each hash is the hash of the file's bytes"] = all(
            report["file_sha256"][name] == digest(env.evidence / name) for name in wanted if (env.evidence / name).is_file()
        )
        checks[f"{layer}: a second report to the same path is refused"] = run_cli(report_arguments(env, env.root / "qualification.json"))[0] == 1
    return checks


def edit_task(path: Path, change: Callable[[dict[str, Any]], object]) -> None:
    """Change the one task of a saved describe output."""
    edit(path, lambda document: change(document["tasks"][0]))


def guard_cases(work: Path) -> dict[str, bool]:
    """A good run still passes when its task has a guard container: ECS stops the guard when the last phase ends, so its exit code is 0, 143 (SIGTERM), 137
    (SIGKILL) or none, and a task definition from before the guard has no guard at all."""
    checks: dict[str, bool] = {}
    variants: dict[str, Callable[[dict[str, Any]], object]] = {
        "exit 143": lambda task: task["containers"][-1].update(exitCode=143),
        "exit 137": lambda task: task["containers"][-1].update(exitCode=137),
        "no exit code": lambda task: task["containers"][-1].pop("exitCode"),
        "exit 1": lambda task: task["containers"][-1].update(exitCode=1),
        "absent from the task definition": lambda task: (task["containers"].pop(), task["overrides"]["containerOverrides"].pop()),
    }
    for layer in acceptance.LAYERS:
        for number, (label, change) in enumerate(variants.items()):
            env, _ = scenario(work, layer, f"guard variant {'abcdefgh'[number]}")
            edit_task(env.root / "describe.json", change)
            code, failed, _ = accept(env)
            checks[f"{layer}: a good run whose guard is {label} passes"] = code == 0 and not failed
            checks[f"{layer}: the task check passes with a guard that is {label}"] = (
                run_cli(["task", "--layer", layer, "--describe", str(env.root / "describe.json")])[0] == 0
            )
        env, _ = scenario(work, layer, "guard stops a failed phase")
        edit(env.root / "describe.json", lambda d: d["tasks"][0]["containers"][3].update(exitCode=1))
        edit(env.root / "describe.json", lambda d: d["tasks"][0]["containers"][-1].update(exitCode=1))
        code, failed, _ = accept(env)
        checks[f"{layer}: a failed phase still fails when the guard ended the task"] = code == 1 and NAMES["exits"] in failed
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
    approval, signed = env.root / "new_approval.json", env.root / "qualification.json"
    approve = ["approve", "--layer", "silver", "--run", SILVER_RUN, "--code-sha256", "a" * 64, "--out", str(approval)]
    checks = {"approve: the owner step is gone and writes nothing": run_cli(approve)[0] == 2 and not approval.exists()}
    checks["sign: the owner step is gone"] = run_cli(["sign", "--report", str(signed), "--owner", "SYNTHETIC owner"])[0] == 2
    checks["report: the approval flag is gone"] = run_cli([*report_arguments(env, env.root / "with_approval.json"), "--approval", str(approval)])[0] == 2
    tracked, outside = ROOT / "docs/layer_acceptance_report_must_not_exist.json", Path(tempfile.gettempdir()) / "layer_acceptance_outside_the_repository.json"
    checks["report: a path that Git tracks is refused"] = run_cli(report_arguments(env, tracked))[0] == 1 and not tracked.exists()
    checks["report: a path outside the repository is refused"] = run_cli(report_arguments(env, outside))[0] == 1 and not outside.exists()
    checks["report: Git ignores the data and plans folders"] = GIT_IGNORED(ROOT / "data/e2e/x.json") and GIT_IGNORED(ROOT / "plans/x/signed.json")
    return checks


def archive_cases(work: Path) -> dict[str, bool]:
    """The publish contract is read from the run's staged ``code.tar.gz`` beside ``code.sha256``, never from the checkout."""
    checks: dict[str, bool] = {}
    other = "ab" * 20  # a revision that this checkout does not hold
    env = make(work, "archive_other_revision", replace(spec("silver", SILVER_RUN, "a"), revision=other), checked=False)
    code, failed, printed = accept(env)
    checks["a run whose revision is not this checkout's passes from its staged archive"] = code == 0 and not failed
    written = env.root / "qualification.json"
    checks["the report hashes the staged archive"] = written.is_file() and "code_archive" in json.loads(written.read_text()).get("file_sha256", {})
    env = make(work, "archive_tampered", spec("silver", SILVER_RUN, "a"), checked=False)
    env.root.joinpath("code.tar.gz").write_bytes(archive_for("other", REVISION))
    code, failed, printed = accept(env)
    checks["an archive that does not match code.sha256 is refused and no report is written"] = (
        code == 1 and "does not match its code.sha256" in printed and not env.root.joinpath("qualification.json").exists()
    )
    env = make(work, "archive_missing", spec("silver", SILVER_RUN, "a"), checked=False)
    env.root.joinpath("code.tar.gz").unlink()
    code, failed, printed = accept(env)
    checks["a missing staged archive is refused and nothing falls back to this checkout"] = (
        code == 1 and "staged code archive is missing" in printed and not env.root.joinpath("qualification.json").exists()
    )
    env = make(work, "archive_commit", replace(spec("silver", SILVER_RUN, "a"), revision=other), checked=False)
    edit(env.evidence / "layer_run.json", lambda d: d.update(git_revision=REVISION))
    code, failed, printed = accept(env)
    checks["an archive of another commit than the run's revision is refused"] = code == 1 and "revision of the run" in printed
    env = make(work, "archive_edited", replace(spec("silver", SILVER_RUN, "a"), edited=acceptance.CONTRACT["silver"]), checked=False)
    code, failed, _ = accept(env)
    checks["the tables of the archive's contract are the reference, not the checkout's"] = code == 1 and NAMES["sets"] in failed
    return checks


def static_cases() -> dict[str, bool]:
    """The command's constants equal the files they restate."""
    writer = (ROOT / "infra/iam/job_writer.tf").read_text()
    prefixes = {
        layer: {f"lakehouse/{name}/" for name in re.findall(r'"([a-z_]+)"', body)}
        for layer, body in re.findall(r"(silver|gold|features) = \{\s*prefixes\s*=\s*\[([^\]]*)\]", writer)
    }
    catalog = json.loads(acceptance.JOBS_FILE.read_text())["jobs"]
    return {
        "the write prefixes equal the layer writer roles": all(
            set(acceptance.WRITE_PREFIXES[layer]) - {"metadata/dbt/", "lakehouse/feast/registry.pb"} == prefixes[layer] for layer in acceptance.LAYERS
        ),
        "the features role may also write the Feast registry": 'job_layer_run_registry_key   = "lakehouse/feast/registry.pb"' in writer,
        "the gold role may also write the dbt metadata": '"${local.bronze_bucket_arn}/metadata/dbt/*"' in writer,
        "the expected containers are init and the job catalog's containers": all(
            acceptance.expected_containers(layer) == ["init", *catalog[layer]["containers"]] for layer in acceptance.LAYERS
        ),
        "the contract tables are read from the frozen revision": all(len(CONTRACT_TABLES[layer]) > 1 for layer in acceptance.LAYERS),
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
    work = Path(tempfile.mkdtemp(prefix="SYNTHETIC_layer_acceptance_", dir=out))
    results: dict[str, dict[str, bool]] = {"features is accepted": {"the acceptance command covers features": "features" in acceptance.LAYERS}}
    results["static"] = static_cases()
    try:
        with mock.patch.object(acceptance, "ignored", lambda path: str(path).startswith(str(work))):
            results["good"] = group("good", good_cases, work)
            results["passed dependencies"] = group("passed dependencies", passed_dependency_cases, work)
            for layer in acceptance.LAYERS:
                results[f"defects {layer}"] = group(f"defects {layer}", cases, work, layer)
            results["inputs"] = group("inputs", input_cases, work)
            results["guard"] = group("guard", guard_cases, work)
            results["host twin"] = group("host twin", host_twin_cases, work)
            results["other runs"] = group("other runs", other_run_cases, work)
            results["archive"] = group("archive", archive_cases, work)
            results["commands"] = command_cases(work)
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
