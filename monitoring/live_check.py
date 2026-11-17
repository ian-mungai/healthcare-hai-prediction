"""Check a Production model's baseline for drift on demand: its newest window year against the years before it, with no gate and no training.

``python -m monitoring.live_check --model m2 --matrix P --manifest P --features P --run-id ID --out DIR --workspace DIR`` reads the inputs that the drift
task's baseline phase fetches (the Production run's DVC matrix, its manifest and the model's feature list). The newest window year is the maximum of the
matrix's ``window_year`` and must agree with the manifest's ``newest_window_year``. The check splits the matrix into a reference (the years before it, labeled
with their own maximum) and a current window (the newest year) and runs ``monitoring.drift_check`` on the split with the tracked drift configuration. The
record ``drift_<HAI type>.json`` (its ``triggered`` flag is always false: nothing is triggered) and Evidently's own report ``report_<HAI type>.html`` go to
``--out``. The report is also added as a snapshot to the Evidently UI workspace ``--workspace``, in one project for the model and the HAI type
(``m2 Elastic Net HAI_1 CLABSI``). The snapshot file and the project's metadata go to ``--out`` too (``snapshot_<HAI type>.json``,
``project_<HAI type>.json``): the publish phase keeps them in S3, from where ``monitoring.live_restore`` rebuilds the workspace.

A run ID is bound to its inputs (model, type, matrix, manifest, feature list, drift configuration and ``--production-version``) when it is first written. The
workspace keeps the first record and report under ``.live_runs/<run ID>/``. A rerun with the same inputs, in any job, copies those bytes to ``--out`` and
computes nothing, so a completed run is a no-op; a rerun with other inputs is refused as ``run_inputs_changed``, before any file or snapshot changes. Snapshot
files are written atomically, and a snapshot file that an interrupted write left incomplete is set aside under ``.incomplete``.

A check that cannot compare is refused with exit 2 and writes nothing: a baseline with no row of the manifest's newest year, with rows newer than it, with
fewer rows of it than the drift configuration's minimum, with no row before it or of a population that is not monitored. Exit 3 means that Evidently is
missing. ``record_sinks`` is the one place where the drift metrics push is wired: each sink gets the record once, when it is first computed.
"""

from __future__ import annotations

import argparse
import contextlib
import hashlib
import importlib
import json
import os
import re
import sys
import tempfile
from argparse import Namespace
from collections.abc import Callable, Iterator, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import duckdb

from monitoring import drift_check, drift_config
from monitoring.drift_check import HAI_TYPES, YEAR, CheckError, deadline
from monitoring.live_workspace import export_run_files, place_snapshot, set_aside_incomplete_snapshots, snapshot_identity, write_bytes
from orchestration.training_contract import MODELS, RUN_ID

# The names people read beside the internal keys, in one list each (the readable keys rule). Both lists must cover exactly the keys of their contract.
MODEL_NAMES = {"m1": "OLS baseline", "m2": "Elastic Net", "m3": "Mixed effects", "m4": "Gradient boosting", "m5": "Spatial", "m6": "Fixed effects"}
HAI_NAMES = {"HAI_1": "CLABSI", "HAI_2": "CAUTI", "HAI_3": "SSI colon", "HAI_4": "SSI hysterectomy", "HAI_5": "MRSA", "HAI_6": "C. difficile"}
if tuple(MODEL_NAMES) != MODELS or tuple(HAI_NAMES) != HAI_TYPES:
    raise ValueError("MODEL_NAMES and HAI_NAMES must name exactly the models and HAI types of the contract")

RecordSink = Callable[[dict[str, Any]], None]
RECORD_SINKS: tuple[RecordSink, ...] = ()


def project_name(model: str, hai_type: str) -> str:
    """The Evidently project of a model and an HAI type: each key with the name beside it."""
    return f"{model} {MODEL_NAMES[model]} {hai_type} {HAI_NAMES[hai_type]}"


LIVE_RUNS = ".live_runs"


@dataclass(frozen=True)
class Plan:
    """What the inputs say after every refusal: the configuration, the manifest, the two years and the two row sets of the split."""

    config: dict[str, Any]
    config_sha: str
    manifest: dict[str, Any]
    newest: int
    reference_newest: int
    reference: Any
    current: Any


def count_of(rows: Any) -> int:
    counted = rows.aggregate("count(*)").fetchone()
    return int(counted[0]) if counted else 0


def maximum_year(rows: Any) -> int | None:
    found = rows.aggregate(f"max({YEAR})").fetchone()
    return None if not found or found[0] is None else int(found[0])


def split_plan(arguments: Namespace) -> Plan:
    """Read and check the inputs and split the matrix by window year. The newest year comes from the data and must be the manifest's."""
    config, config_sha = drift_config.load(arguments.config)
    manifest = drift_check.read_manifest(arguments.manifest, "manifest")
    newest = manifest.get("newest_window_year")
    if not isinstance(newest, int):
        raise CheckError("manifest_invalid", f"{arguments.manifest} must hold an integer newest_window_year")
    if manifest["population"] not in config["trigger_populations"]:
        raise CheckError("population_not_monitored", f"the population {manifest['population']} is not one the drift check monitors")
    drift_check.read_features(arguments.features)
    matrix = drift_check.open_matrix(arguments.matrix, "baseline")
    if YEAR not in matrix.columns:
        raise CheckError("column_missing", f"{YEAR} is not a column of the matrix")
    data_newest = maximum_year(matrix)
    if data_newest is not None and data_newest > newest:
        raise CheckError("manifest_year_stale", f"the baseline holds rows of window year {data_newest} but its manifest names {newest} as the newest year")
    if data_newest is None or data_newest < newest:
        raise CheckError("no_current_window", f"the baseline has no row of window year {newest}, so there is no current window to compare")
    current, reference = matrix.filter(f"{YEAR} = {newest}"), matrix.filter(f"{YEAR} < {newest}")
    rows = count_of(current)
    if rows < config["minimum_current_rows"]:
        raise CheckError(
            "too_few_current_rows", f"the baseline has {rows} rows of window year {newest} and the drift configuration needs {config['minimum_current_rows']}"
        )
    reference_newest = maximum_year(reference)
    if reference_newest is None:
        raise CheckError("no_reference_rows", f"the baseline has no row of window year {newest - 1} or earlier, so there is no reference to compare with")
    return Plan(config, config_sha, manifest, newest, reference_newest, reference, current)


def inputs_identity(arguments: Namespace, plan: Plan) -> tuple[str, dict[str, str]]:
    """The SHA-256 that binds a run ID to what it was computed from, and the fields it is made of (kept beside it to name a field that changed)."""
    parts = {
        "model": arguments.model,
        "hai_type": plan.manifest["hai_type"],
        "matrix_sha256": drift_check.sha256_of(arguments.matrix),
        "manifest_sha256": drift_check.sha256_of(arguments.manifest),
        "features_sha256": drift_check.sha256_of(arguments.features),
        "config_sha256": plan.config_sha,
        "production_version": arguments.production_version or "",
    }
    return hashlib.sha256(json.dumps(parts, sort_keys=True).encode()).hexdigest(), parts


def build(arguments: Namespace, plan: Plan, on_report: Callable[[Any], None]) -> dict[str, Any]:
    """Run the drift check on the split and return its record."""
    with tempfile.TemporaryDirectory(prefix="live_check_") as scratch:
        folder = Path(scratch)
        plan.reference.write_parquet(str(folder / "reference.parquet"))
        plan.current.write_parquet(str(folder / "current.parquet"))
        identity = {"hai_type": plan.manifest["hai_type"], "population": plan.manifest["population"]}
        (folder / "reference_manifest.json").write_text(json.dumps(identity | {"newest_window_year": plan.reference_newest}) + "\n")
        (folder / "current_manifest.json").write_text(json.dumps(identity) + "\n")
        record = drift_check.build_record(
            Namespace(
                reference_matrix=folder / "reference.parquet",
                reference_manifest=folder / "reference_manifest.json",
                current_matrix=folder / "current.parquet",
                current_manifest=folder / "current_manifest.json",
                features=arguments.features,
                run_id=arguments.run_id,
                config=arguments.config,
            ),
            on_report,
        )
    record["triggered"] = False
    return record


def write_html(report: Any, target: Path) -> bytes:
    """Save Evidently's own report as one self-contained HTML file, atomically, and return its bytes."""
    handle, name = tempfile.mkstemp(dir=target.parent, prefix=".report_", suffix=".tmp")
    os.close(handle)
    try:
        report.save_html(name)
        body = Path(name).read_bytes()
        os.replace(name, target)
    finally:
        if os.path.exists(name):
            os.unlink(name)
    return body


def read_binding(workspace: Path, run_id: str) -> dict[str, Any] | None:
    """The run's bound identity and its fields from the run-level record ``.live_runs/<run>/identity.json``, or None when the run has none. The record is apart
    from the cached outputs of any one HAI type, so a missing cache never erases it, and a record that cannot be read is refused, never taken as absent."""
    path = workspace / LIVE_RUNS / run_id / "identity.json"
    if not path.exists():
        return None
    try:
        binding = json.loads(path.read_bytes())
        if not isinstance(binding["inputs_sha256"], str):
            raise TypeError("the identity is not text")
    except (OSError, ValueError, KeyError, TypeError) as error:
        raise CheckError("run_binding_unreadable", f"{path} cannot be read as the run's bound identity: {error}") from None
    return dict(binding)


def write_binding(workspace: Path, run_id: str, identity: str, parts: dict[str, str]) -> None:
    """Bind ``run_id`` to its inputs, once, before the first output is written."""
    body = json.dumps({"inputs_sha256": identity, "parts": parts}, sort_keys=True) + "\n"
    write_bytes(workspace / LIVE_RUNS / run_id / "identity.json", body.encode())


def changed_fields(binding: dict[str, Any] | None, parts: dict[str, str]) -> str:
    """The names of the fields of the identity that differ from the bound ones, or a general name when the binding keeps no fields."""
    bound = binding.get("parts") if binding else None
    if not isinstance(bound, dict):
        return "an input"
    return ", ".join(sorted(key for key in parts if bound.get(key) != parts[key])) or "an input"


def recorded_outputs(workspace: Path, run_id: str, names: list[str]) -> dict[str, bytes] | None:
    """The files that the first computation of ``run_id`` cached in the workspace, or None when it cached none or not all of them."""
    folder = workspace / LIVE_RUNS / run_id
    try:
        return {name: (folder / name).read_bytes() for name in names}
    except OSError:
        return None


def record_outputs(workspace: Path, run_id: str, files: dict[str, bytes]) -> None:
    """Cache the first outputs of ``run_id`` of one HAI type, beside its binding."""
    for name, body in files.items():
        write_bytes(workspace / LIVE_RUNS / run_id / name, body)


@contextlib.contextmanager
def atomic_snapshot_writes() -> Iterator[None]:
    """Make Evidently's local blob storage write each snapshot through a temporary file in the project folder and a rename. Its own writer opens the final path,
    so a stopped task leaves a half file that the loader then fails on. The temporary file is outside the snapshots folder, which the loader reads whole."""
    storage = importlib.import_module("evidently.legacy.ui.storage.local.base").FSSpecBlobStorage
    original = storage.put_blob

    async def put_blob(self: Any, blob_id: str, obj: str) -> str:
        target = Path(self.location.path, blob_id)
        write_bytes(target, obj.encode(), temp_folder=target.parent.parent)
        return blob_id

    storage.put_blob = put_blob
    try:
        yield
    finally:
        storage.put_blob = original


def add_to_workspace(report: Any, workspace: Path, name: str, run_id: str, inputs_sha256: str) -> None:
    """Add the report as a snapshot of the project ``name`` in the Evidently UI workspace, creating the workspace and the project when they are not there. A
    snapshot of the same run is never added twice."""
    set_aside_incomplete_snapshots(workspace)
    with atomic_snapshot_writes():
        store = importlib.import_module("evidently.legacy.ui.workspace").Workspace.create(str(workspace))
        found = [project for project in store.list_projects() if project.name == name]
        project = found[0] if found else store.create_project(name)
        if any(item.metadata.get("run_id") == run_id for item in project.list_snapshots(include_reports=True)):
            return
        report.metadata["run_id"] = run_id
        report.metadata["inputs_sha256"] = inputs_sha256
        store.add_report(project.id, report)


def run(arguments: Namespace, *, record_sinks: Sequence[RecordSink] = ()) -> Path:
    """Run the live check and return the record's path. The inputs are checked and the run is matched with its bound identity before any file changes."""
    plan = split_plan(arguments)
    identity, parts = inputs_identity(arguments, plan)
    hai_type = plan.manifest["hai_type"]
    name = project_name(arguments.model, hai_type)
    names = [f"drift_{hai_type}.json", f"report_{hai_type}.html", f"snapshot_{hai_type}.json", f"project_{hai_type}.json"]
    binding = read_binding(arguments.workspace, arguments.run_id)
    snapshot = snapshot_identity(arguments.workspace, name, arguments.run_id)
    for known in (binding["inputs_sha256"] if binding else None, snapshot[1]):
        if known not in (None, identity):
            raise CheckError(
                "run_inputs_changed",
                f"run {arguments.run_id} was first written from other inputs: {changed_fields(binding, parts)} differ (a run is bound to its model, HAI type, "
                "baseline matrix, manifest, feature list, drift configuration and production version). A check of other inputs needs a new run ID",
            )
    target = arguments.out / names[0]
    recorded = recorded_outputs(arguments.workspace, arguments.run_id, names) if binding else None
    if recorded:
        if not snapshot[0]:
            place_snapshot(arguments.workspace, recorded[names[3]], recorded[names[2]])
        for file, body in recorded.items():
            write_bytes(arguments.out / file, body)
        return target
    reports: list[Any] = []
    record = build(arguments, plan, reports.append)
    if len(reports) != 1:
        raise RuntimeError(f"the check computed {len(reports)} Evidently reports, expected one")
    if binding is None:
        write_binding(arguments.workspace, arguments.run_id, identity, parts)
    drift_check.write_record(arguments.out, record)
    write_html(reports[0], arguments.out / names[1])
    add_to_workspace(reports[0], arguments.workspace, name, arguments.run_id, identity)
    project_file, snapshot_file = export_run_files(arguments.workspace, name, arguments.run_id)
    write_bytes(arguments.out / names[3], project_file)
    write_bytes(arguments.out / names[2], snapshot_file)
    record_outputs(arguments.workspace, arguments.run_id, {file: (arguments.out / file).read_bytes() for file in names})
    for sink in record_sinks:
        sink(record)
    return target


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--model", choices=MODELS, required=True, help="the model whose Production baseline is checked")
    for name in ("matrix", "manifest", "features"):
        parser.add_argument(f"--{name}", type=Path, required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--out", type=Path, required=True, help="the folder that receives drift_<HAI type>.json and report_<HAI type>.html")
    parser.add_argument("--workspace", type=Path, required=True, help="the Evidently UI workspace folder that receives the snapshot")
    parser.add_argument("--config", type=Path, default=drift_config.CONFIG)
    parser.add_argument("--production-version", help="the Production run and version of the baseline, bound with the inputs to the run ID")
    arguments = parser.parse_args(argv)
    try:
        if not re.fullmatch(RUN_ID, arguments.run_id):
            raise CheckError("run_invalid", f"{arguments.run_id!r} is not a run ID")
        config, _ = drift_config.load(arguments.config)
        with deadline(int(config["deadline_seconds"])):
            target = run(arguments, record_sinks=RECORD_SINKS)
    except (CheckError, drift_config.ConfigError) as error:
        sys.stderr.write(f"live drift check failed: {error}\n")
        return error.code if isinstance(error, CheckError) else 2
    except duckdb.Error as error:
        sys.stderr.write(f"live drift check failed: matrix_unreadable: the matrix cannot be read: {error}\n")
        return 2
    sys.stdout.write(f"live drift record {target}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
