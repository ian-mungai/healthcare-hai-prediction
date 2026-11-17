"""Check a Production model's baseline for drift on demand: its newest window year against the years before it, with no gate and no training.

``python -m monitoring.live_check --model m2 --matrix P --manifest P --features P --run-id ID --out DIR --workspace DIR`` reads the inputs that the drift
task's baseline phase fetches (the Production run's DVC matrix, its manifest and the model's feature list). It splits the matrix by ``window_year`` into a
reference (the years before the manifest's ``newest_window_year``) and a current window (that year) and runs ``monitoring.drift_check`` on the split with the
tracked drift configuration. The record ``drift_<HAI type>.json`` (its ``triggered`` flag is always false: nothing is triggered) and Evidently's own report
``report_<HAI type>.html`` go to ``--out``. The report is also added as a snapshot to the Evidently UI workspace ``--workspace``, in one project for the model
and the HAI type (``m2 Elastic Net HAI_1 CLABSI``). A rerun of the same run ID writes the same record and adds no second snapshot or project.

A check that cannot compare is refused with exit 2 and writes nothing: a baseline with no row of its newest year, with fewer rows of it than the drift
configuration's minimum, with no row before it or of a population that is not monitored. Exit 3 means that Evidently is missing. ``record_sinks`` is the one
place where the drift metrics push is wired: each sink gets the written record once.
"""

from __future__ import annotations

import argparse
import importlib
import json
import os
import re
import sys
import tempfile
from argparse import Namespace
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Any

import duckdb

from monitoring import drift_check, drift_config
from monitoring.drift_check import HAI_TYPES, YEAR, CheckError, deadline
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


def count_of(rows: Any) -> int:
    counted = rows.aggregate("count(*)").fetchone()
    return int(counted[0]) if counted else 0


def build(arguments: Namespace, on_report: Callable[[Any], None]) -> dict[str, Any]:
    """Split the baseline matrix by window year, run the drift check on the split and return its record."""
    config, _ = drift_config.load(arguments.config)
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
    current, reference = matrix.filter(f"{YEAR} = {newest}"), matrix.filter(f"{YEAR} < {newest}")
    rows = count_of(current)
    if rows == 0:
        raise CheckError("no_current_window", f"the baseline has no row of window year {newest}, so there is no current window to compare")
    if rows < config["minimum_current_rows"]:
        raise CheckError(
            "too_few_current_rows", f"the baseline has {rows} rows of window year {newest} and the drift configuration needs {config['minimum_current_rows']}"
        )
    if count_of(reference) == 0:
        raise CheckError("no_reference_rows", f"the baseline has no row of window year {newest - 1} or earlier, so there is no reference to compare with")
    with tempfile.TemporaryDirectory(prefix="live_check_") as scratch:
        folder = Path(scratch)
        reference.write_parquet(str(folder / "reference.parquet"))
        current.write_parquet(str(folder / "current.parquet"))
        identity = {"hai_type": manifest["hai_type"], "population": manifest["population"]}
        (folder / "reference_manifest.json").write_text(json.dumps(identity | {"newest_window_year": newest - 1}) + "\n")
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


def write_html(report: Any, target: Path) -> None:
    """Save Evidently's own report as one self-contained HTML file, atomically."""
    handle, name = tempfile.mkstemp(dir=target.parent, prefix=".report_", suffix=".tmp")
    os.close(handle)
    try:
        report.save_html(name)
        os.replace(name, target)
    finally:
        if os.path.exists(name):
            os.unlink(name)


def add_to_workspace(report: Any, workspace: Path, name: str, run_id: str) -> None:
    """Add the report as a snapshot of the project ``name`` in the Evidently UI workspace, creating the workspace and the project when they are not there. A
    snapshot of the same run is never added twice."""
    store = importlib.import_module("evidently.legacy.ui.workspace").Workspace.create(str(workspace))
    found = [project for project in store.list_projects() if project.name == name]
    project = found[0] if found else store.create_project(name)
    if any(item.metadata.get("run_id") == run_id for item in project.list_snapshots(include_reports=True)):
        return
    report.metadata["run_id"] = run_id
    store.add_report(project.id, report)


def run(arguments: Namespace, *, record_sinks: Sequence[RecordSink] = ()) -> Path:
    """Run the live check and return the record's path. The files are written only after the comparison, the record first."""
    reports: list[Any] = []
    record = build(arguments, reports.append)
    if len(reports) != 1:
        raise RuntimeError(f"the check computed {len(reports)} Evidently reports, expected one")
    target = drift_check.write_record(arguments.out, record)
    write_html(reports[0], arguments.out / f"report_{record['hai_type']}.html")
    add_to_workspace(reports[0], arguments.workspace, project_name(arguments.model, record["hai_type"]), arguments.run_id)
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
