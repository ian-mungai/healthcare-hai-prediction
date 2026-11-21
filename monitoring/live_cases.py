"""E2E cases for the on-demand live drift check on synthetic fixtures: no real patient, hospital or CMS record is read or written.

Every case writes one small baseline matrix (four window years, like the Production model's DVC baseline) with DuckDB from a fixed hash, starts the real
command as a child process and judges its exit code, its message and the files it leaves. The cases that need Evidently run only when ``.tools/evidently``
exists and are reported as blocked otherwise, never as passed. The phase cases run the real ``monitoring.aws_run`` command on a prepared workspace, and the
publish phase gets an in-memory S3 client (a declared substitution). The DAG cases load the real DAG file against the stand-ins of ``dag_cases``.
"""

from __future__ import annotations

import hashlib
import importlib
import json
import re
import shutil
import sys
from collections.abc import Callable
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest import mock

import duckdb

from monitoring import aws_cases, dag_cases, drift_cases
from monitoring.drift_cases import FEATURES, NUMERIC
from monitoring.drift_check import HAI_TYPES
from orchestration.training_contract import MODELS, RUN_ID
from scripts.process import run_command

ROOT = Path(__file__).resolve().parents[1]
PYTHON = str(ROOT / ".venv/bin/python")
EVIDENTLY_PROGRAM = str(ROOT / ".tools/evidently/bin/python")
LIVE_DAG_FILE = ROOT / "dags/drift_live_check_dag.py"
RUN = drift_cases.RUN_ID
OTHER_RUN = "drift_elastic_net_0a1b2c3e"
BUCKET = "bucket-for-tests"
OUTPUTS = ["drift_HAI_1.json", "project_HAI_1.json", "report_HAI_1.html", "snapshot_HAI_1.json"]
Problems = list[str]


def write_baseline(
    folder: Path,
    *,
    per_year: int = 500,
    current_rows: int = 300,
    shifted: tuple[str, ...] = (),
    years: tuple[int, ...] = (2021, 2022, 2023),
    newest: int | None = 2024,
    population: str = "primary",
    features: tuple[str, ...] = FEATURES,
    hai_type: str = "HAI_1",
    null_before_newest: tuple[str, ...] = (),
    null_in_newest: tuple[str, ...] = (),
) -> dict[str, Path]:
    """A baseline matrix of ``years`` with ``per_year`` rows each plus ``current_rows`` rows of 2024, the manifest that names 2024 as its newest year and the
    feature list. A feature in ``shifted`` moves only in the 2024 rows. A feature in ``null_before_newest`` has no value before 2024 (the shape of a measure
    first published in the newest year) and one in ``null_in_newest`` has none in 2024."""
    reference_rows = per_year * len(years)
    year = f"CASE WHEN i < {reference_rows} THEN ({years[0] if years else 2024} + i // {per_year}) ELSE 2024 END" if years else "2024"

    def missing(name: str) -> str:
        return "NULL::VARCHAR" if name == "f_cat" else "NULL::DOUBLE"

    columns = [
        f"CASE WHEN i >= {reference_rows} THEN {missing(name) if name in null_in_newest else drift_cases.column_sql(name, name in shifted)} "
        f"ELSE {missing(name) if name in null_before_newest else drift_cases.column_sql(name, False)} END AS {name}"
        for name in FEATURES
    ]
    projection = f"'k' || i::VARCHAR AS spine_key, {year} AS window_year, (hash('label' || i::VARCHAR) % 1000) / 500.0 AS label_value, {', '.join(columns)}"
    matrix = folder / "matrix.parquet"
    duckdb.sql("SELECT range AS i FROM range(?)", params=[reference_rows + current_rows]).project(projection).write_parquet(str(matrix))
    manifest = {"hai_type": hai_type, "population": population} | ({} if newest is None else {"newest_window_year": newest})
    paths = {"matrix": matrix, "manifest": folder / "manifest.json", "features": folder / "features.json"}
    paths["manifest"].write_text(json.dumps(manifest) + "\n")
    paths["features"].write_text(json.dumps(list(features)) + "\n")
    return paths


def run_live(
    folder: Path,
    paths: dict[str, Path],
    *,
    evidently: bool = False,
    model: str = "m2",
    run: str = RUN,
    out: Path | None = None,
    workspace: Path | None = None,
    extra: tuple[str, ...] = (),
) -> tuple[int, str, Path, Path]:
    out, workspace = out or folder / "demo_out", workspace or folder / "evidently_workspace"
    arguments = [f"--{name}={path}" for name, path in paths.items()] + [
        f"--model={model}",
        f"--run-id={run}",
        f"--out={out}",
        f"--workspace={workspace}",
        *extra,
    ]
    done = run_command(EVIDENTLY_PROGRAM if evidently else PYTHON, ["-m", "monitoring.live_check", *arguments], cwd=ROOT, timeout=drift_cases.TIMEOUT)
    return done.returncode, done.stdout + done.stderr, out, workspace


def projects_of(workspace: Path) -> dict[str, list[str]]:
    """The projects of an Evidently workspace folder (name to the run IDs of its snapshots), read from the files the UI reads."""
    found: dict[str, list[str]] = {}
    for metadata in sorted(workspace.glob("*/metadata.json")):
        runs = [json.loads(path.read_text())["metadata"].get("run_id", "") for path in sorted((metadata.parent / "snapshots").glob("*.json"))]
        found[json.loads(metadata.read_text())["name"]] = sorted(runs)
    return found


def empty(out: Path, workspace: Path) -> Problems:
    left = sorted(path.name for path in out.iterdir()) if out.exists() else []
    return ([f"a refused check left {left} in the output folder"] if left else []) + (
        [f"a refused check left projects {projects_of(workspace)}"] if workspace.exists() and projects_of(workspace) else []
    )


def refused(name: str, build: Callable[[Path], dict[str, Path]], detail: str = "", model: str = "m2") -> Callable[[Path], Problems]:
    def case(folder: Path) -> Problems:
        code, text, out, workspace = run_live(folder, build(folder), model=model)
        problems = [] if code == 2 else [f"expected exit 2, got {code}: {text[:300]}"]
        problems += [f"the message does not name {needle}: {text[:300]}" for needle in (name, detail) if needle not in text]
        return problems + empty(out, workspace)

    return case


def splits_by_year_and_checks(folder: Path) -> Problems:
    """The split of the fixture: 3 x 500 rows of 2021 to 2023 are the reference, the 300 rows of 2024 the current window, and every feature moved."""
    paths = write_baseline(folder, shifted=FEATURES)
    code, text, out, _ = run_live(folder, paths, evidently=drift_cases.need_evidently())
    if code != 0:
        return [f"exit {code}: {text[:400]}"]
    record = drift_cases.read_record(out)
    problems = drift_cases.schema_problems(record)
    expected = {
        "status": "checked",
        "triggered": False,
        "drifted_count": len(FEATURES),
        "feature_count": len(FEATURES),
        "excluded_features": [],
        "run_id": RUN,
        "hai_type": "HAI_1",
    }
    problems += [f"{key} is {record[key]!r}, expected {value!r}" for key, value in expected.items() if record[key] != value]
    if record["reference"]["newest_window_year"] != 2023 or record["reference"]["rows"] != 1500:
        problems.append(f"the reference is {record['reference']}, expected 1500 rows up to 2023")
    if record["current"]["window_years"] != [2024] or record["current"]["rows"] != 300:
        problems.append(f"the current window is {record['current']}, expected 300 rows of 2024")
    if not record["evidently_version"] or record["drift_share"] != 1.0:
        problems.append(f"Evidently did not compute the drift: version {record['evidently_version']}, share {record['drift_share']}")
    return problems


def excludes_a_feature_without_reference_values(folder: Path) -> Problems:
    """The live shape of drift_elastic_net_d6045404: a feature first published in the newest year has no value in any year before it. The check compares the
    other features, names the feature as excluded in the record and in the UI snapshot, and the Evidently report holds only the compared features."""
    paths = write_baseline(folder, shifted=NUMERIC[:2], null_before_newest=("f_num_d",))
    code, text, out, workspace = run_live(folder, paths, evidently=drift_cases.need_evidently())
    if code != 0:
        return [f"exit {code}: {text[:400]}"]
    record = drift_cases.read_record(out)
    problems = drift_cases.schema_problems(record)
    if record["status"] != "checked" or record["feature_count"] != 4 or "f_num_d" in {item["name"] for item in record["features"]}:
        problems.append(f"expected a checked record of 4 compared features without f_num_d, got {record['status']} {record['feature_count']}")
    if record["excluded_features"] != [{"name": "f_num_d", "reason": "all_missing_in_reference"}]:
        problems.append(f"excluded_features is {record['excluded_features']}")
    snapshot = json.loads((out / "snapshot_HAI_1.json").read_text())
    if snapshot["metadata"].get("excluded_features") != "f_num_d":
        problems.append(f"the UI snapshot does not name the excluded feature: {snapshot['metadata']}")
    if "f_num_d" in (out / "report_HAI_1.html").read_text():
        problems.append("the Evidently report holds the excluded feature")
    if projects_of(workspace) != {"m2 Elastic Net HAI_1 CLABSI": [RUN]}:
        problems.append(f"the workspace holds {projects_of(workspace)}")
    return problems


def a_rerun_with_an_excluded_feature_reuses_the_bytes(folder: Path) -> Problems:
    paths, evidently = write_baseline(folder, null_before_newest=("f_num_d",)), drift_cases.need_evidently()
    code, text, out, _ = run_live(folder, paths, evidently=evidently)
    if code != 0:
        return [f"exit {code}: {text[:400]}"]
    first = {name: (out / name).read_bytes() for name in OUTPUTS}
    code, text, _, _ = run_live(folder, paths, evidently=evidently)
    if code != 0:
        return [f"the rerun exited {code}: {text[:300]}"]
    return [] if first == {name: (out / name).read_bytes() for name in OUTPUTS} else ["a rerun of a run with an excluded feature changed an output"]


def stays_below_the_share_without_a_shift(folder: Path) -> Problems:
    """No feature moves, so the share of drifted features stays far below the configured half (chance may flag one feature of five at p below 0.05)."""
    code, text, out, _ = run_live(folder, write_baseline(folder), evidently=drift_cases.need_evidently())
    if code != 0:
        return [f"exit {code}: {text[:400]}"]
    record = drift_cases.read_record(out)
    return [] if record["status"] == "checked" and record["drift_share"] < 0.5 else [f"expected a share below half, got {record['drift_share']}"]


def writes_the_record_and_the_html_to_the_demo_folder_only(folder: Path) -> Problems:
    """Only the record and Evidently's own HTML report land in the output folder, the HTML is the self-contained report, and the workspace holds one project."""
    paths = write_baseline(folder, shifted=NUMERIC[:2])
    code, text, out, workspace = run_live(folder, paths, evidently=drift_cases.need_evidently())
    if code != 0:
        return [f"exit {code}: {text[:400]}"]
    problems = [] if sorted(path.name for path in out.iterdir()) == OUTPUTS else [f"the output folder holds {sorted(path.name for path in out.iterdir())}"]
    html = (out / "report_HAI_1.html").read_text() if (out / "report_HAI_1.html").is_file() else ""
    if "<html" not in html[:2000].lower() or len(html) < 100_000 or "<script src=" in html:
        problems.append(f"report_HAI_1.html is not a self-contained Evidently report ({len(html)} characters)")
    if projects_of(workspace) != {"m2 Elastic Net HAI_1 CLABSI": [RUN]}:
        problems.append(f"the workspace holds {projects_of(workspace)}, expected one project with this run")
    project_folder = next(workspace.glob("*/metadata.json")).parent
    snapshot_file = next((project_folder / "snapshots").glob("*.json"))
    if (out / "snapshot_HAI_1.json").read_bytes() != snapshot_file.read_bytes() or (out / "project_HAI_1.json").read_bytes() != (
        project_folder / "metadata.json"
    ).read_bytes():
        problems.append("the exported snapshot or project file differs from the workspace file the UI reads")
    stray = sorted(path.name for path in folder.iterdir() if path.name not in {"demo_out", "evidently_workspace", *(path.name for path in paths.values())})
    return problems + ([f"the command left {stray} beside its inputs"] if stray else [])


def rerun_adds_nothing(folder: Path) -> Problems:
    """A rerun of the same run writes the same record bytes, keeps one project and adds no second snapshot."""
    paths, evidently = write_baseline(folder, shifted=NUMERIC[:2]), drift_cases.need_evidently()
    code, text, out, workspace = run_live(folder, paths, evidently=evidently)
    if code != 0:
        return [f"exit {code}: {text[:400]}"]
    first = (out / "drift_HAI_1.json").read_bytes()
    code, text, _, _ = run_live(folder, paths, evidently=evidently)
    problems = [] if code == 0 else [f"the rerun exited {code}: {text[:300]}"]
    if (out / "drift_HAI_1.json").read_bytes() != first:
        problems.append("a rerun on the same inputs changed the record")
    if projects_of(workspace) != {"m2 Elastic Net HAI_1 CLABSI": [RUN]}:
        problems.append(f"the rerun changed the workspace: {projects_of(workspace)}")
    return problems


def another_run_adds_a_snapshot_to_the_same_project(folder: Path) -> Problems:
    paths, evidently = write_baseline(folder, shifted=NUMERIC[:2]), drift_cases.need_evidently()
    for run in (RUN, OTHER_RUN):
        code, text, _, workspace = run_live(folder, paths, evidently=evidently, run=run, out=folder / f"out_{run}")
        if code != 0:
            return [f"exit {code}: {text[:400]}"]
    found = projects_of(workspace)
    return [] if found == {"m2 Elastic Net HAI_1 CLABSI": sorted([RUN, OTHER_RUN])} else [f"expected one project with both runs, got {found}"]


def another_model_gets_its_own_project(folder: Path) -> Problems:
    paths, evidently = write_baseline(folder), drift_cases.need_evidently()
    for model, run in (("m1", RUN), ("m4", OTHER_RUN)):
        code, text, _, workspace = run_live(folder, paths, evidently=evidently, model=model, run=run, out=folder / f"out_{model}")
        if code != 0:
            return [f"exit {code}: {text[:400]}"]
    found = projects_of(workspace)
    return (
        []
        if sorted(found) == ["m1 OLS baseline HAI_1 CLABSI", "m4 Gradient boosting HAI_1 CLABSI"]
        else [f"expected one project per model, got {sorted(found)}"]
    )


def baseline_in(folder: Path, name: str, **keywords: Any) -> dict[str, Path]:
    """A baseline matrix, manifest and feature list in the sub-folder ``name`` of ``folder``."""
    (folder / name).mkdir(exist_ok=True)
    return write_baseline(folder / name, **keywords)


def recorded(out: Path) -> dict[str, bytes]:
    return {path.name: path.read_bytes() for path in sorted(out.iterdir())} if out.exists() else {}


PRODUCTION = "--production-version=run1:3"


def identity_file(workspace: Path) -> bytes:
    return (workspace / ".live_runs" / RUN / "identity.json").read_bytes()


def changed_inputs_under_the_same_run_are_refused(folder: Path) -> Problems:
    """A run ID is bound to the model, the HAI type, the baseline matrix, its manifest, the feature list, the drift configuration and the production version at
    its first write. A change of any one of the seven under the same ID is refused as run_inputs_changed, naming the field, before a file or a snapshot changes;
    the stored binding is never replaced, so the first inputs still replay after every refusal."""
    evidently = drift_cases.need_evidently()
    first = baseline_in(folder, "first", shifted=FEATURES)
    code, text, out, workspace = run_live(folder, first, evidently=evidently, extra=(PRODUCTION,))
    if code != 0:
        return [f"exit {code}: {text[:400]}"]
    before, snapshots, binding = recorded(out), projects_of(workspace), identity_file(workspace)
    manifest_changed = baseline_in(folder, "manifest", shifted=FEATURES)
    manifest_changed["manifest"].write_text(manifest_changed["manifest"].read_text().replace("}", ', "note": "other"}'))
    configuration = json.loads((ROOT / "config/monitoring/drift.json").read_text()) | {"deadline_seconds": 601}
    (folder / "drift_other.json").write_text(json.dumps(configuration) + "\n")
    variants: dict[str, tuple[dict[str, Path], dict[str, Any], str]] = {
        "baseline matrix": (baseline_in(folder, "matrix"), {}, "matrix_sha256"),
        "manifest": (manifest_changed, {}, "manifest_sha256"),
        "feature list": (baseline_in(folder, "features", shifted=FEATURES, features=FEATURES[:3]), {}, "features_sha256"),
        "model": (first, {"model": "m1"}, "model"),
        "HAI type": (baseline_in(folder, "type", shifted=FEATURES, hai_type="HAI_2"), {}, "hai_type"),
        "drift configuration": (first, {"extra": (PRODUCTION, f"--config={folder / 'drift_other.json'}")}, "config_sha256"),
        "production version": (first, {"extra": ("--production-version=run1:4",)}, "production_version"),
    }
    problems = []
    for name, (paths, keywords, field) in variants.items():
        keywords = {"extra": (PRODUCTION,), **keywords}
        code, text, other_out, _ = run_live(folder, paths, evidently=evidently, out=folder / f"out_{name.replace(' ', '_')}", **keywords)
        if code != 2 or "run_inputs_changed" not in text or RUN not in text or field not in text:
            problems.append(f"{name}: expected exit 2 naming run_inputs_changed, the run and {field}, got {code}: {text[:240]}")
        if recorded(other_out):
            problems.append(f"{name}: a refused rerun wrote {sorted(recorded(other_out))}")
        if identity_file(workspace) != binding or projects_of(workspace) != snapshots or recorded(out) != before:
            problems.append(f"{name}: a refused rerun changed the stored binding, the workspace or the first outputs")
        code, text, replay_out, _ = run_live(folder, first, out=folder / f"replay_{name.replace(' ', '_')}", extra=(PRODUCTION,))
        if code != 0 or recorded(replay_out) != before:
            problems.append(f"{name}: after the refusal the first inputs no longer replay (exit {code}): {text[:200]}")
    return problems


def the_binding_survives_lost_cached_outputs(folder: Path) -> Problems:
    """The run's binding is its own record: with the cached record, report and snapshot files gone, a rerun with other inputs (a matrix or an HAI type that has
    no cached files at all) is still refused and the binding is not replaced."""
    evidently = drift_cases.need_evidently()
    first = baseline_in(folder, "first", shifted=FEATURES)
    code, text, _, workspace = run_live(folder, first, evidently=evidently)
    if code != 0:
        return [f"exit {code}: {text[:400]}"]
    binding = identity_file(workspace)
    for cached in (workspace / ".live_runs" / RUN).iterdir():
        if cached.name != "identity.json":
            cached.unlink()
    problems = []
    for name, paths in {"matrix": baseline_in(folder, "matrix"), "type": baseline_in(folder, "type", shifted=FEATURES, hai_type="HAI_2")}.items():
        code, text, out, _ = run_live(folder, paths, evidently=evidently, out=folder / f"out_{name}")
        if code != 2 or "run_inputs_changed" not in text or recorded(out) or identity_file(workspace) != binding:
            problems.append(f"{name}: expected a refusal that keeps the binding, got {code}: {text[:240]}")
    return problems


def a_rerun_reuses_the_recorded_outputs_byte_for_byte(folder: Path) -> Problems:
    """A rerun of a completed run ID with the same inputs, in another job (a new output folder, the same workspace), recomputes nothing: the record and
    Evidently's HTML are the recorded bytes and the workspace keeps its one snapshot."""
    paths, evidently = write_baseline(folder, shifted=NUMERIC[:2]), drift_cases.need_evidently()
    code, text, first_out, workspace = run_live(folder, paths, evidently=evidently, out=folder / "job_one")
    if code != 0:
        return [f"exit {code}: {text[:400]}"]
    code, text, second_out, _ = run_live(folder, paths, evidently=evidently, out=folder / "job_two")
    problems = [] if code == 0 else [f"the rerun exited {code}: {text[:300]}"]
    if recorded(second_out) != recorded(first_out) or sorted(recorded(first_out)) != OUTPUTS:
        problems.append("the rerun's record or HTML differs from the recorded bytes")
    code, text, third_out, _ = run_live(folder, paths, out=folder / "job_three")
    if code != 0 or recorded(third_out) != recorded(first_out):
        problems.append(f"a rerun in an environment without Evidently did not reuse the recorded outputs (exit {code}): {text[:200]}")
    if projects_of(workspace) != {"m2 Elastic Net HAI_1 CLABSI": [RUN]}:
        problems.append(f"the rerun changed the workspace: {projects_of(workspace)}")
    return problems


def a_reused_run_calls_no_sink(folder: Path) -> Problems:
    """A rerun that reuses the recorded outputs is a no-op: the metrics sinks run for the first computation only."""
    drift_cases.need_evidently()
    paths = write_baseline(folder, shifted=FEATURES)
    script = (
        "import json, sys\n"
        "from argparse import Namespace\n"
        "from pathlib import Path\n"
        "from monitoring import live_check\n"
        "seen = []\n"
        "for name in ('one', 'two'):\n"
        "    live_check.run(Namespace(model='m2', matrix=Path(sys.argv[1]), manifest=Path(sys.argv[2]), features=Path(sys.argv[3]), run_id=sys.argv[4],\n"
        "        out=Path(sys.argv[5], name), workspace=Path(sys.argv[6]), config=live_check.drift_config.CONFIG, production_version=None),\n"
        "        record_sinks=(lambda record: seen.append(record['hai_type']),))\n"
        "print(json.dumps(seen))\n"
    )
    arguments = [
        "-c",
        script,
        str(paths["matrix"]),
        str(paths["manifest"]),
        str(paths["features"]),
        RUN,
        str(folder / "outs"),
        str(folder / "evidently_workspace"),
    ]
    done = run_command(EVIDENTLY_PROGRAM, arguments, cwd=ROOT, timeout=drift_cases.TIMEOUT)
    if done.returncode != 0:
        return [f"exit {done.returncode}: {(done.stdout + done.stderr)[:400]}"]
    seen = json.loads(done.stdout.strip().splitlines()[-1])
    return [] if seen == ["HAI_1"] else [f"the sinks saw {seen}, expected one record for two runs"]


def a_manifest_older_than_the_data_is_refused(folder: Path) -> Problems:
    """The newest year comes from the data. A stale manifest that names 2023 over a matrix that holds 2024 rows is refused, never run against 2023."""
    paths = write_baseline(folder, shifted=FEATURES, newest=2023)
    code, text, out, workspace = run_live(folder, paths, evidently=drift_cases.need_evidently())
    problems = (
        []
        if code == 2 and "manifest_year_stale" in text and "2023" in text and "2024" in text
        else [f"expected exit 2 naming the years, got {code}: {text[:300]}"]
    )
    return problems + empty(out, workspace)


def reference_years_come_from_the_data(folder: Path) -> Problems:
    """A matrix with 2021, 2022 and 2024 rows has the reference maximum 2022 in its record, not an invented 2023."""
    paths = write_baseline(folder, shifted=FEATURES, years=(2021, 2022))
    code, text, out, _ = run_live(folder, paths, evidently=drift_cases.need_evidently())
    if code != 0:
        return [f"exit {code}: {text[:400]}"]
    record = drift_cases.read_record(out)
    return [] if record["reference"]["newest_window_year"] == 2022 and record["reference"]["rows"] == 1000 else [f"the reference is {record['reference']}"]


SNAPSHOT_SCRIPT = (
    "import json, resource, signal, sys\n"
    "import numpy, pandas\n"
    "from pathlib import Path\n"
    "from evidently.legacy.metric_preset import DataDriftPreset\n"
    "from evidently.legacy.pipeline.column_mapping import ColumnMapping\n"
    "from evidently.legacy.report import Report\n"
    "from monitoring import live_check\n"
    "generator = numpy.random.default_rng(1)\n"
    "frame = lambda shift: pandas.DataFrame({'a': generator.normal(shift, 1, 200)})\n"
    "report = Report(metrics=[DataDriftPreset(columns=['a'])])\n"
    "report.run(reference_data=frame(0), current_data=frame(2), column_mapping=ColumnMapping(numerical_features=['a']))\n"
    "workspace = Path(sys.argv[1])\n"
    "signal.signal(signal.SIGXFSZ, signal.SIG_IGN)\n"
    "hard = resource.getrlimit(resource.RLIMIT_FSIZE)[1]\n"
    "resource.setrlimit(resource.RLIMIT_FSIZE, (3000, hard))\n"
    "try:\n"
    "    live_check.add_to_workspace(report, workspace, 'p', 'r1', 'hash1')\n"
    "    interrupted = False\n"
    "except OSError:\n"
    "    interrupted = True\n"
    "resource.setrlimit(resource.RLIMIT_FSIZE, (hard, hard))\n"
    "def half(path):\n"
    "    try:\n"
    "        json.loads(path.read_text())\n"
    "        return False\n"
    "    except ValueError:\n"
    "        return True\n"
    "halves = [path.name for path in workspace.glob('*/snapshots/*') if half(path)]\n"
    "live_check.add_to_workspace(report, workspace, 'p', 'r1', 'hash1')\n"
    "print(json.dumps({'interrupted': interrupted, 'halves': halves}))\n"
)


def an_interrupted_snapshot_write_leaves_the_workspace_loadable(folder: Path) -> Problems:
    """A snapshot write stopped after 3000 bytes (a file size limit stands for the stopped task) leaves no half file where the loader reads, so the next add
    loads the workspace and adds the snapshot once."""
    workspace = folder / "evidently_workspace"
    program = EVIDENTLY_PROGRAM if drift_cases.need_evidently() else PYTHON
    done = run_command(program, ["-c", SNAPSHOT_SCRIPT, str(workspace)], cwd=ROOT, timeout=drift_cases.TIMEOUT)
    if done.returncode != 0:
        return [f"the add after the interruption failed: {(done.stdout + done.stderr)[-400:]}"]
    result = json.loads(done.stdout.strip().splitlines()[-1])
    problems = [] if result["interrupted"] else ["the first write was not interrupted: the case proves nothing"]
    if result["halves"]:
        problems.append(f"the interrupted write left half files where the loader reads: {result['halves']}")
    for path in workspace.glob("*/snapshots/*"):
        try:
            json.loads(path.read_text())
        except ValueError:
            problems.append(f"{path.name} is not complete JSON")
    return problems + ([] if projects_of(workspace) == {"p": ["r1"]} else [f"the workspace holds {projects_of(workspace)}"])


def a_damaged_snapshot_file_is_set_aside_not_fatal(folder: Path) -> Problems:
    """A snapshot file that an older write left incomplete does not block the workspace: it is set aside (named on stderr, kept under .incomplete), the valid
    files stay, and a rerun of the run that lost its snapshot adds it again with the recorded outputs."""
    paths, evidently = write_baseline(folder, shifted=NUMERIC[:2]), drift_cases.need_evidently()
    code, text, first_out, workspace = run_live(folder, paths, evidently=evidently, out=folder / "one")
    code2, text2, _, _ = run_live(folder, paths, evidently=evidently, run=OTHER_RUN, out=folder / "other")
    if code or code2:
        return [f"setup exit {code} {code2}: {(text + text2)[:300]}"]
    damaged = next(path for path in workspace.glob("*/snapshots/*.json") if json.loads(path.read_text())["metadata"].get("run_id") == RUN)
    damaged.write_text(damaged.read_text()[:100])
    third = "drift_elastic_net_0a1b2c3f"
    code, text, _, _ = run_live(folder, paths, evidently=evidently, run=third, out=folder / "third")
    problems = [] if code == 0 else [f"a damaged snapshot file blocked the workspace (exit {code}): {text[-300:]}"]
    if damaged.name not in text or "set aside" not in text or not list(workspace.glob("*/.incomplete/*.json")) or damaged.exists():
        problems.append("the damaged file was not set aside and named")
    code, text, rerun_out, _ = run_live(folder, paths, out=folder / "rerun")
    if code != 0 or recorded(rerun_out) != recorded(first_out):
        problems.append(f"the rerun of the run that lost its snapshot did not give the recorded outputs (exit {code}): {text[-200:]}")
    found = projects_of(workspace)
    if found != {"m2 Elastic Net HAI_1 CLABSI": sorted([RUN, OTHER_RUN, third])}:
        problems.append(f"the workspace holds {found}")
    return problems


def published(
    folder: Path, s3: aws_cases.MemoryS3, workspace: Path, *, run: str = RUN, tag: str = "a", model: str = "m2", shifted: tuple[str, ...] = NUMERIC[:2]
) -> Problems:
    """Run the real live check into ``workspace`` for ``run`` and publish its outputs to ``s3`` as the publish phase does. Returns the problems it met."""
    sub = folder / tag
    sub.mkdir(exist_ok=True)
    paths = write_baseline(sub, shifted=shifted)
    code, text, out, _ = run_live(sub, paths, evidently=drift_cases.need_evidently(), run=run, model=model, out=sub / "out", workspace=workspace)
    if code != 0:
        return [f"the live check of {run} exited {code}: {text[:300]}"]
    work = sub / "work"
    (work / "drift" / run).mkdir(parents=True)
    done = {"phase": "baseline", "run_id": run, "monitored": ["HAI_1"], "unmonitored": {}, "model": model}
    (work / "drift" / run / "baseline.done.json").write_text(json.dumps(done) + "\n")
    shutil.copytree(out, work / "drift_live" / run)
    importlib.import_module("monitoring.aws_run").publish_live_phase(s3, BUCKET, work, run, "HAI_1")
    return []


def restore(s3: aws_cases.MemoryS3, workspace: Path) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = importlib.import_module("monitoring.live_restore").restore(s3, BUCKET, workspace)
    return result


def file_state(workspace: Path) -> dict[str, tuple[int, int]]:
    return {str(path.relative_to(workspace)): (path.stat().st_size, path.stat().st_mtime_ns) for path in sorted(workspace.rglob("*")) if path.is_file()}


def statuses(reports: list[dict[str, Any]]) -> dict[str, str]:
    return {item["run"]: item["status"] for item in reports}


def restore_rebuilds_an_empty_workspace(folder: Path) -> Problems:
    """Two completed runs of one project, published from a workspace that is then lost, rebuild an empty workspace: the same project folder, the same
    snapshot files and the same project metadata, byte for byte."""
    s3, source, target = aws_cases.MemoryS3(), folder / "source", folder / "restored"
    problems = published(folder, s3, source) + published(folder, s3, source, run=OTHER_RUN, tag="b")
    if problems:
        return problems
    reports = restore(s3, target)
    if statuses(reports) != {RUN: "restored", OTHER_RUN: "restored"}:
        problems.append(f"the restore reported {reports}")
    if projects_of(target) != projects_of(source) or len(projects_of(target)) != 1:
        problems.append(f"the restored workspace holds {projects_of(target)}, the source {projects_of(source)}")
    for path in source.rglob("*.json"):
        if path.is_file() and ".live_runs" not in path.parts and (target / path.relative_to(source)).read_bytes() != path.read_bytes():
            problems.append(f"{path.relative_to(source)} differs after the restore")
    return problems


def restore_rerun_is_a_noop(folder: Path) -> Problems:
    s3, source, target = aws_cases.MemoryS3(), folder / "source", folder / "restored"
    problems = published(folder, s3, source)
    if problems:
        return problems
    restore(s3, target)
    before = file_state(target)
    second = restore(s3, target)
    if statuses(second) != {RUN: "present"}:
        problems.append(f"the second restore reported {second}")
    return problems + ([] if file_state(target) == before else ["the second restore changed a file or added one"])


def restore_skips_and_reports_an_incomplete_run(folder: Path) -> Problems:
    """A run with no completion marker, and a run whose snapshot object is missing, are skipped and named; the complete run is still restored."""
    s3, source, target = aws_cases.MemoryS3(), folder / "source", folder / "restored"
    third = "drift_elastic_net_0a1b2c40"
    problems = published(folder, s3, source) + published(folder, s3, source, run=OTHER_RUN, tag="b") + published(folder, s3, source, run=third, tag="c")
    if problems:
        return problems
    del s3.objects[(BUCKET, f"drift_demo/{OTHER_RUN}/_complete.json")]
    del s3.objects[(BUCKET, f"drift_demo/{third}/snapshot_HAI_1.json")]
    reports = restore(s3, target)
    found = {item["run"]: item for item in reports}
    if statuses(reports) != {RUN: "restored", OTHER_RUN: "skipped", third: "skipped"}:
        problems.append(f"the restore reported {statuses(reports)}")
    if not all(item.get("reason", "").startswith("incomplete") for item in (found.get(OTHER_RUN, {}), found.get(third, {}))):
        problems.append(f"the skips do not name an incomplete run: {reports}")
    return problems + ([] if list(projects_of(target).values()) == [[RUN]] else [f"the workspace holds {projects_of(target)}"])


def restore_refuses_a_mismatched_hash(folder: Path) -> Problems:
    """A snapshot whose bytes differ from the hash its marker recorded is never placed: the run is skipped as hash_mismatch, naming the file, and no file or
    project folder of it exists."""
    s3, source, target = aws_cases.MemoryS3(), folder / "source", folder / "restored"
    problems = published(folder, s3, source)
    if problems:
        return problems
    key = (BUCKET, f"drift_demo/{RUN}/snapshot_HAI_1.json")
    s3.objects[key] = s3.objects[key].replace(b"DataDriftPreset", b"OtherPreset")
    reports = restore(s3, target)
    problems = (
        []
        if statuses(reports) == {RUN: "skipped"} and "hash_mismatch" in reports[0].get("reason", "") and "snapshot_HAI_1.json" in reports[0]["reason"]
        else [f"reported {reports}"]
    )
    return problems + ([f"the refused run left {sorted(file_state(target))}"] if target.exists() and file_state(target) else [])


def restore_skips_a_marker_that_cannot_be_trusted(folder: Path) -> Problems:
    """An unreadable marker, a marker of another run and a marker that names a file outside the run's folder are each skipped by name and place nothing."""
    s3, source = aws_cases.MemoryS3(), folder / "source"
    problems = published(folder, s3, source)
    if problems:
        return problems
    marker_key = (BUCKET, f"drift_demo/{RUN}/_complete.json")
    good = json.loads(s3.objects[marker_key])
    variants = {
        "marker_unreadable": b"{not json",
        "marker_invalid": json.dumps(good | {"run_id": OTHER_RUN}).encode(),
        "marker_invalid ": json.dumps(good | {"files": {**good["files"], "../escape.json": "0" * 64}}).encode(),
    }
    for expected, body in variants.items():
        s3.objects[marker_key] = body
        target = folder / f"restored_{expected.strip()}_{len(body)}"
        reports = restore(s3, target)
        if statuses(reports) != {RUN: "skipped"} or expected.strip() not in reports[0].get("reason", "") or (target.exists() and file_state(target)):
            problems.append(f"{expected.strip()}: reported {reports}, left {sorted(file_state(target)) if target.exists() else []}")
    return problems


def restore_adds_to_an_existing_project_of_the_same_name(folder: Path) -> Problems:
    """A workspace that already holds the project under its own id (made by a check after the loss) takes the restored snapshot into it."""
    s3, source, target = aws_cases.MemoryS3(), folder / "source", folder / "restored"
    problems = published(folder, s3, source) + published(folder, aws_cases.MemoryS3(), target, run=OTHER_RUN, tag="b")
    if problems:
        return problems
    existing = next(target.glob("*/metadata.json")).parent.name
    reports = restore(s3, target)
    found = projects_of(target)
    problems = [] if statuses(reports) == {RUN: "restored"} else [f"reported {reports}"]
    if list(found.values()) != [sorted([RUN, OTHER_RUN])] or [path.parent.name for path in target.glob("*/metadata.json")] != [existing]:
        problems.append(f"the workspace holds {found} in {[path.parent.name for path in target.glob('*/metadata.json')]}")
    return problems


def restore_skips_a_snapshot_of_another_run(folder: Path) -> Problems:
    s3, source, target = aws_cases.MemoryS3(), folder / "source", folder / "restored"
    problems = published(folder, s3, source, run=OTHER_RUN)
    if problems:
        return problems
    for name in list(key for _, key in s3.objects if key.startswith(f"drift_demo/{OTHER_RUN}/")):
        s3.objects[(BUCKET, name.replace(OTHER_RUN, RUN))] = s3.objects[(BUCKET, name)]
    marker_key = (BUCKET, f"drift_demo/{RUN}/_complete.json")
    s3.objects[marker_key] = json.dumps(json.loads(s3.objects[marker_key]) | {"run_id": RUN}).encode()
    reports = restore(s3, target)
    found = {item["run"]: item for item in reports}
    expected = {RUN: "skipped", OTHER_RUN: "restored"}
    return [] if statuses(reports) == expected and "snapshot_run_mismatch" in found[RUN].get("reason", "") else [f"reported {reports}"]


def the_ui_loader_lists_the_restored_projects(folder: Path) -> Problems:
    """Evidently's own workspace loader, which the UI service runs, lists the restored projects and their snapshots by run."""
    s3, source, target = aws_cases.MemoryS3(), folder / "source", folder / "restored"
    problems = published(folder, s3, source) + published(folder, s3, source, run=OTHER_RUN, tag="b")
    if problems:
        return problems
    restore(s3, target)
    script = (
        "import json, sys\n"
        "from evidently.legacy.ui.workspace import Workspace\n"
        "ws = Workspace.create(sys.argv[1])\n"
        "print(json.dumps({p.name: sorted(s.metadata.get('run_id', '') for s in p.list_snapshots(include_reports=True)) for p in ws.list_projects()}))\n"
    )
    done = run_command(EVIDENTLY_PROGRAM, ["-c", script, str(target)], cwd=ROOT, timeout=drift_cases.TIMEOUT)
    if done.returncode != 0:
        return [f"the UI loader failed on the restored workspace: {(done.stdout + done.stderr)[-300:]}"]
    return [] if json.loads(done.stdout.strip().splitlines()[-1]) == projects_of(source) else [f"the loader lists {done.stdout.strip()}"]


def restore_cli_refuses_a_bad_bucket_name(folder: Path) -> Problems:
    done = run_command(PYTHON, ["-m", "monitoring.live_restore", "--bucket=A_bad_bucket", f"--workspace={folder / 'ws'}"], cwd=ROOT, timeout=120)
    text = done.stdout + done.stderr
    return (
        []
        if done.returncode == 2 and "bucket_invalid" in text and not (folder / "ws").exists()
        else [f"expected exit 2 naming bucket_invalid, got {done.returncode}: {text[:300]}"]
    )


WRITER_SCRIPT = (
    "import json, sys, threading\n"
    "import fsspec, numpy, pandas\n"
    "from pathlib import Path\n"
    "from evidently.legacy.metric_preset import DataDriftPreset\n"
    "from evidently.legacy.pipeline.column_mapping import ColumnMapping\n"
    "from evidently.legacy.report import Report\n"
    "from evidently.legacy.ui.storage.local.base import FSSpecBlobStorage\n"
    "from evidently.legacy.ui.workspace import Workspace\n"
    "from monitoring import live_check\n"
    "original = FSSpecBlobStorage.put_blob\n"
    "generator = numpy.random.default_rng(1)\n"
    "frame = lambda shift: pandas.DataFrame({'a': generator.normal(shift, 1, 200)})\n"
    "def finish(coroutine):\n"
    "    try:\n"
    "        coroutine.send(None)\n"
    "    except StopIteration:\n"
    "        return\n"
    "def make_report(hook):\n"
    "    report = Report(metrics=[DataDriftPreset(columns=['a'])])\n"
    "    report.run(reference_data=frame(0), current_data=frame(2), column_mapping=ColumnMapping(numerical_features=['a']))\n"
    "    class Hooked(dict):\n"
    "        def __setitem__(self, key, value):\n"
    "            super().__setitem__(key, value)\n"
    "            if key == 'run_id':\n"
    "                hook()\n"
    "    report.metadata = Hooked(report.metadata)\n"
    "    return report\n"
    "def unrelated():\n"
    "    seen = {}\n"
    "    def hook():\n"
    "        other = FSSpecBlobStorage('memory://unrelated/ws')\n"
    "        seen['class_during'] = FSSpecBlobStorage.put_blob is original\n"
    "        try:\n"
    "            finish(other.put_blob('p/snapshots/x.json', '{}'))\n"
    "        except Exception as error:\n"
    "            seen['error'] = type(error).__name__\n"
    "        seen['memory_has_object'] = fsspec.filesystem('memory').exists('/unrelated/ws/p/snapshots/x.json')\n"
    "        seen['disk_has_object'] = Path('/unrelated').exists()\n"
    "    live_check.add_to_workspace(make_report(hook), Path(sys.argv[2]), 'p', 'r1', 'hash1')\n"
    "    seen['class_after'] = FSSpecBlobStorage.put_blob is original\n"
    "    return seen\n"
    "def overlap():\n"
    "    events = {name: threading.Event() for name in ('a_inside', 'b_inside', 'a_done')}\n"
    "    errors = []\n"
    "    def work(name, workspace, hook):\n"
    "        try:\n"
    "            live_check.add_to_workspace(make_report(hook), workspace, 'p', name, 'hash' + name)\n"
    "        except Exception as error:\n"
    "            errors.append(f'{name}: {type(error).__name__}: {error}')\n"
    "    def hook_a():\n"
    "        events['a_inside'].set()\n"
    "        events['b_inside'].wait(60)\n"
    "    def hook_b():\n"
    "        events['b_inside'].set()\n"
    "        events['a_done'].wait(60)\n"
    "    def run_a():\n"
    "        work('a', Path(sys.argv[2], 'a'), hook_a)\n"
    "        events['a_done'].set()\n"
    "    def run_b():\n"
    "        events['a_inside'].wait(60)\n"
    "        work('b', Path(sys.argv[2], 'b'), hook_b)\n"
    "    threads = [threading.Thread(target=run_a), threading.Thread(target=run_b)]\n"
    "    for thread in threads:\n"
    "        thread.start()\n"
    "    for thread in threads:\n"
    "        thread.join(120)\n"
    "    return {'class_after': FSSpecBlobStorage.put_blob is original, 'errors': errors, 'alive': [thread.is_alive() for thread in threads]}\n"
    "def nesting():\n"
    "    storage = Workspace.create(sys.argv[2]).project_manager.blob_storage\n"
    "    state = {'before': 'put_blob' in vars(storage)}\n"
    "    with live_check.atomic_snapshot_writes(storage):\n"
    "        state['outer'] = 'put_blob' in vars(storage)\n"
    "        outer_writer = vars(storage)['put_blob']\n"
    "        with live_check.atomic_snapshot_writes(storage):\n"
    "            state['inner'] = 'put_blob' in vars(storage)\n"
    "        state['outer_is_back'] = vars(storage)['put_blob'] is outer_writer\n"
    "        state['outer_after_inner'] = 'put_blob' in vars(storage)\n"
    "    state['after'] = 'put_blob' in vars(storage)\n"
    "    try:\n"
    "        with live_check.atomic_snapshot_writes(storage):\n"
    "            raise RuntimeError('stop')\n"
    "    except RuntimeError:\n"
    "        pass\n"
    "    state['after_error'] = 'put_blob' in vars(storage)\n"
    "    state['class'] = FSSpecBlobStorage.put_blob is original\n"
    "    memory = FSSpecBlobStorage('memory://elsewhere/ws')\n"
    "    with live_check.atomic_snapshot_writes(memory):\n"
    "        state['memory_left_alone'] = 'put_blob' not in vars(memory)\n"
    "    return state\n"
    "print(json.dumps({'unrelated': unrelated, 'overlap': overlap, 'nesting': nesting}[sys.argv[1]]()))\n"
)


def writer_result(done: Any) -> tuple[dict[str, Any] | None, str]:
    """The JSON result that ``WRITER_SCRIPT`` printed, or None with the end of its output when it failed."""
    text = done.stdout + done.stderr
    return (json.loads(done.stdout.strip().splitlines()[-1]) if done.returncode == 0 else None), text[-400:]


def the_atomic_writer_leaves_an_unrelated_store_alone(folder: Path) -> Problems:
    """While a workspace snapshot is being added, a write through another store that uses Evidently's own blob class (here an in-memory file system) goes to
    that store, not to the local disk, and the class method is the library's own before, during and after."""
    drift_cases.need_evidently()
    workspace = folder / "ws"
    done = run_command(EVIDENTLY_PROGRAM, ["-c", WRITER_SCRIPT, "unrelated", str(workspace)], cwd=ROOT, timeout=drift_cases.TIMEOUT)
    seen, text = writer_result(done)
    if seen is None:
        return [f"the probe failed: {text}"]
    expected = {"class_during": True, "memory_has_object": True, "disk_has_object": False, "class_after": True}
    problems = [f"{key} is {seen.get(key)!r}, expected {value!r}" for key, value in expected.items() if seen.get(key) != value]
    return problems + ([f"the unrelated write failed with {seen['error']}"] if "error" in seen else [])


def overlapping_writer_contexts_leave_the_library_unpatched(folder: Path) -> Problems:
    """Two workspaces are written in overlapping threads (A enters, B enters, A leaves, B leaves). Afterwards the library's class method is its own, and each
    snapshot is in its own workspace. Evidently 0.7.23's class is never replaced."""
    drift_cases.need_evidently()
    workspace = folder / "ws"
    done = run_command(EVIDENTLY_PROGRAM, ["-c", WRITER_SCRIPT, "overlap", str(workspace)], cwd=ROOT, timeout=drift_cases.TIMEOUT)
    seen, text = writer_result(done)
    if seen is None:
        return [f"the probe failed: {text}"]
    problems = [] if seen["class_after"] else ["the library's put_blob stayed replaced after both contexts ended"]
    problems += [f"a thread failed: {error}" for error in seen["errors"]] + (["a thread did not end"] if any(seen["alive"]) else [])
    for name in ("a", "b"):
        if projects_of(folder / "ws" / name) != {"p": [name]}:
            problems.append(f"workspace {name} holds {projects_of(folder / 'ws' / name)}")
    return problems


def the_atomic_writer_nests_and_restores_on_every_exit(folder: Path) -> Problems:
    """On one workspace's own storage the writer is installed by instance, nests (the outer writer itself is back after an inner one ends) and is removed on a
    normal exit and on an error, leaving the instance as it was. A storage on another file system is not touched."""
    drift_cases.need_evidently()
    workspace = folder / "ws"
    done = run_command(EVIDENTLY_PROGRAM, ["-c", WRITER_SCRIPT, "nesting", str(workspace)], cwd=ROOT, timeout=drift_cases.TIMEOUT)
    seen, text = writer_result(done)
    if seen is None:
        return [f"the probe failed: {text}"]
    expected = {
        "before": False,
        "outer": True,
        "inner": True,
        "outer_is_back": True,
        "outer_after_inner": True,
        "after": False,
        "after_error": False,
        "class": True,
        "memory_left_alone": True,
    }
    return [f"{key} is {seen.get(key)!r}, expected {value!r}" for key, value in expected.items() if seen.get(key) != value]


def passes_the_record_to_each_sink_once(folder: Path) -> Problems:
    """The one call site the monitoring merge wires: every sink in ``record_sinks`` gets the written record once, after the files exist. Nothing else runs."""
    paths = write_baseline(folder, shifted=FEATURES)
    script = (
        "import json, sys\n"
        "from argparse import Namespace\n"
        "from pathlib import Path\n"
        "from monitoring import live_check\n"
        "seen = []\n"
        "def sink(record):\n"
        "    seen.append({'hai_type': record['hai_type'], 'triggered': record['triggered'], 'html': Path(sys.argv[1], 'report_HAI_1.html').is_file()})\n"
        "live_check.run(Namespace(model='m2', matrix=Path(sys.argv[2]), manifest=Path(sys.argv[3]), features=Path(sys.argv[4]), run_id=sys.argv[5],\n"
        "    out=Path(sys.argv[1]), workspace=Path(sys.argv[6]), config=live_check.drift_config.CONFIG, production_version=None), record_sinks=(sink,))\n"
        "print(json.dumps(seen))\n"
    )
    out, workspace = folder / "demo_out", folder / "evidently_workspace"
    arguments = ["-c", script, str(out), str(paths["matrix"]), str(paths["manifest"]), str(paths["features"]), RUN, str(workspace)]
    done = run_command(EVIDENTLY_PROGRAM if drift_cases.need_evidently() else PYTHON, arguments, cwd=ROOT, timeout=drift_cases.TIMEOUT)
    if done.returncode != 0:
        return [f"exit {done.returncode}: {(done.stdout + done.stderr)[:400]}"]
    seen = json.loads(done.stdout.strip().splitlines()[-1])
    return [] if seen == [{"hai_type": "HAI_1", "triggered": False, "html": True}] else [f"the sink saw {seen}"]


def refuses_an_unknown_model(folder: Path) -> Problems:
    code, text, out, workspace = run_live(folder, write_baseline(folder), model="m9")
    return ([] if code == 2 and "m9" in text else [f"expected exit 2 naming m9, got {code}: {text[:300]}"]) + empty(out, workspace)


def names_the_missing_dependency(folder: Path) -> Problems:
    code, text, out, workspace = run_live(folder, write_baseline(folder, shifted=FEATURES))
    problems = (
        [] if code == 3 and "dependency_missing" in text and "evidently" in text.lower() else [f"expected exit 3 naming Evidently, got {code}: {text[:300]}"]
    )
    return problems + empty(out, workspace)


def workspace_for(
    folder: Path, hai_type: str = "HAI_1", model: str = "m2", *, newest: int = 2024, population: str = "primary", with_current: bool = True
) -> Path:
    """A drift workspace as the baseline phase leaves it for the live check: the baseline matrix, its manifest and feature list, and the baseline record."""
    work = folder / "work"
    paths = write_baseline(folder, shifted=FEATURES, newest=newest, population=population, current_rows=300 if with_current else 0)
    type_folder = work / "drift" / RUN / hai_type
    type_folder.mkdir(parents=True)
    for name, target in (("matrix", "reference.parquet"), ("manifest", "reference_manifest.json"), ("features", "features.json")):
        (type_folder / target).write_bytes(paths[name].read_bytes())
    done = {"phase": "baseline", "run_id": RUN, "monitored": [hai_type], "unmonitored": {}, "model": model}
    (work / "drift" / RUN / "baseline.done.json").write_text(json.dumps(done) + "\n")
    return work


def phase(folder: Path, name: str, *extra: str, evidently: bool = False, live: str = "HAI_1") -> tuple[int, str]:
    work = folder / "work"
    arguments = [
        "-m",
        "monitoring.aws_run",
        name,
        f"--run={RUN}",
        f"--work={work}",
        f"--workspace={folder / 'evidently_workspace'}",
        f"--live-type={live}",
        *extra,
    ]
    done = run_command(EVIDENTLY_PROGRAM if evidently else PYTHON, arguments, cwd=ROOT, timeout=drift_cases.TIMEOUT)
    return done.returncode, done.stdout + done.stderr


def check_phase_runs_the_live_check(folder: Path) -> Problems:
    work = workspace_for(folder)
    code, text = phase(folder, "check", evidently=drift_cases.need_evidently())
    out = work / "drift_live" / RUN
    problems = [] if code == 0 else [f"exit {code}: {text[:400]}"]
    if sorted(path.name for path in out.glob("*")) != OUTPUTS:
        problems.append(f"the live outputs are {sorted(path.name for path in out.glob('*'))}")
    if (work / "drift" / RUN / "records").exists():
        problems.append("the live check wrote into the records folder of a gate run")
    if projects_of(folder / "evidently_workspace") != {"m2 Elastic Net HAI_1 CLABSI": [RUN]}:
        problems.append(f"the workspace holds {projects_of(folder / 'evidently_workspace')}")
    return problems


def check_phase_refuses_an_unmonitored_type(folder: Path) -> Problems:
    work = workspace_for(folder)
    done = work / "drift" / RUN / "baseline.done.json"
    done.write_text(json.dumps({"phase": "baseline", "run_id": RUN, "monitored": [], "unmonitored": {"HAI_1": "no_production_model"}, "model": "m2"}) + "\n")
    code, text = phase(folder, "check")
    return (
        []
        if code == 2 and "no_production_model" in text and not (work / "drift_live").exists()
        else [f"expected exit 2 naming no_production_model, got {code}: {text[:300]}"]
    )


def current_phase_reads_nothing_in_live_mode(folder: Path) -> Problems:
    work = workspace_for(folder)
    code, text = phase(folder, "current")
    lines = [line for line in text.splitlines() if line.startswith('{"phase"')]
    result = json.loads(lines[-1])["result"] if code == 0 and lines else {}
    problems = [] if code == 0 and result.get("types") == [] else [f"expected exit 0 with no types, got {code}: {text[:300]}"]
    if (work / "drift" / RUN / "current.done.json").exists():
        problems.append("a live run recorded a current phase of a gate run")
    return problems


def baseline_phase_covers_only_the_live_type(folder: Path) -> Problems:
    """The baseline phase of a live run fetches the one type it was given, for the model it was given, and the gate task still covers all six. The registry
    and the DVC workspace are declared stand-ins (the real baseline fetch has its own cases): the case judges which types and model the phase asks for."""
    runner, calls = importlib.import_module("monitoring.aws_run"), []

    def recorder(store: Any, work: Path, run: str, **keywords: Any) -> dict[str, Any]:
        calls.append(keywords)
        return {"phase": "baseline", "run_id": run, "monitored": [], "unmonitored": {}}

    with (
        mock.patch.object(runner, "baseline_phase", recorder),
        mock.patch.object(runner, "open_store", lambda: object()),
        mock.patch.object(runner, "model_feature_list_sha", lambda root, model: None),
    ):
        runner.main(["baseline", f"--run={RUN}", f"--work={folder / 'work'}", "--live-type=HAI_3", "--model=m2"])
        runner.main(["baseline", f"--run={RUN}", f"--work={folder / 'work'}", "--live-type=", "--model=m2"])
        refused = runner.main(["baseline", f"--run={RUN}", f"--work={folder / 'work'}", "--live-type=HAI_3"])
    live, gate = calls[0], calls[1]
    problems = [] if live["hai_types"] == ("HAI_3",) and live["model"] == "m2" else [f"the live baseline asked for {live}"]
    if gate["hai_types"] != HAI_TYPES:
        problems.append(f"the gate task's baseline asked for {gate['hai_types']}, not all six types")
    return problems + ([] if refused == 2 and len(calls) == 2 else [f"a live baseline with no model exited {refused} after {len(calls)} fetches"])


def baseline_phase_records_the_production_version_of_a_live_run(folder: Path) -> Problems:
    """A live baseline records the Production run and version it fetched, which bind the run ID to the model; the gate task's record stays as it was."""
    baseline, work = aws_cases.workspace_with_two_baselines(folder)
    runner = importlib.import_module("monitoring.aws_run")
    live = runner.baseline_phase(baseline.store, work, RUN, project=baseline.work.project, hai_types=("HAI_1",), record_versions=True)
    gate = runner.baseline_phase(baseline.store, work, OTHER_RUN, project=baseline.work.project, hai_types=("HAI_1",))
    problems = [] if live.get("versions") == {"HAI_1": "run1:1"} else [f"the live record holds {live.get('versions')}"]
    return problems + ([] if "versions" not in gate else ["the gate task's baseline record gained a versions entry"])


def live_type_is_a_hai_type(folder: Path) -> Problems:
    code, text = phase(folder, "current", live="HAI_9")
    return [] if code == 2 and "invalid choice" in text and "HAI_9" in text else [f"expected exit 2 with an invalid choice HAI_9, got {code}: {text[:300]}"]


def live_publish(folder: Path, s3: aws_cases.MemoryS3, *, outputs: bool = True) -> tuple[Path, Any]:
    work = workspace_for(folder)
    out = work / "drift_live" / RUN
    out.mkdir(parents=True)
    if outputs:
        (out / "drift_HAI_1.json").write_text(json.dumps({"run_id": RUN, "hai_type": "HAI_1"}) + "\n")
        (out / "report_HAI_1.html").write_text("<html>report</html>\n")
        (out / "snapshot_HAI_1.json").write_text(json.dumps({"id": "01a1eacb-e868-74d7-b7f3-8795aab9b89f", "metadata": {"run_id": RUN}}) + "\n")
        (out / "project_HAI_1.json").write_text(json.dumps({"id": "01a1eacb-e867-7d8d-94f8-d8cac187f378", "name": "m2 Elastic Net HAI_1 CLABSI"}) + "\n")
    return work, importlib.import_module("monitoring.aws_run").publish_live_phase(s3, BUCKET, work, RUN, "HAI_1")


def publish_writes_the_demo_prefix_then_the_marker(folder: Path) -> Problems:
    s3 = aws_cases.MemoryS3()
    work, done = live_publish(folder, s3)
    out = work / "drift_live" / RUN
    prefix = f"drift_demo/{RUN}/"
    expected = [prefix + name for name in ("drift_HAI_1.json", "report_HAI_1.html", "snapshot_HAI_1.json", "project_HAI_1.json", "_complete.json")]
    problems = [] if s3.puts == expected else [f"the writes were {s3.puts}, expected {expected}"]
    if any(key.startswith(("drift/", f"drift/{RUN}")) for key in s3.puts):
        problems.append("a live check wrote into the drift/ prefix of the gate runs")
    marker = json.loads(s3.objects[(BUCKET, f"drift_demo/{RUN}/_complete.json")])
    files = {name: hashlib.sha256((out / name).read_bytes()).hexdigest() for name in OUTPUTS}
    if marker != {"kind": "drift_live", "run_id": RUN, "hai_type": "HAI_1", "model": "m2", "files": files}:
        problems.append(f"the marker is {marker}")
    return problems or ([] if sorted(done["files"]) == OUTPUTS else [f"the phase reports {done}"])


def publish_repeats_to_the_same_result(folder: Path) -> Problems:
    s3 = aws_cases.MemoryS3()
    work, _ = live_publish(folder, s3)
    first = dict(s3.objects)
    importlib.import_module("monitoring.aws_run").publish_live_phase(s3, BUCKET, work, RUN, "HAI_1")
    return [] if s3.objects == first and len(first) == 5 else ["a second publish changed or added objects"]


def publish_again(folder: Path, s3: aws_cases.MemoryS3, *, html: str | None = None, record: dict[str, Any] | None = None) -> Any:
    """Publish the run that ``live_publish`` prepared in ``folder`` again, after the outputs were changed as the arguments say."""
    out = folder / "work" / "drift_live" / RUN
    if html is not None:
        (out / "report_HAI_1.html").write_text(html)
    if record is not None:
        (out / "drift_HAI_1.json").write_text(json.dumps(record) + "\n")
    return importlib.import_module("monitoring.aws_run").publish_live_phase(s3, BUCKET, folder / "work", RUN, "HAI_1")


def publish_creates_each_object_once_and_the_marker_last(folder: Path) -> Problems:
    """Every demo object and the marker are created with If-None-Match, so no write replaces an object, and the marker is the last write."""
    s3 = aws_cases.MemoryS3()
    live_publish(folder, s3)
    prefix = f"drift_demo/{RUN}/"
    return [] if s3.conditions == ["*"] * 5 and s3.puts[-1] == prefix + "_complete.json" else [f"the writes were {s3.puts} with conditions {s3.conditions}"]


def publish_of_a_completed_run_changes_nothing(folder: Path) -> Problems:
    """A completed run published again with a recomputed HTML (other bytes, same record) writes nothing, so the stored HTML keeps matching the marker."""
    s3 = aws_cases.MemoryS3()
    live_publish(folder, s3)
    stored = dict(s3.objects)
    done = publish_again(folder, s3, html="<html>recomputed with other timestamps</html>\n")
    marker = json.loads(s3.objects[(BUCKET, f"drift_demo/{RUN}/_complete.json")])
    html = s3.objects[(BUCKET, f"drift_demo/{RUN}/report_HAI_1.html")]
    problems = [] if s3.objects == stored and len(s3.puts) == 5 and done.get("reused") is True else ["a completed run was written again"]
    return problems + ([] if marker["files"]["report_HAI_1.html"] == hashlib.sha256(html).hexdigest() else ["the stored HTML does not match the marker"])


def publish_refuses_a_completed_run_with_another_record(folder: Path) -> Problems:
    s3 = aws_cases.MemoryS3()
    live_publish(folder, s3)
    stored = dict(s3.objects)
    try:
        publish_again(folder, s3, record={"run_id": RUN, "hai_type": "HAI_1", "drift_share": 0.2})
    except importlib.import_module("monitoring.drift_check").CheckError as error:
        return [] if "demo_run_conflict" in str(error) and s3.objects == stored else [f"unexpected: {error}, objects changed {s3.objects != stored}"]
    return ["a completed run was published again with another record"]


def publish_retry_after_an_interrupted_marker_completes_it(folder: Path) -> Problems:
    """A publish stopped before its marker leaves the objects and no marker. The retry with the same bytes completes it with a marker that matches them; a retry
    with other HTML is refused by name and never replaces the object or writes a marker."""
    s3 = aws_cases.MemoryS3()
    s3.fail_on = {f"drift_demo/{RUN}/_complete.json"}
    try:
        live_publish(folder, s3)
    except aws_cases.Interrupted:
        pass
    else:
        return ["the interrupted marker put did not interrupt the publish"]
    marker_key = (BUCKET, f"drift_demo/{RUN}/_complete.json")
    problems = [] if marker_key not in s3.objects and len(s3.objects) == 4 else [f"the interrupted publish left {sorted(key[1] for key in s3.objects)}"]
    stored = dict(s3.objects)
    try:
        publish_again(folder, s3, html="<html>other bytes</html>\n")
    except importlib.import_module("monitoring.drift_check").CheckError as error:
        if "demo_object_conflict" not in str(error) or s3.objects != stored:
            problems.append(f"unexpected refusal or write: {error}")
    else:
        problems.append("other HTML replaced an object of an unfinished publish")
    s3.fail_on = set()
    (folder / "work" / "drift_live" / RUN / "report_HAI_1.html").write_text("<html>report</html>\n")
    publish_again(folder, s3)
    marker = json.loads(s3.objects[marker_key])
    html = s3.objects[(BUCKET, f"drift_demo/{RUN}/report_HAI_1.html")]
    return problems + (
        [] if marker["files"]["report_HAI_1.html"] == hashlib.sha256(html).hexdigest() else ["the completed marker does not match the stored HTML"]
    )


def publish_refuses_missing_outputs(folder: Path) -> Problems:
    s3 = aws_cases.MemoryS3()
    try:
        live_publish(folder, s3, outputs=False)
    except importlib.import_module("monitoring.drift_check").CheckError as error:
        return [] if "record_missing" in str(error) and not s3.objects else [f"unexpected: {error}, objects {len(s3.objects)}"]
    return ["a live run without its outputs was published"]


def publish_cli_refuses_a_bad_bucket_name(folder: Path) -> Problems:
    work = workspace_for(folder)
    done = run_command(
        PYTHON, ["-m", "monitoring.aws_run", "publish", f"--run={RUN}", f"--work={work}", "--live-type=HAI_1", "--bucket=A_bad_bucket"], cwd=ROOT, timeout=120
    )
    text = done.stdout + done.stderr
    return [] if done.returncode == 2 and "bucket_invalid" in text else [f"expected exit 2 naming bucket_invalid, got {done.returncode}: {text[:300]}"]


@contextmanager
def loaded_live_dag() -> Any:
    """The real live DAG file against the stand-ins of the drift DAG cases."""
    previous = dag_cases.DAG_FILE
    dag_cases.DAG_FILE = LIVE_DAG_FILE
    try:
        with dag_cases.loaded_dag() as module:
            yield module
    finally:
        dag_cases.DAG_FILE = previous


IAM_FOLDER = ROOT / "infra/iam"
ALLOWED_UI_GRANTS = ["evidently_ui.tf: evidently_ui_restore_read"]
GRANT_BLOCK = re.compile(
    r'resource\s+"(aws_iam_role_policy|aws_iam_role_policy_attachment|aws_iam_policy_attachment|aws_iam_role_policies_exclusive)"\s+"[^"]+"\s*\{'
)


def grants_to_the_ui_role(folder: Path) -> list[str]:
    """The policy-attaching IAM resources of the Terraform files in ``folder`` whose body names the Evidently UI task role."""
    found = []
    for path in sorted(folder.glob("*.tf")):
        text = path.read_text()
        for match in GRANT_BLOCK.finditer(text):
            depth, end = 1, match.end()
            while depth and end < len(text):
                depth += {"{": 1, "}": -1}.get(text[end], 0)
                end += 1
            if "evidently_ui_task" in text[match.end() : end]:
                found.append(f"{path.name}: {text[match.start() : match.end()].split(chr(34))[3]}")
    return found


def ui_role_exists_and_holds_no_grant(folder: Path) -> Problems:
    """The Evidently UI task role is defined in the IAM stack and nothing attaches a policy to it: it calls no AWS service, so it needs no grant."""
    role_file = IAM_FOLDER / "evidently_ui.tf"
    problems = (
        []
        if role_file.is_file() and 'resource "aws_iam_role" "evidently_ui_task"' in role_file.read_text()
        else ["infra/iam/evidently_ui.tf does not define the role"]
    )
    found = grants_to_the_ui_role(IAM_FOLDER)
    return problems + ([] if found == ALLOWED_UI_GRANTS else [f"the policies attached to the UI role are {found}, expected only {ALLOWED_UI_GRANTS}"])


def ui_role_check_catches_a_planted_s3_grant(folder: Path) -> Problems:
    """The check above is effective: in a copy of the IAM stack, an inline S3 read policy and a managed attachment on the role are each found."""
    plant = folder / "iam_copy"
    shutil.copytree(IAM_FOLDER, plant, ignore=shutil.ignore_patterns(".terraform", "tests", "*.tfstate*"))
    unplanted = grants_to_the_ui_role(plant)  # the one policy the restore step reads with
    inline = [
        'resource "aws_iam_role_policy" "ui_reads_s3" {',
        "  role   = aws_iam_role.evidently_ui_task[0].name",
        '  policy = jsonencode({ Statement = [{ Effect = "Allow", Action = ["s3:GetObject"], Resource = ["arn:aws:s3:::example/*"] }] })',
        "}",
    ]
    managed = [
        'resource "aws_iam_role_policy_attachment" "ui_managed" {',
        "  role       = aws_iam_role.evidently_ui_task[0].name",
        '  policy_arn = "arn:aws:iam::aws:policy/S3ReadOnly"',
        "}",
    ]
    (plant / "planted.tf").write_text("\n".join([*inline, *managed]) + "\n")
    planted = grants_to_the_ui_role(plant)
    problems = [] if unplanted == ALLOWED_UI_GRANTS else [f"the copy held {unplanted}, expected only {ALLOWED_UI_GRANTS}"]
    expected = sorted([*ALLOWED_UI_GRANTS, "planted.tf: ui_managed", "planted.tf: ui_reads_s3"])
    return problems + ([] if sorted(planted) == expected else [f"the planted grants were found as {planted}"])


def dag_declares_one_job_chain_and_triggers_nothing(folder: Path) -> Problems:
    with loaded_live_dag() as module:
        dag = module.dag
        nodes = {name: node.upstream_task_ids for name, node in dag.task_dict.items()}
        expected = {"preflight": set(), "job": {"preflight"}, "evidence": {"job"}, "gate": {"evidence"}}
        problems = [] if nodes == expected else [f"the graph is {nodes}"]
        wanted = {"dag_id": "drift_live_check_dag", "schedule": None, "catchup": False, "max_active_runs": 1, "default_args": {"retries": 0}}
        problems += [f"DAG setting {key} is {dag.settings.get(key)!r}, expected {value!r}" for key, value in wanted.items() if dag.settings.get(key) != value]
        problems += [f"{name} triggers {node.settings['trigger_dag_id']}" for name, node in dag.task_dict.items() if "trigger_dag_id" in node.settings]
        return problems


def dag_form_takes_a_run_a_model_and_a_type(folder: Path) -> Problems:
    with loaded_live_dag() as module:
        params = module.dag.settings["params"]
        problems = [] if sorted(params) == ["hai_type", "mode", "model", "run_id"] else [f"the form holds {sorted(params)}"]
        if params["run_id"].default is not None or params["run_id"].settings.get("pattern") != f"^{RUN_ID}$":
            problems.append("run_id is not a required run ID")
        if params["mode"].default != "real" or params["mode"].settings.get("enum") != ["real"]:
            problems.append("the mode is not real only")
        for name, values in (("model", list(MODELS)), ("hai_type", list(HAI_TYPES))):
            if params[name].default is not None or params[name].settings.get("enum") != values:
                problems.append(f"{name} is not a required choice of {values}")
        return problems


def dag_request_carries_the_model_and_the_type(folder: Path) -> Problems:
    with loaded_live_dag() as module:
        runtime = sys.modules["orchestration.dag_runtime"]
        form = {"run_id": RUN, "mode": "real", "model": "m2", "hai_type": "HAI_3"}
        instance = SimpleNamespace(dag_id="drift_live_check_dag", try_number=1, task_id="job", run_id="manual__test")
        with mock.patch.object(dag_cases, "CONTEXT", {"context": {"params": form, "ti": instance}}):
            request = runtime.step_request("drift_live_run", dag_cases.CONTEXT["context"])
        problems = [] if request.training == {"model": "m2", "hai_type": "HAI_3"} else [f"the request carries {request.training}"]
        if (request.template, request.run_id, request.input_run_id, request.mode) != ("drift_live_run", RUN, None, "real"):
            problems.append(f"the request is {request}")
        return problems or ([] if module.dag.settings["dag_id"] == "drift_live_check_dag" else ["wrong DAG"])


def dag_is_registered_in_the_job_tables(folder: Path) -> Problems:
    steps = json.loads((ROOT / "config/orchestration/ecs_broker_jobs.json").read_text())["steps"]
    host = json.loads((ROOT / "config/orchestration/host_jobs.json").read_text())["dags"]
    problems = [] if steps.get("drift_live_run") == "drift" else [f"drift_live_run maps to {steps.get('drift_live_run')!r}, not the drift job"]
    return problems + ([] if host.get("drift_live_check_dag") == "drift" else [f"the host table maps the DAG to {host.get('drift_live_check_dag')!r}"])


def cases() -> list[tuple[str, Callable[[Path], Problems]]]:
    refusals: list[tuple[str, Callable[[Path], Problems]]] = [
        ("live_refuses_when_no_row_is_of_the_newest_year", refused("no_current_window", lambda f: write_baseline(f, current_rows=0), "2024")),
        ("live_refuses_below_the_minimum_of_100_rows", refused("too_few_current_rows", lambda f: write_baseline(f, current_rows=99), "100")),
        ("live_refuses_when_no_row_is_older", refused("no_reference_rows", lambda f: write_baseline(f, years=()), "2023")),
        ("live_refuses_the_sensitivity_population", refused("population_not_monitored", lambda f: write_baseline(f, population="sensitivity"))),
        ("live_refuses_a_manifest_without_the_newest_year", refused("manifest_invalid", lambda f: write_baseline(f, newest=None), "newest_window_year")),
        (
            "live_refuses_a_feature_missing_from_the_matrix",
            refused("feature_missing", lambda f: write_baseline(f, features=(*FEATURES, "f_absent")), "f_absent"),
        ),
    ]
    return [
        *refusals,
        (
            "live_refuses_a_feature_that_loses_its_values_in_the_newest_year",
            refused("feature_all_missing", lambda f: write_baseline(f, null_in_newest=("f_num_b",)), "f_num_b"),
        ),
        (
            "live_refuses_when_no_feature_has_a_reference_value",
            refused("no_comparable_feature", lambda f: write_baseline(f, null_before_newest=FEATURES)),
        ),
        ("live_refuses_an_unknown_model", refuses_an_unknown_model),
        ("live_names_the_missing_dependency", names_the_missing_dependency),
        ("live_splits_by_year_and_checks", splits_by_year_and_checks),
        ("live_excludes_a_feature_without_reference_values", excludes_a_feature_without_reference_values),
        ("live_rerun_with_an_excluded_feature_reuses_the_bytes", a_rerun_with_an_excluded_feature_reuses_the_bytes),
        ("live_stays_below_the_share_without_a_shift", stays_below_the_share_without_a_shift),
        ("live_writes_the_record_and_html_to_the_demo_folder_only", writes_the_record_and_the_html_to_the_demo_folder_only),
        ("live_rerun_adds_nothing", rerun_adds_nothing),
        ("live_another_run_adds_a_snapshot", another_run_adds_a_snapshot_to_the_same_project),
        ("live_another_model_gets_its_own_project", another_model_gets_its_own_project),
        ("live_passes_the_record_to_each_sink_once", passes_the_record_to_each_sink_once),
        ("live_restore_rebuilds_an_empty_workspace", restore_rebuilds_an_empty_workspace),
        ("live_restore_rerun_is_a_noop", restore_rerun_is_a_noop),
        ("live_restore_skips_and_reports_an_incomplete_run", restore_skips_and_reports_an_incomplete_run),
        ("live_restore_refuses_a_mismatched_hash", restore_refuses_a_mismatched_hash),
        ("live_restore_skips_a_marker_that_cannot_be_trusted", restore_skips_a_marker_that_cannot_be_trusted),
        ("live_restore_adds_to_an_existing_project_of_the_same_name", restore_adds_to_an_existing_project_of_the_same_name),
        ("live_restore_skips_a_snapshot_of_another_run", restore_skips_a_snapshot_of_another_run),
        ("live_restore_result_loads_in_the_ui_loader", the_ui_loader_lists_the_restored_projects),
        ("live_restore_cli_refuses_a_bad_bucket_name", restore_cli_refuses_a_bad_bucket_name),
        ("live_changed_inputs_under_the_same_run_are_refused", changed_inputs_under_the_same_run_are_refused),
        ("live_binding_survives_lost_cached_outputs", the_binding_survives_lost_cached_outputs),
        ("live_atomic_writer_leaves_an_unrelated_store_alone", the_atomic_writer_leaves_an_unrelated_store_alone),
        ("live_overlapping_writer_contexts_leave_the_library_unpatched", overlapping_writer_contexts_leave_the_library_unpatched),
        ("live_atomic_writer_nests_and_restores_on_every_exit", the_atomic_writer_nests_and_restores_on_every_exit),
        ("live_rerun_reuses_the_recorded_outputs_byte_for_byte", a_rerun_reuses_the_recorded_outputs_byte_for_byte),
        ("live_reused_run_calls_no_sink", a_reused_run_calls_no_sink),
        ("live_refuses_a_manifest_older_than_the_data", a_manifest_older_than_the_data_is_refused),
        ("live_reference_years_come_from_the_data", reference_years_come_from_the_data),
        ("live_interrupted_snapshot_write_leaves_the_workspace_loadable", an_interrupted_snapshot_write_leaves_the_workspace_loadable),
        ("live_damaged_snapshot_file_is_set_aside_not_fatal", a_damaged_snapshot_file_is_set_aside_not_fatal),
        ("live_task_check_runs_the_live_check", check_phase_runs_the_live_check),
        ("live_task_check_refuses_an_unmonitored_type", check_phase_refuses_an_unmonitored_type),
        ("live_task_current_reads_nothing", current_phase_reads_nothing_in_live_mode),
        ("live_task_baseline_covers_only_the_live_type", baseline_phase_covers_only_the_live_type),
        ("live_task_baseline_records_the_production_version", baseline_phase_records_the_production_version_of_a_live_run),
        ("live_task_type_is_a_hai_type", live_type_is_a_hai_type),
        ("live_task_publish_writes_the_demo_prefix_then_the_marker", publish_writes_the_demo_prefix_then_the_marker),
        ("live_task_publish_repeats_to_the_same_result", publish_repeats_to_the_same_result),
        ("live_task_publish_creates_each_object_once_and_the_marker_last", publish_creates_each_object_once_and_the_marker_last),
        ("live_task_publish_of_a_completed_run_changes_nothing", publish_of_a_completed_run_changes_nothing),
        ("live_task_publish_refuses_a_completed_run_with_another_record", publish_refuses_a_completed_run_with_another_record),
        ("live_task_publish_retry_after_an_interrupted_marker_completes_it", publish_retry_after_an_interrupted_marker_completes_it),
        ("live_task_publish_refuses_missing_outputs", publish_refuses_missing_outputs),
        ("live_task_publish_cli_refuses_a_bad_bucket_name", publish_cli_refuses_a_bad_bucket_name),
        ("live_ui_role_exists_and_holds_no_grant", ui_role_exists_and_holds_no_grant),
        ("live_ui_role_check_catches_a_planted_s3_grant", ui_role_check_catches_a_planted_s3_grant),
        ("live_dag_declares_one_job_chain_and_triggers_nothing", dag_declares_one_job_chain_and_triggers_nothing),
        ("live_dag_form_takes_a_run_a_model_and_a_type", dag_form_takes_a_run_a_model_and_a_type),
        ("live_dag_request_carries_the_model_and_the_type", dag_request_carries_the_model_and_the_type),
        ("live_dag_is_registered_in_the_job_tables", dag_is_registered_in_the_job_tables),
    ]
