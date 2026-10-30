"""E2E cases for the baseline reader and the current window reader on synthetic fixtures: no real patient, hospital or CMS record is read or written.

The baseline cases use the real pinned DVC in a throwaway Git checkout with a local remote, the DVC E2E's run store, and a registry stand-in that serves the
``production`` alias. The current window cases use the training extract cases' synthetic feature reader, which stands in for Feast and the Iceberg
catalog (declared substitutions: no Feast, no catalog and no network). The readers are imported inside each case so that a missing module fails its own case.
"""

from __future__ import annotations

import hashlib
import importlib
import json
from collections.abc import Callable
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest import mock

import duckdb

from monitoring import drift_cases
from scripts.process import run_command
from scripts.training import lane_b_cases
from scripts.training.run_dvc_baseline_e2e import LocalRunStore, Workspace

ROOT = Path(__file__).resolve().parents[1]
PYTHON = str(ROOT / ".venv/bin/python")
Problems = list[str]
MODEL = "hai_model_HAI_1"


class MissingAlias(Exception):
    """What MLflow raises for an alias that does not exist."""

    error_code = "RESOURCE_DOES_NOT_EXIST"


class StoreStandIn:
    """The MLflow client calls the baseline reader makes: the run store of the DVC E2E plus the registry alias lookup."""

    def __init__(self, store: LocalRunStore) -> None:
        self.store = store
        self.aliases: dict[tuple[str, str], Any] = {}

    def get_model_version_by_alias(self, name: str, alias: str) -> Any:
        if (name, alias) not in self.aliases:
            raise MissingAlias(f"no alias {alias} on {name}")
        return self.aliases[(name, alias)]

    def __getattr__(self, name: str) -> Any:
        return getattr(self.store, name)


def feature_document(columns: list[str]) -> dict[str, Any]:
    return {"slot": "m2_elastic_net" if columns else None, "references": [f"feature_matrix:{name}" for name in columns], "columns": columns}


def manifest_document(**changes: Any) -> dict[str, Any]:
    return {"hai_type": "HAI_1", "population": "primary", "newest_window_year": 2021, "label_column": "label_value", **changes}


class Baseline:
    """A recorded baseline in a DVC workspace: matrix, metafile, feature list and manifest artifacts, and a Production alias."""

    def __init__(
        self, top: Path, *, columns: list[str] | None = None, manifest: dict[str, Any] | None = None, artifacts: bool = True, alias: bool = True
    ) -> None:
        self.work = Workspace(top)
        drift_cases.write_matrix(self.work.matrix, 1500, "2019 + i // 501", (), None, None)
        self.original = hashlib.sha256(self.work.matrix.read_bytes()).hexdigest()
        self.work.record()
        self.columns = list(drift_cases.FEATURES) if columns is None else columns
        self.manifest = manifest_document() if manifest is None else manifest
        scratch = top / "artifacts"
        scratch.mkdir()
        self.store = StoreStandIn(self.work.store)
        if artifacts:
            (scratch / "feature_list.json").write_text(json.dumps(feature_document(self.columns)) + "\n")
            (scratch / "manifest_HAI_1.json").write_text(json.dumps(self.manifest) + "\n")
            self.work.store.log_artifact("run1", str(scratch / "feature_list.json"), "bundle")
            self.work.store.log_artifact("run1", str(scratch / "manifest_HAI_1.json"), "extract")
        if alias:
            self.store.aliases[(MODEL, "production")] = SimpleNamespace(run_id="run1", version="1")
        self.work.wipe_local_copies()
        self.out = top / "baseline_out"

    def fetch(self) -> Any:
        baseline = importlib.import_module("monitoring.baseline")
        return baseline.fetch(self.store, "HAI_1", self.out, project=self.work.project)


def refuses(name: str, code: int, build: Callable[[Path], Baseline]) -> Callable[[Path], Problems]:
    def case(folder: Path) -> Problems:
        drift_check = importlib.import_module("monitoring.drift_check")
        baseline = build(folder)
        try:
            baseline.fetch()
        except drift_check.CheckError as error:
            problems = [] if name in str(error) else [f"the error does not name {name}: {str(error)[:300]}"]
            problems += [] if error.code == code else [f"exit code {error.code}, expected {code}"]
        else:
            problems = [f"no CheckError; expected one naming {name}"]
        if baseline.out.exists() and any(baseline.out.iterdir()):
            problems.append(f"a refused fetch left files: {sorted(path.name for path in baseline.out.iterdir())}")
        return problems

    return case


def baseline_round_trip(folder: Path) -> Problems:
    baseline = Baseline(folder)
    result = baseline.fetch()
    problems = []
    if hashlib.sha256((baseline.out / "reference.parquet").read_bytes()).hexdigest() != baseline.original:
        problems.append("the fetched matrix differs from the recorded one")
    if json.loads((baseline.out / "reference_manifest.json").read_text()) != baseline.manifest:
        problems.append("the manifest was changed on the way")
    if json.loads((baseline.out / "features.json").read_text()) != baseline.columns:
        problems.append("the feature list is not the model's columns")
    if (result.run_id, result.hai_type) != ("run1", "HAI_1"):
        problems.append(f"the result names {result.run_id} and {result.hai_type}")
    return problems


def baseline_rerun_changes_nothing(folder: Path) -> Problems:
    baseline = Baseline(folder)
    baseline.fetch()
    first = {path.name: path.read_bytes() for path in baseline.out.iterdir()}
    baseline.work.wipe_local_copies()
    baseline.fetch()
    second = {path.name: path.read_bytes() for path in baseline.out.iterdir()}
    return (
        []
        if first == second and sorted(first) == ["features.json", "reference.parquet", "reference_manifest.json"]
        else ["a rerun changed the files or left others"]
    )


def tampered_remote(folder: Path) -> Problems:
    baseline = Baseline(folder)
    for stored in baseline.work.remote.rglob("*"):
        if stored.is_file():
            stored.chmod(0o644)
            stored.write_bytes(b"not the matrix")
    drift_check = importlib.import_module("monitoring.drift_check")
    try:
        baseline.fetch()
    except drift_check.CheckError as error:
        return [] if "baseline_unloadable" in str(error) else [f"the error does not name baseline_unloadable: {str(error)[:300]}"]
    return ["a replaced remote object was accepted"]


def baseline_feeds_the_check(folder: Path) -> Problems:
    """The reader's files are exactly what the drift check takes: with a current matrix of the baseline's own last year, the check skips for no new window."""
    baseline = Baseline(folder)
    baseline.fetch()
    current = folder / "current.parquet"
    drift_cases.write_matrix(current, 1200, "2021", (), None, None)
    manifest = folder / "current_manifest.json"
    manifest.write_text(json.dumps({"hai_type": "HAI_1", "population": "primary"}) + "\n")
    arguments = [
        f"--reference-matrix={baseline.out / 'reference.parquet'}",
        f"--reference-manifest={baseline.out / 'reference_manifest.json'}",
        f"--features={baseline.out / 'features.json'}",
        f"--current-matrix={current}",
        f"--current-manifest={manifest}",
        f"--run-id={drift_cases.RUN_ID}",
        f"--out={folder / 'record'}",
    ]
    done = run_command(PYTHON, ["-m", "monitoring.drift_check", *arguments], cwd=ROOT, timeout=300)
    if done.returncode:
        return [f"the check refused the reader's files (exit {done.returncode}): {(done.stdout + done.stderr)[:300]}"]
    record = json.loads((folder / "record/drift_HAI_1.json").read_text())
    return (
        [] if (record["status"], record["skip_reason"]) == ("skipped", "no_new_window") else [f"unexpected record {record['status']} {record['skip_reason']}"]
    )


def baseline_cli_names_missing_mlflow(folder: Path) -> Problems:
    done = run_command(
        PYTHON, ["-m", "monitoring.baseline", "--hai-type=HAI_1", "--tracking-uri=file:///nonexistent", f"--out={folder / 'out'}"], cwd=ROOT, timeout=120
    )
    text = done.stdout + done.stderr
    problems = [] if done.returncode == 3 else [f"expected exit 3, got {done.returncode}: {text[:300]}"]
    if "dependency_missing" not in text or "mlflow" not in text.lower():
        problems.append(f"the message does not name the missing package: {text[:300]}")
    return problems


class Current:
    """The training extract cases' synthetic publication: three hospitals, windows 2024 to 2026, hospitals 1 and 2 in the primary population."""

    def __init__(self, top: Path) -> None:
        self.root = top / "root"
        self.root.mkdir()
        lane_b_cases.fixture(self.root)
        self.reader = lane_b_cases.SyntheticReader(self.root)
        self.out = top / "current_out"

    def build(self, **changes: Any) -> Any:
        current_window = importlib.import_module("monitoring.current_window")
        settings: dict[str, Any] = {
            "features_run": "latest",
            "population": "primary",
            "hai_type": "HAI_1",
            "newest_window_year": 2024,
            "features": ["c001__score__number", "state_code"],
            "out": self.out,
        }
        return current_window.build(self.root, self.reader, **{**settings, **changes})

    def rows(self) -> list[tuple[Any, ...]]:
        return duckdb.read_parquet(str(self.out / "current.parquet")).order("spine_key").fetchall()


def current_after_the_baseline(folder: Path) -> Problems:
    current = Current(folder)
    current.build()
    rows = current.rows()
    names = duckdb.read_parquet(str(current.out / "current.parquet")).columns
    manifest = json.loads((current.out / "current_manifest.json").read_text())
    problems = []
    if sorted(names) != sorted(["spine_key", "window_year", "c001__score__number", "state_code"]):
        problems.append(f"the columns are {names}")
    if sorted(row[names.index("spine_key")] for row in rows) != ["000001:2025", "000001:2026", "000002:2025", "000002:2026"]:
        problems.append(f"the rows are {[row[names.index('spine_key')] for row in rows]}")
    if (manifest.get("hai_type"), manifest.get("population"), manifest.get("publication_run_id"), manifest.get("row_count")) != (
        "HAI_1",
        "primary",
        lane_b_cases.PUBLICATION,
        4,
    ):
        problems.append(f"the manifest is {manifest}")
    return problems


def current_follows_the_population(folder: Path) -> Problems:
    current = Current(folder)
    current.build(population="sensitivity")
    return [] if len(current.rows()) == 6 else [f"the sensitivity population gave {len(current.rows())} rows, expected 6"]


def current_keeps_windows_without_a_label(folder: Path) -> Problems:
    """The first synthetic row has no published SIR. The training extract drops it, the drift reader must keep it."""
    current = Current(folder)
    current.build(newest_window_year=2023)
    keys = [row[duckdb.read_parquet(str(current.out / "current.parquet")).columns.index("spine_key")] for row in current.rows()]
    return [] if "000001:2024" in keys and len(keys) == 6 else [f"windows without a label were dropped: {keys}"]


def current_no_new_window(folder: Path) -> Problems:
    current = Current(folder)
    current.build(newest_window_year=2026)
    manifest = json.loads((current.out / "current_manifest.json").read_text())
    return [] if manifest["row_count"] == 0 and current.rows() == [] else ["a baseline at the newest year left rows or no manifest"]


def current_refuses(name: str, **changes: Any) -> Callable[[Path], Problems]:
    def case(folder: Path) -> Problems:
        drift_check = importlib.import_module("monitoring.drift_check")
        current = Current(folder)
        try:
            current.build(**changes)
        except drift_check.CheckError as error:
            problems = [] if name in str(error) else [f"the error does not name {name}: {str(error)[:300]}"]
        else:
            problems = [f"no CheckError; expected one naming {name}"]
        if current.out.exists() and any(current.out.iterdir()):
            problems.append("a refused build left files")
        return problems

    return case


def current_refuses_another_publication(folder: Path) -> Problems:
    drift_check = importlib.import_module("monitoring.drift_check")
    current = Current(folder)
    original = current.reader.historical

    def other(references: Any, run: str) -> Any:
        return SimpleNamespace(**{**vars(original(references, run)), "publication": SimpleNamespace(run_id="other")})

    try:
        with mock.patch.object(current.reader, "historical", side_effect=other):
            current.build()
    except drift_check.CheckError as error:
        return [] if "publication_mismatch" in str(error) else [f"the error does not name publication_mismatch: {str(error)[:300]}"]
    return ["a historical result of another publication was accepted"]


def current_rerun_changes_nothing(folder: Path) -> Problems:
    current = Current(folder)
    current.build()
    first = ((current.out / "current_manifest.json").read_bytes(), current.rows())
    current.build()
    second = ((current.out / "current_manifest.json").read_bytes(), current.rows())
    return (
        []
        if first == second and sorted(path.name for path in current.out.iterdir()) == ["current.parquet", "current_manifest.json"]
        else ["a rerun changed the output"]
    )


def current_cli_names_missing_packages(folder: Path) -> Problems:
    features = folder / "features.json"
    features.write_text(json.dumps(["c001__score__number"]) + "\n")
    arguments = [
        "--hai-type=HAI_1",
        "--population=primary",
        "--features-run=latest",
        "--newest-window-year=2024",
        f"--features={features}",
        f"--out={folder / 'out'}",
    ]
    done = run_command(PYTHON, ["-m", "monitoring.current_window", *arguments], cwd=ROOT, timeout=120)
    text = done.stdout + done.stderr
    problems = [] if done.returncode == 3 else [f"expected exit 3, got {done.returncode}: {text[:300]}"]
    if "dependency_missing" not in text:
        problems.append(f"the message does not name the missing dependency: {text[:300]}")
    return problems


def cases() -> list[tuple[str, Callable[[Path], Problems]]]:
    return [
        ("baseline_round_trip", baseline_round_trip),
        ("baseline_rerun_changes_nothing", baseline_rerun_changes_nothing),
        ("baseline_files_feed_the_check", baseline_feeds_the_check),
        ("baseline_refuses_a_missing_production_alias", refuses("no_production_model", 4, lambda top: Baseline(top, alias=False))),
        ("baseline_refuses_a_model_without_features", refuses("no_features", 4, lambda top: Baseline(top, columns=[]))),
        ("baseline_refuses_missing_artifacts", refuses("artifact_missing", 2, lambda top: Baseline(top, artifacts=False))),
        (
            "baseline_refuses_a_manifest_of_another_type",
            refuses("manifest_invalid", 2, lambda top: Baseline(top, manifest=manifest_document(hai_type="HAI_2"))),
        ),
        (
            "baseline_refuses_a_manifest_without_a_window",
            refuses("manifest_invalid", 2, lambda top: Baseline(top, manifest=manifest_document(newest_window_year=None))),
        ),
        ("baseline_refuses_a_tampered_remote", tampered_remote),
        ("baseline_cli_names_missing_mlflow", baseline_cli_names_missing_mlflow),
        ("current_rows_after_the_baseline", current_after_the_baseline),
        ("current_follows_the_population", current_follows_the_population),
        ("current_keeps_windows_without_a_label", current_keeps_windows_without_a_label),
        ("current_with_no_new_window", current_no_new_window),
        ("current_refuses_an_unknown_feature", current_refuses("feature_unknown", features=["c001__score__number", "not_a_column"])),
        ("current_refuses_an_unknown_population", current_refuses("population_invalid", population="other")),
        ("current_refuses_another_publication", current_refuses_another_publication),
        ("current_rerun_changes_nothing", current_rerun_changes_nothing),
        ("current_cli_names_missing_packages", current_cli_names_missing_packages),
    ]
