"""Synthetic caller scenarios for model bodies, research statistics and separated analyses."""

from __future__ import annotations

import importlib
import json
import math
import os
import sys
import tempfile
import warnings
from collections.abc import Callable, Sequence
from dataclasses import replace
from datetime import date
from functools import partial
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import patch

import numpy as np

from scripts.training.model_test_support import SpatialOLS, exact_f, libraries, upper_f_tail


def require(condition: Any, reason: str) -> None:
    """Fail a scenario at its independently specified observable contract."""
    if not condition:
        raise AssertionError(reason)


def rejects(call: Callable[[], Any], reason: str) -> None:
    """Require the intended domain error, rather than any failing dependency or assertion."""
    from scripts.training.models.common import ModelError

    try:
        call()
    except ModelError as error:
        require(reason in str(error), f"wrong rejection: expected {reason}, got {error}")
    else:
        raise AssertionError(f"missing rejection: {reason}")


def request(years: Sequence[int] = range(2020, 2024), hospitals: int = 48) -> Any:
    """Build synthetic hospital panels over the given window years and small, explicitly non-native analysis settings."""
    from scripts.training.models.common import SLOTS, Dataset, Deadline, Feature, ModelRequest

    rng = np.random.default_rng(726)
    features = (
        Feature("feature_matrix_care_compare:process", "number", "C119", "care_compare"),
        Feature("feature_matrix_hospital_window:cmi", "number", "", "hospital_window"),
        Feature("feature_matrix_hospital:extra", "number", "C001", "hospital"),
    )
    rows = []
    for hospital in range(hospitals):
        effect = (hospital % 6 - 2.5) * 0.2
        for year in years:
            process, cmi, extra = rng.normal(size=3)
            rows.append(
                {
                    "spine_key": f"{990000 + hospital}:{year}",
                    "ccn": str(990000 + hospital),
                    "window_year": year,
                    "control_type_code": str(hospital % 6),
                    "county_fips": str(99001 + hospital // 4),
                    "process": float(process),
                    "cmi": float(cmi),
                    "extra": float(extra),
                    "hai_1__sir__number": float(2 + 0.4 * process + 0.25 * cmi + 0.6 * extra + effect + rng.normal(scale=0.03)),
                }
            )
    config: dict[str, Any] = {
        "master_seed": 42,
        "minimum_rows": {"training": 100, "holdout": 30},
        "thresholds": {"max_missing_fraction": 0.5, "significance_level": 0.05},
        "cross_validation": {"folds": 3, "shuffle": True},
        "moran": {"permutations": 99, "p_value": "p_sim"},
        "bootstrap": {"nested_comparison": {"draws": 99, "confidence_level": 0.95}, "mediation": {"draws": 99, "confidence_level": 0.95}},
        "budgets": {"fit_seconds": 60, "bootstrap_seconds": 20, "shap_seconds": 10},
        "shap": {"sample_cap": 128},
        "slots": {slot: {"grid": {}, "budget_seconds": 10} for slot in SLOTS},
    }
    config["slots"]["m2_elastic_net"]["grid"] = {"alpha": [0.01, 0.1], "l1_ratio": [0.1, 0.5]}
    config["slots"]["m4_boosting"]["grid"] = {"learning_rate": [0.1], "max_depth": [3], "n_estimators": [5]}
    config["slots"]["m4_boosting"]["check"] = {"grid": {"max_depth": [3], "n_estimators": [5]}}
    references = tuple(feature.reference for feature in features)
    lists = {slot: references for slot in SLOTS}
    lists["m1_ols"] = references[:1]
    lists["rq4_reduced"] = references[:1]
    return ModelRequest(
        Dataset(tuple(rows), features, "HAI_1"),
        lists,
        config,
        (),
        (references[0],),
        references[1],
        1,
        Deadline.after(60),
    )


def flagged_request(years: Sequence[int] = range(2018, 2024), hospitals: int = 60) -> Any:
    """``request`` plus a rare 0/1 flag, a two-level category and a partly missing predictor, for the Elastic Net fold and winsorizing cases."""
    from scripts.training.models.common import SLOTS, Dataset, Feature

    base = request(years=years, hospitals=hospitals)
    features = (
        *base.data.features,
        Feature("feature_matrix_hospital:flag", "number", "C002", "hospital"),
        Feature("feature_matrix_hospital:kind", "category", "C003", "hospital"),
    )
    rows = tuple(
        {
            **row,
            "flag": 1.0 if row["ccn"] == "990007" and row["window_year"] == 2019 else 0.0,
            "kind": "ab"[int(row["ccn"]) % 2],
            "extra": None if int(row["ccn"]) % 7 == 0 else row["extra"],
        }
        for row in base.data.rows
    )
    references = tuple(feature.reference for feature in features)
    lists = {slot: references for slot in SLOTS} | {"m1_ols": references[:1], "rq4_reduced": references[:1]}
    return replace(base, data=Dataset(rows, features, "HAI_1"), feature_lists=lists)


def planted(source: Any, years: Sequence[int], count: int = 6, value: float = 1.0e6) -> Any:
    """The same request with the process measure and CMI of the first ``count`` hospitals set to an extreme value in the given window years."""
    from scripts.training.models.common import Dataset

    marked = {str(990000 + number) for number in range(count)}
    rows = tuple(dict(row, process=value, cmi=value) if row["ccn"] in marked and row["window_year"] in years else row for row in source.data.rows)
    return replace(source, data=Dataset(rows, source.data.features, "HAI_1"))


def noise_process_request() -> Any:
    """``flagged_request`` whose outcome depends on CMI alone, whose process measure is independent noise and whose CMI has extreme values.

    Two hospitals have one in a training year, which is fewer than the 1 percent that the 99th percentile passes over, and six have one in the holdout year."""
    from scripts.training.models.common import Dataset

    base = flagged_request()
    rng = np.random.default_rng(17)
    marked = {str(990000 + number) for number in range(6)}
    rows = tuple(
        {
            **row,
            "process": float(rng.normal()),
            "cmi": 1.0e4
            if (row["ccn"] in marked and row["window_year"] == 2023) or (row["ccn"] in {"990000", "990001"} and row["window_year"] == 2020)
            else row["cmi"],
            "hai_1__sir__number": float(2 + 0.5 * row["cmi"] + 0.05 * rng.normal()),
        }
        for row in base.data.rows
    )
    grid = {"alpha": [1.0e9], "l1_ratio": [0.5]}
    slots = {**base.config["slots"], "m2_elastic_net": {**base.config["slots"]["m2_elastic_net"], "grid": grid}}
    return replace(base, data=Dataset(rows, base.data.features, "HAI_1"), config={**base.config, "slots": slots})


def traced(call: Callable[[], Any]) -> tuple[Any, list[dict[str, Any]], list[tuple[frozenset[str], Any]]]:
    """Run a model call and return its result, every preprocessing fit in call order (its rows and its design of those rows) and every plain-row transform."""
    from scripts.training.models.preprocessing import FeatureBatch, FoldPreprocessor

    fits: list[dict[str, Any]] = []
    transforms: list[tuple[frozenset[str], Any]] = []
    fit, transform = FoldPreprocessor.fit, FoldPreprocessor.transform

    def tracked_fit(self: Any, values: Any, y: Any = None) -> Any:
        keys = frozenset(row["spine_key"] for row in getattr(values, "rows", values))
        fits.append({"keys": keys, "design": None})
        return fit(self, values, y)

    def tracked_transform(self: Any, values: Any) -> Any:
        design = transform(self, values)
        if isinstance(values, FeatureBatch):
            fits[-1]["design"] = design
        else:
            transforms.append((frozenset(row["spine_key"] for row in values), design))
        return design

    with patch.object(FoldPreprocessor, "fit", tracked_fit), patch.object(FoldPreprocessor, "transform", tracked_transform):
        return call(), fits, transforms


def panel_standin(y: Any, x: Any, entities: Any, years: Any, names: Any) -> Any:
    """Provide independently computed within slopes at the native panel-library boundary."""
    from scripts.training.models.panel import PanelFits

    unique = sorted(set(entities))
    centered_x = x.copy()
    centered_y = np.asarray(y).copy()
    for entity in unique:
        mask = np.asarray(entities) == entity
        centered_x[mask] -= np.mean(x[mask], axis=0)
        centered_y[mask] -= np.mean(np.asarray(y)[mask])
    slopes = np.linalg.lstsq(centered_x[:, 1:], centered_y, rcond=None)[0]
    beta = np.r_[np.mean(y) - np.mean(x[:, 1:], axis=0) @ slopes, slopes]
    beta[0] -= 0.4
    effects = {entity: float(np.mean(np.asarray(y)[np.asarray(entities) == entity] - x[np.asarray(entities) == entity] @ beta)) for entity in unique}
    return PanelFits(dict(zip(names, beta, strict=True)), effects)


EMPTY_LIST: dict[str, Any] = {"slot": None, "references": [], "columns": [], "fitted_slot": None, "kept_references": [], "kept_columns": []}


def written(folder: Path, name: str, document: Any) -> Path:
    """Write one JSON document under a scratch folder and return its path."""
    path = folder / name
    path.write_text(json.dumps(document) + "\n")
    return path


def serve_with_the_real_loader(document: Any, folder: Path, tags: dict[str, str] | None = None) -> tuple[Any, list[list[str]]]:
    """Load a version through the real serving loader, which reads data only: the feature list, the provenance, the saved prediction check and the model file.

    Only the transports are stand-ins: the MLflow registry record, the download call (the loader's ``fetch`` seam, writing each file at the local path the
    loader chooses), skops (it returns a model whose ``predict`` scores 0.5) and the pandas frame, a list of the row dicts, because the training environment has
    no pandas. The provenance holds the closure digest of the image's own record and one version for each of the four libraries, and the saved check holds one
    row of zeros that scores 0.5. Returns the loaded model and the column names of every frame the pipeline scored after the load.
    """
    mlflow_loader = importlib.import_module("scripts.serving.mlflow_loader")
    scored: list[list[str]] = []

    def predict(frame: Any) -> list[float]:
        scored.append(list(frame[0]))
        return [0.5]

    closure = mlflow_loader.MlflowLoader().closure
    references = document["references"]
    files = {
        "bundle/feature_list.json": document,
        "bundle/provenance.json": {"model_code_closure_sha256": closure, "resolved_versions": {name: "1" for name in mlflow_loader.LIBRARIES}},
        "bundle/self_check.json": {"schema": 1, "slot": "m1_ols", "references": references, "rows": [[0.0] * len(references)], "predictions": [0.5]},
        "model/model.skops": "stand-in",
    }

    def fetch(run_id: str, path: str, local_path: str, limit: int) -> str:
        Path(local_path).write_text(json.dumps(files[path]) + "\n")
        return local_path

    version = SimpleNamespace(run_id="run1", tags={"feature_schema_sha256": "hash1", **(tags or {})}, source="runs:/run1/model")
    library = SimpleNamespace(MlflowClient=lambda: SimpleNamespace(get_model_version=lambda name, number: version))
    skops = SimpleNamespace(load=lambda path, trusted=None: SimpleNamespace(predict=predict))
    modules = {"mlflow": library, "pandas": SimpleNamespace(DataFrame=list), "skops": skops, "skops.io": skops}
    with patch.dict(sys.modules, modules), patch.dict(os.environ), patch.object(mlflow_loader, "installed_version", lambda name: "1"):
        model = mlflow_loader.MlflowLoader(fetch=fetch).load("HAI_1", "1")
        scored.clear()
        return model, scored


def score_with_the_real_promotion_reader(
    document: Any,
    extract_columns: Sequence[str],
    folder: Path,
    *,
    raw: bytes | None = None,
    fetch: Callable[..., str] | None = None,
    loads: list[str] | None = None,
) -> dict[str, float]:
    """Run the real ``promotion_check.champion_predictions`` on a champion with this feature list. Only the store calls, the fetch and the model read are
    stand-ins.

    The fetch writes the champion's four files at the local path it is given: the provenance with the closure digest of the repository's own record, the feature
    list (``raw`` bytes instead of the document when given), a self-check of one row of zeros and a model token. ``fetch`` replaces it (a failing or a
    misbehaving transport) and each champion read is added to ``loads``.
    """
    from scripts import model_code
    from scripts.training import promotion_check

    root = Path(__file__).resolve().parents[2]
    references = document.get("references") if isinstance(document, dict) else None
    references = references if isinstance(references, list) and all(isinstance(item, str) for item in references) else []
    files: dict[str, Any] = {
        "bundle/provenance.json": {model_code.CLOSURE_KEY: model_code.closure_digest(root)},
        "bundle/feature_list.json": document,
        "bundle/self_check.json": {"schema": 1, "slot": "m1_ols", "references": references, "rows": [[0.0] * len(references)], "predictions": [0.5]},
        "model/model.skops": "champion",
    }

    def write(run_id: str, path: str, local_path: str, limit: int) -> str:
        data = raw if raw is not None and path == "bundle/feature_list.json" else (json.dumps(files[path]) + "\n").encode()
        Path(local_path).write_bytes(data)
        return local_path

    def routed(run_id: str, path: str, local_path: str, limit: int) -> str:
        """The ``fetch`` fault applies to the feature list only: the other three files are good."""
        return fetch(run_id, path, local_path, limit) if fetch is not None and path == "bundle/feature_list.json" else write(run_id, path, local_path, limit)

    store = SimpleNamespace(get_run=lambda run_id: SimpleNamespace(data=SimpleNamespace(tags={"newest_window": "2021"})))
    rows = [{"spine_key": "000001:2022"}, {"spine_key": "000002:2022"}]
    pipeline = SimpleNamespace(predict=lambda frame: [1.0, 2.0])
    trusted = json.loads((root / "data_contracts/training/model_contract.json").read_text())["serialization"]["trusted_types"]

    def load(path: str, found: Any) -> Any:
        if loads is not None:
            loads.append(path)
        return pipeline

    with patch.object(promotion_check, "load_pipeline", side_effect=load), patch.object(promotion_check, "fetch_file", create=True, side_effect=routed):
        return promotion_check.champion_predictions(
            store, SimpleNamespace(run_id="run1"), "hai_model_HAI_1", {"columns": list(extract_columns)}, rows, 2022, root, trusted
        )


def monitor_with_the_real_baseline_reader(document: Any, folder: Path) -> tuple[Any, list[str]]:
    """Run the real ``monitoring.baseline.fetch`` on a Production version with this feature list. The registry lookup, the artifact download and the DVC load
    of the training matrix are stand-ins. Returns the result and the written ``features.json``."""
    baseline = importlib.import_module("monitoring.baseline")
    dvc_baseline = importlib.import_module("scripts.training.dvc_baseline")
    artifacts = {
        "bundle/feature_list.json": document,
        "extract/manifest_HAI_1.json": {"hai_type": "HAI_1", "population": "primary", "newest_window_year": 2021},
    }
    matrix = folder / "matrix.parquet"
    matrix.write_bytes(b"matrix")

    def download(run_id: str, path: str, dst_path: str) -> str:
        return str(written(folder, path.replace("/", "_"), artifacts[path]))

    store = SimpleNamespace(get_model_version_by_alias=lambda name, alias: SimpleNamespace(run_id="run1", version="1"), download_artifacts=download)
    out = folder / "baseline_out"
    with patch.object(dvc_baseline, "load", return_value=matrix):
        result = baseline.fetch(store, "HAI_1", out, project=folder)
    return result, json.loads((out / "features.json").read_text())


def must_be_read(label: str, call: Callable[[], Any]) -> Any:
    """Run one reader call on a list that the reader must accept; a named refusal of it fails the case instead of ending the runner."""
    from monitoring.drift_check import CheckError
    from scripts.training.job_failure import JobFailure

    try:
        return call()
    except (CheckError, JobFailure) as error:
        raise AssertionError(f"{label}: the reader refused the list: {error}") from None


def damaged_list(document: dict[str, Any], key: str, kind: str) -> dict[str, Any]:
    """Return a copy of the list with one key absent, empty, of a wrong type or holding a bad element, to see how a reader treats it."""
    names = list(document[key])
    changed = {name: value for name, value in document.items() if name != key}
    if kind == "absent":
        return changed
    damage = {
        "empty": [],
        "text": names[0],
        "null": None,
        "number": 5,
        "dict": dict.fromkeys(names, 1),
        "not_text": [names[0], 2, *names[2:]],
        "empty_name": ["", *names[1:]],
        "repeat": [names[0], names[0], *names[2:]],
        "unknown": [names[0], "no_such_column"],
    }
    return changed | {key: damage[kind]}


def serving_outcome(document: Any, folder: Path, references: Sequence[str]) -> str:
    """What the real serving loader does with this list: its refusal by exception, or whether the loaded model scores a row of the real references."""
    try:
        served, _ = serve_with_the_real_loader(document, folder)
    except (KeyError, TypeError, ValueError) as error:
        return f"refused {type(error).__name__}" + (f" {error.args[0]}" if isinstance(error, KeyError) else "")
    try:
        served.predict(dict.fromkeys(references, 1.0))
    except (KeyError, TypeError, ValueError) as error:
        return f"loaded, scoring fails {type(error).__name__}"
    return "served"


def promotion_outcome(document: Any, extract_columns: Sequence[str], folder: Path, **fault: Any) -> str:
    """What the real promotion reader does with this champion list: its named refusal with its detail, or whether it scores the holdout. A refusal after the
    champion was loaded says so, because a refused list must never reach the model load. ``fault`` is a raw file or a failing fetch."""
    from scripts.training.job_failure import JobFailure

    loads: list[str] = []
    try:
        score_with_the_real_promotion_reader(document, extract_columns, folder, loads=loads, **fault)
    except JobFailure as error:
        return f"refused {error.name}: {error.detail}" + ("; the champion was loaded" if loads else "")
    except Exception as error:
        return f"raised {type(error).__name__}"
    return "scored"


def refusing(error: Exception) -> Callable[..., str]:
    """A fetch that fails with this error."""

    def fetch(run_id: str, path: str, local_path: str, limit: int) -> str:
        raise error

    return fetch


def folder_instead_of_file(run_id: str, path: str, local_path: str, limit: int) -> str:
    """A fetch that leaves a folder where the file must be."""
    Path(local_path).mkdir()
    return local_path


def unreadable_champion_list(kind: str, folder: Path) -> dict[str, Any]:
    """The ``fault`` of one unreadable champion list file: its bytes, or a download that returns no file, a folder or an error."""
    faults: dict[str, dict[str, Any]] = {
        "truncated_json": {"raw": b'{"columns": ["process",'},
        "invalid_encoding": {"raw": b'{"columns": ["\xff\xfe"]}'},
        "empty_file": {"raw": b""},
        "missing_file": {"fetch": lambda run_id, path, local_path, limit: local_path},
        "unreadable_file": {"fetch": folder_instead_of_file},
        "download_missing": {"fetch": refusing(FileNotFoundError("secret-marker"))},
        "download_oserror": {"fetch": refusing(OSError("secret-marker"))},
        "download_error": {"fetch": refusing(RuntimeError("secret-marker"))},
    }
    return faults[kind]


def drift_outcome(document: Any, folder: Path) -> str:
    """What the real drift baseline reader does with this list: its named refusal and exit code, or the columns it monitors."""
    from monitoring.drift_check import CheckError

    try:
        _, features = monitor_with_the_real_baseline_reader(document, folder)
    except CheckError as error:
        return f"refused {error.name} {error.code}"
    return "monitors " + ("kept" if features == document.get("kept_columns") else "every column" if features == document.get("columns") else str(features))


# What each real reader does with the list of the real writer when one key it reads is damaged. Drift and promotion refuse every damaged list by name. The rows
# in ``KEY_LIMITS`` are serving's: it serves a list that it should refuse, or loads one that fails at every request. They pin today's behavior so that
# hardening the loader changes a named row on purpose.
CHAMPION_REFUSAL = "refused champion_feature_list_invalid: the champion's feature list "
CHAMPION_COLUMNS = CHAMPION_REFUSAL + "holds a bad columns key: "
CHAMPION_DOCUMENT = CHAMPION_REFUSAL + "is "
KEY_OUTCOMES: dict[str, str] = {
    "serving references absent": "refused KeyError references",
    "serving references empty": "refused ArtifactRefused",
    "serving references text": "refused ArtifactRefused",
    "serving references null": "refused TypeError",
    "serving references number": "refused TypeError",
    "serving references dict": "refused ArtifactRefused",
    "serving references not_text": "refused ArtifactRefused",
    "serving references empty_name": "refused ArtifactRefused",
    "serving columns absent": "refused ArtifactRefused",
    "serving columns empty": "refused ArtifactRefused",
    "serving columns text": "refused ArtifactRefused",
    "serving columns null": "refused ArtifactRefused",
    "serving columns number": "refused ArtifactRefused",
    "serving columns dict": "refused ArtifactRefused",
    "serving columns not_text": "refused ArtifactRefused",
    "serving columns empty_name": "served",
    "promotion columns absent": CHAMPION_COLUMNS + "absent",
    "promotion columns empty": CHAMPION_COLUMNS + "an empty list",
    "promotion columns text": CHAMPION_COLUMNS + "text, not a list",
    "promotion columns null": CHAMPION_COLUMNS + "null, not a list",
    "promotion columns number": CHAMPION_COLUMNS + "a number, not a list",
    "promotion columns dict": CHAMPION_COLUMNS + "an object, not a list",
    "promotion columns not_text": CHAMPION_COLUMNS + "element 1 is a number, not text",
    "promotion columns empty_name": CHAMPION_COLUMNS + "element 0 is an empty name",
    "promotion columns repeat": CHAMPION_COLUMNS + "element 1 repeats an earlier name",
    "promotion artifact truncated_json": CHAMPION_REFUSAL + "is not JSON: JSONDecodeError",
    "promotion artifact invalid_encoding": CHAMPION_REFUSAL + "is not JSON: UnicodeDecodeError",
    "promotion artifact empty_file": CHAMPION_REFUSAL + "is not JSON: JSONDecodeError",
    "promotion artifact missing_file": CHAMPION_REFUSAL + "was not fetched as one regular file",
    "promotion artifact download_missing": CHAMPION_REFUSAL + "could not be downloaded: FileNotFoundError",
    "promotion artifact unreadable_file": CHAMPION_REFUSAL + "was not fetched as one regular file",
    "promotion artifact download_oserror": CHAMPION_REFUSAL + "could not be downloaded: OSError",
    "promotion artifact download_error": CHAMPION_REFUSAL + "could not be downloaded: RuntimeError",
    "promotion document list": CHAMPION_DOCUMENT + "a list, not an object",
    "promotion document null": CHAMPION_DOCUMENT + "null, not an object",
    "promotion document text": CHAMPION_DOCUMENT + "text, not an object",
    "drift columns absent": "refused artifact_invalid 2",
    "drift columns empty": "refused no_features 4",
    "drift columns text": "refused artifact_invalid 2",
    "drift columns null": "refused artifact_invalid 2",
    "drift columns number": "refused artifact_invalid 2",
    "drift columns dict": "refused artifact_invalid 2",
    "drift columns not_text": "refused artifact_invalid 2",
    "drift columns empty_name": "refused artifact_invalid 2",
    "drift columns absent without a kept list": "refused artifact_invalid 2",
    "drift columns empty without a kept list": "refused no_features 4",
    "drift columns text without a kept list": "refused artifact_invalid 2",
    "drift columns null without a kept list": "refused artifact_invalid 2",
    "drift columns number without a kept list": "refused artifact_invalid 2",
    "drift columns dict without a kept list": "refused artifact_invalid 2",
    "drift columns not_text without a kept list": "refused artifact_invalid 2",
    "drift columns empty_name without a kept list": "refused artifact_invalid 2",
    "drift kept_columns absent": "monitors every column",
    "drift kept_columns empty": "refused no_features 4",
    "drift kept_columns text": "refused artifact_invalid 2",
    "drift kept_columns null": "monitors every column",
    "drift kept_columns number": "refused artifact_invalid 2",
    "drift kept_columns dict": "refused artifact_invalid 2",
    "drift kept_columns not_text": "refused artifact_invalid 2",
    "drift kept_columns empty_name": "refused artifact_invalid 2",
    "drift kept_columns repeat": "refused artifact_invalid 2",
    "drift kept_columns unknown": "refused artifact_invalid 2",
}
KEY_LIMITS = frozenset({"serving columns empty_name"})


def scenarios() -> list[tuple[str, Callable[[], None]]]:
    """Return the predeclared model and analysis scenarios for the light caller runner."""
    from scripts.training import hac_validation, mediation, research_tests
    from scripts.training.models import panel, spatial, suite
    from scripts.training.models.common import MODEL_SLOT, REGISTRABLE, SLOTS, Dataset, Deadline, Feature, seed_for
    from scripts.training.models.linear import OLSRegressor
    from scripts.training.models.preprocessing import FeatureBatch, FoldPreprocessor
    from scripts.training.models.trees import shap_values

    req = request()
    prepared = FoldPreprocessor(req.data.features, forced=(req.cmi_reference,))

    def preprocessing() -> None:
        rows = [{"a": 1.0, "b": None}, {"a": None, "b": None}, {"a": 3.0, "b": 2.0}, {"a": None, "b": None}]
        features = (Feature("v:a", "number"), Feature("v:b", "number"))
        transform = FoldPreprocessor(features, native_missing=True).set_params(native_missing=False)
        fitting = transform.fit_transform(rows)
        require(transform.get_feature_names_out().tolist() == list(fitting.names), "preprocessing feature names differ from the fitted design")
        require(transform.kept_references_ == ("v:a",), "exactly half missing must be retained")
        output = transform.transform([{"a": None, "b": 500.0}])
        require(np.isfinite(output.matrix).all(), "linear missing values must be imputed")
        require(output.names == ("a", "a__missing"), "missing indicator is required")
        before = transform.medians_.copy()
        transform.transform([{"a": 50000.0, "b": 0.0}])
        require(transform.medians_ == before, "holdout changed the training median")
        rejects(lambda: FoldPreprocessor(features, forced=("v:b",)).fit(rows), "forced covariate")

    def unknown_category() -> None:
        transform = FoldPreprocessor((Feature("v:kind", "category"),)).fit([{"kind": "a"}, {"kind": "b"}])
        result = transform.transform([{"kind": "unseen"}])
        require(result.unknown_categories == 1 and np.isfinite(result.matrix).all(), "unseen category must be counted and encoded")

    def input_contract() -> None:
        rejects(lambda: replace(req.data, rows=(*req.data.rows, req.data.rows[0])), "duplicate")
        bad = dict(req.data.rows[0], hai_1__sir__number=float("nan"))
        rejects(lambda: replace(req.data, rows=(bad, *req.data.rows[1:])), "finite label")
        bad_feature = Feature("v:psi_90", "number", "C281")
        rejects(lambda: Dataset(tuple(dict(row, psi_90=0.2) for row in req.data.rows), (*req.data.features, bad_feature), "HAI_1"), "excluded")
        extra = tuple(dict(row, hac_flag=1) for row in req.data.rows)
        rejects(lambda: replace(req.data, rows=extra), "unexpected extract column")
        weighted = tuple(dict(row, hai_1__predicted__number=3.0) for row in req.data.rows)
        require(replace(req.data, rows=weighted).label == "hai_1__sir__number", "the declared weight column of the type must not change the dataset")
        other_weight = tuple(dict(row, hai_2__predicted__number=3.0) for row in req.data.rows)
        rejects(lambda: replace(req.data, rows=other_weight), "unexpected extract column")
        weight_feature = Feature("feature_matrix_hospital:hai_1__predicted__number", "number")
        rejects(lambda: Dataset(weighted, (*req.data.features, weight_feature), "HAI_1"), "weight")
        rejects(lambda: Dataset(weighted, (*req.data.features, Feature("label_hai_1:hai_1__predicted__number", "number")), "HAI_1"), "excluded")
        other_weight_feature = Feature("feature_matrix_hospital:hai_2__predicted__number", "number")
        both_weights = tuple(dict(row, hai_2__predicted__number=3.0) for row in weighted)
        rejects(lambda: Dataset(both_weights, (*req.data.features, other_weight_feature), "HAI_1"), "weight")

    def folds() -> None:
        training, _ = req.data.split()
        with libraries():
            folds = suite.folds_for(training, req.config, seed_for(42, "HAI_1", "folds"))
            repeated = suite.folds_for(training, req.config, seed_for(42, "HAI_1", "folds"))
        require(len(folds) == 3, "wrong hospital fold count")
        for left, right in folds:
            require(not ({training.rows[i]["ccn"] for i in left} & {training.rows[i]["ccn"] for i in right}), "hospital crosses a fold")
        require([(a.tolist(), b.tolist()) for a, b in folds] == [(a.tolist(), b.tolist()) for a, b in repeated], "folds changed for the same seed")
        for left, right in suite.time_folds(training):
            require(max(training.rows[i]["window_year"] for i in left) < min(training.rows[i]["window_year"] for i in right), "panel folds leak time")

    def years_of(keys: Any) -> set[int]:
        return {int(key.split(":")[1]) for key in keys}

    def hospitals_of(keys: Any) -> set[str]:
        return {key.split(":")[0] for key in keys}

    def elastic_net_tunes_on_forward_in_time_folds() -> None:
        wide = flagged_request()
        with libraries():
            result, fits, transforms = traced(lambda: suite.fit_slot(wide, "m2_elastic_net"))
            _, again, _ = traced(lambda: suite.fit_slot(wide, "m2_elastic_net"))
        require(result.status == "fitted", f"the Elastic Net did not fit: {result.status} {result.reason}")
        folds = fits[:-1]
        require(len(folds) == 4, f"training years 2018 to 2022 should give four forward folds, got {len(folds)}")
        for position, (fold, (validation, _)) in enumerate(zip(folds, transforms, strict=False)):
            fitting_years, validation_years = years_of(fold["keys"]), years_of(validation)
            require(validation_years == {2019 + position}, f"fold {position} validates {sorted(validation_years)}, not the next year")
            require(fitting_years == set(range(2018, 2019 + position)), f"fold {position} fits on {sorted(fitting_years)}, not every earlier year")
            require(max(fitting_years) < min(validation_years), f"fold {position} validates a year at or before a fitting year")
            require(hospitals_of(fold["keys"]) & hospitals_of(validation), f"fold {position} is grouped by hospital, not ordered in time")
        require(years_of(fits[-1]["keys"]) == set(range(2018, 2023)), "the final fit did not use every training year")
        require([sorted(fit["keys"]) for fit in again] == [sorted(fit["keys"]) for fit in fits], "the folds changed on a second run with the same seed")
        with libraries():
            _, boosted, boosted_transforms = traced(lambda: suite.fit_slot(wide, "m4_boosting"))
        grouped = boosted[:3]
        require(len(grouped) == 3, "gradient boosting no longer tunes on three hospital folds")
        for fold, (validation, _) in zip(grouped, boosted_transforms, strict=False):
            require(not hospitals_of(fold["keys"]) & hospitals_of(validation), "a gradient boosting fold shares a hospital between fitting and validation")
        training, _ = wide.data.split()
        require(
            set().union(*(validation for validation, _ in boosted_transforms[:3])) == set(training.keys),
            "gradient boosting folds no longer cover the training rows",
        )

    def elastic_net_without_a_validation_year_is_not_estimable() -> None:
        with libraries():
            one = suite.fit_slot(flagged_request(years=(2022, 2023)), "m2_elastic_net")
            two = suite.fit_slot(flagged_request(years=(2021, 2022, 2023)), "m2_elastic_net")
        require(one.status == "not_estimable" and "time-ordered validation fold" in one.reason, f"one training year was not refused: {one.status} {one.reason}")
        require(two.status == "fitted", f"two training years should give one fold: {two.status} {two.reason}")
        require([len(record["folds"]) for record in two.details["tuning"]] == [1] * 4, "two training years should tune on exactly one fold")

    def elastic_net_clips_at_fold_fitted_percentiles() -> None:
        plain = flagged_request()
        extreme = planted(plain, (2022,))
        with libraries():
            result, fits, transforms = traced(lambda: suite.fit_slot(extreme, "m2_elastic_net"))
        require(result.status == "fitted", f"the Elastic Net did not fit: {result.status} {result.reason}")
        fitting = [row for row in plain.data.rows if row["window_year"] < 2022]
        validation = [row for row in extreme.data.rows if row["window_year"] == 2022]
        last = fits[len(fits) - 2]["design"]
        for name in ("process", "cmi", "extra"):
            observed = np.asarray([row[name] for row in fitting if row[name] is not None])
            low, high = np.percentile(observed, (1, 99))
            clipped = np.clip(observed, low, high)
            absent = sum(row[name] is None for row in fitting)
            filled = np.r_[clipped, np.full(absent, np.median(clipped))]
            center, scale = float(filled.mean()), float(filled.std())
            require((absent > 0) == (name == "extra"), f"{name}: the fixture has the wrong missing values")
            fitted = last.matrix[:, last.names.index(name)]
            require(
                np.isclose(fitted.max(), (high - center) / scale) and np.isclose(fitted.min(), (low - center) / scale),
                f"{name} is not clipped on the fitting rows",
            )
            if name == "extra":
                continue
            scored = transforms[len(fits) - 2][1]
            values = scored.matrix[:, scored.names.index(name)]
            extremes = [index for index, row in enumerate(validation) if row[name] == 1.0e6]
            require(
                len(extremes) == 6 and np.allclose(values[extremes], (high - center) / scale),
                f"{name} is not clipped at the fold-fitted 99th percentile on validation rows",
            )

    def elastic_net_final_fit_clips_holdout_rows() -> None:
        plain = flagged_request()
        extreme = planted(plain, (2023,))
        with libraries():
            result, fits, _ = traced(lambda: suite.fit_slot(extreme, "m2_elastic_net"))
        holdout = [row for row in extreme.data.rows if row["window_year"] == 2023]
        require(all(not years_of(fit["keys"]) & {2023} for fit in fits), "the holdout year entered a preprocessing fit")
        training = [row for row in plain.data.rows if row["window_year"] < 2023]
        scored = result.pipeline.named_steps["prepare"].transform(holdout)
        for name in ("process", "cmi"):
            column = np.asarray([row[name] for row in training])
            low, high = np.percentile(column, (1, 99))
            clipped = np.clip(column, low, high)
            expected = (high - float(clipped.mean())) / float(clipped.std())
            values = scored.matrix[:, scored.names.index(name)]
            extremes = [index for index, row in enumerate(holdout) if row[name] == 1.0e6]
            require(len(extremes) == 6 and np.allclose(values[extremes], expected), f"{name} is not clipped at the training 99th percentile on holdout rows")

    def elastic_net_winsorizing_leaves_flags_and_categories() -> None:
        plain = flagged_request()
        with libraries():
            result, fits, _ = traced(lambda: suite.fit_slot(plain, "m2_elastic_net"))
        design = fits[-1]["design"]
        training = [row for row in plain.data.rows if row["window_year"] < 2023]
        flag = np.asarray([row["flag"] for row in training])
        require("flag" in design.names, "the rare flag was clipped to a constant and dropped")
        require(np.allclose(design.matrix[:, design.names.index("flag")], (flag - flag.mean()) / flag.std()), "the rare 0/1 flag was changed")
        kinds = [name for name in design.names if name.startswith("kind__")]
        require(kinds and all(set(np.unique(design.matrix[:, design.names.index(name)])) <= {0.0, 1.0} for name in kinds), "a category column was changed")
        absent = np.asarray([row["extra"] is None for row in training], dtype=np.float64)
        require(np.array_equal(design.matrix[:, design.names.index("extra__missing")], absent), "the missing-value flag was changed")
        require(result.status == "fitted", "the Elastic Net did not fit")

    def other_models_are_not_winsorized() -> None:
        extreme = planted(flagged_request(), (2022,))
        training, _ = extreme.data.split()
        old: dict[str, dict[str, Any]] = {
            "m1_ols": {"drop_dependent": True},
            "m3_mixed": {"forced": (extreme.cmi_reference,), "drop_dependent": True},
            "m4_boosting": {"forced": (extreme.cmi_reference,), "native_missing": True},
            "m5_spatial": {"forced": (extreme.cmi_reference,), "drop_dependent": True},
            "m6_fixed_effects": {"forced": (extreme.cmi_reference,), "drop_dependent": True},
        }
        for slot, arguments in old.items():
            features = extreme.features_for(slot)
            if slot == "m3_mixed":
                features = tuple(feature for feature in features if feature.name != "control_type_code")
            with libraries():
                before = FoldPreprocessor(features, **arguments).fit_transform(training.rows)
                after = suite._pipeline(extreme, slot, {}, False, features).named_steps["prepare"].fit_transform(training.rows)
            require(before.names == after.names and before.matrix.tobytes() == after.matrix.tobytes(), f"{slot}: the design matrix changed")
            raw = np.asarray([row["process"] for row in training.rows])
            unclipped = raw.max() if slot == "m4_boosting" else (raw.max() - raw.mean()) / raw.std()
            require(np.isclose(after.matrix[:, after.names.index("process")].max(), unclipped), f"{slot}: the planted extreme was clipped")

    def rq1_comparison_fits_process_measures_and_cmi_only() -> None:
        from scripts.training.models.elastic import ElasticRegressor

        wide = flagged_request()
        process, cmi = wide.process_references[0], wide.cmi_reference
        seen: list[frozenset[str]] = []
        original = ElasticRegressor.fit

        def tracked(self: Any, design: Any, y: Any) -> Any:
            seen.append(frozenset(design.references))
            return original(self, design, y)

        with libraries(), patch.object(ElasticRegressor, "fit", tracked):
            output = suite.fit_model(wide, "m2")
        with libraries():
            reference = suite.fit_slot(replace(wide, feature_lists={**wide.feature_lists, "m2_elastic_net": (process, cmi)}), "m2_elastic_net")
        record = output.research["process_vs_intercept_cmi"]

        def rmse_of(result: Any) -> float:
            value = result.metrics["rmse"]
            return math.nan if value is None else value

        require(frozenset((process, cmi)) in seen, "no Elastic Net was fitted on the process measures and CMI alone")
        require(np.isclose(record["elastic_net_rmse"], rmse_of(reference)), "the comparison is not the process-and-CMI Elastic Net")
        require(record["status"] == "fitted" and list(record["elastic_net_references"]) == [process, cmi], f"the comparison names other columns: {record}")
        require(not np.isclose(record["elastic_net_rmse"], rmse_of(output.slots["m2_elastic_net"])), "the comparison is still the full Elastic Net")
        require(output.slots["m2_elastic_net"].status == "fitted", "the full Elastic Net of the run changed")

    def elastic_net_fits_each_fold_on_its_fitting_labels() -> None:
        from scripts.training.models.elastic import ElasticRegressor

        wide = flagged_request()
        labels: list[Any] = []
        original = ElasticRegressor.fit

        def tracked(self: Any, design: Any, y: Any) -> Any:
            labels.append(np.array(y))
            return original(self, design, y)

        with libraries(), patch.object(ElasticRegressor, "fit", tracked):
            _, fits, _ = traced(lambda: suite.fit_slot(wide, "m2_elastic_net"))
        training, _ = wide.data.split()
        outcome = {row["spine_key"]: row[training.label] for row in training.rows}
        expected = [np.asarray([outcome[row["spine_key"]] for row in training.rows if row["spine_key"] in fit["keys"]]) for fit in fits]
        folds, options = len(fits) - 1, 4
        require(len(labels) == options * folds + 1, f"expected {options * folds + 1} estimator fits, got {len(labels)}")
        for call, seen in enumerate(labels):
            wanted = expected[-1] if call == len(labels) - 1 else expected[call % folds]
            require(seen.shape == wanted.shape and np.array_equal(seen, wanted), f"estimator fit {call} did not receive the labels of its fitting rows")

    def elastic_net_selects_the_lowest_validation_error() -> None:
        from scripts.training.models.common import seed_for
        from scripts.training.models.elastic import ElasticRegressor

        wide = flagged_request()
        with libraries():
            result, fits, transforms = traced(lambda: suite.fit_slot(wide, "m2_elastic_net"))
            training, _ = wide.data.split()
            outcome = {row["spine_key"]: row[training.label] for row in training.rows}
            seed = seed_for(wide.config["master_seed"], "HAI_1", "m2_elastic_net")
            options, scores = [], []
            for alpha in wide.config["slots"]["m2_elastic_net"]["grid"]["alpha"]:
                for ratio in wide.config["slots"]["m2_elastic_net"]["grid"]["l1_ratio"]:
                    squared, count = 0.0, 0
                    for fit, (validation, design) in zip(fits[:-1], transforms, strict=False):
                        fitting_y = np.asarray([outcome[row["spine_key"]] for row in training.rows if row["spine_key"] in fit["keys"]])
                        validation_y = np.asarray([outcome[row["spine_key"]] for row in training.rows if row["spine_key"] in validation])
                        model = ElasticRegressor(alpha, ratio, (wide.cmi_reference,), seed).fit(fit["design"], fitting_y)
                        squared += float(np.sum((validation_y - np.asarray(model.predict(design))) ** 2))
                        count += len(validation_y)
                    options.append({"alpha": alpha, "l1_ratio": ratio})
                    scores.append(math.sqrt(squared / count))
        require(max(scores) > min(scores) * 1.001, f"the grid options do not differ in validation error: {scores}")
        require(result.details["selected_parameters"] == options[int(np.argmin(scores))], f"selected {result.details['selected_parameters']}, scores {scores}")

    def rq1_comparison_keeps_the_restricted_fits_status() -> None:
        from scripts.training.models.common import DeadlineExceeded, NotConverged, NotEstimable
        from scripts.training.models.elastic import ElasticRegressor

        wide = flagged_request()
        restricted = frozenset((wide.process_references[0], wide.cmi_reference))
        original = ElasticRegressor.fit
        for error, status in (
            (NotConverged("slow path"), "not_converged"),
            (DeadlineExceeded("late"), "not_run"),
            (NotEstimable("singular design"), "not_estimable"),
        ):

            def failing(self: Any, design: Any, y: Any, error: Exception = error) -> Any:
                if frozenset(design.references) == restricted:
                    raise error
                return original(self, design, y)

            with libraries(), patch.object(ElasticRegressor, "fit", failing):
                output = suite.fit_model(wide, "m2")
            record = output.research["process_vs_intercept_cmi"]
            require(record["status"] == status and record.get("reason"), f"a {status} restricted fit was reported as {record}")
            require(status == "not_run" or str(error) in record["reason"], f"the reason of the {status} restricted fit was lost: {record}")
            require(output.slots["m2_elastic_net"].status == "fitted", "the full Elastic Net failed with the restricted one")

    def rq1_comparison_runs_after_a_failed_full_fit() -> None:
        wide = flagged_request()
        full = tuple(reference for reference in wide.feature_lists["m2_elastic_net"] if reference != wide.cmi_reference)
        broken = replace(wide, feature_lists={**wide.feature_lists, "m2_elastic_net": full})
        with libraries(), patch.object(panel, "native_fits", side_effect=panel_standin):
            single = suite.fit_model(broken, "m2")
            every = suite.fit_models(replace(broken, deadline=Deadline.after(60)))
        for name, output in (("the m2 run", single), ("the six-model run", every)):
            require(output.slots["m2_elastic_net"].status == "not_estimable", f"{name}: the full Elastic Net should fail without forced CMI")
            record = output.research["process_vs_intercept_cmi"]
            require(record["status"] == "fitted" and "elastic_net_rmse" in record, f"{name}: RQ1 was skipped after the full fit failed: {record}")

    def rq1_baseline_clips_cmi_at_the_same_bounds() -> None:
        quiet = noise_process_request()
        with libraries():
            record, fits, transforms = traced(lambda: suite.process_cmi_comparison(quiet))
        require(record["status"] == "fitted", f"the comparison did not fit: {record}")
        training, holdout = quiet.data.split()
        observed = np.asarray([row["cmi"] for row in training.rows])
        low, high = np.percentile(observed, (1, 99))
        clipped = np.clip(observed, low, high)
        center, scale = float(clipped.mean()), float(clipped.std())
        require(observed.max() == 1.0e4 and high < 100, "the fixture has no extreme CMI to clip")
        elastic_train, baseline_train = fits[-2]["design"], fits[-1]["design"]
        elastic_holdout, baseline_holdout = transforms[-2][1], transforms[-1][1]
        require(set(fits[-2]["keys"]) == set(training.keys) == set(fits[-1]["keys"]), "the two sides did not fit CMI on the same training rows")
        expected_train = (clipped - center) / scale
        expected_holdout = (np.clip([row["cmi"] for row in holdout.rows], low, high) - center) / scale
        for name, design, expected in (
            ("elastic net training", elastic_train, expected_train),
            ("baseline training", baseline_train, expected_train),
            ("elastic net holdout", elastic_holdout, expected_holdout),
            ("baseline holdout", baseline_holdout, expected_holdout),
        ):
            require(
                np.allclose(design.matrix[:, design.names.index("cmi")], expected), f"the {name} CMI column is not the training-bounds clip of the CMI values"
            )
        require(
            np.array_equal(elastic_train.matrix[:, elastic_train.names.index("cmi")], baseline_train.matrix[:, baseline_train.names.index("cmi")]),
            "the training CMI columns differ",
        )
        require(abs(record["rmse_improvement"]) < 1e-6, f"with no process signal and equal CMI the two sides should score alike: {record['rmse_improvement']}")
        clipping = record["cmi_clipping"]
        require(
            clipping["both_sides"] is True
            and clipping["learned_on"] == "training_rows"
            and np.isclose(clipping["lower"], low)
            and np.isclose(clipping["upper"], high),
            f"the record does not state the bounds: {clipping}",
        )

    def other_intercept_cmi_models_keep_raw_cmi() -> None:
        quiet = noise_process_request()
        training, holdout = quiet.data.split()
        raw = np.asarray([row["cmi"] for row in training.rows])
        slope, intercept = np.polyfit(raw, training.y, 1)
        expected = intercept + slope * np.asarray([row["cmi"] for row in holdout.rows])
        with libraries():
            predicted = research_tests.intercept_cmi_predictions(quiet)
        require(np.allclose(predicted, expected, atol=1e-6), "the shared intercept-plus-CMI helper no longer fits the raw CMI")

    def rq1_result_always_states_the_cmi_clipping() -> None:
        from scripts.training.models.common import DeadlineExceeded, NotConverged, NotEstimable
        from scripts.training.models.elastic import ElasticRegressor

        wide = flagged_request()
        restricted = frozenset((wide.process_references[0], wide.cmi_reference))
        original = ElasticRegressor.fit

        def stated(record: Any, status: str, label: str) -> None:
            clipping = record.get("cmi_clipping")
            require(record["status"] == status and record.get("reason"), f"{label}: the refusal lost its status or reason: {record}")
            require(isinstance(clipping, dict), f"{label}: the {status} RQ1 result has no cmi_clipping record: {record}")
            require(clipping["both_sides"] is False and clipping["learned_on"] is None, f"{label}: a refusal claims verified bounds: {clipping}")
            require(clipping["lower"] is None and clipping["upper"] is None, f"{label}: a refusal states bounds: {clipping}")
            require(isinstance(clipping.get("reason"), str) and clipping["reason"], f"{label}: the record does not say why there are no bounds: {clipping}")

        for error, status in (
            (NotConverged("slow path"), "not_converged"),
            (DeadlineExceeded("late"), "not_run"),
            (NotEstimable("singular design"), "not_estimable"),
        ):

            def failing(self: Any, design: Any, y: Any, error: Exception = error) -> Any:
                if frozenset(design.references) == restricted:
                    raise error
                return original(self, design, y)

            with libraries(), patch.object(ElasticRegressor, "fit", failing):
                stated(suite.fit_model(wide, "m2").research["process_vs_intercept_cmi"], status, f"restricted fit {status}")
        with libraries():
            stated(suite.fit_model(replace(wide, process_references=()), "m2").research["process_vs_intercept_cmi"], "not_estimable", "no process measure")
            flat = Dataset(tuple(dict(row, cmi=1.0) for row in wide.data.rows), wide.data.features, "HAI_1")
            stated(suite.fit_model(replace(wide, data=flat), "m2").research["process_vs_intercept_cmi"], "not_estimable", "constant CMI")
        real = research_tests.intercept_cmi_fit

        def shifted(request: Any, winsorize: bool = False) -> Any:
            predictions, transform = real(request, winsorize)
            transform.bounds_ = {name: (low + 1.0, high) for name, (low, high) in transform.bounds_.items()}
            return predictions, transform

        with libraries(), patch.object(panel, "native_fits", side_effect=panel_standin), patch.object(research_tests, "intercept_cmi_fit", shifted):
            stated(suite.fit_model(wide, "m2").research["process_vs_intercept_cmi"], "not_estimable", "different bounds")
            every = suite.fit_models(replace(wide, deadline=Deadline.after(60)))
        stated(every.research["process_vs_intercept_cmi"], "not_estimable", "different bounds in the six-model run")
        with libraries():
            fitted = suite.fit_model(wide, "m2").research["process_vs_intercept_cmi"]
        require(fitted["status"] == "fitted" and fitted["cmi_clipping"]["both_sides"] is True, f"the fitted record lost its clipping record: {fitted}")

    def rq1_comparison_columns_in_the_real_lists() -> None:
        from scripts.training import job_inputs

        root = Path(__file__).resolve().parents[2]
        lists, known = job_inputs.feature_lists(root, "m2"), job_inputs.catalog(root)
        names = sorted({reference.split(":", 1)[1] for items in lists.values() for reference in items})
        values = {reference.split(":", 1)[1]: "a" if feature.kind == "category" else 1.0 for reference, feature in known.items()}
        rows = [
            {"spine_key": f"{990000 + hospital}:{year}", "ccn": str(990000 + hospital), "window_year": year, "hai_1__sir__number": 1.0}
            | {name: values[name] for name in names}
            for hospital in range(4)
            for year in (2022, 2023)
        ]
        built = job_inputs.model_request("HAI_1", rows, (), job_inputs.config_of(root), 60.0, root, "m2")
        references = suite.process_cmi_references(built)
        earlier = set(json.loads((root / "config/training/earlier_outcomes.json").read_text())["controls"])
        process = [
            reference
            for reference in json.loads((root / "config/training/feature_lists/m1_ols.json").read_text())["references"]
            if known[reference].family == "care_compare"
        ]
        require(references == (*process, built.cmi_reference), "the comparison columns are not the Care Compare process measures plus CMI")
        require(
            not any(known[reference].control.split(".", 1)[0] in earlier or reference.startswith("feature_matrix_history") for reference in references),
            "an earlier outcome is in the comparison",
        )

    def complete_suite() -> None:
        with libraries(), patch.object(panel, "native_fits", side_effect=panel_standin):
            output = suite.fit_models(req)
            again = suite.fit_models(replace(req, deadline=Deadline.after(60)))
        require(set(output.slots) == set(SLOTS), "model result must contain six slots")
        for slot, result in output.slots.items():
            require(result.status in ("fitted", "not_converged", "not_estimable", "not_run"), "unrecognized model status")
            if result.status == "fitted":
                require(len(result.predictions) == 48, f"{slot} changed the holdout row set")
                require(set(result.metrics) >= {"rmse", "mae", "r2"}, "holdout metrics missing")
            else:
                require(bool(result.reason), "failed or omitted model lacks a reason")
        require(output.selected in SLOTS and output.candidate in ("m1_ols", "m2_elastic_net", "m4_boosting"), "selection or registration contract failed")
        require(output.to_result() == again.to_result(), "same synthetic input changed the reproducibility payload")
        require(output.shap["status"] == "fitted", "tree attribution did not run")
        require(
            bool(output.to_result()["p_values"]) and all(isinstance(value, float) and 0 <= value <= 1 for value in output.to_result()["p_values"].values()),
            "p-value bundle field contains nonnumeric results",
        )
        require(
            output.research["joint_process"]["status"] == "fitted" and output.research["joint_process"]["f_statistic"] > 5,
            "joint process test missed the planted effect",
        )
        require(output.research["ownership"]["icc"] > 0 and output.research["ownership"]["reml"] is False, "ownership inference contract failed")
        tree = output.slots["m4_boosting"]
        require(
            tree.pipeline.named_steps["model"].native_.parameters["random_state"] == seed_for(42, "HAI_1", "m4_boosting"),
            "tree passed another seed to its native estimator",
        )
        require(tree.pipeline.named_steps["model"].native_.parameters["n_jobs"] == 1, "tree ignored its thread allocation")
        require(tree.details["forest_check"]["registrable"] is False, "forest check became registrable")
        require(output.slots["m2_elastic_net"].details["grid_edge"] is True, "Elastic Net grid edge was hidden")
        with libraries() as replacements, patch.object(panel, "native_fits", side_effect=panel_standin):
            original = replacements["esda"].Moran

            def significant(*args: Any, **kwargs: Any) -> Any:
                statistic = original(*args, **kwargs)
                statistic.p_sim = 0.001
                return statistic

            replacements["esda"].Moran = significant
            for number in range(1, 7):
                label = f"hai_{number}__sir__number"
                rows = tuple({(label if name == req.data.label else name): value for name, value in row.items()} for row in req.data.rows)
                expanded = suite.fit_models(replace(req, data=Dataset(rows, req.data.features, f"HAI_{number}"), deadline=Deadline.after(60)))
                require(
                    all(result.status == "fitted" for result in expanded.slots.values()), "a six-type spatial-gated caller did not exercise all model bodies"
                )
                spatial_result = expanded.slots["m5_spatial"]
                _, holdout = expanded.request.data.split()
                design = spatial_result.pipeline.named_steps["prepare"].transform(holdout.rows)
                estimator = spatial_result.pipeline.named_steps["model"]
                weights = spatial.build_weights(holdout.rows, ()).native().full()[0]
                fixed = np.column_stack((np.ones(len(design.matrix)), design.matrix)) @ estimator.coefficients_
                expected = np.linalg.solve(np.eye(len(fixed)) - estimator.spatial_parameter_ * weights, fixed)
                require(np.allclose(spatial_result.predictions, expected), "spatial prediction used another graph or outcome values")

            class ErrorDiagnostics(SpatialOLS):
                def __init__(self, *args: Any, **kwargs: Any) -> None:
                    super().__init__(*args, **kwargs)
                    self.rlm_lag, self.rlm_error = (0.2, 0.7), (10.0, 0.002)

            replacements["spreg"].OLS = ErrorDiagnostics
            error_suite = suite.fit_models(replace(req, deadline=Deadline.after(60)))
            spatial_result = error_suite.slots["m5_spatial"]
            require(spatial_result.status == "fitted" and spatial_result.details["kind"] == "error", "LM diagnostics did not choose the error model")
            _, holdout = req.data.split()
            design = spatial_result.pipeline.named_steps["prepare"].transform(holdout.rows)
            expected = np.column_stack((np.ones(len(design.matrix)), design.matrix)) @ spatial_result.pipeline.named_steps["model"].coefficients_
            require(np.allclose(spatial_result.predictions, expected), "spatial error prediction used holdout residuals")

            class BothDiagnostics(SpatialOLS):
                def __init__(self, *args: Any, **kwargs: Any) -> None:
                    super().__init__(*args, **kwargs)
                    self.rlm_lag, self.rlm_error = (10.0, 0.01), (9.0, 0.02)

            replacements["spreg"].OLS = BothDiagnostics
            ambiguous = suite.fit_models(replace(req, deadline=Deadline.after(60))).slots["m5_spatial"]
            require(ambiguous.status == "fitted" and ambiguous.details["kind"] == "lag", "both significant LM tests did not choose the larger statistic")
        group = Feature("feature_matrix_hospital_window:provider_subtype_code", "category", "", "hospital_window")
        lists = {**req.feature_lists, "m3_mixed": (*req.feature_lists["m3_mixed"], group.reference)}
        with libraries():
            overlap = suite.fit_slot(
                replace(
                    req,
                    data=replace(
                        req.data,
                        rows=tuple(dict(row, provider_subtype_code=row["control_type_code"]) for row in req.data.rows),
                        features=(*req.data.features, group),
                    ),
                    feature_lists=lists,
                ),
                "m3_mixed",
            )
        require(overlap.status == "not_converged" and "fixed" in overlap.reason, "ownership fixed effects hid an unidentified random intercept")
        with libraries() as replacements:
            elastic = replacements["sklearn.linear_model"].ElasticNet
            original_elastic = elastic.fit

            def no_elastic_convergence(self: Any, *args: Any, **kwargs: Any) -> Any:
                fitted = original_elastic(self, *args, **kwargs)
                fitted.n_iter_ = 10000
                return fitted

            with patch.object(elastic, "fit", no_elastic_convergence):
                failed_elastic = suite.fit_slot(req, "m2_elastic_net")
            require(failed_elastic.status == "not_converged", "tuning hid Elastic Net nonconvergence")
            mixed = replacements["statsmodels.api"].MixedLM
            original_mixed = mixed.fit

            def no_mixed_convergence(self: Any, **kwargs: Any) -> Any:
                fitted = original_mixed(self, **kwargs)
                fitted.converged = False
                return fitted

            with patch.object(mixed, "fit", no_mixed_convergence):
                failed_mixed = suite.fit_slot(req, "m3_mixed")
            require(failed_mixed.status == "not_converged", "mixed-model nonconvergence was ignored")
            constant = replace(
                req.data,
                rows=tuple(
                    dict(row, process=float(int(row["ccn"]) % 7), cmi=float(int(row["ccn"]) % 9), extra=float(int(row["ccn"]) % 11)) for row in req.data.rows
                ),
            )
            failed_panel = suite.fit_slot(replace(req, data=constant), "m6_fixed_effects")
            require(failed_panel.status == "not_estimable" and "absorbed" in failed_panel.reason, "absorbed panel columns were hidden")

    def collinear_ols() -> None:
        data = replace(req.data, rows=tuple(dict(row, extra=row["process"]) for row in req.data.rows))
        changed = replace(req, data=data, feature_lists={**req.feature_lists, "m1_ols": tuple(feature.reference for feature in data.features)})
        with libraries():
            result = suite.fit_slot(changed, "m1_ols")
        require(
            result.status == "fitted" and [entry["name"] for entry in result.details["dependent_columns"]] == ["extra"],
            "collinear OLS did not drop and record the duplicate column",
        )
        training, _ = changed.data.split()
        design = FoldPreprocessor(changed.features_for("m1_ols")).fit_transform(training.rows)
        with libraries():
            rejects(lambda: OLSRegressor().fit(design, training.y), "rank")

    def dependent_request() -> Any:
        """The base panel plus an exact duplicate, an exact sum and a category level that mirrors a missing-value flag."""
        rows = []
        for row in req.data.rows:
            gap = int(row["ccn"]) % 5 == 0 and row["window_year"] % 2 == 0
            rows.append(
                dict(
                    row,
                    copy=row["process"],
                    total=row["process"] + row["extra"],
                    sparse=None if gap else row["extra"] * 0.5 + row["cmi"],
                    kind="gap" if gap else "full",
                )
            )
        added = (
            Feature("feature_matrix_county_context:copy", "number", "C177.01", "county_context"),
            Feature("feature_matrix_county_context:total", "number", "C177", "county_context"),
            Feature("feature_matrix_county_context:sparse", "number", "C200", "county_context"),
            Feature("feature_matrix_county_context:kind", "category", "C201", "county_context"),
        )
        features = (*req.data.features, *added)
        references = tuple(feature.reference for feature in features)
        lists = {**req.feature_lists, **{slot: references for slot in SLOTS if slot not in ("m1_ols", "rq4_reduced")}}
        return replace(req, data=Dataset(tuple(rows), features, "HAI_1"), feature_lists=lists)

    def dependent_columns_dropped_and_recorded() -> None:
        changed = dependent_request()
        for slot in ("m3_mixed", "m5_spatial", "m6_fixed_effects"):
            panel_slot = slot == "m6_fixed_effects"
            expected = {"copy", "total", "sparse"} if panel_slot else {"copy", "total", "kind__category_1"}
            with libraries(), patch.object(panel, "native_fits", side_effect=panel_standin):
                result = suite.fit_slot(changed, slot)
            require(result.status == "fitted", f"{slot} with dependent columns is {result.status}: {result.reason}")
            dropped = {entry["name"] for entry in result.details["dependent_columns"]}
            require(dropped == expected, f"{slot} dropped {sorted(dropped)}, not the duplicate, the sum and the mirrored category level")
            require(result.details["dependent_column_count"] == len(expected), f"{slot} dependent column count is wrong")
            kinds = {entry["name"]: entry["kind"] for entry in result.details["dependent_columns"]}
            require(
                kinds
                == (
                    {"copy": "value", "total": "value", "sparse": "value"}
                    if panel_slot
                    else {"copy": "value", "total": "value", "kind__category_1": "category_level"}
                ),
                f"{slot} dependent column kinds are wrong",
            )
            names = result.pipeline.named_steps["prepare"].transform(changed.data.split()[1].rows).names
            kept = {"process", "extra", "cmi"} if panel_slot else {"process", "extra", "cmi", "sparse__missing"}
            require(not expected & set(names) and kept <= set(names), f"{slot} holdout design kept a dropped column")
            require(not panel_slot or not any(name.endswith("__missing") for name in names), "the panel design kept a missing-value flag")
            folds = [record for attempt in result.details["tuning"] for record in attempt.get("folds", [])]
            require(all("dependent_columns" in record for record in folds), f"{slot} tuning folds do not record their dependent columns")
        with libraries():
            nested = research_tests.nested_comparison(changed)
        require(nested["status"] == "fitted", "nested comparison with dependent columns did not fit")
        require({entry["name"] for entry in nested["dependent_columns"]} == {"copy", "total", "kind__category_1"}, "nested comparison dropped other columns")
        require(nested["reduced_references_fitted"] == sorted(changed.feature_lists["rq4_reduced"]), "nested comparison dropped a reduced-list column")

    def dependent_step_uses_fitting_rows_only() -> None:
        with libraries():
            dependent_step_on_rows(dependent_request())

    def dependent_step_on_rows(changed: Any) -> None:
        training, holdout = changed.data.split()
        features = changed.features_for("m3_mixed")
        transform = FoldPreprocessor(features, forced=(changed.cmi_reference,), drop_dependent=True)
        transform.fit_transform(training.rows)
        shifted = tuple(dict(row, copy=row["process"] + 1.0 + index % 3) for index, row in enumerate(holdout.rows))
        require("copy" not in transform.transform(shifted).names, "a column dropped on the fitting rows came back on other rows")
        independent = tuple(dict(row, copy=row["process"] * (1 + index % 2)) for index, row in enumerate(training.rows))
        fresh = FoldPreprocessor(features, forced=(changed.cmi_reference,), drop_dependent=True)
        fresh.fit_transform(independent)
        require("copy" not in {entry["name"] for entry in fresh.dependent_records_}, "the step dropped a column that is independent on the fitting rows")
        require("copy" in fresh.transform(holdout.rows).names, "a duplicate only in the holdout rows changed the fitted design")
        plain = FoldPreprocessor(features, forced=(changed.cmi_reference,))
        require("copy" in plain.fit_transform(training.rows).names, "the step ran on a model that did not ask for it")

    def wide_design_is_refused_not_raised() -> None:
        """A fitting matrix with no more rows than columns (intercept included) is reduced and refused, not indexed past its QR diagonal."""
        from scripts.training.models.common import full_rank, with_intercept

        def prepared_wide(rows: list[dict[str, float]], names: tuple[str, ...]) -> Any:
            transform = FoldPreprocessor(tuple(Feature(f"v:{name}", "number") for name in names), drop_dependent=True)
            try:
                with libraries():
                    return transform, transform.fit_transform(rows)
            except IndexError as error:
                raise AssertionError(f"the dependence step indexed past the QR diagonal of a wide design: {error}") from error

        square = [{"a": 1.0, "b": 2.0, "c": 0.5}, {"a": 2.0, "b": 1.0, "c": 3.0}, {"a": 4.0, "b": 0.0, "c": 1.0}]
        _, design = prepared_wide(square, ("a", "b", "c"))
        require(len(design.names) + 1 <= len(square), "the kept columns and the intercept exceed the rank of three rows")
        with libraries():
            rejects(lambda: full_rank(with_intercept(design.matrix)), "residual degrees of freedom")
        rows = [{"a": a, "copy": a, "b": b, "c": c} for a, b, c in ((1.0, 2.0, 0.5), (2.0, 1.0, 3.0), (4.0, 0.0, 1.0), (3.0, 5.0, 2.0))]
        transform, design = prepared_wide(rows, ("a", "copy", "b", "c"))
        dropped = [entry["name"] for entry in transform.dependent_records_]
        require(dropped == ["copy"], f"a wide design dropped {dropped}, not only the duplicate column")
        require(design.names == ("a", "b", "c"), f"a wide design kept {design.names}")

    def forced_elastic() -> None:
        from scripts.training.models.elastic import ElasticRegressor

        training, _ = req.data.split()
        design = prepared.fit_transform(training.rows)
        forced = design.references.index(req.cmi_reference)
        y = 7 + 2 * design.matrix[:, forced]
        with libraries():
            model = ElasticRegressor(100000.0, 0.5, (req.cmi_reference,), 73).fit(design, y)
        require(np.allclose(model.predict(design), y, atol=1e-9), "forced CMI was penalized")
        rows = tuple(dict(row, cmi=None, process=None) if int(row["ccn"]) % 6 == 0 else dict(row) for row in training.rows)
        median = float(np.median([row["cmi"] for row in rows if row["cmi"] is not None]))
        observed = np.asarray([7 + 2 * (median if row["cmi"] is None else row["cmi"]) + 3 * (row["cmi"] is None) for row in rows])
        design = FoldPreprocessor(training.features, forced=(req.cmi_reference,)).fit_transform(rows)
        with libraries():
            model = ElasticRegressor(100000.0, 0.5, (req.cmi_reference,), 73).fit(design, observed)
        require(np.allclose(model.predict(design), observed, atol=1e-9), "aliased forced CMI indicator was penalized")

    def ownership_singleton() -> None:
        changed = replace(req.data, rows=tuple(dict(row, control_type_code=row["ccn"]) for row in req.data.rows))
        with libraries():
            result = suite.fit_slot(replace(req, data=changed), "m3_mixed")
        require(result.status == "not_converged" and "hospital" in result.reason, "singleton ownership group was accepted")

    def tied_selection() -> None:
        from scripts.training.models.common import SlotResult

        rows = {slot: SlotResult(slot, "fitted", predictions=(1.0,), metrics={"rmse": 1.0}) for slot in SLOTS}
        require(suite.select(rows) == "m1_ols", "slot order must break equal RMSE")
        rows["m1_ols"] = SlotResult("m1_ols", "not_estimable", "rank", metrics={"rmse": 0.0})
        require(suite.select(rows) == "m2_elastic_net", "failed slot won selection")
        rows["m3_mixed"].metrics["rmse"] = 0.1
        require(suite.select(rows, registrable=True) == "m2_elastic_net", "research model became registrable")

    def expired() -> None:
        rejects(lambda: suite.fit_models(replace(req, deadline=Deadline(0.0))), "deadline")

    def nested_gain() -> None:
        with libraries():
            result = research_tests.nested_comparison(req)
        require(result["status"] == "fitted" and result["f_statistic"] > 50, "nested test missed the planted gain")
        require(result["rmse_improvement"] > 0.1 and result["interval"][0] > 0, "paired bootstrap missed the planted gain")
        require(result["bootstrap"]["unit"] == "hospital" and result["bootstrap"]["draws"] == 99, "bootstrap contract missing")

    def nested_invalid() -> None:
        changed = replace(req, feature_lists={**req.feature_lists, "rq4_reduced": ("v:missing",)})
        with libraries():
            rejects(lambda: research_tests.nested_comparison(changed), "nested")
        changed = replace(req, feature_lists={**req.feature_lists, "rq4_reduced": (req.cmi_reference,)})
        with libraries():
            rejects(lambda: research_tests.nested_comparison(changed), "CMS-only")

    def nested_no_gain() -> None:
        rng = np.random.default_rng(199)
        data = replace(
            req.data,
            rows=tuple(dict(row, hai_1__sir__number=3 + 0.4 * row["process"] + float(rng.normal(scale=0.1))) for row in req.data.rows),
        )
        with libraries():
            result = research_tests.nested_comparison(replace(req, data=data))
        require(result["interval"][0] <= 0 <= result["interval"][1], "nested no-gain interval excludes zero")

    def spatial_order_and_islands() -> None:
        _, holdout = req.data.split()
        graph = spatial.build_weights(holdout.rows, ())
        require(graph.keys == tuple(row["spine_key"] for row in holdout.rows), "weights reordered hospital keys")
        require(not graph.islands, "same-county neighbors were missed")
        isolated = (*holdout.rows[:-1], dict(holdout.rows[-1], county_fips="99999"))
        graph = spatial.build_weights(isolated, ())
        require(len(graph.islands) == 1, "island count is wrong")
        with libraries():
            rejects(lambda: spatial.moran_test(np.arange(48, dtype=float), tuple(reversed(graph.keys)), graph, 42, 99, Deadline.after(10)), "order")

    def moran_signal() -> None:
        _, holdout = req.data.split()
        graph = spatial.build_weights(holdout.rows, ())
        signal = np.asarray([int(row["county_fips"]) % 2 for row in holdout.rows], dtype=float)
        with libraries() as replacements:
            original = replacements["esda"].Moran

            def seeded(*args: Any, **kwargs: Any) -> Any:
                require(
                    np.array_equal(np.random.get_state()[1], np.random.RandomState(731).get_state()[1]),
                    "global NumPy seed was not set immediately before Moran",
                )
                return original(*args, **kwargs)

            replacements["esda"].Moran = seeded
            first = spatial.moran_test(signal, graph.keys, graph, 731, 99, Deadline.after(10))
            second = spatial.moran_test(signal, graph.keys, graph, 731, 99, Deadline.after(10))
        require(first == second and first["p_value_name"] == "p_sim", "Moran permutations were not seeded or the gate p-value changed")
        require(first["statistic"] > 0.8 and first["p_value"] < 0.05, "spatial pattern was missed")

    def mundlak_panel(seed: int, hospitals: int = 300, correlation: float = 0.0, effect_scale: float = 1.0) -> tuple[Any, Any, Any, Any, Any]:
        """A balanced panel over four window years whose hospital effect is correlated with the first slope by the given amount."""
        rng = np.random.default_rng(seed)
        years = (2018, 2019, 2021, 2022)
        effect = rng.normal(scale=effect_scale, size=hospitals)
        first = correlation * effect[:, None] + rng.normal(size=(hospitals, len(years)))
        second = rng.normal(size=(hospitals, len(years)))
        level = np.asarray([0.0, 0.2, 0.5, 0.4])
        y = 1.0 + 0.5 * first + 0.3 * second + effect[:, None] + level + rng.normal(scale=0.5, size=(hospitals, len(years)))
        entities = np.repeat([f"H{index:04d}" for index in range(hospitals)], len(years))
        return y.reshape(-1), np.column_stack((first.reshape(-1), second.reshape(-1))), ("first", "second"), entities, np.tile(years, hospitals)

    def mundlak_design(slopes: Any, entities: Any, years: Any) -> Any:
        """The Mundlak regressors built with explicit loops: constant, slopes, hospital means of the slopes, later window-year dummies."""
        means = np.zeros_like(slopes)
        for entity in sorted(set(entities)):
            mask = entities == entity
            means[mask] = slopes[mask].mean(axis=0)
        later = sorted(set(years))[1:]
        dummies = np.column_stack([(years == year).astype(float) for year in later])
        return np.column_stack((np.ones(len(slopes)), slopes, means, dummies))

    def hand_wald(y: Any, design: Any, entities: Any, tested: Sequence[int]) -> tuple[float, float]:
        """Cluster-robust and unadjusted Wald statistics of the tested coefficients, one hospital at a time."""
        rows, columns = design.shape
        inverse = np.linalg.inv(design.T @ design)
        beta = inverse @ design.T @ y
        residual = y - design @ beta
        meat = np.zeros((columns, columns))
        labels = sorted(set(entities))
        for entity in labels:
            mask = entities == entity
            score = design[mask].T @ residual[mask]
            meat += np.outer(score, score)
        count = len(labels)
        clustered = count / (count - 1) * (rows - 1) / (rows - columns) * inverse @ meat @ inverse
        unadjusted = float(residual @ residual) / (rows - columns) * inverse
        gamma, index = beta[list(tested)], np.ix_(list(tested), list(tested))
        return float(gamma @ np.linalg.solve(clustered[index], gamma)), float(gamma @ np.linalg.solve(unadjusted[index], gamma))

    def exact_f_tail() -> None:
        for critical, numerator, denominator in ((4.9646, 1, 10), (4.1028, 2, 10), (2.7109, 5, 20), (2.9223, 3, 30), (2.5252, 4, 60), (3.0718, 2, 120)):
            require(
                abs(upper_f_tail(critical, numerator, denominator) - 0.05) < 2e-5,
                f"F tail is wrong at ({numerator}, {denominator}) degrees of freedom",
            )
        for denominator in (3, 49, 299):
            for value in (0.1, 1.0, 7.3):
                require(
                    math.isclose(upper_f_tail(value, 2, denominator), (1 + 2 * value / denominator) ** (-denominator / 2), rel_tol=1e-10),
                    f"F tail is wrong for two numerator degrees of freedom at {denominator}",
                )
        for numerator in (4, 6):
            require(
                math.isclose(upper_f_tail(1.7, numerator, 2), 1 - (numerator * 1.7 / (2 + numerator * 1.7)) ** (numerator / 2), rel_tol=1e-12),
                f"F tail is wrong for two denominator degrees of freedom at {numerator}",
            )
        require(upper_f_tail(0.0, 3, 5) == 1.0, "F tail is not one at zero")

    def mundlak_rejects_correlated_effect() -> None:
        with exact_f() as tail:
            result = research_tests.mundlak(*mundlak_panel(11, correlation=0.8))
        require(result["status"] == "fitted", f"correlated panel was not estimable: {result}")
        require(result["p_value"] < 1e-6, "Mundlak test missed a hospital effect correlated with a regressor")
        require(result["clusters"] == 300 and result["rows"] == 1200, "Mundlak record has the wrong size fields")
        require(
            result["numerator_degrees_of_freedom"] == 2 and result["denominator_degrees_of_freedom"] == 299,
            f"Mundlak record has the wrong F degrees of freedom: {result}",
        )
        require(
            result["tested_columns"] == ["first", "second"]
            and result["covariance"] == "cluster_robust_cr1"
            and result["reference_distribution"] == "f"
            and "degrees_of_freedom" not in result,
            "Mundlak record has the wrong tested columns or reference distribution",
        )
        require(
            math.isclose(result["f_statistic"], result["statistic"] / 2, rel_tol=1e-12),
            "Mundlak F statistic is not the Wald statistic divided by the tested slopes",
        )
        require(tail.calls == [(result["f_statistic"], 2.0, 299.0)], f"Mundlak tail received the wrong statistic or degrees of freedom: {tail.calls}")

    def mundlak_keeps_null_without_correlation() -> None:
        with exact_f():
            single = research_tests.mundlak(*mundlak_panel(12, correlation=0.0))
            p_values = [research_tests.mundlak(*mundlak_panel(seed, correlation=0.0))["p_value"] for seed in range(100, 160)]
        require(single["status"] == "fitted" and single["p_value"] > 0.05, f"Mundlak test rejected an uncorrelated hospital effect at seed 12: {single}")
        rejections = sum(value < 0.05 for value in p_values)
        require(rejections <= 9, f"Mundlak test rejected {rejections} of 60 uncorrelated panels at the 0.05 level (limit 9 for a size near 0.05)")

    def mundlak_clustered_errors_differ_from_unadjusted() -> None:
        y, slopes, names, entities, years = mundlak_panel(21, correlation=0.0, effect_scale=1.0)
        design = mundlak_design(slopes, entities, years)
        _, clustered, unadjusted = research_tests.cluster_covariances(y, design, entities)
        ratio = np.sqrt(np.diag(clustered) / np.diag(unadjusted))[3:5]
        require(bool(np.all((ratio > 1.5) & (ratio < 2.3))), f"clustered errors do not exceed unadjusted errors under within-hospital correlation: {ratio}")
        flat_y, flat_slopes, _, flat_entities, flat_years = mundlak_panel(21, correlation=0.0, effect_scale=0.0)
        _, flat_clustered, flat_unadjusted = research_tests.cluster_covariances(flat_y, mundlak_design(flat_slopes, flat_entities, flat_years), flat_entities)
        flat_ratio = np.sqrt(np.diag(flat_clustered) / np.diag(flat_unadjusted))[3:5]
        require(
            bool(np.all((flat_ratio > 0.85) & (flat_ratio < 1.15))),
            f"clustered errors differ from unadjusted ones without within-hospital correlation: {flat_ratio}",
        )
        with exact_f():
            records = [research_tests.mundlak(*mundlak_panel(21, correlation=correlation)) for correlation in (0.0, 0.8)]
        for record, correlation in zip(records, (0.0, 0.8), strict=True):
            panel_data = mundlak_panel(21, correlation=correlation)
            expected, unadjusted_statistic = hand_wald(panel_data[0], mundlak_design(panel_data[1], panel_data[3], panel_data[4]), panel_data[3], (3, 4))
            require(math.isclose(record["statistic"], expected, rel_tol=1e-9), "Mundlak statistic differs from the hand-computed cluster-robust Wald statistic")
            require(record["statistic"] < 0.9 * unadjusted_statistic, "Mundlak statistic did not use clustered errors")
            if correlation == 0.0:
                require(
                    record["statistic"] < 0.5 * unadjusted_statistic,
                    "clustered Wald statistic is not far below the unadjusted one under hospital-level error correlation",
                )

    def mundlak_reads_p_value_from_f_with_cluster_degrees() -> None:
        for hospitals, correlation in ((300, 0.0), (300, 0.8), (50, 0.0), (50, 0.8), (80, 0.3)):
            data = mundlak_panel(41, hospitals=hospitals, correlation=correlation)
            wald, _ = hand_wald(data[0], mundlak_design(data[1], data[3], data[4]), data[3], (3, 4))
            with exact_f():
                record = research_tests.mundlak(*data)
            denominator = hospitals - 1
            expected_f = wald / 2
            expected_p = (1 + 2 * expected_f / denominator) ** (-denominator / 2)
            where = f"{hospitals} hospitals, correlation {correlation}"
            require(record["status"] == "fitted" and record["clusters"] == hospitals, f"Mundlak record is not fitted for {where}: {record}")
            require(math.isclose(record["f_statistic"], expected_f, rel_tol=1e-9), f"Mundlak F is not the hand-computed Wald statistic over two for {where}")
            require(record["denominator_degrees_of_freedom"] == denominator, f"Mundlak F denominator is not the clusters minus one for {where}")
            require(math.isclose(record["p_value"], expected_p, rel_tol=1e-8), f"Mundlak p-value differs from the exact F(2, {denominator}) tail for {where}")

    def mundlak_numerator_follows_the_tested_slopes() -> None:
        y, slopes, names, entities, years = mundlak_panel(42, hospitals=80, correlation=0.4)
        third = np.random.default_rng(43).normal(size=len(y))
        three = np.column_stack((slopes, third))
        wald, _ = hand_wald(y, mundlak_design(three, entities, years), entities, (4, 5, 6))
        with exact_f() as tail:
            record = research_tests.mundlak(y, three, (*names, "third"), entities, years)
        require(record["status"] == "fitted" and record["tested_columns"] == ["first", "second", "third"], f"three-slope Mundlak test failed: {record}")
        require(record["numerator_degrees_of_freedom"] == 3 and record["denominator_degrees_of_freedom"] == 79, f"wrong F degrees of freedom: {record}")
        require(math.isclose(record["statistic"], wald, rel_tol=1e-9), "three-slope Wald statistic differs from the hand computation")
        require(math.isclose(record["f_statistic"], wald / 3, rel_tol=1e-9), "three-slope F is not the Wald statistic divided by three")
        require(tail.calls == [(record["f_statistic"], 3.0, 79.0)], f"three-slope tail received the wrong arguments: {tail.calls}")
        require(math.isclose(record["p_value"], upper_f_tail(wald / 3, 3, 79), rel_tol=1e-9), "three-slope p-value differs from the exact F(3, 79) tail")

    def mundlak_refuses_few_clusters() -> None:
        with exact_f():
            few = research_tests.mundlak(*mundlak_panel(31, hospitals=49))
            enough = research_tests.mundlak(*mundlak_panel(31, hospitals=50))
        require(
            few["status"] == "not_estimable" and "at least 50 hospital clusters" in few["reason"] and few["clusters"] == 49,
            f"few clusters were not refused: {few}",
        )
        require(enough["status"] == "fitted" and enough["clusters"] == 50, "the cluster minimum is not exactly 50")

    def mundlak_refuses_unidentified_design() -> None:
        y, slopes, names, entities, years = mundlak_panel(32)
        with exact_f():
            duplicated = research_tests.mundlak(y, np.column_stack((slopes, slopes[:, 0])), (*names, "copy"), entities, years)
        require(duplicated["status"] == "not_estimable" and "rank" in duplicated["reason"], f"a duplicated slope was not refused: {duplicated}")

    def time_folds_keep_three_fitting_years() -> None:
        wide = request(years=(2018, 2019, 2021, 2022, 2023), hospitals=60)
        training, _ = wide.data.split()
        folds = suite.time_folds(training)
        require(len(folds) == 1, f"five training years should give one fold, got {len(folds)}")
        for left, right in folds:
            require(len({training.rows[i]["window_year"] for i in left}) >= 3, "a time fold fits on fewer than three years")
            require({training.rows[i]["window_year"] for i in right} == {2022}, "the first validated year is not the fourth training year")

    def panel_refuses_fewer_than_three_fitting_years() -> None:
        from scripts.training.models.common import NotEstimable

        few = request(years=(2020, 2021, 2022), hospitals=60)
        with libraries(), patch.object(panel, "native_fits", side_effect=panel_standin):
            result = suite.fit_slot(few, "m6_fixed_effects")
            enough = suite.fit_slot(request(years=(2019, 2020, 2021, 2022), hospitals=60), "m6_fixed_effects")
        require(
            result.status == "not_estimable" and "at least three fitting years" in result.reason,
            f"two fitting years were not refused: {result.status} {result.reason}",
        )
        require(enough.status == "fitted", "three fitting years were refused")
        training, _ = few.data.split()
        design = FoldPreprocessor(training.features).fit_transform(training.rows)
        try:
            panel.PanelRegressor().fit(design, training.y)
        except NotEstimable as error:
            require("at least three fitting years" in str(error), f"wrong refusal reason: {error}")
        else:
            raise AssertionError("the panel regressor accepted two fitting years")

    def one_model_run_of_m6_carries_mundlak() -> None:
        """The per-model path (``fit_model(request, "m6")``, one model per run) keeps the full Mundlak record as the RQ6 result, not only its status."""
        sixty = request(years=(2019, 2020, 2021, 2022), hospitals=60)
        with libraries(), patch.object(panel, "native_fits", side_effect=panel_standin):
            output = suite.fit_model(sixty, "m6")
        record = output.research["hospital_effects"]
        require(record.get("status") == "fitted" and record.get("test") == "mundlak", f"the m6 run's RQ6 record is not a Mundlak record: {record}")
        require({"f_statistic", "p_value", "clusters", "tested_columns"} <= set(record), f"the m6 run's RQ6 record lacks a Mundlak field: {sorted(record)}")

    def panel_record_carries_mundlak() -> None:
        sixty = request(years=(2019, 2020, 2021, 2022), hospitals=60)
        with libraries() as replacements, patch.object(panel, "native_fits", side_effect=panel_standin):
            output = suite.fit_models(sixty)
        record = output.research["hospital_effects"]
        require(record["status"] == "fitted" and record["test"] == "mundlak", f"RQ6 record is not a Mundlak record: {record}")
        require(
            {"statistic", "f_statistic", "numerator_degrees_of_freedom", "denominator_degrees_of_freedom", "p_value", "clusters"} <= set(record),
            "RQ6 record lacks a Mundlak field",
        )
        tested = len(record["tested_columns"])
        require(record["reference_distribution"] == "f" and record["numerator_degrees_of_freedom"] == tested, f"RQ6 record has the wrong reference: {record}")
        require(record["denominator_degrees_of_freedom"] == 59, f"RQ6 record has the wrong F denominator: {record}")
        require(
            (record["f_statistic"], float(tested), 59.0) in replacements["scipy.stats"].f.calls,
            f"RQ6 p-value was not read from the F tail: {replacements['scipy.stats'].f.calls}",
        )
        require(record["clusters"] == 60 and record["covariance"] == "cluster_robust_cr1", "RQ6 record has the wrong cluster count or covariance")
        require(
            "mundlak" in output.slots["m6_fixed_effects"].details and "hausman" not in output.slots["m6_fixed_effects"].details, "m6 details keep the Hausman"
        )
        require("hausman" not in json.dumps(output.to_result(), sort_keys=True).lower(), "the result payload still names the Hausman test")
        require("hospital_effects.p_value" in output.to_result()["p_values"], "the RQ6 p-value is missing from the p-value bundle")

    def panel_fits_complete_cases() -> None:
        data = request(years=(2019, 2020, 2021, 2022), hospitals=60)
        rows = [dict(row) for row in data.data.rows]
        training_rows = [index for index, row in enumerate(rows) if row["window_year"] < 2022]
        for position in training_rows[0:30:3]:
            rows[position]["process"] = None
        first_hospital = [index for index in training_rows if rows[index]["ccn"] == rows[training_rows[40]]["ccn"]]
        for position in first_hospital[:2]:
            rows[position]["process"] = None
        holdout_position = next(index for index, row in enumerate(rows) if row["window_year"] == 2022)
        rows[holdout_position]["cmi"] = None
        incomplete = sum(row["process"] is None or row["cmi"] is None for row in rows)
        changed = replace(data, data=replace(data.data, rows=tuple(rows)))
        with libraries(), patch.object(panel, "native_fits", side_effect=panel_standin):
            result = suite.fit_slot(changed, "m6_fixed_effects")
        require(result.status == "fitted", f"complete-case panel failed: {result.status} {result.reason}")
        require(result.details["dropped_incomplete_row_count"] == incomplete - 1, "incomplete training rows were not counted")
        require(result.details["dropped_singleton_hospital_count"] == 1, "the hospital left with one complete window was not dropped")
        require(all(value == 0.0 for value in result.details["training_missing_fraction"].values()), "the fitting rows still hold missing values")
        _, holdout = changed.data.split()
        names = result.pipeline.named_steps["prepare"].transform(holdout.rows).names
        require(not any(name.endswith("__missing") for name in names), "m6 kept a missing-value flag")
        require(len(result.predictions) == len(holdout.rows) and np.isfinite(result.predictions).all(), "a holdout row with a missing predictor was not scored")

    def panel_unbalanced_and_unseen() -> None:
        rows = tuple(row for row in req.data.rows if row["ccn"] != "990000" or row["window_year"] in (2020, 2023))
        changed = replace(req, data=replace(req.data, rows=rows))
        with libraries(), patch.object(panel, "native_fits", side_effect=panel_standin):
            result = suite.fit_slot(changed, "m6_fixed_effects")
        require(result.status == "fitted", "unbalanced panel failed")
        require(
            result.details["unseen_holdout_hospitals"] == 1 and result.details["dropped_singleton_hospital_count"] == 1,
            "panel singleton or unseen-effect count is wrong",
        )
        _, holdout = changed.data.split()
        design = result.pipeline.named_steps["prepare"].transform(holdout.rows)
        fitted = result.pipeline.named_steps["model"]
        expected = np.r_[1.0, design.matrix[0, fitted.kept_indices_]] @ fitted.coefficients_ + sum(fitted.effects_.values()) / len(fitted.effects_)
        require(math.isclose(result.predictions[0], expected), "unseen hospital did not receive the mean fitted effect")

    def panel_with(years: Sequence[int], hospitals: int, **columns: Callable[[Any], Any]) -> Any:
        """The synthetic panel plus extra m6 predictors named C202 and up, each valued by ``columns[name](row)``, where None marks a missing value."""
        base = request(years=years, hospitals=hospitals)
        added = tuple(Feature(f"feature_matrix_county_context:{name}", "number", f"C{202 + index}", "county_context") for index, name in enumerate(columns))
        rows = tuple(dict(row, **{name: call(row) for name, call in columns.items()}) for row in base.data.rows)
        lists = {**base.feature_lists, "m6_fixed_effects": (*base.feature_lists["m6_fixed_effects"], *(feature.reference for feature in added))}
        return replace(base, data=Dataset(rows, (*base.data.features, *added), "HAI_1"), feature_lists=lists)

    def value_of(row: Any) -> float:
        """A deterministic predictor value that no other synthetic column explains."""
        return float((int(row["ccn"]) * 7 + row["window_year"] * 13) % 17)

    def fit_m6(changed: Any) -> Any:
        with libraries(), patch.object(panel, "native_fits", side_effect=panel_standin):
            return suite.fit_slot(changed, "m6_fixed_effects")

    def panel_fits_with_an_entirely_missing_predictor() -> None:
        """The reviewer's reproduction: one predictor with no value in any row used to leave no complete row, so m6 fitted nothing."""
        changed = panel_with(range(2019, 2023), 60, late=lambda row: None)
        result = fit_m6(changed)
        require(result.status == "fitted", f"m6 with one entirely missing predictor is {result.status}: {result.reason}")
        selection = result.details["panel_selection"]
        require(selection["learned_on"] == "fitting_rows", f"the selection does not say what it was learned on: {selection}")
        require(
            selection["dropped"] == [{"reference": "feature_matrix_county_context:late", "missing_fraction": 1.0}], f"wrong dropped predictors: {selection}"
        )
        require("feature_matrix_county_context:late" not in selection["kept_references"], "the missing predictor stayed in the kept set")
        prepare = result.pipeline.named_steps["prepare"]
        require(tuple(selection["kept_references"]) == prepare.kept_references_, "the recorded kept set is not the fitted design's kept set")
        require(result.details["dropped_incomplete_row_count"] == 0, "rows were dropped for a predictor that was left out")
        require(result.details["panel_fitting_rows"] == 180, f"panel rows: {result.details['panel_fitting_rows']}")
        _, holdout = changed.data.split()
        require("late" not in prepare.transform(holdout.rows).names, "the dropped predictor is in the design")
        record = result.details["mundlak"]
        require(record["status"] == "fitted" and "late" not in record["tested_columns"], f"the Mundlak test used the dropped predictor: {record}")
        require(record["numerator_degrees_of_freedom"] == len(record["tested_columns"]) > 0, f"q is not the number of tested slopes: {record}")

    def panel_drops_a_mostly_missing_predictor() -> None:
        changed = panel_with(range(2019, 2023), 60, late=lambda row: None if row["window_year"] < 2021 else value_of(row))
        result = fit_m6(changed)
        require(result.status == "fitted", f"m6 with a mostly missing predictor is {result.status}: {result.reason}")
        (dropped,) = result.details["panel_selection"]["dropped"]
        require(dropped["reference"].endswith(":late") and math.isclose(dropped["missing_fraction"], 2 / 3), f"wrong dropped record: {dropped}")
        require(result.details["panel_fitting_rows"] == 180 and result.details["dropped_incomplete_row_count"] == 0, "rows were lost to a dropped predictor")

    def panel_final_fit_selects_before_complete_cases() -> None:
        """Rows missing the sparse predictor are the rows that hold the other one, so complete cases on the full list leave nothing."""
        changed = panel_with(
            range(2019, 2023),
            60,
            sparse=lambda row: None if int(row["ccn"]) % 10 < 6 else value_of(row),
            part=lambda row: None if int(row["ccn"]) % 10 >= 6 else value_of(row) + 1.0,
        )
        training, _ = changed.data.split()
        result = fit_m6(changed)
        require(result.status == "fitted", f"the final fit did not select before complete cases: {result.status} {result.reason}")
        selection = result.details["panel_selection"]
        require([entry["reference"] for entry in selection["dropped"]] == ["feature_matrix_county_context:sparse"], f"wrong dropped predictors: {selection}")
        require("feature_matrix_county_context:part" in selection["kept_references"], "the kept predictor was dropped")
        part = "feature_matrix_county_context:part"
        original = sum(row["part"] is None for row in training.rows) / len(training.rows)
        require(0.3 < original < 0.5, f"the fixture does not hold a kept predictor with a nonzero original missing share: {original}")
        kept_fractions = {entry["reference"]: entry["missing_fraction"] for entry in selection["kept"]}
        require(list(kept_fractions) == list(selection["kept_references"]), f"the kept fractions do not follow the kept references: {selection}")
        require(math.isclose(kept_fractions[part], original), f"a kept predictor lost its original missing fraction: {kept_fractions[part]} != {original}")
        require(selection["dropped"][0]["missing_fraction"] > 0.5 >= max(kept_fractions.values()), "kept and dropped fractions do not meet at the threshold")
        require(result.details["training_missing_fraction"][part] == 0.0, "the complete-row fraction is not recorded separately from the original one")
        left = sum(row["part"] is not None for row in training.rows)
        require(result.details["panel_fitting_rows"] == left, f"complete cases were not kept on the kept set: {result.details['panel_fitting_rows']} != {left}")
        require(result.details["dropped_incomplete_row_count"] == len(training.rows) - left, "the incomplete row count is wrong")
        require("part" in result.details["mundlak"]["tested_columns"], "the Mundlak test lost the kept predictor")

    def panel_fold_selection_ignores_validation_rows() -> None:
        """Fold fitting rows 2018 to 2020: two of three missing drops the predictor, while counting the 2021 validation rows would keep it."""

        def tuned(late: Callable[[Any], Any]) -> list[dict[str, Any]]:
            changed = panel_with(range(2018, 2023), 60, late=late)
            training, _ = changed.data.split()
            with libraries(), patch.object(panel, "native_fits", side_effect=panel_standin):
                _, audit = suite._tune(changed, "m6_fixed_effects", False, training)
            require(audit and audit[0]["status"] == "fitted", f"the m6 tuning did not fit: {audit}")
            return audit[0]["folds"]

        present = tuned(lambda row: None if row["window_year"] in (2018, 2019) else value_of(row))
        absent = tuned(lambda row: None if row["window_year"] in (2018, 2019, 2021) else value_of(row))
        for folds in (present, absent):
            require(len(folds) == 1 and folds[0]["fitting_rows"] == 180, f"unexpected folds: {folds}")
            require(
                not any(reference.endswith(":late") for reference in folds[0]["selected_references"]), "validation rows kept a predictor the fitting rows drop"
            )
        kept = tuned(lambda row: None if row["window_year"] == 2021 else value_of(row))
        require(any(reference.endswith(":late") for reference in kept[0]["selected_references"]), "validation rows removed a predictor the fitting rows keep")
        require(kept[0]["fitting_rows"] == 180, "validation rows changed which fitting rows are complete")

    def panel_tuning_drops_a_fold_that_loses_a_fitting_year() -> None:
        """Training years 2018 to 2022: the 2021 fold fits on 2018 to 2020, and complete cases remove 2018, leaving two years; the 2022 fold keeps three."""
        changed = panel_with(range(2018, 2024), 60, early=lambda row: None if row["window_year"] == 2018 else value_of(row))
        result = fit_m6(changed)
        require(result.status == "fitted", f"a short first fold stopped the m6 tuning: {result.status} {result.reason}")
        tuning = result.details["tuning"]
        fitted = [record for record in tuning if record["status"] == "fitted"]
        require(len(fitted) == 1 and [fold["fitting_rows"] for fold in fitted[0]["folds"]] == [180], f"the later fold did not tune: {tuning}")
        (dropped,) = [record for record in tuning if record["status"] == "fold_dropped"]
        require(dropped["validation_year"] == 2021 and dropped["fitting_years"] == 2, f"the dropped fold is not recorded with its year and count: {dropped}")
        require("at least 3 fitting years" in dropped["reason"], f"the dropped fold does not give its reason: {dropped}")

    def panel_tuning_with_every_fold_short_takes_the_first_option() -> None:
        """Training years 2018 to 2021 give one fold, and it loses 2018: the first option stands with a not_estimable record, and the final fit still runs."""
        changed = panel_with(range(2018, 2023), 60, early=lambda row: None if row["window_year"] == 2018 else value_of(row))
        result = fit_m6(changed)
        require(result.status == "fitted", f"every short fold stopped the m6 fit: {result.status} {result.reason}")
        tuning = result.details["tuning"]
        require(tuning[0]["status"] == "not_estimable" and "no time-ordered validation fold" in tuning[0]["reason"], f"wrong first tuning record: {tuning}")
        (dropped,) = [record for record in tuning if record["status"] == "fold_dropped"]
        require(dropped["validation_year"] == 2021 and dropped["fitting_years"] == 2, f"the dropped fold is not recorded: {dropped}")

    def panel_with_few_clusters_reports_the_tested_slopes() -> None:
        changed = panel_with(range(2019, 2023), 40, late=lambda row: None)
        result = fit_m6(changed)
        record = result.details["mundlak"]
        require(result.status == "fitted", f"fewer than 50 hospitals stopped the fit: {result.status} {result.reason}")
        require(
            record["status"] == "not_estimable" and "at least 50 hospital clusters" in record["reason"] and record["clusters"] == 40, f"wrong refusal: {record}"
        )
        require(record["tested_columns"] and "late" not in record["tested_columns"], f"the refusal does not name the tested slopes: {record}")
        require(record["numerator_degrees_of_freedom"] == len(record["tested_columns"]), f"the refusal does not report q: {record}")

    def panel_selection_is_in_the_run_result() -> None:
        changed = panel_with(range(2019, 2023), 60, late=lambda row: None)
        with libraries(), patch.object(panel, "native_fits", side_effect=panel_standin):
            output = suite.fit_model(changed, "m6")
        payload = output.to_result()
        require(payload["fitted_parameters"]["m6_fixed_effects"]["panel_selection"]["dropped"], "the kept set is not in the result")
        record = output.research["hospital_effects"]
        require(record["status"] == "fitted" and "late" not in record["tested_columns"], f"the RQ6 record used the dropped predictor: {record}")
        json.dumps(payload, sort_keys=True, allow_nan=False)

    def panel_without_repeated_hospitals_stays_not_estimable() -> None:
        base = request(years=range(2019, 2023), hospitals=120)
        rows = tuple(row for row in base.data.rows if row["window_year"] == 2022 or row["window_year"] == 2019 + int(row["ccn"]) % 3)
        single = replace(base, data=replace(base.data, rows=rows))
        result = fit_m6(single)
        require(
            result.status == "not_estimable" and "no complete repeated training hospitals" in result.reason,
            f"a panel with no repeated hospital: {result.status} {result.reason}",
        )
        no_cmi = panel_with(range(2019, 2023), 60, late=value_of)
        absent = replace(no_cmi, data=replace(no_cmi.data, rows=tuple(dict(row, cmi=None) for row in no_cmi.data.rows)))
        result = fit_m6(absent)
        require(result.status == "not_estimable" and "forced covariate" in result.reason, f"a missing forced covariate: {result.status} {result.reason}")

    def shap_failure() -> None:
        from scripts.training.model_test_support import TreeExplainer

        with libraries():
            fitted = suite.fit_slot(req, "m4_boosting")
            _, holdout = req.data.split()
            original = TreeExplainer.shap_values

            def wrong(self: Any, x: Any, *, check_additivity: bool) -> Any:
                return original(self, x, check_additivity=check_additivity) + 1.0

            with patch.object(TreeExplainer, "shap_values", wrong):
                rejects(lambda: shap_values(fitted.pipeline, holdout.rows, 12, 8, Deadline.after(10)), "additivity")

    def mediation_separation() -> None:
        mediator_rows = tuple(
            {
                "spine_key": row["spine_key"],
                "measure": measure,
                "value_number": row["process"] * 0.8 + row["extra"] * 0.2,
                "alignment_status": "aligned",
                "period_end": date(row["window_year"] - 1, 12, 31),
            }
            for row in req.data.rows
            for measure in ("PSI_90", "PSI_13")
        )
        resample = mediation.hospital_resamples

        def hospital_blocks(entities: Any, draws: int, seed: int) -> Any:
            for positions in resample(entities, draws, seed):
                counts = np.bincount(positions, minlength=len(entities))
                for entity in set(entities):
                    require(len(set(counts[[index for index, value in enumerate(entities) if value == entity]])) == 1, "bootstrap split a hospital's windows")
                yield positions

        with libraries(), patch.object(mediation, "hospital_resamples", side_effect=hospital_blocks):
            output = mediation.analyze_mediation(req.data, mediator_rows, req.process_references, (req.cmi_reference,), req.config, Deadline.after(30))
        require(set(output["mediators"]) == {"PSI_90", "PSI_13"}, "both owner-authorized mediators must be analyzed")
        require(all(value["status"] == "fitted" for value in output["mediators"].values()), "mediation did not estimate its association")
        require(all(value["interpretation"] == "associational" for value in output["mediators"].values()), "mediation claims causation")
        require(
            all(effect["interval"][0] < effect["interval"][1] for value in output["mediators"].values() for effect in value["effects"].values()),
            "mediation bootstrap did not refit resampled hospitals",
        )
        future = (dict(mediator_rows[0], period_end=date(2020, 1, 1)), *mediator_rows[1:])
        with libraries():
            rejects(
                lambda: mediation.analyze_mediation(req.data, future, req.process_references, (req.cmi_reference,), req.config, Deadline.after(30)),
                "before the outcome",
            )

    def hac_join() -> None:
        plan = hac_validation.HacPlan(2023, 2025, 2021, (2023,))
        predictions = tuple({"spine_key": f"{990000 + i}:2023", "predicted": float(i), "observed": 0.0} for i in range(8))
        flags = tuple({"spine_key": f"{990000 + i}:2023", "fiscal_year": 2025, "value_number": int(i >= 6), "alignment_status": "aligned"} for i in range(7))
        output = hac_validation.validate_hac(predictions, flags, plan, Deadline.after(10))
        require(output["status"] == "fitted", "explicit HAC program-year mapping was not used")
        require(output["missing_flags"] == 1 and output["matched_hospitals"] == 7, "missing HAC flags became negative flags")
        require(output["auc"] == 1.0 and output["top_quartile_flagged_share"] == 0.5, "HAC ranking or quartile is wrong")
        require(hac_validation.validate_hac(predictions, (), plan, Deadline.after(10))["status"] == "not_run", "missing HAC source must not fail training")
        other = hac_validation.HacPlan(2023, 2024, 2021, (2023,))
        require(hac_validation.validate_hac(predictions, flags, other, Deadline.after(10))["status"] == "not_run", "wrong program year joined")
        rejects(lambda: hac_validation.validate_hac(predictions, (*flags, flags[0]), plan, Deadline.after(10)), "duplicate HAC")
        # The extract keeps the hospital-windows that HAC did not publish or whose period did not match: they carry no fiscal year and no flag.
        unaligned = (
            {"spine_key": "990007:2023", "fiscal_year": None, "value_number": None, "alignment_status": "not_in_source"},
            {"spine_key": "990007:2022", "fiscal_year": None, "value_number": None, "alignment_status": "not_in_source"},
            {"spine_key": "990008:2023", "fiscal_year": None, "value_number": None, "alignment_status": "no_matching_period"},
            # Veterans Affairs CCNs end in a letter; HAC does not publish them.
            {"spine_key": "99001F:2023", "fiscal_year": None, "value_number": None, "alignment_status": "not_in_source"},
        )
        kept = hac_validation.validate_hac(predictions, (*flags, *unaligned), plan, Deadline.after(10))
        compared = ("status", "matched_hospitals", "missing_flags", "auc", "top_quartile_flagged_share")
        require({key: kept[key] for key in compared} == {key: output[key] for key in compared}, "unaligned HAC rows without a fiscal year changed the result")
        aligned_without_year = {"spine_key": "990007:2023", "fiscal_year": None, "value_number": 1, "alignment_status": "aligned"}
        rejects(lambda: hac_validation.validate_hac(predictions, (*flags, aligned_without_year), plan, Deadline.after(10)), "lacks a program fiscal year")
        rejects(lambda: hac_validation.plan_validation({2023: 8, 2022: 8, 2021: 8}, {}, {2025: (2022, 2023)}), "year mapping")

    def shared_missing_indicators() -> None:
        changed_rows = tuple(dict(row, process=None, extra=None) if int(row["ccn"]) % 6 == 0 else dict(row) for row in req.data.rows)
        data = replace(req.data, rows=changed_rows)
        lists = {**req.feature_lists, "m1_ols": tuple(feature.reference for feature in data.features)}
        changed = replace(req, data=data, feature_lists=lists)
        with libraries(), patch.object(panel, "native_fits", side_effect=panel_standin):
            results = {}
            for slot in SLOTS:
                results[slot] = suite.fit_slot(changed, slot)
                require(results[slot].status == "fitted", f"shared missing indicators made {slot} non-estimable: {results[slot].reason}")
            nested = research_tests.nested_comparison(changed)
        require(nested["status"] == "fitted", "shared missing indicators prevented the nested test")
        training, holdout = data.split()
        transform = FoldPreprocessor(data.features).fit(training.rows)
        design = transform.transform(holdout.rows)
        require(design.names.count("process__missing") == 1 and "extra__missing" not in design.names, "duplicate indicator was retained")
        require(transform.indicator_aliases_ == {"extra__missing": "process__missing"}, "indicator aliases were not recorded")
        require(results["m5_spatial"].details["indicator_aliases"] == transform.indicator_aliases_, "result lost the indicator mapping")
        require(results["m6_fixed_effects"].details["indicator_aliases"] == {}, "the panel model kept missing-value indicators")
        masked = np.asarray([row["extra"] is None for row in training.rows], dtype=float)
        learned = transform.transform(training.rows)
        require(np.array_equal(masked, learned.matrix[:, learned.names.index("process__missing")]), "indicator deduplication lost fitting-row information")
        constant_rows = tuple(dict(row, extra=3.0) for row in training.rows)
        constant = FoldPreprocessor(data.features).fit_transform(constant_rows)
        require("extra" not in constant.names, "constant numeric predictor was retained")
        transform = FoldPreprocessor(data.features).fit(constant_rows)
        require(transform.constant_columns_ == {"extra": 3.0}, "constant predictor value was not recorded")
        rejects(
            lambda: FoldPreprocessor(data.features, forced=(req.cmi_reference,)).fit(tuple(dict(row, cmi=1.0) for row in training.rows)), "forced covariate"
        )

        rng = np.random.default_rng(1593)
        categories = (
            Feature("feature_matrix_care_compare:kind_a", "category", family="care_compare"),
            Feature("feature_matrix_care_compare:kind_b", "category", family="care_compare"),
        )
        category_rows = tuple(
            dict(
                row, kind_a=None if int(row["ccn"]) % 6 == 0 else str(rng.integers(0, 3)), kind_b=None if int(row["ccn"]) % 6 == 0 else str(rng.integers(0, 4))
            )
            for row in req.data.rows
        )
        for category_count, numeric_missing in ((2, False), (1, True), (2, True)):
            features = (
                *categories[:category_count],
                *req.data.features[:2],
                replace(req.data.features[2], reference="feature_matrix_care_compare:extra", family="care_compare", control="C120"),
            )
            rows = tuple(dict(row, extra=None) if numeric_missing and int(row["ccn"]) % 6 == 0 else dict(row) for row in category_rows)
            if category_count == 1:
                rows = tuple({key: value for key, value in row.items() if key != "kind_b"} for row in rows)
            data = replace(req.data, rows=rows, features=features)
            references = tuple(feature.reference for feature in features)
            changed = replace(req, data=data, feature_lists={**req.feature_lists, **dict.fromkeys(SLOTS, references)})
            with libraries(), patch.object(panel, "native_fits", side_effect=panel_standin):
                for slot in SLOTS:
                    fitted = suite.fit_slot(changed, slot)
                    require(fitted.status == "fitted", f"categorical shared missing indicators made {slot} non-estimable: {fitted.reason}")
                require(research_tests.nested_comparison(changed)["status"] == "fitted", "categorical shared missingness prevented nesting")
                if numeric_missing:
                    extra_reference = features[-1].reference
                    fitted = suite.fit_slot(changed, "m1_ols")
                    joint = research_tests.joint_significance(fitted.pipeline, (extra_reference,))
                    nested_alias = research_tests.nested_comparison(
                        replace(changed, feature_lists={**changed.feature_lists, "rq4_reduced": (extra_reference,)})
                    )
                    require(joint["numerator_degrees_of_freedom"] == 2, "joint test lost a categorical canonical missing indicator")
                    require(
                        nested_alias["extra_degrees_of_freedom"] == (4 if category_count == 1 else 7),
                        "nested test lost a categorical canonical missing indicator",
                    )
            training, holdout = data.split()
            transform = FoldPreprocessor(features).fit(training.rows)
            learned, scored = transform.transform(training.rows), transform.transform(holdout.rows)
            aliases = {"kind_b__missing": "kind_a__missing"} if category_count == 2 else {}
            if numeric_missing:
                aliases["extra__missing"] = "kind_a__missing"
            require(transform.indicator_aliases_ == aliases, "categorical indicator aliases were not recorded")
            require(all(reference in transform.kept_references_ for reference in references), "categorical repair silently dropped a predictor")
            null_index = learned.names.index("kind_a__missing")
            require(np.array_equal(learned.matrix[:, null_index], [row["kind_a"] is None for row in training.rows]), "categorical null indicator changed")
            require(np.array_equal(scored.matrix[:, null_index], [row["kind_a"] is None for row in holdout.rows]), "scoring changed categorical null coding")
            for feature in features[:category_count]:
                counts = {value: sum(row[feature.name] == value for row in training.rows) for value in ("0", "1", "2", "3")}
                expected = min(counts, key=lambda value: (-counts[value], value))
                require(transform.categories_[feature.reference][0] == expected, "categorical reference is not the fitting-row non-null mode")
                require(null_index in learned.columns_for((feature.reference,)), "categorical indicator alias lost source ownership")
            if numeric_missing:
                require(null_index in learned.columns_for((features[-1].reference,)), "numeric indicator alias lost categorical canonical ownership")
            reversed_design = FoldPreprocessor(tuple(reversed(features))).fit_transform(training.rows)
            require(
                np.linalg.matrix_rank(np.column_stack((np.ones(len(training.rows)), reversed_design.matrix))) == reversed_design.matrix.shape[1] + 1,
                "shared indicator repair depends on predictor order",
            )
        from scripts.training.models.elastic import ElasticRegressor

        features = (categories[0], *req.data.features)
        rows = tuple(
            {key: (None if key == "cmi" and row["kind_a"] is None else value) for key, value in row.items() if key != "kind_b"} for row in category_rows
        )
        median = float(np.median([row["cmi"] for row in rows if row["cmi"] is not None]))
        observed = np.asarray([7 + 2 * (median if row["cmi"] is None else row["cmi"]) + 3 * (row["cmi"] is None) for row in rows])
        design = FoldPreprocessor(features, forced=(req.cmi_reference,)).fit_transform(rows)
        with libraries():
            elastic = ElasticRegressor(100000.0, 0.5, (req.cmi_reference,), 73).fit(design, observed)
        require(np.allclose(elastic.predict(design), observed, atol=1e-9), "categorical canonical indicator penalized forced CMI")

    def nested_indicator_alias_ownership() -> None:
        features = (req.data.features[2], req.data.features[0], req.data.features[1])
        rows = tuple(dict(row, process=None, extra=None) if int(row["ccn"]) % 6 == 0 else dict(row) for row in req.data.rows)
        references = tuple(feature.reference for feature in features)
        changed = replace(
            req, data=replace(req.data, rows=rows, features=features), feature_lists={**req.feature_lists, "m1_ols": references, "m2_elastic_net": references}
        )
        with libraries():
            result = research_tests.nested_comparison(changed)
            fitted = suite.fit_slot(changed, "m1_ols")
            joint = research_tests.joint_significance(fitted.pipeline, req.process_references)
        require(result["extra_degrees_of_freedom"] == 2, "nested reduced design lost an aliased missing indicator")
        require(joint["numerator_degrees_of_freedom"] == 2, "joint process test lost an aliased missing indicator")

    def tuning_prepares_each_fold_once() -> None:
        calls: list[frozenset[str]] = []
        original = FoldPreprocessor.fit

        def tracked(self: Any, values: Any, y: Any = None) -> Any:
            calls.append(frozenset(row["spine_key"] for row in getattr(values, "rows", values)))
            return original(self, values, y)

        with libraries(), patch.object(FoldPreprocessor, "fit", tracked):
            result = suite.fit_slot(req, "m2_elastic_net")
        require(result.status == "fitted", "cached-fold tuning did not fit")
        training, holdout = req.data.split()
        folds = len({row["window_year"] for row in training.rows}) - 1
        require(len(calls) == folds + 1, "tuning refitted preprocessing for each grid option")
        require(calls[-1] == frozenset(training.keys), "final preprocessing did not refit all training rows")
        require(all(not (keys & set(holdout.keys)) for keys in calls), "holdout entered cached preprocessing")

    def excluded_controls_from_column_names() -> None:
        for column in ("c281__value__number", "c283__lower_limits__number", "e038__value__number"):
            for declared in ("", "C119"):
                feature = Feature("feature_matrix_hac:" + column, "number", declared)
                rows = tuple(dict(row, **{column: 0.1}) for row in req.data.rows)
                rejects(partial(Dataset, rows, (*req.data.features, feature), "HAI_1"), "excluded")

    def panel_effect_normalization() -> None:
        from types import SimpleNamespace

        training, _ = req.data.split()
        prepared_panel = FoldPreprocessor(training.features).fit_transform(training.rows)
        entities = tuple(row["ccn"] for row in training.rows)
        years = tuple(row["window_year"] for row in training.rows)
        names = ("intercept", *prepared_panel.names)
        matrix = np.column_stack((np.ones(len(training.rows)), prepared_panel.matrix))
        reference = panel_standin(training.y, matrix, entities, years, names)

        class Series:
            def __init__(self, values: Any, **kwargs: Any) -> None:
                self.values = values
                self.index = tuple(values) if isinstance(values, dict) else ()

            def to_dict(self) -> Any:
                return dict(self.values)

        class Effects:
            @property
            def iloc(self) -> Any:
                return self

            def __getitem__(self, key: Any) -> Any:
                return self

            def groupby(self, **kwargs: Any) -> Any:
                return self

            def mean(self) -> Any:
                return self

            def to_dict(self) -> Any:
                return {entity: reference.effects[entity] + reference.fixed["intercept"] for entity in set(entities)}

        class Native:
            def __init__(self, *args: Any, **kwargs: Any) -> None:
                pass

            def fit(self, **kwargs: Any) -> Any:
                return SimpleNamespace(nobs=len(training.rows), params=Series(reference.fixed), estimated_effects=Effects(), loglik=-10.0)

        fixture_effects = Effects().iloc[:, 0].groupby(level="ccn").mean().to_dict()
        require(set(fixture_effects) == set(entities), "panel effect fixture lost its native indexing interface")
        fake_pandas = SimpleNamespace(MultiIndex=SimpleNamespace(from_arrays=lambda *args, **kwargs: None), DataFrame=Series, Series=Series)
        fake_panel = SimpleNamespace(PanelOLS=Native, RandomEffects=Native)
        original_import = panel.importlib.import_module

        def load(name: str, package: Any = None) -> Any:
            return {"pandas": fake_pandas, "linearmodels.panel": fake_panel}.get(name) or original_import(name, package)

        with patch.object(panel.importlib, "import_module", side_effect=load):
            result = panel.native_fits(training.y, matrix, entities, years, names)
        beta = np.asarray([result.fixed[name] for name in names])
        predicted = matrix @ beta + np.asarray([result.effects[entity] for entity in entities])
        require(abs(float(np.mean(training.y - predicted))) < 1e-10, "panel hospital effects double-counted the intercept")
        with libraries(), patch.object(panel, "native_fits", return_value=result):
            fitted = panel.PanelRegressor().fit(prepared_panel, training.y)
        require(abs(float(np.mean(training.y - fitted.predict(prepared_panel)))) < 1e-10, "panel fitted residual mean is not zero")

    def result_has_aggregate_panel_counts() -> None:
        rows = tuple(row for row in req.data.rows if row["ccn"] != "990000" or row["window_year"] in (2020, 2023))
        with libraries(), patch.object(panel, "native_fits", side_effect=panel_standin):
            output = suite.fit_models(replace(req, data=replace(req.data, rows=rows)))
        payload = output.to_result()
        text = json.dumps(payload, sort_keys=True)
        require(all(row["ccn"] not in text for row in req.data.rows), "canonical result contains hospital identifiers")
        details = payload["fitted_parameters"]["m6_fixed_effects"]
        require(details["training_hospitals_by_window_count"] == {"3": 47}, "panel window histogram is wrong")
        require(details["dropped_singleton_hospital_count"] == 1, "panel singleton aggregate is wrong")

    def nested_and_joint_values() -> None:
        references = tuple(feature.reference for feature in req.data.features)
        changed = replace(
            req,
            feature_lists={**req.feature_lists, "m1_ols": references, "rq4_reduced": (req.process_references[0],)},
            process_references=(references[0], references[2]),
        )
        training, _ = changed.data.split()
        y = training.y
        columns = np.asarray([[1.0, row["process"], row["cmi"], row["extra"]] for row in training.rows])
        reduced = columns[:, [0, 1]]
        joint_reduced = columns[:, [0, 2]]
        full_error = y - columns @ np.linalg.lstsq(columns, y, rcond=None)[0]
        reduced_error = y - reduced @ np.linalg.lstsq(reduced, y, rcond=None)[0]
        full_ssr, reduced_ssr = float(full_error @ full_error), float(reduced_error @ reduced_error)
        degrees = len(y) - columns.shape[1]
        expected_f = ((reduced_ssr - full_ssr) / 2) / (full_ssr / degrees)
        expected_lr = len(y) * math.log(reduced_ssr / full_ssr)
        joint_error = y - joint_reduced @ np.linalg.lstsq(joint_reduced, y, rcond=None)[0]
        expected_joint_f = ((float(joint_error @ joint_error) - full_ssr) / 2) / (full_ssr / degrees)
        with libraries() as replacements:
            result = research_tests.nested_comparison(changed)
            fitted = suite.fit_slot(changed, "m1_ols")
            joint = research_tests.joint_significance(fitted.pipeline, changed.process_references)
        require(math.isclose(result["f_statistic"], expected_f, rel_tol=1e-10), "nested F differs from the independent sums of squares")
        require(math.isclose(result["likelihood_ratio"], expected_lr, rel_tol=1e-10), "nested likelihood ratio differs from the Gaussian likelihood")
        require(result["extra_degrees_of_freedom"] == 2, "nested numerator degrees of freedom changed")
        require(math.isclose(joint["f_statistic"], expected_joint_f, rel_tol=1e-10), "joint F differs from the independent sums of squares")
        require(joint["numerator_degrees_of_freedom"] == 2 and joint["denominator_degrees_of_freedom"] == degrees, "joint F degrees of freedom changed")
        require(
            replacements["scipy.stats"].f.calls == [(result["f_statistic"], 2, degrees), (joint["f_statistic"], 2, degrees)],
            "F tails received wrong numerator or residual degrees of freedom",
        )
        require(replacements["scipy.stats"].chi2.calls == [(result["likelihood_ratio"], 2)], "nested likelihood tail received wrong degrees of freedom")

    def mixed_values_and_unresolved_ownership() -> None:
        training, _ = req.data.split()
        matrix = np.asarray([[1.0, row["process"], row["cmi"], row["extra"]] for row in training.rows])
        residual = training.y - matrix @ np.linalg.lstsq(matrix, training.y, rcond=None)[0]
        ownership = np.asarray([row["control_type_code"] for row in training.rows])
        variance = max(float(np.var([np.mean(residual[ownership == group]) for group in sorted(set(ownership))])), 0.01)
        residual_variance = float(residual @ residual) / (len(residual) - matrix.shape[1])
        with libraries() as replacements:
            replacements["scipy.stats"].chi2 = type("Tail", (), {"sf": staticmethod(lambda statistic, df: 0.2 if statistic == 6 and df == 1 else -1)})
            result = suite.fit_slot(req, "m3_mixed")
        require(result.status == "fitted" and result.details["likelihood_ratio"] == 6.0, "mixed likelihood ratio differs from the supplied log likelihoods")
        require(math.isclose(result.details["icc"], variance / (variance + residual_variance), rel_tol=1e-10), "mixed ICC differs from the variance ratio")
        require(result.details["p_value"] == 0.1, "mixed likelihood test lost its half chi-squared boundary weight")
        with libraries():
            absent = replace(req.data, rows=(dict(req.data.rows[0], control_type_code=None), *req.data.rows[1:]))
            failed = suite.fit_slot(replace(req, data=absent), "m3_mixed")
        require(
            failed.status == "fitted" and failed.details["excluded_missing_ownership"] == {"training": 1, "holdout": 0, "total": 1},
            "missing ownership exclusion or count is wrong",
        )
        with libraries() as replacements:
            mixed = replacements["statsmodels.api"].MixedLM
            original_fit = mixed.fit

            def zero_variance(self: Any, **kwargs: Any) -> Any:
                fitted = original_fit(self, **kwargs)
                fitted.cov_re = np.asarray([[0.0]])
                return fitted

            with patch.object(mixed, "fit", zero_variance):
                boundary = suite.fit_slot(req, "m3_mixed")
        require(boundary.status == "not_converged" and "singular variance" in boundary.reason, "zero ownership variance changed an unresolved rule")

    convergence_warning = type("ConvergenceWarning", (UserWarning,), {})
    singular_warning = type("SingularMatrixWarning", (UserWarning,), {})
    unrelated_warning = type("UnrelatedNotice", (UserWarning,), {})
    scripted_warnings = {
        "warning": ("Retrying MixedLM optimization", convergence_warning),
        "both": ("Retrying MixedLM optimization", convergence_warning),
        "singular": ("Retrying MixedLM optimization", convergence_warning),
        "numeric": ("overflow in the likelihood", RuntimeWarning),
        "singularwarn": ("Covariance of the parameter estimates is singular", singular_warning),
        "unrelated": ("an unrelated notice", unrelated_warning),
        "future": ("an option was renamed", FutureWarning),
    }

    plain_sources = {
        "plain_statsmodels": ("/env/lib/python3.12/site-packages/statsmodels/regression/mixed_linear_model.py", "statsmodels.regression.mixed_linear_model"),
        "plain_other": ("/env/lib/python3.12/site-packages/other_library/thing.py", "other_library.thing"),
        "plain_project": ("/work/statsmodels/project/warning_fixture.py", "project.warning_fixture"),
    }

    def scripted_mixed(
        outcomes: dict[str, str], operation: Any = None, clock: list[float] | None = None, slow_build: bool = False
    ) -> tuple[Any, list[tuple[str, bool]]]:
        """Fit the mixed slot with one scripted outcome per optimizer; a "+late" suffix moves the controlled clock past every deadline."""
        calls: list[tuple[str, bool]] = []
        with libraries() as replacements:
            mixed = replacements["statsmodels.api"].MixedLM
            original_fit = mixed.fit

            def scripted(self: Any, **kwargs: Any) -> Any:
                calls.append((kwargs["method"], kwargs["reml"]))
                fitted = original_fit(self, **kwargs)
                outcome = outcomes[kwargs["method"]]
                late = outcome.endswith("+late")
                outcome = outcome.removesuffix("+late")
                fitted.converged = outcome not in ("unconverged", "both", "singular")
                if outcome in scripted_warnings:
                    warnings.warn(*scripted_warnings[outcome], stacklevel=2)
                if outcome in plain_sources:
                    # A plain UserWarning told apart only by the file and module it was recorded with.
                    warnings.warn_explicit("a plain notice", UserWarning, plain_sources[outcome][0], 1830, module=plain_sources[outcome][1])
                if late and clock is not None:
                    clock[0] += 10_000.0
                if outcome == "singular":
                    raise np.linalg.LinAlgError("Singular matrix")
                return fitted

            original_init = mixed.__init__

            def slow_init(self: Any, *args: Any, **kwargs: Any) -> None:
                original_init(self, *args, **kwargs)
                if slow_build and clock is not None:
                    clock[0] += 10_000.0

            with patch.object(mixed, "fit", scripted), patch.object(mixed, "__init__", slow_init):
                return suite.fit_slot(req if operation is None else operation, "m3_mixed"), calls

    def unclean_converged_attempts() -> None:
        # A fit that reports convergence but warned during the attempt (statsmodels' own warning classes or a RuntimeWarning) is not accepted.
        for outcome in ("singularwarn", "numeric", "warning"):
            moved, calls = scripted_mixed({"lbfgs": outcome, "bfgs": "clean"})
            require(
                moved.status == "fitted" and moved.details["optimizer"] == "bfgs" and [name for name, _ in calls] == ["lbfgs", "bfgs"],
                f"a converged lbfgs fit that warned ({outcome}) was accepted without trying bfgs: {moved.details.get('optimizer')} {calls}",
            )
        sourced, calls = scripted_mixed({"lbfgs": "plain_statsmodels", "bfgs": "clean"})
        require(
            sourced.status == "fitted" and sourced.details["optimizer"] == "bfgs" and len(calls) == 2,
            f"a plain UserWarning recorded from statsmodels made no attempt unclean: {sourced.details.get('optimizer')} {calls}",
        )
        stuck, calls = scripted_mixed({"lbfgs": "singularwarn", "bfgs": "numeric", "powell": "warning", "nm": "singularwarn"})
        require(stuck.status == "not_converged" and len(calls) == 4, f"four warned attempts were accepted or cut short: {stuck.status} {calls}")
        require(
            all(
                text in stuck.reason
                for text in (
                    "lbfgs: Covariance of the parameter estimates is singular",
                    "bfgs: overflow in the likelihood",
                    "powell: Retrying MixedLM optimization",
                    "nm: Covariance of the parameter estimates is singular",
                )
            ),
            f"the reason does not carry each attempt's first warning: {stuck.reason}",
        )
        for outcome in ("unrelated", "future", "plain_other", "plain_project"):
            kept, calls = scripted_mixed({"lbfgs": outcome})
            require(
                kept.status == "fitted" and kept.details["optimizer"] == "lbfgs" and len(calls) == 1,
                f"a warning from another library made a converged attempt unclean ({outcome}): {kept.status} {calls}",
            )

    def caller_warning_filters() -> None:
        # Recording warnings leaves the caller's own filters in force, whatever the category.
        for outcome, category in (("unrelated", unrelated_warning), ("future", FutureWarning), ("numeric", RuntimeWarning)):
            raised = False
            with warnings.catch_warnings():
                warnings.simplefilter("error", category)
                try:
                    scripted_mixed({"lbfgs": outcome, "bfgs": "clean"})
                except Warning as caught:
                    raised = isinstance(caught, category)
            require(raised, f"a {category.__name__} the caller made an error did not reach the caller")
        # A caller's ignore filter hides a warning from the caller, not from the optimizer classification.
        for outcome in ("singularwarn", "numeric", "warning", "plain_statsmodels"):
            for category in (Warning, scripted_warnings.get(outcome, ("", UserWarning))[1]):
                with warnings.catch_warnings():
                    warnings.simplefilter("ignore", category)
                    moved, calls = scripted_mixed({"lbfgs": outcome, "bfgs": "clean"})
                require(
                    moved.status == "fitted" and moved.details["optimizer"] == "bfgs" and len(calls) == 2,
                    f"a caller's ignore filter on {category.__name__} let a warned lbfgs fit ({outcome}) be accepted: {moved.details.get('optimizer')} {calls}",
                )
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            kept, calls = scripted_mixed({"lbfgs": "unrelated"})
        require(kept.status == "fitted" and len(calls) == 1, f"an ignored unrelated warning changed the sequence: {kept.status} {calls}")

    def mixed_optimizer_sequence() -> None:
        first, calls = scripted_mixed({"lbfgs": "clean"})
        require(first.status == "fitted" and first.details["optimizer"] == "lbfgs", "a clean first optimizer was not recorded")
        require(calls == [("lbfgs", False)], f"a converged first optimizer still tried others: {calls}")
        require(first.details["reml"] is False, "the mixed model left maximum likelihood")
        unchanged = {key: value for key, value in first.details.items() if key != "optimizer"}
        later_plans = {
            "bfgs": {"lbfgs": "unconverged", "bfgs": "clean"},
            "powell": {"lbfgs": "warning", "bfgs": "both", "powell": "clean"},
            "nm": {"lbfgs": "both", "bfgs": "unconverged", "powell": "warning", "nm": "clean"},
        }
        for optimizer, plan in later_plans.items():
            result, calls = scripted_mixed(plan)
            require(
                result.status == "fitted" and result.details["optimizer"] == optimizer, f"{optimizer} was not accepted after earlier failed attempts: {result}"
            )
            require(calls == [(name, False) for name in list(plan)], f"optimizers ran out of order or in ML mode: {calls}")
            require(
                {key: value for key, value in result.details.items() if key != "optimizer"} == unchanged,
                "the likelihood-ratio, ICC and variance details changed with the optimizer",
            )
        failed, calls = scripted_mixed({"lbfgs": "both", "bfgs": "warning", "powell": "unconverged", "nm": "both"})
        require(failed.status == "not_converged", "a mixed model with every optimizer failing was accepted")
        require(all(name in failed.reason for name in ("lbfgs", "bfgs", "powell", "nm")), f"the reason does not name the optimizers tried: {failed.reason}")
        require(
            "bfgs: Retrying MixedLM optimization" in failed.reason and "powell: not converged" in failed.reason,
            f"the reason does not tell a warned attempt from an unconverged one: {failed.reason}",
        )
        require([name for name, _ in calls] == ["lbfgs", "bfgs", "powell", "nm"], f"not every optimizer was tried once: {calls}")
        rescued, calls = scripted_mixed({"lbfgs": "singular", "bfgs": "clean"})
        require(
            rescued.status == "fitted" and rescued.details["optimizer"] == "bfgs",
            f"a singular matrix in one optimizer stopped the sequence before a clean fit: {rescued}",
        )
        stuck, _calls = scripted_mixed({"lbfgs": "singular", "bfgs": "singular", "powell": "singular", "nm": "singular"})
        require(
            stuck.status == "not_converged" and "lbfgs: Retrying MixedLM optimization" in stuck.reason,
            f"a singular matrix in every optimizer is not reported with each attempt's warning: {stuck}",
        )
        require(
            stuck.reason.count("LinAlgError: Singular matrix") == 4,
            f"the exception text that follows a warning is missing from an attempt's reason: {stuck.reason}",
        )
        unclean_converged_attempts()
        caller_warning_filters()

    def mixed_optimizer_deadline() -> None:
        from scripts.training.models import common

        def run(plan: dict[str, str], before_first: bool = False, slow_build: bool = False) -> tuple[Any, list[tuple[str, bool]]]:
            clock = [1000.0]
            operation = replace(req, deadline=Deadline(clock[0] + 600.0))
            original = FoldPreprocessor.fit_transform

            def slow(self: Any, *args: Any, **kwargs: Any) -> Any:
                design = original(self, *args, **kwargs)
                clock[0] += 10_000.0 if before_first else 0.0
                return design

            with patch.object(common.time, "monotonic", lambda: clock[0]), patch.object(FoldPreprocessor, "fit_transform", slow):
                return scripted_mixed(plan, operation, clock, slow_build)

        within, calls = run({"lbfgs": "clean", "bfgs": "clean"})
        require(within.status == "fitted" and calls == [("lbfgs", False)], f"a fit inside its deadline changed: {within.status} {calls}")
        for name, plan, expected in (
            ("an exception that ends after the deadline", {"lbfgs": "singular+late", "bfgs": "clean"}, ["lbfgs"]),
            ("an unconverged attempt that ends after the deadline", {"lbfgs": "unconverged+late", "bfgs": "clean"}, ["lbfgs"]),
            ("a warned attempt that ends after the deadline", {"lbfgs": "singularwarn+late", "bfgs": "clean"}, ["lbfgs"]),
            (
                "an exception from the last optimizer that ends after the deadline",
                {"lbfgs": "unconverged", "bfgs": "unconverged", "powell": "unconverged", "nm": "singular+late"},
                ["lbfgs", "bfgs", "powell", "nm"],
            ),
        ):
            late, calls = run(plan)
            require(late.status == "not_run" and late.reason == "deadline", f"{name} was not reported as not_run: {late.status} {late.reason}")
            require([method for method, _ in calls] == expected, f"{name} still started another optimizer: {calls}")
        built, calls = run({"lbfgs": "clean"}, slow_build=True)
        require(
            built.status == "not_run" and built.reason == "deadline" and calls == [],
            f"a fit started after the deadline passed during model construction: {built.status} {calls}",
        )
        expired, calls = run({"lbfgs": "clean"}, before_first=True)
        require(expired.status == "not_run" and calls == [], f"an optimizer started after the deadline had expired: {expired.status} {calls}")

    def mediation_known_values() -> None:
        x = np.asarray([row["process"] for row in req.data.rows])
        cmi = np.asarray([row["cmi"] for row in req.data.rows])
        base = np.column_stack((np.ones(len(x)), x, cmi))
        rng = np.random.default_rng(538)
        noise = rng.normal(size=len(x))
        noise -= base @ np.linalg.lstsq(base, noise, rcond=None)[0]
        mediator_values = 0.8 * x + 0.2 * cmi + noise * 0.3
        augmented = np.column_stack((base, mediator_values))
        noise = rng.normal(size=len(x))
        noise -= augmented @ np.linalg.lstsq(augmented, noise, rcond=None)[0]
        outcome = 1.7 + 0.3 * x + 0.4 * cmi + 1.5 * mediator_values + noise * 0.05
        data = replace(req.data, rows=tuple(dict(row, **{req.data.label: float(value)}) for row, value in zip(req.data.rows, outcome, strict=True)))
        mediator_rows = tuple(
            {
                "spine_key": row["spine_key"],
                "measure": name,
                "value_number": float(value),
                "alignment_status": "aligned",
                "period_end": date(row["window_year"] - 1, 12, 31),
            }
            for row, value in zip(data.rows, mediator_values, strict=True)
            for name in ("PSI_90", "PSI_13")
        )
        with libraries():
            result = mediation.analyze_mediation(data, mediator_rows, req.process_references, (req.cmi_reference,), req.config, Deadline.after(30))
        for fitted in result["mediators"].values():
            effect = fitted["effects"]["process"]
            for name, expected in (("indirect_effect", 1.2), ("direct_effect", 0.3), ("total_effect", 1.5)):
                require(math.isclose(effect[name], expected, abs_tol=1e-9), f"mediation {name} differs from its planted coefficient product")
            require(effect["interval"][0] < 1.2 < effect["interval"][1], "mediation interval missed its planted indirect effect")

    def moran_island_values_and_work() -> None:
        _, holdout = req.data.split()
        rows = (*holdout.rows[:-1], dict(holdout.rows[-1], county_fips="99999"))
        graph = spatial.build_weights(rows, ())
        values = np.asarray([int(row["county_fips"]) % 2 for row in rows], dtype=float)
        active_rows = rows[:-1]
        weights = np.asarray(
            [[float(i != j and left["county_fips"] == right["county_fips"]) for j, right in enumerate(active_rows)] for i, left in enumerate(active_rows)]
        )
        weights /= weights.sum(axis=1, keepdims=True)
        centered = values[:-1] - np.mean(values[:-1])
        expected = float(centered @ weights @ centered / (centered @ centered))
        calls = 0
        original = vars(spatial.Weights)["islands"].fget

        def counted(self: Any) -> Any:
            nonlocal calls
            calls += 1
            return original(self)

        with libraries(), patch.object(spatial.Weights, "islands", property(counted)):
            result = spatial.moran_test(values, graph.keys, graph, 731, 99, Deadline.after(10))
        require(result["included_hospitals"] == 47 and result["islands"] == 1, "Moran included an island")
        require(math.isclose(result["statistic"], expected, abs_tol=1e-12), "Moran with islands differs from the dense reference")
        require(result["p_value"] == 0.01, "Moran permutation tail differs from the seeded island pattern")
        require(calls <= 2, "Moran recomputed the island set per hospital")
        random = np.random.default_rng(224).normal(size=len(rows))
        with libraries():
            null = spatial.moran_test(random, graph.keys, graph, 19, 999, Deadline.after(10))
        require(null["p_value"] > 0.05, "random spatial residuals falsely passed the gate")

    def optional_analysis_failures() -> None:
        from scripts.training.models.common import DeadlineExceeded, NotEstimable

        for owner, name, key in (
            (research_tests, "joint_significance", "joint_process"),
            (research_tests, "intercept_cmi_comparison", "process_vs_intercept_cmi"),
            (suite, "shap_values", "shap"),
        ):
            with (
                libraries(),
                patch.object(panel, "native_fits", side_effect=panel_standin),
                patch.object(owner, name, side_effect=NotEstimable("synthetic optional failure")),
            ):
                result = suite.fit_models(req)
            report = result.shap if key == "shap" else result.research[key]
            require(
                report["status"] == "not_estimable" and "synthetic optional failure" in report["reason"], "optional analysis failure discarded the fitted slots"
            )
            require(result.slots["m1_ols"].status == "fitted" and result.candidate is not None, "optional failure lost the fitted candidate")
            with (
                libraries(),
                patch.object(panel, "native_fits", side_effect=panel_standin),
                patch.object(owner, name, side_effect=DeadlineExceeded("synthetic deadline")),
            ):
                rejects(lambda: suite.fit_models(req), "synthetic deadline")

    def mediation_prepares_source_once() -> None:
        from scripts.training.models import preprocessing as preparation

        rows = tuple(
            {
                "spine_key": row["spine_key"],
                "measure": name,
                "value_number": row["process"] * 0.8 + row["extra"] * 0.2,
                "alignment_status": "aligned",
                "period_end": date(row["window_year"] - 1, 12, 31),
            }
            for row in req.data.rows
            for name in ("PSI_90", "PSI_13")
        )
        calls = 0
        original = preparation.records

        def counted(value: Any) -> Any:
            nonlocal calls
            calls += 1
            return original(value)

        sizes = []
        tolerances = []
        original_solve = np.linalg.lstsq

        def solve(matrix: Any, responses: Any, **kwargs: Any) -> Any:
            sizes.append(len(matrix))
            tolerances.append(kwargs["rcond"])
            return original_solve(matrix, responses, **kwargs)

        with libraries(), patch.object(preparation, "records", counted), patch.object(np.linalg, "lstsq", solve):
            output = mediation.analyze_mediation(req.data, rows, req.process_references, (req.cmi_reference,), req.config, Deadline.after(30))
        require(all(result["status"] == "fitted" for result in output["mediators"].values()), "cached mediation did not fit")
        require(1 <= calls <= 2, "mediation repeated source conversion in bootstrap draws")
        require(len(sizes) == 2 * (req.config["bootstrap"]["mediation"]["draws"] + 1), "mediation changed its bootstrap draw count")
        require(sum(size == len(req.data.rows) for size in sizes) == 2, "mediation repeated duplicate bootstrap rows in its solver")
        require(all(value == np.finfo(np.float64).eps * len(req.data.rows) for value in tolerances), "mediation compression changed the rank cutoff")
        original_products = mediation._products

        def repeated_rows(*args: Any) -> Any:
            return original_products(args[0], args[1], args[2], args[3], args[4], args[5], None, args[7], *args[8:])

        with patch.object(mediation, "_products", repeated_rows):
            reference = mediation.analyze_mediation(req.data, rows, req.process_references, (req.cmi_reference,), req.config, Deadline.after(30))
        for name in ("PSI_90", "PSI_13"):
            actual, expected = output["mediators"][name], reference["mediators"][name]
            require(actual["bootstrap"] == expected["bootstrap"], "mediation compression changed resampling metadata")
            require(actual["status"] == expected["status"], "mediation compression changed estimability")
            for key in actual["effects"]:
                for field in ("indirect_effect", "direct_effect", "total_effect", "interval"):
                    require(
                        np.allclose(actual["effects"][key][field], expected["effects"][key][field], rtol=1e-11, atol=1e-12),
                        "mediation compression changed an estimate or interval",
                    )

    def kept_scenario(model: str) -> tuple[Any, tuple[str, ...], dict[str, Feature]]:
        """A request whose ``model`` list holds a complete constant, an exact duplicate, two predictors with one missing pattern (one constant where
        present), an all-missing one, a 60 % missing one and the ownership column. The fixed-effects model fits complete cases, so its list leaves out the two
        predictors that are missing in most or all rows."""
        names = ("constant", "duplicate", "twin_a", "twin_b", "dead", "sparse")
        found = {name: Feature(f"feature_matrix_hospital:{name}", "number") for name in names}
        found["group"] = Feature("feature_matrix_hospital_window:control_type_code", "category", family="hospital_window")
        rows = tuple(
            dict(
                row,
                constant=7.0,
                duplicate=row["process"],
                twin_a=None if index % 5 == 0 else 0.5 * row["extra"] + float(np.sin(index)),
                twin_b=None if index % 5 == 0 else 7.0,
                dead=None,
                sparse=None if index % 5 < 3 else 1.0 + index % 7,
            )
            for index, row in enumerate(req.data.rows)
        )
        data = replace(req.data, rows=rows, features=(*req.data.features, *found.values()))
        base = tuple(feature.reference for feature in req.data.features)
        middle = ("constant", "duplicate", "twin_a", "twin_b") + (("dead", "sparse") if model != "m6" else ())
        listed = (*base, *(found[name].reference for name in middle), found["group"].reference)
        slot = MODEL_SLOT[model]
        return replace(req, data=data, feature_lists={**req.feature_lists, slot: listed}), listed, found

    def fitted_model_suite(model: str, changed: Any) -> Any:
        """Run the real per-model fit. The spatial test is a declared stand-in (significant) so that m5 reaches its fit; native libraries are stand-ins."""
        significant = {"status": "fitted", "p_value": 0.01, "statistic": 0.0, "islands": 0}
        with libraries(), patch.object(panel, "native_fits", side_effect=panel_standin), patch.object(suite, "moran_test", return_value=significant):
            return suite.fit_model(changed, model)

    def bundle_feature_list_follows_the_fitted_design() -> None:
        from scripts.training import bundle

        for model in MODEL_SLOT:
            slot = MODEL_SLOT[model]
            changed, listed, found = kept_scenario(model)
            fitted = fitted_model_suite(model, changed)
            result = fitted.slots[slot]
            require(result.status == "fitted", f"{model} did not fit: {result.reason}")
            document = bundle.feature_list_document(fitted)
            kept = document["kept_references"]
            # The expectation comes from the fitted transform: the columns it still emits, and the sources of the missing indicators they share.
            design = result.pipeline.named_steps["prepare"].transform(changed.data.split()[0].rows)
            shared = {reference for name, aliases in design.reference_aliases.items() if name in design.names for reference in aliases}
            expected = {reference for reference in listed if reference in design.references or reference in shared}
            if model == "m6":
                panel_columns = set(result.pipeline.named_steps["model"].kept_indices_)
                expected = {reference for reference in expected if panel_columns.intersection(design.columns_for([reference]))}
            if model == "m3":
                expected.add(found["group"].reference)
            require(kept == [reference for reference in listed if reference in expected], f"{model}: the kept list is not the fitted design's sources: {kept}")
            require(document["kept_columns"] == [reference.split(":", 1)[-1] for reference in kept], f"{model}: kept_columns do not follow kept_references")
            require(document["fitted_slot"] == slot, f"{model}: the fitted slot is {document['fitted_slot']}")
            require(
                (document["slot"], document["references"]) == ((slot, list(listed)) if slot in REGISTRABLE else (None, [])),
                f"{model}: the registry keys changed",
            )
            require(document["columns"] == ([reference.split(":", 1)[-1] for reference in listed] if slot in REGISTRABLE else []), f"{model}: columns changed")
            require(found["constant"].reference not in kept, f"{model}: a complete constant predictor is in the kept list")
            require((found["duplicate"].reference in kept) == (model not in ("m1", "m3", "m5", "m6")), f"{model}: an exact duplicate is listed wrongly")
            require(
                (found["twin_b"].reference in kept) == (model != "m6"),
                f"{model}: a predictor that survives only through a shared missing indicator is listed wrongly",
            )
            require(found["twin_a"].reference in kept, f"{model}: a used predictor is missing from the kept list")
            require(
                (found["group"].reference in kept) == (model != "m6"),
                f"{model}: the ownership column is listed wrongly (the hospital effects of m6 absorb it, so its panel does not use it)",
            )
            if model == "m6":
                require(any(name.startswith("control_type_code") for name in result.details["absorbed_columns"]), "m6: the ownership column was not absorbed")
            if model != "m6":
                require(found["dead"].reference not in kept and found["sparse"].reference not in kept, f"{model}: a dropped predictor is in the kept list")
            require(
                set(result.details["selected_references"]) >= set(kept) - {found["group"].reference},
                f"{model}: the kept list holds a source the selector dropped",
            )

    def an_unfitted_run_writes_no_kept_list() -> None:
        from scripts.training import bundle

        # A fixed-effects run with no repeated hospital ends not_estimable. The run is unfitted, which the bundle must say apart from a fitted model that is
        # not registrable.
        base = request(years=range(2019, 2023), hospitals=120)
        single = replace(
            base,
            data=replace(
                base.data, rows=tuple(row for row in base.data.rows if row["window_year"] == 2022 or row["window_year"] == 2019 + int(row["ccn"]) % 3)
            ),
        )
        fitted = fitted_model_suite("m6", single)
        require(fitted.slots["m6_fixed_effects"].status == "not_estimable", f"expected an unfitted m6, got {fitted.slots['m6_fixed_effects'].status}")
        require(bundle.feature_list_document(fitted) == EMPTY_LIST, f"an unfitted run lists {bundle.feature_list_document(fitted)}")
        spatial_run = replace(req, adjacency=())
        with libraries(), patch.object(suite, "moran_test", return_value={"status": "not_estimable", "reason": "stand-in", "islands": 0}):
            unfitted_spatial = suite.fit_model(spatial_run, "m5")
        require(bundle.feature_list_document(unfitted_spatial) == EMPTY_LIST, "an m5 run whose spatial test is not significant lists features")
        require(bundle.feature_list_document(suite.ModelSuite(req, {}, None, None, {}, {})) == EMPTY_LIST, "a suite with no slots lists features")

    def mediation_frozen_category_contrasts() -> None:
        rng = np.random.default_rng(61827)
        categories = np.asarray(["A"] * 480 + ["B"] * 480 + ["C"] * 480 + [None] * 160, dtype=object)
        rng.shuffle(categories)
        numeric = rng.normal(size=(1600, 3))
        features = (Feature("synthetic:exposure", "category"), Feature("synthetic:adjustment", "number"))
        rows = tuple(
            {
                "spine_key": f"{990000 + index // 4}:{2020 + index % 4}",
                "ccn": str(990000 + index // 4),
                "window_year": 2020 + index % 4,
                "exposure": value,
                "adjustment": None if index % 9 == 0 else float(numeric[index, 0]),
                "hai_1__sir__number": float(0.6 * (value == "B") - 0.4 * (value == "C") + numeric[index, 0] + numeric[index, 2]),
            }
            for index, value in enumerate(categories)
        )
        mediator_rows = tuple(
            {
                "spine_key": row["spine_key"],
                "measure": name,
                "value_number": float(0.8 * (row["exposure"] == "B") - 0.5 * (row["exposure"] == "C") + numeric[index, 1]),
                "period_end": date(row["window_year"] - 1, 12, 31),
                "alignment_status": "aligned",
            }
            for index, row in enumerate(rows)
            for name in ("PSI_90", "PSI_13")
        )
        encodings = []
        medians = []
        original_fit = FoldPreprocessor.fit

        def recorded_fit(self: Any, values: Any, y: Any = None) -> Any:
            result = original_fit(self, values, y)
            encodings.append(dict(self.categories_))
            medians.append(self.medians_[features[1].reference])
            return result

        with patch.object(FoldPreprocessor, "fit", recorded_fit):
            output = mediation.analyze_mediation(
                Dataset(rows, features, "HAI_1"), mediator_rows, (features[0].reference,), (features[1].reference,), req.config, Deadline.after(30)
            )
        require(all(result["status"] == "fitted" for result in output["mediators"].values()), "categorical mediation did not fit")
        require(len(encodings) >= 2 * (req.config["bootstrap"]["mediation"]["draws"] + 1), "categorical bootstrap did not refit numeric preprocessing")
        require(all(value == encodings[0] for value in encodings), "mediation categorical reference or level set changed across draws")
        require(len(set(medians)) > 2, "mediation froze numeric imputation across draws")

        def uncached(self: FeatureBatch, levels: Any) -> FeatureBatch:
            return FeatureBatch(self.rows, self.columns)

        with patch.object(FeatureBatch, "encode_categories", uncached):
            uncached_output = mediation.analyze_mediation(
                Dataset(rows, features, "HAI_1"), mediator_rows, (features[0].reference,), (features[1].reference,), req.config, Deadline.after(30)
            )
        require(output == uncached_output, "frozen category codes changed a mediation estimate or interval")
        reference = FoldPreprocessor(features, forced=(features[1].reference,)).fit(rows)
        names = reference.transform(rows).names
        for omitted in ("A", "B", "C", None):
            sample = tuple(row for row in rows if row["exposure"] != omitted)
            fitted = FoldPreprocessor(features, forced=(features[1].reference,), category_levels=reference.categories_)
            design = fitted.fit_transform(sample)
            require(fitted.categories_ == reference.categories_, "absent categorical level changed the frozen encoding")
            require(design.names == names, "absent categorical level removed a frozen column")
            for index, category in enumerate(reference.categories_[features[0].reference][1:], 1):
                column = "exposure__missing" if category is None else f"exposure__category_{index}"
                expected = np.asarray([row["exposure"] == category for row in sample], dtype=float)
                require(np.array_equal(design.matrix[:, design.names.index(column)], expected), "frozen category column changed its level identity")

        for kinds in (("category", "category"), ("category", "number"), ("number", "category")):
            shared_features = tuple(
                Feature(f"synthetic:shared_{index}", "category") if kind == "category" else Feature(f"synthetic:shared_{index}", "number")
                for index, kind in enumerate(kinds)
            )
            shared_rows = tuple(
                {
                    feature.name: None if index % 10 == 0 else str((index // (column + 1)) % 3) if feature.kind == "category" else float(index % 17)
                    for column, feature in enumerate(shared_features)
                }
                for index in range(120)
            )
            full = FoldPreprocessor(shared_features).fit(shared_rows)
            expected_names = full.transform(shared_rows).names
            no_nulls = tuple(row for row in shared_rows if row[shared_features[0].name] is not None)
            shared_design = FoldPreprocessor(shared_features, category_levels=full.categories_, indicator_aliases=full.indicator_aliases_).fit_transform(
                no_nulls
            )
            require(shared_design.names == expected_names, "absent null level changed frozen shared indicator columns")

    def model_action_budgets() -> None:
        from orchestration.training_contract import ACTION_SECONDS, PROMOTION_STEPS, TRAINING_STEPS, action_environment, parameters_for

        parameters = {"features_run": "latest", "population": "primary", "hai_types": ["HAI_1"], "trigger": "manual", "mlflow": "local", "model": "m2"}
        require(parameters_for("training_dag", parameters) == parameters, "local analysis parameters changed at the action boundary")
        for action in ("mediation", "hac_validate"):
            require(action in TRAINING_STEPS and action not in PROMOTION_STEPS, "analysis action belongs to another workflow")
            require(action_environment("training_job", action, "real") == "offline", "analysis action lost its offline execution boundary")
        require(ACTION_SECONDS["mediation"] == 3000, "mediation step budget must be 3000 seconds")
        require(ACTION_SECONDS["hac_validate"] == 1800, "HAC step budget must stay 1800 seconds")
        configuration = json.loads((Path(__file__).resolve().parents[2] / "config/training/models.json").read_bytes())
        require(configuration["bootstrap"]["mediation"]["draws"] == 5000, "mediation production draw count changed")

    def mediation_attempted_drop_counts() -> None:
        covariate = Feature("synthetic:rare_group", "category")
        exposure = Feature("synthetic:exposure", "category")
        rows = tuple(
            dict(
                row,
                rare_group="Y" if index < 4 else "Z" if index < 8 else "A" if index % 2 else "B",
                exposure="Z" if 8 <= index < 12 else "A" if (index // 4) % 2 else "B",
            )
            for index, row in enumerate(req.data.rows)
        )
        mediator_rows = tuple(
            {
                "spine_key": row["spine_key"],
                "measure": name,
                "value_number": row["process"] * 0.8 + row["extra"] * 0.2,
                "period_end": date(row["window_year"] - 1, 12, 31),
                "alignment_status": "aligned",
            }
            for row in rows
            for name in mediation.MEDIATORS
        )
        for rare_process in (False, True):
            features = (*req.data.features, covariate, exposure)
            processes = (*req.process_references, exposure.reference) if rare_process else req.process_references
            data = replace(req.data, rows=rows, features=features)
            output = mediation.analyze_mediation(data, mediator_rows, processes, (req.cmi_reference, covariate.reference), req.config, Deadline.after(30))
            for name, result in output["mediators"].items():
                draws = req.config["bootstrap"]["mediation"]["draws"]
                seed = seed_for(req.config["master_seed"], data.hai_type, "mediation_" + name)
                omitted = [
                    (not np.any(positions < 4), not np.any((positions >= 4) & (positions < 8)), not np.any((positions >= 8) & (positions < 12)))
                    for positions in research_tests.hospital_resamples(tuple(row["ccn"] for row in rows), draws, seed)
                ]
                expected_dropped = sum(left or right for left, right, _ in omitted)
                require(sum(left and right for left, right, _ in omitted) > 0, "counter fixture never dropped two covariate columns")
                expected_invalid = sum(process for _, _, process in omitted) if rare_process else 0
                require(
                    result["status"] == "fitted" and result["bootstrap"]["invalid_draws"] == expected_invalid, "counter fixture has unexpected invalid draws"
                )
                if rare_process:
                    require(
                        sum((left or right) and process for left, right, process in omitted) > 0, "counter fixture never dropped a column in an invalid draw"
                    )
                require(
                    result["bootstrap"]["draws_with_dropped_covariate_columns"] == expected_dropped,
                    "dropped-covariate counter must count attempted draws rather than columns or valid draws",
                )

    def nullable_ownership_stub() -> None:
        with patch.dict(sys.modules, {"pandas": SimpleNamespace(NA=object())}):
            missing_ownership_policy()

    def mediation_rare_covariate_levels() -> None:
        covariate = Feature("synthetic:rare_group", "category")
        for rare in ("Z", None):
            rows = tuple(dict(row, rare_group=rare if index < 4 else "A" if index % 2 else "B") for index, row in enumerate(req.data.rows))
            data = replace(req.data, rows=rows, features=(*req.data.features, covariate))
            mediator_rows = tuple(
                {
                    "spine_key": row["spine_key"],
                    "measure": name,
                    "value_number": row["process"] * 0.8 + row["extra"] * 0.2,
                    "period_end": date(row["window_year"] - 1, 12, 31),
                    "alignment_status": "aligned",
                }
                for row in rows
                for name in mediation.MEDIATORS
            )
            output = mediation.analyze_mediation(
                data, mediator_rows, req.process_references, (req.cmi_reference, covariate.reference), req.config, Deadline.after(30)
            )
            for name, result in output["mediators"].items():
                seed = seed_for(req.config["master_seed"], data.hai_type, "mediation_" + name)
                draws = req.config["bootstrap"]["mediation"]["draws"]
                absent = sum(not np.any(positions < 4) for positions in research_tests.hospital_resamples(tuple(row["ccn"] for row in rows), draws, seed))
                require(result["status"] == "fitted" and result["bootstrap"]["invalid_draws"] == 0, "absent covariate level invalidated a mediation draw")
                require(0 < absent < draws, "rare-covariate fixture did not exercise both hospital draws")
                require(result["bootstrap"]["draws_with_dropped_covariate_columns"] == absent, "dropped-covariate draw count is wrong")
                require(set(result["effects"]) == {"process"}, "covariate removal changed process coefficient identity")
                matrix = np.column_stack(
                    (
                        np.ones(len(rows)),
                        [row["process"] for row in rows],
                        [row["cmi"] for row in rows],
                        [row["rare_group"] == "B" for row in rows],
                        [index < 4 for index in range(len(rows))],
                    )
                )
                mediator = np.asarray([row["value_number"] for row in mediator_rows if row["measure"] == name])
                coefficients = np.linalg.lstsq(matrix, np.column_stack((mediator, data.y)), rcond=None)[0]
                residuals = np.column_stack((mediator, data.y)) - matrix @ coefficients
                b = residuals[:, 0] @ residuals[:, 1] / (residuals[:, 0] @ residuals[:, 0])
                expected = coefficients[1, 0] * b
                require(
                    np.isclose(result["effects"]["process"]["indirect_effect"], expected, rtol=1e-11, atol=1e-12),
                    "covariate removal shifted a process coefficient",
                )

    def mediation_missing_process_level() -> None:
        exposure = Feature("synthetic:exposure", "category")
        widths: list[int] = []
        original_solve = np.linalg.lstsq

        def recorded_solve(matrix: Any, responses: Any, **kwargs: Any) -> Any:
            widths.append(matrix.shape[1])
            return original_solve(matrix, responses, **kwargs)

        for rare in ("Z", None):
            rows = tuple(dict(row, exposure=rare if index < 4 else "A" if index % 2 else "B") for index, row in enumerate(req.data.rows))
            data = replace(req.data, rows=rows, features=(*req.data.features, exposure))
            mediator_rows = tuple(
                {
                    "spine_key": row["spine_key"],
                    "measure": name,
                    "value_number": row["process"] * 0.8 + row["extra"] * 0.2,
                    "period_end": date(row["window_year"] - 1, 12, 31),
                    "alignment_status": "aligned",
                }
                for row in rows
                for name in mediation.MEDIATORS
            )
            widths.clear()
            with patch.object(np.linalg, "lstsq", recorded_solve):
                output = mediation.analyze_mediation(data, mediator_rows, (exposure.reference,), (req.cmi_reference,), req.config, Deadline.after(30))
            require(set(widths) == {4}, "absent process level was removed from the frozen design")
            for name, result in output["mediators"].items():
                seed = seed_for(req.config["master_seed"], data.hai_type, "mediation_" + name)
                draws = req.config["bootstrap"]["mediation"]["draws"]
                absent = sum(not np.any(positions < 4) for positions in research_tests.hospital_resamples(tuple(row["ccn"] for row in rows), draws, seed))
                require(
                    result["status"] == "fitted" and result["bootstrap"]["invalid_draws"] == absent,
                    "absent process level was dropped instead of invalidating its draw",
                )
                require(result["bootstrap"]["draws_with_dropped_covariate_columns"] == 0, "process column was counted as a dropped covariate")

    def mediation_frozen_indicator_aliases() -> None:
        rows = tuple(dict(row, cmi=None, extra=None) if index < 4 else row for index, row in enumerate(req.data.rows))
        data = replace(req.data, rows=rows)
        mediator_rows = tuple(
            {
                "spine_key": row["spine_key"],
                "measure": name,
                "value_number": row["process"] * 0.8 + req.data.rows[index]["extra"] * 0.2 + 0.1 * np.sin(index),
                "period_end": date(row["window_year"] - 1, 12, 31),
                "alignment_status": "aligned",
            }
            for index, row in enumerate(rows)
            for name in mediation.MEDIATORS
        )
        aliases = []
        original = FoldPreprocessor.fit

        def recorded(self: Any, values: Any, y: Any = None) -> Any:
            result = original(self, values, y)
            aliases.append(dict(self.indicator_aliases_))
            return result

        with patch.object(FoldPreprocessor, "fit", recorded):
            output = mediation.analyze_mediation(
                data, mediator_rows, req.process_references, (req.cmi_reference, req.data.features[2].reference), req.config, Deadline.after(30)
            )
        require(
            len(aliases) == 2 * (req.config["bootstrap"]["mediation"]["draws"] + 2) and all(value == {"extra__missing": "cmi__missing"} for value in aliases),
            "mediation did not freeze shared indicator aliases across draws",
        )
        require(
            all(result["status"] == "fitted" and result["bootstrap"]["invalid_draws"] == 0 for result in output["mediators"].values()),
            "frozen shared-indicator mediation did not fit every draw",
        )

    def native_shap_failure() -> None:
        class ExplainerFailure(Exception):
            pass

        try:
            with (
                libraries() as replacements,
                patch.object(panel, "native_fits", side_effect=panel_standin),
                patch.object(replacements["shap.utils._exceptions"], "ExplainerError", ExplainerFailure),
                patch.object(replacements["shap"], "TreeExplainer", side_effect=ExplainerFailure("synthetic explainer failure")),
            ):
                output = suite.fit_models(req)
        except ExplainerFailure as error:
            raise AssertionError("native SHAP failure discarded the fitted slots") from error
        require(output.shap["status"] == "not_estimable" and output.candidate is not None, "native SHAP failure lost its explicit status or candidate")

    def configured_shap_sample() -> None:
        config = {**req.config, "shap": {"sample_cap": 7}}
        with libraries(), patch.object(panel, "native_fits", side_effect=panel_standin):
            output = suite.fit_models(replace(req, config=config))
        require(output.shap["sample_count"] == 7 and output.shap["sample_cap"] == 7, "SHAP ignored the configured sample cap")

    def approved_nested_policy() -> None:
        changed = replace(req, feature_lists={**req.feature_lists, "rq4_reduced": (req.process_references[0],)})
        with libraries():
            result = research_tests.nested_comparison(changed)
        require(result["status"] == "fitted" and result["cmi_in_reduced"] is False, "CMS-only nested comparison did not fit without CMI")
        require(req.cmi_reference in result["full_references"], "nested full model lost CMI")
        invalid = [req.data.features[1], req.data.features[2]]
        invalid.extend(
            Feature(f"feature_matrix_hospital_window:{name}", "number", family="hospital_window") for name in ("cmi_cases", "has_cmi", "is_cmi_held")
        )
        invalid.extend(
            (Feature("feature_matrix_cost_report:cost", "number", family="cost_report"), Feature("feature_matrix_county:svi", "number", family="county"))
        )
        for feature in invalid:
            extra = () if feature.reference in {item.reference for item in req.data.features} else (feature,)
            rows = tuple(dict(row, **{feature.name: 1.0}) for row in req.data.rows) if extra else req.data.rows
            data = replace(req.data, rows=rows, features=(*req.data.features, *extra))
            lists = {
                **changed.feature_lists,
                "rq4_reduced": (req.process_references[0], feature.reference),
                "m2_elastic_net": (*req.feature_lists["m2_elastic_net"], *(item.reference for item in extra)),
            }
            with libraries():
                rejects(partial(research_tests.nested_comparison, replace(changed, data=data, feature_lists=lists)), "CMS-only")
        lists = {
            **changed.feature_lists,
            "m2_elastic_net": tuple(reference for reference in req.feature_lists["m2_elastic_net"] if reference != req.cmi_reference),
        }
        with libraries():
            rejects(lambda: research_tests.nested_comparison(replace(changed, feature_lists=lists)), "full list omits CMI")

        designated = Feature("feature_matrix_hospital_window:declared_case_mix", "number", family="hospital_window")
        renamed = replace(
            changed,
            cmi_reference=designated.reference,
            data=replace(
                req.data,
                features=(req.data.features[0], designated, req.data.features[2]),
                rows=tuple({**{key: value for key, value in row.items() if key != "cmi"}, designated.name: row["cmi"]} for row in req.data.rows),
            ),
            feature_lists={
                **{
                    slot: tuple(designated.reference if reference == req.cmi_reference else reference for reference in references)
                    for slot, references in changed.feature_lists.items()
                },
                "rq4_reduced": (req.process_references[0], designated.reference),
            },
        )
        with libraries():
            rejects(lambda: research_tests.nested_comparison(renamed), "CMS-only")

    def ownership_fixed_part_policy() -> None:
        group = Feature("feature_matrix_hospital_window:control_type_code", "category", family="hospital_window")
        lists = {**req.feature_lists, **{slot: (*req.feature_lists[slot], group.reference) for slot in SLOTS}}
        changed = replace(req, data=replace(req.data, features=(*req.data.features, group)), feature_lists=lists)
        with libraries(), patch.object(panel, "native_fits", side_effect=panel_standin):
            for slot in SLOTS:
                result = suite.fit_slot(changed, slot)
                require(result.status == "fitted", f"ownership fixed-part policy prevented {slot}: {result.reason}")
                kept = result.details["selected_references"]
                require((group.reference in kept) == (slot != "m3_mixed"), f"ownership fixed-part policy is wrong for {slot}")
                if slot == "m3_mixed":
                    require(
                        not any(name.startswith("control_type_code") for name in result.details["coefficients"]), "mixed fixed coefficients contain ownership"
                    )
        require(changed.feature_lists == lists, "mixed ownership exclusion mutated the caller's feature lists")

    def missing_ownership_policy() -> None:
        nullable = getattr(sys.modules.get("pandas"), "NA", None)
        nulls = (None, float("nan"), np.float64("nan"), np.float32("nan"))
        for null in (*nulls, nullable) if nullable is not None else nulls:
            missing_ownership_value(null)

    def missing_ownership_value(null: Any) -> None:
        missing = {0, 3, 5, 11}
        rows = tuple(
            dict(row, control_type_code=null, process=10000.0 if row["window_year"] < 2023 else row["process"]) if index in missing else row
            for index, row in enumerate(req.data.rows)
        )
        changed = replace(req, data=replace(req.data, rows=rows), feature_lists={**req.feature_lists, "rq4_reduced": (req.process_references[0],)})
        with libraries(), patch.object(panel, "native_fits", side_effect=panel_standin):
            training, _ = changed.data.split()
            design = FoldPreprocessor(changed.data.features).fit_transform(training.rows)
            rejects(lambda: suite.MixedRegressor().fit(design, training.y), "ownership group is missing")
            result = suite.fit_slot(changed, "m3_mixed")
            require(result.status == "fitted" and len(result.predictions) == 46, "mixed model did not exclude only missing-ownership windows")
            require(result.details["excluded_missing_ownership"] == {"training": 2, "holdout": 2, "total": 4}, "missing-ownership counts are wrong")
            require(result.details["training_rows"] == 142 and result.details["holdout_rows"] == 46, "mixed eligible row counts are wrong")
            require(result.holdout_positions == tuple(index for index in range(48) if index not in (0, 2)), "mixed predictions lost their holdout positions")
            training, _ = changed.data.split()
            expected_median = float(np.median([row["process"] for row in training.rows if isinstance(row["control_type_code"], str)]))
            require(
                result.pipeline.named_steps["prepare"].medians_[req.process_references[0]] == expected_median,
                "excluded ownership rows changed mixed preprocessing",
            )
            original_moran = suite.moran_test
            _, full_holdout = changed.data.split()
            eligible = full_holdout.subset(result.holdout_positions)

            def checked_moran(residuals: Any, keys: Any, *args: Any, **kwargs: Any) -> Any:
                require(
                    keys == eligible.keys and np.allclose(residuals, eligible.y - np.asarray(result.predictions)),
                    "Moran residuals do not match the eligible mixed holdout rows",
                )
                return original_moran(residuals, keys, *args, **kwargs)

            with patch.object(suite, "moran_test", side_effect=checked_moran):
                output = suite.fit_models(changed)
            repeated = suite.fit_models(replace(changed, deadline=Deadline.after(60)))
            for slot in ("m1_ols", "m2_elastic_net", "m4_boosting", "m6_fixed_effects"):
                require(output.slots[slot].status == "fitted" and len(output.slots[slot].predictions) == 48, f"missing ownership removed rows from {slot}")
                require(output.slots[slot].details["training_rows"] == 144, f"missing ownership removed training rows from {slot}")
            payload = output.to_result()
            require(payload == repeated.to_result(), "missing-ownership results changed on rerun")
            mixed = output.slots["m3_mixed"]
            for changed_mixed in (
                replace(mixed, predictions=(mixed.predictions[0] + 0.1, *mixed.predictions[1:])),
                replace(mixed, holdout_positions=(0, *mixed.holdout_positions[1:])),
            ):
                changed_output = replace(output, slots={**output.slots, "m3_mixed": changed_mixed})
                require(
                    changed_output.to_result()["holdout_predictions_content_sha256"] != payload["holdout_predictions_content_sha256"],
                    "canonical prediction hash lost a mixed prediction or its row identity",
                )
            require(payload["counts"]["training"] == 144 and payload["counts"]["holdout"] == 48, "mixed filtering changed global split counts")
            require(payload["selection"]["comparison_holdout_rows"] == 46, "model selection used unmatched holdout cohorts")
            require(payload["selection"]["registry_holdout_rows"] == 48, "mixed exclusions changed the registry cohort")
            require(
                output.research["spatial"]["residual_model"] == "m3_mixed" and output.research["spatial"]["included_hospitals"] == 46,
                "Moran residuals lost the mixed model's eligible row alignment",
            )
            _, holdout = changed.data.split()
            tree_predictions = dict(zip(holdout.keys, output.slots["m4_boosting"].predictions, strict=True))
            stressed_rows = tuple(
                dict(row, **{changed.data.label: tree_predictions[row["spine_key"]] if isinstance(row["control_type_code"], str) else 10000.0})
                if row["window_year"] == 2023
                else row
                for row in changed.data.rows
            )
            stressed = suite.fit_models(replace(changed, data=replace(changed.data, rows=stressed_rows), deadline=Deadline.after(60)))
            require(stressed.selected == "m4_boosting" and stressed.candidate == "m4_boosting", "model selection ranked different holdout cohorts")
            require(stressed.to_result()["selection"]["comparison_rmse"]["m4_boosting"] == 0.0, "comparison RMSE used excluded mixed holdout windows")
            for split in ("training", "holdout"):
                empty_rows = tuple(
                    dict(row, control_type_code=null) if ((row["window_year"] < 2023) == (split == "training")) else row for row in req.data.rows
                )
                failed = suite.fit_slot(replace(changed, data=replace(req.data, rows=empty_rows)), "m3_mixed")
                require(failed.status == "not_estimable" and "ownership" in failed.reason, f"empty mixed {split} split did not produce its intended status")
                require(
                    failed.details["excluded_missing_ownership"][split] == (144 if split == "training" else 48), "failed mixed model lost its exclusion count"
                )

    def robust_spatial_choice_policy() -> None:
        choices = (
            ((8.0, 0.01), (20.0, 0.5), "lag", "one_significant_robust_alternative"),
            ((20.0, 0.5), (8.0, 0.01), "error", "one_significant_robust_alternative"),
            ((10.0, 0.02), (9.0, 0.001), "lag", "both_significant_larger_robust_statistic"),
            ((9.0, 0.001), (10.0, 0.02), "error", "both_significant_larger_robust_statistic"),
        )
        for lag, error, expected, rule in choices:

            class Diagnostics(SpatialOLS):
                def __init__(self, *args: Any, lag: tuple[float, float] = lag, error: tuple[float, float] = error, **kwargs: Any) -> None:
                    super().__init__(*args, **kwargs)
                    self.rlm_lag, self.rlm_error = lag, error

            with libraries() as replacements:
                replacements["spreg"].OLS = Diagnostics
                result = suite.fit_slot(req, "m5_spatial")
            require(result.status == "fitted" and result.details["kind"] == expected, "robust LM choice ignored significance or the larger statistic")
            require(result.details["lm_selection"] == rule, "spatial model did not record its selection rule")
        for lag, error, reason in (
            ((1.0, 0.05), (2.0, 0.5), "significant"),
            ((8.0, 0.01), (8.0, 0.01), "equal"),
            ((float("nan"), 0.01), (8.0, 0.01), "finite"),
        ):

            class RejectedDiagnostics(SpatialOLS):
                def __init__(self, *args: Any, lag: tuple[float, float] = lag, error: tuple[float, float] = error, **kwargs: Any) -> None:
                    super().__init__(*args, **kwargs)
                    self.rlm_lag, self.rlm_error = lag, error

            with libraries() as replacements:
                replacements["spreg"].OLS = RejectedDiagnostics
                result = suite.fit_slot(req, "m5_spatial")
            require(result.status == "not_estimable" and reason in result.reason, "unresolved robust LM diagnostics produced a fitted model")

    def feature_list_reaches_the_real_readers() -> None:
        """The real writer's list, with and without the keys of the kept list, through the three real readers: serving and promotion read the whole reviewed
        list, drift reads the kept columns when it has them and every column when it has not. A reader that took a key it must not read, or required a key
        that an older list lacks, fails here. Two older shapes are sent: the legacy list (``slot``, ``references``, ``columns``, which every writer from the
        first fit bundle to ``de30648~1`` wrote, so version 4 holds it) and the two keys the readers read, with no ``slot``."""
        from scripts.training import bundle
        from scripts.training.job_failure import JobFailure

        for model in ("m1", "m2", "m4"):
            changed, _, _ = kept_scenario(model)
            document = bundle.feature_list_document(fitted_model_suite(model, changed))
            legacy = {key: document[key] for key in ("slot", "references", "columns")}
            read_keys = {key: document[key] for key in ("references", "columns")}
            require(document["slot"] in REGISTRABLE and document["references"], f"{model}: the writer's list is not a registry list: {document}")
            require(
                0 < len(document["kept_columns"]) < len(document["columns"]) and set(document["kept_columns"]) <= set(document["columns"]),
                f"{model}: the fixture must keep fewer columns than it lists so that a reader of the wrong key shows: {document}",
            )
            dropped = sorted(set(document["columns"]) - set(document["kept_columns"]))[0]
            with tempfile.TemporaryDirectory() as scratch:
                folder = Path(scratch)
                for label, shape in (("legacy three-key", legacy), ("reader two-key", read_keys), ("six-key", document)):
                    served, frames = serve_with_the_real_loader(shape, folder)
                    require(served.references == tuple(document["references"]), f"{model} {label}: serving references {served.references}")
                    row = {reference: float(index) for index, reference in enumerate(document["references"])}
                    require(served.predict(row) == 0.5 and frames == [document["columns"]], f"{model} {label}: serving scored the columns {frames}")
                    scored = must_be_read(f"{model} {label}: promotion", partial(score_with_the_real_promotion_reader, shape, document["columns"], folder))
                    require(scored == {"000001:2022": 1.0, "000002:2022": 2.0}, f"{model} {label}: promotion scored {scored}")
                    try:
                        score_with_the_real_promotion_reader(shape, [name for name in document["columns"] if name != dropped], folder)
                    except JobFailure as error:
                        require(error.name == "champion_features" and dropped in str(error), f"{model} {label}: promotion named {error}")
                    else:
                        raise AssertionError(f"{model} {label}: promotion accepted an extract that lacks the listed column {dropped}")
                    result, features = must_be_read(f"{model} {label}: drift", partial(monitor_with_the_real_baseline_reader, shape, folder))
                    expected = document["kept_columns"] if shape is document else document["columns"]
                    require(features == expected and result.features == expected, f"{model} {label}: drift monitors {features}, expected {expected}")

    def feature_list_keys_absent_empty_or_of_a_wrong_type() -> None:
        """Each key a real reader reads (serving: references, columns; promotion: columns; drift: columns, kept_columns), taken from the real writer's list
        and made absent, empty, null, a number, a text, a dict, or a list with a non-text or an empty name. Drift's kept list also gets a repeat and an unknown
        column, and drift's columns are also damaged with no kept list, where the kept list cannot mask the damage. Each reader's outcome is the one in
        ``KEY_OUTCOMES``; the rows in ``KEY_LIMITS`` are serving's accepted-but-wrong behavior, listed so that they cannot change unseen. An absent key never
        serves: serving and promotion refuse it, drift refuses an absent ``columns`` and reads an absent kept list as none."""
        from scripts.training import bundle

        changed, _, _ = kept_scenario("m1")
        document = bundle.feature_list_document(fitted_model_suite("m1", changed))
        require(0 < len(document["kept_columns"]) < len(document["columns"]) and len(document["columns"]) > 3, f"the fixture list is too short: {document}")
        shared = ("absent", "empty", "text", "null", "number", "dict", "not_text", "empty_name")
        found: dict[str, str] = {}
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)

            def fresh(label: str) -> Path:
                path = folder / label.replace(" ", "_")
                path.mkdir()
                return path

            for kind in shared:
                for key in ("references", "columns"):
                    label = f"serving {key} {kind}"
                    found[label] = serving_outcome(damaged_list(document, key, kind), fresh(label), document["references"])
                label = f"promotion columns {kind}"
                found[label] = promotion_outcome(damaged_list(document, "columns", kind), document["columns"], fresh(label))
            for kind, value in (("list", ["columns"]), ("null", None), ("text", "columns")):
                label = f"promotion document {kind}"
                found[label] = promotion_outcome(value, document["columns"], fresh(label))
            label = "promotion columns repeat"
            found[label] = promotion_outcome(damaged_list(document, "columns", "repeat"), document["columns"], fresh(label))
            for kind in (
                "truncated_json",
                "invalid_encoding",
                "empty_file",
                "missing_file",
                "unreadable_file",
                "download_missing",
                "download_oserror",
                "download_error",
            ):
                label = f"promotion artifact {kind}"
                found[label] = promotion_outcome(document, document["columns"], fresh(label), **unreadable_champion_list(kind, folder))
            for key, kinds in (("columns", shared), ("kept_columns", (*shared, "repeat", "unknown"))):
                for kind in kinds:
                    label = f"drift {key} {kind}"
                    found[label] = drift_outcome(damaged_list(document, key, kind), fresh(label))
            legacy = {key: value for key, value in document.items() if not key.startswith("kept_")}
            for kind in shared:
                label = f"drift columns {kind} without a kept list"
                found[label] = drift_outcome(damaged_list(legacy, "columns", kind), fresh(label))
        different = [
            f"{label}: {found.get(label)!r}, expected {KEY_OUTCOMES.get(label)!r}"
            for label in sorted({*found, *KEY_OUTCOMES})
            if found.get(label) != KEY_OUTCOMES.get(label)
        ]
        require(not different, "a reader treats a damaged key differently: " + "; ".join(different))
        require(set(KEY_OUTCOMES) >= KEY_LIMITS, "a limit row names no outcome")
        require(not [label for label in KEY_LIMITS if KEY_OUTCOMES[label].startswith("refused")], "a refusal is listed as a limit")

    def non_registrable_lists_are_refused_by_the_readers_that_need_features() -> None:
        """m3, m5 and m6 are fitted but never registered: their list has an empty registry part and a kept list (the ownership column is left out of m6's).
        An unfitted run's list is empty. No version can carry them to a reader; if one did, drift has nothing to monitor and a set-tagged load refuses."""
        from monitoring.drift_check import CheckError
        from scripts.training import bundle

        documents = {"unfitted": EMPTY_LIST}
        for model in ("m3", "m5", "m6"):
            changed, _, _ = kept_scenario(model)
            documents[model] = bundle.feature_list_document(fitted_model_suite(model, changed))
            require(
                documents[model]["slot"] is None and documents[model]["references"] == [] and documents[model]["kept_columns"],
                f"{model}: expected an empty registry part and a kept list: {documents[model]}",
            )
        with tempfile.TemporaryDirectory() as scratch:
            for name, document in documents.items():
                try:
                    monitor_with_the_real_baseline_reader(document, Path(scratch))
                except CheckError as error:
                    require(error.name == "no_features" and error.code == 4, f"{name}: drift refused with {error}")
                else:
                    raise AssertionError(f"{name}: drift monitored a list that has no registry columns")
                try:
                    serve_with_the_real_loader(document, Path(scratch), {"feature_set": "m2", "feature_set_schema_sha256": "digest"})
                except ValueError as error:
                    require("no references" in str(error), f"{name}: serving refused with {error}")
                else:
                    raise AssertionError(f"{name}: serving loaded a set model that lists no reference")

    return [
        ("model_action_budgets", model_action_budgets),
        ("mediation_attempted_drop_counts", mediation_attempted_drop_counts),
        ("nullable_ownership_stub", nullable_ownership_stub),
        ("mediation_rare_covariate_levels", mediation_rare_covariate_levels),
        ("mediation_missing_process_level", mediation_missing_process_level),
        ("mediation_frozen_indicator_aliases", mediation_frozen_indicator_aliases),
        ("approved_nested_policy", approved_nested_policy),
        ("ownership_fixed_part_policy", ownership_fixed_part_policy),
        ("missing_ownership_policy", missing_ownership_policy),
        ("robust_spatial_choice_policy", robust_spatial_choice_policy),
        ("fold_local_missingness", preprocessing),
        ("unseen_category", unknown_category),
        ("extract_input_contract", input_contract),
        ("seeded_hospital_and_time_folds", folds),
        ("elastic_net_tunes_on_forward_in_time_folds", elastic_net_tunes_on_forward_in_time_folds),
        ("elastic_net_without_a_validation_year_is_not_estimable", elastic_net_without_a_validation_year_is_not_estimable),
        ("elastic_net_clips_at_fold_fitted_percentiles", elastic_net_clips_at_fold_fitted_percentiles),
        ("elastic_net_final_fit_clips_holdout_rows", elastic_net_final_fit_clips_holdout_rows),
        ("elastic_net_winsorizing_leaves_flags_and_categories", elastic_net_winsorizing_leaves_flags_and_categories),
        ("other_models_are_not_winsorized", other_models_are_not_winsorized),
        ("rq1_comparison_fits_process_measures_and_cmi_only", rq1_comparison_fits_process_measures_and_cmi_only),
        ("rq1_comparison_columns_in_the_real_lists", rq1_comparison_columns_in_the_real_lists),
        ("rq1_result_always_states_the_cmi_clipping", rq1_result_always_states_the_cmi_clipping),
        ("rq1_baseline_clips_cmi_at_the_same_bounds", rq1_baseline_clips_cmi_at_the_same_bounds),
        ("other_intercept_cmi_models_keep_raw_cmi", other_intercept_cmi_models_keep_raw_cmi),
        ("elastic_net_fits_each_fold_on_its_fitting_labels", elastic_net_fits_each_fold_on_its_fitting_labels),
        ("elastic_net_selects_the_lowest_validation_error", elastic_net_selects_the_lowest_validation_error),
        ("rq1_comparison_keeps_the_restricted_fits_status", rq1_comparison_keeps_the_restricted_fits_status),
        ("rq1_comparison_runs_after_a_failed_full_fit", rq1_comparison_runs_after_a_failed_full_fit),
        ("six_slot_caller_and_repeatability", complete_suite),
        ("rank_deficient_ols", collinear_ols),
        ("dependent_columns_dropped_and_recorded", dependent_columns_dropped_and_recorded),
        ("dependent_step_uses_fitting_rows_only", dependent_step_uses_fitting_rows_only),
        ("wide_design_is_refused_not_raised", wide_design_is_refused_not_raised),
        ("unpenalized_cmi", forced_elastic),
        ("singleton_ownership", ownership_singleton),
        ("rmse_selection_and_registration", tied_selection),
        ("operation_deadline", expired),
        ("nested_gain", nested_gain),
        ("invalid_nesting", nested_invalid),
        ("nested_no_gain", nested_no_gain),
        ("spatial_order_and_islands", spatial_order_and_islands),
        ("seeded_moran_pattern", moran_signal),
        ("exact_f_tail", exact_f_tail),
        ("mundlak_rejects_correlated_effect", mundlak_rejects_correlated_effect),
        ("mundlak_keeps_null_without_correlation", mundlak_keeps_null_without_correlation),
        ("mundlak_clustered_errors_differ_from_unadjusted", mundlak_clustered_errors_differ_from_unadjusted),
        ("mundlak_reads_p_value_from_f_with_cluster_degrees", mundlak_reads_p_value_from_f_with_cluster_degrees),
        ("mundlak_numerator_follows_the_tested_slopes", mundlak_numerator_follows_the_tested_slopes),
        ("mundlak_refuses_few_clusters", mundlak_refuses_few_clusters),
        ("mundlak_refuses_unidentified_design", mundlak_refuses_unidentified_design),
        ("time_folds_keep_three_fitting_years", time_folds_keep_three_fitting_years),
        ("panel_refuses_fewer_than_three_fitting_years", panel_refuses_fewer_than_three_fitting_years),
        ("panel_record_carries_mundlak", panel_record_carries_mundlak),
        ("one_model_run_of_m6_carries_mundlak", one_model_run_of_m6_carries_mundlak),
        ("panel_fits_complete_cases", panel_fits_complete_cases),
        ("panel_fits_with_an_entirely_missing_predictor", panel_fits_with_an_entirely_missing_predictor),
        ("panel_drops_a_mostly_missing_predictor", panel_drops_a_mostly_missing_predictor),
        ("panel_final_fit_selects_before_complete_cases", panel_final_fit_selects_before_complete_cases),
        ("panel_fold_selection_ignores_validation_rows", panel_fold_selection_ignores_validation_rows),
        ("panel_tuning_drops_a_fold_that_loses_a_fitting_year", panel_tuning_drops_a_fold_that_loses_a_fitting_year),
        ("panel_tuning_with_every_fold_short_takes_the_first_option", panel_tuning_with_every_fold_short_takes_the_first_option),
        ("panel_with_few_clusters_reports_the_tested_slopes", panel_with_few_clusters_reports_the_tested_slopes),
        ("panel_selection_is_in_the_run_result", panel_selection_is_in_the_run_result),
        ("panel_without_repeated_hospitals_stays_not_estimable", panel_without_repeated_hospitals_stays_not_estimable),
        ("unbalanced_panel_and_unseen_effect", panel_unbalanced_and_unseen),
        ("shap_additivity_refusal", shap_failure),
        ("separated_mediation", mediation_separation),
        ("hac_year_join_and_missingness", hac_join),
        ("shared_missing_indicators", shared_missing_indicators),
        ("tuning_prepares_each_fold_once", tuning_prepares_each_fold_once),
        ("nested_indicator_alias_ownership", nested_indicator_alias_ownership),
        ("excluded_controls_from_column_names", excluded_controls_from_column_names),
        ("panel_effect_normalization", panel_effect_normalization),
        ("aggregate_panel_counts", result_has_aggregate_panel_counts),
        ("nested_and_joint_values", nested_and_joint_values),
        ("mixed_values_and_ownership_hold", mixed_values_and_unresolved_ownership),
        ("mixed_optimizer_sequence", mixed_optimizer_sequence),
        ("mixed_optimizer_deadline", mixed_optimizer_deadline),
        ("mediation_known_values", mediation_known_values),
        ("moran_island_values_and_work", moran_island_values_and_work),
        ("optional_analysis_failures", optional_analysis_failures),
        ("configured_shap_sample", configured_shap_sample),
        ("native_shap_failure", native_shap_failure),
        ("mediation_prepares_source_once", mediation_prepares_source_once),
        ("mediation_frozen_category_contrasts", mediation_frozen_category_contrasts),
        ("bundle_feature_list_follows_the_fitted_design", bundle_feature_list_follows_the_fitted_design),
        ("bundle_feature_list_of_an_unfitted_run_is_empty", an_unfitted_run_writes_no_kept_list),
        ("feature_list_reaches_the_real_readers", feature_list_reaches_the_real_readers),
        ("feature_list_keys_absent_empty_or_of_a_wrong_type", feature_list_keys_absent_empty_or_of_a_wrong_type),
        ("non_registrable_lists_are_refused_by_the_readers_that_need_features", non_registrable_lists_are_refused_by_the_readers_that_need_features),
    ]


def source_paths(root: Path) -> list[Path]:
    """Name only model implementation and caller-check sources for bounded evidence hashing."""
    return sorted(
        [
            *root.joinpath("scripts/training/models").glob("*.py"),
            root / "orchestration/training_contract.py",
            root / "config/training/models.json",
            root / "data_contracts/training/models.schema.json",
            *[
                root / f"scripts/training/{name}.py"
                for name in ("research_tests", "mediation", "hac_validation", "model_cases", "model_test_support", "run_model_checks")
            ],
        ]
    )
