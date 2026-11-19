"""Load a registered model version for serving, reading data only. It runs only in the serving image.

The image holds the code of the trusted model types (``data_contracts/training/model_code.json`` lists its files), so a load needs only the model artifact's
data: the loader downloads four files of the version's source run, never ``model/MLmodel`` and never ``model/code/``, and never calls
``mlflow.sklearn.load_model``.
Every download goes through one ``fetch`` call into one temporary folder that exists for one load and is removed when the load ends, with or without a refusal.

The steps of one load: the registry's source must be ``runs:/<run_id>/model``; the version tags give the schema hash and the feature set; the feature list
(``bundle/feature_list.json``) and the provenance (``bundle/provenance.json``) are checked as data; the model is admitted only when its recorded closure digest
equals the image's and the four library versions equal the installed ones; the saved prediction check (``bundle/self_check.json``) is checked as data;
``model/model.skops`` is loaded by skops under the trusted types of the image's own ``model_contract.json``, never a list that the artifact brings; and the
prediction wrapper that the service calls reproduces the saved predictions. A value published as null reaches the model as NaN.

The refusals are classes of their own, because the service shows only the class name in ``/v1/health`` and in its log (``type(error).__name__``).
"""

from __future__ import annotations

import gc
import importlib
import importlib.metadata
import json
import logging
import math
import os
import re
import resource
import sys
import tempfile
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any

from scripts import model_code
from scripts.serving.service import LoadedModel

LOG = logging.getLogger(__name__)
ROOT = Path(__file__).resolve().parents[2]
CONTRACT = "data_contracts/training/model_contract.json"
FEATURE_LIST = "bundle/feature_list.json"
PROVENANCE = "bundle/provenance.json"
SELF_CHECK = "bundle/self_check.json"
MODEL = "model/model.skops"
PROXY_DOWNLOADS = "MLFLOW_ENABLE_PROXY_MULTIPART_DOWNLOAD"
SET_NAME = re.compile(r"[a-z][a-z0-9_]{0,15}")
DIGEST = re.compile(r"[0-9a-f]{64}")
CLOSURE_KEY = "model_code_closure_sha256"
# The libraries whose recorded version must equal the installed one: a model saved with one release is not safe to load with another.
LIBRARIES = ("scikit-learn", "xgboost", "numpy", "scipy")
# Provisional bounds (the first live load logs the real sizes): each JSON file 1 MiB, the model 64 MiB, at most 2000 features, 256 characters per text.
JSON_CAP = 1024 * 1024
MODEL_CAP = 64 * 1024 * 1024
MAX_FEATURES = 2000
MAX_TEXT = 256
MAX_ROWS = 8
SELF_CHECK_KEYS = {"schema", "slot", "references", "rows", "predictions"}
# The prediction check: ``math.isclose(got, saved, rel_tol=REL_TOL, abs_tol=ABS_TOL)``. Equal closure and library versions leave only floating-point summation
# order to differ (about 1e-15 relative), so 1e-9 leaves six orders of headroom and a wrong feature order or a changed model differs by far more.
REL_TOL = 1e-9
ABS_TOL = 1e-12

Fetch = Callable[[str, str, str], str]


class ArtifactRefused(ValueError):
    """The registry's source, a file's size or a file's shape is not one the loader reads."""


class ModelCodeMissing(ValueError):
    """The model records no closure digest of the model code it was trained with."""


class ModelCodeMismatch(ValueError):
    """The model was trained with model code that is not the code in this image."""


class LibraryMismatch(ValueError):
    """The model records another version of scikit-learn, xgboost, numpy or scipy than the image installs."""


class SelfCheckMissing(ValueError):
    """The model has no saved prediction check (a version trained before the check existed, or a file that cannot be downloaded)."""


class PredictionMismatch(ValueError):
    """The image's model, run through the service's wrapper, does not reproduce the saved predictions."""


class ImageCodeMismatch(RuntimeError):
    """The image's own model code files hash to another value than the record the image holds (a build from a dirty tree)."""


def installed_version(name: str) -> str:
    return importlib.metadata.version(name)


def mlflow_fetch(run_id: str, artifact_path: str, destination: str) -> str:
    """Download one file of a run into ``destination`` and return its local path (the MLflow call with an explicit destination)."""
    mlflow = importlib.import_module("mlflow")
    return str(mlflow.artifacts.download_artifacts(run_id=run_id, artifact_path=artifact_path, dst_path=destination))


def image_closure(root: Path) -> str:
    """The closure digest of the files at ``root``, which must equal the record's ``closure_sha256``. RecordError names a bad record or a missing file."""
    record = model_code.read_record(root / model_code.RECORD)
    actual = model_code.closure_digest(root)
    if actual != record["closure_sha256"]:
        raise ImageCodeMismatch(f"the image's model code files hash to {actual}, and its record holds {record['closure_sha256']}")
    return actual


def refuse_constant(token: str) -> Any:
    raise ValueError(f"{token} is not a JSON number")


def refuse_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    keys = [key for key, _ in pairs]
    if len(keys) != len(set(keys)):
        raise ValueError("a key appears twice")
    return dict(pairs)


def read_json(path: str, name: str) -> Any:
    """Read one downloaded file as JSON data: at most ``JSON_CAP`` bytes, one document, no duplicate key and no NaN or Infinity token."""
    try:
        with open(path, "rb") as stream:
            data = stream.read(JSON_CAP + 1)
    except OSError as error:
        raise ArtifactRefused(f"{name} cannot be read") from error
    if len(data) > JSON_CAP:
        raise ArtifactRefused(f"{name} is over {JSON_CAP} bytes")
    try:
        return json.loads(data.decode(), object_pairs_hook=refuse_duplicates, parse_constant=refuse_constant)
    except (UnicodeDecodeError, ValueError, RecursionError) as error:
        raise ArtifactRefused(f"{name} is not one JSON document of finite numbers with no repeated key") from error


def text_list(document: Mapping[str, Any], key: str, name: str) -> list[str]:
    values = document.get(key)
    if not isinstance(values, list) or len(values) > MAX_FEATURES or not all(isinstance(item, str) and len(item) <= MAX_TEXT for item in values):
        raise ArtifactRefused(f"{name} must hold {key} as a list of at most {MAX_FEATURES} texts of at most {MAX_TEXT} characters")
    return values


def check_feature_list(document: Any) -> tuple[tuple[str, ...], tuple[str, ...]]:
    if not isinstance(document, dict):
        raise ArtifactRefused(f"{FEATURE_LIST} must be one JSON object")
    references, columns = text_list(document, "references", FEATURE_LIST), text_list(document, "columns", FEATURE_LIST)
    if len(references) != len(columns):
        raise ArtifactRefused(f"{FEATURE_LIST} must list as many columns as references")
    return tuple(references), tuple(columns)


def check_admission(provenance: Any, closure: str) -> None:
    """Exact equality: the closure digest, then the four library versions."""
    if not isinstance(provenance, dict):
        raise ArtifactRefused(f"{PROVENANCE} must be one JSON object")
    recorded = provenance.get(CLOSURE_KEY)
    if recorded is None:
        raise ModelCodeMissing(f"{PROVENANCE} holds no {CLOSURE_KEY}")
    if not isinstance(recorded, str) or not DIGEST.fullmatch(recorded):
        raise ArtifactRefused(f"{CLOSURE_KEY} must be 64 lowercase hex characters")
    if recorded != closure:
        raise ModelCodeMismatch(f"the model was trained with model code {recorded}, and this image holds {closure}")
    versions = provenance.get("resolved_versions")
    if not isinstance(versions, dict):
        raise ArtifactRefused(f"{PROVENANCE} must hold resolved_versions as an object")
    for library in LIBRARIES:
        trained = versions.get(library)
        if trained is not None and not isinstance(trained, str):
            raise ArtifactRefused(f"the recorded version of {library} must be text or null")
        try:
            installed: str | None = installed_version(library)
        except importlib.metadata.PackageNotFoundError:
            installed = None
        if trained is None or trained != installed:
            raise LibraryMismatch(f"{library}: the model was trained with {trained}, and this image installs {installed}")


def finite_number(value: Any) -> bool:
    """Whether a JSON value is a number (not a boolean) that a float holds and that is finite: an integer of 400 digits is not."""
    if isinstance(value, bool) or not isinstance(value, int | float):
        return False
    try:
        return math.isfinite(float(value))
    except OverflowError:
        return False


def check_cell(value: Any, row: int) -> None:
    if value is None or (isinstance(value, str) and len(value) <= MAX_TEXT) or finite_number(value):
        return
    raise ArtifactRefused(f"{SELF_CHECK} row {row} holds a cell that is not null, a finite number or short text")


def check_self_check(document: Any, references: tuple[str, ...]) -> tuple[list[list[Any]], list[float]]:
    """The shape of ``bundle/self_check.json`` as data: a closed key set, the feature list's references, 1 to ``MAX_ROWS`` rows, as many finite predictions."""
    if not isinstance(document, dict) or set(document) != SELF_CHECK_KEYS:
        raise ArtifactRefused(f"{SELF_CHECK} must be one JSON object with exactly the keys {sorted(SELF_CHECK_KEYS)}")
    if type(document["schema"]) is not int or document["schema"] != 1:
        raise ArtifactRefused(f"{SELF_CHECK} must have schema 1")
    if not isinstance(document["slot"], str) or len(document["slot"]) > MAX_TEXT:
        raise ArtifactRefused(f"{SELF_CHECK} must name its slot as text")
    if document["references"] != list(references):
        raise ArtifactRefused(f"the references of {SELF_CHECK} differ from those of {FEATURE_LIST}")
    rows, predictions = document["rows"], document["predictions"]
    if not isinstance(rows, list) or not 1 <= len(rows) <= MAX_ROWS:
        raise ArtifactRefused(f"{SELF_CHECK} must hold 1 to {MAX_ROWS} rows")
    for index, row in enumerate(rows):
        if not isinstance(row, list) or len(row) != len(references):
            raise ArtifactRefused(f"{SELF_CHECK} row {index} must be a list with one cell per reference")
        for value in row:
            check_cell(value, index)
    if not isinstance(predictions, list) or len(predictions) != len(rows):
        raise ArtifactRefused(f"{SELF_CHECK} must hold one prediction per row")
    for index, value in enumerate(predictions):
        if not finite_number(value):
            raise ArtifactRefused(f"{SELF_CHECK} prediction {index} must be a finite number")
    return rows, [float(value) for value in predictions]


def replay(predict: Callable[[Mapping[str, Any]], float], references: tuple[str, ...], rows: list[list[Any]], saved: list[float]) -> None:
    """Run each saved row through the prediction wrapper. A raised error, a non-finite result or a value outside the tolerance is ``PredictionMismatch``.
    Messages name the row, never a value."""
    for index, (row, expected) in enumerate(zip(rows, saved, strict=True)):
        try:
            got = predict(dict(zip(references, row, strict=True)))
        except Exception as error:
            raise PredictionMismatch(f"row {index}: the model raised {type(error).__name__}") from error
        if not math.isfinite(got) or not math.isclose(got, expected, rel_tol=REL_TOL, abs_tol=ABS_TOL):
            raise PredictionMismatch(f"row {index}: the prediction is not the saved one")


def peak_memory_mib() -> float:
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return peak / (1024 * 1024) if sys.platform == "darwin" else peak / 1024


class MlflowLoader:
    def __init__(self, fetch: Fetch | None = None, root: Path = ROOT) -> None:
        """Check the image's own model code against its record (the start of the process fails when they differ), then read the image's contract.

        ``fetch(run_id, artifact_path, destination)`` downloads one file into ``destination`` and returns its path; the default is the MLflow download call.
        """
        self._fetch: Fetch = fetch or mlflow_fetch
        self.closure = image_closure(root)
        self._contract = json.loads((root / CONTRACT).read_text())

    def load(self, hai_type: str, version: str) -> LoadedModel:
        # The task reaches the tracking server only. With no setting, the 3.17.2 client asks the server's /server-info, which advertises presigned downloads
        # for an S3 artifact destination, and reads each file straight from S3. A failure of that read is hidden behind one MlflowException. Setting
        # ``false`` before any client or download exists keeps the run file download on the tracking server.
        os.environ[PROXY_DOWNLOADS] = "false"
        mlflow = importlib.import_module("mlflow")
        pandas = importlib.import_module("pandas")
        name = self._contract["registered_models"][hai_type]
        info = mlflow.MlflowClient().get_model_version(name, version)
        if info.source != f"runs:/{info.run_id}/model":
            raise ArtifactRefused("the model version's source is not runs:/<run_id>/model of its own run")
        feature_set = str(info.tags.get("feature_set", ""))
        if feature_set:
            if not SET_NAME.fullmatch(feature_set):
                raise ValueError("the model version names a feature set that is not a set name")
            if not info.tags.get("feature_set_schema_sha256"):
                raise ValueError("the model version names a feature set but holds no schema hash of the set")
            schema_hash = str(info.tags["feature_set_schema_sha256"])
        else:
            schema_hash = str(info.tags["feature_schema_sha256"])
        # One folder for this load: every download lands in it, and it is removed when the ``with`` ends, after skops and the self-check or at a refusal.
        sizes: dict[str, int] = {}

        def download(path: str, folder: str) -> str:
            local = self._fetch(info.run_id, path, folder)
            sizes[path] = os.stat(local).st_size
            return local

        with tempfile.TemporaryDirectory(prefix="model_load_") as folder:
            references, columns = check_feature_list(read_json(download(FEATURE_LIST, folder), FEATURE_LIST))
            if feature_set and not references:
                raise ValueError("the model version names a feature set but its feature list has no references")
            try:
                recorded = download(PROVENANCE, folder)
            except Exception as error:
                raise ModelCodeMissing(f"{PROVENANCE} cannot be downloaded") from error
            provenance = read_json(recorded, PROVENANCE)
            check_admission(provenance, self.closure)
            try:
                saved = download(SELF_CHECK, folder)
            except Exception as error:
                raise SelfCheckMissing(f"{SELF_CHECK} cannot be downloaded") from error
            rows, predictions = check_self_check(read_json(saved, SELF_CHECK), references)
            model_path = download(MODEL, folder)
            if sizes[MODEL] > MODEL_CAP:
                raise ArtifactRefused(f"{MODEL} is over {MODEL_CAP} bytes")
            pipeline: Any = importlib.import_module("skops.io").load(model_path, trusted=list(self._contract["serialization"]["trusted_types"]))

            def predict(values: Mapping[str, Any]) -> float:
                row = {column: math.nan if values[reference] is None else values[reference] for reference, column in zip(references, columns, strict=True)}
                return float(pipeline.predict(pandas.DataFrame([row]))[0])

            replay(predict, references, rows, predictions)
        LOG.info("model %s version %s loaded; file bytes %s; process peak memory %.0f MiB", hai_type, version, sizes, peak_memory_mib())
        return LoadedModel(version, references, schema_hash, predict, feature_set)

    def release(self, model: LoadedModel) -> None:
        gc.collect()
