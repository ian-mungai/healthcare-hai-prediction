"""Load a registered model version for serving, reading data only. It runs only in the serving image.

The image holds the code of the trusted model types (``data_contracts/training/model_code.json`` lists its files), so a load needs only the model artifact's
data: the loader downloads four files of the version's source run, never ``model/MLmodel`` and never ``model/code/``, and never calls
``mlflow.sklearn.load_model``.
Every download goes through one ``fetch`` call, which streams one named file to a local path with a fixed name, inside one temporary folder that exists for
one load and is removed when the load ends, with or without a refusal. The fetch counts the bytes it writes against the file's cap and stops at the first
byte over it. It never asks MLflow to download a path as a folder: a path that is a folder is refused before any byte of a file below it is fetched.

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
FEATURE_LIST = model_code.FEATURE_LIST
PROVENANCE = model_code.PROVENANCE
SELF_CHECK = model_code.SELF_CHECK
MODEL = model_code.MODEL_FILE
PROXY_DOWNLOADS = "MLFLOW_ENABLE_PROXY_MULTIPART_DOWNLOAD"
SET_NAME = re.compile(r"[a-z][a-z0-9_]{0,15}")
# The byte caps and the text and count bounds of the data rules, which ``scripts/model_code.py`` holds for every reader of the bundle files.
JSON_CAP = model_code.JSON_CAP
MODEL_CAP = model_code.MODEL_CAP
MAX_FEATURES = model_code.MAX_FEATURES
MAX_TEXT = model_code.MAX_TEXT
# The libraries whose recorded version must equal the installed one: a model saved with one release is not safe to load with another.
LIBRARIES = ("scikit-learn", "xgboost", "numpy", "scipy")
# The local names of the four files: the loader chooses them, the artifact never does.
LOCAL_NAMES = {FEATURE_LIST: "feature_list.json", PROVENANCE: "provenance.json", SELF_CHECK: "self_check.json", MODEL: "model.skops"}
# The prediction check: ``math.isclose(got, saved, rel_tol=REL_TOL, abs_tol=ABS_TOL)``. Equal closure and library versions leave only floating-point summation
# order to differ (about 1e-15 relative), so 1e-9 leaves six orders of headroom and a wrong feature order or a changed model differs by far more.
REL_TOL = 1e-9
ABS_TOL = 1e-12

Fetch = model_code.Fetch


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


def default_fetch(run_id: str, artifact_path: str, local_path: str, limit: int) -> str:
    """The bounded single-file fetch of ``scripts/model_code.py``, with a broken rule reported as ``ArtifactRefused``."""
    return refused(model_code.mlflow_fetch, run_id, artifact_path, local_path, limit)


def image_closure(root: Path) -> str:
    """The closure digest of the files at ``root``, which must equal the record's ``closure_sha256``. RecordError names a bad record or a missing file."""
    record = model_code.read_record(root / model_code.RECORD)
    actual = model_code.closure_digest(root)
    if actual != record["closure_sha256"]:
        raise ImageCodeMismatch(f"the image's model code files hash to {actual}, and its record holds {record['closure_sha256']}")
    return actual


def refused[Result](rule: Callable[..., Result], *arguments: Any) -> Result:
    """Apply one data rule of ``scripts/model_code.py`` and report a broken rule as ``ArtifactRefused``."""
    try:
        return rule(*arguments)
    except model_code.DataRuleError as error:
        raise ArtifactRefused(str(error)) from error


def read_json(path: str, name: str) -> Any:
    """Read one downloaded file as JSON data: at most ``JSON_CAP`` bytes, one UTF-8 document, no duplicate key and no NaN or Infinity token."""
    return refused(model_code.read_json_file, path, name)


def check_feature_list(document: Any) -> tuple[tuple[str, ...], tuple[str, ...]]:
    """The closed shape of ``bundle/feature_list.json`` (the references and the columns it returns)."""
    return refused(model_code.check_feature_list, document)


def check_provenance_shape(provenance: Any) -> None:
    """The closed shape of ``bundle/provenance.json``."""
    refused(model_code.check_provenance, provenance)


def check_self_check(document: Any, references: tuple[str, ...]) -> tuple[list[list[Any]], list[float]]:
    """The shape of ``bundle/self_check.json`` as data (the rows and the predictions it returns)."""
    return refused(model_code.check_self_check, document, references)


def check_admission(provenance: Any, closure: str) -> None:
    """The closed shape of the provenance, then exact equality: the closure digest, then the four library versions."""
    check_provenance_shape(provenance)
    recorded = provenance.get(model_code.CLOSURE_KEY)
    if recorded is None:
        raise ModelCodeMissing(f"{PROVENANCE} holds no {model_code.CLOSURE_KEY}")
    if not isinstance(recorded, str) or not model_code.DIGEST.fullmatch(recorded):
        raise ArtifactRefused(f"{model_code.CLOSURE_KEY} must be 64 lowercase hex characters")
    if recorded != closure:
        raise ModelCodeMismatch(f"the model was trained with model code {recorded}, and this image holds {closure}")
    versions = provenance.get("resolved_versions")
    if not isinstance(versions, dict):
        raise ArtifactRefused(f"{PROVENANCE} must hold resolved_versions as an object")
    for library in LIBRARIES:
        trained = versions.get(library)
        try:
            installed: str | None = installed_version(library)
        except importlib.metadata.PackageNotFoundError:
            installed = None
        if trained is None or trained != installed:
            raise LibraryMismatch(f"{library}: the model was trained with {trained}, and this image installs {installed}")


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

        ``fetch(run_id, artifact_path, local_path, limit)`` writes the one file at ``local_path``, which the loader chose, never more than ``limit`` bytes, and
        returns ``local_path``; the default streams it from the tracking server.
        """
        self._fetch: Fetch = fetch or default_fetch
        self.closure = image_closure(root)
        self._contract = json.loads((root / CONTRACT).read_text())

    def load(self, hai_type: str, version: str) -> LoadedModel:
        # The task reaches the tracking server only. With no setting, the 3.17.2 client asks the server's /server-info, which advertises presigned downloads
        # for an S3 artifact destination, and reads each file straight from S3. Setting ``false`` before any client exists keeps any later MLflow download on
        # the tracking server; the default fetch already sends its own streamed GET there.
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
        # One folder for this load: every download lands in it under a fixed name, and it is removed when the ``with`` ends, after skops and the
        # self-check or at a refusal.
        sizes: dict[str, int] = {}

        def download(path: str, folder: str, limit: int) -> str:
            """Fetch one file to its fixed local name and require that to be one regular file (no link, folder or other file) of at most ``limit`` bytes."""
            local = refused(model_code.fetch_regular_file, self._fetch, info.run_id, path, os.path.join(folder, LOCAL_NAMES[path]), limit)
            sizes[path] = os.lstat(local).st_size
            return local

        with tempfile.TemporaryDirectory(prefix="model_load_") as folder:
            references, columns = check_feature_list(read_json(download(FEATURE_LIST, folder, JSON_CAP), FEATURE_LIST))
            if feature_set and not references:
                raise ValueError("the model version names a feature set but its feature list has no references")
            try:
                recorded = download(PROVENANCE, folder, JSON_CAP)
            except ArtifactRefused:
                raise
            except Exception as error:
                raise ModelCodeMissing(f"{PROVENANCE} cannot be downloaded") from error
            provenance = read_json(recorded, PROVENANCE)
            check_admission(provenance, self.closure)
            try:
                saved = download(SELF_CHECK, folder, JSON_CAP)
            except ArtifactRefused:
                raise
            except Exception as error:
                raise SelfCheckMissing(f"{SELF_CHECK} cannot be downloaded") from error
            rows, predictions = check_self_check(read_json(saved, SELF_CHECK), references)
            model_path = download(MODEL, folder, MODEL_CAP)
            pipeline: Any = model_code.load_data_only(model_path, self._contract["serialization"]["trusted_types"])

            def predict(values: Mapping[str, Any]) -> float:
                row = {column: math.nan if values[reference] is None else values[reference] for reference, column in zip(references, columns, strict=True)}
                return float(pipeline.predict(pandas.DataFrame([row]))[0])

            replay(predict, references, rows, predictions)
        LOG.info("model %s version %s loaded; file bytes %s; process peak memory %.0f MiB", hai_type, version, sizes, peak_memory_mib())
        return LoadedModel(version, references, schema_hash, predict, feature_set)

    def release(self, model: LoadedModel) -> None:
        gc.collect()
