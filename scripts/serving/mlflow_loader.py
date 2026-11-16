"""Load a registered model version for serving. It runs only in the serving image.

The version supplies its feature references from the bundle's ``feature_list.json`` in the source run, and the feature schema hash from the version
tags the promotion check wrote. A version trained on a model set carries ``feature_set`` and ``feature_set_schema_sha256``: it is served from that set's
online layout and compared with the set's schema hash. A version with no ``feature_set`` is served from the single path with ``feature_schema_sha256``.
The model code travels in the model artifact (``code_paths``) and its classes load under the contract's trusted types.
A value published as null reaches the model as NaN. The serving contract has already refused an absent view row.
"""

from __future__ import annotations

import gc
import importlib
import json
import math
import os
import re
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from scripts.serving.service import LoadedModel

CONTRACT = Path(__file__).resolve().parents[2] / "data_contracts/training/model_contract.json"
FEATURE_LIST = "bundle/feature_list.json"
PROXY_DOWNLOADS = "MLFLOW_ENABLE_PROXY_MULTIPART_DOWNLOAD"
SET_NAME = re.compile(r"[a-z][a-z0-9_]{0,15}")


class MlflowLoader:
    def __init__(self) -> None:
        self._contract = json.loads(CONTRACT.read_text())

    def load(self, hai_type: str, version: str) -> LoadedModel:
        # The task reaches the tracking server only. With no setting, the 3.17.2 client asks the server's /server-info, which advertises presigned downloads
        # for an S3 artifact destination, and reads each file straight from S3. A failure of that read is hidden behind one MlflowException. Setting
        # ``false`` before any client or download exists keeps the run file download and the models:/ model download on the tracking server.
        os.environ[PROXY_DOWNLOADS] = "false"
        mlflow = importlib.import_module("mlflow")
        pandas = importlib.import_module("pandas")
        name = self._contract["registered_models"][hai_type]
        info = mlflow.MlflowClient().get_model_version(name, version)
        feature_set = str(info.tags.get("feature_set", ""))
        if feature_set:
            if not SET_NAME.fullmatch(feature_set):
                raise ValueError("the model version names a feature set that is not a set name")
            if not info.tags.get("feature_set_schema_sha256"):
                raise ValueError("the model version names a feature set but holds no schema hash of the set")
            schema_hash = str(info.tags["feature_set_schema_sha256"])
        else:
            schema_hash = str(info.tags["feature_schema_sha256"])
        listing = json.loads(Path(mlflow.artifacts.download_artifacts(run_id=info.run_id, artifact_path=FEATURE_LIST)).read_text())
        references, columns = tuple(listing["references"]), tuple(listing["columns"])
        pipeline: Any = mlflow.sklearn.load_model(f"models:/{name}/{version}")

        def predict(values: Mapping[str, Any]) -> float:
            row = {column: math.nan if values[reference] is None else values[reference] for reference, column in zip(references, columns, strict=True)}
            return float(pipeline.predict(pandas.DataFrame([row]))[0])

        if feature_set and not references:
            raise ValueError("the model version names a feature set but its feature list has no references")
        return LoadedModel(version, references, schema_hash, predict, feature_set)

    def release(self, model: LoadedModel) -> None:
        gc.collect()
