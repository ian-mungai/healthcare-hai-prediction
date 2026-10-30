"""Load a registered model version for serving. It runs only in the serving image.

The version supplies its feature references from the bundle's ``feature_list.json`` in the source run, and the feature schema hash from the version
tag the promotion check wrote. The model code travels in the model artifact (``code_paths``) and its classes load under the contract's trusted types.
Missing feature values reach the model as NaN.
"""

from __future__ import annotations

import gc
import importlib
import json
import math
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from scripts.serving.service import LoadedModel

CONTRACT = Path(__file__).resolve().parents[2] / "data_contracts/training/model_contract.json"
FEATURE_LIST = "bundle/feature_list.json"


class MlflowLoader:
    def __init__(self) -> None:
        self._contract = json.loads(CONTRACT.read_text())

    def load(self, hai_type: str, version: str) -> LoadedModel:
        mlflow = importlib.import_module("mlflow")
        pandas = importlib.import_module("pandas")
        name = self._contract["registered_models"][hai_type]
        info = mlflow.MlflowClient().get_model_version(name, version)
        schema_hash = str(info.tags["feature_schema_sha256"])
        listing = json.loads(Path(mlflow.artifacts.download_artifacts(run_id=info.run_id, artifact_path=FEATURE_LIST)).read_text())
        references, columns = tuple(listing["references"]), tuple(listing["columns"])
        pipeline: Any = mlflow.sklearn.load_model(f"models:/{name}/{version}")

        def predict(values: Mapping[str, Any]) -> float:
            row = {column: math.nan if values[reference] is None else values[reference] for reference, column in zip(references, columns, strict=True)}
            return float(pipeline.predict(pandas.DataFrame([row]))[0])

        return LoadedModel(version, references, schema_hash, predict)

    def release(self, model: LoadedModel) -> None:
        gc.collect()
