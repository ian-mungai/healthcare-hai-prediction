"""Build the per-type fit bundle that ``data_contracts/training/bundle.json`` describes, as in-memory files.

``result.json`` holds only the reproducible result of the suite; run-specific facts (run ID, timings, try) live in ``provenance.json``. Native library
calls (Parquet, the MLflow model format) sit behind two functions so that host checks can replace them.
"""

from __future__ import annotations

import hashlib
import importlib
import importlib.metadata
import json
import tempfile
import time
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from scripts.training.models.common import SLOTS, Feature
from scripts.training.models.suite import ModelSuite

PARQUET_TYPES = {"varchar": "string", "double": "float64"}


def prefix(hai_type: str) -> str:
    """Return the bundle folder of a type, relative to the fit output folder."""
    return f"bundles/bundle_{hai_type}"


def canonical(document: Mapping[str, Any]) -> bytes:
    """Return the canonical bytes of a JSON document: sorted keys, fixed separators, finite numbers."""
    return (json.dumps(document, sort_keys=True, separators=(",", ":"), allow_nan=False) + "\n").encode()


def write_table(path: Path, rows: Sequence[Mapping[str, Any]], columns: Mapping[str, str]) -> None:
    """Write a typed Parquet table with the training image's pyarrow."""
    arrow = importlib.import_module("pyarrow")
    parquet = importlib.import_module("pyarrow.parquet")
    schema = arrow.schema([(name, getattr(arrow, PARQUET_TYPES[columns[name]])()) for name in sorted(columns)])
    parquet.write_table(arrow.Table.from_pylist([dict(row) for row in rows], schema=schema), path, compression="zstd", version="2.6", use_dictionary=False)


def save_model(pipeline: Any, folder: Path, features: Sequence[Feature], contract: Mapping[str, Any], code_root: Path) -> None:
    """Save the registry candidate with ``mlflow.sklearn.save_model`` in the serialization format of the model contract, with a bare-column signature."""
    types = importlib.import_module("mlflow.types.schema")
    signature = importlib.import_module("mlflow.models.signature").ModelSignature(
        inputs=types.Schema([types.ColSpec("string" if feature.kind == "category" else "double", feature.name) for feature in features])
    )
    serialization = contract["serialization"]
    importlib.import_module("mlflow.sklearn").save_model(
        pipeline,
        str(folder),
        serialization_format=serialization["format"],
        skops_trusted_types=list(serialization["trusted_types"]),
        code_paths=[str(code_root / path) for path in serialization["code_paths"]],
        signature=signature,
    )


def holdout_table(suite: ModelSuite) -> tuple[list[dict[str, Any]], dict[str, str]]:
    """Return one row per holdout hospital-window, sorted by key, with the observed SIR and each slot's prediction (null where it made none)."""
    _, holdout = suite.request.data.split()
    predicted = {
        slot: dict(zip(result.holdout_positions, result.predictions, strict=True)) for slot, result in suite.slots.items() if result.status == "fitted"
    }
    rows = [
        {"spine_key": holdout.keys[index], "observed_sir": float(holdout.y[index])}
        | {f"predicted_{slot}": predicted.get(slot, {}).get(index) for slot in SLOTS}
        for index in sorted(range(len(holdout.rows)), key=lambda index: holdout.keys[index])
    ]
    columns = {"spine_key": "varchar", "observed_sir": "double"} | {f"predicted_{slot}": "double" for slot in SLOTS}
    return rows, columns


def resolved_versions(contract: Mapping[str, Any]) -> dict[str, str | None]:
    """Return the installed version of each package the model contract requires, or null for one the image does not carry as a Python package."""
    found: dict[str, str | None] = {}
    for name in contract["resolved_versions_required"]:
        try:
            found[name] = importlib.metadata.version(name)
        except importlib.metadata.PackageNotFoundError:
            found[name] = None
    return found


def build(
    suite: ModelSuite,
    hai_type: str,
    inputs: Mapping[str, Any],
    record: Mapping[str, Any],
    run: str,
    contract: Mapping[str, Any],
    code_root: Path,
    started: float,
) -> dict[str, bytes]:
    """Return every file of one type's bundle, keyed by its path relative to the fit output folder."""
    base = prefix(hai_type)
    result = suite.to_result()
    result["input_hashes"] = {**result["input_hashes"], **inputs["hashes"]}
    files = {f"{base}/result.json": canonical(result)}
    files[f"{base}/provenance.json"] = canonical(
        {
            "run_id": run,
            "timings": {"fit_seconds": round(time.monotonic() - started, 3)},
            "try": None,
            "git_commit": record["git_commit"],
            "git_dirty": record.get("git_dirty"),
            "code_digest": record["code_digest"],
            "resolved_versions": resolved_versions(contract),
            "inputs": inputs["records"],
        }
    )
    rows, columns = holdout_table(suite)
    candidate = suite.candidate
    features = suite.request.features_for(candidate) if candidate is not None else ()
    files[f"{base}/feature_list.json"] = canonical(
        {"slot": candidate, "references": [feature.reference for feature in features], "columns": [feature.name for feature in features]}
    )
    with tempfile.TemporaryDirectory(prefix="bundle_") as scratch:
        table = Path(scratch) / "holdout_predictions.parquet"
        write_table(table, rows, columns)
        files[f"{base}/holdout_predictions.parquet"] = table.read_bytes()
        if candidate is not None:
            model = Path(scratch) / "model"
            save_model(suite.slots[candidate].pipeline, model, features, contract, code_root)
            for path in sorted(model.rglob("*")):
                if path.is_file():
                    files[f"{base}/model/{path.relative_to(model).as_posix()}"] = path.read_bytes()
    files[f"{base}/shap.json"] = canonical(dict(suite.shap))
    return files


def sha256_of(files: Mapping[str, str], hai_type: str) -> str:
    """Return the bundle hash of a type from the SHA-256 of each of its files, as the fit completion record lists them."""
    base = prefix(hai_type) + "/"
    chosen = {name.removeprefix(base): digest for name, digest in sorted(files.items()) if name.startswith(base)}
    return hashlib.sha256(json.dumps(chosen, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
