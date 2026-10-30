"""Read the mounted training inputs of one job action and rebuild the model request from them."""

from __future__ import annotations

import importlib
import json
import os
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any, Literal

from jsonschema import Draft202012Validator

from orchestration import training_contract
from scripts.training.models.common import SLOTS, Dataset, Deadline, Feature, ModelError, ModelRequest
from scripts.training.step_records import file_sha256

ROOT = Path(__file__).resolve().parents[2]
LIST_SLOTS = (*SLOTS, "rq4_reduced")
PROCESS_FAMILY = "care_compare"
ADJACENCY = "adjacency.parquet"


class InputError(Exception):
    """A mounted input is missing, changed or inconsistent; the name is the job failure name."""

    def __init__(self, name: str, detail: str) -> None:
        super().__init__(detail)
        self.name, self.detail = name, detail


def read_rows(path: Path) -> list[dict[str, Any]]:
    """Read one Parquet file into row dictionaries with the training image's pyarrow."""
    try:
        parquet = importlib.import_module("pyarrow.parquet")
    except ImportError as error:
        raise InputError("dependency_missing", f"cannot read Parquet: {error}") from None
    try:
        return [dict(row) for row in parquet.read_table(path).to_pylist()]
    except (OSError, ValueError) as error:
        raise InputError("extract_unreadable", f"{path.name}: {error}") from None


def input_root(parameters: Path) -> Path:
    """Return the folder that holds the mounted inputs of earlier steps, next to the parameter file."""
    return parameters.parent / "in"


def threads() -> int:
    """Return the thread count the launcher passes, one when it passes none."""
    try:
        return max(1, int(os.environ.get("JOB_THREADS", "1")))
    except ValueError:
        raise InputError("threads", "JOB_THREADS is not a whole number") from None


def read_json(path: Path, name: str) -> Any:
    """Read one JSON input and name the failure when it is absent or unreadable."""
    try:
        return json.loads(path.read_bytes())
    except FileNotFoundError:
        raise InputError("input_missing", f"{path.name} is not mounted") from None
    except (OSError, ValueError) as error:
        raise InputError(name, f"{path.name} cannot be read: {error}") from None


def parameters_of(path: Path) -> dict[str, Any]:
    """Return the validated training parameters of the run."""
    raw = read_json(path, "parameters")
    try:
        return training_contract.parameters_for("training_dag", raw)
    except (ValueError, TypeError) as error:
        raise InputError("parameters", str(error)) from None


def config_of(root: Path) -> dict[str, Any]:
    """Return the model configuration of the code snapshot."""
    return dict(read_json(root / "config/training/models.json", "config"))


def manifest_of(extract: Path, hai_type: str, run: str) -> dict[str, Any]:
    """Return a type's manifest after the schema, the run and the type match."""
    manifest = read_json(extract / f"manifest_{hai_type}.json", "extract_manifest")
    schema = read_json(ROOT / "data_contracts/training/extract_manifest.schema.json", "extract_manifest")
    errors = sorted(Draft202012Validator(schema).iter_errors(manifest), key=lambda error: tuple(str(part) for part in error.absolute_path))
    if errors:
        raise InputError(
            "extract_manifest", f"manifest_{hai_type}.json violates the schema at {'.'.join(str(part) for part in errors[0].absolute_path) or 'root'}"
        )
    if manifest["run_id"] != run or manifest["hai_type"] != hai_type or manifest["file"] != f"extract_{hai_type}.parquet":
        raise InputError("extract_manifest", f"manifest_{hai_type}.json names another run, type or file")
    return dict(manifest)


def table_of(extract: Path, entry: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Read one table whose bytes and row count match its manifest entry."""
    path = extract / str(entry["file"])
    try:
        digest = file_sha256(path)
    except FileNotFoundError:
        raise InputError("input_missing", f"{path.name} is not mounted") from None
    except (OSError, ValueError) as error:
        raise InputError("extract_unreadable", f"{path.name}: {error}") from None
    if digest != entry["sha256"]:
        raise InputError("extract_changed", f"{path.name} differs from its manifest hash")
    rows = read_rows(path)
    if len(rows) != entry["row_count"]:
        raise InputError("extract_changed", f"{path.name} has another row count than its manifest")
    return rows


def extract_of(extract: Path, hai_type: str, run: str) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Return a type's manifest and its verified analysis rows."""
    manifest = manifest_of(extract, hai_type, run)
    return manifest, table_of(extract, manifest)


def auxiliary_of(extract: Path, manifest: Mapping[str, Any], name: str) -> list[dict[str, Any]]:
    """Return one verified shared table named by a manifest."""
    return table_of(extract, manifest["auxiliary_files"][name])


def catalog(root: Path) -> dict[str, Feature]:
    """Return every registered predictor of the feature schema, keyed by its view and column reference."""
    schema = read_json(root / "data_contracts/features/feature_schema.json", "feature_schema")
    found: dict[str, Feature] = {}
    for table in schema["tables"].values():
        for column in table["columns"]:
            if column.get("view") is None or column.get("role") != "predictor":
                continue
            control = str((column.get("pivot") or {}).get("match", {}).get("measure_control", ""))
            kind: Literal["number", "category"] = "category" if column["type"] == "varchar" else "number"
            reference = f"{column['view']}:{column['name']}"
            found[reference] = Feature(reference, kind, control, str(table.get("family", "")))
    return found


def feature_lists(root: Path) -> dict[str, tuple[str, ...]]:
    """Return the reviewed predictor references of each slot and the reduced RQ4 model."""
    return {slot: tuple(read_json(root / f"config/training/feature_lists/{slot}.json", "feature_list")["references"]) for slot in LIST_SLOTS}


def model_request(
    hai_type: str, rows: Sequence[Mapping[str, Any]], adjacency: Sequence[Mapping[str, Any]], config: Mapping[str, Any], seconds: float, root: Path
) -> ModelRequest:
    """Build the six-model request from verified rows, the feature lists and the schema's registered predictors."""
    lists = feature_lists(root)
    known = catalog(root)
    references = sorted({reference for items in lists.values() for reference in items})
    unknown = [reference for reference in references if reference not in known]
    if unknown:
        raise InputError("feature_lists", f"the feature lists name references the schema does not register: {unknown[0]}")
    forced = config["slots"]["m2_elastic_net"]["forced_references"]
    if len(forced) != 1 or forced[0] not in known:
        raise InputError("feature_lists", "the Elastic Net slot must force exactly one registered covariate")
    processes = tuple(reference for reference in lists["m1_ols"] if known[reference].family == PROCESS_FAMILY)
    try:
        data = Dataset(tuple(rows), tuple(known[reference] for reference in references), hai_type)
        return ModelRequest(data, lists, config, tuple(adjacency), processes, forced[0], threads(), Deadline.after(seconds))
    except ModelError as error:
        raise InputError("fit_input", str(error)) from None
