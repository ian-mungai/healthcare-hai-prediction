"""Fetch the Production model's drift baseline from MLflow and DVC (project decision 21).

``python -m monitoring.baseline --hai-type HAI_1 --tracking-uri URI --out DIR`` looks up the ``production`` alias of the type's registered model, reads the
source run's ``bundle/feature_list.json`` and ``extract/manifest_<type>.json`` artifacts, loads the run's training matrix with ``dvc_baseline.load`` and
writes the three inputs of ``monitoring.drift_check`` into ``DIR``: ``reference.parquet``, ``reference_manifest.json`` and ``features.json`` (the model's
matrix columns). Nothing is written when any step is refused, and a rerun writes the same bytes.

Exit codes: 0 written, 2 a refused input (the message names the failure), 3 a package is missing, 4 there is nothing to monitor (no Production model, or a
model without features). Run it from the repository root as a module, in an environment with MLflow and the pinned DVC.
"""

from __future__ import annotations

import argparse
import importlib
import json
import os
import shutil
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from monitoring import drift_config
from monitoring.drift_check import HAI_TYPES, POPULATIONS, CheckError, deadline
from scripts.training import dvc_baseline, registry

ROOT = Path(__file__).resolve().parents[1]
MODEL_CONTRACT = ROOT / "data_contracts/training/model_contract.json"
NOTHING_TO_MONITOR = 4


@dataclass(frozen=True)
class Baseline:
    """What was fetched: the Production version's source run, the manifest of its matrix and the model's feature columns."""

    hai_type: str
    run_id: str
    version: str
    manifest: dict[str, Any]
    features: list[str]


def artifact(store: Any, run_id: str, path: str, scratch: Path) -> Any:
    """Download one JSON artifact of the run; any failure to find or read it is refused by its path."""
    try:
        return json.loads(Path(store.download_artifacts(run_id, path, dst_path=str(scratch))).read_bytes())
    except Exception as error:
        raise CheckError("artifact_missing", f"{path} of run {run_id}: {type(error).__name__}: {error}") from None


def write_json(path: Path, document: Any) -> None:
    path.write_text(json.dumps(document, indent=2, sort_keys=True) + "\n")


def fetch(store: Any, hai_type: str, out: Path, *, project: Path = ROOT, deadlines: dvc_baseline.Deadlines | None = None) -> Baseline:
    """Write the baseline inputs for ``hai_type`` into ``out`` and return what was fetched."""
    if hai_type not in HAI_TYPES:
        raise CheckError("hai_type_invalid", f"{hai_type} is not one of {', '.join(HAI_TYPES)}")
    model = json.loads(MODEL_CONTRACT.read_text())["registered_models"][hai_type]
    version = registry.champion(store, model)
    if version is None:
        raise CheckError("no_production_model", f"{model} has no version under the production alias", NOTHING_TO_MONITOR)
    run_id = str(version.run_id)
    with tempfile.TemporaryDirectory(prefix="baseline_") as temporary:
        scratch = Path(temporary)
        columns = artifact(store, run_id, "bundle/feature_list.json", scratch).get("columns")
        manifest = artifact(store, run_id, f"extract/manifest_{hai_type}.json", scratch)
        if not isinstance(columns, list) or any(not isinstance(name, str) or not name for name in columns):
            raise CheckError("artifact_invalid", f"bundle/feature_list.json of run {run_id} must hold a list of column names")
        if not columns:
            raise CheckError("no_features", f"the production model of run {run_id} has no features", NOTHING_TO_MONITOR)
        if manifest.get("hai_type") != hai_type or manifest.get("population") not in POPULATIONS or not isinstance(manifest.get("newest_window_year"), int):
            raise CheckError("manifest_invalid", f"the extract manifest of run {run_id} must name {hai_type}, a population and an integer newest_window_year")
        try:
            matrix = dvc_baseline.load(dvc_baseline.Run(store, run_id), project=project, deadlines=deadlines)
        except dvc_baseline.BaselineError as error:
            raise CheckError("baseline_unloadable", str(error)) from None
        out.mkdir(parents=True, exist_ok=True)
        staged = Path(tempfile.mkdtemp(prefix=".baseline_", dir=out))
        try:
            shutil.copyfile(matrix, staged / "reference.parquet")
            write_json(staged / "reference_manifest.json", manifest)
            write_json(staged / "features.json", columns)
            for name in ("reference.parquet", "reference_manifest.json", "features.json"):
                os.replace(staged / name, out / name)
        finally:
            shutil.rmtree(staged, ignore_errors=True)
    return Baseline(hai_type, run_id, str(version.version), manifest, columns)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--hai-type", required=True)
    parser.add_argument("--tracking-uri", required=True, help="the MLflow tracking and registry address")
    parser.add_argument("--out", type=Path, required=True, help="the folder that receives the three inputs of the drift check")
    parser.add_argument("--config", type=Path, default=drift_config.CONFIG)
    arguments = parser.parse_args(argv)
    try:
        config, _ = drift_config.load(arguments.config)
        try:
            client = importlib.import_module("mlflow.tracking").MlflowClient(tracking_uri=arguments.tracking_uri, registry_uri=arguments.tracking_uri)
        except ImportError as error:
            raise CheckError("dependency_missing", f"the baseline reader needs the mlflow package: {error}", 3) from None
        with deadline(int(config["deadline_seconds"])):
            result = fetch(client, arguments.hai_type, arguments.out)
    except (CheckError, drift_config.ConfigError) as error:
        sys.stderr.write(f"baseline failed: {error}\n")
        return error.code if isinstance(error, CheckError) else 2
    sys.stdout.write(f"baseline {result.hai_type} run {result.run_id} version {result.version} in {arguments.out}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
