"""Read the current feature windows of one population from a features publication (project decision 21).

``python -m monitoring.current_window --hai-type HAI_1 --population primary --features-run latest --newest-window-year 2023 --features FILE --out DIR`` reads
the selected publication through the feature store reader of ``scripts.features.training_extract`` and writes the inputs of ``monitoring.drift_check``'s
current side into ``DIR``: ``current.parquet`` (``spine_key``, ``window_year`` and the model's feature columns) and ``current_manifest.json``. A row is a
hospital window of the population whose year is after ``--newest-window-year``, the newest window of the baseline. Unlike the training extract, a window
without a published SIR is kept: those newest windows are exactly what has not been scored yet. Nothing is written when a step is refused, and a rerun on
the same publication writes the same manifest and rows.

Exit codes: 0 written, 2 a refused input (the message names the failure), 3 a package or the feature store environment is missing. Run it from the
repository root as a module, in the feature image.
"""

from __future__ import annotations

import argparse
import contextlib
import json
import math
import os
import shutil
import sys
import tempfile
from datetime import date, datetime
from pathlib import Path
from typing import Any

import duckdb

from monitoring import drift_config
from monitoring.drift_check import HAI_TYPES, POPULATIONS, CheckError, deadline, read_features
from scripts.features import training_extract

ROOT = Path(__file__).resolve().parents[1]
DUCKDB_TYPES = {"double": "DOUBLE", "varchar": "VARCHAR", "bigint": "BIGINT", "boolean": "BOOLEAN", "date": "DATE"}


def plain(value: Any) -> Any:
    """Return a JSON value: dates as text, a number that is not finite as missing."""
    if isinstance(value, datetime | date):
        return value.isoformat()
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


def build(root: Path, reader: Any, *, features_run: str, population: str, hai_type: str, newest_window_year: int, features: list[str], out: Path) -> Path:
    """Write the current window files for ``hai_type`` and ``population`` into ``out`` and return the manifest path."""
    if hai_type not in HAI_TYPES:
        raise CheckError("hai_type_invalid", f"{hai_type} is not one of {', '.join(HAI_TYPES)}")
    if population not in POPULATIONS:
        raise CheckError("population_invalid", f"{population} is not one of {', '.join(POPULATIONS)}")
    try:
        references, predictors, _ = training_extract.reference_union(root, reader.schema)
    except training_extract.ExtractError as error:
        raise CheckError("schema_invalid", str(error)) from None
    reference_of = {reference.split(":", 1)[1]: reference for reference in references}
    for name in features:
        if name not in predictors:
            raise CheckError("feature_unknown", f"{name} is not a predictor the feature schema serves")
        if predictors[name]["type"] not in DUCKDB_TYPES:
            raise CheckError("feature_type_unsupported", f"{name} has the type {predictors[name]['type']}")
    selected = None if features_run == "latest" else features_run
    publication = reader.publication(selected)
    if selected is not None and publication.run_id != selected:
        raise CheckError("publication_mismatch", "the feature reader selected another publication")
    if "int_hospital_spine" not in publication.snapshot_set:
        raise CheckError("publication_incomplete", f"publication {publication.run_id} has no int_hospital_spine snapshot")
    history = reader.historical(sorted(reference_of[name] for name in features), publication.run_id)
    if history.publication != publication or history.feature_schema_sha256 != reader.schema_sha256:
        raise CheckError("publication_mismatch", "the historical features came from another publication or schema")
    held = {row["spine_key"]: row for row in history.frame.to_pylist()}
    spine = reader.consistent(publication, lambda: reader.scan(publication, "int_hospital_spine", training_extract.SPINE_FIELDS))
    rows = []
    for entry in sorted(spine, key=lambda item: item["spine_key"]):
        match = training_extract.KEY.fullmatch(entry["spine_key"])
        if match is None or type(entry.get(f"is_{population}_population")) is not bool:
            raise CheckError("spine_invalid", f"the spine row {entry.get('spine_key')!r} has a malformed key or population flag")
        year = int(match[2])
        if not entry[f"is_{population}_population"] or year <= newest_window_year:
            continue
        if entry["spine_key"] not in held:
            raise CheckError("spine_mismatch", f"{entry['spine_key']} is in the population spine and not in the historical features")
        rows.append({"spine_key": entry["spine_key"], "window_year": year, **{name: plain(held[entry["spine_key"]][name]) for name in features}})
    columns = {"spine_key": "varchar", "window_year": "bigint", **{name: predictors[name]["type"] for name in features}}
    manifest = {
        "hai_type": hai_type,
        "population": population,
        "publication_run_id": publication.run_id,
        "features_snapshot_set_sha256": publication.snapshot_set_sha256,
        "newest_window_year": newest_window_year,
        "row_count": len(rows),
        "columns": columns,
    }
    out.mkdir(parents=True, exist_ok=True)
    staged = Path(tempfile.mkdtemp(prefix=".current_", dir=out))
    try:
        lines = staged / "rows.jsonl"
        lines.write_text("".join(json.dumps(row, sort_keys=True) + "\n" for row in rows))
        typed = {name: DUCKDB_TYPES[kind] for name, kind in columns.items()}
        duckdb.read_json(str(lines), columns=typed, format="newline_delimited").write_parquet(str(staged / "current.parquet"))
        (staged / "current_manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
        for name in ("current.parquet", "current_manifest.json"):
            os.replace(staged / name, out / name)
    finally:
        shutil.rmtree(staged, ignore_errors=True)
    return out / "current_manifest.json"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--hai-type", required=True)
    parser.add_argument("--population", required=True)
    parser.add_argument("--features-run", required=True, help="latest or a publication run id")
    parser.add_argument("--newest-window-year", type=int, required=True, help="the newest window year of the baseline")
    parser.add_argument("--features", type=Path, required=True, help="the baseline's features.json")
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--root", type=Path, default=ROOT)
    parser.add_argument("--config", type=Path, default=drift_config.CONFIG)
    arguments = parser.parse_args(argv)
    try:
        config, _ = drift_config.load(arguments.config)
        features = read_features(arguments.features)
        with deadline(int(config["deadline_seconds"])), contextlib.ExitStack() as stack:
            try:
                reader = stack.enter_context(training_extract.live_reader(arguments.root, os.environ, arguments.features_run))
            except (ImportError, KeyError, RuntimeError) as error:
                raise CheckError(
                    "dependency_missing", f"the current window reader needs the feature image and its environment: {type(error).__name__}: {error}", 3
                ) from None
            target = build(
                arguments.root,
                reader,
                features_run=arguments.features_run,
                population=arguments.population,
                hai_type=arguments.hai_type,
                newest_window_year=arguments.newest_window_year,
                features=features,
                out=arguments.out,
            )
    except (CheckError, drift_config.ConfigError) as error:
        sys.stderr.write(f"current window failed: {error}\n")
        return error.code if isinstance(error, CheckError) else 2
    sys.stdout.write(f"current window {target}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
