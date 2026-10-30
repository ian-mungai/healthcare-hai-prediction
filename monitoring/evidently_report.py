"""The only module that imports Evidently: one dataset drift report over two frames, read back as one result per feature.

The report is Evidently's ``DataDriftPreset`` from its ``legacy`` namespace, whose result layout (``drift_by_columns``) is documented and stable in the pinned
release. The tests are named explicitly because Evidently's own choice depends on the row count: above 1000 reference rows it picks Wasserstein distance and
Jensen-Shannon distance, not the Kolmogorov-Smirnov and chi-square tests of decision 21.
"""

from __future__ import annotations

import importlib
import importlib.metadata
from collections.abc import Mapping, Sequence
from typing import Any


class DependencyMissing(Exception):
    """A package the report needs is not installed in this environment."""


def ensure() -> None:
    """Fail with the missing package's name before any data is read into memory."""
    try:
        importlib.import_module("pandas")
        importlib.import_module("evidently.legacy.report")
    except ImportError as error:
        raise DependencyMissing(f"a package the report needs is not installed in this environment: {error}") from None


def run(reference: Any, current: Any, numerical: Sequence[str], categorical: Sequence[str], config: Mapping[str, Any]) -> tuple[str, dict[str, dict[str, Any]]]:
    """Return the Evidently version and, for each feature, its column type, test name, threshold, score and drift flag."""
    try:
        preset = importlib.import_module("evidently.legacy.metric_preset").DataDriftPreset
        mapping = importlib.import_module("evidently.legacy.pipeline.column_mapping").ColumnMapping
        report_class = importlib.import_module("evidently.legacy.report").Report
        version = importlib.metadata.version("evidently")
    except (ImportError, importlib.metadata.PackageNotFoundError) as error:
        raise DependencyMissing(f"evidently is not installed in this environment: {error}") from None
    threshold = float(config["test_threshold"])
    report = report_class(
        metrics=[
            preset(
                columns=[*numerical, *categorical],
                drift_share=float(config["drift_share"]),
                num_stattest=config["numeric_test"],
                cat_stattest=config["categorical_test"],
                num_stattest_threshold=threshold,
                cat_stattest_threshold=threshold,
            )
        ]
    )
    report.run(
        reference_data=reference, current_data=current, column_mapping=mapping(numerical_features=list(numerical), categorical_features=list(categorical))
    )
    tables = [item["result"]["drift_by_columns"] for item in report.as_dict()["metrics"] if "drift_by_columns" in item["result"]]
    if len(tables) != 1:
        raise RuntimeError(f"the report holds {len(tables)} drift tables, expected one")
    found: dict[str, dict[str, Any]] = {}
    for name, entry in tables[0].items():
        found[name] = {
            "column_type": "numerical" if name in numerical else "categorical",
            "test": str(entry["stattest_name"]),
            "threshold": float(entry["stattest_threshold"]),
            "score": float(entry["drift_score"]),
            "drifted": bool(entry["drift_detected"]),
        }
    return version, found
