"""Run IDs, digests and request documents that the direct launch cases share, with the two table readers they use to pick real bronze tables."""

from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
RUN = "20261030T100000Z-abcdef01"
SILVER_RUN = "20261030T100000Z-0badcafe"
GOLD_RUN = "20261030T100000Z-90d0c0de"
BRONZE_RUN = "20261030T090000Z-b0b0b0b0"
PROMOTE_RUN = "20261030T110000Z-9a9a9a9a"
DRIFT_RUN = "20261030T120000Z-d71f70a1"
SERVE_RUN = "20261030T120000Z-5e5e5e5e"
DIGEST = "ab" * 32
INVENTORY = "cd" * 32
# Every DAG step launches on AWS, so the training parameters name the aws MLflow lane (lead decision, Nov 7 2026).
TRAIN_PARAMS = {
    "run_id": RUN,
    "mode": "real",
    "features_run": "latest",
    "population": "primary",
    "hai_types": ["HAI_1"],
    "trigger": "manual",
    "mlflow": "aws",
    "model": "m2",
}


def collections_of_table_map() -> dict[str, str]:
    """The table map's collection of each table, from the committed file."""
    return {entry["table"]: entry["collection"] for entry in json.loads((ROOT / "config/lakehouse/bronze_tables.json").read_text())["tables"]}


def table_pair() -> tuple[list[str], str]:
    """Two tables of one collection and one table of another, in table map order."""
    by_collection: dict[str, list[str]] = {}
    for table, collection in collections_of_table_map().items():
        by_collection.setdefault(collection, []).append(table)
    pair = next(tables[:2] for tables in by_collection.values() if len(tables) >= 2)
    other = next(tables[0] for tables in by_collection.values() if tables[0] not in pair)
    return pair, other
