"""Hand the status of an OpenMetadata ingestion workflow to the exit push of the metadata job: entities ingested and failures of its source and sink.

The workflow objects of the ingestion framework keep a status with the records and failures of each step (``source.get_status()``, ``sink.get_status()``).
They are only counted: no entity name or failure text becomes a label, and anything unexpected adds nothing. Never raises.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from scripts import metrics_push

SOURCES = ("glue", "dbt", "raw_files")


def step_counts(step: Any) -> tuple[int, int]:
    status = step.get_status()
    return len(status.records), len(status.failures)


def record(config: Mapping[str, Any], workflow: Any) -> None:
    """The entities and failures of one workflow, labelled with the type of its source when it is one of the three sources of the job."""
    try:
        source = str(config.get("source", {}).get("type", ""))
        if source not in SOURCES:
            return
        entities, failures = step_counts(workflow.source)
        failures += step_counts(workflow.sink)[1]
        metrics_push.add("hai_openmetadata_ingest_entities", entities, source=source)
        metrics_push.add("hai_openmetadata_ingest_failures", failures, source=source)
    except Exception:
        return
