"""Hold synthetic units in a run the way acceptance does: write the queue and a sealed classification beside it, then accept exactly those bytes."""

from __future__ import annotations

import json
from collections.abc import Collection, Mapping

from scripts.acquisition import redownload as harness
from scripts.acquisition import unit_classification
from scripts.acquisition.s3_store import encoded_json


def classification_body(queue_body: bytes, units: list[dict], classes: Mapping[str, str] | None = None, unreviewed: Collection[str] = ()) -> bytes:
    """A sealed classification. Active units are reviewed non personal unless ``classes`` says otherwise or they are listed as unreviewed."""
    chosen = classes or {}
    document = {
        "kind": unit_classification.KIND,
        "queue_sha256": harness.digest(queue_body),
        "reviewed_by": "synthetic owner",
        "units": {
            unit["snapshot_id"]: {
                "classification": chosen.get(unit["snapshot_id"], unit_classification.NON_PERSONAL),
                "status": unit_classification.PROPOSED if unit["snapshot_id"] in unreviewed else unit_classification.REVIEWED,
                "source": "synthetic",
            }
            for unit in units
            if unit["disposition"] in harness.ACTIVE
        },
    }
    return encoded_json({**document, "classification_sha256": unit_classification.body_digest(document)})


def hold(run: harness.Run, units: list[dict], classes: Mapping[str, str] | None = None, unreviewed: Collection[str] = (), number: int = 2) -> harness.Accepted:
    """Write the queue and its classification, make the run hold those bytes and return what it holds."""
    body = json.dumps({"run": number, "queue": units}).encode()
    run.queue_path.write_bytes(body)
    sealed = classification_body(body, units, classes, unreviewed)
    unit_classification.classification_path(run.queue_path).write_bytes(sealed)
    run.cache["accepted"] = harness.Accepted.build(body, sealed)
    return run.cache["accepted"]
