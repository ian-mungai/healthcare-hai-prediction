"""Register the tables of a committed bronze load in the bronze Glue mirror database, the way the silver, gold and features publishes do.

``bronze.main`` calls ``mirror_after_load`` once, after the load and its metadata generation are committed. The switch, the time budget, the Glue
registration and the failure report are those of ``glue_mirror`` (``tables_to_mirror``, ``Budget``, ``mirror``, ``explain``); this module only names
what a bronze load mirrors. Only the real ``bronze`` namespace is mirrored: a qualification run (``bronze_e2e``), the dictionary namespaces and the
other test namespaces mirror nothing, and nothing runs while ``enabled`` is false in ``config/lakehouse/athena_mirror.json``.

The load is already committed, so the hook never raises and never undoes it. It reads each table's committed snapshot from the load itself, so a table
that the load left out is simply not mirrored, and a rerun of the load registers whatever is missing or stale. The report (status, reason, one outcome
per table) goes into the loader report, standard error when it failed and, through the check phase, the run record.
"""

from __future__ import annotations

import contextlib
import sys
import time
from collections.abc import Mapping, Sequence
from typing import Any

from scripts.lakehouse import glue_mirror

REAL_NAMESPACE = "bronze"
# The run that a registration names when the loader was started by hand, with no bronze run ID.
HAND_RUN = "bronze_by_hand"


def mirror_after_load(
    spark: Any,
    namespace: str,
    names: Sequence[str],
    snapshots: Mapping[str, int],
    run_id: str | None = None,
    source: Any = None,
    glue: Any = None,
    config: Any = None,
) -> dict[str, Any]:
    """Register the committed snapshot of each loaded table of ``names`` in the bronze Glue mirror database. Never raises.

    ``snapshots`` maps each loaded table to the snapshot that the load committed; a table of ``names`` without one is not mirrored. One absolute deadline
    starts here, before the first Spark read, and bounds every Spark and Glue call, so no step blocks the job past it and no write is issued after it.
    """
    entered = time.monotonic()
    try:
        config = config or glue_mirror.load_config()
        mode = "real" if namespace == REAL_NAMESPACE else "qualify"
        wanted, reason = glue_mirror.tables_to_mirror(config, namespace, mode, [name for name in names if name in snapshots])
        if not wanted:
            return {"status": "skipped", "reason": reason, "tables": {}}
        budget = glue_mirror.Budget(config.mirror_seconds, started=entered)
        if source is None:
            from scripts.lakehouse.session import CATALOG

            source = glue_mirror.SparkSource(spark, CATALOG, namespace)
        if glue is None:
            glue = budget.run("creating the Glue client", glue_mirror.glue_client)
        report = glue_mirror.mirror(config, namespace, mode, {name: int(snapshots[name]) for name in wanted}, source, glue, run_id or HAND_RUN, budget)
    except Exception as error:
        report = {"status": "failed", "reason": glue_mirror.explain(error), "tables": {}}
    if report["status"] == "failed":
        with contextlib.suppress(OSError):
            sys.stderr.write(f"bronze load: the Glue mirror FAILED and the load stays committed: {report['reason']}. Rerun the load to retry the mirror.\n")
    return report
