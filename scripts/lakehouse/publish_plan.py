"""Read-only plan of a silver, gold or features publish: what the catalog holds now against what the export would write.

    python -m scripts.lakehouse.publish_plan --layer silver|gold|features --run <run ID> --mode real|publish_fixture --sealed-sha256 <digest> \
        --accepted-result-sha256 <digest> --publish-dir <folder> --output <file> [--local-warehouse <folder>]

It runs in the Spark image beside the publish modules and checks the export the way ``candidates`` does. It then only reads: the namespace, each table's row
count, a content fingerprint of the table and of the export, and the counts of keys that the merge would add and delete. It creates no namespace, table
or branch and writes no catalog metadata. Its only file is ``--output``, in the task's own run folder. A table it cannot read is recorded with an ``error`` and
the plan lists it as unknown.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from scripts.lakehouse import gold_publish
from scripts.lakehouse.gold_publish import Context, PublishError

COUNT = "SELECT count(*) FROM {source}"
ADDED = "SELECT count(*) FROM {view} s LEFT ANTI JOIN {table} t ON {on}"
REMOVED = "SELECT count(*) FROM {table} t LEFT ANTI JOIN {view} s ON {on}"


def number(ctx: Context, statement: str) -> int:
    return int(ctx.spark.sql(statement).collect()[0][0])


def table_state(ctx: Context, name: str, item: dict[str, Any], view: str) -> dict[str, Any]:
    """Whether the table exists and, if it does, how its rows differ from the export; only reads."""
    if not gold_publish.table_exists(ctx, name):
        return {"exists": False}
    table = ctx.fq(name)
    on = " AND ".join(f"t.{gold_publish.quote(column)} <=> s.{gold_publish.quote(column)}" for column in item["merge_key"])
    same = gold_publish.fingerprint(ctx, item["columns"], table) == gold_publish.fingerprint(ctx, item["columns"], view)
    return {
        "exists": True,
        "rows_now": number(ctx, COUNT.format(source=table)),
        "same": same,
        "added": number(ctx, ADDED.format(view=view, table=table, on=on)),
        "removed": number(ctx, REMOVED.format(view=view, table=table, on=on)),
    }


def plan_state(ctx: Context) -> dict[str, Any]:
    manifest, _, export_dir = gold_publish.verified_export(ctx)
    try:
        ctx.sql(gold_publish.NAMESPACE_EXISTS).collect()
        namespace_exists = True
    except Exception as error:
        if not any(marker in str(error) for marker in gold_publish.ABSENT_NAMESPACE):
            raise
        namespace_exists = False
    tables: dict[str, Any] = {}
    for name, item in manifest["tables"].items():
        try:
            tables[name] = table_state(ctx, name, item, gold_publish.parquet_view(ctx, export_dir, name, item))
        except Exception as error:  # one unreadable table is listed as unknown; the others are still read
            tables[name] = {"error": f"{type(error).__name__}: {str(error)[:200]}"}
    runs = ctx.plan.runs_table
    runs_state: dict[str, Any] = {"exists": gold_publish.table_exists(ctx, runs)}
    if runs_state["exists"]:
        runs_state["rows_now"] = number(ctx, COUNT.format(source=ctx.fq(runs)))
        runs_state["run_committed"] = gold_publish.committed_run(ctx) is not None
    else:
        runs_state |= {"rows_now": 0, "run_committed": False}
    return {"namespace_exists": namespace_exists, "tables": tables, "runs_table": runs_state}


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--layer", required=True, choices=("silver", "gold", "features"))
    parser.add_argument("--run", required=True)
    parser.add_argument("--mode", required=True, choices=gold_publish.MODES)
    parser.add_argument("--sealed-sha256", required=True)
    parser.add_argument("--accepted-result-sha256", required=True)
    parser.add_argument("--publish-dir", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--local-warehouse", type=Path)
    args = parser.parse_args(argv)
    if args.local_warehouse is not None and (args.mode != "publish_fixture" or os.environ.get("POLARIS_URI")):
        parser.error("a local warehouse is allowed only in publish_fixture without a remote catalog")
    try:
        from scripts.features import features_publish
        from scripts.lakehouse import silver_publish
        from scripts.lakehouse.session import spark_session

        plans = {
            "silver": lambda: silver_publish.publish_plan(args.mode),
            "gold": gold_publish.load_plan,
            "features": lambda: features_publish.publish_plan(args.mode),
        }
        spark = spark_session(f"{args.layer}_publish_plan", args.local_warehouse)
        try:
            ctx = Context(
                spark, plans[args.layer](), args.run, args.mode, args.publish_dir, args.sealed_sha256, accepted_result_sha256=args.accepted_result_sha256
            )
            state = plan_state(ctx)
        finally:
            spark.stop()
        args.output.write_text(json.dumps(state, sort_keys=True) + "\n")
    except PublishError as error:
        sys.stderr.write(f"publish plan: {error}\n")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
