"""Publish the features layer through candidate branches and record only the committed snapshot set.

The features publish is the gold publish engine with a third contract (``data_contracts/lakehouse/features_publish.json``), as the silver publish is: the steps
``candidates``, ``reconcile``, ``backup`` and ``commit`` are ``gold_publish``'s own, and ``record`` writes ``features_snapshot.json`` after comparing the commit
artifact with the catalog. The pin of the gold input is ``scripts/features/features_input.py``. The writer principal is ``features_writer``.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from collections.abc import Sequence
from dataclasses import replace
from pathlib import Path
from typing import Any

from scripts.lakehouse import silver_publish
from scripts.lakehouse.gold_publish import (
    MODES,
    Context,
    Plan,
    build_store,
    catalog_backup,
    commit_publish,
    load_plan,
    publish_candidates,
    reconcile_candidates,
)
from scripts.lakehouse.gold_publish_record import PublishError

CONTRACT = Path(__file__).resolve().parents[2] / "data_contracts/lakehouse/features_publish.json"
NAMESPACES = {"publish_fixture": "features_e2e", "real": "features"}
WRITER = "features_writer"


def publish_plan(mode: str, contract: Path = CONTRACT) -> Plan:
    """Return the fixed features plan with backups scoped to that mode's storage prefix; any other mode or namespace mapping is refused."""
    plan = load_plan(contract)
    if mode not in MODES or dict(plan.namespaces) != NAMESPACES or json.loads(contract.read_text())["publish"].get("writer_principal") != WRITER:
        raise PublishError("the features publish mode, namespace or writer principal is invalid")
    return replace(plan, backup_prefix=f"lakehouse/{plan.namespaces[mode]}/catalog_backups/")


def record_committed(ctx: Context, path: Path) -> dict[str, Any]:
    """Record this producer's committed set after comparing its commit artifact with the catalog (the silver check, worded for features)."""
    try:
        return silver_publish.record_committed(ctx, path)
    except PublishError as error:
        raise PublishError(str(error).replace("silver", "features")) from error


def main(argv: Sequence[str] | None = None) -> int:
    """Run one features publish phase or record the verified committed set."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("step", choices=("candidates", "reconcile", "backup", "commit", "record"))
    parser.add_argument("--run", required=True)
    parser.add_argument("--mode", required=True, choices=MODES)
    parser.add_argument("--sealed-sha256", default="")
    parser.add_argument("--accepted-result-sha256", default="")
    parser.add_argument("--publish-dir", type=Path, default=Path("/workspace/out/publish"))
    parser.add_argument("--record", type=Path, default=Path("/workspace/out/features_snapshot.json"))
    parser.add_argument("--local-warehouse", type=Path)
    args = parser.parse_args(list(sys.argv[1:] if argv is None else argv))
    if args.local_warehouse is not None and (args.mode != "publish_fixture" or os.environ.get("POLARIS_URI")):
        parser.error("a local warehouse is allowed only in publish_fixture without a remote catalog")
    try:
        from scripts.lakehouse.session import spark_session

        plan = publish_plan(args.mode)
        spark = spark_session(f"features_{args.step}", args.local_warehouse)
        try:
            publishing = args.step != "record"
            ctx = Context(
                spark,
                plan,
                args.run,
                args.mode,
                args.publish_dir,
                args.sealed_sha256,
                build_store(plan, args.local_warehouse) if args.step == "backup" else None,
                create_namespace=args.local_warehouse is not None,
                needs_sealed=publishing,
                accepted_result_sha256=args.accepted_result_sha256,
            )
            if args.step == "record":
                result = record_committed(ctx, args.record)
            else:
                result = {"candidates": publish_candidates, "reconcile": reconcile_candidates, "backup": catalog_backup, "commit": commit_publish}[args.step](
                    ctx
                )
            output = {"step": args.step, "tables": len(result.get("snapshot_set", {})) or result.get("tables", 0)}
            if args.step == "record":
                output["sha256"] = hashlib.sha256(args.record.read_bytes()).hexdigest()
            sys.stdout.write(json.dumps(output) + "\n")
        finally:
            spark.stop()
    except PublishError as error:
        sys.stderr.write(f"features publish: {error}\n")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
