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

from scripts.features import feature_schema, feature_sets
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


def publish_plan(mode: str, contract: Path = CONTRACT, feature_set: str = "") -> Plan:
    """Return the fixed features plan with backups scoped to that mode's storage prefix; any other mode or namespace mapping is refused.

    ``feature_set`` limits the plan to the tables of that set (``scripts.features.feature_sets``); empty keeps the whole contract."""
    plan = load_plan(contract)
    if mode not in MODES or dict(plan.namespaces) != NAMESPACES or json.loads(contract.read_text())["publish"].get("writer_principal") != WRITER:
        raise PublishError("the features publish mode, namespace or writer principal is invalid")
    schema = feature_schema.load_schema(contract.parents[2])
    plan = replace(plan, backup_prefix=f"lakehouse/{plan.namespaces[mode]}/catalog_backups/", column_change_notes=feature_sets.column_change_notes(schema))
    if not feature_sets.checked(feature_set):
        return plan
    return feature_sets.scoped_plan(plan, feature_set, schema)


def record_committed(ctx: Context, path: Path, feature_set: str = "", base_run: str | None = None) -> dict[str, Any]:
    """Record this producer's committed set after comparing its commit artifact with the catalog (the silver check, worded for features).

    A run with a feature set records the set; a model set also records ``base_run``, the base run it read."""
    extra: dict[str, Any] = {}
    if feature_sets.checked(feature_set):
        extra["feature_set"] = feature_set
        if feature_set != feature_sets.BASE:
            extra["base_run"] = base_run
    try:
        return silver_publish.record_committed(ctx, path, extra)
    except PublishError as error:
        raise PublishError(str(error).replace("silver", "features")) from error


def base_run_of(pin: Path | None, feature_set: str) -> str | None:
    """The base run a model set read, from its base pin; base and the single path read none. A model set without a readable pin is refused."""
    if feature_sets.checked(feature_set) in ("", feature_sets.BASE):
        return None
    if pin is None or not pin.is_file():
        raise PublishError(f"the record of model set {feature_set} needs the base pin it read (--base-pin)")
    run = json.loads(pin.read_text()).get("source_run_id")
    if not isinstance(run, str):
        raise PublishError("the base pin names no base run")
    return run


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
    parser.add_argument("--feature-set", default=os.environ.get(feature_sets.VARIABLE, ""), help="the set this job publishes (empty: the single path)")
    parser.add_argument("--base-pin", type=Path, help="record: the base pin of a model set run, which names the base run it read")
    args = parser.parse_args(list(sys.argv[1:] if argv is None else argv))
    if args.local_warehouse is not None and (args.mode != "publish_fixture" or os.environ.get("POLARIS_URI")):
        parser.error("a local warehouse is allowed only in publish_fixture without a remote catalog")
    try:
        from scripts.lakehouse.session import spark_session

        plan = publish_plan(args.mode, feature_set=args.feature_set)
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
                result = record_committed(ctx, args.record, args.feature_set, base_run_of(args.base_pin, args.feature_set))
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
