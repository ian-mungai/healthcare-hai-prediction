"""Run one bound AWS bronze command using the task identity and the closed namespace inputs."""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict
from pathlib import Path

from scripts.lakehouse import bronze, bronze_writer, checksums, dictionary, smoke

WORKERS = {
    "scripts.lakehouse.bronze": bronze.main,
    "scripts.lakehouse.dictionary": dictionary.main,
    "scripts.lakehouse.checksums": checksums.main,
    "scripts.lakehouse.smoke": smoke.main,
}


def execute(plan: bronze_writer.JobPlan) -> int:
    """Invoke the existing loader entry point with only the arguments produced by the binding planner."""
    return WORKERS[plan.module](list(plan.argv))


def main(argv: list[str] | None = None) -> int:
    """Resolve the immutable run binding and execute or describe its one permitted command."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binding", required=True, type=Path)
    parser.add_argument("--job", required=True, choices=tuple(bronze_writer.MUTATIONS))
    parser.add_argument("--authority-epoch", required=True, type=int, help="current authority epoch verified independently by the worker bootstrap")
    parser.add_argument("--tables", nargs="*", default=[])
    parser.add_argument("--fixture-action", choices=("create", "drop"))
    parser.add_argument("--describe", action="store_true")
    args = parser.parse_args(argv)
    try:
        binding = bronze_writer.Binding.from_document(json.loads(args.binding.read_text()))
        table_map = json.loads((bronze_writer.ROOT / "config/lakehouse/bronze_tables.json").read_text())
        plan = bronze_writer.plan(
            binding, args.job, tables=args.tables, table_map=table_map, authority_epoch=args.authority_epoch, fixture_action=args.fixture_action
        )
        if args.describe:
            sys.stdout.write(json.dumps(asdict(plan), sort_keys=True) + "\n")
            return 0
        return execute(plan)
    except (bronze_writer.BindingError, OSError, json.JSONDecodeError) as error:
        sys.stderr.write(f"AWS bronze command refused: {error}\n")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
