"""Print the launch plan's memory figures: ask the launcher, which alone reads Docker, for the plan.

    .venv/bin/python -m scripts.lakehouse.memory_budget_cli            # print DuckDB's limit, for example 26GB
    .venv/bin/python -m scripts.lakehouse.memory_budget_cli --spark    # print Spark's heap, for example 24g
    .venv/bin/python -m scripts.lakehouse.memory_budget_cli --explain  # also print the figures it comes from
    .venv/bin/python -m scripts.lakehouse.memory_budget_cli --launch   # container bytes, DuckDB, Spark, CPU count
    .venv/bin/python -m scripts.lakehouse.memory_budget_cli --launch-json  # the same plan with its evidence

The arithmetic is in ``scripts/lakehouse/memory_budget.py``. The command line is a separate module because it imports the launcher, which
the Spark driver (``bronze_run``) must not pull in through ``memory_budget``.
"""

from __future__ import annotations

import argparse
import json
import sys


def main() -> int:
    """Print DuckDB's limit or Spark's heap, and the figures behind it when asked; the launcher reads Docker."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--spark", action="store_true", help="print Spark's job heap instead of DuckDB's limit")
    parser.add_argument("--explain", action="store_true", help="also print the figures the limit comes from")
    output = parser.add_mutually_exclusive_group()
    output.add_argument("--launch", action="store_true", help="print container bytes, DuckDB memory, Spark heap and CPU count")
    output.add_argument("--launch-json", action="store_true", help="print the complete launch plan as JSON")
    parser.add_argument("--test-registry", help="a test registry folder")
    args = parser.parse_args()
    from orchestration import launcher

    try:
        plan = launcher.load_client(args.test_registry).call("plan")
    except launcher.ClientError as error:
        sys.stderr.write(f"memory budget: {error}\n")
        return 1
    environment = plan["environment"]
    if args.launch or args.launch_json:
        sys.stdout.write((json.dumps(plan) if args.launch_json else " ".join(environment.values())) + "\n")
        return 0
    if args.explain:
        sys.stderr.write(json.dumps(plan["budget"]) + "\n")
    sys.stdout.write(environment["SPARK_JOB_MEMORY" if args.spark else "DUCKDB_MEMORY_LIMIT"] + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
