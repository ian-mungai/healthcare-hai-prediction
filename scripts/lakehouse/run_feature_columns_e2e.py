"""End-to-end run of the feature-column check: the committed files pass and each planted defect fails for its own reason.

Run from the repository root:

    .venv/bin/python -m scripts.lakehouse.run_feature_columns_e2e [--output-directory DIR]

The run builds the feature phase once on empty gold tables (the steps of ``feature_columns.build_database``, contracts off) and compares the columns the
database gives with the committed YAML, then plants one defect per case in a copy of the documented columns: an undocumented column, an undocumented model, a
stale column, a stale model, a wrong data type, a missing data type and a missing description. Two further cases change a model's SQL in a scratch copy of the
dbt project, so that it gives one more column than the YAML documents: the check run without contracts reports the column, and the build with the contract on
fails by itself. One case plants a ``run_query`` in a model and requires the check to stop at admission (``feature_columns.admission``) with no build.
A last case adds a post-hook to a bridge filter model that inserts the raw bridge rows of every seed and requires the filter table to hold
only its own seed, because the check builds the filter tables itself in DuckDB. The artifact (``report.json`` in the output folder, default
``data/e2e/feature_columns/<UTC time>/``) lists each case with the message that matched. The run needs no Docker and no AWS; it starts dbt and DuckDB from
``.tools/sqlfluff`` and takes about three minutes.
"""

from __future__ import annotations

import argparse
import copy
import json
import shutil
import sys
import tempfile
import time
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from scripts.lakehouse import feature_columns as columns
from scripts.lakehouse import feature_shards

Docs = dict[str, dict[str, dict[str, str]]]
MODEL = "int_spine_hospital_measures"
COLUMN = "value_code"
SQL_MODEL = f"dbt/models/features/{MODEL}.sql"
SCRATCH_FILES = ("data_contracts/lakehouse/gold_contract.json",)
FILTER_MODEL = "dbt/models/features/bridge/feature_bridge_geography_measures.sql"
FILTER_HOOK = "{{ config(materialized='table', post_hook=\"insert into {{ this }} select * from {{ ref('bridge_measure_control') }}\") }}"
ADMISSION_STARTS = {"dbt parse", "dbt ls tag:features", "feature_allowlist"}
SEEDS_QUERY = "import duckdb,sys;print(sorted(r[0] for r in duckdb.connect(sys.argv[1], read_only=True).execute(sys.argv[2]).fetchall()))"


def cases() -> list[tuple[str, Callable[[Docs], None], str | None]]:
    """The defects in the documented columns with the message each must produce; ``None`` is the committed state, which must pass."""

    def drop_column(docs: Docs) -> None:
        del docs[MODEL][COLUMN]

    def drop_model(docs: Docs) -> None:
        del docs[MODEL]

    def add_column(docs: Docs) -> None:
        docs[MODEL]["column_that_is_gone"] = {"data_type": "varchar", "description": "A column the model no longer gives."}

    def add_model(docs: Docs) -> None:
        docs["int_model_that_is_gone"] = {"x": {"data_type": "varchar", "description": "A model with no SQL file."}}

    def retype(docs: Docs) -> None:
        docs[MODEL][COLUMN]["data_type"] = "bigint"

    def no_type(docs: Docs) -> None:
        docs[MODEL][COLUMN]["data_type"] = ""

    def no_description(docs: Docs) -> None:
        docs[MODEL][COLUMN]["description"] = ""

    return [
        ("committed_files", lambda docs: None, None),
        ("undocumented_column", drop_column, f"{MODEL}.{COLUMN} (varchar) is an output column that the YAML does not document"),
        ("undocumented_model", drop_model, f"model {MODEL} has no entry in the YAML"),
        ("stale_column", add_column, f"{MODEL}.column_that_is_gone is documented in the YAML but the model does not give it"),
        ("stale_model", add_model, "the YAML documents model int_model_that_is_gone, which has no SQL file"),
        ("wrong_data_type", retype, f"{MODEL}.{COLUMN} is varchar in the model but data_type bigint in the YAML"),
        ("missing_data_type", no_type, f"{MODEL}.{COLUMN} has no data_type in the YAML"),
        ("missing_description", no_description, f"{MODEL}.{COLUMN} has no description in the YAML"),
    ]


def scratch_root(folder: Path) -> Path:
    """A copy of the dbt project whose hospital measures model gives one more column than the YAML documents."""
    root = folder / "root"
    shutil.copytree(columns.REPO_ROOT / "dbt", root / "dbt", ignore=shutil.ignore_patterns("target", "logs", "dbt_packages"))
    for name in SCRATCH_FILES:
        (root / name).parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(columns.REPO_ROOT / name, root / name)
    path = root / SQL_MODEL
    text = path.read_text()
    marker = "    spine.spine_key || ':' || controls.measure_source || ':' || controls.measure_control || ':' || controls.field as alignment_key\n"
    if marker not in text:
        raise RuntimeError(f"{SQL_MODEL} does not hold the line this case extends")
    path.write_text(text.replace(marker, marker.rstrip("\n") + ",\n    1 as planted_extra_column\n", 1))
    return root


def rejected_project_case(folder: Path) -> dict[str, Any]:
    """The column check admits the project before it builds: a ``run_query`` in a model is reported, and no dbt run, compile or worker starts."""
    root = folder / "rejected"
    shutil.copytree(columns.REPO_ROOT / "dbt", root / "dbt", ignore=shutil.ignore_patterns("target", "logs", "dbt_packages"))
    path = root / SQL_MODEL
    path.write_text("{% if execute %}{% do run_query('select 1') %}{% endif %}\n" + path.read_text())
    columns.STARTED_STEPS.clear()
    problems, _ = columns.check(root)
    labels = sorted(set(columns.STARTED_STEPS))
    matched = [problem for problem in problems if f"model.hai.{MODEL} calls run_query()" in problem]
    return {
        "case": "column_check_does_not_build_a_rejected_project",
        "expected": f"model.hai.{MODEL} calls run_query() and only the admission steps start",
        "matched": matched[:1],
        "process_starts": labels,
        "ok": bool(matched) and set(labels) <= ADMISSION_STARTS,
    }


def hooked_filter_case(folder: Path) -> dict[str, Any]:
    """A post-hook on a bridge filter model must not reach its table: the table holds the rows of its own seed only."""
    root = folder / "hooked"
    shutil.copytree(columns.REPO_ROOT / "dbt", root / "dbt", ignore=shutil.ignore_patterns("target", "logs", "dbt_packages"))
    for name in SCRATCH_FILES:
        (root / name).parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(columns.REPO_ROOT / name, root / name)
    text = (root / FILTER_MODEL).read_text()
    if "{{ config(materialized='table') }}" not in text:
        raise RuntimeError(f"{FILTER_MODEL} does not hold the config this case changes")
    (root / FILTER_MODEL).write_text(text.replace("{{ config(materialized='table') }}", FILTER_HOOK, 1))
    build = folder / "hooked_build"
    build.mkdir()
    database = columns.build_database(root, build)
    seeds = columns.step(
        columns.REPO_ROOT / ".tools/sqlfluff/bin/python",
        ["-c", SEEDS_QUERY, str(database), "select distinct source_seed from main.feature_bridge_geography_measures"],
        columns.REPO_ROOT,
        columns.tool_environment(columns.REPO_ROOT),
        "read the filter table",
    ).strip()
    return {
        "case": "post_hook_on_a_filter_model_leaves_its_table_alone",
        "expected": "['geography_measures']",
        "seeds": seeds,
        "ok": seeds == "['geography_measures']",
    }


def case_names() -> list[str]:
    """List the complete column case inventory without building a database."""
    return [name for name, _, _ in cases()] + [
        "model_gives_an_extra_column",
        "contract_stops_the_build_on_an_extra_column",
        "column_check_does_not_build_a_rejected_project",
        "post_hook_on_a_filter_model_leaves_its_table_alone",
    ]


def run(output: Path, only: list[str] | None = None, shard: str = "") -> dict[str, Any]:
    """Run the selected cases and return their report with exact shard membership."""
    inventory = case_names()
    selected = feature_shards.select(inventory, shard, only)
    before = feature_shards.snapshot("columns")
    results: list[dict[str, Any]] = []
    models = columns.sql_models(columns.REPO_ROOT)
    docs = columns.documented(columns.REPO_ROOT / columns.MODELS_YAML)
    with tempfile.TemporaryDirectory(prefix="feature_columns_e2e_") as folder:
        base = Path(folder) / "base"
        base.mkdir()
        documented_cases = [case for case in cases() if case[0] in selected]
        built = columns.read_columns(columns.build_database(columns.REPO_ROOT, base), models) if documented_cases else {}
        for name, damage, expected in documented_cases:
            with feature_shards.timed_case("columns", name) as started:
                planted: Docs = copy.deepcopy(docs)
                damage(planted)
                problems = columns.compare(models, built, planted)
                if expected is None:
                    results.append({"case": name, "expected": "no problem", "problems": problems[:3], "ok": not problems})
                else:
                    matched = [problem for problem in problems if expected in problem]
                    results.append(
                        {"case": name, "expected": expected, "matched": matched[:1], "problems": len(problems), "ok": len(matched) == 1 and len(problems) == 1}
                    )
                results[-1]["seconds"] = round(time.monotonic() - started, 3)
        if {"model_gives_an_extra_column", "contract_stops_the_build_on_an_extra_column"}.intersection(selected):
            root = scratch_root(Path(folder))
        if "model_gives_an_extra_column" in selected:
            with feature_shards.timed_case("columns", "model_gives_an_extra_column") as started:
                problems, counts = columns.check(root)
                expected = f"{MODEL}.planted_extra_column (integer) is an output column that the YAML does not document"
                matched = [problem for problem in problems if expected in problem]
                results.append(
                    {
                        "case": "model_gives_an_extra_column",
                        "expected": expected,
                        "matched": matched[:1],
                        "problems": len(problems),
                        "counts": counts,
                        "ok": len(matched) == 1,
                    }
                )
                results[-1]["seconds"] = round(time.monotonic() - started, 3)
        if "contract_stops_the_build_on_an_extra_column" in selected:
            with feature_shards.timed_case("columns", "contract_stops_the_build_on_an_extra_column") as started:
                contract_folder = Path(folder) / ("contract_" + "long_working_path_" * 10)
                contract_folder.mkdir()
                try:
                    columns.build_database(root, contract_folder, contract=True)
                    results.append(
                        {"case": "contract_stops_the_build_on_an_extra_column", "expected": "dbt contract error", "ok": False, "error": "the build passed"}
                    )
                except columns.BuildError as error:
                    message = str(error)
                    diagnostic = message.removeprefix("dbt run tag:features failed: ")
                    results.append(
                        {
                            "case": "contract_stops_the_build_on_an_extra_column",
                            "expected": "dbt run fails with a bounded contract error naming planted_extra_column despite a long working path",
                            "error": message,
                            "diagnostic_characters": len(diagnostic),
                            "working_path_characters": len(str(contract_folder)),
                            "ok": "planted_extra_column" in message and "contract" in message.lower() and len(diagnostic) <= 1200,
                        }
                    )
                results[-1]["seconds"] = round(time.monotonic() - started, 3)
        if "column_check_does_not_build_a_rejected_project" in selected:
            with feature_shards.timed_case("columns", "column_check_does_not_build_a_rejected_project") as started:
                results.append(rejected_project_case(Path(folder)))
                results[-1]["seconds"] = round(time.monotonic() - started, 3)
        if "post_hook_on_a_filter_model_leaves_its_table_alone" in selected:
            with feature_shards.timed_case("columns", "post_hook_on_a_filter_model_leaves_its_table_alone") as started:
                results.append(hooked_filter_case(Path(folder)))
                results[-1]["seconds"] = round(time.monotonic() - started, 3)
    report: dict[str, Any] = {
        "written_utc": datetime.now(UTC).isoformat(),
        "mode": "iteration" if only else "full",
        "note": "not commit evidence" if only else "complete assigned shard",
        "committed_counts": {"models": len(models), "columns": sum(len(value) for value in built.values())},
        "cases": results,
        "status": "pass" if all(result["ok"] for result in results) else "fail",
    }
    return feature_shards.finish(output, "columns", shard, inventory, selected, before, report)


def main() -> int:
    """Run the cases, write the artifact and fail when a case does not behave as expected."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--output-directory", type=Path, default=None)
    parser.add_argument("--only", action="append", help="run only cases whose name contains this text (repeatable)")
    parser.add_argument("--shard", default="", help="run every Nth case as K/N, with 1 <= K <= N")
    parser.add_argument("--list-cases", action="store_true", help="print the selected case names as JSON without running them")
    args = parser.parse_args()
    try:
        selected = feature_shards.select(case_names(), args.shard, args.only)
    except ValueError as error:
        parser.error(str(error))
    if args.list_cases:
        sys.stdout.write(json.dumps(selected) + "\n")
        return 0
    if args.output_directory is None:
        label = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ") + ("_shard" + args.shard.replace("/", "of") if args.shard else "")
        args.output_directory = columns.REPO_ROOT / "data/e2e/feature_columns" / label
    report = run(args.output_directory, args.only, args.shard)
    failed = [result["case"] for result in report["cases"] if not result["ok"]]
    sys.stdout.write(f"feature columns e2e: {report['status']}; artifact {args.output_directory / 'report.json'}\n")
    if failed:
        sys.stderr.write(f"feature columns e2e: failed cases {failed}\n")
    return int(report["status"] != "pass")


if __name__ == "__main__":
    raise SystemExit(main())
