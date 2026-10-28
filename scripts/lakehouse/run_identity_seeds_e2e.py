"""End-to-end run of the gold identity seed generators: committed files pass and each damaged copy fails as expected.

Run from the repository root:

    .venv/bin/python -m scripts.lakehouse.run_identity_seeds_e2e [--output-directory DIR]

Every case copies the classification list, the registry documents or the seed to a scratch folder, damages one thing and runs the generator's check on the
copy. The completeness test of the dataset registry coverage is rendered the way dbt renders it for a real and for a fixture target.
The artifact (``report.json`` in the output folder, default ``data/e2e/gold_identity_seeds/<UTC time>/``) lists each case with the messages that matched.
"""

from __future__ import annotations

import argparse
import copy
import csv
import json
import shutil
import sys
import tempfile
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from jinja2 import Environment, StrictUndefined, select_autoescape

from scripts.lakehouse import identity_splits as splits
from scripts.lakehouse import source_labels as labels

Rows = list[dict[str, str]]
Documents = dict[str, Any]
COMPLETENESS_TEST = splits.REPO_ROOT / "dbt" / "tests" / "assert_bronze_datasets_placed_or_listed.sql"
SYNTHETIC_ID = "fixture_dataset"


class Refused(Exception):
    """The rendered test raised a compiler error, as dbt does for ``exceptions.raise_compiler_error``."""


def load(path: Path) -> tuple[list[str], Rows]:
    """Return a CSV file's header and rows."""
    with path.open(newline="") as handle:
        reader = csv.DictReader(handle)
        return list(reader.fieldnames or []), list(reader)


def save(path: Path, header: list[str], rows: Rows) -> None:
    """Write a CSV file with Unix line endings."""
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=header, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def edit(path: Path, change: Callable[[Rows], Rows]) -> None:
    """Rewrite a CSV file's rows with a change."""
    header, rows = load(path)
    save(path, header, change(rows))


def set_cell(code: str, column: str, value: str) -> Callable[[Rows], Rows]:
    """Return a change that sets a column on the row of a code (it must exist)."""

    def change(rows: Rows) -> Rows:
        hits = [row for row in rows if row["code"] == code]
        if not hits:
            raise splits.IdentityError(f"no row for code {code!r}")
        for row in hits:
            row[column] = value
        return rows

    return change


def classification_cases(folder: Path) -> list[tuple[str, Callable[[], None], str]]:
    """Return the damaged classification cases: a name, the change applied to the scratch copy and the message the check must give."""
    path = folder / "identity_classification.csv"

    def header_extra() -> None:
        header, rows = load(path)
        save(path, [*header, "approved_on"], [{**row, "approved_on": "2026-10-12"} for row in rows])

    def duplicate_row() -> None:
        edit(path, lambda rows: [*rows, dict(rows[0])])

    def unknown_dimension() -> None:
        edit(path, set_cell("6", "dimension", "dim_county"))

    def measure_without_fact() -> None:
        edit(path, set_cell("PSI_08", "fact", ""))

    def drg_with_fact() -> None:
        edit(path, set_cell("068", "fact", "fct_medicare_inpatient_drg"))

    return [
        ("proposed_for_approval_stops", lambda: edit(path, set_cell("ED_1b", "approval_status", "proposed_for_approval")), "is proposed_for_approval"),
        ("redefined_with_single_rule", lambda: edit(path, set_cell("PSI_08", "identity_rule", "single_latest")), "class redefined needs identity_rule split"),
        ("renamed_with_split_rule", lambda: edit(path, set_cell("ED_1b", "identity_rule", "split")), "class renamed needs identity_rule single_latest"),
        ("hold_marked_approved", lambda: edit(path, set_cell("068", "approval_status", "approved")), "class unresolved needs approval_status owner_hold"),
        ("renamed_marked_hold", lambda: edit(path, set_cell("ED_1b", "approval_status", "owner_hold")), "class renamed needs approval_status approved"),
        ("unknown_class", lambda: edit(path, set_cell("ED_1b", "classification", "moved")), "classification 'moved' is not one of"),
        ("blank_citation_url", lambda: edit(path, set_cell("6", "citation_urls", "")), "has no citation URL"),
        ("blank_locator", lambda: edit(path, set_cell("6", "locators", "")), "has no locator"),
        ("blank_source_ids", lambda: edit(path, set_cell("6", "source_ids", "")), "has no source ID"),
        ("non_https_citation", lambda: edit(path, set_cell("6", "citation_urls", "http://example.invalid/page")), "is not an https URL"),
        ("decision_record_column", header_extra, "unexpected columns"),
        ("duplicate_key", duplicate_row, "is listed twice"),
        ("unknown_dimension", unknown_dimension, "dimension 'dim_county' is not one of"),
        ("measure_without_fact", measure_without_fact, "needs a fact"),
        ("drg_with_fact", drg_with_fact, "must not name a fact"),
    ]


def seed_cases(folder: Path) -> list[tuple[str, Callable[[], None], str]]:
    """Return the damaged identity seed cases."""
    seed = folder / "identity_splits.csv"
    return [
        ("seed_cell_edited", lambda: edit(seed, set_cell("6", "identity_rule", "single_latest")), "differs from what the classification list generates"),
        ("seed_row_removed", lambda: edit(seed, lambda rows: rows[1:]), "differs from what the classification list generates"),
        ("seed_key_listed_twice", lambda: edit(seed, lambda rows: [*rows, dict(rows[0])]), "differs from what the classification list generates"),
        ("seed_missing", lambda: seed.unlink(), "is missing; run --write"),
    ]


def label_documents_cases() -> list[tuple[str, Callable[[Documents, Documents], None], str]]:
    """Return the damaged registry cases: a name, the change applied to deep copies of the registry and the additions, and the message."""

    def first_entry(additions: Documents) -> dict[str, Any]:
        return next(entry for entry in additions["source_metadata"] if entry["dataset_ids"])

    def blank_publisher(_registry: Documents, additions: Documents) -> None:
        first_entry(additions)["publisher"] = " "

    def unknown_source(_registry: Documents, additions: Documents) -> None:
        first_entry(additions)["source_id"] = "NOT-A-SOURCE"

    def placed_twice(_registry: Documents, additions: Documents) -> None:
        entries = [entry for entry in additions["source_metadata"] if entry["dataset_ids"]]
        entries[1]["dataset_ids"] = [*entries[1]["dataset_ids"], entries[0]["dataset_ids"][0]]

    def placed_and_unplaced(_registry: Documents, additions: Documents) -> None:
        additions["unplaced_dataset_ids"].append({"dataset_id": first_entry(additions)["dataset_ids"][0], "detail": "damaged"})

    def placements_disagree(_registry: Documents, additions: Documents) -> None:
        additions["dataset_id_placements"] = additions["dataset_id_placements"][1:]

    def blank_title(registry: Documents, additions: Documents) -> None:
        source_id = first_entry(additions)["source_id"]
        next(record for record in registry["sources"] if record["source_id"] == source_id)["title"] = ""

    def bad_dataset_id(_registry: Documents, additions: Documents) -> None:
        first_entry(additions)["dataset_ids"][0] = "Not A Dataset"

    def unplaced_twice(_registry: Documents, additions: Documents) -> None:
        additions["unplaced_dataset_ids"].append(dict(additions["unplaced_dataset_ids"][0]))

    return [
        ("blank_publisher", blank_publisher, "has no publisher"),
        ("unknown_source_id", unknown_source, "is not in the registry"),
        ("dataset_placed_twice", placed_twice, "is placed by two sources"),
        ("dataset_placed_and_unplaced", placed_and_unplaced, "is both placed and unplaced"),
        ("placements_disagree_with_source_metadata", placements_disagree, "dataset_id_placements and source_metadata disagree"),
        ("blank_registry_title", blank_title, "has no title"),
        ("invalid_dataset_id", bad_dataset_id, "is not a valid dataset ID"),
        ("unplaced_listed_twice", unplaced_twice, "is listed unplaced twice"),
    ]


def label_seed_cases(folder: Path) -> list[tuple[str, Callable[[], None], str]]:
    """Return the damaged label seed cases."""
    seed = folder / "source_labels.csv"

    def placed_row() -> Callable[[Rows], Rows]:
        def change(rows: Rows) -> Rows:
            next(row for row in rows if row["placement"] == "placed")["label"] = "A guessed title"
            return rows

        return change

    def unplaced_row() -> Callable[[Rows], Rows]:
        def change(rows: Rows) -> Rows:
            row = next(row for row in rows if row["placement"] == "unplaced")
            row["publisher"], row["label"] = "A guessed publisher", "A guessed title"
            return rows

        return change

    return [
        ("label_edited", lambda: edit(seed, placed_row()), "differs from what the registry generates"),
        ("unplaced_dataset_labeled", lambda: edit(seed, unplaced_row()), "differs from what the registry generates"),
        ("dataset_removed", lambda: edit(seed, lambda rows: rows[1:]), "differs from what the registry generates"),
        ("seed_missing", lambda: seed.unlink(), "is missing; run --write"),
    ]


def expected_rules(seed_text: str) -> list[str]:
    """Return the problems of the committed identity seed against the approved identity rule of each named key."""
    rows = list(csv.DictReader(seed_text.splitlines()))
    wanted = {
        ("dim_measure", "OP_18b"): "single_latest",
        ("dim_measure", "PSI_09"): "split",
        ("dim_measure", "PSI_08"): "split",
        ("dim_measure", "HCP_COVID_19"): "split",
        ("dim_measure", "ED_1b"): "single_latest",
        ("dim_rurality_code", "6"): "split",
        ("dim_rurality_code", "9"): "split",
        ("dim_drg", "068"): "hold_single_latest",
        ("dim_drg", "835"): "hold_single_latest",
    }
    found = {(row["dimension"], row["code"]): row["identity_rule"] for row in rows}
    problems = [f"{key} has rule {found.get(key)!r}, expected {rule!r}" for key, rule in wanted.items() if found.get(key) != rule]
    counts = {rule: sum(1 for row in rows if row["identity_rule"] == rule) for rule in ("split", "single_latest", "hold_single_latest")}
    if counts != {"split": 7, "single_latest": 7, "hold_single_latest": 11}:
        problems.append(f"rule counts are {counts}, expected 7 split, 7 single_latest and 11 hold_single_latest")
    return problems


def run_identity(scratch: Path) -> list[dict[str, Any]]:
    """Run the identity seed cases on copies in ``scratch``."""
    results: list[dict[str, Any]] = []
    base = splits.check()
    results.append({"case": "identity_committed_files", "problems": base, "ok": not base})
    decisions = expected_rules(splits.SEED.read_text())
    results.append({"case": "identity_owner_decisions_in_seed", "problems": decisions, "ok": not decisions})
    for name, damage, expected in classification_cases(scratch) + seed_cases(scratch):
        shutil.rmtree(scratch, ignore_errors=True)
        scratch.mkdir(parents=True)
        shutil.copy(splits.CLASSIFICATION, scratch / "identity_classification.csv")
        shutil.copy(splits.SEED, scratch / "identity_splits.csv")
        damage()
        problems = splits.check(scratch / "identity_classification.csv", scratch / "identity_splits.csv")
        matched = [problem for problem in problems if expected in problem]
        results.append({"case": name, "expected": expected, "matched": matched[:3], "problems": len(problems), "ok": bool(matched)})
    shutil.rmtree(scratch, ignore_errors=True)
    scratch.mkdir(parents=True)
    shutil.copy(splits.CLASSIFICATION, scratch / "identity_classification.csv")
    edit(scratch / "identity_classification.csv", set_cell("ED_1b", "approval_status", "proposed_for_approval"))
    try:
        splits.write(scratch / "identity_classification.csv", scratch / "identity_splits.csv")
        stopped = False
    except splits.IdentityError:
        stopped = True
    results.append(
        {
            "case": "write_stops_on_proposed_key",
            "seed_written": (scratch / "identity_splits.csv").exists(),
            "ok": stopped and not (scratch / "identity_splits.csv").exists(),
        }
    )
    shutil.copy(splits.CLASSIFICATION, scratch / "identity_classification.csv")
    splits.write(scratch / "identity_classification.csv", scratch / "identity_splits.csv")
    first = (scratch / "identity_splits.csv").read_bytes()
    splits.write(scratch / "identity_classification.csv", scratch / "identity_splits.csv")
    results.append({"case": "write_is_idempotent", "ok": first == (scratch / "identity_splits.csv").read_bytes() and first == splits.SEED.read_bytes()})
    return results


def run_labels(scratch: Path) -> list[dict[str, Any]]:
    """Run the source label cases on copies in ``scratch``."""
    results: list[dict[str, Any]] = []
    registry, additions = labels.read_documents()
    base = labels.check()
    results.append({"case": "labels_committed_files", "problems": base, "ok": not base})
    rows = labels.build_rows(registry, additions)
    placed = sum(1 for row in rows if row["placement"] == "placed")
    counted = {"rows": len(rows), "placed": placed, "unplaced": len(rows) - placed}
    results.append({"case": "labels_every_listed_id_has_a_row", "counts": counted, "ok": len(rows) == placed + len(additions["unplaced_dataset_ids"])})
    labeled = [row["dataset_id"] for row in rows if row["placement"] == "unplaced" and (row["publisher"] or row["label"])]
    results.append({"case": "labels_unplaced_have_no_label", "labeled": labeled[:3], "ok": not labeled and counted["unplaced"] > 0})
    for name, damage, expected in label_documents_cases():
        damaged_registry, damaged_additions = copy.deepcopy(registry), copy.deepcopy(additions)
        damage(damaged_registry, damaged_additions)
        problems = labels.problems_of(damaged_registry, damaged_additions, labels.SEED)
        matched = [problem for problem in problems if expected in problem]
        results.append({"case": name, "expected": expected, "matched": matched[:3], "problems": len(problems), "ok": bool(matched)})
    for seed_name, seed_damage, seed_expected in label_seed_cases(scratch):
        shutil.rmtree(scratch, ignore_errors=True)
        scratch.mkdir(parents=True)
        shutil.copy(labels.SEED, scratch / "source_labels.csv")
        seed_damage()
        problems = labels.problems_of(registry, additions, scratch / "source_labels.csv")
        matched = [problem for problem in problems if seed_expected in problem]
        results.append({"case": seed_name, "expected": seed_expected, "matched": matched[:3], "problems": len(problems), "ok": bool(matched)})
    shutil.rmtree(scratch, ignore_errors=True)
    scratch.mkdir(parents=True)
    labels.write(scratch / "source_labels.csv", registry, additions)
    first = (scratch / "source_labels.csv").read_bytes()
    labels.write(scratch / "source_labels.csv", registry, additions)
    results.append({"case": "labels_write_is_idempotent", "ok": first == (scratch / "source_labels.csv").read_bytes() and first == labels.SEED.read_bytes()})
    return results


def render_completeness_test(target_name: str, variables: dict[str, Any]) -> str:
    """Return the SQL of the completeness test as dbt renders it for a target and the given variables, or raise ``Refused``."""

    def var(name: str, default: Any = None) -> Any:
        return variables.get(name, default)

    def ref(name: str) -> str:
        return f'"{name}"'

    def compiler_error(message: str) -> None:
        raise Refused(message)

    environment = Environment(undefined=StrictUndefined, autoescape=select_autoescape(default_for_string=False))
    template = environment.from_string(COMPLETENESS_TEST.read_text())
    return template.render(target={"name": target_name}, var=var, ref=ref, exceptions={"raise_compiler_error": compiler_error})


def exemption_of(target_name: str, variables: dict[str, Any]) -> str:
    """Return what the rendered test exempts for a target ('' for nothing, the clause text otherwise) or 'refused: <message>'."""
    try:
        sql = render_completeness_test(target_name, variables)
    except Refused as error:
        return f"refused: {error}"
    return " ".join(line.strip() for line in sql.splitlines() if " not in (" in line)


def run_completeness() -> list[dict[str, Any]]:
    """Render the completeness test for each target and check who may exempt the synthetic dataset ID."""
    real = exemption_of("lakehouse", {})
    clause = f"not in ('{SYNTHETIC_ID}')"
    refused = exemption_of("lakehouse", {"synthetic_dataset_ids": [SYNTHETIC_ID]})
    fixture = exemption_of("fixture", {})
    cleared = exemption_of("fixture", {"synthetic_dataset_ids": []})
    source = render_completeness_test("lakehouse", {})
    return [
        {
            "case": "completeness_reads_the_set_dim_source_reads",
            "ok": '"stg_bronze__release_observations"' in source and '"stg_bronze__file_copies"' not in source,
        },
        {"case": "completeness_real_target_rejects_unlisted_id", "exemption": real, "ok": real == ""},
        {"case": "completeness_lint_target_exempts_nothing", "exemption": exemption_of("lint", {}), "ok": exemption_of("lint", {}) == ""},
        {
            "case": "completeness_real_target_refuses_synthetic_exemption",
            "exemption": refused,
            "ok": refused.startswith("refused:") and "fixture target" in refused,
        },
        {"case": "completeness_fixture_target_exempts_synthetic_id", "exemption": fixture, "ok": clause in fixture},
        {"case": "completeness_fixture_variable_clears_exemption", "exemption": cleared, "ok": cleared == ""},
    ]


def run(output: Path) -> dict[str, Any]:
    """Run every case and return the report."""
    with tempfile.TemporaryDirectory() as folder:
        scratch = Path(folder) / "case"
        results = run_identity(scratch) + run_labels(scratch) + run_completeness()
    report: dict[str, Any] = {"written_utc": datetime.now(UTC).isoformat(), "cases": results, "status": "pass" if all(r["ok"] for r in results) else "fail"}
    output.mkdir(parents=True, exist_ok=True)
    (output / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    return report


def main() -> int:
    """Run the cases, write the artifact and fail when a case does not behave as expected."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--output-directory", type=Path, default=splits.REPO_ROOT / "data/e2e/gold_identity_seeds" / datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    )
    args = parser.parse_args()
    report = run(args.output_directory)
    failed = [result["case"] for result in report["cases"] if not result["ok"]]
    sys.stdout.write(f"gold identity seeds e2e: {report['status']}; {len(report['cases'])} cases; artifact {args.output_directory / 'report.json'}\n")
    if failed:
        sys.stderr.write(f"gold identity seeds e2e: failed cases {failed}\n")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
