"""End-to-end run of the registry check on the committed files and on one failing mutation per integrity row (design v4 step S4).

Every run reads a copy of the committed inputs in a temporary folder, so the repository is never written. The committed files must pass ``check`` with no byte
changed and fail ``check --release`` by name. Each mutation must make ``check`` exit 1 with one line that starts with its predicate and holds its subject. The
record of the run is a JSON file.

    python -m scripts.ingestion.run_registry_e2e --out <record.json>
"""

from __future__ import annotations

import argparse
import contextlib
import hashlib
import io
import json
import os
import shutil
import sys
import tempfile
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from scripts.ingestion import registry, scope

ROOT = registry.ROOT
LINKED_FOLDERS = ("baseline_run", "bootstrap_seed")
COPIED = ("scripts", "config/ingestion", "config/lakehouse", "config/acquisition")
# What the dbt parse of a tree reads: its dbt project and the locked package archive, which may not be reached through a link.
PARSE_FOLDERS = ("dbt", "data/analytics/dbt/package_archive")
LOCK = registry.LOCK
COLLECTIONS = registry.AUTHORED["collections"]
GOLDEN = registry.AUTHORED["unit_golden"]
OUTCOMES = registry.AUTHORED["manual_outcomes"]
CONTRACTS = registry.AUTHORED["layout_contracts"]
QUEUE = registry.QUEUE
TABLE_MAP = registry.TABLE_MAP


@dataclass(frozen=True)
class Mutation:
    """One failing edit: the integrity row it breaks, what it does, whether the lock is written again after it and the subject the check must name. The check
    must print one line that starts with ``predicate`` (the row, unless another predicate reports it) and holds the subject, and ``also`` anywhere. A
    ``supplied`` edit makes the scope report given to the check out of the derived one."""

    row: str
    label: str
    edit: Callable[[Path], None]
    subject: str
    relock: bool = False
    predicate: str = ""
    also: str = ""
    supplied: Callable[[dict[str, Any]], Any] | None = None


def copy_tree(destination: Path) -> Path:
    """The inputs the check reads as real files, except the two large folders no code closure names, which are linked file by file."""
    for folder in COPIED:
        shutil.copytree(ROOT / folder, destination / folder, ignore=shutil.ignore_patterns("__pycache__", *LINKED_FOLDERS))
    for folder in PARSE_FOLDERS:
        shutil.copytree(ROOT / folder, destination / folder, ignore=shutil.ignore_patterns("__pycache__", *registry.NOT_PARSE_INPUTS))
    for folder in LINKED_FOLDERS:
        for source in (ROOT / "config/acquisition" / folder).rglob("*"):
            if source.is_file():
                target = destination / source.relative_to(ROOT)
                target.parent.mkdir(parents=True, exist_ok=True)
                os.symlink(source, target)
    return destination


def replace(root: Path, relative: str, body: bytes) -> None:
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.unlink(missing_ok=True)
    path.write_bytes(body)


def append(root: Path, relative: str, text: str) -> None:
    replace(root, relative, (root / relative).read_bytes() + text.encode())


def edit_json(root: Path, relative: str, change: Callable[[Any], None]) -> None:
    document = json.loads((root / relative).read_bytes())
    change(document)
    replace(root, relative, registry.encode(document))


def source_of(document: Any, source_id: str) -> dict[str, Any]:
    return next(source for collection in document["collections"] for source in collection["sources"] if source["source_id"] == source_id)


def flip_byte(root: Path) -> None:
    body = bytearray((root / QUEUE).read_bytes())
    body[10] ^= 1
    replace(root, QUEUE, bytes(body))


def duplicate_table(root: Path) -> None:
    """Add a second table that selects the same files as an existing one, so an output matches two tables."""

    def change(document: Any) -> None:
        original = next(table for table in document["tables"] if table["table"] == "cms_hai_hospital")
        document["tables"].append({**original, "table": "zz_duplicate_hai", "dictionary": original["dictionary"]})

    edit_json(root, TABLE_MAP, change)


def move_out_of_scope(report: dict[str, Any]) -> dict[str, Any]:
    """The report with one table moved to the leaving list and its map digest unchanged."""
    changed = json.loads(scope.report_bytes(report))
    row = next(row for row in changed["in_scope"] if row["table"] == "cms_hai_hospital")
    changed["in_scope"].remove(row)
    changed["leaving"].append(row)
    return changed


def unchanged(root: Path) -> None:
    """No edit of the tree: the mutation is in the supplied report."""


def delete_and_unlist(name: str) -> Callable[[Path], None]:
    def change(root: Path) -> None:
        (root / name).unlink()
        edit_json(root, LOCK, lambda lock: lock.update(files=[row for row in lock["files"] if row["path"] != name]))

    return change


def unreadable_outcomes(root: Path) -> None:
    replace(root, f"{registry.SCHEMAS}/manual_outcomes.schema.json", b"true\n")
    replace(root, OUTCOMES, b"null\n")


def binding_mutations() -> list[Mutation]:
    """The broken scope, lock bindings, schemas and inputs that must each fail by name."""
    plan = "config/acquisition/census_acs_tables_plan.json"
    classified = "config/acquisition/hcai_util_2018_2025_plan.lock.json"
    live_schema = f"{registry.SCHEMAS}/live_check.schema.json"
    deep = "(" * 750 + "x" + ")" * 750
    return [
        Mutation("I1", "a supplied scope report moves a table out of scope", unchanged, "scope report", also="R1 usable-input", supplied=move_out_of_scope),
        Mutation("I2", "a supplied scope report is a number", unchanged, "scope report", supplied=lambda report: 5),
        Mutation("I1", "the active Census plan is deleted", lambda root: (root / plan).unlink(), plan),
        Mutation("I1", "a classified plan's lock is deleted and unlisted", delete_and_unlist(classified), classified),
        Mutation(
            "I1",
            "the source registry parent digest is false",
            lambda root: edit_json(root, LOCK, lambda d: d["parents"]["source_registry"].update(sha256="0" * 64)),
            "source registry parent",
        ),
        Mutation(
            "I1",
            "the queue is listed a second time with another digest",
            lambda root: edit_json(root, LOCK, lambda d: d["files"].insert(0, {"path": QUEUE, "sha256": "0" * 64})),
            f"{QUEUE}: is listed by the lock more than once",
        ),
        Mutation(
            "I2",
            "a manual outcome is removed",
            lambda root: edit_json(root, OUTCOMES, lambda d: d["outcomes"].pop(0)),
            "is a manual unit with no outcome",
            relock=True,
            also="open: R1 manual outcomes",
        ),
        Mutation(
            "I1",
            "a manual outcome is removed without relocking",
            lambda root: edit_json(root, OUTCOMES, lambda d: d["outcomes"].pop(0)),
            f"{OUTCOMES}: differs from the lock",
            also="open: R1 manual outcomes",
        ),
        Mutation("I1", "a required shape schema is deleted and unlisted", delete_and_unlist(live_schema), live_schema, also="open: R1 usable-input"),
        Mutation(
            "I2",
            "a draft is unreadable under a schema that accepts anything",
            unreadable_outcomes,
            f"{OUTCOMES}: lacks the structure its reader needs",
            relock=True,
            also="open: R1 manual outcomes",
        ),
        Mutation(
            "I2",
            "a bound schema refers to an external address",
            lambda root: replace(root, live_schema, b'{"$ref": "https://registry.invalid/live.schema.json"}\n'),
            f"{live_schema}: has a reference that resolves",
            relock=True,
        ),
        Mutation(
            "I7",
            "a gap pin is repeated in front with an unknown migration label",
            lambda root: edit_json(root, LOCK, lambda d: d["pins"]["gaps"].insert(0, {**d["pins"]["gaps"][0], "step": "S99"})),
            "cms_hospital_cost_reports_dictionaries: is pinned more than once",
            also="cms_hospital_cost_reports_dictionaries: has migration label 'S99'",
        ),
        Mutation(
            "I2",
            "a bound schema repeats a keyword",
            lambda root: replace(root, live_schema, b'{"type": "object", "type": "array"}\n'),
            f"{live_schema}: is not a usable JSON schema",
            relock=True,
        ),
        Mutation("I2", "a bound shape schema is no schema", lambda root: replace(root, live_schema, b'{"type": 5}\n'), live_schema, relock=True),
        Mutation(
            "I2",
            "a feed pattern overflows the regex compiler",
            lambda root: edit_json(root, COLLECTIONS, lambda d: d["feeds"][0].update(name_regex=deep)),
            "pattern is not a regular expression",
            relock=True,
        ),
    ]


def mutations() -> list[Mutation]:
    return [
        Mutation("lock", "no lock", lambda root: (root / LOCK).unlink(), LOCK),
        Mutation("I1", "one byte of queue.json changes", flip_byte, QUEUE),
        Mutation(
            "I2",
            "a field is added to collections.json",
            lambda root: edit_json(root, COLLECTIONS, lambda d: d["collections"][0].update(surprise=1)),
            COLLECTIONS,
            relock=True,
        ),
        Mutation(
            "I2",
            "a decision reference is not the registry's",
            lambda root: edit_json(root, COLLECTIONS, lambda d: source_of(d, "IL")["decision_ref"].update(sha256="0" * 64)),
            "IL",
            relock=True,
        ),
        Mutation(
            "I3",
            "a unit key is dropped from the golden list",
            lambda root: edit_json(root, GOLDEN, lambda d: d["units"].pop(5)),
            "the golden list lacks",
            relock=True,
        ),
        Mutation("I4", "a second table selects the same files", duplicate_table, "zz_duplicate_hai", relock=True),
        Mutation(
            "I5",
            "a declared feed selects no table and is not a pinned gap",
            lambda root: edit_json(root, COLLECTIONS, lambda d: d["feeds"].append({"table": "cms_hai_hospital", "source_id": "HPSA", "name_regex": r"\.csv$"})),
            "cms_hai_hospital",
            relock=True,
        ),
        Mutation(
            "I6",
            "a file only the workbook closure runs is edited",
            lambda root: append(root, "scripts/acquisition/store_hud_xlsx.py", "# edited\n"),
            "hud_api_workbook",
        ),
        Mutation(
            "I7",
            "a pinned gap is removed",
            lambda root: edit_json(root, LOCK, lambda d: d["pins"]["gaps"].remove(next(g for g in d["pins"]["gaps"] if g["table"] == "hcai_finance_csv"))),
            "hcai_finance_csv",
        ),
        Mutation(
            "I8",
            "a pinned gap carries an unknown migration label",
            lambda root: edit_json(root, LOCK, lambda d: d["pins"]["gaps"][0].update(step="S99")),
            "cms_hospital_cost_reports_dictionaries: has migration label 'S99'",
        ),
        Mutation(
            "I9",
            "a source with automatic units is marked not scheduled",
            lambda root: edit_json(root, COLLECTIONS, lambda d: source_of(d, "HUD").update(acquisition="not_scheduled")),
            "HUD",
            relock=True,
        ),
        Mutation(
            "I10",
            "a document output is declared as a feed",
            lambda root: edit_json(
                root, COLLECTIONS, lambda d: d["feeds"].append({"table": "cms_hai_hospital", "source_id": "HCAI_UTIL", "name_regex": r"\."})
            ),
            "cms_hai_hospital",
            relock=True,
            predicate="I5",
        ),
        Mutation(
            "I11",
            "the pinned census seat gap is removed",
            lambda root: edit_json(root, LOCK, lambda d: d["pins"]["seat_gaps"].pop("census_acs_tables")),
            "census_acs_tables",
        ),
        Mutation(
            "I8",
            "a pinned gap carries the nonexistent step S6",
            lambda root: edit_json(root, LOCK, lambda d: d["pins"]["gaps"][0].update(step="S6")),
            "cms_hospital_cost_reports_dictionaries: has migration label 'S6'",
        ),
        Mutation(
            "I12",
            "a layout contract's column list is not a list",
            lambda root: edit_json(root, CONTRACTS, lambda d: d["tables"]["hcai_finance_csv"].update(consumer_columns="oshpd_id")),
            CONTRACTS,
            relock=True,
        ),
        *binding_mutations(),
    ]


def run_command(arguments: Sequence[str]) -> tuple[int, str]:
    """The real command, in this process: its exit code and everything it printed."""
    output = io.StringIO()
    with contextlib.redirect_stdout(output), contextlib.redirect_stderr(output):
        code = registry.main(arguments)
    return code, output.getvalue()


def file_digests(root: Path) -> dict[str, str]:
    return {name: hashlib.sha256((root / name).read_bytes()).hexdigest() for name in registry.bound_paths(root)}


def run_mutation(mutation: Mutation, report: dict[str, Any], scratch: Path) -> dict[str, Any]:
    name = mutation.label.replace(" ", "_")
    root = copy_tree(scratch / name)
    mutation.edit(root)
    if mutation.relock:
        registry.write_lock(root)
    arguments = ["check", "--root", str(root)]
    if mutation.supplied:
        report_file = scratch / f"report_{name}.json"
        report_file.write_text(json.dumps(mutation.supplied(report)))
        arguments += ["--scope-report", str(report_file)]
    code, text = run_command(arguments)
    predicate = mutation.predicate or mutation.row
    lines = text.splitlines()
    found = [line for line in lines if line.startswith(f"{predicate} ") and mutation.subject in line]
    named = bool(found) and mutation.also in text
    shutil.rmtree(root)
    return {
        "row": mutation.row,
        "label": mutation.label,
        "exit_code": code,
        "predicate": predicate,
        "names": mutation.subject,
        "also": mutation.also,
        "named": named,
        "passed": code == 1 and named,
        "problems": [line[:260] for line in (found or lines)[:3]],
    }


def run(out: Path) -> dict[str, Any]:
    """The whole run: the committed files with and without ``--release``, then every mutation."""
    scratch_root = Path(tempfile.mkdtemp(prefix="hai_registry_e2e_")).resolve()
    try:
        report = registry.derived_scope(ROOT)
        report_file = scratch_root / "scope_report.json"
        report_file.write_bytes(scope.report_bytes(report))
        before = file_digests(ROOT)
        plain_code, plain = run_command(["check"])
        release_code, release = run_command(["check", "--release", "--scope-report", str(report_file)])
        record: dict[str, Any] = {
            "committed_files": {
                "check": {"exit_code": plain_code, "expected": 0, "output": plain},
                "check_release": {"exit_code": release_code, "expected": 1, "output": release},
                "bound_files_unchanged": before == file_digests(ROOT),
            },
            "mutations": [run_mutation(mutation, report, scratch_root) for mutation in mutations()],
        }
        committed = record["committed_files"]
        record["passed"] = plain_code == 0 and release_code == 1 and committed["bound_files_unchanged"] and all(item["passed"] for item in record["mutations"])
    finally:
        shutil.rmtree(scratch_root, ignore_errors=True)
    out.write_bytes(registry.encode(record))
    return record


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run the registry check on the committed files and on one failing mutation per integrity row.")
    parser.add_argument("--out", type=Path, required=True, help="the JSON record to write")
    record = run(parser.parse_args(argv).out)
    for item in record["mutations"]:
        sys.stdout.write(f"{item['row']:>4} {'ok  ' if item['passed'] else 'FAIL'} {item['label']}\n")
    sys.stdout.write(f"registry e2e: {'pass' if record['passed'] else 'fail'}\n")
    return 0 if record["passed"] else 1


if __name__ == "__main__":
    sys.exit(main())
