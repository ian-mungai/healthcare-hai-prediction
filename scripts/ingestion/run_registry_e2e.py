"""End-to-end run of the registry check on the committed files and on one failing mutation per integrity row (design v4 step S4).

Every run reads a copy of the committed inputs in a temporary folder, so the repository is never written. The committed files must pass ``check`` with no byte
changed and fail ``check --release`` by name. Each mutation must make ``check`` exit 1 and name its subject. The record of the run is a JSON file.

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
LOCK = registry.LOCK
COLLECTIONS = registry.AUTHORED["collections"]
GOLDEN = registry.AUTHORED["unit_golden"]
OUTCOMES = registry.AUTHORED["manual_outcomes"]
CONTRACTS = registry.AUTHORED["layout_contracts"]
QUEUE = registry.QUEUE
TABLE_MAP = registry.TABLE_MAP


@dataclass(frozen=True)
class Mutation:
    """One failing edit: the integrity row it breaks, what it does, whether the lock is written again after it and the subject the check must name."""

    row: str
    label: str
    edit: Callable[[Path], None]
    subject: str
    relock: bool = False
    rederive_scope: bool = False


def copy_tree(destination: Path) -> Path:
    """The inputs the check reads as real files, except the two large folders no code closure names, which are linked file by file."""
    for folder in COPIED:
        shutil.copytree(ROOT / folder, destination / folder, ignore=shutil.ignore_patterns("__pycache__", *LINKED_FOLDERS))
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
        Mutation("I4", "a second table selects the same files", duplicate_table, "zz_duplicate_hai", relock=True, rederive_scope=True),
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
            "cms_hospital_cost_reports_dictionaries",
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
            "I8 cms_hospital_cost_reports_dictionaries",
        ),
        Mutation(
            "I12",
            "a layout contract's column list is not a list",
            lambda root: edit_json(root, CONTRACTS, lambda d: d["tables"]["hcai_finance_csv"].update(consumer_columns="oshpd_id")),
            CONTRACTS,
            relock=True,
        ),
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
    root = copy_tree(scratch / mutation.label.replace(" ", "_"))
    use = report
    mutation.edit(root)
    if mutation.rederive_scope:
        use, problems, _ = scope.scope_of_project(ROOT / "dbt", root / TABLE_MAP, ROOT)
        if problems:
            return {"row": mutation.row, "label": mutation.label, "passed": False, "error": problems}
    report_file = scratch / f"report_{mutation.label.replace(' ', '_')}.json"
    report_file.write_bytes(scope.report_bytes(use))
    if mutation.relock:
        registry.write_lock(root, use)
    code, text = run_command(["check", "--root", str(root), "--scope-report", str(report_file)])
    named = mutation.subject in text
    lines = text.splitlines()
    shown = [line for line in lines if mutation.subject in line][:3] or lines[:3]
    return {
        "row": mutation.row,
        "label": mutation.label,
        "exit_code": code,
        "names": mutation.subject,
        "named": named,
        "passed": code == 1 and named,
        "problems": [line[:260] for line in shown],
    }


def run(out: Path) -> dict[str, Any]:
    """The whole run: the committed files with and without ``--release``, then every mutation."""
    scratch_root = Path(tempfile.mkdtemp(prefix="hai_registry_e2e_"))
    try:
        report, problems, _ = scope.scope_of_project(ROOT / "dbt", ROOT / TABLE_MAP, ROOT)
        if problems:
            raise registry.RegistryFileError("; ".join(problems))
        report_file = scratch_root / "scope_report.json"
        report_file.write_bytes(scope.report_bytes(report))
        before = file_digests(ROOT)
        plain_code, plain = run_command(["check", "--scope-report", str(report_file)])
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
