"""The scope report: the default bronze scope derived from a fresh ``dbt parse`` (migration step S2, read only).

Run from the repository root:

    .venv/bin/python -m scripts.ingestion.scope --out <report.json> [--timing-out <timing.json>]

A bronze table is a **root** when a dbt model other than the operational copy-accounting model reads ``source('bronze', table)``. A **dictionary** is the table
named by ``dictionary.table`` of a root. **Default scope** is the roots plus the dictionaries; every other mapped table **leaves**. The report also derives the
silver source declaration (roots and the four loader tables), the pins, and the Glue mirror names, with the real ``pins.pinned_tables``.

Output contract:

* The report is JSON with sorted keys, two-space indent and one trailing newline. It holds no time and no host path, so two runs on the same tree give the
  same bytes. The parse seconds go to the output line and to the optional ``--timing-out`` file, never into the report.
* The report is written only when ``problems`` is empty (exit 0). A run with a problem prints each one by name, writes no report and exits 1. A run that
  cannot parse exits 2.
* Equality with another document (the S2 draft, a committed registry file) is :func:`equality_problems`: the consumer-scope projection only. It never reads
  acquisition annotations (queue counts, queued-output flags, notes), gap records, policy relationships or retained history.

The module reads the dbt project, the table map and the locked dbt package archive. It writes only the paths it is given.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib
import json
import sys
import tempfile
import time
from collections.abc import Collection, Mapping, Sequence
from pathlib import Path
from typing import Any

from scripts.lakehouse import bronze, dbt_package_archive, pins
from scripts.quality import dbt_manifest_labels

ROOT = Path(__file__).resolve().parents[2]
TABLE_MAP = Path("config/lakehouse/bronze_tables.json")
PROJECT = Path("dbt")
PARSE_SECONDS = dbt_manifest_labels.PARSE_SECONDS
KIND = "bronze_scope_report"
SCHEMA_VERSION = 1
# The tables bronze itself writes beside the mapped ones; silver declares them, but no map entry and no scope decision covers them.
LOADER_TABLES = ("column_map", "file_metadata", "file_preambles", "stored_copies")
# The model that loops over copy_tables (stg_bronze__file_copies.sql): an output of the scope calculation, so it is no reader that makes a root.
OPERATIONAL_MODELS = ("stg_bronze__file_copies",)
LEAVING_REASON = "no_model_reader_and_not_a_required_dictionary"
# The consumer-scope projection that equality compares. Everything else in a report or a draft is annotation or text.
PROJECTION = {
    "in_scope": (
        "table",
        "group",
        "collection",
        "format",
        "dictionary_table",
        "roles",
        "analytical_readers",
        "operational_readers",
        "source_family",
        "silver_source_declared",
    ),
    "dictionary": ("table", "group", "collection", "format", "required_by", "silver_source_declared", "silver_pin"),
    "leaving": ("table", "group", "collection", "format", "reason"),
}
COUNT_KEYS = (
    "mapped",
    "in_scope_data",
    "dictionary",
    "default_scope",
    "leaving",
    "collections_before",
    "collections_retained",
    "collections_leaving",
    "silver_declared_sources",
    "silver_pins",
    "silver_pins_if_dictionaries_declared",
    "glue_mirror_bronze_names",
)
DECLARATION_KEYS = ("declared_sources", "analytical_roots", "loader_tables")
PINS_KEYS = ("decided_roots_only", "if_all_scope_tables_declared", "extra_if_all_declared", "current_yml_declared_sources")


class ScopeError(RuntimeError):
    """The scope cannot be derived: the parse failed or an input is malformed."""


def projected_counts(roots: int, dictionaries: int, extra_loader_sources: int = 0) -> dict[str, int]:
    """The derived counts of a reader graph by formula: mapped = roots + dictionaries; declared = roots + loaders; pins = roots + 1; mirror = mapped + loaders.

    ``extra_loader_sources`` counts loader-style tables declared as dbt sources but not mapped (Q4 B's ``source_objects`` is one): each adds a declared source
    and a mirror name and no pin. Recompute with the report; never freeze these numbers.
    """
    loaders = len(LOADER_TABLES) + extra_loader_sources
    return {"mapped": roots + dictionaries, "declared": roots + loaders, "pins": roots + 1, "mirror_names": roots + dictionaries + loaders}


def read_copy_tables(project_dir: Path) -> dict[str, str]:
    """The ``copy_tables`` variable of the dbt project: bronze table to source family."""
    yaml = importlib.import_module("yaml")
    document = yaml.safe_load((project_dir / "dbt_project.yml").read_text())
    copy_tables = (document.get("vars") or {}).get("copy_tables")
    if not isinstance(copy_tables, dict):
        raise ScopeError(f"{project_dir / 'dbt_project.yml'} has no copy_tables variable")
    return {str(table): str(family) for table, family in copy_tables.items()}


def bronze_readers(manifest: Mapping[str, Any]) -> dict[str, list[str]]:
    """Each bronze source table to the sorted names of the model nodes that read it. Tests, seeds and other node kinds do not count."""
    sources = {unique_id: str(source["name"]) for unique_id, source in manifest.get("sources", {}).items() if source.get("source_name") == "bronze"}
    readers: dict[str, set[str]] = {table: set() for table in sources.values()}
    for node in manifest.get("nodes", {}).values():
        if node.get("resource_type") != "model":
            continue
        for dependency in (node.get("depends_on") or {}).get("nodes") or []:
            if dependency in sources:
                readers[sources[dependency]].add(str(node["name"]))
    return {table: sorted(models) for table, models in readers.items()}


def _digest(document: Any) -> str:
    """SHA-256 of a document's canonical JSON."""
    return hashlib.sha256(json.dumps(document, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def _scope_problems(
    readers: Mapping[str, Sequence[str]], mapped: Mapping[str, Mapping[str, Any]], copy_tables: Mapping[str, str], roots: Collection[str]
) -> list[str]:
    """Every named inconsistency between the readers, the map and copy_tables."""
    problems: list[str] = []
    for table, models in sorted(readers.items()):
        if table not in mapped and table not in LOADER_TABLES:
            problems.extend(f"model {model} reads bronze source {table}: it is not in the table map and not a loader table" for model in models)
    for table in sorted(copy_tables):
        if table not in roots:
            where = "is not in the table map" if table not in mapped else f"has no reader except {', '.join(OPERATIONAL_MODELS)}"
            problems.append(f"copy_tables names {table}, which is no root: it {where}")
    problems.extend(f"{table} has an analytical reader but copy_tables does not name it" for table in sorted(roots) if table not in copy_tables)
    for table in sorted(roots):
        target = mapped[table].get("dictionary", {}).get("table")
        if target is not None and target not in mapped:
            problems.append(f"{table} names the dictionary {target}, which is not in the table map")
        elif target is not None and "dictionary" in mapped[target] and mapped[target]["dictionary"]["table"] not in set(roots) | {target}:
            problems.append(f"the dictionary {target} names its own dictionary {mapped[target]['dictionary']['table']}: chains are not derived")
    return problems


def derive_scope(manifest: Mapping[str, Any], table_map: Mapping[str, Any], copy_tables: Mapping[str, str]) -> tuple[dict[str, Any], list[str]]:
    """Derive the scope report and its problems from a parsed manifest, a table map document and the copy_tables variable. Pure: reads nothing."""
    mapped = {str(entry["table"]): entry for entry in table_map["tables"]}
    readers = bronze_readers(manifest)
    operational = set(OPERATIONAL_MODELS)
    analytical = {table: [model for model in models if model not in operational] for table, models in readers.items()}
    roots = sorted(table for table, models in analytical.items() if models and table in mapped)
    problems = _scope_problems(readers, mapped, copy_tables, set(roots))
    required_by: dict[str, list[str]] = {}
    for table in roots:
        target = mapped[table].get("dictionary", {}).get("table")
        if target is not None and target in mapped and target not in roots:
            required_by.setdefault(target, []).append(table)
    dictionaries = sorted(required_by)
    scope_tables = set(roots) | set(dictionaries)
    declared = set(roots) | set(LOADER_TABLES)
    pinned = pins.pinned_tables(scope_tables, declared)
    pinned_all = pins.pinned_tables(scope_tables, declared | set(dictionaries))
    mirror = sorted(scope_tables | declared)
    collections_before = {str(entry["collection"]) for entry in mapped.values()}
    collections_kept = {str(mapped[table]["collection"]) for table in scope_tables}
    declared_in_dbt = len(readers)

    def base(table: str) -> dict[str, Any]:
        return {key: mapped[table][key] for key in ("table", "group", "collection", "format")}

    in_scope = [
        {
            **base(table),
            "dictionary_table": mapped[table].get("dictionary", {}).get("table"),
            "roles": list(mapped[table].get("roles", ["data"])),
            "analytical_readers": analytical[table],
            "operational_readers": [model for model in readers[table] if model in operational],
            "source_family": copy_tables.get(table),
            "silver_source_declared": True,
        }
        for table in roots
    ]
    dictionary = [{**base(table), "required_by": required_by[table], "silver_source_declared": False, "silver_pin": table in pinned} for table in dictionaries]
    leaving = [{**base(table), "reason": LEAVING_REASON} for table in sorted(set(mapped) - scope_tables)]
    counts = {
        "mapped": len(mapped),
        "in_scope_data": len(roots),
        "dictionary": len(dictionaries),
        "default_scope": len(scope_tables),
        "leaving": len(leaving),
        "collections_before": len(collections_before),
        "collections_retained": len(collections_kept),
        "collections_leaving": len(collections_before - collections_kept),
        "silver_declared_sources": len(declared),
        "silver_pins": len(pinned),
        "silver_pins_if_dictionaries_declared": len(pinned_all),
        "glue_mirror_bronze_names": len(mirror),
    }
    q4 = projected_counts(len(roots), len(dictionaries), extra_loader_sources=1)
    report = {
        "kind": KIND,
        "schema_version": SCHEMA_VERSION,
        "inputs": {
            "table_map_sha256": _digest(table_map),
            "reader_graph_sha256": _digest(readers),
            "dbt_version": str(manifest.get("metadata", {}).get("dbt_version", "")),
        },
        "operational_models": list(OPERATIONAL_MODELS),
        "loader_tables": list(LOADER_TABLES),
        "counts": counts,
        "in_scope": in_scope,
        "dictionary": dictionary,
        "leaving": leaving,
        "silver_source_declaration": {
            "declared_sources": len(declared),
            "analytical_roots": len(roots),
            "loader_tables": list(LOADER_TABLES),
            "pins": {
                "decided_roots_only": len(pinned),
                "if_all_scope_tables_declared": len(pinned_all),
                "extra_if_all_declared": sorted(set(pinned_all) - set(pinned)),
                "current_yml_declared_sources": declared_in_dbt,
            },
            "glue_mirror_names": {"map_union_declared": len(mirror)},
        },
        "derived_variants": {
            "q4_b_index_table_declared": {
                "silver_declared_sources": q4["declared"],
                "silver_pins": q4["pins"],
                "glue_mirror_bronze_names": q4["mirror_names"],
            }
        },
        "problems": sorted(problems),
    }
    return report, sorted(problems)


def _projected(entries: Sequence[Mapping[str, Any]], keys: Sequence[str]) -> dict[str, dict[str, Any]]:
    """Entries by table name, each cut down to the projected keys; a missing key shows as None."""
    return {str(entry["table"]): {key: entry.get(key) for key in keys} for entry in entries}


def equality_problems(report: Mapping[str, Any], expected: Mapping[str, Any]) -> list[str]:
    """Where the consumer-scope projection of ``report`` differs from ``expected`` (a report or a draft). An empty list means equal.

    The projection is the table lists with the keys of :data:`PROJECTION`, the twelve :data:`COUNT_KEYS` and the silver declaration facts. List order is
    ignored: entries are matched by table name. A table on one side only, a changed key or a changed count is named. Acquisition annotations, gap records,
    policy relationships, retained history and descriptive text are not compared.
    """
    found: list[str] = []
    for group, keys in PROJECTION.items():
        ours, theirs = _projected(report[group], keys), _projected(expected[group], keys)
        found.extend(f"{group}: {table} is in the report only" for table in sorted(set(ours) - set(theirs)))
        found.extend(f"{group}: {table} is in the expected document only" for table in sorted(set(theirs) - set(ours)))
        for table in sorted(set(ours) & set(theirs)):
            found.extend(
                f"{group}: {table}: {key} is {ours[table][key]!r}, expected {theirs[table][key]!r}" for key in keys if ours[table][key] != theirs[table][key]
            )
    found.extend(
        f"counts: {key} is {report['counts'].get(key)!r}, expected {expected['counts'].get(key)!r}"
        for key in COUNT_KEYS
        if report["counts"].get(key) != expected["counts"].get(key)
    )
    ours_declaration, theirs_declaration = report["silver_source_declaration"], expected["silver_source_declaration"]
    found.extend(
        f"silver_source_declaration: {key} is {ours_declaration.get(key)!r}, expected {theirs_declaration.get(key)!r}"
        for key in DECLARATION_KEYS
        if ours_declaration.get(key) != theirs_declaration.get(key)
    )
    found.extend(
        f"silver_source_declaration.pins: {key} is {ours_declaration['pins'].get(key)!r}, expected {theirs_declaration['pins'].get(key)!r}"
        for key in PINS_KEYS
        if ours_declaration["pins"].get(key) != theirs_declaration["pins"].get(key)
    )
    ours_mirror, theirs_mirror = (
        ours_declaration["glue_mirror_names"].get("map_union_declared"),
        theirs_declaration["glue_mirror_names"].get("map_union_declared"),
    )
    if ours_mirror != theirs_mirror:
        found.append(f"silver_source_declaration.glue_mirror_names: map_union_declared is {ours_mirror!r}, expected {theirs_mirror!r}")
    return found


def report_bytes(report: Mapping[str, Any]) -> bytes:
    """The report's exact bytes: sorted keys, two-space indent, one trailing newline."""
    return (json.dumps(report, indent=2, sort_keys=True) + "\n").encode()


def scope_of_project(project_dir: Path, table_map_path: Path, root: Path | None) -> tuple[dict[str, Any], list[str], float]:
    """Parse a dbt project with the pinned dbt and derive its scope; returns the report, its problems and the parse seconds.

    ``root`` is the repository whose locked package archive supplies the dbt packages; None parses without packages (a synthetic project).
    """
    table_map = bronze.load_table_map(table_map_path)
    copy_tables = read_copy_tables(project_dir)
    with tempfile.TemporaryDirectory(prefix="hai_scope_") as scratch:
        work = Path(scratch)
        packages = None
        if root is not None:
            dbt_manifest_labels.install_packages(root, work / "dbt_packages")
            packages = work / "dbt_packages"
        began = time.monotonic()
        manifest = dbt_manifest_labels.parse_manifest(project_dir, project_dir, work, packages)
        seconds = time.monotonic() - began
    report, problems = derive_scope(manifest, table_map, copy_tables)
    return report, problems, seconds


def main(argv: Sequence[str] | None = None) -> int:
    """Write the scope report; exit 0 on a clean report, 1 on named problems (no report written), 2 when the project cannot be parsed."""
    parser = argparse.ArgumentParser(description="Derive the default bronze scope from a fresh dbt parse (read only).")
    parser.add_argument("--out", type=Path, required=True, help="the report file to write, only on a clean run")
    parser.add_argument("--timing-out", type=Path, help="a file for the dbt parse seconds, kept apart from the deterministic report")
    parser.add_argument("--project", type=Path, default=ROOT / PROJECT, help="the dbt project folder")
    parser.add_argument("--table-map", type=Path, default=ROOT / TABLE_MAP, help="the bronze table map")
    parser.add_argument("--no-packages", action="store_true", help="parse without the locked dbt packages (a synthetic project)")
    arguments = parser.parse_args(argv)
    try:
        report, problems, seconds = scope_of_project(arguments.project, arguments.table_map, None if arguments.no_packages else ROOT)
    except (ScopeError, bronze.BronzeError, dbt_manifest_labels.ManifestError, dbt_package_archive.ArchiveError, OSError) as error:
        sys.stderr.write(f"scope report: cannot derive: {error}\n")
        return 2
    for line in problems:
        sys.stdout.write(f"{line}\n")
    counts = report["counts"]
    summary = f"roots {counts['in_scope_data']}, dictionaries {counts['dictionary']}, leaving {counts['leaving']}, dbt parse {seconds:.1f} s"
    sys.stdout.write(f"scope report: {'fail' if problems else 'ok'} ({len(problems)} problems, {summary})\n")
    if arguments.timing_out is not None:
        arguments.timing_out.write_text(json.dumps({"dbt_parse_seconds": round(seconds, 3)}, indent=2) + "\n")
    if problems:
        return 1
    arguments.out.write_bytes(report_bytes(report))
    return 0


if __name__ == "__main__":
    sys.exit(main())
