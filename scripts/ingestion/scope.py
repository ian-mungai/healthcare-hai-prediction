"""The scope report: the default bronze scope derived from a fresh ``dbt parse`` (read only).

Run from the repository root:

    .venv/bin/python -m scripts.ingestion.scope --out <report.json> [--timing-out <timing.json>]

A bronze table is a **root** in two cases: a dbt model other than the operational copy-accounting model reads ``source('bronze', table)``; or ``copy_tables``
names it, the copy-accounting model reads it and the project defers its staging model (``vars.deferred_models``, read by ``deferred_models``). A declared dbt
source that no model reads is no reader. A **dictionary** is the table named by
``dictionary.table`` of a root. **Default scope** is the roots plus the dictionaries; every other mapped table **leaves**. The report also derives the silver
source declaration (roots and the four loader tables), the pins and the Glue mirror names, with the real ``pins.pinned_tables``.

``leaving`` lists the tables of the given map outside that consumer scope. It is not the list of tables bronze stopped loading: that is
``config/lakehouse/bronze_leaving_scope.json``. The loaded map may keep a table that no consumer derives by an owner decision.

Output contract:

* The report is JSON with sorted keys, two-space indent and one trailing newline. It holds no time and no host path, so two runs on the same tree give the
  same bytes. The parse seconds go to the output line and to the optional ``--timing-out`` file, never into the report.
* Before anything is read or parsed, ``--out`` and ``--timing-out`` are refused (exit 2, nothing written) when the two name one file or when either names
  the table map, the pinned dbt or a path in the dbt project folder, in the repository's ``config/`` or ``dbt/`` folder or in the package archive folder.
  Links are followed and letter case is ignored.
* The report is written only when ``problems`` is empty (exit 0), to a temporary file that replaces the report atomically. A run with a scope problem
  prints each one by name, writes no report and exits 1. A run whose input cannot be read or parsed (a malformed table map or dbt_project.yml, a missing file,
  a parse that fails or passes its deadline, a report that cannot be written) prints the file or step by name and exits 2. A process-cleanup failure of the
  parser (a process group that survived KILL) is not turned into an exit code: it propagates, so the process module's own exit status applies. A caller
  must require exit 0 of the current run before it reads or binds the file: an older report at the path is not touched by a failing run.
* Equality with another document (a draft, a committed registry file) is :func:`equality_problems`: the consumer-scope projection only. It never reads
  acquisition annotations (queue counts, queued-output flags, notes), gap records, policy relationships or retained history.

The module reads the dbt project, the table map and the locked dbt package archive. It writes only the paths it is given.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib
import json
import os
import re
import sys
import tempfile
import time
from collections.abc import Collection, Mapping, Sequence
from pathlib import Path
from typing import Any

from scripts.lakehouse import bronze, dbt_package_archive, deferred_models, pins
from scripts.process import LaunchesNotStopped, ProcessGroupSurvived, SessionListingFailed, TimeoutExpired
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
DECLARATION_KEYS = ("declared_sources", "data_roots", "analytical_roots", "loader_tables")
PINS_KEYS = ("decided_roots_only", "if_all_scope_tables_declared", "extra_if_all_declared", "current_yml_declared_sources")


class ScopeError(RuntimeError):
    """The scope cannot be derived: the parse failed or an input is malformed."""


def projected_counts(roots: int, dictionaries: int, extra_loader_sources: int = 0) -> dict[str, int]:
    """The derived counts of a reader graph by formula: mapped = roots + dictionaries; declared = roots + loaders; pins = roots + 1; mirror = mapped + loaders.

    ``extra_loader_sources`` counts loader-style tables declared as dbt sources but not mapped (the searchable index table ``source_objects`` is one, once the
    index is delivered): each adds a declared source and a mirror name and no pin. Recompute with the report; never freeze these numbers.
    """
    loaders = len(LOADER_TABLES) + extra_loader_sources
    return {"mapped": roots + dictionaries, "declared": roots + loaders, "pins": roots + 1, "mirror_names": roots + dictionaries + loaders}


_ABSENT = object()


def _fail(label: str, where: str, problem: str) -> ScopeError:
    """The error for one bad value: the input, the path in it and what is wrong."""
    return ScopeError(f"{label}: {where}: {problem}")


def _name(value: Any, label: str, where: str) -> str:
    """A non-empty string at ``where``, or a ScopeError naming it."""
    if value is _ABSENT:
        raise _fail(label, where, "is missing")
    if not isinstance(value, str):
        raise _fail(label, where, f"must be a string, not {type(value).__name__}")
    if not value.strip():
        raise _fail(label, where, "is empty")
    return value


def _object(value: Any, label: str, where: str) -> Mapping[str, Any]:
    """An object (a mapping) at ``where``, or a ScopeError naming it."""
    if value is _ABSENT:
        raise _fail(label, where, "is missing")
    if not isinstance(value, dict):
        raise _fail(label, where, f"must be an object, not {type(value).__name__}")
    return value


def _array(value: Any, label: str, where: str) -> list[Any]:
    """A list at ``where``, or a ScopeError naming it."""
    if value is _ABSENT:
        raise _fail(label, where, "is missing")
    if not isinstance(value, list):
        raise _fail(label, where, f"must be a list, not {type(value).__name__}")
    return value


def validate_table_map(document: Any, label: str) -> None:
    """Check every value derive_scope reads from the table map: the document, ``tables``, and per entry ``table``, ``group``, ``collection`` and ``format``
    (non-empty strings), ``roles`` (an optional list of non-empty strings) and ``dictionary`` (an optional object with a non-empty string ``table``).
    Other keys are ignored."""
    tables = _array(_object(document, label, "the document").get("tables", _ABSENT), label, "tables")
    for index, row in enumerate(tables):
        where = f"tables[{index}]"
        fields = _object(row, label, where)
        for key in ("table", "group", "collection", "format"):
            _name(fields.get(key, _ABSENT), label, f"{where}.{key}")
        where = f"tables[{index}] ({fields['table']})"
        if "roles" in fields:
            for position, role in enumerate(_array(fields["roles"], label, f"{where}.roles")):
                _name(role, label, f"{where}.roles[{position}]")
        if "dictionary" in fields:
            _name(_object(fields["dictionary"], label, f"{where}.dictionary").get("table", _ABSENT), label, f"{where}.dictionary.table")


def validate_copy_tables(copy_tables: Any, label: str) -> None:
    """Check the copy_tables variable: an object of non-empty string table names to non-empty string source families."""
    for table, family in _object(copy_tables, label, "vars.copy_tables").items():
        _name(table, label, "a vars.copy_tables key")
        _name(family, label, f"vars.copy_tables.{table}")


def validate_deferred_models(record: Any, label: str) -> None:
    """Check the deferred_models variable: absent, null or a list of objects each with a ``models`` list of non-empty strings. Other keys are ignored."""
    if record is None:
        return
    for index, entry in enumerate(_array(record, label, "vars.deferred_models")):
        where = f"vars.deferred_models[{index}]"
        models = _array(_object(entry, label, where).get("models", _ABSENT), label, f"{where}.models")
        for position, model in enumerate(models):
            _name(model, label, f"{where}.models[{position}]")


def project_file_problems(project_dir: Path) -> list[str]:
    """Hints for a failed parse: the YAML files of the project that do not read as YAML or whose ``sources`` block has the wrong shape (a list of objects
    with a ``name``, each with a ``tables`` list of objects with a ``name``). dbt is the only reader of the sources file, so this names the file when
    dbt's own text does not."""
    yaml = importlib.import_module("yaml")
    found = []
    for path in sorted(project_dir.rglob("*.yml")):
        if {"dbt_packages", "target"} & set(path.relative_to(project_dir).parts):
            continue
        try:
            document = yaml.safe_load(path.read_text())
        except (yaml.YAMLError, UnicodeDecodeError, RecursionError, OSError) as error:
            found.append(f"{path} does not read as YAML ({type(error).__name__})")
            continue
        if not isinstance(document, dict) or "sources" not in document:
            continue
        sources = document["sources"]
        if not isinstance(sources, list) or not all(isinstance(source, dict) and isinstance(source.get("name"), str) for source in sources):
            found.append(f"{path}: sources must be a list of objects with a name")
            continue
        for source in sources:
            tables = source.get("tables", [])
            if not isinstance(tables, list) or not all(isinstance(table, dict) and isinstance(table.get("name"), str) for table in tables):
                found.append(f"{path}: the tables of source {source['name']} must be a list of objects with a name")
    return found


def validate_manifest(manifest: Any, label: str) -> None:
    """Check every value bronze_readers and derive_scope read from the manifest: ``metadata`` (with ``dbt_version`` a string when present), every source
    (an object with string ``source_name`` and ``name``) and every node (an object with a string ``resource_type``; a model also a ``name``, a
    ``depends_on`` object and its ``nodes`` list of strings). Other keys, and the fields of nodes that are not models, are ignored."""
    document = _object(manifest, label, "the document")
    metadata = _object(document.get("metadata", _ABSENT), label, "metadata")
    if "dbt_version" in metadata:
        _name(metadata["dbt_version"], label, "metadata.dbt_version")
    for unique_id, source in _object(document.get("sources", _ABSENT), label, "sources").items():
        fields = _object(source, label, f"sources[{unique_id}]")
        for key in ("source_name", "name"):
            _name(fields.get(key, _ABSENT), label, f"sources[{unique_id}].{key}")
    for unique_id, node in _object(document.get("nodes", _ABSENT), label, "nodes").items():
        where = f"nodes[{unique_id}]"
        fields = _object(node, label, where)
        if _name(fields.get("resource_type", _ABSENT), label, f"{where}.resource_type") != "model":
            continue
        _name(fields.get("name", _ABSENT), label, f"{where}.name")
        dependencies = _array(
            _object(fields.get("depends_on", _ABSENT), label, f"{where}.depends_on").get("nodes", _ABSENT), label, f"{where}.depends_on.nodes"
        )
        for position, dependency in enumerate(dependencies):
            _name(dependency, label, f"{where}.depends_on.nodes[{position}]")


def _read_variables(project_dir: Path) -> tuple[Mapping[str, Any], str]:
    """The ``vars`` of the dbt project file and the file's path. A file that is unreadable or shaped wrongly is a ScopeError naming it."""
    yaml = importlib.import_module("yaml")
    path = project_dir / "dbt_project.yml"
    try:
        document = yaml.safe_load(path.read_text())
    except (yaml.YAMLError, UnicodeDecodeError, RecursionError) as error:
        raise ScopeError(f"{path} is not valid YAML: {type(error).__name__}: {error}") from error
    return _object(_object(document, str(path), "the document").get("vars", _ABSENT), str(path), "vars"), str(path)


def read_copy_tables(project_dir: Path) -> dict[str, str]:
    """The ``copy_tables`` variable of the dbt project: bronze table to source family. A file that is unreadable or shaped wrongly is a ScopeError naming it."""
    variables, label = _read_variables(project_dir)
    validate_copy_tables(variables.get("copy_tables", _ABSENT), label)
    return dict(variables["copy_tables"])


def read_deferred_models(project_dir: Path) -> frozenset[str]:
    """The model names of the ``deferred_models`` variable of the dbt project; no record means none. A wrong shape is a ScopeError naming the file."""
    variables, label = _read_variables(project_dir)
    validate_deferred_models(variables.get("deferred_models"), label)
    return deferred_models.deferred_names({"vars": variables})


def load_map(path: Path) -> dict[str, Any]:
    """Read and validate the table map with the bronze loader and then with :func:`validate_table_map`; an invalid file is a ScopeError naming it, and a
    missing file stays an OSError."""
    try:
        document = bronze.load_table_map(path)
    except (ValueError, KeyError, TypeError, AttributeError, RecursionError, re.error) as error:
        raise ScopeError(f"{path} is not a valid table map: {type(error).__name__}: {error}") from error
    validate_table_map(document, str(path))
    return dict(document)


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


def derive_scope(
    manifest: Mapping[str, Any], table_map: Mapping[str, Any], copy_tables: Mapping[str, str], deferred: Collection[str] = ()
) -> tuple[dict[str, Any], list[str]]:
    """Derive the scope report and its problems from a parsed manifest, a table map document, the copy_tables variable and the deferred model names. Pure:
    reads nothing."""
    validate_table_map(table_map, "the table map")
    validate_copy_tables(copy_tables, "copy_tables")
    validate_manifest(manifest, "the manifest")
    names = [str(entry["table"]) for entry in table_map["tables"]]
    repeated = sorted({name for name in names if names.count(name) > 1})
    if repeated:
        raise ScopeError(f"the table map repeats {', '.join(repeated)}")
    mapped = {str(entry["table"]): entry for entry in table_map["tables"]}
    readers = bronze_readers(manifest)
    operational = set(OPERATIONAL_MODELS)
    analytical = {table: [model for model in models if model not in operational] for table, models in readers.items()}
    # A copied table whose own staging model is deferred stays in scope without an analytical reader while the copy-accounting model still reads its copies.
    # ``readers`` holds every declared source, so a declaration alone must not count.
    deferred_copies = {
        table: f"{deferred_models.STAGING_PREFIX}{table}"
        for table in copy_tables
        if table in mapped and operational & set(readers.get(table, ())) and f"{deferred_models.STAGING_PREFIX}{table}" in deferred
    }
    roots = sorted(table for table in mapped if analytical.get(table) or table in deferred_copies)
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
            "deferred_staging_model": None if analytical[table] else deferred_copies[table],
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
    interim = projected_counts(len(roots), len(dictionaries))
    delivered = projected_counts(len(roots), len(dictionaries), extra_loader_sources=1)
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
            "data_roots": len(roots),
            "analytical_roots": sum(1 for table in roots if analytical[table]),
            "loader_tables": list(LOADER_TABLES),
            "pins": {
                "decided_roots_only": len(pinned),
                "if_all_scope_tables_declared": len(pinned_all),
                "extra_if_all_declared": sorted(set(pinned_all) - set(pinned)),
                "current_yml_declared_sources": declared_in_dbt,
            },
            "glue_mirror_names": {"map_union_declared": len(mirror)},
        },
        "counts_by_q4_state": {"interim": interim, "index_delivered": delivered},
        "problems": sorted(problems),
    }
    return report, sorted(problems)


def _canonical(document: Any) -> str:
    """Canonical JSON text of a document: two rows are byte identical when these texts are equal."""
    return json.dumps(document, sort_keys=True, separators=(",", ":"))


def _projected(entries: Sequence[Mapping[str, Any]], keys: Sequence[str], where: str, found: list[str]) -> tuple[dict[str, dict[str, Any]], set[str]]:
    """Entries by table name, each cut down to the projected keys, and the tables that appear more than once with different rows; both are named in ``found``.

    A row without a table name is named. A table whose rows are not all byte identical gets exactly one diagnostic, whatever the order or the number of repeats:
    it shows every distinct definition (sorted) and says so when the definitions differ only outside the projection. Such a table is left out of the field
    comparison by the caller, so the answer never depends on which duplicate comes first.
    """
    rows: dict[str, list[Mapping[str, Any]]] = {}
    for entry in entries:
        if "table" not in entry:
            found.append(f"{where}: an entry has no table name")
            continue
        rows.setdefault(str(entry["table"]), []).append(entry)
    conflicts: set[str] = set()
    for table, group in sorted(rows.items()):
        if len({_canonical(row) for row in group}) > 1:
            conflicts.add(table)
            shown = sorted({_canonical({key: row[key] for key in keys if key in row}) for row in group})
            outside = " (the rows differ only outside the projection)" if len(shown) == 1 else ""
            found.append(f"{where}: {table} appears more than once with different definitions{outside}: {' and '.join(shown)}")
    return {table: {key: group[0][key] for key in keys if key in group[0]} for table, group in rows.items()}, conflicts


def _differences(where: str, ours: Mapping[str, Any], theirs: Mapping[str, Any], keys: Sequence[str]) -> list[str]:
    """Where the report side and the expected side differ in ``keys``. Each key must be on both sides, because a missing key is not null. Values are
    compared as JSON text, so true is not 1 and false is not 0."""
    found: list[str] = []
    for key in keys:
        absent = [side for side, values in (("the report", ours), ("the expected document", theirs)) if key not in values]
        if absent:
            found.extend(f"{where}: {key} is missing in {side}" for side in absent)
        elif _canonical(ours[key]) != _canonical(theirs[key]):
            found.append(f"{where}: {key} is {ours[key]!r}, expected {theirs[key]!r}")
    return found


def _shape_problems(document: Any, where: str) -> list[str]:
    """Named problems with the shape of a report or an expected document: the parts equality reads must be objects, and lists of objects with string tables."""
    if not isinstance(document, dict):
        return [f"{where}: the document is not an object"]
    found = []
    for group in PROJECTION:
        rows = document.get(group)
        if not isinstance(rows, list):
            found.append(f"{where}: {group} is missing or not a list")
            continue
        for index, row in enumerate(rows):
            if not isinstance(row, dict):
                found.append(f"{where}: {group}[{index}] is not an object")
            elif "table" in row and not isinstance(row["table"], str):
                found.append(f"{where}: {group}[{index}].table is not a string")
    declaration = document.get("silver_source_declaration")
    for name, value in (("counts", document.get("counts")), ("silver_source_declaration", declaration)):
        if not isinstance(value, dict):
            found.append(f"{where}: {name} is missing or not an object")
    if isinstance(declaration, dict):
        found.extend(
            f"{where}: silver_source_declaration.{name} is missing or not an object"
            for name in ("pins", "glue_mirror_names")
            if not isinstance(declaration.get(name), dict)
        )
    return found


def equality_problems(report: Mapping[str, Any], expected: Mapping[str, Any]) -> list[str]:
    """Where the consumer-scope projection of ``report`` differs from ``expected`` (a report or a draft). An empty list means equal.

    The projection is the table lists with the keys of :data:`PROJECTION`, the twelve :data:`COUNT_KEYS` and the silver declaration facts. Each of these
    keys must be on both sides, and values are equal only when their JSON texts are. List order of the table lists is ignored: entries are matched by table
    name. A table on one side only, a missing or changed key or a changed count is named. A table repeated within one side gets
    exactly one diagnostic showing every distinct definition (never resolved to one of them) unless the repeated rows are byte identical. A document of the
    wrong shape is named, not raised. Acquisition annotations, gap records, policy relationships, retained history and descriptive text are not compared.
    """
    found = _shape_problems(report, "report") + _shape_problems(expected, "expected document")
    if found:
        return found
    for group, keys in PROJECTION.items():
        ours, ours_conflicts = _projected(report[group], keys, f"{group} (report)", found)
        theirs, theirs_conflicts = _projected(expected[group], keys, f"{group} (expected document)", found)
        skip = ours_conflicts | theirs_conflicts
        ours, theirs = ({table: row for table, row in side.items() if table not in skip} for side in (ours, theirs))
        found.extend(f"{group}: {table} is in the report only" for table in sorted(set(ours) - set(theirs)))
        found.extend(f"{group}: {table} is in the expected document only" for table in sorted(set(theirs) - set(ours)))
        for table in sorted(set(ours) & set(theirs)):
            found.extend(_differences(f"{group}: {table}", ours[table], theirs[table], keys))
    found.extend(_differences("counts", report["counts"], expected["counts"], COUNT_KEYS))
    ours_declaration, theirs_declaration = report["silver_source_declaration"], expected["silver_source_declaration"]
    found.extend(_differences("silver_source_declaration", ours_declaration, theirs_declaration, DECLARATION_KEYS))
    found.extend(_differences("silver_source_declaration.pins", ours_declaration["pins"], theirs_declaration["pins"], PINS_KEYS))
    mirrors = ours_declaration["glue_mirror_names"], theirs_declaration["glue_mirror_names"]
    found.extend(_differences("silver_source_declaration.glue_mirror_names", *mirrors, ("map_union_declared",)))
    return found


def report_bytes(report: Mapping[str, Any]) -> bytes:
    """The report's exact bytes: sorted keys, two-space indent, one trailing newline."""
    return (json.dumps(report, indent=2, sort_keys=True) + "\n").encode()


def write_atomically(path: Path, data: bytes) -> None:
    """Write ``data`` to a temporary file beside ``path`` and replace ``path`` with it, so a failed write leaves an older file whole and no temporary file."""
    handle, name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(handle, "wb") as stream:
            stream.write(data)
        os.replace(name, path)
    except BaseException:
        Path(name).unlink(missing_ok=True)
        raise


def _landing_places(path: Path) -> list[str]:
    """The paths a write to ``path`` can touch: the directory entry the atomic replace puts in place and the file the links of ``path`` lead to."""
    return [os.path.normpath(os.path.join(os.path.realpath(path.parent), path.name)), os.path.realpath(path)]


def _same_file(first: str, second: str) -> bool:
    """Whether two resolved paths name one file: equal text in any letter case (the macOS file system ignores case) or one file on disk."""
    if first.casefold() == second.casefold():
        return True
    try:
        return os.path.samefile(first, second)
    except OSError:
        return False


def _within(path: str, folder: str) -> bool:
    """Whether a resolved path is a resolved folder or lies below it, in any letter case."""
    top = Path(folder.casefold()).parts
    return Path(path.casefold()).parts[: len(top)] == top


def destination_problems(out: Path, timing_out: Path | None, project_dir: Path, table_map_path: Path) -> list[str]:
    """Why ``--out`` or ``--timing-out`` may not be written, one line each. A line says that the two name one file or that one of them names an input of
    the command or a path in a production folder of the repository. An empty list means both are free."""
    files = {"the table map": table_map_path, "the pinned dbt": dbt_manifest_labels.DBT}
    folders = {
        "the dbt project folder": project_dir,
        "the repository's config folder": ROOT / "config",
        "the repository's dbt folder": ROOT / PROJECT,
        "the dbt package archive folder": ROOT / dbt_package_archive.ARCHIVES,
    }
    destinations = {"--out": out, "--timing-out": timing_out}
    places = {flag: _landing_places(path) for flag, path in destinations.items() if path is not None}
    found = []
    if "--timing-out" in places and any(_same_file(first, second) for first in places["--out"] for second in places["--timing-out"]):
        found.append(f"--timing-out {timing_out} is refused: it names the same file as --out {out}")
    for flag, landing in places.items():
        reasons = [f"it is {what}" for what, path in files.items() if any(_same_file(place, os.path.realpath(path)) for place in landing)]
        reasons += [f"it is in {what} ({path})" for what, path in folders.items() if any(_within(place, os.path.realpath(path)) for place in landing)]
        if reasons:
            found.append(f"{flag} {destinations[flag]} is refused: {reasons[0]}")
    return found


def scope_of_project(project_dir: Path, table_map_path: Path, root: Path | None) -> tuple[dict[str, Any], list[str], float]:
    """Parse a dbt project with the pinned dbt and derive its scope; returns the report, its problems and the parse seconds.

    ``root`` is the repository whose locked package archive supplies the dbt packages; None parses without packages (a synthetic project).
    """
    # dbt runs in a scratch folder, where a relative project path would not resolve.
    project_dir = project_dir.absolute()
    table_map = load_map(table_map_path)
    copy_tables = read_copy_tables(project_dir)
    deferred = read_deferred_models(project_dir)
    with tempfile.TemporaryDirectory(prefix="hai_scope_") as scratch:
        work = Path(scratch)
        packages = None
        if root is not None:
            dbt_manifest_labels.install_packages(root, work / "dbt_packages")
            packages = work / "dbt_packages"
        began = time.monotonic()
        try:
            manifest = dbt_manifest_labels.parse_manifest(project_dir, project_dir, work, packages)
        except ProcessGroupSurvived:
            raise
        except TimeoutExpired as expired:
            raise ScopeError(f"the dbt parse of {project_dir} did not finish within {expired.timeout} s") from expired
        except dbt_manifest_labels.ManifestError as error:
            hints = project_file_problems(project_dir)
            raise ScopeError(f"the dbt parse of {project_dir} failed{': ' + '; '.join(hints) if hints else ''}\n{error}") from error
        except (ValueError, RecursionError) as error:
            raise ScopeError(f"manifest.json of the dbt parse of {project_dir} is not valid JSON: {type(error).__name__}: {error}") from error
        seconds = time.monotonic() - began
    validate_manifest(manifest, f"manifest.json of the dbt parse of {project_dir}")
    report, problems = derive_scope(manifest, table_map, copy_tables, deferred)
    return report, problems, seconds


def main(argv: Sequence[str] | None = None) -> int:
    """Write the scope report; exit 0 on a clean report, 1 on named problems (no report written), 2 on a refused destination or a project that cannot be
    parsed."""
    parser = argparse.ArgumentParser(description="Derive the default bronze scope from a fresh dbt parse (read only).")
    parser.add_argument("--out", type=Path, required=True, help="the report file to write, only on a clean run")
    parser.add_argument("--timing-out", type=Path, help="a file for the dbt parse seconds, kept apart from the deterministic report")
    parser.add_argument("--project", type=Path, default=ROOT / PROJECT, help="the dbt project folder")
    parser.add_argument("--table-map", type=Path, default=ROOT / TABLE_MAP, help="the bronze table map")
    parser.add_argument("--no-packages", action="store_true", help="parse without the locked dbt packages (a synthetic project)")
    arguments = parser.parse_args(argv)
    try:
        refused = destination_problems(arguments.out, arguments.timing_out, arguments.project, arguments.table_map)
        if refused:
            raise ScopeError("; ".join(refused))
        report, problems, seconds = scope_of_project(arguments.project, arguments.table_map, None if arguments.no_packages else ROOT)
        for line in problems:
            sys.stdout.write(f"{line}\n")
        counts = report["counts"]
        summary = f"roots {counts['in_scope_data']}, dictionaries {counts['dictionary']}, leaving {counts['leaving']}, dbt parse {seconds:.1f} s"
        sys.stdout.write(f"scope report: {'fail' if problems else 'ok'} ({len(problems)} problems, {summary})\n")
        if arguments.timing_out is not None:
            write_atomically(arguments.timing_out, (json.dumps({"dbt_parse_seconds": round(seconds, 3)}, indent=2) + "\n").encode())
        if problems:
            return 1
        write_atomically(arguments.out, report_bytes(report))
    except (ProcessGroupSurvived, LaunchesNotStopped, SessionListingFailed):
        raise
    except (ScopeError, dbt_manifest_labels.ManifestError, dbt_package_archive.ArchiveError, OSError) as error:
        sys.stderr.write(f"scope report: cannot derive: {error}\n")
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
