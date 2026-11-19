"""Derive, write and check the declared bronze consumer columns (host side; the bronze job reads only ``bronze_consumer_columns``).

``derive`` compiles the real dbt project with the lint target (the pinned dbt of .tools/sqlfluff, an in-memory DuckDB with nothing attached), parses every
compiled model and test (``bronze_consumer_lineage``) and returns one declaration per bronze table that any model reads. The compile cannot see what a model
resolves only when it runs; the one such reader is ``int_acs_county_values``, which reads the columns that ``dbt/seeds/acs_variable_map.csv`` maps for each ACS
table, so ``DYNAMIC_READERS`` adds those reads from the seed.

``check`` fails on every difference between the committed file and the models, and on a completeness gap: every staged table (a ``vars.copy_tables`` entry whose
``stg_`` model is not deferred) and every deferred one must be declared, and a deferred table carries its note.
"""

from __future__ import annotations

import csv
import importlib
import json
import sys
import tempfile
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from scripts.lakehouse import bronze_consumer_columns as columns
from scripts.lakehouse import bronze_consumer_lineage as lineage
from scripts.lakehouse import deferred_models

ROOT = Path(__file__).resolve().parents[2]
DBT = ROOT / ".tools" / "sqlfluff" / "bin" / "dbt"
PROJECT = "dbt/dbt_project.yml"
COMPILE_SECONDS = 300
BASIS = (
    "The bronze columns that compiled silver models and tests read, found by parsing the compiled SQL: each column lists the models that read it, and "
    "a table that "
    "cannot be reduced to a column list is declared 'any' with the reason. Regenerate with "
    "`.venv/bin/python -m scripts.lakehouse.bronze_consumer_columns --write`; the bronze load refuses a loaded table that lacks a declared column."
)


@dataclass(frozen=True)
class DynamicReader:
    """A reader whose columns are chosen when the model runs, so the lint compile cannot see them.

    It names the model, the seed, the seed column that names the bronze table, the seed column that names the bronze column, the seed filter and the columns the
    model always reads of each table.
    """

    model: str
    seed: str
    table_column: str
    name_column: str
    filter: Mapping[str, str]
    always: tuple[str, ...]


DYNAMIC_READERS = (
    DynamicReader(
        model="int_acs_county_values",
        seed="dbt/seeds/acs_variable_map.csv",
        table_column="bronze_table",
        name_column="column_name",
        filter={"status": "mapped"},
        always=("geo_id",),
    ),
)


def compile_project(project: Path, target: Path, packages: Path | None = None) -> Path:
    """Compile the dbt project with the lint target and return the folder of the compiled models."""
    from scripts.process import run_command

    environment = {
        "DUCKDB_MEMORY_LIMIT": "2GB",
        "JOB_THREADS": "1",
        "DBT_PACKAGES_INSTALL_PATH": str((packages or ROOT / "data/analytics/dbt/dbt_packages").resolve()),
    }
    arguments = ["compile", "--target", "lint", "--project-dir", str(project), "--profiles-dir", str(project), "--target-path", str(target)]
    result = run_command(str(DBT), arguments, env=environment, timeout=COMPILE_SECONDS)
    if result.returncode != 0:
        tail = result.stdout.strip().splitlines()[-1:] or result.stderr.strip()[-300:]
        raise columns.ConsumerColumnsError(f"dbt compile failed with code {result.returncode}: {tail}")
    return target / "compiled"


def read_project(root: Path) -> dict[str, Any]:
    """The dbt project file (its ``vars`` hold ``copy_tables`` and ``deferred_models``)."""
    yaml = importlib.import_module("yaml")
    project: dict[str, Any] = yaml.safe_load((root / PROJECT).read_text())
    return project


def deferred_tables(project: Mapping[str, Any]) -> dict[str, str]:
    """Each copied table whose staging view is deferred, with the deferral note (the staged tables are the others, read by the shared helper)."""
    staged = set(deferred_models.staged_tables(project))
    deferred = deferred_models.deferred_names(project)
    return {
        table: f"stg_{table} is deferred in vars.deferred_models; no silver model reads its columns now, only the lineage readers"
        for table in project["vars"]["copy_tables"]
        if table not in staged and f"{deferred_models.STAGING_PREFIX}{table}" in deferred
    }


def dynamic_reads(root: Path) -> dict[str, dict[str, set[str]]]:
    """The reads that the lint compile cannot see: table to column to models, from the seeds of ``DYNAMIC_READERS``."""
    found: dict[str, dict[str, set[str]]] = {}
    for reader in DYNAMIC_READERS:
        with (root / reader.seed).open(newline="") as handle:
            for row in csv.DictReader(handle):
                if any(row[key] != value for key, value in reader.filter.items()):
                    continue
                table = found.setdefault(row[reader.table_column], {})
                table.setdefault(row[reader.name_column].lower(), set()).add(reader.model)
                for name in reader.always:
                    table.setdefault(name, set()).add(reader.model)
    return found


def derive(compiled: Path, root: Path = ROOT, parser: lineage.Parser = lineage.parse) -> tuple[dict[str, dict[str, Any]], dict[str, str]]:
    """One entry per bronze table that a model reads, and the models that could not be analyzed (name to reason)."""
    models = lineage.read_models(compiled)
    needs, unparsed = lineage.bronze_needs(models, parser)
    entries: dict[str, dict[str, Any]] = {}
    for table, value in needs.items():
        entries[table] = {"any": value} if isinstance(value, str) else {"columns": {name: sorted(readers) for name, readers in sorted(value.items())}}
    for table, added in dynamic_reads(root).items():
        entry = entries.get(table)
        if entry is None or "any" in entry:
            continue
        for name, readers in added.items():
            entry["columns"][name] = sorted({*entry["columns"].get(name, []), *readers})
        entry["columns"] = dict(sorted(entry["columns"].items()))
    for table, note in deferred_tables(read_project(root)).items():
        entries.setdefault(table, {"columns": {}})["deferred"] = note
    return dict(sorted(entries.items())), unparsed


def render(entries: Mapping[str, Mapping[str, Any]]) -> str:
    """The contract file text for the derived entries."""
    document = {"version": columns.VERSION, "basis": BASIS, "tables": {table: dict(entry) for table, entry in sorted(entries.items())}}
    return json.dumps(document, indent=1) + "\n"


def entry_of(declaration: columns.Declaration) -> dict[str, Any]:
    """A loaded declaration in the shape ``derive`` returns."""
    entry: dict[str, Any] = (
        {"any": declaration.reason} if declaration.columns is None else {"columns": {name: list(models) for name, models in declaration.columns.items()}}
    )
    if declaration.deferred:
        entry["deferred"] = declaration.deferred
    return entry


def differences(derived: Mapping[str, Mapping[str, Any]], contract: Mapping[str, columns.Declaration]) -> list[str]:
    """One line for each table, column or reader on which the models and the contract disagree, naming the table and the column."""
    lines: list[str] = []
    for table in sorted(set(derived) | set(contract)):
        if table not in contract:
            lines.append(f"{table}: a model reads it but the contract does not declare it")
            continue
        if table not in derived:
            lines.append(f"{table}: the contract declares it but no model reads it")
            continue
        old, new = entry_of(contract[table]), derived[table]
        if ("any" in old) != ("any" in new):
            kinds = {True: "any column", False: "a column list"}
            lines.append(f"{table}: the contract declares {kinds['any' in old]} but the models give {kinds['any' in new]}")
            continue
        if "any" in new:
            if old["any"] != new["any"]:
                lines.append(f"{table}: the reason it is read through any column changed ({new['any']})")
            continue
        for name in sorted(set(new["columns"]) - set(old["columns"])):
            lines.append(f"{table}: a model reads {name} but the contract does not declare it")
        for name in sorted(set(old["columns"]) - set(new["columns"])):
            lines.append(f"{table}: the contract declares {name} but no model reads it")
        for name in sorted(set(old["columns"]) & set(new["columns"])):
            if old["columns"][name] != new["columns"][name]:
                lines.append(f"{table}: the models that read {name} changed ({', '.join(new['columns'][name])})")
        if old.get("deferred", "") != new.get("deferred", ""):
            lines.append(f"{table}: the deferral note differs from vars.deferred_models")
    return lines


def completeness(contract: Mapping[str, columns.Declaration], project: Mapping[str, Any]) -> list[str]:
    """The gaps between the declared tables and the staged and deferred tables of the project."""
    lines: list[str] = []
    deferred = deferred_tables(project)
    for table in deferred_models.staged_tables(project):
        if table not in contract:
            lines.append(f"{table}: its staging view {deferred_models.STAGING_PREFIX}{table} exists but the contract has no declaration for the table")
    for table in deferred:
        if table not in contract:
            lines.append(f"{table}: its staging view is deferred and the contract does not declare it as deferred")
        elif not contract[table].deferred:
            lines.append(f"{table}: its staging view is deferred but its declaration has no deferred note")
    for table, declaration in contract.items():
        if declaration.deferred and table not in deferred:
            lines.append(f"{table}: its declaration carries a deferred note but its staging view is not deferred")
    return lines


def layering(models: list[lineage.Model]) -> list[str]:
    """Models outside silver that read a bronze table or a staging model. The drift check runs with the silver group, so a gold or feature model must not."""
    reads, _ = lineage.analyze(models)
    outside = {model.name for model in models if model.external}
    found = []
    for relation, readers in sorted(reads.readers.items()):
        staged = relation[0] == "table" and relation[1].startswith(deferred_models.STAGING_PREFIX) and not relation[1].startswith("stg_bronze__")
        if relation[0] == "bronze" or staged:
            found += [
                f"{reader}: a model outside silver reads {relation[1]}, which the declaration check does not schedule for its edits"
                for reader in sorted(readers & outside)
            ]
    return found


def run(root: Path | None = None, write: bool = False) -> int:
    """Check or write the contract file against a compile of the real dbt project."""
    base = root or ROOT
    with tempfile.TemporaryDirectory(prefix="bronze_columns_") as folder:
        compiled = compile_project(base / "dbt", Path(folder) / "target", packages=ROOT / "data/analytics/dbt/dbt_packages")
        derived, unparsed = derive(compiled, base)
        placed = layering(lineage.read_models(compiled))
    path = base / columns.CONTRACT
    if write:
        path.write_text(render(derived))
        sys.stdout.write(f"wrote {len(derived)} tables to {columns.CONTRACT}\n")
        return 0
    loaded = columns.load(path)
    lines = [*differences(derived, loaded), *completeness(loaded, read_project(base)), *placed]
    for line in lines:
        sys.stderr.write(f"{columns.CONTRACT}: {line}\n")
    if lines:
        sys.stderr.write(f"{len(lines)} differences. Regenerate with: .venv/bin/python -m scripts.lakehouse.bronze_consumer_columns --write\n")
        return 1
    anys = sorted(table for table, entry in derived.items() if "any" in entry)
    sys.stdout.write(f"bronze consumer columns ok: {len(derived)} tables declared, {len(anys)} read through any column ({', '.join(anys)})\n")
    if unparsed:
        names = ", ".join(sorted(unparsed))
        sys.stdout.write(f"{len(unparsed)} models could not be parsed (a bronze or staging relation they name is read through any column): {names}\n")
    return 0
