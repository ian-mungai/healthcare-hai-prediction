"""Independent check of the feature models' output columns against their YAML, and the database both feature checks build.

Run from the repository root:

    .venv/bin/python -m scripts.lakehouse.feature_columns --check [--root DIR] [--database FILE]

The YAML is ``dbt/models/features/_features__models.yml``. dbt enforces the column contract of every feature model when it builds (``+contract`` in
``dbt/dbt_project.yml``). This check does not rely on it: it builds the feature phase once more on a scratch DuckDB file whose gold tables are empty
(the columns and types of ``data_contracts/lakehouse/gold_contract.json``, the real seeds, the control bridge) with the contract switched off, reads
the columns the database gives each model and compares them with the YAML. It fails on a model without a YAML entry, a YAML entry without a model, a
column the model gives that the YAML does not document, a documented column the model does not give, a data type that differs and a column without a
data type or description. ``--database`` reads a file built earlier by the same steps instead of building. The DuckDB steps run in the SQLFluff tool
environment (``.tools/sqlfluff``), which holds dbt and DuckDB; the comparison runs on the host.

The build is shared with ``feature_sources`` so that one reviewed path decides what a feature model can read. ``build_shell`` builds the empty gold tables,
all seeds and the control bridge in ``full.duckdb`` and copies into ``shell.duckdb`` only what a feature model may read: the published gold tables of the
frozen contract (as far as the contract lists their columns), the seeds of ``dbt/seeds/features`` and the live reference seeds directly under ``dbt/seeds``.
``run_features`` then builds the feature phase in two stages. Stage 1 is no dbt run: this module builds the bridge filter tables itself in DuckDB, one per
retired seed (``create table <filter> as select * from bridge_measure_control where source_seed = ?``, the seed taken from the file name
``dbt/models/features/bridge/feature_bridge_<seed>.sql``), so no hook, macro or configuration of the project can change their rows. Stage 2 runs every other
``tag:features`` model with dbt on a new file that holds the tables of ``shell.duckdb`` without ``bridge_measure_control`` plus those filter tables, with
external access off. The scratch copy of the project has its ``on-run-start`` and ``on-run-end`` lists emptied, so no run hook acts on either stage. A read
of the raw bridge in stage 2 (a ``ref``, ``query_table``, a macro, an alias, an aggregate) and a read of any other relation (silver, bronze, a retired seed,
a bridge seed) or of a file fails when DuckDB binds the query, however the read is spelled. ``compile_manifest`` and ``parsed_manifest`` also read the selection
of ``dbt ls --select tag:features`` into the manifest, and ``bind_nodes`` binds the compiled query of every selected model and test (the generic tests of
seeds included) in the database of its stage. ``feature_sources`` rejects what the guard cannot bind or trust (hooks, macro overrides, other node types,
a relation that the run added or rewrote) and, through ``feature_allowlist``, every Jinja construct, macro and config setting outside a closed list.
End-to-end run with planted defects: ``.venv/bin/python -m
scripts.lakehouse.run_feature_columns_e2e``."""

from __future__ import annotations

import argparse
import importlib
import json
import os
import re
import shutil
import sys
import tempfile
from pathlib import Path
from typing import Any, NamedTuple

from scripts.process import TimeoutExpired, run_command

yaml: Any = importlib.import_module("yaml")

REPO_ROOT = Path(__file__).resolve().parents[2]
MODELS = "dbt/models/features"
MODELS_YAML = f"{MODELS}/_features__models.yml"
GOLD_CONTRACT = "data_contracts/lakehouse/gold_contract.json"
ISOLATION_RULE = "the feature phase reads gold only (see docs/feature_phase_saved.md)"
RULE = "every output column of a feature model is documented with its data type and description (sql_and_dbt.md item 8, design: frozen output columns)"
FEATURE_SEEDS = "dbt/seeds/features"
LIVE_SEEDS = "dbt/seeds"
STEP_SECONDS = 600
CONTRACT_ON = re.compile(r"(\+contract:\s*\n\s*enforced:\s*)true")
SCOPE = ("models/features/", "tests/features/", "seeds/features/")
FILTER_FOLDER = f"{MODELS}/bridge"
FILTER_PATH = "models/features/bridge"
FILTER_FILE = re.compile(r"models/features/bridge/feature_bridge_[a-z0-9_]+\.sql")
FILTER_PREFIX = "feature_bridge_"
FILTER_NAME = re.compile(r"feature_bridge_[a-z0-9_]+")
RUN_HOOKS = ("on-run-start", "on-run-end")
# The one statement that makes a bridge filter table (the seed is its parameter); the raw bridge is attached as ``shell`` while it runs.
FILTER_SQL = "select * from shell.main.bridge_measure_control where source_seed = ?"
RAW_BRIDGE = "bridge_measure_control"
# The gold models that ``build_shell`` runs with dbt, beside every seed: the registry controls and the raw control bridge built from the bridge seeds.
SHELL_MODELS = {"dim_registry_control", RAW_BRIDGE}
# Only the feature run opens its database with this option: no file, extension or other database can be read, and DuckDB does not allow switching it on again.
LOCKED_SETTINGS = "      config_options:\n        enable_external_access: false\n"
SHELL_HINT = (
    "The database holds only the gold tables of the frozen contract that list their columns (not bridge_measure_control), the live seeds (dbt/seeds and "
    "dbt/seeds/features) and the bridge filter models (read feature_bridge_<retired seed> instead of the bridge); a model or test reads only the relations "
    "that dbt/models/features/_feature_inputs.json pins for it"
)
UNREADABLE = "unreadable: "
# The label of every build step this process started (dbt runs and DuckDB workers), in order. The end-to-end runs read it to show which steps a rejected
# project started.
STARTED_STEPS: list[str] = []
ERROR_IN_NODE = re.compile(r"(?:Database|Compilation|Runtime) Error in (?:model|test|seed) (\S+) \((\S+)\)\n((?:  .*\n?)+)")


class BuildError(Exception):
    """The scratch build of the feature phase failed."""


def sql_models(root: Path) -> list[str]:
    """The names of the feature models, one SQL file each (the bridge filter models in the subfolder included)."""
    return sorted(path.stem for path in (root / MODELS).rglob("*.sql"))


def documented(path: Path) -> dict[str, dict[str, dict[str, str]]]:
    """The documented columns of each model of a YAML file: name, data type and description by column."""
    document = yaml.safe_load(path.read_text()) or {}
    return {
        model["name"]: {
            column["name"]: {"data_type": str(column.get("data_type") or ""), "description": " ".join(str(column.get("description") or "").split())}
            for column in model.get("columns") or []
        }
        for model in document.get("models") or []
    }


def compare(models: list[str], built: dict[str, list[list[str]]], docs: dict[str, dict[str, dict[str, str]]]) -> list[str]:
    """Problems between the columns the database gives each model and the YAML."""
    problems: list[str] = []
    for model in sorted(set(models) | set(docs)):
        where = f"{MODELS_YAML}:1"
        if model not in docs:
            problems.append(f"{where}: model {model} has no entry in the YAML, so none of its columns is documented. Rule: {RULE}.")
            continue
        if model not in models:
            problems.append(f"{where}: the YAML documents model {model}, which has no SQL file in {MODELS}. Rule: {RULE}.")
            continue
        columns = built.get(model)
        if columns is None:
            problems.append(f"{where}: model {model} was not built, so its columns were not read. Rule: {RULE}.")
            continue
        types = {name: kind.lower().replace(" ", "") for name, kind in columns}
        for name in types:
            if name not in docs[model]:
                problems.append(
                    f"{where}: {model}.{name} ({types[name]}) is an output column that the YAML does not document. "
                    f"Rule: {RULE}. Add it with data_type and description."
                )
        for name, entry in docs[model].items():
            if name not in types:
                problems.append(
                    f"{where}: {model}.{name} is documented in the YAML but the model does not give it. Rule: {RULE}. Remove it or restore the column."
                )
                continue
            if not entry["data_type"]:
                problems.append(f"{where}: {model}.{name} has no data_type in the YAML; the model gives {types[name]}. Rule: {RULE}.")
            elif entry["data_type"].lower().replace(" ", "") != types[name]:
                problems.append(f"{where}: {model}.{name} is {types[name]} in the model but data_type {entry['data_type']} in the YAML. Rule: {RULE}.")
            if not entry["description"]:
                problems.append(f"{where}: {model}.{name} has no description in the YAML. Rule: {RULE}.")
    return problems


def require_tools(tool_root: Path) -> None:
    """Fail when the SQLFluff tool environment, which holds dbt and DuckDB, is not installed."""
    if not (tool_root / ".tools/sqlfluff/bin/python").is_file():
        raise BuildError(f"{tool_root}/.tools/sqlfluff is missing; run .venv/bin/python -m scripts.quality.install_sqlfluff")


def execute(program: Path, arguments: list[str], cwd: Path, env: dict[str, str], label: str) -> tuple[int, str]:
    """Run one build step and return its exit code and output; raise when it runs too long. The label of each step is added to ``STARTED_STEPS`` first."""
    STARTED_STEPS.append(label)
    try:
        result = run_command(str(program), arguments, cwd=cwd, env=env, timeout=STEP_SECONDS)
    except TimeoutExpired as error:
        raise BuildError(f"{label} ran longer than {STEP_SECONDS} seconds") from error
    return result.returncode, (result.stdout if result.returncode == 0 else f"{result.stdout}\n{result.stderr}")


def step(program: Path, arguments: list[str], cwd: Path, env: dict[str, str], label: str) -> str:
    """Run one build step and return its output; raise when it fails or runs too long."""
    code, output = execute(program, arguments, cwd, env, label)
    if code != 0:
        raise BuildError(f"{label} exited {code}: {output.strip()[-1200:]}")
    return output


def is_filter(node: dict[str, Any]) -> bool:
    """Whether a manifest node is a bridge filter model: a model at the exact path ``models/features/bridge/feature_bridge_<seed>.sql``.

    A folder prefix is not enough: a generic test declared in ``models/features/bridge_review.yml`` is no filter model and is bound in stage 2 like every test.
    """
    return node.get("resource_type") == "model" and FILTER_FILE.fullmatch(node.get("original_file_path", "")) is not None


def selected_nodes(manifest: dict[str, Any]) -> dict[str, Any]:
    """The manifest nodes of the feature phase by unique id: every node that ``dbt ls --select tag:features`` listed (``compile_manifest`` stores the list)
    and every node of any type that lives in a feature folder (models, singular tests, seeds and the generic tests that a YAML file there declares)."""
    chosen = manifest.get("feature_selection", {})
    return {
        unique_id: node for unique_id, node in manifest.get("nodes", {}).items() if unique_id in chosen or node.get("original_file_path", "").startswith(SCOPE)
    }


def tool_environment(tool_root: Path) -> dict[str, str]:
    """The environment of a dbt or DuckDB step: no usage statistics, and the verified dbt packages when the checkout has them."""
    env = {**os.environ, "DBT_SEND_ANONYMOUS_USAGE_STATS": "false"}
    packages = (tool_root / ".tools").resolve().parent / "data" / "analytics" / "dbt" / "dbt_packages"
    if packages.is_dir():
        env["DBT_PACKAGES_INSTALL_PATH"] = str(packages)
    return env


def copy_project(root: Path, project: Path, contract: bool = True) -> None:
    """Copy the dbt project to a scratch folder for a build: ``contract=False`` switches the model contracts off, and the lists ``on-run-start`` and
    ``on-run-end`` are emptied, so no run hook (the project's own or one added) acts on the scratch databases. ``feature_sources`` pins the project's lists."""
    shutil.copytree(root / "dbt", project, ignore=shutil.ignore_patterns("target", "logs", "dbt_packages"))
    path = project / "dbt_project.yml"
    text = path.read_text()
    if not contract:
        if not CONTRACT_ON.search(text):
            raise BuildError("dbt/dbt_project.yml has no +contract enforced: true for the feature models, so there is no contract to switch off")
        text = CONTRACT_ON.sub(r"\1false", text)
    document = yaml.safe_load(text) or {}
    document.update({key: [] for key in RUN_HOOKS})
    path.write_text(yaml.safe_dump(document, sort_keys=False, width=10**6, allow_unicode=True))


def write_profile(folder: Path, database: Path, locked: bool) -> Path:
    """Write ``profiles.yml`` with one DuckDB target ``shell`` on a file and return the folder; ``locked`` turns external access off (DuckDB keeps it off)."""
    folder.mkdir(parents=True, exist_ok=True)
    (folder / "profiles.yml").write_text(
        f"hai:\n  target: shell\n  outputs:\n    shell:\n      type: duckdb\n      path: {database}\n      threads: 1\n{LOCKED_SETTINGS if locked else ''}"
    )
    return folder


def dbt_arguments(project: Path, profiles: Path, work: Path) -> list[str]:
    """The options every dbt step of the shell build shares."""
    return [
        "--project-dir",
        str(project),
        "--profiles-dir",
        str(profiles),
        "--profile",
        "hai",
        "--target",
        "shell",
        "--target-path",
        str(work / "target"),
        "--log-path",
        str(work / "log"),
        "--no-partial-parse",
        "--no-version-check",
        "--no-use-colors",
    ]


def worker(tool_root: Path, action: str, *arguments: str) -> str:
    """Run one DuckDB step of this module in the tool environment."""
    python = tool_root / ".tools/sqlfluff/bin/python"
    return step(python, ["-m", "scripts.lakehouse.feature_columns", "--worker", action, *arguments], REPO_ROOT, tool_environment(tool_root), f"worker {action}")


def build_shell(root: Path, folder: Path, tool_root: Path = REPO_ROOT) -> Path:
    """Build the empty gold tables, seeds and control bridge in ``full.duckdb``, copy what a feature model may read into ``shell.duckdb`` and return that file.

    What a feature model may read is the published gold tables of the frozen contract (the empty ones, ``dim_measure``, ``dim_registry_control`` and
    ``bridge_measure_control`` with their rows), the seeds of ``dbt/seeds/features`` and the live reference seeds directly under ``dbt/seeds`` (static lookup
    tables that hold no silver data). Silver tables do not exist in either file; the bridge seeds and every other seed exist only in ``full.duckdb``.
    """
    require_tools(tool_root)
    dbt = tool_root / ".tools/sqlfluff/bin/dbt"
    project = folder / "gold_project"
    copy_project(root, project)
    full, shell = folder / "full.duckdb", folder / "shell.duckdb"
    profiles = write_profile(folder / "gold_profiles", full, locked=False)
    env = tool_environment(tool_root)

    def run_dbt(*arguments: str) -> str:
        return step(dbt, [*arguments, *dbt_arguments(project, profiles, folder / "gold_work")], root, env, f"dbt {' '.join(arguments[:3])}")

    worker(tool_root, "shell", str(full), str(root / GOLD_CONTRACT))
    run_dbt("seed", "--select", "resource_type:seed")
    worker(tool_root, "fill", str(full))
    run_dbt("run", "--select", "dim_registry_control", "bridge_measure_control")
    seeds = sorted({path.stem for folder in (FEATURE_SEEDS, LIVE_SEEDS) for path in (root / folder).glob("*.csv")})
    worker(tool_root, "restrict", str(full), str(shell), str(root / GOLD_CONTRACT), *seeds)
    return shell


def read_relations(database: Path, tool_root: Path = REPO_ROOT) -> dict[str, str]:
    """Every table and view of a database as ``schema.name`` in lower case, with the row count and a digest of its rows."""
    relations: dict[str, str] = json.loads(worker(tool_root, "relations", str(database)))
    return relations


class FeatureRun(NamedTuple):
    """A run of the feature models in two stages: ``stage_one`` is a copy of the restricted database that holds the raw bridge (the filter models bind
    there), ``database`` is the stage-2 database of every other model and ``relations`` holds the row count and digest of each relation it held before the
    run."""

    database: Path
    stage_one: Path
    project: Path
    profiles: Path
    work: Path
    output: str
    succeeded: bool
    relations: dict[str, str]


def run_features(root: Path, folder: Path, shell: Path, tool_root: Path = REPO_ROOT, contract: bool = False) -> FeatureRun:
    """Build the feature phase in two stages from ``shell`` (dbt contracts off unless ``contract``); a failed run is returned, not raised.

    Stage 1 is the worker ``stage_two``: it makes the new stage-2 database from ``shell`` without the raw bridge and builds the bridge filter tables in it with
    trusted SQL (no dbt run, so no hook or macro of the project touches them). Stage 2 runs every other ``tag:features`` model with dbt on that file, so a
    stage-2 read of the raw bridge fails because the table is absent.
    """
    require_tools(tool_root)
    project = folder / "dbt"
    copy_project(root, project, contract)
    # Both databases are named features.duckdb, so the compiled relation names of the one manifest bind in either stage.
    stage_one = folder / "stage_one" / "features.duckdb"
    stage_one.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(shell, stage_one)
    dbt = tool_root / ".tools/sqlfluff/bin/dbt"
    env = tool_environment(tool_root)
    database = folder / "features.duckdb"
    filters = sorted(path.stem for path in (root / FILTER_FOLDER).glob("*.sql") if FILTER_NAME.fullmatch(path.stem))
    worker(tool_root, "stage_two", str(shell), str(database), *filters)
    relations = read_relations(database, tool_root)
    profiles = write_profile(folder / "profiles", database, locked=True)
    filter_paths = [f"path:{FILTER_PATH}/{name}.sql" for name in filters]
    code, output = execute(
        dbt,
        ["run", "--select", "tag:features", "--exclude", "resource_type:seed", *filter_paths, *dbt_arguments(project, profiles, folder)],
        root,
        env,
        "dbt run tag:features",
    )
    return FeatureRun(database, stage_one, project, profiles, folder, output, code == 0, relations)


def build_database(root: Path, folder: Path, tool_root: Path = REPO_ROOT, contract: bool = False) -> Path:
    """Build the feature phase on empty gold tables in ``folder/features.duckdb`` (dbt contracts off unless ``contract``) and return the file."""
    run = run_features(root, folder, build_shell(root, folder, tool_root), tool_root, contract)
    if not run.succeeded:
        raise BuildError(f"dbt run tag:features failed: {run.output.strip()[-1200:]}")
    return run.database


def list_selection(project: Path, profiles: Path, work: Path, root: Path, tool_root: Path) -> dict[str, str]:
    """The nodes that ``dbt ls --select tag:features --resource-type all`` lists, by unique id with their resource type."""
    listing = step(
        tool_root / ".tools/sqlfluff/bin/dbt",
        [
            "ls",
            "--select",
            "tag:features",
            "--resource-type",
            "all",
            "--output",
            "json",
            "--output-keys",
            "unique_id",
            "resource_type",
            *dbt_arguments(project, profiles, work),
        ],
        root,
        tool_environment(tool_root),
        "dbt ls tag:features",
    )
    rows = [json.loads(line) for line in listing.splitlines() if line.startswith("{")]
    return {row["unique_id"]: row["resource_type"] for row in rows}


def admission(root: Path, folder: Path, tool_root: Path = REPO_ROOT) -> list[tuple[str, str]]:
    """The admission check: parse a scratch copy of the project without running or compiling anything and return the ``(location, message)`` pairs where it
    steps outside the closed lists of ``feature_allowlist``. No dbt build, compile or query runs before this returns, and a caller that gets a pair stops.
    """
    project, profiles = folder / "dbt", write_profile(folder / "profiles", folder / "unused.duckdb", locked=True)
    copy_project(root, project)
    manifest = parsed_manifest(project, profiles, folder, root, tool_root)
    path = folder / "allowlist_manifest.json"
    path.write_text(json.dumps(manifest))
    output = step(
        tool_root / ".tools/sqlfluff/bin/python",
        ["-m", "scripts.lakehouse.feature_allowlist", str(path), str(root)],
        REPO_ROOT,
        tool_environment(tool_root),
        "feature_allowlist",
    )
    pairs: list[tuple[str, str]] = [(where, message) for where, message in json.loads(output)]
    return pairs


def parsed_manifest(project: Path, profiles: Path, work: Path, root: Path, tool_root: Path = REPO_ROOT) -> dict[str, Any]:
    """Parse the project without running or compiling anything and return the manifest, with the selection of ``list_selection`` under ``feature_selection``.

    ``feature_sources`` checks this manifest against the allow lists before the project builds anything.
    """
    step(tool_root / ".tools/sqlfluff/bin/dbt", ["parse", *dbt_arguments(project, profiles, work)], root, tool_environment(tool_root), "dbt parse")
    manifest: dict[str, Any] = json.loads((work / "target" / "manifest.json").read_text())
    manifest["feature_selection"] = list_selection(project, profiles, work, root, tool_root)
    return manifest


def compile_manifest(run: FeatureRun, root: Path, tool_root: Path = REPO_ROOT) -> dict[str, Any]:
    """Compile the feature models and tests on the run's database and return the parsed manifest, compiled code included, with the selection stored."""
    step(
        tool_root / ".tools/sqlfluff/bin/dbt",
        ["compile", "--select", "tag:features", *dbt_arguments(run.project, run.profiles, run.work)],
        root,
        tool_environment(tool_root),
        "dbt compile tag:features",
    )
    manifest: dict[str, Any] = json.loads((run.work / "target" / "manifest.json").read_text())
    manifest["feature_selection"] = list_selection(run.project, run.profiles, run.work, root, tool_root)
    return manifest


def run_errors(output: str) -> dict[str, str]:
    """The failed nodes of a dbt run by name, with the first lines of dbt's message."""
    return {name: " ".join(message.split())[:400] for name, _, message in ERROR_IN_NODE.findall(output)}


def bind_nodes(run: FeatureRun, manifest: dict[str, Any], inputs: dict[str, list[str]], tool_root: Path = REPO_ROOT) -> dict[str, dict[str, Any]]:
    """Bind the compiled query of every selected node that has one, the filter models on the stage-1 database and the rest on the stage-2 database.

    A node that ``inputs`` lists (by unique id, with the relations it may read) is bound on a database that holds only those relations, so a read of any other
    relation fails to bind, whatever the spelling. Returns the error of each node by unique id.
    """
    in_scope_nodes = selected_nodes(manifest)
    bound: dict[str, dict[str, Any]] = {}
    for database, stage_one in ((run.stage_one, True), (run.database, False)):
        nodes = {unique_id: {**node, "inputs": inputs.get(unique_id)} for unique_id, node in in_scope_nodes.items() if is_filter(node) == stage_one}
        path = run.work / ("bind_one.json" if stage_one else "bind_two.json")
        path.write_text(json.dumps({"nodes": nodes}))
        bound.update(json.loads(worker(tool_root, "bind", str(database), str(path))))
    return bound


def read_columns(database: Path, models: list[str], tool_root: Path = REPO_ROOT) -> dict[str, list[list[str]]]:
    """The columns and types each model has in a database, read in the tool environment."""
    require_tools(tool_root)
    output = step(
        tool_root / ".tools/sqlfluff/bin/python",
        ["-m", "scripts.lakehouse.feature_columns", "--worker", "describe", str(database), *models],
        REPO_ROOT,
        dict(os.environ),
        "worker describe",
    )
    parsed: dict[str, list[list[str]]] = json.loads(output)
    return parsed


def check(root: Path = REPO_ROOT, database: Path | None = None) -> tuple[list[str], dict[str, int]]:
    """Problems between the feature models' built columns and the YAML, and the counts of what was compared."""
    models = sql_models(root)
    docs = documented(root / MODELS_YAML)
    try:
        if database is None:
            with tempfile.TemporaryDirectory(prefix="feature_columns_") as folder:
                rejected = admission(root, Path(folder) / "admission")
                if rejected:
                    return [f"{where}: {message}. Rule: {ISOLATION_RULE}." for where, message in rejected], {}
                built = read_columns(build_database(root, Path(folder)), models)
        else:
            built = read_columns(database, models)
    except BuildError as error:
        return [f"{MODELS_YAML}:1: the scratch build could not run, so the columns were not compared: {error}. Rule: {RULE}."], {}
    problems = compare(models, built, docs)
    return problems, {"models": len(models), "columns": sum(len(columns) for columns in built.values())}


def worker_shell(database: Path, contract: Path) -> None:
    """Create an empty table for each gold fact, dimension and bridge of the frozen gold contract."""
    duckdb: Any = importlib.import_module("duckdb")
    document = json.loads(contract.read_text())
    tables = {name: value["columns"] for name, value in document["facts"].items()}
    tables.update({name: value["columns"] for name, value in document["dimensions_and_bridges"].items() if "columns" in value})
    connection = duckdb.connect(str(database))
    for name, columns in tables.items():
        connection.execute(f'create table main."{name}" (' + ", ".join(f'"{column["name"]}" {column["type"]}' for column in columns) + ")")
    connection.close()


def worker_fill(database: Path) -> None:
    """Fill dim_measure with the distinct fact, measure key and value column of the bridge seeds, so the control bridge can join it."""
    duckdb: Any = importlib.import_module("duckdb")
    connection = duckdb.connect(str(database))
    seeds = [
        row[0]
        for row in connection.execute(
            "select table_name from information_schema.tables where table_name like '%\\_bridge' escape '\\' and table_name not like 'bridge\\_%' escape '\\'"
        ).fetchall()
    ]
    measures: set[tuple[str, str, str, str, str]] = set()
    for seed in seeds:
        measures.update(
            tuple(row)
            for row in connection.execute(
                "select fact, measure_key, value_column, vintage_start, vintage_end from query_table(?) where measure_key is not null and measure_key <> ''",
                [seed],
            ).fetchall()
        )
    # dim_measure is a gold dimension without a column list in the frozen contract; the scratch table holds the columns the feature models join.
    connection.execute(
        "create table main.dim_measure (measure_sk varchar, measure_identity varchar, fact varchar, measure_key varchar, value_column varchar, "
        "measure_label varchar, unit varchar, value_kind varchar, vintage_start varchar, vintage_end varchar, edition_start varchar, edition_end varchar, "
        "identity_rule varchar)"
    )
    connection.executemany(
        "insert into main.dim_measure (measure_sk, measure_identity, fact, measure_key, value_column, vintage_start, vintage_end, identity_rule) "
        "values (md5(? || '|' || ? || '|' || ?), ? || '|' || ? || '|' || ?, ?, ?, ?, ?, ?, 'single_latest')",
        [(fact, key, column, fact, key, column, fact, key, column, start, end) for fact, key, column, start, end in sorted(measures, key=repr)],
    )
    connection.close()


def worker_restrict(full: Path, shell: Path, contract: Path, seeds: list[str]) -> None:
    """Copy into a new file only the gold tables of the frozen contract and the feature seeds that ``full`` holds, and fail when anything else is there."""
    duckdb: Any = importlib.import_module("duckdb")
    document = json.loads(contract.read_text())
    allowed = {*document["facts"], *document["dimensions_and_bridges"], *seeds}
    connection = duckdb.connect(str(shell))
    connection.execute(f"attach '{full}' as source (read_only)")
    held = [
        row[0]
        for row in connection.execute("select table_name from information_schema.tables where table_catalog = 'source' and table_schema = 'main'").fetchall()
    ]
    for name in sorted(set(held) & allowed):
        connection.table(f'source.main."{name}"').create(name)
    connection.execute("detach source")
    copied = sorted(row[0] for row in connection.execute("select table_name from information_schema.tables where table_schema = 'main'").fetchall())
    stray = [name for name in copied if name not in allowed]
    schemas = [
        row[0]
        for row in connection.execute(
            "select schema_name from information_schema.schemata where schema_name not in ('main', 'information_schema', 'pg_catalog')"
        ).fetchall()
    ]
    missing_seeds = sorted(set(seeds) - set(copied))
    connection.close()
    if stray or schemas or missing_seeds:
        raise SystemExit(f"the restricted database holds {stray} outside the allowlist, the schemas {schemas} and lacks the seeds {missing_seeds}")


def worker_stage_two(shell: Path, stage_two: Path, filters: list[str]) -> None:
    """Make the stage-2 database: every table of ``shell`` except the raw control bridge, plus one filter table per name in ``filters``.

    Each filter table is ``create table <name> as select * from bridge_measure_control where source_seed = ?`` with the seed taken from the name
    (``feature_bridge_<seed>``). The statement is fixed here and runs in DuckDB, outside dbt, so nothing in the project (a hook, a macro, a ``ref`` override)
    can change what a filter table holds. The step fails when a name is not a filter model name, or when the raw bridge is in the result.
    """
    duckdb: Any = importlib.import_module("duckdb")
    bad = [name for name in filters if not FILTER_NAME.fullmatch(name)]
    if bad:
        raise SystemExit(f"{bad} are no bridge filter model names (feature_bridge_<retired seed>)")
    connection = duckdb.connect(str(stage_two))
    connection.execute(f"attach '{shell}' as shell (read_only)")
    tables = [
        row[0]
        for row in connection.execute("select table_name from information_schema.tables where table_catalog = 'shell' and table_schema = 'main'").fetchall()
    ]
    for name in sorted(set(tables) - {RAW_BRIDGE}):
        connection.table(f'shell.main."{name}"').create(name)
    for name in sorted(filters):
        connection.sql(FILTER_SQL, params=[name.removeprefix(FILTER_PREFIX)]).create(name)
    connection.execute("detach shell")
    held = connection.execute("select count(*) from information_schema.tables where lower(table_name) = ?", [RAW_BRIDGE]).fetchone()[0]
    connection.close()
    if held:
        raise SystemExit(f"the stage-2 database holds {RAW_BRIDGE}")


def worker_relations(database: Path) -> None:
    """Print every table and view of a database as a JSON object: ``schema.name`` in lower case, then the row count and an md5 of the rows.

    The digest makes the stage-2 comparison one of content, not of names: a hook, a macro or a model that rewrote a filter table or a gold table changes it.
    Each relation is read through a quoted name that tables and views share; one that cannot be read is printed as ``unreadable: <error>``, which the check
    reports as a problem.
    """
    duckdb: Any = importlib.import_module("duckdb")
    connection = duckdb.connect(str(database), read_only=True)
    found: dict[str, str] = {}
    for schema, name in connection.execute("select table_schema, table_name from information_schema.tables order by 1, 2").fetchall():
        relation = '"' + schema.replace('"', '""') + '"."' + name.replace('"', '""') + '"'
        try:
            count, digest = (
                connection.sql("from " + relation)
                .set_alias("t")
                .aggregate("count(*), coalesce(md5(string_agg(t::varchar, '|' order by t::varchar)), '')")
                .fetchone()
            )
            found[f"{schema}.{name}".lower()] = f"{count} rows, md5 {digest}"
        except duckdb.Error as error:
            found[f"{schema}.{name}".lower()] = f"{UNREADABLE}{' '.join(str(error).split())[:120]}"
    connection.close()
    sys.stdout.write(json.dumps(found))


def restricted_database(duckdb: Any, source: Path, relations: list[str], folder: Path) -> Path:
    """Make a database file ``features.duckdb`` in ``folder`` that holds the columns (no rows) of only the named ``main`` relations of ``source``.

    The file has the same name as the build's, so the compiled relation names resolve in it. It is made under another name while ``source`` is attached as
    ``features``, because a view of the build refers to the relations it reads by that catalog name.
    """
    staged = folder / "staged" / "restricted.duckdb"
    staged.parent.mkdir()
    connection = duckdb.connect(str(staged))
    connection.execute(f"attach '{source}' as features (read_only)")
    for relation in relations:
        schema, name = relation.split(".", 1)
        if schema != "main":
            raise SystemExit(f"{relation} is outside the main schema")
        connection.sql('from features.main."' + name.replace('"', '""') + '" limit 0').create(name)
    connection.execute("detach features")
    connection.close()
    target = folder / "features.duckdb"
    staged.replace(target)
    return target


def worker_bind(database: Path, manifest: Path) -> None:
    """Bind the compiled query of each node of a manifest file on a database and print its error as JSON.

    External access is off and locked, so a table function cannot read a file either. A relation that the database does not hold fails here, whatever the
    query text says; this is how a stage-2 read of the raw control bridge is caught. A node with an ``inputs`` list is bound on a database that holds only
    those relations (``restricted_database``), so it cannot read a filter table or any other relation that is not pinned for it.
    """
    duckdb: Any = importlib.import_module("duckdb")
    nodes = json.loads(manifest.read_text())["nodes"]
    shared = duckdb.connect(str(database), read_only=True, config={"enable_external_access": False})
    shared.execute("set lock_configuration = true")
    result: dict[str, dict[str, Any]] = {}
    for unique_id, node in sorted(nodes.items()):
        code = node.get("compiled_code")
        if not code:
            continue
        try:
            if node.get("inputs") is None:
                shared.execute("explain " + code)
            else:
                with tempfile.TemporaryDirectory(prefix="feature_bind_") as folder:
                    target = restricted_database(duckdb, database, node["inputs"], Path(folder))
                    connection = duckdb.connect(str(target), read_only=True, config={"enable_external_access": False})
                    connection.execute("set lock_configuration = true")
                    connection.execute("explain " + code)
                    connection.close()
            result[unique_id] = {"error": ""}
        except duckdb.Error as error:
            result[unique_id] = {"error": " ".join(str(error).split())[:500]}
    shared.close()
    sys.stdout.write(json.dumps(result))


def worker_describe(database: Path, models: list[str]) -> None:
    """Print the column names and types of each model as JSON."""
    duckdb: Any = importlib.import_module("duckdb")
    connection = duckdb.connect(str(database), read_only=True)
    built: dict[str, list[list[str]]] = {}
    for model in models:
        exists = connection.execute("select count(*) from information_schema.tables where table_name = ?", [model]).fetchone()[0]
        if exists:
            built[model] = [[row[0], row[1]] for row in connection.execute(f'describe main."{model}"').fetchall()]
    connection.close()
    sys.stdout.write(json.dumps(built))


def main() -> int:
    """Run the check, or one DuckDB step of it, and print each problem; fail when there is one."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--check", action="store_true")
    parser.add_argument("--root", type=Path, default=REPO_ROOT)
    parser.add_argument("--database", type=Path, help="a database built by this check's steps, read instead of building one")
    parser.add_argument(
        "--worker", choices=["shell", "fill", "restrict", "stage_two", "relations", "bind", "describe"], help="one DuckDB step (used by the check itself)"
    )
    parser.add_argument("arguments", nargs="*")
    args = parser.parse_args()
    if args.worker == "shell":
        worker_shell(Path(args.arguments[0]), Path(args.arguments[1]))
        return 0
    if args.worker == "fill":
        worker_fill(Path(args.arguments[0]))
        return 0
    if args.worker == "restrict":
        worker_restrict(Path(args.arguments[0]), Path(args.arguments[1]), Path(args.arguments[2]), args.arguments[3:])
        return 0
    if args.worker == "stage_two":
        worker_stage_two(Path(args.arguments[0]), Path(args.arguments[1]), args.arguments[2:])
        return 0
    if args.worker == "relations":
        worker_relations(Path(args.arguments[0]))
        return 0
    if args.worker == "bind":
        worker_bind(Path(args.arguments[0]), Path(args.arguments[1]))
        return 0
    if args.worker == "describe":
        worker_describe(Path(args.arguments[0]), args.arguments[1:])
        return 0
    if not args.check:
        parser.error("give --check")
    problems, counts = check(args.root, args.database)
    for problem in problems:
        sys.stderr.write(problem + "\n")
    sys.stdout.write(f"feature columns: {'fail' if problems else 'pass'}; {counts}\n")
    return int(bool(problems))


if __name__ == "__main__":
    raise SystemExit(main())
