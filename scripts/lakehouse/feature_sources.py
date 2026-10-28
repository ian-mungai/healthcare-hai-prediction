"""Check of the feature phase: it reads gold, never silver, and the gold selections leave it out (docs/feature_phase_saved.md).

Run from the repository root:

    .venv/bin/python -m scripts.lakehouse.feature_sources --check [--root DIR]

The models, macros and tests under ``dbt/models/features``, ``dbt/macros/features`` and ``dbt/tests/features`` may ``ref()`` only a gold model, a live
seed or a feature model. Three layers enforce this. A fourth rule keeps the shared control bridge ``bridge_measure_control`` (the rows of all 15 retired
seeds) out of the feature models except through a filter by ``source_seed``; it is built into the resolved pass, not read from SQL text.

1. The text pass is the fast first line; it fails on a ``ref()`` of a ``stg_`` or ``int_`` silver model that is not a feature model, a ``ref()`` of a
   retired registry-map seed (one per bridge seed in ``dbt/seeds/gold``), any ``source()`` call, a ``ref()`` or ``source()`` whose argument is not a quoted
   literal (a Jinja variable, an expression: the text pass cannot resolve it) and a silver model or retired seed named as a relation in SQL text. It also
   pins the bridge filter models: ``dbt/models/features/bridge`` holds one file ``feature_bridge_<retired seed>.sql`` per retired seed, each exactly
   ``BRIDGE_FILTER`` with its seed, and each is read by some other feature model or test.
2. The resolved pass does not read text. It has two layers, and the second starts only when the first finds nothing.
   Admission (``feature_columns.admission``, ``feature_allowlist``) parses a scratch copy of the project and runs nothing. It checks closed lists: Jinja
   constructs and calls, dbt macros, config keys and values (hooks included), Jinja strings in YAML, the SHA-256 of the two run-hook macros and of the project
   macros they reach, and the ``ref()`` targets that ``dbt/models/features/_feature_inputs.json`` pins for each model and test. SQL that runs outside the
   compiled query of a node (``run_query``, ``statement``, ``adapter.execute``, ``sql_header``) and a read of another source's filter table fail here by
   content, and the check returns before any dbt build, compile or query.
   The build (``built_check``) builds the feature phase in two stages (``feature_columns.run_features``). Stage 1 is no dbt run: the check
   builds the 15 filter tables itself in DuckDB from a database that holds the gold-contract tables, the raw bridge included, so no hook or macro changes them.
   Stage 2 runs every other feature model with dbt on a database that holds the published gold tables of the frozen contract without the raw bridge, the seeds
   of ``dbt/seeds/features``, the live reference seeds directly under ``dbt/seeds`` and the filter tables, with external access off, then compiles the
   models and tests and binds the compiled query of each selected node (the generic tests of seeds included) on a database that holds only the relations
   pinned for that node. A read of the raw bridge, silver, bronze, a retired seed, a bridge seed, another source's filter table or any other relation fails
   because the relation does not exist there, however the read is spelled (case, quotes, ``query_table``, a macro, an alias, an aggregate), and a table
   function that reads a file or another database fails because external access is off. The pass also reads the parsed manifest and fails on any resolved
   dependency that is a bronze source, a silver model, a retired seed or any other node outside gold, the live seeds and the feature phase, and on a feature
   model without an enforced column contract. It fails by name on a selected node it cannot bind (a snapshot, an analysis, a unit test, a source), a macro
   that overrides ``ref``, ``source``, a materialization or a dbt or dbt-duckdb macro, a filter model whose compiled query is not the template and a relation
   in the stage-2 database that the run or the compile added, rewrote (count and digest of the rows) or could not read. The configuration pass pins the two
   run hooks of the project (``RUN_HOOKS``; the scratch builds run with both lists emptied) and allows only the top-level keys of ``PROJECT_KEYS`` in
   ``dbt_project.yml``.
3. The configuration pass needs the folder tag ``features`` (models, tests and the recipe seeds), the default selector that leaves the tag out of a command
   with no selection (an ``exclude`` of the tag, not a mention of it) and the exclusion ``tag:features`` in the gold build selections (as code or an option
   pair, not in a comment).

End-to-end run with planted defects: ``.venv/bin/python -m scripts.lakehouse.run_feature_sources_e2e``.
"""

from __future__ import annotations

import argparse
import ast
import importlib
import json
import re
import sys
import tempfile
from pathlib import Path
from typing import Any

from scripts.lakehouse import feature_allowlist as allowlist
from scripts.lakehouse import feature_columns as shell

yaml: Any = importlib.import_module("yaml")

REPO_ROOT = Path(__file__).resolve().parents[2]
FOLDERS = {"model": "dbt/models/features", "macro": "dbt/macros/features", "test": "dbt/tests/features"}
RULE = shell.ISOLATION_RULE
TAG = "features"
EXCLUSION = "--exclude tag:features"
JINJA_BLOCK = re.compile(r"\{\{.*?\}\}|\{%.*?%\}", re.DOTALL)
JINJA_COMMENT = re.compile(r"\{#.*?#\}", re.DOTALL)
CALL = re.compile(r"\b(ref|source)\s*\(\s*((?:'[^']*'|\"[^\"]*\")(?:\s*,\s*(?:'[^']*'|\"[^\"]*\"))*)\s*\)")
FUNCTION = re.compile(r"\b(?:ref|source)\s*\(")
SQL_SKIPPED = re.compile(r"'(?:[^']|'')*'|--[^\n]*|/\*.*?\*/|\{#.*?#\}|\{\{.*?\}\}|\{%.*?%\}", re.DOTALL)
# The one text of a bridge filter model; <seed> is the retired seed named in the file name. Stage 1 of the feature build runs these files alone, and a
# table holds the rows that stage 2 copies.
BRIDGE_FILTER = "{{ config(materialized='table') }}\n\nselect *\nfrom {{ ref('bridge_measure_control') }}\nwhere source_seed = '<seed>'\n"
FILTER_PREFIX = shell.FILTER_PREFIX
# The only run hooks of the project: they act on the build marker of a real silver and gold build and on nothing else. The scratch builds of the feature
# checks run with both lists emptied (feature_columns.copy_project), so no hook acts on them; any other entry fails the check by name.
RUN_HOOKS = {"on-run-start": ["{{ hai_drop_build_marker() }}"], "on-run-end": ["{{ hai_finalize_build_marker(results) }}"]}
# The only top-level keys of dbt_project.yml (the rest is a closed list too: query-comment, dispatch, quoting, snapshots and the like can run macros or change
# what dbt runs).
PROJECT_KEYS = frozenset(
    {
        "name",
        "version",
        "config-version",
        "profile",
        "model-paths",
        "seed-paths",
        "test-paths",
        "macro-paths",
        "clean-targets",
        "packages-install-path",
        "flags",
        "on-run-start",
        "on-run-end",
        "vars",
        "models",
        "seeds",
        "data_tests",
    }
)
SUPPORTED_TYPES = {"model", "test", "seed"}
INTERNAL_PACKAGES = {"dbt", "dbt_duckdb"}
OVERRIDABLE_BUILTINS = {"ref", "source"}
BRIDGE_SUFFIX = "_bridge"
GOLD_BUILD_FILES = ("scripts/lakehouse/pipeline_build.py", "scripts/lakehouse/run_gold_e2e.py", "config/orchestration/broker_templates.json")


def names(folder: Path, suffix: str) -> set[str]:
    """The file stems below a folder."""
    return {path.stem for path in folder.rglob(f"*{suffix}")} if folder.is_dir() else set()


def calls(text: str) -> list[tuple[int, str, list[str]]]:
    """Every ``ref`` and ``source`` call inside a Jinja block, with its line and quoted arguments (Jinja comments blanked first)."""
    blanked = JINJA_COMMENT.sub(lambda match: "\n" * match.group(0).count("\n"), text)
    found = []
    for block in JINJA_BLOCK.finditer(blanked):
        for call in CALL.finditer(block.group(0)):
            arguments = [part.strip().strip("'\"") for part in call.group(2).split(",")]
            found.append((blanked[: block.start()].count("\n") + 1 + block.group(0)[: call.start()].count("\n"), call.group(1), arguments))
    return found


def retired_seeds(root: Path) -> set[str]:
    """The names of the retired registry-map seeds: each bridge seed in ``dbt/seeds/gold`` is named ``<retired seed>_bridge``."""
    return {stem.removesuffix(BRIDGE_SUFFIX) for stem in names(root / "dbt/seeds/gold", ".csv") if stem.endswith(BRIDGE_SUFFIX)}


def unsupported_calls(text: str) -> list[tuple[int, str]]:
    """The line and text of each ``ref`` or ``source`` call inside a Jinja block whose arguments are not all quoted literals.

    ``calls`` reads only literal arguments, so a call built from a Jinja variable, an expression or a keyword argument would pass unseen.
    """
    blanked = JINJA_COMMENT.sub(lambda match: "\n" * match.group(0).count("\n"), text)
    found = []
    for block in JINJA_BLOCK.finditer(blanked):
        body = block.group(0)
        literal = {call.start() for call in CALL.finditer(body)}
        for function in FUNCTION.finditer(body):
            if function.start() not in literal:
                line = blanked[: block.start()].count("\n") + 1 + body[: function.start()].count("\n")
                found.append((line, body[function.start() : function.start() + 60].split("\n")[0]))
    return found


def sql_only(text: str) -> str:
    """The SQL text outside Jinja blocks, SQL comments and single-quoted strings, with the line breaks kept.

    One left-to-right scan blanks them, so a quote inside a comment starts no string.
    """
    return SQL_SKIPPED.sub(lambda match: re.sub(r"[^\n]", " ", match.group(0)), text)


def named_relations(text: str, names: set[str]) -> list[tuple[int, str]]:
    """The line and name of each silver model or retired seed that SQL text names as a word, whether or not it is qualified or double-quoted."""
    if not names:
        return []
    pattern = re.compile(r"(?<![A-Za-z0-9_])(" + "|".join(re.escape(name) for name in sorted(names, key=len, reverse=True)) + r")(?![A-Za-z0-9_])")
    return [(text[: match.start()].count("\n") + 1, match.group(1)) for match in pattern.finditer(text)]


def bridge_filter_text(seed: str) -> str:
    """The exact text of the bridge filter model of a retired seed."""
    return BRIDGE_FILTER.replace("<seed>", seed)


def filter_problems(root: Path, retired: set[str], read: set[str]) -> list[str]:
    """Problems with the bridge filter models: the folder holds one file per retired seed, each is the template with its seed and each has a reader.

    ``read`` holds the names that the feature files outside the folder pass to ``ref()``. The file name carries the seed, so the template needs no list.
    """
    problems: list[str] = []
    folder = root / shell.FILTER_FOLDER
    found: set[str] = set()
    for path in sorted(folder.rglob("*")) if folder.is_dir() else []:
        where = f"{shell.FILTER_FOLDER}/{path.relative_to(folder).as_posix()}:1"
        seed = path.stem.removeprefix(FILTER_PREFIX)
        if path.is_dir():
            continue
        if path.parent != folder or path.suffix != ".sql" or not path.stem.startswith(FILTER_PREFIX):
            problems.append(f"{where}: {path.name} is no bridge filter model: the folder holds only {FILTER_PREFIX}<retired seed>.sql files. Rule: {RULE}.")
        elif seed not in retired:
            problems.append(f"{where}: {path.name} names no retired seed (the bridge seeds in dbt/seeds/gold). Rule: {RULE}.")
        elif path.read_text() != bridge_filter_text(seed):
            problems.append(
                f"{where}: {path.name} does not equal the bridge filter template for source_seed '{seed}'; the file must hold exactly "
                f"{bridge_filter_text(seed)!r} (feature_sources.BRIDGE_FILTER). Rule: {RULE}."
            )
        else:
            found.add(seed)
            if path.stem not in read:
                problems.append(
                    f"{where}: {path.stem} is read by no feature model or test. Rule: {RULE}. Read it where the seed's controls are used, or remove it."
                )
    for seed in sorted(retired - found):
        problems.append(f"{shell.FILTER_FOLDER}:1: retired seed {seed} has no filter model {FILTER_PREFIX}{seed}.sql. Rule: {RULE}. Add the template file.")
    return problems


def check_references(root: Path) -> tuple[list[str], dict[str, int]]:
    """Problems with the references of the feature files, and the counts of what was read."""
    problems: list[str] = []
    files = {kind: sorted((root / folder).rglob("*.sql")) if (root / folder).is_dir() else [] for kind, folder in FOLDERS.items()}
    if not files["model"]:
        problems.append(f"{FOLDERS['model']}:1: no feature model found, so nothing was checked. Rule: {RULE}. Restore the folder or ask the owner.")
    features = {path.stem for path in files["model"]}
    retired = retired_seeds(root)
    if not retired:
        problems.append(f"dbt/seeds/gold:1: no bridge seed found, so no retired seed name is known. Rule: {RULE}. Restore the folder or ask the owner.")
    gold = names(root / "dbt/models/gold", ".sql")
    silver = names(root / "dbt/models/silver", ".sql")
    seeds = names(root / "dbt/seeds", ".csv")
    read: set[str] = set()
    for paths in files.values():
        for path in paths:
            text = path.read_text()
            where = path.relative_to(root).as_posix()
            for line, call in unsupported_calls(text):
                problems.append(
                    f"{where}:{line}: {call.strip()!r} is a ref() or source() call whose argument is not a quoted literal, so the check cannot see "
                    f"what it reads. Rule: {RULE}. Write the model name as a quoted literal."
                )
            for line, name in named_relations(sql_only(text), silver | retired):
                problems.append(
                    f"{where}:{line}: {name} is a silver model or retired seed named in SQL text, outside ref(), so dbt does not see the dependency. "
                    f"Rule: {RULE}. Read its gold model with ref()."
                )
            for line, function, arguments in calls(text):
                target = arguments[-1]
                if function == "ref" and not path.is_relative_to(root / shell.FILTER_FOLDER):
                    read.add(target)
                if function == "source":
                    problems.append(
                        f"{where}:{line}: source('{arguments[0]}', '{target}') reads bronze; the feature phase reads gold. Rule: {RULE}. Read a gold model."
                    )
                elif target in retired:
                    problems.append(
                        f"{where}:{line}: ref('{target}') is a retired registry-map seed. Rule: {RULE}. "
                        f"Read bridge_measure_control filtered by source_seed = '{target}' and the formula recipe in dbt/seeds/features."
                    )
                elif target in features or target in gold or target in seeds:
                    continue
                elif target in silver or target.startswith(("stg_", "int_")):
                    problems.append(f"{where}:{line}: ref('{target}') is a silver model; read its gold fact or bridge. Rule: {RULE}.")
                else:
                    problems.append(f"{where}:{line}: ref('{target}') names no gold model, live seed or feature model. Rule: {RULE}.")
    problems.extend(filter_problems(root, retired, read))
    return problems, {"models": len(files["model"]), "macros": len(files["macro"]), "tests": len(files["test"]), "retired_seeds": len(retired)}


def tag_list(node: Any, key: str) -> list[Any]:
    """The tags of a dbt_project.yml node."""
    value = node.get(key) if isinstance(node, dict) else None
    return value if isinstance(value, list) else []


def holds(definition: Any, wanted: Any) -> bool:
    """Whether a YAML structure is ``wanted`` or holds it as an element, at any depth."""
    if definition == wanted:
        return True
    children = definition.values() if isinstance(definition, dict) else definition if isinstance(definition, list) else []
    return any(holds(child, wanted) for child in children)


def excluded_in_code(text: str) -> bool:
    """Whether Python source excludes the feature tag in code: a list ``["--exclude", "tag:features"]`` or a string that holds ``--exclude tag:features``.

    A comment or a docstring that only says it does not count, and a source that does not parse does not either.
    """
    try:
        tree = ast.parse(text)
    except SyntaxError:
        return False
    docstrings = {id(node.value) for node in ast.walk(tree) if isinstance(node, ast.Expr)}
    for node in ast.walk(tree):
        if isinstance(node, ast.List) and [getattr(item, "value", None) for item in node.elts] == ["--exclude", f"tag:{TAG}"]:
            return True
        if isinstance(node, ast.Constant) and isinstance(node.value, str) and id(node) not in docstrings and EXCLUSION in node.value:
            return True
    return False


def check_configuration(root: Path) -> list[str]:
    """Problems with the folder tags (models, tests and recipe seeds), the default selector and the gold build selections."""
    problems: list[str] = []
    project = yaml.safe_load((root / "dbt/dbt_project.yml").read_text()) or {}
    models = ((project.get("models") or {}).get("hai") or {}).get("features") or {}
    tests = ((project.get("data_tests") or {}).get("hai") or {}).get("features") or {}
    if TAG not in tag_list(models, "+tags"):
        problems.append(
            f"dbt/dbt_project.yml:1: models.hai.features has no +tags with '{TAG}', so the gold selections cannot leave the models out. Rule: {RULE}."
        )
    if models.get("+materialized") != "view":
        problems.append(f"dbt/dbt_project.yml:1: models.hai.features is not +materialized: view; nothing is stored beside gold. Rule: {RULE}.")
    if TAG not in tag_list(tests, "+tags"):
        problems.append(
            f"dbt/dbt_project.yml:1: data_tests.hai.features has no +tags with '{TAG}', so the folder's singular tests run in a gold build. Rule: {RULE}."
        )
    for key, pinned in RUN_HOOKS.items():
        if project.get(key) != pinned:
            problems.append(
                f"dbt/dbt_project.yml:1: {key} is {project.get(key)!r}, not the pinned {pinned!r}; a run hook acts on every build, the feature build included. "
                f"Rule: {RULE}. Remove the extra hook or ask the owner to change feature_sources.RUN_HOOKS."
            )
    problems.extend(
        f"dbt/dbt_project.yml:1: the key {key} is outside the keys of the feature build ({', '.join(sorted(PROJECT_KEYS))}); for example dispatch "
        f"changes which macro adapter.dispatch() calls and query-comment runs a macro before every query. Rule: {RULE}. Remove it or ask the owner "
        f"to change feature_sources.PROJECT_KEYS."
        for key in sorted(set(project) - PROJECT_KEYS)
    )
    seeds = ((project.get("seeds") or {}).get("hai") or {}).get("features") or {}
    if TAG not in tag_list(seeds, "+tags"):
        problems.append(f"dbt/dbt_project.yml:1: seeds.hai.features has no +tags with '{TAG}', so the gold builds load the formula recipes. Rule: {RULE}.")
    selectors = root / "dbt/selectors.yml"
    default = (
        [item for item in (yaml.safe_load(selectors.read_text()) or {}).get("selectors", []) if item.get("default") is True] if selectors.is_file() else []
    )
    if not default or not holds(default[0].get("definition"), {"exclude": [{"method": "tag", "value": TAG}]}):
        problems.append(
            f"dbt/selectors.yml:1: no default selector leaves tag '{TAG}' out, so a command with no selection builds the feature phase. Rule: {RULE}."
        )
    for name in GOLD_BUILD_FILES:
        text = (root / name).read_text() if (root / name).is_file() else ""
        if name.endswith(".json"):
            modes = json.loads(text)["templates"]["gold_build"]["modes"] if text else {}
            selected = [row["launcher"]["args"] for row in modes.values() if row.get("launcher", {}).get("args")]
            ok = bool(selected) and all(any(args[i : i + 2] == ["--exclude", f"tag:{TAG}"] for i in range(len(args))) for args in selected)
        else:
            ok = excluded_in_code(text)
        if not ok:
            problems.append(f"{name}:1: the gold build selection does not exclude tag:{TAG}. Rule: {RULE}. Add {EXCLUSION} to it.")
    return problems


def reachable_features(unique_id: str, nodes: dict[str, Any], selected: dict[str, Any]) -> set[str]:
    """The selected feature nodes a node reads, directly or through other selected nodes."""
    found: set[str] = set()
    pending = [unique_id]
    while pending:
        for dependency in nodes.get(pending.pop(), {}).get("depends_on", {}).get("nodes", []):
            if dependency not in found and dependency in selected:
                found.add(dependency)
                pending.append(dependency)
    return found


def dispatched(name: str) -> str:
    """The name a macro has without an adapter prefix (``duckdb__split_part`` is ``split_part``), the name ``adapter.dispatch`` looks up."""
    return re.sub(r"^[a-z0-9]+__", "", name)


def macro_problems(manifest: dict[str, Any]) -> list[str]:
    """Problems with macros that replace what dbt itself runs: an allowlist of none.

    Any macro outside dbt and dbt-duckdb that is named ``ref`` or ``source`` or defines a materialization fails, and so does a macro of the project that has the
    name of a dbt or dbt-duckdb macro (also without its adapter prefix): a ``generate_*_name``, an adapter implementation, a test or a dispatched helper.
    """
    problems: list[str] = []
    macros: dict[str, Any] = manifest.get("macros", {})
    project = manifest.get("metadata", {}).get("project_name", "")
    internal = {macro["name"] for macro in macros.values() if macro.get("package_name") in INTERNAL_PACKAGES}
    internal |= {dispatched(name) for name in internal}
    for unique_id, macro in sorted(macros.items()):
        name = macro["name"]
        if macro.get("package_name") in INTERNAL_PACKAGES:
            continue
        if name in OVERRIDABLE_BUILTINS:
            why = f"overrides the dbt built-in {name}()"
        elif name.startswith("materialization_"):
            why = "defines a dbt materialization"
        elif macro.get("package_name") == project and (name in internal or dispatched(name) in internal):
            why = "has the name of a dbt or dbt-duckdb macro, so it overrides that macro"
        else:
            continue
        problems.append(
            f"dbt/{macro.get('original_file_path')}:1: {unique_id} {why}, so it changes what the feature build runs. Rule: {RULE}. Rename or remove it."
        )
    return problems


def filter_sql_problems(manifest: dict[str, Any], selected: dict[str, Any]) -> list[str]:
    """Problems with the compiled query of each bridge filter model: it must be exactly ``select *`` from the raw bridge where ``source_seed`` is its seed.

    The filter tables themselves are built by ``feature_columns.worker_stage_two`` from the file name; this keeps the dbt model, which the real build runs,
    equal to them after dbt resolved every macro, ``ref`` and configuration.
    """
    problems: list[str] = []
    bridge: dict[str, Any] = next(
        (node for node in manifest.get("nodes", {}).values() if node.get("resource_type") == "model" and node.get("name") == shell.RAW_BRIDGE), {}
    )
    for unique_id, node in sorted(selected.items()):
        path = node.get("original_file_path", "")
        if not shell.is_filter(node) or not node.get("compiled_code"):
            continue
        expected = f"select *\nfrom {bridge.get('relation_name')}\nwhere source_seed = '{node['name'].removeprefix(FILTER_PREFIX)}'"
        if node["compiled_code"].strip() != expected:
            problems.append(
                f"dbt/{path}:1: the compiled query of {unique_id} is {node['compiled_code'].strip()!r}, not {expected!r}; the filter tables "
                f"of the check are built from that query only. Rule: {RULE}."
            )
    return problems


def relation_problems(selected: dict[str, Any], before: dict[str, str], after: dict[str, str], failed: dict[str, str]) -> list[str]:
    """Problems with the relations of the stage-2 database after the run and the compile: they must be those that the database held before the run, with the
    same rows, plus the models that the run built (a hook, a macro or a model could add a relation or rewrite a filter table or a gold table).

    ``before`` and ``after`` map each relation to its row count and digest (``feature_columns.read_relations``), so a rewritten input fails with its name kept.
    """
    built = {
        f"{node['schema']}.{node['alias']}".lower()
        for node in selected.values()
        if node.get("resource_type") == "model"
        and not shell.is_filter(node)
        and (node.get("config") or {}).get("materialized") != "ephemeral"
        and node["name"] not in failed
    }
    expected = set(before) | built
    problems = [
        f"dbt/dbt_project.yml:1: the stage-2 database holds the relation {name}, which neither the restricted database "
        f"nor a feature model gives it. Rule: {RULE}."
        for name in sorted(set(after) - expected)
    ]
    problems.extend(
        f"dbt/dbt_project.yml:1: the stage-2 database holds the relation {name} with other rows after the run than before "
        f"({before[name]}, then {after[name]}): a hook, a macro or a model rewrote an input. Rule: {RULE}."
        for name in sorted(before)
        if name in after and after[name] != before[name]
    )
    problems.extend(
        f"dbt/dbt_project.yml:1: the stage-2 database holds the relation {name}, which cannot be read to compare its rows ({digest}). Rule: {RULE}."
        for name, digest in sorted([*before.items(), *after.items()])
        if digest.startswith(shell.UNREADABLE)
    )
    if not failed:
        problems.extend(
            f"dbt/dbt_project.yml:1: the stage-2 database lacks the relation {name}, which the run should have built or kept. Rule: {RULE}."
            for name in sorted(expected - set(after))
        )
    return problems


def manifest_problems(
    manifest: dict[str, Any],
    retired: set[str],
    failed: dict[str, str],
    bound: dict[str, dict[str, Any]],
    before: dict[str, str] | None = None,
    after: dict[str, str] | None = None,
) -> tuple[list[str], dict[str, int]]:
    """Problems in the resolved dependencies, the build and the binding of every selected feature node, and the counts of what was read.

    ``failed`` holds the dbt error of each model that did not build, by model name; ``bound`` holds the binding result of each node, by unique id; ``before``
    and ``after`` list the relations of the stage-2 database before the run and after the run and the compile (``after=None`` skips that comparison).
    Beyond the dependencies it fails on a node type that no step binds (a snapshot, an analysis, a unit test, a source), a macro that overrides dbt, a
    bridge filter model whose compiled query is not the template and a relation that the run left in the database.
    """
    problems: list[str] = []
    nodes: dict[str, Any] = manifest.get("nodes", {})
    in_scope = shell.selected_nodes(manifest)
    kinds = {**{unique_id: kind for unique_id, kind in manifest.get("feature_selection", {}).items() if unique_id not in nodes}}
    kinds.update({unique_id: node["resource_type"] for unique_id, node in in_scope.items()})
    kinds.update(
        {unique_id: "unit_test" for unique_id, unit in manifest.get("unit_tests", {}).items() if unit.get("original_file_path", "").startswith(shell.SCOPE)}
    )
    for unique_id, kind in sorted(kinds.items()):
        if kind not in SUPPORTED_TYPES:
            where = f"dbt/{nodes[unique_id]['original_file_path']}" if unique_id in nodes else "dbt/dbt_project.yml"
            problems.append(
                f"{where}:1: {unique_id} has the resource type '{kind}' in the feature selection, which no step of the check "
                f"can bind to the restricted database. "
                f"Rule: {RULE}. Remove it or ask the owner."
            )
    problems.extend(macro_problems(manifest))
    problems.extend(filter_sql_problems(manifest, in_scope))
    edges = 0
    for unique_id, node in sorted(in_scope.items()):
        where = f"dbt/{node['original_file_path']}"
        for dependency in node.get("depends_on", {}).get("nodes", []):
            edges += 1
            target = nodes.get(dependency)
            if dependency.startswith("source."):
                problems.append(f"{where}:1: {unique_id} depends on {dependency}, a bronze source; the feature phase reads gold. Rule: {RULE}.")
            elif target is None:
                problems.append(f"{where}:1: {unique_id} depends on {dependency}, which the manifest does not hold. Rule: {RULE}.")
            else:
                path = target.get("original_file_path", "")
                if target.get("resource_type") == "seed":
                    if target["name"] in retired:
                        problems.append(f"{where}:1: {unique_id} depends on {dependency}, a retired registry-map seed. Rule: {RULE}.")
                elif not (target.get("resource_type") == "model" and path.startswith(("models/gold/", "models/features/"))):
                    kind = "a silver model" if path.startswith("models/silver/") else "outside gold, the live seeds and the feature phase"
                    problems.append(f"{where}:1: {unique_id} depends on {dependency}, {kind} ({path}). Rule: {RULE}.")
        if node["name"] in failed:
            problems.append(
                f"{where}:1: {unique_id} did not build on a database that holds only what the feature phase may read: {failed[node['name']]}. "
                f"{shell.SHELL_HINT}. Rule: {RULE}."
            )
        if node["resource_type"] not in {"model", "test"}:
            continue
        result = bound.get(unique_id)
        if result is None:
            problems.append(f"{where}:1: {unique_id} has no binding result (no compiled code in the manifest, or the binding step skipped it). Rule: {RULE}.")
            continue
        upstream_failed = any(nodes[name]["name"] in failed for name in reachable_features(unique_id, nodes, in_scope))
        if result["error"] and node["name"] not in failed and not upstream_failed:
            problems.append(
                f"{where}:1: the compiled query of {unique_id} does not bind on a database that holds only what the feature phase may read: "
                f"{result['error']}. {shell.SHELL_HINT}. Rule: {RULE}."
            )
        is_model = node["resource_type"] == "model" and node.get("original_file_path", "").startswith("models/features/")
        if is_model and not (node.get("config", {}).get("contract") or {}).get("enforced"):
            problems.append(f"{where}:1: {unique_id} has no enforced column contract, so a column change does not fail the build. Rule: {RULE}.")
    if after is not None:
        problems.extend(relation_problems(in_scope, before or {}, after, failed))
    counts = {"nodes": len(in_scope), "dependencies": edges, "bound": sum(1 for result in bound.values() if not result["error"])}
    return problems, counts


def admission_problems(root: Path, folder: Path) -> list[str]:
    """Problems of the admission check (``feature_columns.admission``, ``feature_allowlist``), worded for the report.

    Everything the feature build can execute (Jinja, macros, config, hooks, YAML strings, the run-hook macros) must be in a closed list, and every model and
    test may ``ref()`` only the nodes pinned for it in ``dbt/models/features/_feature_inputs.json``. SQL that runs outside the compiled query of a node
    (``run_query``, ``statement``, ``adapter.execute``, a ``sql_header``) fails by content. ``resolved_check`` stops at any problem, before anything builds.
    """
    return [f"{where}: {message}. Rule: {RULE}." for where, message in shell.admission(root, folder)]


def resolved_check(root: Path, shell_database: Path | None = None) -> tuple[list[str], dict[str, int]]:
    """Admit the project, and only when admission finds nothing build and bind the feature phase (``built_check``).

    Code that fails admission never runs: no dbt run, compile, seed or query starts, so a forbidden read or write cannot happen.
    """
    try:
        with tempfile.TemporaryDirectory(prefix="feature_admission_") as name:
            rejected = admission_problems(root, Path(name))
    except shell.BuildError as error:
        return [f"dbt/dbt_project.yml:1: the admission check could not run: {error}. Rule: {RULE}."], {}
    if rejected:
        return rejected, {}
    return built_check(root, shell_database)


def built_check(root: Path, shell_database: Path | None = None) -> tuple[list[str], dict[str, int]]:
    """Build and bind the feature phase on the restricted database (``shell_database`` is a file ``feature_columns.build_shell`` made, reused when given).

    This is the second layer, after admission: it catches what the lists let through (a wrong read, a relation the run added or rewrote). The end-to-end
    cases call it directly to prove that the second layer works on its own.
    """
    try:
        with tempfile.TemporaryDirectory(prefix="feature_resolved_") as name:
            folder = Path(name)
            database = shell_database or shell.build_shell(root, folder / "build")
            run = shell.run_features(root, folder / "run", database, contract=True)
            failed = shell.run_errors(run.output)
            if not run.succeeded and not failed:
                raise shell.BuildError(f"dbt run tag:features failed: {run.output.strip()[-800:]}")
            manifest = shell.compile_manifest(run, root)
            inputs = allowlist.bind_inputs(manifest, allowlist.load_pins(root))
            bound = shell.bind_nodes(run, manifest, inputs)
            return manifest_problems(manifest, retired_seeds(root), failed, bound, run.relations, shell.read_relations(run.database))
    except shell.BuildError as error:
        return [f"dbt/dbt_project.yml:1: the resolved check could not run: {error}. Rule: {RULE}."], {}


def check(root: Path = REPO_ROOT) -> tuple[list[str], dict[str, int]]:
    """All problems of the feature phase under a repository root, and the counts."""
    problems, counts = check_references(root)
    return [*problems, *check_configuration(root)], counts


def main() -> int:
    """Run the check and print each problem; fail when there is one."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--check", action="store_true", required=True)
    parser.add_argument("--root", type=Path, default=REPO_ROOT)
    args = parser.parse_args()
    problems, counts = check(args.root)
    resolved, resolved_counts = resolved_check(args.root)
    problems.extend(resolved)
    counts = {**counts, **resolved_counts}
    for problem in problems:
        sys.stderr.write(problem + "\n")
    sys.stdout.write(f"feature sources: {'fail' if problems else 'pass'}; {counts}\n")
    return int(bool(problems))


if __name__ == "__main__":
    raise SystemExit(main())
