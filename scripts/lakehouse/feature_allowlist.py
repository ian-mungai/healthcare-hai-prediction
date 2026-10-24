"""Allow lists of the feature guard, and the check of a parsed dbt project against them (docs/feature_phase_saved.md).

Run in the SQLFluff tool environment (it holds dbt and its Jinja) from the repository root, by ``feature_sources``:

    .tools/sqlfluff/bin/python -m scripts.lakehouse.feature_allowlist MANIFEST_FILE PROJECT_ROOT

``MANIFEST_FILE`` is the manifest of a scratch copy of the project with the selection of ``dbt ls --select tag:features`` stored under ``feature_selection``
(``feature_columns.parsed_manifest``). The output is a JSON list of ``[location, message]`` pairs, empty when everything is inside the lists.

The earlier rounds of the review kept finding SQL that runs outside the compiled query of a node (``run_query``, ``statement``, ``adapter.execute``, a
``sql_header``, a run hook whose body changed). A list of what is forbidden cannot be complete, so everything the feature build can execute must be in a
closed list here, and anything else fails with its node, file and line. The scope is every selected node, every seed and the gold models that the shell
build runs (``feature_columns.SHELL_MODELS``).

1. **Jinja.** The code of each node in scope and of each project macro it reaches (transitively, from ``depends_on.macros`` in the manifest) is parsed with
   dbt's Jinja and walked. Only the constructs in ``NODE_TYPES`` are allowed: no ``do`` statement, ``set`` block, call block, dict, import, include or
   arithmetic. Calls are allowed only to ``ref``, ``config`` (keyword arguments from ``CONFIG_CALL_KEYS`` only), ``return`` and the macros the code depends
   on. The only filter is ``join`` and the only attributes are ``loop.first`` and ``loop.last``. Every name the code reads without declaring it must be one
   of those functions, so ``execute``, ``adapter``, ``target``, ``var``, ``env_var`` and ``modules`` fail, and nothing environment-dependent can feed a
   ``ref``: it resolves the same at parse and run time. A dbt or package macro may be reached only if its id is in ``PACKAGE_MACROS``. A project macro
   that has the name of a dbt context function fails: it would replace that function for every node.
2. **Config.** The resolved config of each node in scope (node, folder and project levels merged by dbt) may hold only the keys of ``INERT`` (with a value
   from its list) and ``FREE`` (with a value its rule accepts). A ``sql_header``, ``persist_docs``, ``grants`` or any other key fails. The SQL text that a
   column ``data_type`` or a seed ``column_types`` entry carries into a create-table statement must be a plain type text, and a constraint fails. A pre-hook
   or post-hook of a node in scope and a run hook that a package adds (an ``operation`` node) fail here too.
3. **Run hooks.** The project's two run hooks stay (``feature_sources.RUN_HOOKS``), and the SHA-256 of the code of each hook macro and of each project
   macro they reach is pinned in ``HOOK_MACRO_SHA256``. The scratch builds run with the hooks emptied.
4. **Inputs.** ``dbt/models/features/_feature_inputs.json`` is the one committed list of what each feature model and singular test may read: its
   ``ref()`` targets (gold facts, seeds, other feature models and the filter tables of its own sources). A target outside the list, a model or test without
   an entry and an entry without a node fail at admission. A generic test may read only the node it tests. After the build, ``bind_inputs`` gives the
   worker of ``feature_columns.bind_nodes`` the relations each node may read, and it binds the compiled query on a database that holds only those, so a
   direct read (a relation name, ``query_table``) of another source's filter table fails as well.
5. **YAML.** A Jinja string in a property file of the project (a generic test argument, a description) is rendered at parse time with the macros in
   reach, so it may call ``ref`` only.
"""

from __future__ import annotations

import hashlib
import importlib
import json
import re
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any

from scripts.lakehouse import feature_columns as shell

yaml: Any = importlib.import_module("yaml")

# The Jinja constructs of the feature models, tests and macros today; anything else fails. Template, Output, TemplateData and Const are text and values;
# Assign, For, If, Not, Compare, Operand, Concat, CondExpr, List, Tuple, Filter, Getattr, Name, Call and Keyword are what the macros use; Macro is a block.
NODE_TYPES = frozenset(
    {
        "Assign",
        "Call",
        "Compare",
        "Concat",
        "CondExpr",
        "Const",
        "Filter",
        "For",
        "Getattr",
        "If",
        "Keyword",
        "List",
        "Macro",
        "Name",
        "Not",
        "Operand",
        "Output",
        "Template",
        "TemplateData",
        "Tuple",
    }
)
# The functions any node or macro may call besides the macros it depends on, and the name dbt gives the arguments of a generic test.
CALLS = frozenset({"ref", "config", "return"})
GENERIC_TEST_ARGUMENTS = "_dbt_generic_test_kwargs"
FILTERS = frozenset({"join"})
LOOP_ATTRIBUTES = frozenset({"first", "last"})
# The dbt macros that the feature nodes reach today: the three generic tests the YAML files use (unique, not_null, accepted_values) with their default
# implementations, the subquery that applies a test's ``where`` (null by the config list below) and the store-failures switch (null as well).
PACKAGE_MACROS = frozenset(
    {
        "macro.dbt.default__get_where_subquery",
        "macro.dbt.default__test_accepted_values",
        "macro.dbt.default__test_not_null",
        "macro.dbt.default__test_unique",
        "macro.dbt.get_where_subquery",
        "macro.dbt.should_store_failures",
        "macro.dbt.test_accepted_values",
        "macro.dbt.test_not_null",
        "macro.dbt.test_unique",
    }
)
HOOK_MACROS = ("macro.hai.hai_drop_build_marker", "macro.hai.hai_finalize_build_marker")
# SHA-256 of the macro code (the manifest's macro_sql) of each hook macro and each project macro they reach, as of dbt/macros/build_marker_hooks.sql and
# dbt/macros/build_provenance.sql. A change needs a new review of the hooks: they act on the build marker of a real build and on nothing else.
HOOK_MACRO_SHA256 = {
    "macro.hai.hai_build_eligible": "9e9f9531bf349c0e3a7a5753719c6d19f1956981218d3e09eadbdd5808acb1cb",
    "macro.hai.hai_build_provenance": "2a07480d4f847d269c176e4fd9ac31dd6bb3473edf9d208e7eea7451475cb355",
    "macro.hai.hai_build_selection": "65cf815d81e767c674551776c5625a0dc266b7f2b6edf2bbc8e2d2de996ef428",
    "macro.hai.hai_drop_build_marker": "8f98a6465ec4d67cd3821dd92adf58b9c8e29565fdd293193729454576d8be9c",
    "macro.hai.hai_drop_marker_relation": "05b0a0ebeaae9c6cfc0b3de0005d18310a44a282e3318abacb6de1ed3a3a7a86",
    "macro.hai.hai_finalize_build_marker": "bdd25488b86490a9e1b91954c00ff77daab5b295c6017a15ae55d7c973df3a81",
    "macro.hai.hai_marker_applies": "370ba5f80765169624c1a5cb927c0307ab67aa2fb89eaf42298113a3d4eac296",
    "macro.hai.hai_marker_untouched_commands": "d4a6f0f91152f0b76f85ed1cdf76eee525edf97b077a1ec97032567f6fa31f97",
    "macro.hai.hai_provenance_required": "f3534acf46b502a04fb4838de9a66ca5e4fdba3f68010ff1064b06c771bfa246",
    "macro.hai.hai_sql_literal": "60e1488d97e38c4e0deca30e9049c2c391eb15a49e3c1ba03e253b384c395ba3",
    "macro.hai.hai_write_build_marker": "0b4a0544e1a17a3515180fdfefb98a08d92d8e4347a51be1fc7530320241d079",
}
IDENTIFIER = re.compile(r"[A-Za-z0-9_]+")
# A DuckDB type as text (varchar, decimal(18,3), varchar[], struct(a integer)); no quote, semicolon or dash, so it cannot end a statement or start a comment.
TYPE_TEXT = re.compile(r"[A-Za-z0-9_ (),\[\]]+")
INPUTS_FILE = "dbt/models/features/_feature_inputs.json"
MATERIALIZED = {"model": {"view", "table"}, "seed": {"seed"}, "test": {"test"}}
# The config keys that must keep the value dbt gives them (a list of the values that pass).
INERT: dict[str, list[Any]] = {
    "access": ["protected"],
    "batch_size": [None],
    "begin": [None],
    "concurrent_batches": [None],
    "database": [None],
    "delimiter": [","],
    "docs": [{"node_color": None, "show": True}],
    "enabled": [True],
    "error_if": ["!= 0"],
    "event_time": [None],
    "fail_calc": ["count(*)"],
    "freshness": [None],
    "full_refresh": [None],
    "grants": [{}],
    "group": [None],
    "incremental_strategy": [None],
    "limit": [None],
    "lookback": [1],
    "on_configuration_change": ["apply"],
    "on_schema_change": ["ignore"],
    "packages": [[]],
    "persist_docs": [{}],
    "post-hook": [[]],
    "pre-hook": [[]],
    "quote_columns": [None],
    "quoting": [{}],
    "schema": [None, "dbt_test__audit"],
    "severity": ["ERROR"],
    "static_analysis": [None],
    "store_failures": [None],
    "store_failures_as": [None],
    "unique_key": [None],
    "warn_if": ["!= 0"],
    "where": [None],
}
# The config keys whose value may differ, and what a value must be.
FREE: dict[str, Callable[[Any, str], bool]] = {
    "alias": lambda value, kind: value is None or (isinstance(value, str) and IDENTIFIER.fullmatch(value) is not None),
    "column_types": lambda value, kind: isinstance(value, dict) and all(TYPE_TEXT.fullmatch(str(kind_text)) for kind_text in value.values()),
    "contract": lambda value, kind: (
        isinstance(value, dict) and set(value) <= {"enforced", "alias_types"} and all(isinstance(item, bool) for item in value.values())
    ),
    "materialized": lambda value, kind: value in MATERIALIZED.get(kind, set()),
    "meta": lambda value, kind: isinstance(value, dict),
    "tags": lambda value, kind: isinstance(value, list) and all(isinstance(item, str) and IDENTIFIER.fullmatch(item) for item in value),
}
# What a ``config()`` call in a node may set: the keys whose value is free.
CONFIG_CALL_KEYS = frozenset(FREE)
# Files of the project that are not property files: they are checked on their own (``feature_sources.check_configuration``).
PROJECT_FILES = frozenset({"dbt_project.yml", "profiles.yml", "selectors.yml", "packages.yml", "package-lock.yml"})
SKIPPED_FOLDERS = frozenset({"target", "logs", "dbt_packages"})
JINJA_MARK = re.compile(r"\{\{|\{%|\{#")
Problem = tuple[str, str]


def dotted(node: Any) -> str:
    """The text of an attribute chain such as ``adapter.execute``."""
    nodes: Any = importlib.import_module("jinja2.nodes")
    if isinstance(node, nodes.Name):
        return str(node.name)
    if isinstance(node, nodes.Getattr):
        return f"{dotted(node.node)}.{node.attr}"
    return type(node).__name__


def call_problems(call: Any, names: frozenset[str]) -> list[tuple[int, str]]:
    """Problems with one call: a plain name from ``names``, and for ``config`` only key=value pairs from ``CONFIG_CALL_KEYS``."""
    nodes: Any = importlib.import_module("jinja2.nodes")
    callee = call.node
    if not isinstance(callee, nodes.Name):
        return [(call.lineno, f"calls {dotted(callee)}(), which is no plain function name")]
    if callee.name not in names:
        return [(call.lineno, f"calls {callee.name}(), which is neither an allowed function nor a macro it depends on")]
    if callee.name == "config" and (call.args or call.dyn_args or call.dyn_kwargs):
        return [(call.lineno, "calls config() with an argument that is not a key=value pair")]
    return [
        (call.lineno, f"calls config() with the key {item.key}, which the feature phase does not allow")
        for item in call.kwargs
        if callee.name == "config" and item.key not in CONFIG_CALL_KEYS
    ]


def template_problems(environment: Any, code: str, names: frozenset[str]) -> list[tuple[int, str]]:
    """The line and message of each construct of ``code`` that is outside the allow list; ``names`` are the functions it may call."""
    nodes: Any = importlib.import_module("jinja2.nodes")
    meta: Any = importlib.import_module("jinja2.meta")
    try:
        tree = environment.parse(code)
    except Exception as error:
        return [(int(getattr(error, "lineno", 1) or 1), f"cannot be parsed as Jinja ({error})")]
    found: list[tuple[int, str]] = []
    for node in tree.find_all(nodes.Node):
        kind = type(node).__name__
        if kind not in NODE_TYPES:
            found.append((node.lineno, f"uses the Jinja construct {kind}, which the feature phase does not allow"))
        elif isinstance(node, nodes.Call):
            found.extend(call_problems(node, names))
        elif isinstance(node, nodes.Filter) and node.name not in FILTERS:
            found.append((node.lineno, f"uses the filter {node.name}, which the feature phase does not allow"))
        elif isinstance(node, nodes.Getattr) and not (isinstance(node.node, nodes.Name) and node.node.name == "loop" and node.attr in LOOP_ATTRIBUTES):
            found.append((node.lineno, f"reads {dotted(node)}, which the feature phase does not allow"))
    found.extend((1, f"uses the name {name}, which the feature phase does not allow") for name in sorted(meta.find_undeclared_variables(tree) - names))
    return found


def location(root: Path, path: str, code: str, line: int) -> str:
    """``dbt/<file>:<line>`` of a line of ``code``, counted from where the code starts in its file (line 1 when the file does not hold it)."""
    text = (root / "dbt" / path).read_text() if (root / "dbt" / path).is_file() else ""
    start = text.find(code) if code else -1
    return f"dbt/{path}:{line + text[:start].count(chr(10)) if start >= 0 else 1}"


def reached_macros(manifest: dict[str, Any], start: list[str]) -> dict[str, Any]:
    """The macros that ``start`` reaches directly or through other macros, by unique id (an id the manifest lacks maps to ``None``)."""
    macros = manifest["macros"]
    found: dict[str, Any] = {}
    pending = list(start)
    while pending:
        unique_id = pending.pop()
        if unique_id not in found:
            found[unique_id] = macros.get(unique_id)
            pending.extend((macros.get(unique_id) or {}).get("depends_on", {}).get("macros", []))
    return found


def scope_nodes(manifest: dict[str, Any]) -> dict[str, Any]:
    """The nodes whose code and config the guard trusts: the selected feature nodes, every seed and the gold models that the shell build runs."""
    nodes: dict[str, Any] = manifest["nodes"]
    extra = {
        unique_id: node
        for unique_id, node in nodes.items()
        if node["resource_type"] == "seed" or (node["resource_type"] == "model" and node["name"] in shell.SHELL_MODELS)
    }
    return {**extra, **shell.selected_nodes(manifest)}


def macro_names(manifest: dict[str, Any], ids: list[str]) -> frozenset[str]:
    """The names a node or macro may call: ``CALLS``, the generic test arguments, and the macros in ``ids`` that are project macros or in ``PACKAGE_MACROS``."""
    project = manifest["metadata"]["project_name"]
    macros = manifest["macros"]
    allowed = {macros[item]["name"] for item in ids if item in macros and (macros[item]["package_name"] == project or item in PACKAGE_MACROS)}
    return frozenset(CALLS | {GENERIC_TEST_ARGUMENTS} | allowed)


def code_problems(manifest: dict[str, Any], root: Path, environment: Any) -> list[Problem]:
    """Jinja problems of each node in scope and of each project macro it reaches, and the macros it reaches that no list allows."""
    problems: list[Problem] = []
    scope = scope_nodes(manifest)
    for unique_id, node in sorted(scope.items()):
        if node.get("raw_code") and node["resource_type"] in {"model", "test"}:
            names = macro_names(manifest, node["depends_on"].get("macros", []))
            problems.extend(
                (location(root, node["original_file_path"], node["raw_code"], line), f"{unique_id} {message}")
                for line, message in template_problems(environment, node["raw_code"], names)
            )
    project = manifest["metadata"]["project_name"]
    for unique_id, macro in sorted(reached_macros(manifest, [item for node in scope.values() for item in node["depends_on"].get("macros", [])]).items()):
        if macro is None:
            problems.append(("dbt/dbt_project.yml:1", f"{unique_id} is reached by a node but the manifest does not hold it"))
        elif macro["package_name"] != project:
            if unique_id not in PACKAGE_MACROS:
                problems.append(
                    ("dbt/dbt_project.yml:1", f"{unique_id} is a dbt or package macro that the feature phase may not reach (feature_allowlist.PACKAGE_MACROS)")
                )
        else:
            names = macro_names(manifest, macro["depends_on"].get("macros", []))
            problems.extend(
                (location(root, macro["original_file_path"], macro["macro_sql"], line), f"{unique_id} {message}")
                for line, message in template_problems(environment, macro["macro_sql"], names)
            )
    return problems


def context_names() -> set[str]:
    """The names dbt gives every model and macro (``config``, ``var``, ``return``, ``log``, ...): a project macro with one of them replaces that function."""
    providers: Any = importlib.import_module("dbt.context.providers")
    return {name for context in (providers.ProviderContext, providers.ModelContext) for name in (*context._context_members_, *context._context_attrs_)} - {
        "ref",
        "source",
    }


def shadow_problems(manifest: dict[str, Any]) -> list[Problem]:
    """Problems with project macros that have the name of a dbt context function."""
    names = context_names()
    project = manifest["metadata"]["project_name"]
    return [
        (
            f"dbt/{macro['original_file_path']}:1",
            f"{unique_id} has the name of the dbt context function {macro['name']}(), so it replaces that function for every node",
        )
        for unique_id, macro in sorted(manifest["macros"].items())
        if macro["package_name"] == project and macro["name"] in names
    ]


def config_problems(manifest: dict[str, Any]) -> list[Problem]:
    """Problems with the resolved config of each node in scope, and with the type text and constraints of its columns."""
    problems: list[Problem] = []
    for unique_id, node in sorted(scope_nodes(manifest).items()):
        where = f"dbt/{node['original_file_path']}:1"
        for key, value in sorted((node.get("config") or {}).items()):
            allowed = FREE[key](value, node["resource_type"]) if key in FREE else key in INERT and value in INERT[key]
            if not allowed:
                problems.append((where, f"{unique_id} has the config key {key} with the value {str(value)[:80]!r}, which the feature phase does not allow"))
        if node.get("constraints"):
            problems.append((where, f"{unique_id} has a constraint, which carries SQL text into its create-table statement"))
        for name, column in sorted((node.get("columns") or {}).items()):
            if column.get("constraints") or not (column.get("data_type") is None or TYPE_TEXT.fullmatch(column["data_type"])):
                problems.append((where, f"{unique_id} column {name} has a constraint or a data_type {column.get('data_type')!r} that is no plain type text"))
    return problems


def hook_problems(manifest: dict[str, Any]) -> list[Problem]:
    """Problems with the pin of the run hooks' implementation: each project macro that the hook macros reach is compared with ``HOOK_MACRO_SHA256``."""
    project = manifest["metadata"]["project_name"]
    reached = {
        unique_id: macro for unique_id, macro in reached_macros(manifest, list(HOOK_MACROS)).items() if macro is None or macro["package_name"] == project
    }
    problems: list[Problem] = []
    for unique_id, macro in sorted(reached.items()):
        digest = hashlib.sha256((macro or {}).get("macro_sql", "").encode()).hexdigest()
        where = f"dbt/{macro['original_file_path']}:1" if macro else "dbt/dbt_project.yml:1"
        if macro is None:
            problems.append((where, f"{unique_id} is reached by a run hook but the manifest does not hold it"))
        elif unique_id not in HOOK_MACRO_SHA256:
            problems.append((where, f"{unique_id} is reached by a run hook but is not pinned in feature_allowlist.HOOK_MACRO_SHA256 (sha256 {digest})"))
        elif digest != HOOK_MACRO_SHA256[unique_id]:
            problems.append((where, f"{unique_id} is reached by a run hook; its code changed (sha256 {digest}, pinned {HOOK_MACRO_SHA256[unique_id]})"))
    problems.extend(
        ("dbt/dbt_project.yml:1", f"{unique_id} is pinned as a run hook macro but no run hook reaches it any more (feature_allowlist.HOOK_MACRO_SHA256)")
        for unique_id in sorted(set(HOOK_MACRO_SHA256) - set(reached))
    )
    return problems


def load_pins(root: Path) -> dict[str, dict[str, list[str]]]:
    """The committed inputs of the feature models and singular tests (``INPUTS_FILE``): ``models`` and ``tests``, each name with the nodes it may ``ref()``."""
    pins: dict[str, dict[str, list[str]]] = json.loads((root / INPUTS_FILE).read_text())
    return pins


def input_nodes(manifest: dict[str, Any]) -> dict[str, Any]:
    """The feature models and tests whose inputs are pinned: the selected ones, the bridge filter models excluded (their text is fixed by the template)."""
    return {
        unique_id: node
        for unique_id, node in shell.selected_nodes(manifest).items()
        if node["resource_type"] in {"model", "test"} and not shell.is_filter(node)
    }


def permitted_nodes(manifest: dict[str, Any], node: dict[str, Any], pins: dict[str, dict[str, list[str]]]) -> list[str] | None:
    """The unique ids that a node may read, or ``None`` when no entry pins it. A generic test may read the node it tests and nothing else."""
    if node["resource_type"] == "test" and node.get("test_metadata"):
        return [node["attached_node"]]
    names = pins.get("models" if node["resource_type"] == "model" else "tests", {}).get(node["name"])
    if names is None:
        return None
    return [unique_id for unique_id, other in manifest["nodes"].items() if other["name"] in names and other["resource_type"] in {"model", "seed"}]


def input_problems(manifest: dict[str, Any], pins: dict[str, dict[str, list[str]]]) -> list[Problem]:
    """Problems with what each feature model and singular test reads through ``ref()``: a target not in its list, no entry and an entry without a node."""
    problems: list[Problem] = []
    nodes = input_nodes(manifest)
    for unique_id, node in sorted(nodes.items()):
        where = f"dbt/{node['original_file_path']}:1"
        permitted = permitted_nodes(manifest, node, pins)
        if permitted is None:
            problems.append((where, f"{unique_id} has no entry in {INPUTS_FILE}, so what it may read is not pinned"))
            continue
        problems.extend(
            (where, f"{unique_id} reads {manifest['nodes'][target]['name']}, which {INPUTS_FILE} does not list for it")
            for target in node["depends_on"].get("nodes", [])
            if target not in permitted
        )
    for kind, resource in (("models", "model"), ("tests", "test")):
        known = {node["name"] for node in nodes.values() if node["resource_type"] == resource and not node.get("test_metadata")}
        problems.extend(
            (INPUTS_FILE + ":1", f"{INPUTS_FILE} pins {kind[:-1]} {name}, which is not a selected feature {kind[:-1]}")
            for name in sorted(set(pins.get(kind, {})) - known)
        )
    return problems


def bind_inputs(manifest: dict[str, Any], pins: dict[str, dict[str, list[str]]]) -> dict[str, list[str]]:
    """For each pinned node the relations (``schema.name``) it may read: its pinned inputs, which the compiled query is bound against and nothing else.

    A node without an entry is not listed: admission has already failed it, and the build layer binds it on the whole stage-2 database.
    """
    found: dict[str, list[str]] = {}
    for unique_id, node in input_nodes(manifest).items():
        permitted = permitted_nodes(manifest, node, pins)
        if permitted is not None:
            found[unique_id] = sorted({f"{manifest['nodes'][target]['schema']}.{manifest['nodes'][target]['alias']}" for target in permitted})
    return found


def operation_problems(manifest: dict[str, Any]) -> list[Problem]:
    """Problems with run hooks that a package adds: an ``operation`` node runs SQL that no check binds."""
    return [
        ("dbt/dbt_project.yml:1", f"{unique_id} is a run hook that runs during the feature build")
        for unique_id, node in sorted(manifest["nodes"].items())
        if node["resource_type"] == "operation"
    ]


def strings(value: Any) -> list[str]:
    """Every string in a YAML document."""
    if isinstance(value, str):
        return [value]
    if isinstance(value, dict):
        return [text for item in (*value.keys(), *value.values()) for text in strings(item)]
    if isinstance(value, list):
        return [text for item in value for text in strings(item)]
    return []


def yaml_problems(root: Path, environment: Any) -> list[Problem]:
    """Problems with Jinja strings in the property files of the project: dbt renders them at parse time, so they may call ``ref`` only."""
    problems: list[Problem] = []
    folder = root / "dbt"
    for path in sorted([*folder.rglob("*.yml"), *folder.rglob("*.yaml")]):
        relative = path.relative_to(folder)
        if relative.parts[0] in SKIPPED_FOLDERS or (len(relative.parts) == 1 and path.name in PROJECT_FILES):
            continue
        text = path.read_text()
        for item in strings(yaml.safe_load(text)):
            if JINJA_MARK.search(item):
                start = text.find(item.split("\n")[0].strip())
                line = text[:start].count("\n") + 1 if start >= 0 else 1
                problems.extend(
                    (f"dbt/{relative.as_posix()}:{line}", f"holds the Jinja string {item[:60]!r} that {message}")
                    for _, message in template_problems(environment, item, frozenset({"ref"}))
                )
    return problems


def check(manifest: dict[str, Any], root: Path) -> list[Problem]:
    """Every problem of the compiled project against the allow lists."""
    environment = importlib.import_module("dbt_common.clients.jinja").get_environment(None, capture_macros=False)
    return [
        *code_problems(manifest, root, environment),
        *shadow_problems(manifest),
        *config_problems(manifest),
        *hook_problems(manifest),
        *operation_problems(manifest),
        *input_problems(manifest, load_pins(root)),
        *yaml_problems(root, environment),
    ]


def main() -> int:
    """Print the problems of a manifest file as JSON."""
    manifest = json.loads(Path(sys.argv[1]).read_text())
    sys.stdout.write(json.dumps(check(manifest, Path(sys.argv[2]))))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
