"""Source contracts of the modules that survive the SDK worker plane: no catch-all reaper, signals only in an owned stop, closed helper names."""

from __future__ import annotations

import ast
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SOURCE_PATHS = (
    "orchestration/worker_protocol.py",
    "orchestration/worker_stderr.py",
    "orchestration/worker_recovery.py",
    "scripts/process_group.py",
)
REAPERS = frozenset({"wait", "waitpid", "waitid", "wait3", "wait4"})
SIGNAL_FUNCTIONS = frozenset({"kill", "killpg", "signal_group", "signal_owned", "waitpid"})


def _call_name(call: ast.Call, aliases: dict[str, str]) -> str:
    if isinstance(call.func, ast.Name):
        return aliases.get(call.func.id, call.func.id)
    return call.func.attr if isinstance(call.func, ast.Attribute) else ""


def _function_name(node: ast.AST, parents: dict[ast.AST, ast.AST]) -> str | None:
    parent = parents.get(node)
    while parent is not None:
        if isinstance(parent, (ast.FunctionDef, ast.AsyncFunctionDef)):
            return parent.name
        parent = parents.get(parent)
    return None


def source_checks(root: Path = ROOT) -> dict[str, bool]:
    """Check source contracts across the worker modules that survive the SDK worker plane."""
    checks: dict[str, bool] = {}
    trees = {path: ast.parse((root / path).read_text()) for path in SOURCE_PATHS}
    for path, tree in trees.items():
        parents = {child: parent for parent in ast.walk(tree) for child in ast.iter_child_nodes(parent)}
        aliases = {alias.asname or alias.name: alias.name for node in ast.walk(tree) if isinstance(node, ast.ImportFrom) for alias in node.names}
        calls = [node for node in ast.walk(tree) if isinstance(node, ast.Call)]
        allowed_reaps = True
        for call in calls:
            if _call_name(call, aliases) not in REAPERS:
                continue
            name = ast.unparse(call.func)
            enclosing_function = _function_name(call, parents)
            owned_reap = (
                path == "scripts/process_group.py"
                and enclosing_function == "stop_owned"
                and name == "os.waitpid"
                and len(call.args) == 2
                and ast.unparse(call.args[0]) == "handle.pid"
                and ast.unparse(call.args[1]) == "os.WNOHANG"
            )
            allowed_reaps = allowed_reaps and owned_reap
        checks[f"{path}:no_catch_all_reaper"] = allowed_reaps
        checks[f"{path}:signal_imports_closed"] = not any(
            alias.name in SIGNAL_FUNCTIONS and not (path == "scripts/process_group.py" and _function_name(node, parents) == "stop_owned")
            for node in ast.walk(tree)
            if isinstance(node, ast.ImportFrom)
            for alias in node.names
        )
        signals_allowed = True
        for call in calls:
            name = _call_name(call, aliases)
            if name not in SIGNAL_FUNCTIONS - {"waitpid"}:
                continue
            signal_enclosing = _function_name(call, parents)
            owned_stop = path == "scripts/process_group.py" and signal_enclosing == "stop_owned"
            signals_allowed = signals_allowed and owned_stop
        checks[f"{path}:signals_only_in_owned_stop"] = signals_allowed
    tree = trees["scripts/process_group.py"]
    parents = {child: parent for parent in ast.walk(tree) for child in ast.iter_child_nodes(parent)}
    guarded = True
    aliases = {alias.asname or alias.name: alias.name for node in ast.walk(tree) if isinstance(node, ast.ImportFrom) for alias in node.names}
    for call in (node for node in ast.walk(tree) if isinstance(node, ast.Call) and _call_name(node, aliases) in SIGNAL_FUNCTIONS):
        chain = []
        ancestor = parents.get(call)
        while ancestor is not None:
            chain.append(ancestor)
            ancestor = parents.get(ancestor)
        guarded = guarded and any(isinstance(node, ast.With) and any(ast.unparse(item.context_expr) == "handle.lock" for item in node.items) for node in chain)
        guarded = guarded and next((node.name for node in chain if isinstance(node, ast.FunctionDef)), None) == "stop_owned"
    checks["scripts/process_group.py:signals_and_reaps_hold_handle_lock"] = guarded
    stop = next(node for node in ast.walk(tree) if isinstance(node, ast.FunctionDef) and node.name == "stop_owned")
    checks["scripts/process_group.py:reaped_state_refuses_signals"] = any(
        isinstance(node, ast.If) and ast.unparse(node.test) == "handle.state == 'read_only'" and isinstance(node.body[0], ast.Return) for node in ast.walk(stop)
    )
    reapers = []
    for function in (node for node in ast.walk(tree) if isinstance(node, ast.FunctionDef)):
        if any(isinstance(call, ast.Call) and isinstance(call.func, ast.Attribute) and call.func.attr == "waitpid" for call in ast.walk(function)):
            reapers.append(function.name)
    checks["scripts/process_group.py:only_guarded_stop_reaps"] = set(reapers) <= {"stop_owned"}
    helpers = {node.attr for node in ast.walk(tree) if isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name) and node.value.id == "_portable"}
    checks["portable_helper_names_closed"] = helpers == {"group_state", "signal_group", "wait_group_gone"}
    recovery = trees["orchestration/worker_recovery.py"]
    recovery_aliases = {alias.asname or alias.name: alias.name for node in ast.walk(recovery) if isinstance(node, ast.ImportFrom) for alias in node.names}
    checks["recovery:signal_imports_closed"] = not any(
        alias.name in SIGNAL_FUNCTIONS for node in ast.walk(recovery) if isinstance(node, ast.ImportFrom) for alias in node.names
    )
    checks["recovery:no_spawner_pid_read"] = not any(
        (isinstance(node, ast.Attribute) and node.attr in {"marker_parent", "getpid", "getppid"}) or (isinstance(node, ast.Name) and node.id == "marker_parent")
        for node in ast.walk(recovery)
    )
    for function in (node for node in ast.walk(recovery) if isinstance(node, ast.FunctionDef)):
        checks[f"recovery:{function.name}:no_signal_or_reap"] = not any(
            isinstance(node, ast.Call) and _call_name(node, recovery_aliases) in SIGNAL_FUNCTIONS for node in ast.walk(function)
        )
    return checks
