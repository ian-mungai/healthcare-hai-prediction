"""Find the tracked files a Python module names: the paths its strings, ``/`` joins and f-strings build.

Used by the CI-groups end-to-end suite (the reader scans) and by scripts/quality/launcher_inputs.py (the files the launcher suite reads).
"""

from __future__ import annotations

import ast


def fills_template(template: list[str | None], path: str, names: set[str]) -> bool:
    """True when ``path`` is the f-string ``template`` (constant parts, ``None`` per placeholder) with each placeholder replaced by a string of ``names``."""

    def from_part(index: int, position: int) -> bool:
        if index == len(template):
            return position == len(path)
        part = template[index]
        if part is not None:
            return path.startswith(part, position) and from_part(index + 1, position + len(part))
        return any(path[position:end] in names and from_part(index + 1, end) for end in range(position + 1, len(path) + 1))

    return from_part(0, 0)


def source_inputs(source: str, tracked: set[str], rooted: bool = False) -> dict[str, str]:
    """Every tracked file that a string anywhere in ``source`` names, in module-level constants and inside functions alike, with the text that names it.
    A string constant, or a ``/`` join of strings (``Path(..)``, names assigned a join anywhere in the module), names a file or the files in a folder when it
    holds a slash; a join that is only created (the receiver of ``mkdir``, ``write_text``, ``write_bytes``, ``touch`` or ``unlink``) is not read. An f-string
    names every tracked file it matches when each placeholder is replaced by a string constant of the module (``f"dbt/models/{folder}/{name}.sql"``).
    Docstrings and one-component names are not read as paths, unless ``rooted`` is set: then a join that starts at ``ROOT`` or ``REPO_ROOT``
    (``ROOT / "docker-compose.yaml"``, ``ROOT / "dbt"``) and a string equal to a tracked file of the repository root also name their files. The source is
    read, not imported."""
    tree = ast.parse(source)
    in_fstring = {id(part) for node in ast.walk(tree) if isinstance(node, ast.JoinedStr) for part in node.values}
    docstrings = {id(node.value) for node in ast.walk(tree) if isinstance(node, ast.Expr)}
    strings = {
        node.value
        for node in ast.walk(tree)
        if isinstance(node, ast.Constant) and isinstance(node.value, str) and id(node) not in in_fstring and id(node) not in docstrings
    }
    created = {
        id(node.func.value)
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr in {"mkdir", "write_text", "write_bytes", "touch", "unlink"}
    }
    known = {"ROOT": "", "REPO_ROOT": ""}
    roots = {"ROOT", "REPO_ROOT"}
    rooted_names: set[str] = set()
    rooted_variables: set[str] = set()

    def operand(node: ast.expr) -> str | None:
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            return node.value
        if isinstance(node, ast.Name):
            return known.get(node.id)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "Path" and len(node.args) == 1:
            return operand(node.args[0])
        return None

    def joined(top: ast.BinOp) -> list[str]:
        parts: list[str | None] = []
        node: ast.expr = top
        while isinstance(node, ast.BinOp) and isinstance(node.op, ast.Div):
            parts.append(operand(node.right))
            node = node.left
        parts.append(operand(node))
        starts_at_root = isinstance(node, ast.Name) and (node.id in roots or node.id in rooted_variables)
        runs: list[str] = []
        run: list[str] = []
        first = True
        for part in [*reversed(parts), None]:
            if part is None:
                if len(run) > 1:
                    runs.append("/".join(piece.strip("/") for piece in run if piece.strip("/")))
                    if first and starts_at_root:
                        rooted_names.add(runs[-1])
                run = []
                first = False
            else:
                run.append(part)
        return runs

    assigned = [
        (node.targets[0].id, node.value)
        for node in ast.walk(tree)
        if isinstance(node, ast.Assign) and len(node.targets) == 1 and isinstance(node.targets[0], ast.Name) and node.targets[0].id not in known
    ]
    for _ in range(3):
        for name, value in assigned:
            if isinstance(value, ast.BinOp) and isinstance(value.op, ast.Div):
                if len(runs := joined(value)) == 1:
                    known[name] = runs[0]
                    if runs[0] in rooted_names:
                        rooted_variables.add(name)
            elif (text := operand(value)) is not None:
                known[name] = text
    inner = {id(node.left) for node in ast.walk(tree) if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Div)}
    names = set(strings)
    for node in ast.walk(tree):
        if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Div) and id(node) not in inner and id(node) not in created:
            names.update(joined(node))
    templates: list[list[str | None]] = [
        [part.value if isinstance(part, ast.Constant) and isinstance(part.value, str) else None for part in node.values]
        for node in ast.walk(tree)
        if isinstance(node, ast.JoinedStr)
    ]
    if rooted:
        names.update(name for name in strings if "/" not in name and name in tracked)
    found: dict[str, str] = {}
    for path in sorted(tracked):
        for text in sorted(names):
            base = text.strip("/")
            if ("/" in base or (rooted and (base in rooted_names or base in tracked))) and (path == base or path.startswith(f"{base}/")):
                found.setdefault(path, text)
        for template in templates:
            if None in template and any(template) and fills_template(template, path, strings):
                found.setdefault(path, 'f"' + "{}".join(part or "" for part in template) + '"')
    return found
