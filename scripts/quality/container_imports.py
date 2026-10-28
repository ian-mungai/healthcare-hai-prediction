"""Resolve first-party Python imports against one container's static file locations."""

from __future__ import annotations

import ast
from collections import deque
from collections.abc import Iterator
from dataclasses import dataclass
from importlib.util import resolve_name
from pathlib import Path, PurePosixPath


@dataclass(frozen=True)
class Bind:
    """One repository source, or an external source, at a container path."""

    source: str | None
    target: str


@dataclass(frozen=True)
class Entry:
    """A Python module or script invoked inside a container."""

    value: str
    module: bool = True


@dataclass(frozen=True)
class Container:
    """The declared files and Python search roots of one container invocation."""

    name: str
    binds: tuple[Bind, ...]
    roots: tuple[str, ...]
    entry: Entry
    declaration: str
    line: int = 1
    workdir: str | None = None


@dataclass(frozen=True, order=True)
class Finding:
    """One unavailable first-party import, independent of the invocations that reach it."""

    path: str
    line: int
    name: str
    reason: str


def module_name(path: PurePosixPath) -> str:
    name = path.with_suffix("").as_posix().replace("/", ".")
    return name.removesuffix(".__init__")


def locations(files: set[str], binds: tuple[Bind, ...]) -> dict[str, str]:
    """Map visible Python files to their container paths, with nested mounts taking precedence."""
    result: dict[str, str] = {}
    for bind in sorted(binds, key=lambda item: len(PurePosixPath(item.target).parts)):
        target = bind.target.rstrip("/")
        result = {path: source for path, source in result.items() if path != target and not path.startswith(target + "/")}
        if bind.source is not None:
            source = bind.source.rstrip("/")
            for path in sorted(files):
                if path == source:
                    result[target] = path
                elif path.startswith(source + "/"):
                    result[target + path[len(source) :]] = path
    return result


def modules_at(paths: dict[str, str], roots: tuple[str, ...]) -> dict[str, str | None]:
    """Index modules and namespace packages by container import name, in Python search order."""
    result: dict[str, str | None] = {}
    for root in roots:
        for target, source in sorted(paths.items()):
            try:
                relative = PurePosixPath(target).relative_to(root)
            except ValueError:
                continue
            name = module_name(relative)
            if not all(part.isidentifier() for part in name.split(".")):
                continue
            result.setdefault(name, source)
            parts = name.split(".")
            for length in range(1, len(parts)):
                result.setdefault(".".join(parts[:length]), None)
    for name in list(result):
        if result[name] is None:
            for root in roots:
                if initializer := paths.get(root.rstrip("/") + "/" + name.replace(".", "/") + "/__init__.py"):
                    result[name] = initializer
                    break
    return result


def executable_nodes(nodes: list[ast.stmt], entry: bool) -> list[ast.AST]:
    result: list[ast.AST] = []
    for node in nodes:
        if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef):
            result.extend(node.decorator_list)
            result.extend(node.args.defaults)
            result.extend(value for value in node.args.kw_defaults if value is not None)
        elif isinstance(node, ast.ClassDef):
            result.extend(node.bases)
            result.extend(node.decorator_list)
            result.extend(executable_nodes(node.body, entry))
        elif isinstance(node, ast.If) and isinstance(node.test, ast.Compare) and isinstance(node.test.ops[0], ast.Eq):
            text = ast.unparse(node.test)
            if text in {"__name__ == '__main__'", "'__main__' == __name__"}:
                result.extend(executable_nodes(node.body if entry else node.orelse, entry))
            else:
                result.append(node)
        else:
            result.append(node)
    return result


def runtime_walk(node: ast.AST, type_guards: set[str]) -> Iterator[ast.AST]:
    """Walk runtime syntax while excluding the bodies of recognized TYPE_CHECKING guards."""
    yield node
    children = [node.test, *node.orelse] if isinstance(node, ast.If) and dotted(node.test) in type_guards else ast.iter_child_nodes(node)
    for child in children:
        yield from runtime_walk(child, type_guards)


def dotted(node: ast.AST) -> str:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        parent = dotted(node.value)
        return f"{parent}.{node.attr}" if parent else ""
    return ""


def alias_names(node: ast.AST, aliases: dict[str, set[str]]) -> set[str]:
    expression = dotted(node)
    if not expression:
        return set()
    head, _, tail = expression.partition(".")
    return {f"{value}.{tail}" if tail else value for value in aliases.get(head, {head})}


class ImportGraph:
    """Follow imports, package initialization and referenced definitions without importing application code."""

    def __init__(self, root: Path, files: set[str], container: Container, first_party: set[str], trees: dict[str, ast.Module]) -> None:
        self.root = root
        self.container = container
        self.paths = locations(files, container.binds)
        roots = container.roots
        self.entry_path = container.entry.value
        if not container.entry.module:
            if not self.entry_path.startswith("/"):
                if container.workdir is None:
                    raise ValueError(
                        f"{container.declaration}:{container.line}: container {container.name}: working folder cannot be resolved for a relative script"
                    )
                self.entry_path = str(PurePosixPath(container.workdir) / self.entry_path)
            roots = (str(PurePosixPath(self.entry_path).parent), *roots)
        self.modules = modules_at(self.paths, roots)
        self.known = {module_name(PurePosixPath(path)): path for path in files}
        aliases = {
            (str(PurePosixPath(source).parent) + "/", name if source.endswith("/__init__.py") else name.rpartition(".")[0])
            for name, source in self.modules.items()
            if source is not None
        }
        for folder, prefix in sorted(aliases):
            for candidate in sorted(files):
                if not candidate.startswith(folder):
                    continue
                alias = ".".join(filter(None, (prefix, module_name(PurePosixPath(candidate.removeprefix(folder))))))
                self.known.setdefault(alias, candidate)
        self.first_party = first_party | {name.split(".")[0] for name in self.known} | {name.split(".")[0] for name in self.modules}
        self.pending: deque[tuple[str, str | None, bool]] = deque()
        self.visited: set[tuple[str, str | None, bool]] = set()
        self.trees = trees
        self.type_guards: dict[str, set[str]] = {}
        self.findings: set[Finding] = set()

    def problem(self, path: str, line: int, name: str, reason: str) -> None:
        self.findings.add(Finding(path, line, name, reason))

    def tree(self, path: str) -> ast.Module:
        if path not in self.trees:
            try:
                self.trees[path] = ast.parse((self.root / path).read_text(encoding="utf-8"), filename=path)
            except SyntaxError as error:
                raise ValueError(f"{path}:{error.lineno}: Python syntax cannot be checked: {error.msg}") from error
        return self.trees[path]

    def require(self, name: str, path: str, line: int, *, entry: bool = False) -> None:
        if name.split(".")[0] not in self.first_party:
            return
        if name not in self.modules:
            self.problem(path, line, name, "is outside the declared mounts or Python search roots")
            return
        parts = name.split(".")
        for length in range(1, len(parts)):
            package = ".".join(parts[:length])
            if package in self.modules:
                self.pending.append((package, None, False))
        self.pending.append((name, None, entry))

    def reference(self, name: str, path: str, line: int) -> None:
        parts = name.split(".")
        for length in range(len(parts), 0, -1):
            module = ".".join(parts[:length])
            if module in self.modules or module in self.known:
                self.require(module, path, line)
                if length < len(parts) and module in self.modules:
                    self.pending.append((module, parts[length], False))
                return

    def scan(self, name: str, symbol: str | None, entry: bool) -> None:
        path = self.modules[name]
        if path is None:
            return
        tree = self.tree(path)
        definitions = {node.name: node for node in tree.body if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef | ast.ClassDef)}
        global_nodes = executable_nodes(tree.body, entry)
        if symbol is None:
            nodes = global_nodes
            if entry:
                self.pending.extend((name, definition, False) for definition in definitions)
        elif symbol in definitions:
            nodes = [definitions[symbol]]
        elif symbol == "*":
            self.pending.extend((name, definition, False) for definition in definitions)
            return
        else:
            return
        package = name if path.endswith("/__init__.py") else name.rpartition(".")[0]
        aliases: dict[str, set[str]] = {}
        type_guards = self.type_guards.get(path)
        if type_guards is None:
            type_guards = set()
            for node in ast.walk(tree):
                if isinstance(node, ast.ImportFrom) and node.module == "typing":
                    type_guards.update(alias.asname or alias.name for alias in node.names if alias.name == "TYPE_CHECKING")
                elif isinstance(node, ast.Import):
                    type_guards.update(f"{alias.asname or alias.name}.TYPE_CHECKING" for alias in node.names if alias.name == "typing")
            self.type_guards[path] = type_guards
        walked = [item for node in nodes for item in runtime_walk(node, type_guards)]
        parents = {child: parent for parent in walked for child in ast.iter_child_nodes(parent)}
        imports = [item for node in global_nodes for item in runtime_walk(node, type_guards) if isinstance(item, ast.Import | ast.ImportFrom)]
        imports.extend(item for item in walked if isinstance(item, ast.Import | ast.ImportFrom))
        for import_node in imports:
            if isinstance(import_node, ast.Import):
                for alias in import_node.names:
                    self.require(alias.name, path, import_node.lineno)
                    aliases.setdefault(alias.asname or alias.name.split(".")[0], set()).add(alias.name if alias.asname else alias.name.split(".")[0])
            else:
                assert_from = import_node.module or ""
                if import_node.level:
                    parts = package.split(".") if package else []
                    if import_node.level > len(parts):
                        self.problem(path, import_node.lineno, "." * import_node.level + assert_from, "has no containing package")
                        continue
                    assert_from = ".".join([*parts[: len(parts) - import_node.level + 1], *([assert_from] if assert_from else [])])
                self.require(assert_from, path, import_node.lineno)
                for alias in import_node.names:
                    qualified = f"{assert_from}.{alias.name}"
                    aliases.setdefault(alias.asname or alias.name, set()).add(qualified)
                    if qualified in self.known or qualified in self.modules:
                        self.require(qualified, path, import_node.lineno)
                    else:
                        self.reference(qualified, path, import_node.lineno)
        for node in walked:
            if isinstance(node, ast.Assign):
                for bound_name in node.targets:
                    if isinstance(bound_name, ast.Name) and (resolved := alias_names(node.value, aliases)):
                        aliases.setdefault(bound_name.id, set()).update(resolved)
        for node in walked:
            if isinstance(node, ast.Call):
                resolved = alias_names(node.func, aliases)
                if resolved & {"importlib.import_module", "__import__"} and node.args and isinstance(node.args[0], ast.Constant):
                    target = node.args[0].value
                    if isinstance(target, str):
                        if target.startswith("."):
                            context = node.args[1] if len(node.args) > 1 else next((word.value for word in node.keywords if word.arg == "package"), None)
                            base = context.value if isinstance(context, ast.Constant) and isinstance(context.value, str) else ""
                            if isinstance(context, ast.Name) and context.id == "__package__":
                                base = package
                            try:
                                target = resolve_name(target, base)
                            except (ValueError, ImportError):
                                self.problem(path, node.lineno, target, "has no statically known containing package")
                                continue
                        self.require(target, path, node.lineno)
                        for assignment in walked:
                            if isinstance(assignment, ast.Assign) and assignment.value is node:
                                for bound in assignment.targets:
                                    if isinstance(bound, ast.Name):
                                        aliases.setdefault(bound.id, set()).add(target)
        for node in walked:
            if isinstance(node, ast.Name) and isinstance(node.ctx, ast.Load) and node.id in definitions:
                self.pending.append((name, node.id, False))
            if isinstance(node, ast.Name) and isinstance(node.ctx, ast.Load):
                parent = parents.get(node)
                if isinstance(parent, ast.Call | ast.keyword | ast.List | ast.Tuple | ast.Dict | ast.Return):
                    for value in aliases.get(node.id, set()):
                        if value in self.modules:
                            self.pending.append((value, "*", False))
            expression = dotted(node) if isinstance(node, ast.Name | ast.Attribute) else ""
            head, _, tail = expression.partition(".")
            for value in aliases.get(head, set()):
                self.reference(f"{value}.{tail}" if tail else value, path, getattr(node, "lineno", 1))

    def check(self) -> list[Finding]:
        """Return stable findings for the declared entry point and its first-party dependencies."""
        container = self.container
        if container.entry.module:
            self.require(container.entry.value, container.declaration, container.line, entry=True)
        elif path := self.paths.get(self.entry_path):
            self.modules["__main__"] = path
            self.pending.append(("__main__", None, True))
        else:
            self.problem(container.declaration, container.line, container.entry.value, "is outside the declared mounts")
        while self.pending:
            task = self.pending.popleft()
            if task not in self.visited:
                self.visited.add(task)
                self.scan(*task)
        return sorted(self.findings)
