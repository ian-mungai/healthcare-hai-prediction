"""Compute the repository files a collector can run, so its reviewed code version binds those files and no others."""

import ast
import hashlib
import posixpath
import re
from pathlib import Path

from scripts.acquisition.source_registry import REPO_ROOT, RegistryError, require

CODE_DIRECTORIES = ("scripts",)
MODULE_LITERAL = re.compile(r"[A-Za-z_]\w*(\.[A-Za-z_]\w*)+")
FORBIDDEN_MODULES = {"importlib", "runpy", "imp", "pkgutil", "builtins"}
FORBIDDEN_NAMES = {"__import__", "exec", "eval", "compile", "__builtins__"}
FORBIDDEN_ATTRIBUTES = {"__import__", "__builtins__"}
COMPILED_SUFFIXES = (".so", ".pyd", ".pyc")
SCRIPT_SUFFIXES = {".sh", ".bash", ".zsh", ".ksh", ".csh", ".fish", ".ps1", ".bat", ".cmd", ".pl", ".rb", ".js", ".php"}
PATH_FUNCTIONS = {"Path", "PurePath", "PosixPath", "PurePosixPath", "join", "joinpath", "normpath", "abspath", "realpath", "relpath"}
LONGEST_PATH = 1024


def is_file(path: Path) -> bool:
    """True when the path is a file. A name the system cannot look up is not a file."""
    try:
        return path.is_file()
    except (OSError, ValueError):
        return False


def compiled_file(base: Path, root: Path) -> Path | None:
    """A compiled or extension file Python would load for this module, such as native.so or native.cpython-312-darwin.so, else None."""
    for folder, stem in ((base, "__init__"), (base.parent, base.name)):
        if (root / folder).is_dir():
            for child in sorted((root / folder).iterdir()):
                if child.name.startswith(f"{stem}.") and child.name.endswith(COMPILED_SUFFIXES):
                    return folder / child.name
    return None


def module_file(parts: list[str], root: Path) -> str | None:
    """The repository file Python loads for exactly this dotted module, a package before a module of the same name, else None.

    A compiled or extension file for the module cannot be read, so it is refused."""
    if not parts:
        return None
    base = Path(*parts)
    opaque = compiled_file(base, root)
    require(opaque is None, f"Cannot bind code: {opaque.as_posix() if opaque else ''} is compiled code that cannot be read")
    return next((c.as_posix() for c in (base / "__init__.py", base.with_suffix(".py")) if (root / c).is_file()), None)


def longest_module_file(parts: list[str], root: Path) -> str | None:
    """The file of the longest existing prefix of a dotted name, because the tail may be an attribute."""
    return next((found for size in range(len(parts), 0, -1) if (found := module_file(parts[:size], root))), None)


def descendants(parts: list[str], root: Path) -> list[list[str]]:
    """Every module name below a repository package at any depth, because a wildcard import loads what __all__ names, dotted names too."""
    if not (module_file(parts, root) or "").endswith("__init__.py"):
        return []
    found: list[list[str]] = []
    pending = [parts]
    while pending:
        current = pending.pop()
        for child in sorted((root / Path(*current)).iterdir()):
            stem = child.name.split(".")[0]
            if child.is_dir() and stem not in {"", "__pycache__"}:
                found.append([*current, child.name])
                pending.append([*current, child.name])
            elif stem not in {"", "__init__", "__pycache__"} and child.name.endswith((".py", *COMPILED_SUFFIXES)):
                found.append([*current, stem])
    return found


def resolve_relative(package: list[str], level: int, named: list[str], where: str) -> list[str]:
    """The module a relative name means inside a package. A level above the top-level package is refused."""
    require(level - 1 <= len(package), f"{where} imports above its top-level package")
    return [*package[: len(package) - (level - 1)], *named]


def absolute_base(node: ast.ImportFrom, relative: str) -> list[str]:
    """The module an import-from names, a relative one resolved against the package of the importing file."""
    named = node.module.split(".") if node.module else []
    if not node.level:
        return named
    return resolve_relative(list(Path(relative).with_suffix("").parts[:-1]), node.level, named, f"Cannot bind code: {relative}:{node.lineno}")


def literal_text(node: ast.expr | None) -> str | None:
    """The text of a string literal, else None."""
    return node.value if isinstance(node, ast.Constant) and isinstance(node.value, str) else None


def argument(call: ast.Call, position: int, keyword: str) -> ast.expr | None:
    """The call argument at this position or keyword, else None. Unpacked arguments cannot be read, so they are refused."""
    unpacked = any(isinstance(a, ast.Starred) for a in call.args) or any(k.arg is None for k in call.keywords)
    require(not unpacked, f"Cannot bind code: line {call.lineno} imports a module with unpacked arguments")
    return call.args[position] if len(call.args) > position else next((k.value for k in call.keywords if k.arg == keyword), None)


def dynamic_import(call: ast.Call, relative: str) -> tuple[list[str], list[str]]:
    """The module a plain literal __import__ call loads and the names its fromlist may load, else refuse.

    The name, the fromlist and the level must be literals, and the level must be 0. The globals argument is never read."""
    where = f"Cannot bind code: {relative}:{call.lineno}"
    text = literal_text(argument(call, 0, "name"))
    require(text is not None, f"{where} imports a module by a computed name")
    named = [p for p in str(text).split(".") if p]
    require(bool(named) and named[0] not in FORBIDDEN_MODULES, f"{where} imports {text}, which runs code by name")
    level = argument(call, 4, "level")
    require(level is None or (isinstance(level, ast.Constant) and level.value == 0), f"{where} imports a relative or computed level")
    fromlist = argument(call, 3, "fromlist")
    if fromlist is None or (isinstance(fromlist, ast.Constant) and fromlist.value is None):
        return named, []
    names = [literal_text(e) for e in fromlist.elts] if isinstance(fromlist, ast.List | ast.Tuple) else [None]
    require(all(n is not None for n in names), f"{where} imports with a computed fromlist")
    return named, [str(n) for n in names]


def binds_name(node: ast.AST) -> list[str]:
    """Every name this node loads, stores or declares, so a refused name is caught in any role."""
    if isinstance(node, ast.Name):
        return [node.id]
    if isinstance(node, ast.Attribute):
        return [node.attr] if node.attr in FORBIDDEN_ATTRIBUTES else []
    if isinstance(node, ast.alias):
        return [node.name, node.asname or ""]
    if isinstance(node, ast.arg):
        return [node.arg]
    if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef | ast.ClassDef | ast.ExceptHandler | ast.MatchAs | ast.MatchStar):
        return [node.name or ""]
    if isinstance(node, ast.MatchMapping):
        return [node.rest or ""]
    if isinstance(node, ast.Global | ast.Nonlocal):
        return node.names
    return []


def refuse_dynamic(tree: ast.Module, relative: str) -> dict[int, ast.Call]:
    """Refuse a whole class of dynamic code loading, whatever the spelling.

    Any import of importlib, runpy, imp, pkgutil or builtins is refused, and so is any use of __import__, exec, eval, compile or
    __builtins__ except a call to __import__ with a literal name. Return those calls keyed by the node of the called name."""
    plain = {id(n.func): n for n in ast.walk(tree) if isinstance(n, ast.Call) and isinstance(n.func, ast.Name) and n.func.id == "__import__"}
    for node in ast.walk(tree):
        where = f"Cannot bind code: {relative}:{getattr(node, 'lineno', 0)}"
        modules = [] if not isinstance(node, ast.Import | ast.ImportFrom) else [a.name for a in node.names]
        if isinstance(node, ast.ImportFrom) and not node.level:
            modules.append(node.module or "")
        for module in modules:
            require(module.split(".")[0] not in FORBIDDEN_MODULES, f"{where} imports {module}, which runs code by name")
        for name in [] if id(node) in plain else binds_name(node):
            require(name not in FORBIDDEN_NAMES | FORBIDDEN_ATTRIBUTES, f"{where} uses {name}, which runs code by name")
    return plain


def literal_tail(node: ast.expr) -> str | None:
    """The literal text a path expression ends with, folded across a string, a / chain and a Path or join call. A part that is not a literal ends it."""
    text = literal_text(node)
    if text is not None:
        return text
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Div):
        right = literal_tail(node.right)
        left = literal_tail(node.left) if right is not None else None
        return right if left is None or (right or "").startswith("/") else posixpath.join(left, right or "")
    if isinstance(node, ast.Call):
        name = node.func.id if isinstance(node.func, ast.Name) else node.func.attr if isinstance(node.func, ast.Attribute) else ""
        if name in PATH_FUNCTIONS:
            pieces = [*([node.func.value] if isinstance(node.func, ast.Attribute) and name == "joinpath" else []), *node.args]
            tail: list[str] = []
            for piece in reversed(pieces):
                if (text := literal_tail(piece)) is None:
                    break
                tail.insert(0, text)
            return posixpath.join(*tail) if tail else None
    return None


def path_texts(tree: ast.Module) -> set[str]:
    """Every string in the file that can name a path: each string with a slash and each folded Path, join or / expression."""
    found = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Constant | ast.BinOp | ast.Call) and (text := literal_tail(node)) is not None and "/" in text.replace("\\", "/"):
            found.add(text)
    return found


def repository_paths(text: str, root: Path, relative: str) -> list[str]:
    """The repository-relative paths a text can name, normalized and tested from the root and from the folder of the file that holds it.

    A relative path that leaves the repository and names an existing file is refused. An absolute path outside the repository is not followed."""
    if "\n" in text or "\0" in text or len(text) > LONGEST_PATH:
        return []
    found: list[str] = []
    for form in dict.fromkeys([text, text.replace("\\", "/")]):
        for base in dict.fromkeys(["", posixpath.dirname(relative)]):
            joined = posixpath.normpath(posixpath.join(base, form))
            if joined.startswith("/"):
                try:
                    joined = Path(joined).relative_to(root).as_posix()
                except ValueError:
                    continue
            elif joined == ".." or joined.startswith("../"):
                require(not is_file(root / joined), f"Cannot bind code: {relative} names {text}, a file outside the repository")
                continue
            if joined != "." and joined not in found:
                found.append(joined)
    return found


def refuse_executable(text: str, root: Path, relative: str, code_directories: tuple[str, ...]) -> None:
    """Refuse a named file, already normalized, that runs code the closure cannot read: a script by suffix, an executable bit or, in code, a #! line."""
    target = root / text
    if text.endswith(".py"):
        return
    executable = target.suffix.lower() in SCRIPT_SUFFIXES or bool(target.stat().st_mode & 0o111)
    if not executable and text.split("/")[0].casefold() in {d.casefold() for d in code_directories}:
        executable = target.read_bytes()[:2] == b"#!"
    require(not executable, f"Cannot bind code: {relative} names {text}, an executable file whose own dependencies are not bound")


def references(relative: str, root: Path, code_directories: tuple[str, ...]) -> set[str]:
    """Every repository file this file names: imports, literal dynamic imports, module names and code paths held in string literals."""
    try:
        tree = ast.parse((root / relative).read_text(encoding="utf-8"), filename=relative)
    except (SyntaxError, UnicodeDecodeError, ValueError) as error:
        raise RegistryError(f"Cannot bind code: {relative} is not readable Python ({error})") from error
    found: set[str] = set()

    def add_module(name: str | list[str], exact: bool) -> None:
        parts = name.split(".") if isinstance(name, str) else name
        target = module_file(parts, root) if exact else longest_module_file(parts, root)
        if target:
            found.add(target)

    def add_children(base: list[str], names: list[str]) -> None:
        for name in names:
            current = list(base)
            for part in filter(None, name.split(".")):
                if part == "*":
                    for child in descendants(current, root):
                        add_module(child, exact=True)
                    break
                current.append(part)
                add_module(current, exact=True)

    imports = refuse_dynamic(tree, relative)
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                add_module(alias.name, exact=False)
        elif isinstance(node, ast.ImportFrom):
            base = absolute_base(node, relative)
            names = [alias.name for alias in node.names]
            require(
                "*" not in names or bool(base and module_file(base, root)),
                f"Cannot bind code: {relative}:{node.lineno} imports * from a module outside the repository",
            )
            if base:
                add_module(base, exact=False)
                add_children(base, names)
        elif isinstance(node, ast.Call) and id(node.func) in imports:
            base, names = dynamic_import(node, relative)
            add_module(base, exact=False)
            add_children(base, names)
        elif isinstance(node, ast.Constant) and isinstance(node.value, str) and MODULE_LITERAL.fullmatch(node.value):
            add_module(node.value, exact=True)
    for text in sorted(path_texts(tree)):
        for path in repository_paths(text, root, relative):
            if is_file(root / path):
                refuse_executable(path, root, relative, code_directories)
                if path.split("/")[0] in code_directories:
                    found.add(path)
    return found


def parent_packages(relative: str, root: Path) -> set[str]:
    """The package initializers Python runs before it loads this file."""
    parts = Path(relative).parts
    return {init for size in range(1, len(parts)) if (root / (init := Path(*parts[:size], "__init__.py").as_posix())).is_file()}


def closure(entry: str, root: Path | None = None, code_directories: tuple[str, ...] = CODE_DIRECTORIES) -> list[str]:
    """The sorted repository-relative files of the entry module and everything it can run, found through its imports and literal references."""
    base = REPO_ROOT if root is None else root
    start = module_file(entry.split("."), base)
    require(start is not None, f"Cannot bind code: entry module {entry} is not in the repository")
    seen: set[str] = set()
    pending = [str(start)]
    while pending:
        current = pending.pop()
        if current in seen:
            continue
        seen.add(current)
        pending.extend(parent_packages(current, base) | (references(current, base, code_directories) if current.endswith(".py") else set()))
    return sorted(seen)


def closure_hashes(entry: str, root: Path | None = None, code_directories: tuple[str, ...] = CODE_DIRECTORIES) -> dict[str, str]:
    """Repository-relative path to byte SHA-256 for every file in the closure of an entry module."""
    base = REPO_ROOT if root is None else root
    return {name: hashlib.sha256((base / name).read_bytes()).hexdigest() for name in closure(entry, root, code_directories)}
