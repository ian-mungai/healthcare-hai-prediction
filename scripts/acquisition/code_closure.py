"""Compute the repository files a collector can run, so its reviewed code version binds those files and no others."""

import ast
import hashlib
import os
import posixpath
import re
import stat
from functools import lru_cache
from pathlib import Path

from scripts.acquisition.source_registry import REPO_ROOT, RegistryError, require

CODE_DIRECTORIES = ("scripts",)
MODULE_LITERAL = re.compile(r"[A-Za-z_]\w*(\.[A-Za-z_]\w*)+")
FORBIDDEN_MODULES = {"importlib", "runpy", "imp", "pkgutil", "builtins"}
FORBIDDEN_NAMES = {"__import__", "exec", "eval", "compile", "__builtins__"}
FORBIDDEN_ATTRIBUTES = {"__import__", "__builtins__"}
COMPILED_SUFFIXES = (".so", ".pyd", ".pyc")
COMPILED_TARGETS = (*COMPILED_SUFFIXES, ".pyo")
LINK_LIMIT = 40
SCRIPT_SUFFIXES = {".sh", ".bash", ".zsh", ".ksh", ".csh", ".fish", ".ps1", ".bat", ".cmd", ".pl", ".rb", ".js", ".php"}
PATH_FUNCTIONS = {"Path", "PurePath", "PosixPath", "PurePosixPath", "join", "joinpath", "normpath", "abspath", "realpath", "relpath"}
LONGEST_PATH = 1024


def compiled_file(base: Path, root: Path) -> Path | None:
    """A compiled or extension file Python would load for this module, such as native.so or native.cpython-312-darwin.so, else None."""
    for folder, stem in ((base, "__init__"), (base.parent, base.name)):
        if (root / folder).is_dir():
            for name in sorted(os.listdir(root / folder)):
                if name.startswith(f"{stem}.") and name.endswith(COMPILED_SUFFIXES):
                    return folder / name
    return None


def module_file(parts: list[str], root: Path, folder: str = "") -> str | None:
    """The repository file Python loads for exactly this dotted module, looked up in a folder of the import path, else None.

    The folder is repository-relative, and empty for the repository root. A package comes before a module of the same name.
    A compiled or extension file for the module cannot be read, so it is refused."""
    if not parts:
        return None
    base = Path(folder, *parts)
    opaque = compiled_file(base, root)
    require(opaque is None, f"Cannot bind code: {opaque.as_posix() if opaque else ''} is compiled code that cannot be read")
    return next((c.as_posix() for c in (base / "__init__.py", base.with_suffix(".py")) if (root / c).is_file()), None)


def longest_module_file(parts: list[str], root: Path, folder: str = "") -> str | None:
    """The file of the longest existing prefix of a dotted name, because the tail may be an attribute."""
    return next((found for size in range(len(parts), 0, -1) if (found := module_file(parts[:size], root, folder))), None)


def folder_identity(path: Path) -> tuple[int, int]:
    """The device and inode of a folder, links followed, so two names of one folder compare equal."""
    status = os.stat(path)
    return status.st_dev, status.st_ino


def descendants(parts: list[str], root: Path, folder: str = "") -> list[list[str]]:
    """Every module name below a repository package at any depth, because a wildcard import loads what __all__ names, dotted names too.

    A folder is entered once, whatever its names. Real folders claim an identity before a link to a folder does, so a link
    that loops back (a -> .) adds nothing and never hides a real name."""
    if not (module_file(parts, root, folder) or "").endswith("__init__.py"):
        return []
    found: list[list[str]] = []
    visited = {folder_identity(root / folder / Path(*parts))}
    pending: list[list[str]] = [parts]
    linked: list[list[str]] = []
    while pending or linked:
        from_link = not pending
        current = linked.pop() if from_link else pending.pop()
        if from_link:
            key = folder_identity(root / folder / Path(*current))
            if key in visited:
                continue
            visited.add(key)
            found.append(current)
        for child in sorted((root / folder / Path(*current)).iterdir()):
            stem = child.name.split(".")[0]
            if child.is_dir() and stem not in {"", "__pycache__"}:
                name = [*current, child.name]
                if child.is_symlink():
                    linked.append(name)
                elif (key := folder_identity(child)) not in visited:
                    visited.add(key)
                    found.append(name)
                    pending.append(name)
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


def status_of(path: str) -> os.stat_result | None:
    """The status of the file or folder a path reaches on disk, links followed, else None when the system cannot look it up."""
    try:
        return os.stat(path)
    except (OSError, ValueError):
        return None


def code_index(root: Path, code_directories: tuple[str, ...]) -> dict[tuple[int, int], list[str]]:
    """The stored repository-relative names of each regular file and each folder under the code directories, keyed by device and inode.

    A hard link gives one file several names. A folder reached through a link has the name of the real folder.
    The index is built for each closure, so a file added or linked later is seen."""
    index: dict[tuple[int, int], list[str]] = {}
    for directory in code_directories:
        for folder, _, names in os.walk(root / directory):
            for full in [Path(folder), *(Path(folder, name) for name in names)]:
                status = full.lstat()
                if stat.S_ISREG(status.st_mode) or stat.S_ISDIR(status.st_mode):
                    index.setdefault((status.st_dev, status.st_ino), []).append(full.relative_to(root).as_posix())
    return {key: sorted(names) for key, names in index.items()}


def inside(path: str, root: Path) -> bool:
    """True when a parent folder of the path, with every link resolved, is the repository root, compared by device and inode."""
    home = os.stat(root)
    for folder in Path(os.path.realpath(path)).parents:
        found = status_of(str(folder))
        if found and (found.st_dev, found.st_ino) == (home.st_dev, home.st_ino):
            return True
    return False


def spellings(text: str, root: Path, relative: str) -> dict[str, bool]:
    """Each path on disk a text can name, as written and normalized, from the root and from the folder of the file that holds it.

    The value is True when the text is absolute."""
    if "\n" in text or "\0" in text or len(text) > LONGEST_PATH:
        return {}
    found: dict[str, bool] = {}
    for form in dict.fromkeys([text, text.replace("\\", "/")]):
        for base in dict.fromkeys(["", posixpath.dirname(relative)]):
            joined = posixpath.join(root.as_posix(), base, form)
            for full in (joined, posixpath.normpath(joined)):
                found.setdefault(full, form.startswith("/"))
    return found


def refuse_executable(full: str, status: os.stat_result, names: list[str], text: str, relative: str) -> None:
    """Refuse a reached file that runs code the closure cannot read: compiled code, a script by suffix, an executable bit or, in code, a #! line.

    A file stored under a .py name is Python, which the closure reads instead. Every spelling of the file counts, in any letter case."""
    real = os.path.realpath(full)
    compiled = Path(full).suffix.lower() in COMPILED_TARGETS or Path(real).suffix.lower() in COMPILED_TARGETS
    require(not compiled, f"Cannot bind code: {relative} names {text}, compiled code that cannot be read")
    if any(name.casefold().endswith(".py") for name in names or [real]):
        return
    executable = any(Path(name).suffix.lower() in SCRIPT_SUFFIXES for name in [full, real, *names]) or bool(status.st_mode & 0o111)
    if not executable and names:
        executable = Path(full).read_bytes()[:2] == b"#!"
    require(not executable, f"Cannot bind code: {relative} names {text}, an executable file whose own dependencies are not bound")


def script_folders(path: str) -> list[str]:
    """The folders Python may put first on the import path when it runs a script path: the folder of the path as written,
    of each link level it passes and of the real file."""
    found, current = [], path
    for _ in range(LINK_LIMIT):
        found.append(os.path.dirname(current))
        if not os.path.islink(current):
            break
        current = os.path.join(os.path.dirname(current), os.readlink(current))
    return [*found, os.path.dirname(os.path.realpath(path))]


def stored_folders(folders: list[str], names: list[str], index: dict[tuple[int, int], list[str]]) -> set[str]:
    """The stored names of code folders: each given folder matched by device and inode, and the folder of each stored file name."""
    found = {posixpath.dirname(name) for name in names}
    for folder in folders:
        status = status_of(folder)
        if status is not None and stat.S_ISDIR(status.st_mode):
            found.update(index.get((status.st_dev, status.st_ino), []))
    return found


def reached_files(text: str, root: Path, relative: str, index: dict[tuple[int, int], list[str]]) -> tuple[set[str], set[str]]:
    """The code files a path text reaches on disk, matched by device and inode, so any spelling of a repository file binds it.
    Also the code folders Python puts first on the import path when it runs those files.

    A folder stands for the __main__.py Python runs in it, and is itself the first import folder. For a script it is the script's folder.
    A relative spelling that reaches a file outside the repository is refused. An absolute spelling outside the repository is not followed."""
    found: set[str] = set()
    launched: set[str] = set()
    for full, absolute in spellings(text, root, relative).items():
        status = status_of(full)
        folders = script_folders(full)
        if status is not None and stat.S_ISDIR(status.st_mode):
            folders = [full, os.path.realpath(full)]
            full = posixpath.join(full, "__main__.py")
            status = status_of(full)
        if status is None or not stat.S_ISREG(status.st_mode):
            continue
        names = index.get((status.st_dev, status.st_ino), [])
        if not names and not inside(full, root):
            require(absolute, f"Cannot bind code: {relative} names {text}, a file outside the repository")
            continue
        refuse_executable(full, status, names, text, relative)
        found.update(names)
        launched |= stored_folders(folders, names, index)
    return found, launched


@lru_cache(maxsize=2048)
def analyzed(source: str, relative: str) -> tuple[tuple[ast.AST, ...], dict[int, ast.Call], tuple[str, ...]]:
    """Parse one file's text and run the checks that read only that text. A closure runs for each code check, so the same text is analyzed once per process."""
    tree = ast.parse(source, filename=relative)
    return tuple(ast.walk(tree)), refuse_dynamic(tree, relative), tuple(sorted(path_texts(tree)))


def references(
    relative: str, root: Path, index: dict[tuple[int, int], list[str]], folders: set[str] | frozenset[str] = frozenset()
) -> tuple[set[str], set[str]]:
    """Every repository file this file names: imports, literal dynamic imports, module names and code paths held in string literals.
    Also the code folders that a script or folder path in this file puts first on the import path.

    A module is looked up in the repository root and in each of the given launch folders."""
    try:
        nodes, imports, texts = analyzed((root / relative).read_text(encoding="utf-8"), relative)
    except (SyntaxError, UnicodeDecodeError, ValueError) as error:
        raise RegistryError(f"Cannot bind code: {relative} is not readable Python ({error})") from error
    found: set[str] = set()
    launched: set[str] = set()
    lookups = ["", *sorted(folders)]

    def add_module(name: str | list[str], exact: bool) -> None:
        parts = name.split(".") if isinstance(name, str) else name
        for folder in lookups:
            if target := module_file(parts, root, folder) if exact else longest_module_file(parts, root, folder):
                found.add(target)

    def add_children(base: list[str], names: list[str]) -> None:
        for name in names:
            current = list(base)
            for part in filter(None, name.split(".")):
                if part == "*":
                    for folder in lookups:
                        for child in descendants(current, root, folder):
                            add_module(child, exact=True)
                    break
                current.append(part)
                add_module(current, exact=True)

    for node in nodes:
        if isinstance(node, ast.Import):
            for alias in node.names:
                add_module(alias.name, exact=False)
        elif isinstance(node, ast.ImportFrom):
            base = absolute_base(node, relative)
            names = [alias.name for alias in node.names]
            require(
                "*" not in names or bool(base and any(module_file(base, root, folder) for folder in lookups)),
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
    for text in texts:
        files, folders_found = reached_files(text, root, relative, index)
        found |= files
        launched |= folders_found
    return found, launched


def parent_packages(relative: str, root: Path) -> set[str]:
    """The package initializers Python runs before it loads this file."""
    parts = Path(relative).parts
    return {init for size in range(1, len(parts)) if (root / (init := Path(*parts[:size], "__init__.py").as_posix())).is_file()}


def walk(start: str, root: Path, index: dict[tuple[int, int], list[str]], folders: set[str]) -> tuple[set[str], set[str]]:
    """The files reached from a start file when modules are also looked up in the launch folders, and the launch folders found on the way."""
    seen: set[str] = set()
    launched: set[str] = set()
    pending = [start]
    while pending:
        current = pending.pop()
        if current in seen:
            continue
        seen.add(current)
        pending.extend(parent_packages(current, root))
        if current.casefold().endswith(".py"):
            files, found = references(current, root, index, folders)
            pending.extend(files)
            launched |= found
    return seen, launched


def closure(entry: str, root: Path | None = None, code_directories: tuple[str, ...] = CODE_DIRECTORIES) -> list[str]:
    """The sorted repository-relative files of the entry module and everything it can run, found through its imports and literal references.

    A script or folder run by path puts its folder first on the import path of the whole process, so that folder is searched for every
    file. A new folder can change what an earlier file reaches, so the walk repeats until the set of launch folders stops growing."""
    base = REPO_ROOT if root is None else root
    start = module_file(entry.split("."), base)
    require(start is not None, f"Cannot bind code: entry module {entry} is not in the repository")
    index = code_index(base, code_directories)
    folders: set[str] = set()
    while True:
        seen, launched = walk(str(start), base, index, folders)
        if launched <= folders:
            return sorted(seen)
        folders |= launched


def closure_hashes(entry: str, root: Path | None = None, code_directories: tuple[str, ...] = CODE_DIRECTORIES) -> dict[str, str]:
    """Repository-relative path to byte SHA-256 for every file in the closure of an entry module."""
    base = REPO_ROOT if root is None else root
    return {name: hashlib.sha256((base / name).read_bytes()).hexdigest() for name in closure(entry, root, code_directories)}
