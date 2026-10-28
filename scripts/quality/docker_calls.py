"""A guard against Docker calls outside the launcher: it refuses every process start it cannot prove is free of Docker (D1 section 2 item 4).

Run from the repository root:

    .venv/bin/python -m scripts.quality.docker_calls                 # every Python and shell file Git tracks or would track
    .venv/bin/python -m scripts.quality.docker_calls path/a.py b.sh  # named files (the pre-commit form)

Only ``orchestration/launcher.py`` changes container, network or volume state, so it is the only file that may start ``docker``, open
the Docker socket or run ``docker compose``. This is a guard, not a proof: a static check of Python cannot see every route a program can
take at run time, so it refuses the routes it cannot prove instead of searching for the ones it knows.

Every call that starts a process (the ``subprocess`` functions, ``os.system``, ``os.popen``, the ``os.exec*``, ``os.spawn*`` and
``os.posix_spawn*`` families, ``pty.spawn``, ``asyncio`` subprocess creation, an event loop's ``subprocess_exec`` and
``subprocess_shell`` and ``scripts.process.run_command``) is a finding unless its program is proved: every value it may hold is a literal
path or name other than ``docker`` or ``docker-compose``, also through ``pathlib.Path``, ``str``, ``/``, ``resolve()``, ``shutil.which`` and
the other passthroughs in ``PASSTHROUGH`` (matched by their qualified definition, never by name alone). A program that runs other code
needs its arguments proved too: a shell (literal commands that never name docker, or a script path), an interpreter such as
``sys.executable`` (``-c`` code that is literal, never names docker and uses no process API; ``-m`` with a literal module; or a script
path) and a trampoline such as ``env`` or ``sudo`` (the program it runs, checked in turn). Names are resolved as Python resolves them
(the innermost scope that binds them); a name declared global or nonlocal is resolved file-wide. Anything else (an attribute, a call
result, a parameter of a lambda, method or nested function) is unproved and a finding.

A top-level function that passes its own parameters to a process start is a wrapper: it is not a finding itself, and every call of it in
any checked file is judged by its arguments, with the argument list of a shell or interpreter program carried through wrappers of
wrappers; relative imports are resolved against their package. A process-starting function or module used other than by a direct call
(saved, passed on, wrapped in ``functools.partial``, read with ``getattr`` by a computed name or imported by a computed name) is a finding,
and so are ``exec``, ``eval``, ``compile``, ``__import__`` with a computed name and any import of ``ctypes``. Any use of the launcher's
Engine API classes (``DockerApi``, ``UnixConnection``, ``EventStream``) or of a ``docker.sock`` path is a finding. In shell files a
non-comment line that runs ``docker`` or ``docker-compose`` (directly, through ``exec``, ``env``, ``command``, ``sudo``, a path, a quoted
``bash -c`` string or a variable that holds the program) or runs ``eval`` is a finding.

Process starts the check cannot prove and the lead has read are listed in ``REVIEWED``; each entry binds the normalized syntax tree of its whole
file and of every repository module that file imports directly (plain, ``from``, star and relative imports; ``ast.dump`` without positions),
its call-site line, its finding class and its exact number of findings. Comments, whitespace, formatting and moved lines leave an entry
current; every change of code, documentation or annotation in those files brings the finding back. When any of those files can read source
text (``__file__``, ``inspect`` and the like, also through an alias of ``import_module``), the whole set is bound by its bytes instead. Three
files are exempt, each for a stated reason (``EXEMPT``). Known limits (issue WV3-006): a ``PATH`` the repository changes, a program symlink the
repository creates, a script the repository writes at run time and then runs, and shell commands assembled from variables are not followed.
The check covers the repository's own code, which is the trust boundary (D1 section 12). Failure mode 918 is in plans/wp/l1a_failure_modes.md.
"""

from __future__ import annotations

import ast
import hashlib
import json
import re
import sys
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path

from scripts.process import run_command

LAUNCHER = "orchestration/launcher.py"
EXEMPT = {
    LAUNCHER: "the launcher is the one place that calls Docker",
    "scripts/process.py": "it defines run_command, which this check treats as a process start at every call",
    "services/hai/deadline_run.py": "the in-container deadline entrypoint runs the job's own command inside a container that has no Docker socket",
}
# Process starts the check cannot prove, read by hand and found not to run Docker, each pinned to the SHA-256 of its stripped source line: an
# edit of that line brings the finding back, and an entry that no longer matches a finding is a finding itself. A direct Docker call, an Engine
# API use or a socket path can never be listed.
REVIEWED = "config/quality/docker_calls_reviewed.json"
REVIEWABLE = ("whose program the check cannot prove", "is used as a value", "imported by a computed name")
# The format of the bound code: a different version, or another Python minor version (its ``ast.dump`` text may differ), fails every entry until the
# entries are written again. The JSON must name it in its ``binding`` field.
# When any file of an entry's bound set can read source text (its own or another module's), the whole set is bound by bytes: a comment is
# then part of what the code can read.
SOURCE_NAMES = frozenset({"__file__", "__spec__", "__loader__", "__cached__"})
SOURCE_ATTRIBUTES = SOURCE_NAMES | {
    "co_filename",
    "origin",
    "get_source",
    "get_data",
    "getsource",
    "getsourcelines",
    "getsourcefile",
    "getfile",
    "getabsfile",
    "findsource",
    "getcomments",
}
SOURCE_MODULES = frozenset({"inspect", "linecache", "importlib.resources", "pkgutil", "tokenize", "traceback", "zipimport"})
# The functions that import a module by name at run time. An alias is any local name that may hold one of them.
IMPORT_FUNCTIONS = frozenset({"__import__", "import_module"})
BINDING = f"ast-3-py{sys.version_info.major}.{sys.version_info.minor}"
PROGRAM_PATH = re.compile(r"^(?:.*/)?docker(?:-compose)?$")
COMMAND_LINE = re.compile(r"^(?:\S*/)?docker(-compose)?(\s|$)")
SHELL_PREFIX = r"(?:^|[\s;&|(`\"'])(?:(?:exec|command|sudo|nohup|time)\s+|env\s+(?:\S+=\S*\s+)*)*"
SHELL_COMMAND = re.compile(SHELL_PREFIX + r"[\"']?(?:\S*/)?docker(?:-compose)?[\"']?(?:\s|$|[;&|)`\"'])")
# A shell command string is Docker-free only when the word docker (or docker-compose) does not occur in it at all.
DOCKER_WORD = re.compile(r"(?<![\w.-])docker(?:-compose)?(?![\w.-])")
SHELL_NAMES = ("sh", "bash", "zsh", "dash", "ksh", "fish", "csh", "tcsh")
ENGINE_API = {"DockerApi", "UnixConnection", "EventStream"}
SOCKET = re.compile(r"docker\.sock")
FIX = f"ask the launcher through its client (orchestration/launcher.py); only {LAUNCHER} may call Docker"

# The process-starting functions: (module, name) -> how the call names its program.
# Roles: "program" (a path or name), "argv" (a list whose first element is the program, or a string program), "shell" (a shell command
# string), "exec" (a path, then the arguments) and "spawn" (a mode, a path, then the arguments).
EXEC_NAMES = {f"exec{suffix}" for suffix in ("l", "le", "lp", "lpe", "v", "ve", "vp", "vpe")}
SPAWN_NAMES = {f"spawn{suffix}" for suffix in ("l", "le", "lp", "lpe", "v", "ve", "vp", "vpe")}
PROCESS: dict[tuple[str, str], str] = {
    **{("subprocess", name): "argv" for name in ("run", "call", "check_call", "check_output", "Popen")},
    ("subprocess", "getoutput"): "shell",
    ("subprocess", "getstatusoutput"): "shell",
    ("os", "system"): "shell",
    ("os", "popen"): "shell",
    ("os", "startfile"): "program",
    ("os", "posix_spawn"): "exec",
    ("os", "posix_spawnp"): "exec",
    **{("os", name): "exec" for name in EXEC_NAMES},
    **{("os", name): "spawn" for name in SPAWN_NAMES},
    ("pty", "spawn"): "argv",
    ("asyncio", "create_subprocess_exec"): "exec",
    ("asyncio", "create_subprocess_shell"): "shell",
    ("asyncio.subprocess", "create_subprocess_exec"): "exec",
    ("asyncio.subprocess", "create_subprocess_shell"): "shell",
    ("scripts.process", "run_command"): "exec",
    ("asyncio.loop", "subprocess_exec"): "loop_exec",
    ("asyncio.loop", "subprocess_shell"): "loop_shell",
}
PROCESS_MODULES = {"subprocess", "pty", "asyncio.subprocess", "scripts.process"}
# Calls that keep naming the program they are given, matched by their qualified definition only (a local function named ``which`` or
# ``Path`` is not one of them).
PASSTHROUGH = {
    "builtins.str",
    "pathlib.Path",
    "pathlib.PurePath",
    "pathlib.PosixPath",
    "pathlib.PurePosixPath",
    "os.fspath",
    "shutil.which",
    "os.path.abspath",
    "os.path.realpath",
    "os.path.expanduser",
    "os.path.normpath",
    "scripts.process.find_program",
}
PATH_JOIN = "os.path.join"
PASSTHROUGH_METHODS = {"resolve", "absolute", "expanduser", "as_posix"}
# Programs that run other code: their arguments decide what runs, so they are checked by kind.
INTERPRETER = re.compile(r"^(?:python(?:\d+(?:\.\d+)*)?|pypy\d*|node|perl|ruby|php|osascript|Rscript|lua|tclsh|wish|awk|gawk)$")
TRAMPOLINES = {"env", "sudo", "doas", "nohup", "time", "nice", "ionice", "timeout", "xargs", "exec", "command", "caffeinate", "stdbuf", "arch"}
CODE_FLAGS = {"-c", "-e", "-E", "-r"}
# Code given to an interpreter is proved only when it is literal, never names docker and uses none of these.
CODE_TOKENS = re.compile(
    r"subprocess|os\.system|os\.popen|os\.exec|os\.spawn|posix_spawn|\bpty\b|ctypes|\bexec\s*\(|\beval\s*\(|\bcompile\s*\(|__import__|importlib"
    r"|multiprocessing|asyncio|run_command|startfile|\bsystem\s*\(|\bpopen\s*\("
)
DYNAMIC_CODE = {"exec", "eval", "compile", "__import__", "breakpoint"}

# Verdicts, ordered: a call is a finding at UNPROVED or DOCKER.
SAFE, SHELL, PARAM, UNPROVED, DOCKER = range(5)


@dataclass(frozen=True)
class Finding:
    """One process start or Docker use outside the launcher that the check cannot clear."""

    location: str
    problem: str


@dataclass
class Verdict:
    """What a program expression may be: its worst case and the enclosing function's parameters it depends on (with their roles)."""

    level: int = SAFE
    params: dict[str, str] = field(default_factory=dict)
    # At SHELL level: which kinds of code-running program it may be ("shell", "python", "interpreter", "trampoline").
    kinds: frozenset[str] = frozenset()

    def join(self, other: Verdict) -> Verdict:
        return Verdict(max(self.level, other.level), {**self.params, **other.params}, self.kinds | other.kinds)


def program_kind(name: str) -> Verdict:
    """A literal program name: a shell, an interpreter or a trampoline needs its arguments checked; any other name is proved."""
    if name in SHELL_NAMES:
        return Verdict(SHELL, kinds=frozenset({"shell"}))
    if INTERPRETER.match(name):
        return Verdict(SHELL, kinds=frozenset({"python" if name.startswith(("python", "pypy")) else "interpreter"}))
    if name in TRAMPOLINES:
        return Verdict(SHELL, kinds=frozenset({"trampoline"}))
    return Verdict(SAFE)


def code_verdict(code: ast.expr | None) -> Verdict:
    """Code given to an interpreter or ``exec``: proved only when literal, without the word docker and without a process API."""
    if not (isinstance(code, ast.Constant) and isinstance(code.value, str | bytes)):
        return Verdict(UNPROVED)
    text = code.value.decode(errors="replace") if isinstance(code.value, bytes) else code.value
    if DOCKER_WORD.search(text):
        return Verdict(DOCKER)
    return Verdict(UNPROVED) if CODE_TOKENS.search(text) else Verdict(SAFE)


def docstring_nodes(tree: ast.AST) -> set[int]:
    """The ``id`` of every string expression statement (documentation), which is not a call and is not checked."""
    return {id(node.value) for node in ast.walk(tree) if isinstance(node, ast.Expr) and isinstance(node.value, ast.Constant)}


def module_name(path: str) -> str:
    """The import name of a repository Python file."""
    return path.removesuffix(".py").removesuffix("/__init__").replace("/", ".")


class FileModel:
    """One parsed Python file: its imports, every value bound to each name and each function's parameters."""

    def __init__(self, path: str, tree: ast.Module) -> None:
        self.path, self.tree, self.module = path, tree, module_name(path)
        self.imports: dict[str, str] = {}  # local name -> qualified module or module.attribute
        self.star_modules: list[str] = []
        self.assigned: dict[str, list[ast.expr]] = {}  # file-wide: used to follow aliases of process functions (the cautious direction)
        self.augmented: dict[str, list[ast.expr]] = {}
        # Per scope (function, lambda, class or module) as Python resolves names: used to read the values a program or argument may hold.
        self.scoped: dict[tuple[int, str], list[ast.expr]] = {}
        self.scoped_appended: set[tuple[int, str]] = set()
        self.free_names: set[str] = set()  # declared global or nonlocal somewhere: resolved file-wide
        self.parent: dict[int, ast.AST] = {}
        self.top_functions = {node.name: node for node in tree.body if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef)}
        self.top_classes = {node.name for node in tree.body if isinstance(node, ast.ClassDef)}
        # Every name the file binds itself, so a builtin such as ``str`` or ``exec`` is trusted only when the file does not redefine it.
        self.defined = {node.name for node in ast.walk(tree) if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef | ast.ClassDef)}
        loops: list[ast.For | ast.AsyncFor | ast.comprehension] = []
        for node in ast.walk(tree):
            for child in ast.iter_child_nodes(node):
                self.parent[id(child)] = node
            if isinstance(node, ast.Import):
                for alias in node.names:
                    self.imports[alias.asname or alias.name.split(".")[0]] = alias.name if alias.asname else alias.name.split(".")[0]
            elif isinstance(node, ast.ImportFrom) and (node.module or node.level):
                source = self.absolute(node)
                for alias in node.names:
                    if alias.name == "*":
                        self.star_modules.append(source)
                    else:
                        self.imports[alias.asname or alias.name] = f"{source}.{alias.name}" if source else alias.name
            elif isinstance(node, ast.Global | ast.Nonlocal):
                self.free_names.update(node.names)
            elif isinstance(node, ast.AugAssign) and isinstance(node.target, ast.Name):
                # ``x += more``: x keeps its earlier values as a prefix; what is appended is not a value of x by itself.
                self.augmented.setdefault(node.target.id, []).append(node.value)
                self.scoped_appended.add((id(self.scope_of(node)), node.target.id))
            elif isinstance(node, ast.Assign | ast.AnnAssign | ast.AugAssign | ast.NamedExpr) and node.value is not None:
                for target in node.targets if isinstance(node, ast.Assign) else [node.target]:
                    for name in ast.walk(target):
                        if isinstance(name, ast.Name):
                            self.bind(node, name.id, [node.value])
            elif isinstance(node, ast.For | ast.AsyncFor | ast.comprehension):
                loops.append(node)
            elif isinstance(node, ast.withitem) and node.optional_vars is not None:
                for name in ast.walk(node.optional_vars):
                    if isinstance(name, ast.Name):
                        self.bind(node, name.id, [node.context_expr])

        self.defined |= set(self.assigned) | set(self.augmented) | set(self.imports)
        unreadable = ast.Call(func=ast.Name(id="unreadable", ctx=ast.Load()), args=[], keywords=[])
        for loop in loops:
            # A loop binds each element of the sequences its iterable may be (a literal, a name bound only to literals, or the items or values
            # of a literal dict); a tuple target takes the elements position by position. Anything else binds a value the check cannot read.
            sequences = self.sequences(loop.iter, frozenset())  # resolved from the loop's own scope
            elements: list[ast.expr] = [element for sequence in sequences for element in sequence] if sequences is not None else [unreadable]
            targets = loop.target.elts if isinstance(loop.target, ast.Tuple | ast.List) else None
            if targets is None:
                for name in ast.walk(loop.target):
                    if isinstance(name, ast.Name):
                        self.bind(loop, name.id, elements)
                continue
            for position, target in enumerate(targets):
                values = [
                    element.elts[position] if isinstance(element, ast.Tuple | ast.List) and len(element.elts) == len(targets) else unreadable
                    for element in elements
                ]
                for name in ast.walk(target):
                    if isinstance(name, ast.Name):
                        self.bind(loop, name.id, values if isinstance(target, ast.Name) else [unreadable])

    def bind(self, node: ast.AST, name: str, values: list[ast.expr]) -> None:
        """Record values bound to a name, file-wide and in the scope of the binding node."""
        self.assigned.setdefault(name, []).extend(values)
        self.scoped.setdefault((id(self.scope_of(node)), name), []).extend(values)

    def scope_of(self, node: ast.AST) -> ast.AST:
        """The innermost function, lambda or class around a node, or the module."""
        current = self.parent.get(id(node))
        while current is not None and not isinstance(current, ast.FunctionDef | ast.AsyncFunctionDef | ast.Lambda | ast.ClassDef):
            current = self.parent.get(id(current))
        return current if current is not None else self.tree

    def values_of(self, node: ast.Name) -> tuple[list[ast.expr] | None, bool]:
        """The values a name may hold where it is read, as Python resolves it: the innermost scope that binds it (class bodies are not
        seen from their methods), then the module. Returns (None, False) for a parameter of an enclosing function, and whether values
        are appended later with ``+=``. A name declared global or nonlocal anywhere is resolved file-wide."""
        if node.id in self.free_names:
            return self.assigned.get(node.id, []), node.id in self.augmented
        scope: ast.AST | None = self.scope_of(node)
        first = True
        while scope is not None:
            if not (isinstance(scope, ast.ClassDef) and not first):
                key = (id(scope), node.id)
                if key in self.scoped or key in self.scoped_appended:
                    return self.scoped.get(key, []), key in self.scoped_appended
                if isinstance(scope, ast.FunctionDef | ast.AsyncFunctionDef | ast.Lambda) and node.id in self.parameters(scope):
                    return None, False
            first = False
            scope = None if scope is self.tree else self.scope_of(scope)
        return [], False

    def absolute(self, node: ast.ImportFrom) -> str:
        """The module an import names, a relative one resolved against this file's package."""
        if not node.level:
            return str(node.module)
        parts = self.module.split(".")
        package = parts if self.path.endswith("__init__.py") else parts[:-1]
        base = package[: len(package) - (node.level - 1)] if node.level > 1 else package
        return ".".join([*base, *([node.module] if node.module else [])])

    def sequences(self, node: ast.expr, seen: frozenset[str]) -> list[list[ast.expr]] | None:
        """Every literal sequence an iterable may be, or None."""
        if isinstance(node, ast.List | ast.Tuple | ast.Set):
            return [list(node.elts)]
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr in ("items", "values") and not node.args:
            dicts = self.dicts(node.func.value, seen)
            if dicts is None:
                return None
            if node.func.attr == "values":
                return [list(d.values) for d in dicts]
            return [[ast.Tuple(elts=[key, value], ctx=ast.Load()) for key, value in zip(d.keys, d.values, strict=True) if key is not None] for d in dicts]
        if isinstance(node, ast.Name) and node.id not in seen:
            values, appended = self.values_of(node)
            if not values or appended:
                return None
            found: list[list[ast.expr]] = []
            for value in values:
                more = self.sequences(value, seen | {node.id})
                if more is None:
                    return None
                found += more
            return found
        return None

    def dicts(self, node: ast.expr, seen: frozenset[str]) -> list[ast.Dict] | None:
        """Every literal dict (with no ``**`` entry) a name or expression may be, or None."""
        if isinstance(node, ast.Dict):
            return None if any(key is None for key in node.keys) else [node]
        if isinstance(node, ast.Name) and node.id not in seen:
            values, appended = self.values_of(node)
            if not values or appended:
                return None
            found: list[ast.Dict] = []
            for value in values:
                more = self.dicts(value, seen | {node.id})
                if more is None:
                    return None
                found += more
            return found
        return None

    def enclosing_function(self, node: ast.AST) -> ast.AST | None:
        """The innermost function or lambda around a node."""
        current = self.parent.get(id(node))
        while current is not None and not isinstance(current, ast.FunctionDef | ast.AsyncFunctionDef | ast.Lambda):
            current = self.parent.get(id(current))
        return current

    @staticmethod
    def parameters(function: ast.AST) -> list[str]:
        """A function's parameter names in order (positional first)."""
        if not isinstance(function, ast.FunctionDef | ast.AsyncFunctionDef | ast.Lambda):
            return []
        arguments = function.args
        names = [a.arg for a in (*arguments.posonlyargs, *arguments.args)]
        names += [a.arg for a in (arguments.vararg, *arguments.kwonlyargs, arguments.kwarg) if a is not None]
        return names


class Checker:
    """The Python check over a set of files: wrapper discovery across all of them, then findings in the named ones."""

    def __init__(self, models: dict[str, FileModel]) -> None:
        self.models = models
        # Wrappers: qualified function name -> {parameter name: role}.
        self.wrappers: dict[str, dict[str, str]] = {}
        # Resolvers: top-level functions that return a program derived only from one parameter (``which(name)``) -> that parameter.
        self.resolvers: dict[str, str] = {}

    # -- callee resolution -----------------------------------------------------------------------------------------

    def qualify(self, model: FileModel, node: ast.expr, seen: frozenset[str] = frozenset()) -> set[str]:
        """Every qualified name an expression may refer to (modules, functions), through imports, aliases and constant getattr."""
        if isinstance(node, ast.Name):
            found: set[str] = set()
            if node.id in model.imports:
                found.add(model.imports[node.id])
            if node.id in model.top_functions or node.id in model.top_classes:
                found.add(f"{model.module}.{node.id}")
            if node.id in ("str",) and node.id not in model.defined:
                found.add(f"builtins.{node.id}")
            for module in model.star_modules:
                found.add(f"{module}.{node.id}")
            if node.id not in seen:
                for value in model.assigned.get(node.id, []):
                    found |= self.qualify(model, value, seen | {node.id})
            return found
        if isinstance(node, ast.Attribute):
            return {f"{base}.{node.attr}" for base in self.qualify(model, node.value, seen)}
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "getattr" and len(node.args) >= 2:
            name = node.args[1]
            if isinstance(name, ast.Constant) and isinstance(name.value, str):
                return {f"{base}.{name.value}" for base in self.qualify(model, node.args[0], seen)}
        imported = self.constant_import(model, node, seen)
        return set() if imported is None else {imported}

    def constant_import(self, model: FileModel, node: ast.expr, seen: frozenset[str] = frozenset()) -> str | None:
        """The module a run-time import names by a literal (``__import__("x")``, ``importlib.import_module("x")``), else None."""
        if not (isinstance(node, ast.Call) and node.args and isinstance(node.args[0], ast.Constant) and isinstance(node.args[0].value, str)):
            return None
        dynamic = isinstance(node.func, ast.Name) and node.func.id == "__import__"
        dynamic = dynamic or "importlib.import_module" in self.qualify(model, node.func, seen)
        return node.args[0].value if dynamic else None

    def role_of(self, qualified: str) -> str | None:
        """The program role of a process-starting function or a wrapper (None for anything else)."""
        module, _, name = qualified.rpartition(".")
        if (module, name) in PROCESS:
            return PROCESS[(module, name)]
        return "wrapper" if qualified in self.wrappers else None

    def process_target(self, qualified: str) -> bool:
        """True for a process-starting module, function or wrapper."""
        return qualified in PROCESS_MODULES or self.role_of(qualified) is not None or qualified == "os"

    # -- program evaluation ----------------------------------------------------------------------------------------

    def program(self, model: FileModel, node: ast.expr, site: ast.AST, seen: frozenset[str] = frozenset()) -> Verdict:
        """Whether an expression naming a program is provably not Docker (SHELL: a shell, whose commands are checked separately)."""
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            if PROGRAM_PATH.match(node.value) or COMMAND_LINE.match(node.value):
                return Verdict(DOCKER)
            return program_kind(Path(node.value).name)
        if isinstance(node, ast.Attribute) and "sys.executable" in self.qualify(model, node):
            return Verdict(SHELL, kinds=frozenset({"python"}))  # the interpreter: what it runs depends on its arguments
        if isinstance(node, ast.Attribute) and node.attr == "__file__" and isinstance(node.value, ast.Name) and node.value.id in model.imports:
            return Verdict(SAFE)  # an imported module's own source file (a repository module is checked itself)
        if isinstance(node, ast.Call):
            func = node.func
            callee = self.qualify(model, func)
            if callee and callee <= PASSTHROUGH and len(node.args) == 1 and not node.keywords:
                return self.program(model, node.args[0], site, seen)
            if callee and callee <= {PATH_JOIN} and node.args:  # os.path.join: the last part names the program
                return self.program(model, node.args[-1], site, seen)
            if isinstance(func, ast.Attribute) and func.attr in PASSTHROUGH_METHODS and not node.args:
                return self.program(model, func.value, site, seen)
            for qualified in self.qualify(model, func):
                if qualified in self.resolvers:
                    function = model_function(self.models, qualified)
                    names = FileModel.parameters(function) if function is not None else []
                    parameter = self.resolvers[qualified]
                    keywords = {keyword.arg: keyword.value for keyword in node.keywords if keyword.arg}
                    index = names.index(parameter) if parameter in names else -1
                    argument = keywords.get(parameter, node.args[index] if 0 <= index < len(node.args) else None)
                    return Verdict(UNPROVED) if argument is None else self.program(model, argument, site, seen)
            return Verdict(UNPROVED)
        if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Div):
            return self.program(model, node.right, site, seen)  # a path join: the last part names the program
        if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
            if isinstance(node.left, ast.Constant) and isinstance(node.right, ast.Constant):
                return self.program(model, ast.Constant(str(node.left.value) + str(node.right.value)), site, seen)
            return Verdict(UNPROVED)
        if isinstance(node, ast.JoinedStr):
            if all(isinstance(part, ast.Constant) for part in node.values):
                return self.program(model, ast.Constant("".join(str(part.value) for part in node.values if isinstance(part, ast.Constant))), site, seen)
            return Verdict(UNPROVED)
        if isinstance(node, ast.IfExp):
            return self.program(model, node.body, site, seen).join(self.program(model, node.orelse, site, seen))
        if isinstance(node, ast.BoolOp):  # ``which(name) or name``: any operand may be the value
            verdict = Verdict(SAFE)
            for value in node.values:
                verdict = verdict.join(self.program(model, value, site, seen))
            return verdict
        if isinstance(node, ast.Name):
            return self.name(model, node, site, seen, "program")
        if isinstance(node, ast.Subscript):
            return self.element(model, node, site, seen)
        return Verdict(UNPROVED)

    def name(self, model: FileModel, node: ast.Name, site: ast.AST, seen: frozenset[str], role: str) -> Verdict:
        """A name: a parameter of the call's own function, or every value bound to it in the file."""
        function = model.enclosing_function(site)
        if node.id in model.parameters(function) if function is not None else False:
            return Verdict(PARAM, {node.id: role})
        if node.id in seen:
            return Verdict(SAFE)  # a cycle adds no new value
        values, _appended = model.values_of(node)
        if not values:
            return Verdict(UNPROVED)  # unbound here, or a parameter of an outer function
        verdict = Verdict(SAFE)
        for value in values:
            verdict = verdict.join(self.evaluate(model, value, site, seen | {node.id}, role))
        return verdict

    def element(self, model: FileModel, node: ast.Subscript, site: ast.AST, seen: frozenset[str]) -> Verdict:
        """An indexed element of a list: the first element of a literal list, or a parameter list used as an argument vector."""
        function = model.enclosing_function(site)
        if isinstance(node.value, ast.Name) and function is not None and node.value.id in model.parameters(function):
            return Verdict(PARAM, {node.value.id: "argv"})
        lists = self.lists(model, node.value, seen)
        if lists is None:
            return Verdict(UNPROVED)
        index = node.slice
        verdict = Verdict(SAFE)
        for items in lists:
            if isinstance(index, ast.Constant) and isinstance(index.value, int) and -len(items) <= index.value < len(items):
                verdict = verdict.join(self.program(model, items[index.value], site, seen))
            else:
                for item in items:  # an index the check cannot evaluate may select any element
                    verdict = verdict.join(self.program(model, item, site, seen))
        return verdict

    def lists(self, model: FileModel, node: ast.expr, seen: frozenset[str]) -> list[list[ast.expr]] | None:
        """Every literal list an expression may be, or None when one of its values is not a literal list."""
        if isinstance(node, ast.List | ast.Tuple):
            # A ``*name`` element whose list can be read is expanded in place; any other is kept (and stays unreadable).
            options: list[list[ast.expr]] = [[]]
            for element in node.elts:
                spread = self.lists(model, element.value, seen) if isinstance(element, ast.Starred) else None
                if isinstance(element, ast.Starred) and isinstance(element.value, ast.Subscript) and isinstance(element.value.slice, ast.Slice):
                    spread = self.items(model, element.value)
                if spread is None or len(options) * len(spread) > 64:
                    options = [[*items, element] for items in options]
                else:
                    options = [[*items, *more] for items in options for more in spread]
            return options
        if isinstance(node, ast.Name) and node.id not in seen:
            values, appended = model.values_of(node)
            if not values:
                return None
            found: list[list[ast.expr]] = []
            for value in values:
                more = self.lists(model, value, seen | {node.id})
                if more is None:
                    return None
                found += more
            if appended:  # appended later: the known prefix stays, the tail is unknown
                tail = ast.Starred(value=ast.Name(id="appended", ctx=ast.Load()), ctx=ast.Load())
                found = [[*items, tail] for items in found]
            return found
        if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
            left, right = self.lists(model, node.left, seen), self.lists(model, node.right, seen)
            if left is None or right is None:
                return None
            return [a + b for a in left for b in right]
        return None

    def evaluate(self, model: FileModel, node: ast.expr, site: ast.AST, seen: frozenset[str], role: str) -> Verdict:
        """An argument in a role: a program, an argument vector, a shell command string or a shell's argument list."""
        if role == "program":
            return self.program(model, node, site, seen)
        if role == "shell":
            return self.shell_command(model, node, site, seen)
        if role == "shell_args":
            return self.shell_arguments(model, node, site, seen)
        if role.startswith("args_kinds:"):  # the argument list of a code-running program the wrapper fixes
            kinds = frozenset(role.partition(":")[2].split(","))
            return self.with_arguments(model, Verdict(SHELL, kinds=kinds), self.items(model, node), site, seen)
        if role == "module":  # a module run with ``python -m``: a literal name (repository modules are checked themselves)
            if isinstance(node, ast.Constant) and isinstance(node.value, str):
                return Verdict(SAFE)
            return self.name(model, node, site, seen, "module") if isinstance(node, ast.Name) else Verdict(UNPROVED)
        # "argv": a list whose first element is the program, or a single string program
        if isinstance(node, ast.Starred):
            return self.evaluate(model, node.value, site, seen, "argv")
        if isinstance(node, ast.Name):
            function = model.enclosing_function(site)
            if function is not None and node.id in model.parameters(function):
                return Verdict(PARAM, {node.id: "argv"})
        lists = self.lists(model, node, seen)
        if lists is None:
            if isinstance(node, ast.Name) and node.id not in seen:
                verdict = Verdict(SAFE)
                for value in model.values_of(node)[0] or [node]:
                    verdict = verdict.join(Verdict(UNPROVED) if value is node else self.evaluate(model, value, site, seen | {node.id}, "argv"))
                return verdict
            return self.program(model, node, site, seen)
        verdict = Verdict(SAFE)
        for items in lists:
            if not items or isinstance(items[0], ast.Starred):
                verdict = verdict.join(Verdict(UNPROVED))
                continue
            verdict = verdict.join(self.with_arguments(model, self.program(model, items[0], site, seen), [items[1:]], site, seen))
        return verdict

    def with_arguments(self, model: FileModel, program: Verdict, options: list[list[ast.expr]] | None, site: ast.AST, seen: frozenset[str]) -> Verdict:
        """A program verdict completed by its arguments: a shell, an interpreter or a trampoline is proved only by what its arguments run.
        ``options`` holds every argument list the call may pass; None when one of them cannot be read."""
        if program.level != SHELL:
            return program
        verdict = Verdict(SAFE, program.params)
        for kind in program.kinds or frozenset({"shell"}):
            if options is None or not options:
                verdict = verdict.join(Verdict(UNPROVED))
                continue
            for items in options:
                verdict = verdict.join(self.arguments_of(model, kind, items, site, seen))
        return verdict

    def code(self, model: FileModel, node: ast.expr | None, seen: frozenset[str]) -> Verdict:
        """Code given to an interpreter: a literal, or a name bound only to literals, checked by ``code_verdict``."""
        if isinstance(node, ast.Name) and node.id not in seen:
            values, appended = model.values_of(node)
            if not values or appended:
                return Verdict(UNPROVED)
            verdict = Verdict(SAFE)
            for value in values:
                verdict = verdict.join(self.code(model, value, seen | {node.id}))
            return verdict
        return code_verdict(node)

    def arguments_of(self, model: FileModel, kind: str, items: list[ast.expr], site: ast.AST, seen: frozenset[str]) -> Verdict:
        """Whether the arguments of one kind of code-running program are proved free of Docker."""
        if kind == "shell":
            # A script path first (checked as a program; repository scripts are checked by the shell rules), then literal arguments only.
            if items and not (isinstance(items[0], ast.Constant) and str(items[0].value).startswith("-")) and not isinstance(items[0], ast.Starred):
                script = self.program(model, items[0], site, seen)
                rest = Verdict(SAFE)
                for item in items[1:]:
                    rest = rest.join(self.shell_command(model, item, site, seen))
                return (Verdict(UNPROVED) if script.level >= SHELL else script).join(rest)
            verdict = Verdict(SAFE)
            for item in items:
                verdict = verdict.join(self.shell_command(model, item, site, seen))
            return verdict
        if kind == "trampoline":
            # Options and NAME=value settings are skipped; the first other argument is the program the trampoline runs.
            for position, item in enumerate(items):
                head = item.values[0] if isinstance(item, ast.JoinedStr) and item.values else None
                if isinstance(head, ast.Constant) and isinstance(head.value, str) and re.match(r"^[A-Za-z_]\w*=", head.value):
                    continue  # f"NAME={value}": a setting, whatever its value
                if not isinstance(item, ast.Constant) or not isinstance(item.value, str):
                    return Verdict(UNPROVED)
                if item.value.startswith("-") or "=" in item.value or re.fullmatch(r"\d+[smhd]?", item.value):
                    continue
                return self.with_arguments(model, self.program(model, item, site, seen), [items[position + 1 :]], site, seen)
            return Verdict(SAFE)
        # An interpreter: code given by a flag must be literal safe code; ``-m`` names a module (repository modules are checked themselves);
        # otherwise the first other argument is a script path.
        for position, item in enumerate(items):
            if isinstance(item, ast.Starred):
                return Verdict(UNPROVED)
            if not isinstance(item, ast.Constant) or not isinstance(item.value, str):
                # Not an option: the script path the interpreter runs (a repository script is checked itself).
                script = self.program(model, item, site, seen)
                return Verdict(UNPROVED) if script.level >= SHELL else script
            if item.value in CODE_FLAGS:
                code = items[position + 1] if position + 1 < len(items) else None
                return self.code(model, code, seen)
            if item.value == "-m" and kind == "python":
                module = items[position + 1] if position + 1 < len(items) else None
                if isinstance(module, ast.Constant) and isinstance(module.value, str):
                    return Verdict(SAFE)
                return self.evaluate(model, module, site, seen, "module") if module is not None else Verdict(UNPROVED)
            if item.value.startswith("-"):
                continue
            script = self.program(model, item, site, seen)
            return Verdict(UNPROVED) if script.level >= SHELL else script
        return Verdict(SAFE)

    def shell_command(self, model: FileModel, node: ast.expr, site: ast.AST, seen: frozenset[str]) -> Verdict:
        """A shell command string: proved only when it is literal and never names docker."""
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            return Verdict(DOCKER if DOCKER_WORD.search(node.value) else SAFE)
        if isinstance(node, ast.Name):
            return self.name(model, node, site, seen, "shell")
        if isinstance(node, ast.List | ast.Tuple):  # a list with shell=True: its first element is the command string
            return self.shell_command(model, node.elts[0], site, seen) if node.elts else Verdict(SAFE)
        return Verdict(UNPROVED)

    def shell_arguments(self, model: FileModel, node: ast.expr, site: ast.AST, seen: frozenset[str]) -> Verdict:
        """A shell's argument list: every element must be a literal that never names docker (a script path or ``-c`` command alike)."""
        if isinstance(node, ast.Name):
            function = model.enclosing_function(site)
            if function is not None and node.id in model.parameters(function):
                return Verdict(PARAM, {node.id: "shell_args"})
        lists = self.lists(model, node, seen)
        if lists is None:
            return Verdict(UNPROVED)
        verdict = Verdict(SAFE)
        for item in (item for items in lists for item in items):
            verdict = verdict.join(self.shell_command(model, item, site, seen))
        return verdict

    # -- process starts ----------------------------------------------------------------------------------------------

    def start_verdict(self, model: FileModel, node: ast.Call, qualified: str) -> Verdict:
        """Whether one process start is proved free of Docker."""
        role = self.role_of(qualified)
        keywords = {str(keyword.arg): keyword.value for keyword in node.keywords if keyword.arg}
        positional = list(node.args)
        if any(isinstance(argument, ast.Starred) for argument in positional[:2]) and role != "argv":
            return self.evaluate(model, next(a for a in positional if isinstance(a, ast.Starred)), node, frozenset(), "argv")
        if role == "wrapper":
            function = model_function(self.models, qualified)
            if function is None:
                return Verdict(UNPROVED)
            names = FileModel.parameters(function)

            def argument_for(parameter: str) -> ast.expr | None:
                index = names.index(parameter) if parameter in names else -1
                return keywords.get(parameter, positional[index] if 0 <= index < len(positional) else None)

            verdict = Verdict(SAFE)
            for parameter, parameter_role in self.wrappers[qualified].items():
                argument = argument_for(parameter)
                if argument is None:
                    verdict = verdict.join(Verdict(UNPROVED))
                elif parameter_role.startswith("program"):
                    verdict = verdict.join(self.wrapped_program(model, node, argument, parameter_role, argument_for))
                else:
                    verdict = verdict.join(self.evaluate(model, argument, node, frozenset(), parameter_role))
            return verdict
        if role in ("shell", "loop_shell"):
            index = 1 if role == "loop_shell" else 0
            argument = positional[index] if len(positional) > index else keywords.get("cmd", keywords.get("command"))
            return Verdict(UNPROVED) if argument is None else self.shell_command(model, argument, node, frozenset())
        if role == "argv":
            argument = positional[0] if positional else keywords.get("args", keywords.get("argv"))
            if argument is None:
                return Verdict(UNPROVED)
            shell = keywords.get("shell")
            if shell is not None and not (isinstance(shell, ast.Constant) and shell.value is False):
                return self.shell_command(model, argument, node, frozenset())
            verdict = self.evaluate(model, argument, node, frozenset(), "argv")
            executable = keywords.get("executable")
            return verdict if executable is None else verdict.join(self.program(model, executable, node, frozenset()))
        offset = 1 if role in ("spawn", "loop_exec") else 0
        program = positional[offset] if len(positional) > offset else keywords.get("program", keywords.get("path", keywords.get("file")))
        if program is None:
            return Verdict(UNPROVED)
        verdict = self.program(model, program, node, frozenset())
        arguments = self.argument_expression(qualified, positional, keywords, offset)
        return self.program_with_arguments(model, node, verdict, arguments)

    @staticmethod
    def argument_expression(qualified: str, positional: list[ast.expr], keywords: dict[str, ast.expr], offset: int) -> ast.expr:
        """The expression holding a call's arguments after its program (``argv[0]`` dropped for the ``os`` families)."""
        name = qualified.rpartition(".")[2]
        if qualified == "scripts.process.run_command":
            return positional[1] if len(positional) > 1 else keywords.get("args", ast.List(elts=[], ctx=ast.Load()))
        if name.startswith(("execv", "spawnv", "posix_spawn")):
            vector = positional[offset + 1] if len(positional) > offset + 1 else keywords.get("argv", keywords.get("args"))
            if vector is None:
                return ast.List(elts=[], ctx=ast.Load())
            return ast.Subscript(value=vector, slice=ast.Slice(lower=ast.Constant(1)), ctx=ast.Load())
        if name.startswith(("execl", "spawnl")):
            return ast.List(elts=positional[offset + 2 :], ctx=ast.Load())
        return ast.List(elts=positional[offset + 1 :], ctx=ast.Load())

    def program_with_arguments(self, model: FileModel, site: ast.AST, verdict: Verdict, arguments: ast.expr) -> Verdict:
        """A start's program verdict completed by its arguments; a program that is a parameter records how its arguments are given."""
        if verdict.level == PARAM and set(verdict.params.values()) == {"program"}:
            (parameter,) = verdict.params
            function = model.enclosing_function(site)
            held = arguments.value if isinstance(arguments, ast.Subscript) else arguments
            if isinstance(held, ast.Name) and function is not None and held.id in model.parameters(function):
                return Verdict(PARAM, {parameter: f"program_with:{held.id}"})
            fixed = self.with_arguments(
                model, Verdict(SHELL, kinds=frozenset({"shell", "python", "trampoline"})), self.items(model, arguments), site, frozenset()
            )
            return Verdict(PARAM, {parameter: "program_fixed" if fixed.level == SAFE else "program"})
        if verdict.level == PARAM and "argv" in verdict.params.values():
            return verdict  # ``run(argv[0], argv[1:])``: the whole vector is judged at each call site
        function = model.enclosing_function(site)
        if verdict.level == SHELL and isinstance(arguments, ast.Name) and function is not None and arguments.id in model.parameters(function):
            return Verdict(PARAM, {**verdict.params, arguments.id: "args_kinds:" + ",".join(sorted(verdict.kinds))})
        return self.with_arguments(model, verdict, self.items(model, arguments), site, frozenset())

    def items(self, model: FileModel, arguments: ast.expr) -> list[list[ast.expr]] | None:
        """Every argument list an expression may be (a slice of each included), or None when one of them cannot be read."""
        if isinstance(arguments, ast.Subscript) and isinstance(arguments.slice, ast.Slice):
            lists = self.lists(model, arguments.value, frozenset())
            lower, upper = arguments.slice.lower, arguments.slice.upper
            start = lower.value if isinstance(lower, ast.Constant) and isinstance(lower.value, int) else 0 if lower is None else None
            stop = upper.value if isinstance(upper, ast.Constant) and isinstance(upper.value, int) and upper.value >= 0 else None
            if lists is None or start is None or (upper is not None and stop is None) or arguments.slice.step is not None:
                return None
            return [items[start:stop] for items in lists]  # an unread ``*`` element inside the slice stays unread
        return self.lists(model, arguments, frozenset())

    def wrapped_program(self, model: FileModel, site: ast.Call, argument: ast.expr, role: str, argument_for: Callable[[str], ast.expr | None]) -> Verdict:
        """A wrapper's program parameter at one call: the program given, completed by the arguments the wrapper passes with it."""
        verdict = self.program(model, argument, site, frozenset())
        if role == "program_fixed":
            return Verdict(SAFE, verdict.params) if verdict.level == SHELL else self.relay(verdict, "program_fixed")
        if role == "program":
            return Verdict(UNPROVED) if verdict.level == SHELL else verdict
        held = argument_for(role.partition(":")[2])
        if held is None:
            return Verdict(UNPROVED)
        if verdict.level == PARAM and set(verdict.params.values()) == {"program"}:
            return self.program_with_arguments(model, site, verdict, held)
        return self.with_arguments(model, verdict, self.items(model, held), site, frozenset())

    @staticmethod
    def relay(verdict: Verdict, role: str) -> Verdict:
        """A wrapper of a wrapper passes its own program parameter on with the same argument role."""
        if verdict.level == PARAM and set(verdict.params.values()) == {"program"}:
            return Verdict(PARAM, dict.fromkeys(verdict.params, role))
        return verdict

    def starts(self, model: FileModel) -> list[tuple[ast.Call, str, Verdict]]:
        """Every process start in a file with its verdict."""
        found = []
        for node in ast.walk(model.tree):
            if isinstance(node, ast.Call):
                targets = [name for name in self.qualify(model, node.func) if self.role_of(name) is not None]
                if isinstance(node.func, ast.Attribute) and node.func.attr in ("subprocess_exec", "subprocess_shell"):
                    targets.append(f"asyncio.loop.{node.func.attr}")  # an event loop's process API, whatever object holds the loop
                for qualified in targets:
                    verdict = self.start_verdict(model, node, qualified)
                    found.append((node, qualified, Verdict(UNPROVED) if verdict.level == SHELL else verdict))
        return found

    def discover_resolvers(self) -> None:
        """Top-level functions whose every return value is a program derived from one parameter, across every file, to a fixed point."""
        changed = True
        while changed:
            changed = False
            for model in self.models.values():
                for name, function in model.top_functions.items():
                    qualified = f"{model.module}.{name}"
                    if qualified in self.resolvers:
                        continue
                    returns = [node for node in ast.walk(function) if isinstance(node, ast.Return) and model.enclosing_function(node) is function]
                    values = [(node, node.value) for node in returns if node.value is not None]
                    if not returns or len(values) != len(returns):
                        continue
                    verdict = Verdict(SAFE)
                    for node, value in values:
                        verdict = verdict.join(self.program(model, value, node))
                    if verdict.level == PARAM and len(verdict.params) == 1:
                        self.resolvers[qualified] = next(iter(verdict.params))
                        changed = True

    def discover_wrappers(self) -> None:
        """Top-level functions that pass their own parameters to a process start, across every file, to a fixed point."""
        changed = True
        while changed:
            changed = False
            for model in self.models.values():
                for node, _qualified, verdict in self.starts(model):
                    if verdict.level != PARAM:
                        continue
                    function = model.enclosing_function(node)
                    if not isinstance(function, ast.FunctionDef | ast.AsyncFunctionDef) or model.top_functions.get(function.name) is not function:
                        continue
                    name = f"{model.module}.{function.name}"
                    merged = {**self.wrappers.get(name, {}), **verdict.params}
                    if merged != self.wrappers.get(name):
                        self.wrappers[name] = merged
                        changed = True

    # -- findings ----------------------------------------------------------------------------------------------------

    def findings(self, model: FileModel) -> list[Finding]:
        """The findings of one file."""
        path, documentation = model.path, docstring_nodes(model.tree)
        found: list[Finding] = []
        callees = {id(node.func) for node in ast.walk(model.tree) if isinstance(node, ast.Call)}
        for node in ast.walk(model.tree):
            if isinstance(node, ast.Name | ast.Attribute) and (node.id if isinstance(node, ast.Name) else node.attr) in ENGINE_API:
                found.append(Finding(f"{path}:{node.lineno}", "the Docker Engine API is used"))
            elif isinstance(node, ast.alias) and node.name.split(".")[-1] in ENGINE_API:
                found.append(Finding(f"{path}:{getattr(node, 'lineno', 1)}", "the Docker Engine API is imported"))
            elif isinstance(node, ast.Constant) and isinstance(node.value, str) and id(node) not in documentation and SOCKET.search(node.value):
                found.append(Finding(f"{path}:{node.lineno}", "the Docker socket is named"))
            elif isinstance(node, ast.Import | ast.ImportFrom) and any(
                (alias.name if isinstance(node, ast.Import) else str(node.module or "")).split(".")[0] == "ctypes" for alias in node.names
            ):
                found.append(Finding(f"{path}:{node.lineno}", "ctypes is imported; it can call the C library's process functions"))
            elif (
                isinstance(node, ast.Name)
                and node.id in DYNAMIC_CODE
                and node.id not in model.defined
                and isinstance(node.ctx, ast.Load)
                and not (node.id == "__import__" and self.literal_import_call(model, node))
            ):
                what = "called" if id(node) in callees else "used as a value"
                found.append(Finding(f"{path}:{node.lineno}", f"{node.id} is {what}: it runs code the check cannot follow"))
            elif isinstance(node, ast.Call) and self.computed_import(model, node):
                found.append(Finding(f"{path}:{node.lineno}", "a module is imported by a computed name at run time, so its process starts cannot be followed"))
            elif isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "getattr" and len(node.args) >= 2:
                computed = not (isinstance(node.args[1], ast.Constant) and isinstance(node.args[1].value, str))
                if computed and any(self.process_target(q) for q in self.qualify(model, node.args[0])):
                    found.append(Finding(f"{path}:{node.lineno}", "getattr reads a process-starting module by a computed name"))
            elif isinstance(node, ast.Name | ast.Attribute) and id(node) not in callees and self.escapes(model, node):
                found.append(Finding(f"{path}:{node.lineno}", "a process-starting function or module is used as a value, so its calls cannot be followed"))
        for node in ast.walk(model.tree):
            # Any call given the docker program as a literal (a lookup such as ``shutil.which("docker")`` included) is a finding too.
            if isinstance(node, ast.Call) and any(
                isinstance(argument, ast.Constant) and isinstance(argument.value, str) and PROGRAM_PATH.match(argument.value)
                for argument in [*node.args[:2], *(keyword.value for keyword in node.keywords)]
            ):
                found.append(Finding(f"{path}:{node.lineno}", "a call is given the docker program"))
        for node, qualified, verdict in self.starts(model):
            if verdict.level == DOCKER:
                found.append(Finding(f"{path}:{node.lineno}", "a call starts the docker program"))
            elif verdict.level in (UNPROVED, PARAM) and not self.is_wrapper_site(model, node, verdict):
                found.append(Finding(f"{path}:{node.lineno}", f"a process start ({qualified}) whose program the check cannot prove is not Docker"))
        starting = {finding.location for finding in found if finding.problem == "a call starts the docker program"}
        found = [finding for finding in found if not (finding.problem == "a call is given the docker program" and finding.location in starting)]
        return sorted(set(found), key=lambda finding: (int(finding.location.rsplit(":", 1)[1]), finding.problem))

    def literal_import_call(self, model: FileModel, node: ast.Name) -> bool:
        """True when ``__import__`` is called with a literal module name (followed like an import statement)."""
        parent = model.parent.get(id(node))
        return isinstance(parent, ast.Call) and parent.func is node and self.constant_import(model, parent) is not None

    def computed_import(self, model: FileModel, node: ast.Call) -> bool:
        """A run-time import whose module name is not a literal (a literal one is followed like an import statement)."""
        dynamic = isinstance(node.func, ast.Name) and node.func.id == "__import__"
        dynamic = dynamic or "importlib.import_module" in self.qualify(model, node.func)
        return dynamic and self.constant_import(model, node) is None

    def escapes(self, model: FileModel, node: ast.Name | ast.Attribute) -> bool:
        """A process-starting function or module referenced other than as a direct call or as the base of an attribute read."""
        parent = model.parent.get(id(node))
        if isinstance(parent, ast.Attribute) and parent.value is node:
            return False  # subprocess.run(...) or os.environ: the attribute itself is judged
        if isinstance(node, ast.Name) and not isinstance(node.ctx, ast.Load):
            return False
        if isinstance(parent, ast.Call) and isinstance(parent.func, ast.Name) and parent.func.id == "getattr" and parent.args and parent.args[0] is node:
            return False  # judged at the getattr call
        return any(self.process_target(name) for name in self.qualify(model, node) if name.partition(".")[0] != "os" or name.count(".") == 1)

    def is_wrapper_site(self, model: FileModel, node: ast.Call, verdict: Verdict) -> bool:
        """True when an unproved start depends only on the parameters of a top-level function that is a registered wrapper."""
        if verdict.level != PARAM:
            return False
        function = model.enclosing_function(node)
        if not isinstance(function, ast.FunctionDef | ast.AsyncFunctionDef) or model.top_functions.get(function.name) is not function:
            return False
        return f"{model.module}.{function.name}" in self.wrappers


def model_function(models: dict[str, FileModel], qualified: str) -> ast.FunctionDef | ast.AsyncFunctionDef | None:
    """The definition of a top-level function by its qualified name."""
    module, _, name = qualified.rpartition(".")
    for model in models.values():
        if model.module == module and name in model.top_functions:
            function = model.top_functions[name]
            return function if isinstance(function, ast.FunctionDef | ast.AsyncFunctionDef) else None
    return None


def shell_findings(path: str, text: str) -> list[Finding]:
    """Non-comment lines of a shell file that run docker or docker-compose, also through a variable that holds the program."""
    assignment = re.compile(r"^\s*(?:export\s+|readonly\s+|local\s+)?([A-Za-z_]\w*)=[\"']?(?:\S*/)?docker(?:-compose)?[\"']?\s*$")
    held = {match.group(1) for line in text.splitlines() if (match := assignment.match(line))}
    findings = []
    for number, line in enumerate(text.splitlines(), 1):
        code = line.strip()
        if not code or code.startswith("#"):
            continue
        by_variable = any(re.search(SHELL_PREFIX + r"[\"']?\$\{?" + re.escape(name) + r"\}?[\"']?(?:\s|$)", code) for name in held)
        if SHELL_COMMAND.search(code) or by_variable:
            findings.append(Finding(f"{path}:{number}", "a shell line runs docker"))
        elif re.search(SHELL_PREFIX + r"eval(?:\s|$)", code):
            findings.append(Finding(f"{path}:{number}", "a shell line runs eval, whose command the check cannot read"))
    return findings


def parse(root: Path, paths: list[str]) -> dict[str, FileModel]:
    """Parse the Python files among the paths; an unparsable file is skipped (the Python checks report it)."""
    models = {}
    for relative in paths:
        if relative.endswith(".py") and (root / relative).is_file():
            try:
                models[relative] = FileModel(relative, ast.parse((root / relative).read_text(errors="replace")))
            except SyntaxError:
                continue
    return models


def digest(text: str) -> str:
    """A binding digest, written ``sha256:<hex>``: a bare digest beside a path that contains ``api`` reads as an API key to the secret scan."""
    return "sha256:" + hashlib.sha256(text.encode()).hexdigest()


def dump_tree(source: bytes) -> str | None:
    """The normalized syntax tree of Python source (no positions, so comments, whitespace and line moves do not count), or None when it does not parse."""
    try:
        return ast.dump(ast.parse(source), include_attributes=False)
    except (SyntaxError, ValueError, RecursionError):
        return None


def is_source_module(name: str) -> bool:
    """True for a module that reads source text, or a submodule of one."""
    return any(name == module or name.startswith(f"{module}.") for module in SOURCE_MODULES)


def mentions_import(node: ast.AST, aliases: frozenset[str]) -> bool:
    """True when a name, an attribute or a string inside the node is a run-time import function or an alias of one."""
    return any(
        (isinstance(part, ast.Name) and part.id in aliases)
        or (isinstance(part, ast.Attribute) and part.attr in IMPORT_FUNCTIONS)
        or (isinstance(part, ast.Constant) and part.value in IMPORT_FUNCTIONS)
        for part in ast.walk(node)
    )


def is_import_call(call: ast.Call, aliases: frozenset[str]) -> bool:
    """True when a call may be ``__import__(...)`` or ``importlib.import_module(...)`` under any name in ``aliases``."""
    func = call.func
    if isinstance(func, ast.Name):
        return func.id in aliases
    if isinstance(func, ast.Attribute):
        return func.attr in aliases
    return isinstance(func, ast.Call) and mentions_import(func, aliases)


def import_aliases(tree: ast.AST) -> frozenset[str]:
    """The local names that may hold ``__import__`` or ``importlib.import_module``: both names, each ``from m import import_module as x`` and every
    name assigned a value that mentions one of them (``load = importlib.import_module``, ``getattr(importlib, "import_module")``). The result
    of an import call is a module, not an alias. The scan repeats until no name is added."""
    aliases = set(IMPORT_FUNCTIONS)
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            aliases.update(alias.asname or alias.name for alias in node.names if alias.name in IMPORT_FUNCTIONS)
    grown = True
    while grown:
        grown = False
        for node in ast.walk(tree):
            if not (isinstance(node, ast.Assign | ast.AnnAssign | ast.NamedExpr) and node.value is not None):
                continue
            value = node.value
            if (isinstance(value, ast.Call) and is_import_call(value, frozenset(aliases))) or not mentions_import(value, frozenset(aliases)):
                continue
            targets = node.targets if isinstance(node, ast.Assign) else [node.target]
            names = {name.id for target in targets for name in ast.walk(target) if isinstance(name, ast.Name)}
            if not names <= aliases:
                aliases |= names
                grown = True
    return frozenset(aliases)


def call_argument(call: ast.Call, position: int, keyword: str) -> ast.expr | None:
    """The argument at a position or under a keyword (None when the call gives neither, or a ``*`` argument may hide it)."""
    if any(isinstance(argument, ast.Starred) for argument in call.args[: position + 1]):
        return None
    if len(call.args) > position:
        return call.args[position]
    return next((item.value for item in call.keywords if item.arg == keyword), None)


def literal_string(node: ast.expr | None) -> str | None:
    """The value of a string literal, else None."""
    return node.value if isinstance(node, ast.Constant) and isinstance(node.value, str) else None


def source_node(node: ast.AST, aliases: frozenset[str]) -> bool:
    """True for a name, attribute, import, run-time import (under any alias) or ``argv[0]`` that can lead to source text. A bare ``__file__`` counts:
    it is the way to the file's own text, and a path built from it reaches the text of other files."""
    if isinstance(node, ast.Name):
        return node.id in SOURCE_NAMES
    if isinstance(node, ast.Attribute):
        return node.attr in SOURCE_ATTRIBUTES
    if isinstance(node, ast.Import):
        return any(is_source_module(alias.name) for alias in node.names)
    if isinstance(node, ast.ImportFrom):
        return node.level == 0 and any(is_source_module(name) for name in [node.module or "", *(f"{node.module}.{alias.name}" for alias in node.names)])
    if isinstance(node, ast.Call):
        leaf = node.func.id if isinstance(node.func, ast.Name) else node.func.attr if isinstance(node.func, ast.Attribute) else ""
        first = call_argument(node, 0, "name")
        return (leaf == "find_spec" or is_import_call(node, aliases)) and isinstance(first, ast.Constant) and is_source_module(str(first.value))
    if isinstance(node, ast.Subscript):
        base = node.value.attr if isinstance(node.value, ast.Attribute) else node.value.id if isinstance(node.value, ast.Name) else ""
        return base == "argv" and isinstance(node.slice, ast.Constant) and node.slice.value == 0
    return False


def reads_source(source: bytes) -> bool:
    """True when Python source may read source text: ``__file__``, ``inspect``, ``linecache``, ``importlib.resources`` and the like. Conservative:
    the test is on names, attributes and imports, never on how they are used."""
    try:
        tree = ast.parse(source)
    except (SyntaxError, ValueError, RecursionError):
        return False
    aliases = import_aliases(tree)
    return any(source_node(node, aliases) for node in ast.walk(tree))


def bytes_digest(source: bytes) -> str:
    """The digest of a file's bytes."""
    return "bytes:" + hashlib.sha256(source).hexdigest()


def tree_digest(source: bytes) -> str:
    """The digest of a module's normalized tree; of its bytes when it does not parse."""
    text = dump_tree(source)
    return digest(text) if text is not None else bytes_digest(source)


def statement_imports(model: FileModel) -> list[str]:
    """The names of every import statement in a file, at any depth: the full dotted name of ``import a.b.c``, the module and the module.name of
    ``from m import n`` (``n`` may be a submodule), the module of a star import. Relative imports are resolved against the file's package."""
    names: list[str] = []
    for node in ast.walk(model.tree):
        if isinstance(node, ast.Import):
            names += [alias.name for alias in node.names]
        elif isinstance(node, ast.ImportFrom):
            source = model.absolute(node) if node.module or node.level else ""
            names += [source] if source else []
            names += [f"{source}.{alias.name}" if source else alias.name for alias in node.names if alias.name != "*"]
    return names


def file_package(model: FileModel) -> str:
    """The package a file's relative names resolve against (a package ``__init__`` is its own package)."""
    return model.module if model.path.endswith("__init__.py") else model.module.rpartition(".")[0]


def runtime_import_names(model: FileModel, name: str, package: str | None, level: ast.expr | None) -> list[str]:
    """The module names a literal run-time import may mean. A relative name resolves against the literal package argument, else against the
    file's own package; with no literal package the bare name is also tried (more files, never fewer). Python's rule: with ``n`` leading dots,
    drop ``n - 1`` trailing parts of the package. ``__import__`` takes its depth from ``level`` (a depth the check cannot read counts as 1)."""
    dots = len(name) - len(name.lstrip("."))
    rest = name[dots:]
    if dots:
        depth = dots
    elif level is None or (isinstance(level, ast.Constant) and level.value == 0):
        return [name]
    else:
        depth = level.value if isinstance(level, ast.Constant) and isinstance(level.value, int) else 1
    base = (package if package is not None else file_package(model)).split(".")
    kept = [part for part in base[: max(len(base) - (depth - 1), 0)] if part]
    resolved = ".".join([*kept, *([rest] if rest else [])])
    return [item for item in dict.fromkeys([resolved, *([rest] if package is None or not kept else [])]) if item]


def literal_imports(model: FileModel) -> list[str]:
    """The modules a file imports at run time by a literal name: ``__import__("x")`` and ``importlib.import_module("x")`` under any alias, with the
    literal package argument and the relative depth resolved (``import_module(".helpers", "pkg")`` is ``pkg.helpers``)."""
    aliases = import_aliases(model.tree)
    names: list[str] = []
    for node in ast.walk(model.tree):
        if not (isinstance(node, ast.Call) and is_import_call(node, aliases)):
            continue
        name = literal_string(call_argument(node, 0, "name"))
        if name is not None:
            names += runtime_import_names(model, name, literal_string(call_argument(node, 1, "package")), call_argument(node, 4, "level"))
    return names


def import_names(model: FileModel) -> list[str]:
    """Every module name a file imports, by statement or by a literal run-time import."""
    return [*statement_imports(model), *literal_imports(model)]


def module_candidates(name: str, base: str) -> list[str]:
    """The files a dotted module name may be, from the folder ``base`` ("" is the repository root): for each prefix, the module file and the
    package ``__init__``, longest prefix first."""
    parts = name.split(".")
    found = []
    for size in range(len(parts), 0, -1):
        stem = Path(base, *parts[:size])
        found += [f"{stem}.py", f"{stem}/__init__.py"]
    return found


def import_files(root: Path, model: FileModel) -> list[str]:
    """The repository files a file imports directly: for each imported name every module and package ``__init__`` on its path, looked up from the
    repository root and from the folder of the file (a script run from its folder imports its neighbours)."""
    found: set[str] = set()
    folder = str(Path(model.path).parent) if "/" in model.path else ""
    for name in import_names(model):
        for base in dict.fromkeys(["", folder]):
            found.update(candidate for candidate in module_candidates(name, base) if (root / candidate).is_file())
    found.discard(model.path)
    return sorted(found)


def bound_paths(root: Path, model: FileModel) -> list[str]:
    """The files an entry binds: the file itself and the repository modules it imports."""
    return [model.path, *import_files(root, model)]


def set_reads_source(root: Path, paths: list[str], cache: dict[str, str]) -> bool:
    """True when any file of a bound set (the entry, listed first, or an imported file) can read source text, so that every file of the set counts by
    its bytes. ``cache`` holds the answer per file for one run."""
    for path in paths:
        key = f"reads\t{path}"
        if key not in cache:
            cache[key] = "yes" if reads_source((root / path).read_bytes()) else ""
        if cache[key]:
            return True
    return False


def bound_files(root: Path, model: FileModel, cache: dict[str, str]) -> dict[str, str]:
    """File -> digest for everything an entry binds; ``cache`` holds the tree digests and source-reading answers of one run."""
    paths = bound_paths(root, model)
    if set_reads_source(root, paths, cache):
        return {path: bytes_digest((root / path).read_bytes()) for path in paths}
    for path in paths:
        if path not in cache:
            cache[path] = tree_digest((root / path).read_bytes())
    return {path: cache[path] for path in paths}


def binding_fields(bound: dict[str, str]) -> dict[str, object]:
    """The two fields of an entry that record its binding: one digest over every bound file and digest, and how many files it covers."""
    return {"bound_sha256": digest("\n".join(f"{path}\t{value}" for path, value in sorted(bound.items()))), "bound_files": len(bound)}


def site_entry(
    models: dict[str, FileModel], relative: str, root: Path, line: int, problem: str, occurrences: int, reason: str, cache: dict[str, str] | None = None
) -> dict[str, object]:
    """A reviewed-site entry for the current source (used by the lead to write ``REVIEWED``)."""
    code = (root / relative).read_text(errors="replace").splitlines()[line - 1].strip()
    return {
        "path": relative,
        "line": line,
        "code_sha256": hashlib.sha256(code.encode()).hexdigest(),
        "problem": problem,
        "occurrences": occurrences,
        **binding_fields(bound_files(root, models[relative], {} if cache is None else cache)),
        "reason": reason,
    }


def entry_hits(entry: dict[str, object], left: list[Finding], lines: list[str]) -> list[Finding]:
    """The findings a reviewed entry names: its finding class, a reviewable kind and the SHA-256 of the stripped source line."""
    return [
        finding
        for finding in left
        if str(entry.get("problem", "")) in finding.problem
        and any(kind in finding.problem for kind in REVIEWABLE)
        and hashlib.sha256(lines[int(finding.location.rsplit(":", 1)[1]) - 1].strip().encode()).hexdigest() == str(entry["code_sha256"])
    ]


def binding_current(document: dict[str, object]) -> bool:
    """True when the reviewed file names the binding format this code writes; a missing or other value fails every entry."""
    return document.get("binding") == BINDING


def reviewed_findings(
    entries: list[dict[str, object]],
    found: list[Finding],
    models: dict[str, FileModel],
    relative: str,
    root: Path,
    cache: dict[str, str],
    format_current: bool = True,
) -> list[Finding]:
    """The findings left after the reviewed entries of one file: an entry clears its findings only while the file, the repository modules it
    imports, the finding class and the count are exactly as reviewed; an entry that does not match is a finding itself."""
    lines = (root / relative).read_text(errors="replace").splitlines()
    left = list(found)
    for entry in entries:
        hits = entry_hits(entry, left, lines)
        hit_lines = [int(finding.location.rsplit(":", 1)[1]) for finding in hits]
        now = binding_fields(bound_files(root, models[relative], cache))
        current = format_current and entry.get("bound_sha256") == now["bound_sha256"]
        if current and len(hits) == int(str(entry.get("occurrences", 0))) and hits:
            left = [finding for finding in left if finding not in hits]
        else:
            left.append(
                Finding(
                    f"{relative}:{hit_lines[0] if hit_lines else entry.get('line', 1)}",
                    f"a reviewed entry in {REVIEWED} does not match the reviewed code (binding format, file, imported modules or count; "
                    f"reviewed {entry.get('bound_files', 'no')} files, now {now['bound_files']}); review it again",
                )
            )
    return left


def check(paths: list[str], root: Path, reviewed_file: Path | None = None) -> list[Finding]:
    """Every finding in the named files, relative to the repository root. Wrappers are discovered over every tracked file as well, so a
    named file's call of a wrapper defined elsewhere is judged by its arguments. The exempt files are not checked."""
    named = []
    for name in paths:
        relative = str(Path(name).resolve().relative_to(root.resolve())) if Path(name).is_absolute() else name
        if relative not in EXEMPT and (root / relative).is_file():
            named.append(relative)
    everything = sorted({*named, *(name for name in tracked_files(root) if name.endswith(".py") and name not in EXEMPT)})
    models = parse(root, everything)
    checker = Checker(models)
    checker.discover_resolvers()
    checker.discover_wrappers()
    source = reviewed_file or root / REVIEWED
    document = json.loads(source.read_text()) if source.is_file() else {}
    reviewed = document.get("sites", [])
    format_current = binding_current(document)
    cache: dict[str, str] = {}
    findings: list[Finding] = []
    for relative in named:
        if relative in models:
            entries = [entry for entry in reviewed if entry["path"] == relative]
            findings += reviewed_findings(entries, checker.findings(models[relative]), models, relative, root, cache, format_current)
        elif relative.endswith(".sh"):
            findings += shell_findings(relative, (root / relative).read_text(errors="replace"))
    return findings


def tracked_files(root: Path) -> list[str]:
    """The Python and shell files Git tracks or would track."""
    result = run_command("git", ["ls-files", "--cached", "--others", "--exclude-standard", "--", "*.py", "*.sh"], cwd=root, timeout=60)
    return [line for line in result.stdout.splitlines() if line]


def main(argv: list[str] | None = None) -> int:
    """Check the named files, or every tracked Python and shell file; print each finding and exit 1 when there is one."""
    arguments = sys.argv[1:] if argv is None else argv
    root = Path(__file__).resolve().parents[2]
    findings = check(arguments or tracked_files(root), root)
    for finding in findings:
        sys.stderr.write(f"{finding.location}: {finding.problem}; {FIX}\n")
    if not findings:
        sys.stdout.write("docker-calls: no Docker call outside the launcher\n")
    return 1 if findings else 0


if __name__ == "__main__":
    sys.exit(main())
