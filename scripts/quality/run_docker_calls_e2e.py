"""E2E for how a reviewed process start is bound (scripts/quality/docker_calls.py, config/quality/docker_calls_reviewed.json).

Run from the repository root: ``.venv/bin/python -m scripts.quality.run_docker_calls_e2e``. Every case writes a small scratch repository
(``site.py``, ``member.py``, ``mutate.py``, ``alias.py``, ``star.py``, ``doc.py``, ``anno.py``, the import forms ``dotted.py``, ``reused.py``,
``fallback.py``, ``runtime.py`` and ``runtime_<form>.py`` (aliases, package arguments), ``tools/run.py``, ``pkg/rel.py``,
``pkg/sub/mod.py``, the source readers ``own.py``, ``argv.py``, ``reader.py``, ``xread_entry.py`` and ``with_name_entry.py``
and the helper module ``helpers.py``), builds the reviewed entry for one process start with ``docker_calls.site_entry`` and then edits the scratch
files. An edit that changes no syntax tree (comments, whitespace, quote style, line wrapping, moved lines) must leave the entry current; any change of code,
documentation or annotation in the file or in a module it imports must bring the finding back together with the "does not match the
reviewed code" finding, and so must a missing or unsupported ``binding`` field. A file that reads source text (its own or an imported
module's) binds its whole set of files by bytes, so a comment edit in any of them brings the finding back there too. The second part plants a defect
against each rule of the binding and requires the cases named for it to fail. Failure modes F1 to F16 are in plans (local). The report goes to
data/e2e/docker_calls/<UTC>_<pid>/report.json with the revision and the SHA-256 of the files under test.
"""

from __future__ import annotations

import ast
import hashlib
import json
import os
import shutil
import sys
import tempfile
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from scripts.process import run_command
from scripts.quality import docker_calls

ROOT = Path(__file__).resolve().parents[2]
WATCHED = ("scripts/quality/docker_calls.py", "scripts/quality/run_docker_calls_e2e.py", "config/quality/docker_calls_reviewed.json", "scripts/process.py")
CANNOT_PROVE = "whose program the check cannot prove"
MISMATCH = "does not match the reviewed code"
SUPPORTED = object()
MISSING = object()

HELPERS = '''"""Helper module of the fixture."""

PROGRAM = "echo"
ANNOTATION = "echo"


def pick(name):
    return [name]


def other():
    return 2


def described():
    """echo"""


def annotated(name: ANNOTATION):
    return name
'''
SITE = '''"""Fixture module."""

import json

from helpers import pick
from scripts.process import run_command

PREFIX = "echo"


def unrelated():
    return 1


def local_helper(name):
    return [PREFIX, name]


def go():
    program = pick(local_helper(PREFIX))
    run_command(program[0], [])
'''
KLASS = """from helpers import pick
from scripts.process import run_command


class Runner:
    def __init__(self):
        self.program = pick("x")

    def label(self):
        return 1

    def go(self):
        run_command(self.program[0], [])
"""
MEMBER = """from scripts.process import run_command


class Runner:
    def __init__(self, program="echo"):
        self.program = program

    def go(self):
        run_command(self.program, [])
"""
MUTATE = """from helpers import pick
from scripts.process import run_command

ARGS = []
ARGS.append("before")


def go():
    run_command(pick("x")[0], ARGS)
"""
REPLACED = """import helpers
from scripts.process import run_command

helpers.pick = lambda name: ["echo"]


def go():
    run_command(helpers.pick("x")[0], [])
"""
ALIAS = """from helpers import pick
from scripts.process import run_command


class Runner:
    def run(self, command):
        run_command(command[0], [])


runner = Runner()
alias = runner.run


def go():
    alias(pick("echo"))
"""
STAR = """from helpers import *
from scripts.process import run_command


def go():
    run_command(pick("x")[0], [])
"""
RELATIVE = """from .helpers import pick
from scripts.process import run_command


def go():
    run_command(pick("x")[0], [])
"""
DOCUMENTED = """from helpers import described
from scripts.process import run_command


def go():
    run_command(described.__doc__, [])
"""
ANNOTATED = """from helpers import annotated
from scripts.process import run_command


def go():
    run_command(annotated.__annotations__["name"], [])
"""
BROKEN = """from brokenhelper import pick
from scripts.process import run_command


def go():
    run_command(pick("x")[0], [])
"""
GLOB = """from helpers import pick
from scripts.process import run_command

TARGET = pick("a")


def configure():
    global TARGET
    TARGET = pick("b")


def other_work():
    return 1


def go():
    run_command(TARGET[0], [])
"""
EVIDENCE = """from helpers import pick
from scripts.process import run_command


class Evidence:
    def run(self, command):
        run_command(command[0], [])


def suite():
    Evidence().run(pick("x"))


def unrelated_suite():
    return 1
"""
READER = """{prelude}

def program():
    return {expression}


# echo
"""
READERS = {
    "file": ("from pathlib import Path\n", "Path(__file__).read_text().splitlines()[-1][2:]"),
    "open": ("", "open(__file__).read().splitlines()[-1][2:]"),
    "inspect": ("import inspect\nimport sys\n", "inspect.getsource(sys.modules[__name__]).splitlines()[-1][2:]"),
    "linecache": ("import linecache\n", 'linecache.getlines("reader.py")[-1][2:]'),
    "resources": ("from importlib import resources\n", 'resources.read_text("pkg", "reader.py").splitlines()[-1][2:]'),
    "spec": ("", "__spec__.loader.get_source(__name__).splitlines()[-1][2:]"),
}
READER_ENTRY = """from reader import program
from scripts.process import run_command


def go():
    run_command(program(), [])
"""
OWN_SOURCE = """from pathlib import Path
from scripts.process import run_command


def go():
    run_command(Path(__file__).read_text().splitlines()[-1][2:], [])


# echo
"""
ARGV_SOURCE = """import sys
from scripts.process import run_command


def go():
    run_command(open(sys.argv[0]).read().splitlines()[-1][2:], [])


# echo
"""
IMPORTED_SOURCE = """import helpers
import inspect
from scripts.process import run_command


def go():
    run_command(inspect.getsource(helpers).splitlines()[-1][2:], [])
"""
ROOT_ENTRY = """from pathlib import Path

from helpers import pick
from scripts.process import run_command

ROOT = Path(__file__).resolve().parent


def go():
    run_command(pick(str(ROOT))[0], [])
"""
PAYLOAD = """VALUE = 1


# echo
"""
XHELPER = """from pathlib import Path

import payload


def program():
    return Path(payload.__file__).read_text().splitlines()[-1][2:]
"""
XREAD_ENTRY = """import payload
import xhelper
from scripts.process import run_command


def go():
    run_command(xhelper.program(), [])
"""
WITH_NAME_ENTRY = """from pathlib import Path

import payload
from scripts.process import run_command


def go():
    run_command(Path(__file__).with_name("payload.py").read_text().splitlines()[-1][2:], [])
"""
ALIAS_SOURCE = """from importlib import import_module as load
from scripts.process import run_command


def go():
    run_command(load("linecache").getlines("alias_source.py")[-1][2:].strip(), [])


# echo
"""
RUNTIME_ENTRY = """{prelude}
from scripts.process import run_command

mod = {expression}


def go():
    run_command(mod.pick("x")[0], [])
"""
# name -> (prelude, expression, the module file that the import names)
RUNTIME_IMPORTS = {
    "alias_from": ("from importlib import import_module as load\n", 'load("helpers")', "helpers.py"),
    "alias_assigned": ("import importlib\n\nload = importlib.import_module\n", 'load("helpers")', "helpers.py"),
    "module_alias": ("import importlib as il\n", 'il.import_module("helpers")', "helpers.py"),
    "relative_package": ("import importlib\n", 'importlib.import_module(".helpers", "pkg")', "pkg/helpers.py"),
    "relative_keyword": ("import importlib\n", 'importlib.import_module(".helpers", package="pkg")', "pkg/helpers.py"),
    "two_levels": ("import importlib\n", 'importlib.import_module("..helpers", "pkg.sub")', "pkg/helpers.py"),
    "alias_relative": ("from importlib import import_module as load\n", 'load(".helpers", "pkg")', "pkg/helpers.py"),
}
DOTTED = """import pkg.helpers
from scripts.process import run_command


def go():
    run_command(pkg.helpers.pick("x")[0], [])
"""
DOTTED_ALIAS = """import pkg.helpers as h
from scripts.process import run_command


def go():
    run_command(h.pick("x")[0], [])
"""
REUSED_ALIAS = """from scripts.process import run_command


def first():
    import helpers as h

    run_command(h.pick("x")[0], [])


def second():
    import pkg.helpers as h

    return h.pick("y")
"""
TWO_LEVELS = """from ..helpers import pick
from scripts.process import run_command


def go():
    run_command(pick("x")[0], [])
"""
FROM_PACKAGE = """from . import helpers
from scripts.process import run_command


def go():
    run_command(helpers.pick("x")[0], [])
"""
DEEP = """import pkg.sub.deep
from scripts.process import run_command


def go():
    run_command(pkg.sub.deep.pick("x")[0], [])
"""
FALLBACK = """try:
    from helpers import pick
except ImportError:
    from fallbackhelper import pick
from scripts.process import run_command


def go():
    run_command(pick("x")[0], [])
"""
RUNTIME = """import importlib
from scripts.process import run_command

helpers = importlib.import_module("helpers")


def go():
    run_command(helpers.pick("x")[0], [])
"""
SIBLING = """import sibling
from scripts.process import run_command


def go():
    run_command(sibling.pick("x")[0], [])
"""
FILES = {
    "helpers.py": HELPERS,
    "site.py": SITE,
    "klass.py": KLASS,
    "member.py": MEMBER,
    "mutate.py": MUTATE,
    "replaced.py": REPLACED,
    "alias.py": ALIAS,
    "star.py": STAR,
    "doc.py": DOCUMENTED,
    "anno.py": ANNOTATED,
    "broken.py": BROKEN,
    "brokenhelper.py": "def pick(:\n",
    "pkg/__init__.py": "MARK = 1\n",
    "pkg/helpers.py": "def pick(name):\n    return [name]\n",
    "pkg/rel.py": RELATIVE,
    "pkg/rel2.py": FROM_PACKAGE,
    "pkg/sub/__init__.py": "MARK = 1\n",
    "pkg/sub/deep.py": "def pick(name):\n    return [name]\n",
    "pkg/sub/mod.py": TWO_LEVELS,
    "dotted.py": DOTTED,
    "dotted_alias.py": DOTTED_ALIAS,
    "reused.py": REUSED_ALIAS,
    "deep.py": DEEP,
    "fallback.py": FALLBACK,
    "fallbackhelper.py": "def pick(name):\n    return [name]\n",
    "runtime.py": RUNTIME,
    "tools/run.py": SIBLING,
    "tools/sibling.py": "def pick(name):\n    return [name]\n",
    "own.py": OWN_SOURCE,
    "argv.py": ARGV_SOURCE,
    "imported_source.py": IMPORTED_SOURCE,
    "glob.py": GLOB,
    "evidence.py": EVIDENCE,
    "payload.py": PAYLOAD,
    "xhelper.py": XHELPER,
    "xread_entry.py": XREAD_ENTRY,
    "with_name_entry.py": WITH_NAME_ENTRY,
    "alias_source.py": ALIAS_SOURCE,
}
CALL = "run_command(program[0], [])"


class Scratch:
    """A scratch repository with the fixture files, the reviewed entry for one site and the edit helpers."""

    def __init__(self, root: Path) -> None:
        self.root = root
        for name, text in FILES.items():
            (root / name).parent.mkdir(parents=True, exist_ok=True)
            (root / name).write_text(text)
        (root / "reviewed.json").write_text("{}")

    def text(self, name: str) -> str:
        return (self.root / name).read_text()

    def edit(self, name: str, old: str, new: str) -> None:
        text = self.text(name)
        if old not in text:
            raise AssertionError(f"{name} has no {old!r}")
        (self.root / name).write_text(text.replace(old, new, 1))

    def write(self, name: str, text: str) -> None:
        (self.root / name).write_text(text)

    def entry(self, name: str, find: str) -> dict[str, Any]:
        """The reviewed entry for the process start on the line that holds ``find`` in a file."""
        line = next(number for number, text in enumerate(self.text(name).splitlines(), 1) if find in text)
        models = docker_calls.parse(self.root, [name])
        return dict(docker_calls.site_entry(models, name, self.root, line, CANNOT_PROVE, 1, "scratch"))

    def problems(self, name: str, entries: list[dict[str, Any]], binding: object = SUPPORTED) -> list[str]:
        document: dict[str, Any] = {"sites": entries}
        if binding is not MISSING:
            document["binding"] = docker_calls.BINDING if binding is SUPPORTED else binding
        (self.root / "reviewed.json").write_text(json.dumps(document))
        return [finding.problem for finding in docker_calls.check([name], self.root, reviewed_file=self.root / "reviewed.json")]


def cleared(problems: list[str]) -> bool:
    return problems == []


def brought_back(problems: list[str]) -> bool:
    return any(CANNOT_PROVE in problem for problem in problems) and any(MISMATCH in problem for problem in problems)


def edit_case(file: str, find: str, edit: Callable[[Scratch], None], expect_current: bool) -> Callable[[Scratch], bool]:
    """A case: review the site on ``find`` in ``file``, apply the edit, and expect the entry current or not."""

    def run(scratch: Scratch) -> bool:
        entry = scratch.entry(file, find)
        if not cleared(scratch.problems(file, [entry])):
            return False
        edit(scratch)
        problems = scratch.problems(file, [entry])
        return cleared(problems) if expect_current else brought_back(problems)

    return run


def file_edit_case(file: str, find: str, target: str, old: str, new: str) -> Callable[[Scratch], bool]:
    """A case: the entry of ``find`` in ``file`` must come back after ``old`` becomes ``new`` in ``target`` (the file or a module it imports)."""
    return edit_case(file, find, lambda scratch: scratch.edit(target, old, new), False)


def comment_and_formatting_edits(scratch: Scratch) -> bool:
    """Every edit that leaves the syntax tree as it was, applied one after the other to the same entry."""
    entry = scratch.entry("site.py", CALL)
    steps: list[Callable[[], None]] = [
        lambda: scratch.edit("site.py", "def unrelated():\n    return 1", "# a comment\ndef unrelated():  # trailing\n    # inside\n    return 1"),
        lambda: scratch.edit("site.py", 'PREFIX = "echo"', "PREFIX = 'echo'  # single quotes"),
        lambda: scratch.edit("site.py", "return [PREFIX, name]", "return [\n        PREFIX,\n        name,\n    ]"),
        lambda: scratch.edit("site.py", "program = pick(local_helper(PREFIX))", "program = pick( local_helper( PREFIX ) )"),
        lambda: scratch.edit("site.py", "def unrelated():", "\n\n\ndef unrelated():"),
        lambda: scratch.edit("helpers.py", "return [name]", "# comment\n    return [ name ]"),
        lambda: scratch.edit("helpers.py", "def other():", "\n\n\n# separator\ndef other():"),
    ]
    for step in steps:
        step()
        if not cleared(scratch.problems("site.py", [entry])):
            return False
    return True


def moved_within_function(scratch: Scratch) -> bool:
    """The call moves to another line of its function and the function moves down the file (blank lines and comments only)."""
    entry = scratch.entry("site.py", CALL)
    scratch.edit("site.py", "def go():", "# padding\n\n\n\ndef go():")
    scratch.edit("site.py", f"    {CALL}", f"\n\n    # note\n    {CALL}")
    return cleared(scratch.problems("site.py", [entry]))


def moved_to_other_function(scratch: Scratch) -> bool:
    """The same two lines move into a different function (a different tree): the review no longer holds."""
    entry = scratch.entry("site.py", CALL)
    scratch.edit(
        "site.py",
        "def go():\n    program = pick(local_helper(PREFIX))\n    " + CALL,
        "def go():\n    return 0\n\n\ndef go_elsewhere():\n    program = pick(local_helper(PREFIX))\n    " + CALL,
    )
    return brought_back(scratch.problems("site.py", [entry]))


def unrelated_code_edits(scratch: Scratch) -> bool:
    """Every change of code, documentation or imports in the file or in a module it imports brings the finding back, each on its own."""
    edits = [
        ("site.py", "def unrelated():\n    return 1", "def unrelated():\n    return 2"),
        ("site.py", "def unrelated():\n    return 1", "def unrelated():\n    return 1\n\n\ndef added():\n    return 3"),
        ("site.py", '"""Fixture module."""', '"""Changed."""'),
        ("site.py", "import json\n", "import json\nimport os\n"),
        ("helpers.py", "def other():\n    return 2", "def other():\n    return 3"),
        ("helpers.py", '"""Helper module of the fixture."""', '"""Changed."""'),
    ]
    entry = scratch.entry("site.py", CALL)
    for name, old, new in edits:
        original = scratch.text(name)
        scratch.edit(name, old, new)
        failed = not brought_back(scratch.problems("site.py", [entry]))
        scratch.write(name, original)
        if failed:
            return False
    return cleared(scratch.problems("site.py", [entry]))


def duplicate_call(scratch: Scratch) -> bool:
    entry = scratch.entry("site.py", CALL)
    scratch.write("site.py", scratch.text("site.py") + f"\n\ndef again():\n    program = pick(PREFIX)\n    {CALL}\n")
    return brought_back(scratch.problems("site.py", [entry]))


def wrong_class_or_count(scratch: Scratch) -> bool:
    entry = scratch.entry("site.py", CALL)
    count = scratch.problems("site.py", [{**entry, "occurrences": 2}])
    kind = scratch.problems("site.py", [{**entry, "problem": "is used as a value"}])
    return any(CANNOT_PROVE in problem for problem in count) and any(CANNOT_PROVE in problem for problem in kind)


def legacy_entry_rejected(scratch: Scratch) -> bool:
    entry = scratch.entry("site.py", CALL)
    legacy = {key: value for key, value in entry.items() if key not in ("bound_sha256", "bound_files")}
    legacy["file_sha256"] = hashlib.sha256((scratch.root / "site.py").read_bytes()).hexdigest()
    legacy["dependencies"] = {}
    return brought_back(scratch.problems("site.py", [legacy]))


def binding_edited(scratch: Scratch) -> bool:
    """A hand-edited or removed binding digest does not clear the finding."""
    entry = scratch.entry("site.py", CALL)
    return brought_back(scratch.problems("site.py", [{**entry, "bound_sha256": "sha256:" + "0" * 64}])) and brought_back(
        scratch.problems("site.py", [{key: value for key, value in entry.items() if key != "bound_sha256"}])
    )


def binding_field_missing(scratch: Scratch) -> bool:
    """The JSON without its ``binding`` field is not current, whatever its entries hold."""
    entry = scratch.entry("site.py", CALL)
    return cleared(scratch.problems("site.py", [entry])) and brought_back(scratch.problems("site.py", [entry], binding=MISSING))


def binding_field_unsupported(scratch: Scratch) -> bool:
    """A null, another format version and another Python minor version are not current."""
    entry = scratch.entry("site.py", CALL)
    others: list[object] = [None, "", "ast-0-py3.12", f"{docker_calls.BINDING}x", "ast-2-py2.7"]
    return all(brought_back(scratch.problems("site.py", [entry], binding=other)) for other in others)


def unparsable_dependency(scratch: Scratch) -> bool:
    """A module that does not parse is bound by its bytes: unchanged it stays current, any edit brings the finding back."""
    entry = scratch.entry("broken.py", "run_command(pick")
    unchanged = cleared(scratch.problems("broken.py", [entry]))
    scratch.write("brokenhelper.py", "def pick(:\n# comment\n")
    return unchanged and brought_back(scratch.problems("broken.py", [entry]))


def global_rebind(scratch: Scratch) -> bool:
    entry = scratch.entry("glob.py", "run_command(TARGET[0], [])")
    scratch.edit("glob.py", "def other_work():\n    return 1", "def other_work():  # comment\n    return 1")
    unrelated = cleared(scratch.problems("glob.py", [entry]))
    scratch.edit("glob.py", '    TARGET = pick("b")', '    TARGET = pick("c")')
    return unrelated and brought_back(scratch.problems("glob.py", [entry]))


def method_state(scratch: Scratch) -> bool:
    entry = scratch.entry("klass.py", "run_command(self.program[0], [])")
    scratch.edit("klass.py", "def label(self):", "def label(self):  # comment")
    unrelated = cleared(scratch.problems("klass.py", [entry]))
    scratch.edit("klass.py", 'self.program = pick("x")', 'self.program = pick("y")')
    return unrelated and brought_back(scratch.problems("klass.py", [entry]))


def caller_argument(scratch: Scratch) -> bool:
    entry = scratch.entry("evidence.py", "run_command(command[0], [])")
    scratch.edit("evidence.py", "def unrelated_suite():\n    return 1", "def unrelated_suite():  # comment\n    return 1")
    unrelated = cleared(scratch.problems("evidence.py", [entry]))
    scratch.edit("evidence.py", 'Evidence().run(pick("x"))', 'Evidence().run(pick("y"))')
    return unrelated and brought_back(scratch.problems("evidence.py", [entry]))


def reader_case(variant: str) -> Callable[[Scratch], bool]:
    """A case: an imported helper that reads its own source; its last comment changes (a comment edit, which no tree sees)."""

    def run(scratch: Scratch) -> bool:
        prelude, expression = READERS[variant]
        scratch.write("reader.py", READER.format(prelude=prelude, expression=expression))
        scratch.write("reader_entry.py", READER_ENTRY)
        return file_edit_case("reader_entry.py", "run_command(program()", "reader.py", "# echo", "# docker")(scratch)

    return run


def own_file_name_case(scratch: Scratch) -> bool:
    """An entry that uses its own ``__file__`` (the usual way to find the repository root) binds the whole set by bytes: a code change or a
    comment in the helper brings the finding back, and so does a comment in the entry."""
    scratch.write("root_entry.py", ROOT_ENTRY)
    entry = scratch.entry("root_entry.py", "run_command(pick")
    scratch.edit("helpers.py", "def other():", "def other():  # note")
    comment = brought_back(scratch.problems("root_entry.py", [entry]))
    scratch.edit("helpers.py", "def other():  # note", "def other():")
    scratch.edit("helpers.py", "return [name]", "return [name, name]")
    code = brought_back(scratch.problems("root_entry.py", [entry]))
    scratch.edit("helpers.py", "return [name, name]", "return [name]")
    scratch.edit("root_entry.py", "def go():", "# note\ndef go():")
    return comment and code and brought_back(scratch.problems("root_entry.py", [entry]))


def runtime_import_case(variant: str) -> Callable[[Scratch], bool]:
    """A case: the module that a literal run-time import names (through an alias, a package argument or a relative depth) changes."""

    def run(scratch: Scratch) -> bool:
        prelude, expression, target = RUNTIME_IMPORTS[variant]
        scratch.write(f"runtime_{variant}.py", RUNTIME_ENTRY.format(prelude=prelude, expression=expression))
        return file_edit_case(f"runtime_{variant}.py", "run_command(mod.pick", target, "return [name]", "return [name, name]")(scratch)

    return run


def package_init_on_path(scratch: Scratch) -> bool:
    """Each package ``__init__`` on the path of ``import pkg.sub.deep`` is bound: a statement changed in either brings the finding back."""
    entry = scratch.entry("deep.py", "run_command(pkg.sub.deep")
    for name in ("pkg/__init__.py", "pkg/sub/__init__.py", "pkg/sub/deep.py"):
        original = scratch.text(name)
        scratch.edit(
            name, "MARK = 1" if name.endswith("__init__.py") else "return [name]", "MARK = 2" if name.endswith("__init__.py") else "return [name, name]"
        )
        failed = not brought_back(scratch.problems("deep.py", [entry]))
        scratch.write(name, original)
        if failed:
            return False
    return cleared(scratch.problems("deep.py", [entry]))


CASES: dict[str, Callable[[Scratch], bool]] = {
    "comment_and_formatting_edits_keep_entry": comment_and_formatting_edits,
    "call_moved_within_function": moved_within_function,
    "unrelated_code_edit_invalidates": unrelated_code_edits,
    "call_line_edit": file_edit_case("site.py", CALL, "site.py", CALL, "run_command(program[0], ['-v'])"),
    "enclosing_function_edit": file_edit_case("site.py", CALL, "site.py", "    program = pick(", "    extra = 1\n    program = pick("),
    "module_constant_edit": file_edit_case("site.py", CALL, "site.py", 'PREFIX = "echo"', 'PREFIX = "other"'),
    "same_module_helper_edit": file_edit_case("site.py", CALL, "site.py", "return [PREFIX, name]", "return [name, PREFIX]"),
    "method_state_edit": method_state,
    "global_rebind": global_rebind,
    "imported_helper_edit": file_edit_case("site.py", CALL, "helpers.py", "return [name]", "return [name, name]"),
    "import_repoint": file_edit_case("site.py", CALL, "site.py", "from helpers import pick", "from helpers import other as pick"),
    "caller_argument_edit": caller_argument,
    "member_state_initializer_default": file_edit_case("member.py", "run_command(self.program", "member.py", 'program="echo"', 'program="docker"'),
    "module_level_mutation": file_edit_case("mutate.py", "run_command(pick", "mutate.py", 'ARGS.append("before")', 'ARGS.append("after")'),
    "imported_attribute_replacement": file_edit_case("replaced.py", "run_command(helpers", "replaced.py", '["echo"]', '["printf"]'),
    "caller_alias": file_edit_case("alias.py", "run_command(command[0]", "alias.py", 'alias(pick("echo"))', 'alias(pick("printf"))'),
    "star_imported_helper": file_edit_case("star.py", "run_command(pick", "helpers.py", "return [name]", "return [name, name]"),
    "relative_import_helper": file_edit_case("pkg/rel.py", "run_command(pick", "pkg/helpers.py", "return [name]", "return [name, name]"),
    "docstring_used_as_program": file_edit_case("doc.py", "run_command(described", "helpers.py", '"""echo"""', '"""printf"""'),
    "annotation_used_as_program": file_edit_case("anno.py", "run_command(annotated", "helpers.py", 'ANNOTATION = "echo"', 'ANNOTATION = "printf"'),
    "unparsable_dependency_falls_back_to_bytes": unparsable_dependency,
    "duplicate_call": duplicate_call,
    "call_moved_to_other_function": moved_to_other_function,
    "legacy_entry_rejected": legacy_entry_rejected,
    "wrong_class_or_count": wrong_class_or_count,
    "binding_edited": binding_edited,
    "binding_field_missing": binding_field_missing,
    "binding_field_unsupported": binding_field_unsupported,
    "source_read_helper_file": reader_case("file"),
    "source_read_helper_open": reader_case("open"),
    "source_read_helper_inspect": reader_case("inspect"),
    "source_read_helper_linecache": reader_case("linecache"),
    "source_read_helper_resources": reader_case("resources"),
    "source_read_helper_spec": reader_case("spec"),
    "source_read_entry_file": file_edit_case("own.py", "run_command(Path(__file__)", "own.py", "# echo", "# docker"),
    "source_read_entry_argv": file_edit_case("argv.py", "run_command(open(sys.argv", "argv.py", "# echo", "# docker"),
    "source_read_entry_imports_helper": file_edit_case(
        "imported_source.py", "run_command(inspect.getsource", "helpers.py", "def other():", "def other():  # note"
    ),
    "own_file_name_binds_set_by_bytes": own_file_name_case,
    "source_read_cross_file_helper": file_edit_case("xread_entry.py", "run_command(xhelper", "payload.py", "# echo", "# docker"),
    "source_read_cross_file_with_name": file_edit_case("with_name_entry.py", "run_command(Path(__file__)", "payload.py", "# echo", "# docker"),
    "source_read_via_import_alias": file_edit_case("alias_source.py", "run_command(load(", "alias_source.py", "# echo", "# docker"),
    **{f"runtime_import_{name}": runtime_import_case(name) for name in RUNTIME_IMPORTS},
    "dotted_import_helper": file_edit_case("dotted.py", "run_command(pkg.helpers", "pkg/helpers.py", "return [name]", "return [name, name]"),
    "dotted_import_with_alias": file_edit_case("dotted_alias.py", "run_command(h.pick", "pkg/helpers.py", "return [name]", "return [name, name]"),
    "reused_alias_earlier_module": file_edit_case("reused.py", "run_command(h.pick", "helpers.py", "return [name]", "return [name, name]"),
    "relative_import_two_levels": file_edit_case("pkg/sub/mod.py", "run_command(pick", "pkg/helpers.py", "return [name]", "return [name, name]"),
    "relative_from_import_submodule": file_edit_case("pkg/rel2.py", "run_command(helpers.pick", "pkg/helpers.py", "return [name]", "return [name, name]"),
    "package_init_on_path": package_init_on_path,
    "fallback_import_first_module": file_edit_case("fallback.py", "run_command(pick", "helpers.py", "return [name]", "return [name, name]"),
    "literal_runtime_import": file_edit_case("runtime.py", "run_command(helpers.pick", "helpers.py", "return [name]", "return [name, name]"),
    "script_folder_import": file_edit_case("tools/run.py", "run_command(sibling.pick", "tools/sibling.py", "return [name]", "return [name, name]"),
}


ORIGINAL_DUMP = docker_calls.dump_tree
ORIGINAL_CANDIDATES = docker_calls.module_candidates
ORIGINAL_SET = docker_calls.set_reads_source
ORIGINAL_RUNTIME = docker_calls.runtime_import_names


def with_positions(source: bytes) -> str | None:
    """Planted defect: the dump keeps line positions."""
    if ORIGINAL_DUMP(source) is None:
        return None
    return ast.dump(ast.parse(source), include_attributes=True)


def without_documentation(source: bytes) -> str | None:
    """Planted defect: docstrings are dropped before the dump."""
    if ORIGINAL_DUMP(source) is None:
        return None
    tree = ast.parse(source)
    for node in ast.walk(tree):
        body = getattr(node, "body", None)
        if isinstance(body, list) and body and isinstance(body[0], ast.Expr) and isinstance(body[0].value, ast.Constant):
            body[0] = ast.Pass()
    return ast.dump(tree)


def source_text(source: bytes) -> str | None:
    """Planted defect: the binding is the file text, not its tree."""
    if ORIGINAL_DUMP(source) is None:
        return None
    return source.decode(errors="replace")


def dropped_on_failure(source: bytes) -> str | None:
    """Planted defect: a file that does not parse is bound as empty instead of by its bytes."""
    return ORIGINAL_DUMP(source) or ""


def own_file_left_out(root: Path, model: Any) -> list[str]:
    """Planted defect: only the imported modules are bound."""
    return docker_calls.import_files(root, model)


def imports_not_followed(root: Path, model: Any) -> list[str]:
    """Planted defect: only the file itself is bound."""
    return [model.path]


def name_map_imports(model: Any) -> list[str]:
    """Planted defect: the imports are read from the name map (one value per local name, the first dotted part for ``import a.b``)."""
    return list(model.imports.values())


def one_level_only(self: Any, node: Any) -> str:
    """Planted defect: a relative import above one level resolves as one level."""
    if not node.level:
        return str(node.module)
    parts = self.module.split(".")
    package = parts if self.path.endswith("__init__.py") else parts[:-1]
    return ".".join([*package, *([node.module] if node.module else [])])


def deepest_module_only(name: str, base: str) -> list[str]:
    """Planted defect: only the deepest module of a dotted name is bound, not the packages on its path."""
    return ORIGINAL_CANDIDATES(name, base)[:2]


def no_folder_candidates(name: str, base: str) -> list[str]:
    """Planted defect: a name is looked up from the repository root only."""
    return ORIGINAL_CANDIDATES(name, base) if not base else []


def no_literal_imports(_model: Any) -> list[str]:
    """Planted defect: run-time imports by a literal are not collected."""
    return []


def never_reads_source(_source: bytes) -> bool:
    """Planted defect: no file is taken as reading its source."""
    return False


def only_file_name(source: bytes) -> bool:
    """Planted defect: only the name ``__file__`` marks a source-reading file."""
    return b"__file__" in source


def set_never_reads(_root: Path, _paths: list[str], _cache: dict[str, str]) -> bool:
    """Planted defect: no bound set is taken as reading source."""
    return False


def set_entry_only(root: Path, paths: list[str], cache: dict[str, str]) -> bool:
    """Planted defect: only the entry file of a bound set is tested for source reading."""
    return ORIGINAL_SET(root, paths[:1], cache)


def set_imports_only(root: Path, paths: list[str], cache: dict[str, str]) -> bool:
    """Planted defect: only the imported files of a bound set are tested (the entry is skipped)."""
    return ORIGINAL_SET(root, paths[1:], cache)


def aliases_not_collected(_tree: ast.AST) -> frozenset[str]:
    """Planted defect: only the two plain names count as import functions."""
    return docker_calls.IMPORT_FUNCTIONS


def package_ignored(_model: Any, name: str, _package: str | None, _level: ast.expr | None) -> list[str]:
    """Planted defect: the literal package argument is ignored, the name keeps only its module part."""
    return [name.lstrip(".")]


def one_dot_only(model: Any, name: str, package: str | None, level: ast.expr | None) -> list[str]:
    """Planted defect: every relative name resolves one level up, whatever its dots."""
    return ORIGINAL_RUNTIME(model, f".{name.lstrip('.')}" if name.startswith(".") else name, package, level)


def positional_only(call: ast.Call, position: int, _keyword: str) -> ast.expr | None:
    """Planted defect: keyword arguments are not read."""
    return call.args[position] if len(call.args) > position else None


def relative_not_resolved(self: Any, node: Any) -> str:
    """Planted defect: a relative import keeps its bare module name."""
    return str(node.module)


def binding_missing_is_current(document: dict[str, Any]) -> bool:
    """Planted defect: a JSON without ``binding`` is taken as the supported version."""
    return bool(document.get("binding", docker_calls.BINDING) == docker_calls.BINDING)


def binding_not_compared(_document: dict[str, Any]) -> bool:
    """Planted defect: any ``binding`` value is accepted."""
    return True


TREE_CASES = {"comment_and_formatting_edits_keep_entry", "call_moved_within_function"}
IMPORT_CASES = {"imported_helper_edit", "star_imported_helper", "relative_import_helper", "docstring_used_as_program", "annotation_used_as_program"}
OWN_FILE_CASES = {
    "member_state_initializer_default",
    "module_level_mutation",
    "imported_attribute_replacement",
    "caller_alias",
    "unrelated_code_edit_invalidates",
    "method_state_edit",
}
HELPER_SOURCE_CASES = {f"source_read_helper_{name}" for name in READERS}
CROSS_FILE_CASES = {"source_read_cross_file_helper", "source_read_cross_file_with_name", "own_file_name_binds_set_by_bytes"}
SOURCE_CASES = {
    *HELPER_SOURCE_CASES,
    *CROSS_FILE_CASES,
    "source_read_entry_file",
    "source_read_entry_argv",
    "source_read_entry_imports_helper",
    "source_read_via_import_alias",
}
FILE_NAME_CASES = {"source_read_helper_file", "source_read_helper_open", "source_read_entry_file", *CROSS_FILE_CASES}
RUNTIME_CASES = {f"runtime_import_{name}" for name in RUNTIME_IMPORTS}
NAME_MAP_CASES = {"star_imported_helper", "dotted_import_helper", "reused_alias_earlier_module", "fallback_import_first_module"}
# name -> (owner, attribute replaced, replacement, cases that must fail with the defect)
PLANTED: dict[str, tuple[Any, str, Callable[..., Any], set[str]]] = {
    "dump keeps line positions": (docker_calls, "dump_tree", with_positions, TREE_CASES),
    "binding is the file text": (docker_calls, "dump_tree", source_text, TREE_CASES),
    "own file left out": (docker_calls, "bound_paths", own_file_left_out, OWN_FILE_CASES),
    "imports not followed": (docker_calls, "bound_paths", imports_not_followed, IMPORT_CASES),
    "relative imports not resolved": (
        docker_calls.FileModel,
        "absolute",
        relative_not_resolved,
        {"relative_import_two_levels", "relative_from_import_submodule"},
    ),
    "relative level above one not resolved": (docker_calls.FileModel, "absolute", one_level_only, {"relative_import_two_levels"}),
    "imports read from the name map": (docker_calls, "statement_imports", name_map_imports, NAME_MAP_CASES),
    "package prefixes dropped": (docker_calls, "module_candidates", deepest_module_only, {"package_init_on_path"}),
    "script folder not tried": (docker_calls, "module_candidates", no_folder_candidates, {"script_folder_import"}),
    "literal run-time imports not collected": (docker_calls, "literal_imports", no_literal_imports, {"literal_runtime_import", *RUNTIME_CASES}),
    "import aliases not collected": (
        docker_calls,
        "import_aliases",
        aliases_not_collected,
        {"runtime_import_alias_from", "runtime_import_alias_assigned", "runtime_import_alias_relative", "source_read_via_import_alias"},
    ),
    "literal package argument ignored": (
        docker_calls,
        "runtime_import_names",
        package_ignored,
        {"runtime_import_relative_package", "runtime_import_relative_keyword", "runtime_import_two_levels", "runtime_import_alias_relative"},
    ),
    "relative depth ignored": (docker_calls, "runtime_import_names", one_dot_only, {"runtime_import_two_levels"}),
    "keyword arguments not read": (docker_calls, "call_argument", positional_only, {"runtime_import_relative_keyword"}),
    "source reading not detected": (docker_calls, "reads_source", never_reads_source, SOURCE_CASES),
    "source reading limited to the name __file__": (docker_calls, "reads_source", only_file_name, SOURCE_CASES - FILE_NAME_CASES),
    "bound set tested for source reading by its entry only": (
        docker_calls,
        "set_reads_source",
        set_entry_only,
        {*HELPER_SOURCE_CASES, "source_read_cross_file_helper"},
    ),
    "bound set tested for source reading without its entry": (
        docker_calls,
        "set_reads_source",
        set_imports_only,
        {"source_read_cross_file_with_name", "own_file_name_binds_set_by_bytes", "source_read_entry_file"},
    ),
    "bound set never tested for source reading": (docker_calls, "set_reads_source", set_never_reads, SOURCE_CASES),
    "documentation stripped": (docker_calls, "dump_tree", without_documentation, {"docstring_used_as_program", "unrelated_code_edit_invalidates"}),
    "unparsable module dropped": (docker_calls, "dump_tree", dropped_on_failure, {"unparsable_dependency_falls_back_to_bytes"}),
    "missing binding taken as current": (docker_calls, "binding_current", binding_missing_is_current, {"binding_field_missing"}),
    "binding value not compared": (docker_calls, "binding_current", binding_not_compared, {"binding_field_missing", "binding_field_unsupported"}),
}


@contextmanager
def planted(owner: Any, method: str, replacement: Callable[..., Any]) -> Iterator[None]:
    original = getattr(owner, method)
    setattr(owner, method, replacement)
    try:
        yield
    finally:
        setattr(owner, method, original)


def run_all(work: Path) -> dict[str, bool]:
    """Run every case in its own scratch repository and return name -> passed."""
    results: dict[str, bool] = {}
    for name, case in CASES.items():
        folder = Path(tempfile.mkdtemp(prefix=f"{name}_", dir=work))
        try:
            results[name] = case(Scratch(folder))
        except Exception as error:
            sys.stdout.write(f"      error in {name}: {type(error).__name__}: {error}\n")
            results[name] = False
    return results


def sha256_of(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest() if path.is_file() else ""


def main() -> int:
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ") + f"_{os.getpid()}"
    report = ROOT / "data" / "e2e" / "docker_calls" / stamp
    report.mkdir(parents=True)
    work = Path(tempfile.mkdtemp(prefix="docker_calls_e2e"))
    try:
        base = run_all(work)
        for name, ok in base.items():
            sys.stdout.write(f"{'ok' if ok else 'FAIL':<5} {name}\n")
        defects: dict[str, dict[str, Any]] = {}
        for defect, (owner, method, replacement, expected) in PLANTED.items():
            with planted(owner, method, replacement):
                failed = {name for name, ok in run_all(work).items() if not ok}
            caught = expected <= failed
            defects[defect] = {"method": method, "expected": sorted(expected), "failed": sorted(failed), "caught": caught}
            sys.stdout.write(f"{'ok' if caught else 'FAIL':<5} planted: {defect} -> {', '.join(sorted(failed)) or 'nothing failed'}\n")
    finally:
        shutil.rmtree(work, ignore_errors=True)
    head = run_command("git", ["rev-parse", "HEAD"], cwd=ROOT).stdout.strip()
    passed = all(base.values()) and all(item["caught"] for item in defects.values())
    summary = {
        "command": ".venv/bin/python -m scripts.quality.run_docker_calls_e2e",
        "head": head,
        "files": {name: sha256_of(ROOT / name) for name in WATCHED},
        "cases": base,
        "planted_defects": defects,
        "passed": passed,
    }
    (report / "report.json").write_text(json.dumps(summary, indent=2) + "\n")
    sys.stdout.write(f"{'all passed' if passed else 'FAILED'}: {report.relative_to(ROOT)}/report.json\n")
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
