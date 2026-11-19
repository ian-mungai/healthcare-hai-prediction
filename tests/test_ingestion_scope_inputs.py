"""Every value the scope report reads from an input file, with a wrong type, a missing key, an empty value or an unexpected shape.

The coverage table below lists each file, each read path in it and the bad values tried there. A bad value must give exit 2, the file named on stderr
and no report. A second case feeds seeded arbitrary JSON-like values at every read path and requires an exit 2, a valid report or a printed scope
problem, and never an uncaught exception. The parse is replaced by a fake that returns the manifest (or writes its bytes), so each case takes
milliseconds; the sources file, which only dbt reads, is covered by real parses in test_ingestion_scope.py.

What the tool reads, by file:

* table map (``map.json``): the document, ``tables`` and each entry; per entry ``table``, ``group``, ``collection``, ``format`` (non-empty strings),
  ``roles`` (optional list of non-empty strings) and ``dictionary`` (optional object with a non-empty string ``table``).
* project file (``dbt_project.yml``): the document, ``vars``, ``vars.copy_tables`` and each key (a non-empty string) and value (a non-empty string).
* manifest (``manifest.json`` of the parse): the document, ``metadata`` and its ``dbt_version``, ``sources`` and each source (an object with string
  ``source_name`` and ``name``), ``nodes`` and each node (an object with a string ``resource_type``); each model also needs a ``name``, a ``depends_on``
  object and its ``nodes`` list of strings.
* sources file (``_bronze__sources.yml``): read by dbt only; a malformed one fails the parse.
"""

from __future__ import annotations

import copy
import itertools
import json
import zlib
from collections.abc import Callable
from pathlib import Path
from typing import Any, NamedTuple

import pytest

from scripts.ingestion import scope
from scripts.process import CompletedProcess
from scripts.quality import dbt_manifest_labels
from tests.support import check
from tests.test_ingestion_scope_logic import OPERATIONAL, dictionary_of, entry, graph_1, manifest, table_map

MISSING = object()
UID_T_A = "source.p.bronze.t_a"
UID_OTHER = "source.p.other.s"
MODEL = "model.p.stg_a"
OPERATIONAL_UID = f"model.p.{OPERATIONAL}"
TEST_NODE = "test.p.ut"


def base_map() -> dict[str, Any]:
    """A table map of one root with a dictionary, the dictionary table and one leaving table."""
    first = entry("t_a", "x/a", "d_a")
    first["roles"] = ["data"]
    return table_map(first, dictionary_of("d_a", "x/a"), entry("t_x", "x/e"))


def base_project() -> dict[str, Any]:
    """A dbt_project.yml document (written as JSON, which YAML reads) with one copy_tables entry and one other variable."""
    return {"name": "synth", "version": "1.0", "config-version": 2, "profile": "synth", "vars": {"other": 1, "copy_tables": {"t_a": "fam"}}}


def base_manifest() -> dict[str, Any]:
    """A manifest: one model reading t_a, the operational model, a test node reading t_x and a non-bronze source."""
    document = manifest({"stg_a": ["t_a"], OPERATIONAL: ["t_a"]}, ["t_a", "d_a", "t_x"], {"ut": ["t_x"]})
    document["sources"][UID_OTHER] = {"source_name": "other", "name": "s"}
    return document


class Outcome(NamedTuple):
    """What one command-line run did."""

    code: int
    out: str
    err: str
    report: dict[str, Any] | None


def report_of(outcome: Outcome) -> dict[str, Any]:
    """The report of a run that wrote one, or a failure."""
    if outcome.report is None:
        raise AssertionError(f"no report was written: exit {outcome.code} {outcome.err}")
    return outcome.report


def set_path(document: Any, path: tuple[Any, ...], value: Any) -> Any:
    """A deep copy of ``document`` with ``value`` at ``path`` (``MISSING`` deletes the key); an empty path replaces the document."""
    if not path:
        return copy.deepcopy(value)
    changed = copy.deepcopy(document)
    cursor = changed
    for step in path[:-1]:
        cursor = cursor[step]
    if value is MISSING:
        del cursor[path[-1]]
    else:
        cursor[path[-1]] = copy.deepcopy(value)
    return changed


class Runner:
    """Runs the command line on written input files with the parse replaced; one fresh folder per run."""

    def __init__(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
        self.tmp_path, self.monkeypatch, self.capsys, self.count = tmp_path, monkeypatch, capsys, 0

    def run(
        self,
        map_document: Any = MISSING,
        project_document: Any = MISSING,
        manifest_document: Any = MISSING,
        *,
        map_text: str | None = None,
        project_text: str | None = None,
        project_bytes: bytes | None = None,
        manifest_bytes: bytes | None = None,
    ) -> Outcome:
        """One run; a ``*_text`` or ``manifest_bytes`` argument writes those exact bytes instead of the JSON of the document."""
        self.count += 1
        folder = self.tmp_path / f"run{self.count}"
        project = folder / "project"
        project.mkdir(parents=True)
        (folder / "map.json").write_text(map_text if map_text is not None else json.dumps(base_map() if map_document is MISSING else map_document))
        if project_bytes is not None:
            (project / "dbt_project.yml").write_bytes(project_bytes)
        else:
            (project / "dbt_project.yml").write_text(
                project_text if project_text is not None else json.dumps(base_project() if project_document is MISSING else project_document)
            )
        if manifest_bytes is not None:

            def write_manifest(program: str, args: Any, *, cwd: Path, **kwargs: Any) -> CompletedProcess[str]:
                (cwd / "target").mkdir()
                (cwd / "target" / "manifest.json").write_bytes(manifest_bytes)
                return CompletedProcess(args, 0, "", "")

            self.monkeypatch.setattr(dbt_manifest_labels, "run_command", write_manifest)
        else:
            parsed = copy.deepcopy(base_manifest() if manifest_document is MISSING else manifest_document)
            self.monkeypatch.setattr(dbt_manifest_labels, "parse_manifest", lambda *args, **kwargs: parsed)
        out = folder / "out.json"
        code = scope.main(["--project", str(project), "--table-map", str(folder / "map.json"), "--no-packages", "--out", str(out)])
        captured = self.capsys.readouterr()
        return Outcome(code, captured.out, captured.err, json.loads(out.read_text()) if out.exists() else None)


@pytest.fixture
def runner(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> Runner:
    """A runner bound to this test's folder and fixtures."""
    return Runner(tmp_path, monkeypatch, capsys)


STR_REQUIRED: list[Any] = [MISSING, None, 5, 1.5, True, "", "   ", [], {}, ["x"], {"a": 1}]
DICT_REQUIRED: list[Any] = [MISSING, None, 5, "x", [], [1], True]
LIST_REQUIRED: list[Any] = [MISSING, None, 5, "x", {}, {"a": 1}, True]
NOT_AN_OBJECT: list[Any] = [None, 5, 1.5, "x", [], [1], True]
OPTIONAL_OBJECT: list[Any] = [None, 5, "x", [], [1], True]
OPTIONAL_STRINGS: list[Any] = [None, 5, "x", {}, [5], [None], [""], [[]], True]
DEPENDENCY_LIST: list[Any] = [*LIST_REQUIRED, [5], [None], [[]], [{}]]
MAP, PROJECT, MANIFEST = "map", "project", "manifest"
FILE_NAMES = {MAP: "map.json", PROJECT: "dbt_project.yml", MANIFEST: "manifest.json"}

# (file, path in the file, bad values). Every value read from every input file is a row here; a bad value must give exit 2 naming the file.
COVERAGE: list[tuple[str, tuple[Any, ...], list[Any]]] = [
    (MAP, (), NOT_AN_OBJECT),
    (MAP, ("tables",), LIST_REQUIRED),
    *[
        row
        for index in range(3)
        for row in [(MAP, ("tables", index), NOT_AN_OBJECT)]
        + [(MAP, ("tables", index, key), STR_REQUIRED) for key in ("table", "group", "collection", "format")]
    ],
    (MAP, ("tables", 0, "roles"), OPTIONAL_STRINGS),
    (MAP, ("tables", 0, "dictionary"), [*OPTIONAL_OBJECT, {}]),
    (MAP, ("tables", 0, "dictionary", "table"), STR_REQUIRED),
    (PROJECT, (), NOT_AN_OBJECT),
    (PROJECT, ("vars",), DICT_REQUIRED),
    (PROJECT, ("vars", "copy_tables"), DICT_REQUIRED),
    (PROJECT, ("vars", "copy_tables", "t_a"), [None, 5, 1.5, True, "", "   ", [], {}, ["x"]]),
    (MANIFEST, (), NOT_AN_OBJECT),
    (MANIFEST, ("metadata",), DICT_REQUIRED),
    (MANIFEST, ("metadata", "dbt_version"), [None, 5, True, [], {}]),
    (MANIFEST, ("sources",), DICT_REQUIRED),
    *[
        row
        for uid in (UID_T_A, UID_OTHER)
        for row in [(MANIFEST, ("sources", uid), NOT_AN_OBJECT)] + [(MANIFEST, ("sources", uid, key), STR_REQUIRED) for key in ("source_name", "name")]
    ],
    (MANIFEST, ("nodes",), DICT_REQUIRED),
    *[
        row
        for uid in (MODEL, OPERATIONAL_UID, TEST_NODE)
        for row in [(MANIFEST, ("nodes", uid), NOT_AN_OBJECT), (MANIFEST, ("nodes", uid, "resource_type"), STR_REQUIRED)]
    ],
    *[
        row
        for uid in (MODEL, OPERATIONAL_UID)
        for row in [
            (MANIFEST, ("nodes", uid, "name"), STR_REQUIRED),
            (MANIFEST, ("nodes", uid, "depends_on"), DICT_REQUIRED),
            (MANIFEST, ("nodes", uid, "depends_on", "nodes"), DEPENDENCY_LIST),
        ]
    ],
]
ROWS = [(file, path, value) for file, path, values in COVERAGE for value in values]
BASES: dict[str, Callable[[], Any]] = {MAP: base_map, PROJECT: base_project, MANIFEST: base_manifest}


def run_changed(runner: Runner, file: str, path: tuple[Any, ...], value: Any) -> Outcome:
    """Run with ``value`` at ``path`` of the given file and everything else good."""
    changed = set_path(BASES[file](), path, value)
    return runner.run(**{{MAP: "map_document", PROJECT: "project_document", MANIFEST: "manifest_document"}[file]: changed})


def phrase_ok(value: Any, message: str, path: tuple[Any, ...] = ("x",)) -> bool:
    """A missing value must be reported as missing, a blank string as empty and a wrong whole document as not an object; any other bad value as a wrong
    type or shape."""
    if not path:
        return "the document: must be an object" in message
    if value is MISSING:
        return "is missing" in message
    if isinstance(value, str) and not value.strip():
        return "is empty" in message
    return any(phrase in message for phrase in ("must be", "is missing", "is empty"))


def label(value: Any) -> str:
    """A short readable test id for a bad value."""
    return "MISSING" if value is MISSING else repr(value)[:30]


def test_the_unchanged_inputs_give_a_clean_report(runner: Runner) -> None:
    """The base documents are valid: exit 0 with one root and one dictionary, so every failure below comes from the changed value."""
    outcome = runner.run()
    report = report_of(outcome)
    check(outcome.code == 0, f"{outcome.code} {outcome.err}")
    check([row["table"] for row in report["in_scope"]] == ["t_a"] and [row["table"] for row in report["dictionary"]] == ["d_a"], "scope")


def test_the_coverage_table_lists_each_read_path_once_with_values() -> None:
    """The table has no empty row and covers the three files and every container level."""
    check(all(values for _, _, values in COVERAGE), "a row without values")
    check({file for file, _, _ in COVERAGE} == {MAP, PROJECT, MANIFEST}, "a file is missing")
    check(len({(file, path) for file, path, _ in COVERAGE}) == len(COVERAGE), "a path is listed twice")
    check(len(ROWS) > 400, f"only {len(ROWS)} cases")


@pytest.mark.parametrize(("file", "path", "value"), ROWS, ids=[f"{file}:{'.'.join(map(str, path)) or 'root'}={label(value)}" for file, path, value in ROWS])
def test_a_bad_value_at_any_read_path_exits_2_naming_the_file_and_writes_no_report(runner: Runner, file: str, path: tuple[Any, ...], value: Any) -> None:
    """Wrong type, missing key, empty value or wrong container at a read path: exit 2, the file on stderr, no report, no exception."""
    outcome = run_changed(runner, file, path, value)
    check(outcome.code == 2, f"exit {outcome.code}: {outcome.out} {outcome.err}")
    check(FILE_NAMES[file] in outcome.err, f"{FILE_NAMES[file]} is not named: {outcome.err}")
    check(outcome.report is None, "a report was written")
    if file != MAP:  # the bronze loader reads the table map first and words its own errors
        check(phrase_ok(value, outcome.err, path), f"the message does not say what is wrong ({label(value)}): {outcome.err}")


def copy_tables_in(project: Any) -> Any:
    """The copy_tables value of a project document, or MISSING when it is not there."""
    variables = project.get("vars") if isinstance(project, dict) else None
    return variables["copy_tables"] if isinstance(variables, dict) and "copy_tables" in variables else MISSING


def direct_rows() -> list[tuple[str, tuple[Any, ...], Any]]:
    """The coverage rows that reach one validator directly: every table map and manifest row, and the copy_tables rows of the project file."""
    return [row for row in ROWS if row[0] != PROJECT or row[1][:2] == ("vars", "copy_tables")]


@pytest.mark.parametrize(
    ("file", "path", "value"), direct_rows(), ids=[f"{file}:{'.'.join(map(str, path)) or 'root'}={label(value)}" for file, path, value in direct_rows()]
)
def test_each_validator_refuses_the_bad_value_by_itself_with_the_right_words(file: str, path: tuple[Any, ...], value: Any) -> None:
    """The validators are the library API S4 and S5 call, and the command line shadows some of them (the bronze loader reads the map first): each is called
    directly, and so is derive_scope, which must refuse the same values with the input named and the kind of fault said."""
    changed = set_path(BASES[file](), path, value)
    validators = {
        MAP: (scope.validate_table_map, "the table map"),
        PROJECT: (scope.validate_copy_tables, "copy_tables"),
        MANIFEST: (scope.validate_manifest, "the manifest"),
    }
    validate, name = validators[file]
    subject = copy_tables_in(changed) if file == PROJECT else changed
    if subject is MISSING:
        return
    with pytest.raises(scope.ScopeError) as direct:
        validate(subject, "input.json")
    check("input.json" in str(direct.value) and phrase_ok(value, str(direct.value), path), f"{direct.value}")
    arguments = {MAP: base_map(), PROJECT: copy_tables_in(base_project()), MANIFEST: base_manifest()}
    arguments[file] = subject
    with pytest.raises(scope.ScopeError) as derived:
        scope.derive_scope(arguments[MANIFEST], arguments[MAP], arguments[PROJECT])
    check(name in str(derived.value) and phrase_ok(value, str(derived.value), path), f"{derived.value}")


# Shapes that must not matter.
ACCEPTED: list[tuple[str, tuple[Any, ...], list[Any]]] = [
    (MAP, ("tables", 0, "unknown_key"), [None, 5, "x", [], {"a": [1]}]),
    (MAP, ("unknown_key",), [None, 5, "x", [], {"a": [1]}]),
    (MAP, ("tables", 0, "roles"), [["data"]]),
    (PROJECT, ("unknown_key",), [None, 5, "x", [], {"a": [1]}]),
    (PROJECT, ("vars", "other"), [None, 5, "x", [], {"a": [1]}]),
    (MANIFEST, ("child_map",), [None, 5, "x", [], {"a": [1]}]),
    (MANIFEST, ("metadata", "extra"), [None, 5, "x", [], {"a": [1]}]),
    (MANIFEST, ("metadata", "dbt_version"), [MISSING, "1.2.3"]),
    (MANIFEST, ("sources", UID_T_A, "extra"), [None, 5, "x", [], {"a": [1]}]),
    (MANIFEST, ("nodes", MODEL, "extra"), [None, 5, "x", [], {"a": [1]}]),
    (MANIFEST, ("nodes", TEST_NODE, "depends_on"), [MISSING, None, 5, "x", [], {"nodes": 5}]),
    (MANIFEST, ("nodes", TEST_NODE, "name"), [MISSING, None, 5]),
]
ACCEPTED_ROWS = [(file, path, value) for file, path, values in ACCEPTED for value in values]


@pytest.mark.parametrize(
    ("file", "path", "value"), ACCEPTED_ROWS, ids=[f"{file}:{'.'.join(map(str, path))}={label(value)}" for file, path, value in ACCEPTED_ROWS]
)
def test_an_unexpected_extra_shape_that_nothing_reads_is_ignored(runner: Runner, file: str, path: tuple[Any, ...], value: Any) -> None:
    """An unknown key, a spare variable, a non-bronze source or a test node's own fields may hold anything: exit 0 and the report scope is unchanged."""
    outcome = run_changed(runner, file, path, value)
    report = report_of(outcome)
    check(outcome.code == 0, f"exit {outcome.code}: {outcome.out} {outcome.err}")
    check([row["table"] for row in report["in_scope"]] == ["t_a"] and report["counts"]["leaving"] == 1, "the scope changed")


# Completeness: every path of every base document is classified, and every one is fuzzed.
def walk(document: Any, prefix: tuple[Any, ...] = ()) -> list[tuple[Any, ...]]:
    """Every path (containers and leaves, never the empty path) of a JSON-like document."""
    found: list[tuple[Any, ...]] = []
    if isinstance(document, dict):
        for key, value in document.items():
            found.append((*prefix, key))
            found.extend(walk(value, (*prefix, key)))
    elif isinstance(document, list):
        for index, value in enumerate(document):
            found.append((*prefix, index))
            found.extend(walk(value, (*prefix, index)))
    return found


def generic(path: tuple[Any, ...]) -> tuple[Any, ...]:
    """A path with every list position replaced by ``*``, so a row for ``tables[0]`` also stands for ``tables[1]``."""
    return tuple("*" if isinstance(step, int) else step for step in path)


# Paths in the base documents that no rule of this tool reads (the bronze loader reads the first three groups and rejects bad values with its own words,
# which the command line turns into exit 2; the fuzz below covers them).
IGNORED = {
    MAP: [
        ("version",),
        ("namespace",),
        ("classification",),
        ("tables", "*", "dataset_ids"),
        ("tables", "*", "encodings"),
        ("tables", "*", "dictionary", "file_pattern"),
    ],
    PROJECT: [("name",), ("version",), ("config-version",), ("profile",), ("vars", "other")],
    MANIFEST: [],
}


@pytest.mark.parametrize("file", [MAP, PROJECT, MANIFEST])
def test_every_path_of_every_base_document_is_classified(file: str) -> None:
    """A path that is neither a coverage row, an accepted extra nor listed as unread is a value the table does not mention: a new read needs a row."""
    known = [generic(path) for f, path, _ in COVERAGE + ACCEPTED if f == file] + [generic(path) for path in IGNORED[file]]
    unclassified = [
        path for path in walk(BASES[file]()) if not any(generic(path)[: len(row)] == row or row[: len(generic(path))] == generic(path) for row in known)
    ]
    check(unclassified == [], f"{file}: paths with no row in the table: {unclassified}")


@pytest.mark.parametrize("file", [MAP, PROJECT, MANIFEST])
def test_every_path_of_every_base_document_survives_arbitrary_values(runner: Runner, file: str) -> None:
    """Each path of the base document, deleted and then set to seeded arbitrary values: an exit 0 report, a printed scope problem or a named exit 2."""
    rng = Lcg(f"scope-every-path-{file}")
    paths = walk(BASES[file]())
    check(len(paths) >= 8, f"{len(paths)} paths")
    for path in paths:
        graceful(run_changed(runner, file, path, MISSING), file)
        for _ in range(8):
            graceful(run_changed(runner, file, path, json_like(rng)), file)


# Whole-file shapes.
GARBAGE_TEXT = [
    "",
    " ",
    "\n\n",
    "[",
    "{",
    '{"tables": [}',
    "}{",
    "null",
    "- a\n- b\n",
    "a: b: c",
    "\t\t",
    "\x00",
    "[" * 200000,
    "{" * 200000,
    "x: " + "[" * 200000,
]


@pytest.mark.parametrize("text", GARBAGE_TEXT, ids=[f"{index}_{text[:8]!r}" for index, text in enumerate(GARBAGE_TEXT)])
def test_a_table_map_file_of_any_text_exits_2_naming_the_file(runner: Runner, text: str) -> None:
    """Empty, truncated, non-JSON or absurdly nested text as the table map file: exit 2, the file named, no report."""
    outcome = runner.run(map_text=text)
    check(outcome.code == 2 and "map.json" in outcome.err and outcome.report is None, f"{outcome.code}: {outcome.err[:200]}")


@pytest.mark.parametrize(
    "text", [*GARBAGE_TEXT, "vars:\n  copy_tables:\n    1: fam\n", "vars:\n  copy_tables:\n    ~: fam\n", "vars:\n  copy_tables:\n    true: fam\n"]
)
def test_a_project_file_of_any_text_exits_2_naming_the_file(runner: Runner, text: str) -> None:
    """Text that is not a project file with a copy_tables mapping of string pairs: exit 2, the file named, no report."""
    outcome = runner.run(project_text=text)
    check(outcome.code == 2 and "dbt_project.yml" in outcome.err and outcome.report is None, f"{outcome.code}: {outcome.err[:200]}")


@pytest.mark.parametrize("data", [b"\xff\xfe\x00bad", b"vars: \xc3\x28\n", b"\x00" * 50])
def test_a_project_file_of_undecodable_bytes_exits_2_naming_the_file(runner: Runner, data: bytes) -> None:
    """Bytes that are not text: exit 2, the project file named, no report."""
    outcome = runner.run(project_bytes=data)
    check(outcome.code == 2 and "dbt_project.yml" in outcome.err and outcome.report is None, f"{outcome.code}: {outcome.err[:200]}")


YAML_SHAPES = [
    "vars:\n  copy_tables: !!set {a, b}\n",
    "vars:\n  copy_tables:\n    ? [a, b]\n    : fam\n",
    "vars:\n  copy_tables:\n    t_a: 2026-11-18\n",
    "vars:\n  copy_tables:\n    t_a: &x [1]\n    t_b: *x\n",
    "vars: !!omap [a: 1]\n",
    "vars:\n  copy_tables: &c {t_a: fam}\n  again: *c\n  bad: *missing\n",
]


@pytest.mark.parametrize("text", YAML_SHAPES, ids=[str(index) for index in range(len(YAML_SHAPES))])
def test_yaml_only_shapes_in_the_project_file_exit_2(runner: Runner, text: str) -> None:
    """YAML features that JSON cannot write (sets, complex keys, dates, aliases): exit 2 naming the project file."""
    outcome = runner.run(project_text=text)
    check(outcome.code == 2 and "dbt_project.yml" in outcome.err and outcome.report is None, f"{outcome.code}: {outcome.err[:200]}")


MANIFEST_BYTES = [
    b"",
    b"\xff\xfe\x00bad",
    b"{",
    b"null",
    b"5",
    b'"x"',
    b"[]",
    b"[" * 200000,
    b'{"nodes": [}',
    b"1" * 6000,
    b"{}",
    b'{"nodes": {}, "sources": {}}',
]


@pytest.mark.parametrize("data", MANIFEST_BYTES, ids=[f"{index}_{data[:8]!r}" for index, data in enumerate(MANIFEST_BYTES)])
def test_a_manifest_file_of_any_bytes_exits_2_naming_the_manifest(runner: Runner, data: bytes) -> None:
    """The parse leaves bytes that are not a manifest (undecodable, truncated, a scalar, too deep, missing keys): exit 2 naming manifest.json."""
    outcome = runner.run(manifest_bytes=data)
    check(outcome.code == 2 and "manifest.json" in outcome.err and outcome.report is None, f"{outcome.code}: {outcome.err[:200]}")


# Property-style: seeded arbitrary values at every read path.
class Lcg:
    """A small deterministic pseudo-random source (a linear congruential generator), so the arbitrary values are the same on every machine and run."""

    def __init__(self, seed: str) -> None:
        self.state = zlib.crc32(seed.encode()) or 1

    def next(self) -> int:
        """The next 31-bit number."""
        self.state = (self.state * 1103515245 + 12345) % 2**31
        return self.state >> 4

    def randint(self, low: int, high: int) -> int:
        """A number from low to high inclusive."""
        return low + self.next() % (high - low + 1)

    def choice(self, items: list[Any] | str) -> Any:
        """One item."""
        return items[self.randint(0, len(items) - 1)]

    def random(self) -> float:
        """A number from 0 up to 1."""
        return self.next() / 2**27


def json_like(rng: Lcg, depth: int = 0) -> Any:
    """An arbitrary JSON-like value: scalars of every type (large ints, NaN, empty and odd strings), and nested lists and objects."""
    kinds = ["none", "bool", "int", "bigint", "float", "nan", "str", "empty", "name", "list", "dict", "key_dict"]
    kind = rng.choice(kinds if depth < 3 else kinds[:9])
    simple: dict[str, Any] = {
        "none": None,
        "bool": rng.choice([True, False]),
        "int": rng.randint(-5, 5),
        "bigint": 10 ** rng.randint(20, 40),
        "float": rng.random() * 100,
        "nan": float("nan"),
        "str": "".join(rng.choice("abc é‮\n\t'\"{}[]") for _ in range(rng.randint(1, 12))),
        "empty": "",
        "name": rng.choice(["t_a", "d_a", "t_x", "csv", "x/a", "data", "table", "tables", "group", "name", "nodes", "vars"]),
    }
    if kind in simple:
        return simple[kind]
    if kind == "list":
        return [json_like(rng, depth + 1) for _ in range(rng.randint(0, 3))]
    keys = ["table", "group", "collection", "format", "name", "nodes", "tables", "source_name", "resource_type", "depends_on", "dictionary", "roles"]
    return {(rng.choice(keys) if kind == "key_dict" else f"k{index}"): json_like(rng, depth + 1) for index in range(rng.randint(0, 3))}


def graceful(outcome: Outcome, file: str) -> None:
    """An outcome is acceptable when it is a report (exit 0), a printed scope problem with no report (exit 1) or a named input error (exit 2)."""
    if outcome.code == 0:
        check(outcome.report is not None and outcome.report["kind"] == scope.KIND, "exit 0 without a valid report")
    elif outcome.code == 1:
        check(
            outcome.report is None and "scope report: fail" in outcome.out and "Traceback" not in outcome.err,
            f"exit 1 without a scope problem: {outcome.out} {outcome.err}",
        )
    else:
        check(outcome.code == 2 and FILE_NAMES[file] in outcome.err and outcome.report is None, f"exit {outcome.code}: {outcome.err[:300]}")


@pytest.mark.parametrize("file", [MAP, PROJECT, MANIFEST])
def test_arbitrary_values_at_every_read_path_never_escape_as_an_exception(runner: Runner, file: str) -> None:
    """Seeded arbitrary JSON-like values at each read path (and anywhere new keys could sit): never an uncaught exception, never an unexplained exit 1."""
    rng = Lcg(f"scope-inputs-{file}")
    paths = sorted({path for f, path, _ in COVERAGE + ACCEPTED if f == file}, key=repr)
    count = 0
    for path in paths:
        for _ in range(12):
            graceful(run_changed(runner, file, path, json_like(rng)), file)
            count += 1
    check(len(paths) >= 4 and count == 12 * len(paths), f"{len(paths)} paths, {count} runs")


@pytest.mark.parametrize("file", [MAP, PROJECT, MANIFEST])
def test_arbitrary_whole_documents_never_escape_as_an_exception(runner: Runner, file: str) -> None:
    """Whole inputs replaced by arbitrary JSON-like values."""
    rng = Lcg(f"scope-documents-{file}")
    for _ in range(60):
        graceful(run_changed(runner, file, (), json_like(rng)), file)


# Duplicate diagnostics, every permutation and multiplicity.
def three_definitions() -> dict[str, dict[str, Any]]:
    """Three different definitions (and one annotation-only variant) of the same table row, from graph 1's first root."""
    report, _ = graph_1()
    original = report["in_scope"][0]
    return {
        "A": original,
        "B": {**original, "analytical_readers": ["different_reader"]},
        "C": {**original, "source_family": "other_family"},
        "N": {**original, "notes": ["annotation only"]},
    }


# Per group: two different projected fields to change (B and C), and the value each takes.
CHANGES = {
    "in_scope": (("analytical_readers", ["different_reader"]), ("source_family", "other_family")),
    "dictionary": (("required_by", ["nobody"]), ("silver_pin", True)),
    "leaving": (("reason", "other_reason"), ("group", "other_group")),
}


def variants_of(group: str, report: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """The four rows A (as is), B and C (one projected field changed each) and N (an annotation only) of a group's first row."""
    base = report[group][0]
    (b_key, b_value), (c_key, c_value) = CHANGES[group]
    return {"A": base, "B": {**base, b_key: b_value}, "C": {**base, c_key: c_value}, "N": {**base, "notes": ["annotation only"]}}


@pytest.mark.parametrize("group", ["in_scope", "dictionary", "leaving"])
@pytest.mark.parametrize("side", ["expected", "report"])
def test_exactly_one_diagnostic_per_conflicting_table_for_every_order_and_multiplicity(group: str, side: str) -> None:
    """Every sequence of two to five rows drawn from A, B, C and N for one table: one problem when two or more distinct rows occur, none when all are the
    same, and the same text for the same set of distinct rows whatever the order or the number of repeats."""
    report, _ = graph_1()
    variants = variants_of(group, report)
    table = report[group][0]["table"]
    by_distinct: dict[frozenset[str], set[str]] = {}
    cases = 0
    for length in range(2, 6):
        for names in itertools.product("ABCN", repeat=length):
            document = copy.deepcopy(report)
            document[group] = [copy.deepcopy(variants[name]) for name in names] + document[group][1:]
            found = scope.equality_problems(document, report) if side == "report" else scope.equality_problems(report, document)
            cases += 1
            if len(set(names)) == 1:
                # Identical repeats are one definition: A and N equal the other side in the projection; B and C differ from it in one field.
                check(all("more than once" not in line for line in found), f"{names}: identical rows are no duplicate conflict: {found}")
                check((found == []) == (names[0] in "AN"), f"{names}: {found}")
                continue
            check(len(found) == 1 and table in found[0] and "more than once" in found[0], f"{names}: {found}")
            by_distinct.setdefault(frozenset(names), set()).add(found[0])
    check(cases == 4**2 + 4**3 + 4**4 + 4**5, f"{cases} sequences")
    check(len(by_distinct) == 11, f"the distinct sets seen: {sorted(map(sorted, by_distinct))}")
    check(all(len(messages) == 1 for messages in by_distinct.values()), "the text depends on order or repeats")
    for distinct, messages in by_distinct.items():
        projections = {scope._canonical({key: variants[name].get(key) for key in scope.PROJECTION[group]}) for name in distinct}
        check(("outside the projection" in next(iter(messages))) == (len(projections) == 1), f"{sorted(distinct)}: {messages}")


def test_every_distinct_definition_is_shown() -> None:
    """With three conflicting definitions the one diagnostic shows all three."""
    report, _ = graph_1()
    variants = three_definitions()
    document = copy.deepcopy(report)
    document["in_scope"] = [copy.deepcopy(variants[name]) for name in "CABB"] + document["in_scope"][1:]
    found = scope.equality_problems(report, document)
    check(len(found) == 1, f"{found}")
    check(
        all(
            json.dumps(variants[name][key], separators=(",", ":")) in found[0]
            for name, key in (("A", "analytical_readers"), ("B", "analytical_readers"), ("C", "source_family"))
        ),
        found[0],
    )


def test_the_two_row_orders_of_the_round_2_review_give_one_diagnostic_each() -> None:
    """[A, B, B] and [B, B, A] (the multiset the review used) both give exactly one diagnostic, and it is the same text."""
    report, _ = graph_1()
    variants = three_definitions()
    first = copy.deepcopy(report)
    first["in_scope"] = [variants["A"], variants["B"], variants["B"]] + first["in_scope"][1:]
    second = copy.deepcopy(report)
    second["in_scope"] = [variants["B"], variants["B"], variants["A"]] + second["in_scope"][1:]
    one, two = scope.equality_problems(report, first), scope.equality_problems(report, second)
    check(len(one) == 1 and one == two, f"{one} against {two}")


# Shapes of the documents that equality compares.
EQUALITY_BAD: list[tuple[tuple[Any, ...], list[Any], str]] = [
    ((), [None, 5, "x", []], "the document is not an object"),
    (("counts",), [MISSING, None, 5, "x", []], "counts is missing or not an object"),
    (("in_scope",), [MISSING, None, 5, "x", {}], "in_scope is missing or not a list"),
    (("in_scope", 0), [None, 5, "x", []], "in_scope[0] is not an object"),
    (("in_scope", 0, "table"), [None, 5, []], "in_scope[0].table is not a string"),
    (("silver_source_declaration",), [MISSING, None, 5, "x", []], "silver_source_declaration is missing or not an object"),
    (("silver_source_declaration", "pins"), [MISSING, None, 5, "x", []], "silver_source_declaration.pins is missing or not an object"),
    (
        ("silver_source_declaration", "glue_mirror_names"),
        [MISSING, None, 5, "x", []],
        "silver_source_declaration.glue_mirror_names is missing or not an object",
    ),
]
EQUALITY_ROWS = [(path, value, phrase) for path, values, phrase in EQUALITY_BAD for value in values]


@pytest.mark.parametrize("side", ["report", "expected"])
@pytest.mark.parametrize(
    ("path", "value", "phrase"), EQUALITY_ROWS, ids=[f"{'.'.join(map(str, path)) or 'root'}={label(value)}" for path, value, _ in EQUALITY_ROWS]
)
def test_equality_names_a_malformed_document_instead_of_raising(side: str, path: tuple[Any, ...], value: Any, phrase: str) -> None:
    """A report or an expected document with a wrong shape at a read path is the named problem, on either side, and only that problem."""
    report, _ = graph_1()
    broken = set_path(report, path, value)
    found = scope.equality_problems(broken, report) if side == "report" else scope.equality_problems(report, broken)
    word = "report" if side == "report" else "expected document"
    check(found == [f"{word}: {phrase}"], f"{found}")
