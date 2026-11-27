"""Bind, classify and check the ingestion registry (design v4 step S4).

``check`` tests that the registry files agree with each other and with the code and data they describe (integrity predicates I1 to I12).
``check --release`` adds the usable-input readiness predicates R1 to R5. Both always print a READINESS block. Nothing here reads S3 or the network, and
nothing in the live pipeline calls it: it reads files and writes only ``registry.lock.json`` (``lock``) and ``unit_golden.json`` (``golden``).

Both commands derive the bronze scope from the checked tree's own dbt project (``scripts.ingestion.scope``). A supplied scope report is only compared with the
derived one: a difference is an I1 problem, and a report that cannot be read or is no JSON object is an I2 problem. The schemas are the checked tree's own.

The READINESS block is computed when the queue, the table map and the derived scope are the locked ones. No other file and no other failure removes it.

Commands (the exit code is 0 for a pass, 1 for named problems and 2 when the check cannot run)::

    python -m scripts.ingestion.registry check [--release] [--root DIR] [--scope-report FILE]
    python -m scripts.ingestion.registry lock [--root DIR] [--scope-report FILE]
    python -m scripts.ingestion.registry golden [--root DIR]

``golden`` writes the unit golden list from the queue and capture specifications with the capture code's own unit keys. Run it before ``lock`` and review both.

The end-to-end run of the committed files and of one failing mutation per integrity row writes a JSON record:
``python -m scripts.ingestion.run_registry_e2e --out <record.json>``.
"""

from __future__ import annotations

import argparse
import contextlib
import json
import re
import sys
from collections import defaultdict
from collections.abc import Collection, Iterable, Mapping, Sequence
from pathlib import Path
from typing import Any, NamedTuple

from scripts.acquisition import (
    ahrq_compendium_contract,
    capture_mode,
    cms_owners_contract,
    hcai_util_contract,
    hud_xlsx_contract,
    manual_content,
    onc_mu_contract,
    redownload,
    storage_controls,
    stored_mirror,
    wonder_export_contract,
)
from scripts.acquisition.code_closure import closure_hashes
from scripts.acquisition.code_versions import read_code_versions
from scripts.acquisition.dataset_layout import source_folder
from scripts.acquisition.source_registry import RegistryError
from scripts.infrastructure import startup_check
from scripts.ingestion import registry_readiness, scope
from scripts.ingestion import registry_records as records
from scripts.ingestion.registry_model import MANUAL_ROUTE, NEEDS, Output, Problem, RegistryFileError, Result, Tree, canonical, default_scope, sha256_bytes
from scripts.lakehouse import bronze, dbt_package_archive
from scripts.quality import dbt_manifest_labels

ROOT = Path(__file__).resolve().parents[2]
INGESTION = "config/ingestion"
LOCK = f"{INGESTION}/registry.lock.json"
SCHEMAS = f"{INGESTION}/schemas"
ACQUISITION = "config/acquisition"
BASELINE = f"{ACQUISITION}/baseline_run"
TABLE_MAP = "config/lakehouse/bronze_tables.json"
RETIRED_OBJECTS = "config/lakehouse/retired_objects.json"
# The tables that left the bronze load. Their collections keep their records here: retention is a fixed rule (design v4 section 2).
LEAVING_RECORD = "config/lakehouse/bronze_leaving_scope.json"
LAYOUT = f"{ACQUISITION}/collection_layout.json"
SOURCE_REGISTRY = f"{ACQUISITION}/source_registry.json"
SEED_MANIFEST = f"{ACQUISITION}/bootstrap_seed/manifest.json"
QUEUE = f"{BASELINE}/queue.json"
SPECS = f"{BASELINE}/capture_specs.json"
AUTHORED = {
    "collections": f"{INGESTION}/collections.json",
    "manual_outcomes": f"{INGESTION}/manual_outcomes.json",
    "input_evidence": f"{INGESTION}/input_evidence.json",
    "layout_contracts": f"{INGESTION}/layout_contracts.json",
    "unit_golden": f"{INGESTION}/unit_golden.json",
}
# The folders of a dbt project that the parse does not read: it installs packages and writes its outputs in a scratch folder.
NOT_PARSE_INPUTS = frozenset({"dbt_packages", "target", "logs"})
# The report bytes derived in this process, by the digest of the parse inputs.
PARSED: dict[str, bytes] = {}
# No scope report was supplied. None is a supplied value: a report that is the JSON null.
NOT_SUPPLIED: Any = object()
# A scope report was supplied and its file cannot be read.
UNREADABLE_REPORT: Any = object()
# What reading inputs of a wrong shape raises.
INPUT_ERRORS = (
    RegistryFileError,
    scope.ScopeError,
    capture_mode.SpecError,
    AttributeError,
    LookupError,
    TypeError,
    ValueError,
    ArithmeticError,
    RecursionError,
    OSError,
)
# What stops a command that cannot run at all.
CANNOT_RUN = (*INPUT_ERRORS, dbt_manifest_labels.ManifestError, dbt_package_archive.ArchiveError)
FROZEN = (TABLE_MAP, RETIRED_OBJECTS, LEAVING_RECORD, LAYOUT, SOURCE_REGISTRY, SEED_MANIFEST)
STEP = re.compile(r"S(?:0|1|2|3|4|5|6a|6b|7|8|8a|9|10|11|12|13a|13b|14)")
STEP_LIST = re.compile(rf"{STEP.pattern}(?:, {STEP.pattern})*")
SEAT_RULES: dict[str, list[str]] = {
    "plan": ["ADAPTERS (capture_mode)", "ADAPTERS (redownload)", "LINEAGE_ID", "PLAN_MODES", "PLAN_PATHS", "PLANNED"],
    "mmd": ["ADAPTERS (capture_mode)", "ADAPTERS (redownload)", "PLAN_MODES"],
    "manual": ["ADAPTERS (redownload)", "PLANNED"],
    "file": ["URL_BOUND"],
    "seed": ["MODES (manual_content)", "SEED_ONLY_MODES"],
}
SEAT_CLASS = {
    "file": "file",
    "mmd_api": "mmd",
    "hud_xlsx": "manual",
    "wonder_export": "manual",
    "ahrq_compendium": "seed",
    "hospital_archive": "seed",
}


class Entry(NamedTuple):
    """One registered collector entry point: its name, the stem of its committed version list and the module whose closure the list must hold."""

    name: str
    stem: str
    module: str


ENTRY_POINTS = (
    Entry("bls_api", "bls_api", "scripts.acquisition.collect_bls_api"),
    Entry("census_acs_api", "census_acs_api", "scripts.acquisition.collect_census_acs_api"),
    Entry("census_acs_detailed", "census_acs_detailed", "scripts.acquisition.collect_census_acs_detailed"),
    Entry("census_acs_tables", "census_acs_tables", "scripts.acquisition.collect_census_acs_tables"),
    Entry("hud_api", "hud_api", "scripts.acquisition.collect_hud_api"),
    Entry("hud_api_workbook", "hud_api", "scripts.acquisition.store_hud_xlsx"),
    Entry("mmd_api", "mmd_api", "scripts.acquisition.collect_mmd_api"),
    Entry("nber_hc", "nber_hc", "scripts.acquisition.store_nber_hc"),
    Entry("reference_download", "reference_download", "scripts.acquisition.store_reference_download"),
    Entry("wonder_export", "wonder_export", "scripts.acquisition.store_wonder_export"),
    Entry("cms_owners", "cms_owners", cms_owners_contract.CODE_ENTRY),
    Entry("hcai_util", "hcai_util", hcai_util_contract.CODE_ENTRY),
    Entry("onc_mu", "onc_mu", onc_mu_contract.CODE_ENTRY),
)


class LockRefused(Exception):
    """The lock command writes nothing: the problems say why."""

    def __init__(self, problems: list[Problem]) -> None:
        super().__init__("; ".join(map(str, problems)))
        self.problems = problems


def encode(document: Mapping[str, Any]) -> bytes:
    """A registry file as written: sorted keys, two-space indent, one trailing newline."""
    return (json.dumps(document, indent=2, sort_keys=True) + "\n").encode()


def unrepeated(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    """A JSON object with each key once: the parser would keep the last of two and hide the other."""
    found = dict(pairs)
    if len(found) != len(pairs):
        raise ValueError("an object repeats a key")
    return found


def read_json(root: Path, relative: str) -> Any:
    """A file of the tree parsed as JSON; a missing, unreadable or malformed file, or one that repeats a key, is a RegistryFileError naming it."""
    try:
        return json.loads((root / relative).read_bytes(), object_pairs_hook=unrepeated)
    except OSError as error:
        raise RegistryFileError(f"{relative} cannot be read: {error.strerror or type(error).__name__}") from error
    except (ValueError, RecursionError) as error:
        raise RegistryFileError(f"{relative} is not valid JSON: {error}") from error


def map_digest(root: Path) -> str:
    """The digest the scope report records for the table map: SHA-256 of the canonical JSON of the loaded map."""
    return sha256_bytes(json.dumps(scope.load_map(root / TABLE_MAP), sort_keys=True, separators=(",", ":")).encode())


def parse_inputs_digest(root: Path) -> str:
    """One digest of what the scope of a tree is derived from: the files of its dbt project, its table map and its package record."""
    project = root / "dbt"
    found = [path for path in sorted(project.rglob("*")) if path.is_file() and not NOT_PARSE_INPUTS & set(path.relative_to(project).parts)]
    found += [root / TABLE_MAP, root / dbt_package_archive.RECORD]
    return sha256_bytes(canonical([[path.relative_to(root).as_posix(), sha256_bytes(path.read_bytes()) if path.is_file() else None] for path in found]))


def derived_scope(root: Path) -> dict[str, Any]:
    """The scope report of the tree's own dbt project: the one scope both commands use. A process parses each distinct set of parse inputs once."""
    root = root.resolve()
    key = parse_inputs_digest(root)
    if key not in PARSED:
        report, problems, _ = scope.scope_of_project(root / "dbt", root / TABLE_MAP, root)
        if problems:
            raise scope.ScopeError("; ".join(problems))
        PARSED[key] = scope.report_bytes(report)
    return json.loads(PARSED[key])


def read_supplied(path: Path) -> Any:
    """The value of a supplied scope report. Text that is not JSON reads as None, which the comparison refuses as no JSON object, and a file that cannot be
    read is UNREADABLE_REPORT, which it refuses by name."""
    try:
        return json.loads(path.read_bytes())
    except OSError:
        return UNREADABLE_REPORT
    except (ValueError, RecursionError):
        return None


def supplied_scope_problems(derived: Mapping[str, Any], supplied: Any) -> list[Problem]:
    """A supplied scope report is compared with the derived one and never used: a difference is an I1 problem."""
    if supplied is NOT_SUPPLIED:
        return []
    if supplied is UNREADABLE_REPORT:
        return [Problem("I2", "scope report", "cannot be read")]
    if not isinstance(supplied, Mapping):
        return [Problem("I2", "scope report", "is not a JSON object")]
    try:
        same = scope.report_bytes(supplied) == scope.report_bytes(derived)
    except (TypeError, ValueError, RecursionError):
        same = False
    return [] if same else [Problem("I1", "scope report", "differs from the report derived from the tree's dbt project, which is the one used")]


def lock_of(plan: str) -> str:
    """The companion lock of a plan, by the name its loader reads."""
    return Path(plan).with_suffix(".lock.json").as_posix()


def runtime_plans() -> list[str]:
    """The locked plans the runtime's own lists name: each plan-based collector's (``startup_check.PLAN_PATHS``) and each manual content handler's."""
    paths = [path for plans in startup_check.PLAN_PATHS.values() for path in plans()]
    paths += [contract.PLAN_PATH for contract in (hud_xlsx_contract, wonder_export_contract, ahrq_compendium_contract)]
    return sorted({path.relative_to(ROOT).as_posix() for path in paths})


def schema_path(name: str) -> str:
    return f"{SCHEMAS}/{name}.schema.json"


def bound_paths(root: Path) -> list[str]:
    """Every file the lock binds: the authored files and schemas, the frozen inputs, the baseline run, and the runtime's and the classified plans, each with
    its companion lock. A required schema, a plan or a lock is bound whether or not the tree holds it, so a missing one fails by name."""
    found = {*AUTHORED.values(), *FROZEN, *map(schema_path, records.REQUIRED_SCHEMAS)}
    for folder in (SCHEMAS, BASELINE):
        found.update(path.relative_to(root).as_posix() for path in (root / folder).rglob("*") if path.is_file())
    for plan in runtime_plans():
        found.update((plan, lock_of(plan)))
    try:
        for plan in read_json(root, AUTHORED["collections"]).get("plans", []):
            found.update((plan["path"], lock_of(plan["path"])))
    except (RegistryFileError, AttributeError, KeyError, TypeError, ValueError):
        pass
    return sorted(found)


def load_lock(root: Path, schemas: records.Schemas) -> tuple[dict[str, Any] | None, list[Problem]]:
    """The lock document, or the problem that names why it cannot be used."""
    try:
        lock = read_json(root, LOCK)
    except RegistryFileError as error:
        text = f"{LOCK} is missing: run the lock command" if not (root / LOCK).exists() else str(error)
        return None, [Problem("lock", LOCK, text)]
    errors = schema_errors(schemas, "registry_lock", lock) or schemas.needs_errors("registry_lock", lock)
    return (None, [Problem("I2", LOCK, text) for text in errors]) if errors else (lock, [])


def schema_errors(schemas: records.Schemas, name: str, document: Any) -> list[str]:
    """The first few places a document breaks its JSON schema, as positions and keywords only: no input value reaches a diagnostic."""
    try:
        return schemas.errors(name, document, limit=5)
    except records.Refusal as refusal:
        return [str(refusal)]


def lock_files(lock: Mapping[str, Any]) -> dict[str, str]:
    """The lock's bound files as a path to SHA-256 map. The lock lists them as path and digest records, sorted by path."""
    return {row["path"]: row["sha256"] for row in lock["files"]}


def lock_problems(root: Path, schemas: records.Schemas | None = None) -> list[Problem]:
    """I1: each bound file's SHA-256 equals its lock entry, the lock binds exactly the bound set, lists each file once and names the source registry it binds
    as its parent."""
    schemas = schemas or records.Schemas(root)
    lock, problems = load_lock(root, schemas)
    if lock is None:
        missing = [Problem("I1", name, "is a required schema the tree does not hold") for name in map(schema_path, records.REQUIRED_SCHEMAS)]
        return [problem for problem in missing if not (root / problem.subject).is_file()] + problems + draft_problems(root, schemas)
    wanted, bound = set(bound_paths(root)), lock_files(lock)
    listed = [row["path"] for row in lock["files"]]
    problems += [Problem("I1", name, "is listed by the lock more than once") for name in sorted({name for name in listed if listed.count(name) > 1})]
    parent = lock["parents"]["source_registry"]
    if (parent["path"], parent["sha256"]) != (SOURCE_REGISTRY, bound.get(SOURCE_REGISTRY)):
        problems.append(Problem("I1", "source registry parent", f"differs from the lock's own entry for {SOURCE_REGISTRY} (path or digest)"))
    for name in sorted(wanted - set(bound)):
        problems.append(Problem("I1", name, "is a bound file the lock does not list: regenerate the lock and review it"))
    for name in sorted(set(bound) - wanted):
        problems.append(Problem("I1", name, "is listed by the lock but is not a bound file"))
    for name in sorted(wanted & set(bound)):
        try:
            actual = sha256_bytes((root / name).read_bytes())
        except (OSError, ValueError):
            problems.append(Problem("I1", name, "is bound by the lock but cannot be read"))
            continue
        if actual != bound[name]:
            problems.append(Problem("I1", name, f"differs from the lock (lock {bound[name][:12]}, file {actual[:12]})"))
    return problems + draft_problems(root, schemas) if problems else problems


def draft_problems(root: Path, schemas: records.Schemas) -> list[Problem]:
    """I2 for a tree whose lock does not bind it, where the other predicates do not run: each authored draft that lacks what its readers need. A draft that
    cannot be read at all is named by its own binding."""
    problems: list[Problem] = []
    for name, relative in AUTHORED.items():
        if name not in NEEDS:
            continue
        try:
            document = read_json(root, relative)
        except RegistryFileError:
            continue
        predicate = "I12" if name == "layout_contracts" else "I2"
        problems += [Problem(predicate, relative, text) for text in schemas.needs_errors(name, document)]
    return problems


def load_queue(root: Path) -> Tree:
    """The queue, the capture specifications, the routes and the table map: what the queued outputs and their tables come from. Everything else stays empty.
    A file that cannot be read raises RegistryFileError naming it."""
    queue, specs = read_json(root, QUEUE), read_json(root, SPECS)
    layout = read_json(root, LAYOUT)
    routes = {source: f"{group['publisher']}/{group['collection']}" for group in layout["groups"] for source in group["source_ids"]}
    return Tree(
        root=root,
        units=queue["queue"],
        specs=specs["specs"],
        routes=routes,
        tables=scope.load_map(root / TABLE_MAP)["tables"],
        collections={},
        golden={},
        manual={},
        contracts={},
        lock={},
        source_registry={},
    )


def load_inputs(root: Path, lock: dict[str, Any]) -> Tree:
    """The queue's tree with the lock and the frozen records. The authored files stay empty until :func:`load_authored`."""
    tree = load_queue(root)
    tree.lock = lock
    tree.source_registry = read_json(root, SOURCE_REGISTRY)
    tree.leaving = frozenset(row["collection"] for row in read_json(root, LEAVING_RECORD)["tables"])
    return tree


def load_authored(tree: Tree) -> Tree:
    """Add the authored files to a tree of inputs."""
    tree.collections = read_json(tree.root, AUTHORED["collections"])
    tree.golden = read_json(tree.root, AUTHORED["unit_golden"])
    tree.manual = read_json(tree.root, AUTHORED["manual_outcomes"])
    tree.contracts = read_json(tree.root, AUTHORED["layout_contracts"])
    return tree


def unit_class(unit: Mapping[str, Any]) -> str:
    if unit["route"] == MANUAL_ROUTE:
        return "manual"
    return "file" if unit["mode"] == "file" else "plan_mode"


def unit_key(unit: dict[str, Any], spec: dict[str, Any]) -> tuple[str, str]:
    """The unit's key and its kind. The request key needs a mode the capture code knows; a manual unit and a mode no seat lists keep the queue's snapshot ID."""
    if unit["route"] == MANUAL_ROUTE or redownload.handler_mode(unit) not in capture_mode.COLLECTOR_MODES | {"file"}:
        return unit["snapshot_id"], "snapshot_id"
    return capture_mode.unit_key_of(unit, spec), "unit_key"


def unit_counts(units: Sequence[Mapping[str, Any]]) -> dict[str, int]:
    """The golden list's counts of a queue: all units, the receipt-bearing ones and each class."""
    counts = {"units": len(units), "receipt_bearing": sum(1 for unit in units if unit.get("receipt"))}
    counts.update({name: sum(1 for unit in units if unit_class(unit) == name) for name in ("file", "plan_mode", "manual")})
    return counts


def build_golden(root: Path) -> dict[str, Any]:
    """The golden unit list of the tree's queue and capture specifications, with the capture code's own unit keys."""
    units, specs = read_json(root, QUEUE)["queue"], read_json(root, SPECS)["specs"]
    rows = []
    for unit in units:
        try:
            key, kind = unit_key(unit, specs[unit["snapshot_id"]])
        except (KeyError, capture_mode.SpecError) as error:
            raise RegistryFileError(f"{SPECS}: unit {unit['snapshot_id']} has no usable key: {type(error).__name__}") from error
        rows.append({"unit": key, "kind": kind, "class": unit_class(unit), "receipt": bool(unit.get("receipt"))})
    if len({row["unit"] for row in rows}) != len(rows):
        raise RegistryFileError(f"{QUEUE}: two units have one key")
    return {"kind": "unit_golden", "schema_version": 1, "counts": unit_counts(units), "units": sorted(rows, key=lambda row: row["unit"])}


def write_golden(root: Path) -> dict[str, Any]:
    """Generate the golden unit list and write it."""
    golden = build_golden(root)
    (root / AUTHORED["unit_golden"]).write_bytes(encode(golden))
    return golden


def unit_problems(tree: Tree) -> list[Problem]:
    """I3: the unit keys are unique and equal the golden list, with the classes and receipt counts it records."""
    problems: list[Problem] = []
    computed: dict[str, dict[str, Any]] = {}
    for unit in tree.units:
        try:
            key, kind = unit_key(unit, tree.specs[unit["snapshot_id"]])
        except (KeyError, capture_mode.SpecError) as error:
            problems.append(Problem("I3", unit["snapshot_id"], f"has no usable key: {type(error).__name__}: {error}"))
            continue
        if key in computed:
            problems.append(Problem("I3", key, f"is the key of two units ({computed[key]['snapshot_id']} and {unit['snapshot_id']})"))
        computed[key] = {"snapshot_id": unit["snapshot_id"], "kind": kind, "class": unit_class(unit), "receipt": bool(unit.get("receipt"))}
    golden = {row["unit"]: row for row in tree.golden["units"]}
    for key in sorted(set(computed) - set(golden)):
        problems.append(Problem("I3", key, f"is a unit key the golden list lacks (unit {computed[key]['snapshot_id']})"))
    for key in sorted(set(golden) - set(computed)):
        problems.append(Problem("I3", key, "is in the golden list but no unit has it"))
    for key in sorted(set(computed) & set(golden)):
        row = golden[key]
        for name in ("kind", "class", "receipt"):
            if row[name] != computed[key][name]:
                problems.append(Problem("I3", key, f"changed {name}: golden {row[name]!r}, queue {computed[key][name]!r}"))
    for name, value in unit_counts(tree.units).items():
        if tree.golden["counts"][name] != value:
            problems.append(Problem("I3", name, f"count differs: golden {tree.golden['counts'][name]}, queue {value}"))
    if len(golden) != len(tree.golden["units"]):
        problems.append(Problem("I3", "unit_golden", "lists a key twice"))
    return problems


def unit_outputs(tree: Tree) -> list[Output]:
    """Every file the queue's units would store: an archive member by its own dataset ID, role and name, any other by the source's dataset folder."""
    outputs: list[Output] = []
    for unit in tree.units:
        spec = tree.specs[unit["snapshot_id"]]
        collection = tree.routes[unit["source_id"]]
        if "archive_members" in spec:
            outputs += [
                Output(unit["snapshot_id"], unit["source_id"], collection, member["role"], member["dataset_id"], member["member_chain"][-1], True)
                for member in spec["archive_members"]
            ]
            continue
        role = (spec.get("receipt_template") or {}).get("role") or "data"
        dataset = source_folder(unit["source_id"])
        outputs += [
            Output(unit["snapshot_id"], unit["source_id"], collection, role, dataset, name, False)
            for name in spec["stored_data_files"] or [capture_mode.download_name(unit, spec)]
        ]
    return outputs


def selections(tree: Tree, outputs: Sequence[Output]) -> list[list[str]]:
    """For each output the tables the real ``bronze.select`` picks after the collection and role filter."""
    by_collection: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for table in tree.tables:
        by_collection[table["collection"]].append(table)
    return [
        [
            table["table"]
            for table in by_collection.get(item.collection, [])
            if item.role in table["roles"] and bronze.select(table, item.dataset_id, item.name, item.snapshot_id)
        ]
        for item in outputs
    ]


def unselected(scope_tables: Mapping[str, str], picked: Sequence[Sequence[str]]) -> list[str]:
    """The default-scope tables no queued output selects."""
    selected = {table for tables in picked for table in tables}
    return sorted(table for table in scope_tables if table not in selected)


def computed_gaps(tree: Tree, scope_tables: Mapping[str, str], picked: Sequence[Sequence[str]]) -> list[dict[str, str]]:
    """The gap tables, each with its kind and its authored migration step."""
    labels = tree.collections["gaps"]
    return [{"table": table, "kind": scope_tables[table], "step": labels.get(table, {}).get("step", "")} for table in unselected(scope_tables, picked)]


def gap_problems(tree: Tree, gaps: list[dict[str, str]]) -> list[Problem]:
    """I7: the computed gap set equals the gap set pinned for this map digest, with one pin for a table, and I8: the migration label of every pin and of every
    unpinned gap is a step of the design. Each pin row is read, so a second row of a table hides neither."""
    pins = tree.lock["pins"]
    rows = pins["gaps"]
    tables = [row["table"] for row in rows]
    computed = {gap["table"]: gap for gap in gaps}
    unpinned = [gap for gap in gaps if gap["table"] not in tables]
    labels = [
        Problem("I8", row["table"], f"has migration label {row['step']!r}, which is not a step of design v4 section 8")
        for row in sorted([*rows, *unpinned], key=lambda row: row["table"])
        if not STEP_LIST.fullmatch(row["step"])
    ]
    problems = [Problem("I7", table, "is pinned more than once: keep one pin for a table") for table in sorted(set(tables)) if tables.count(table) > 1]
    if pins["map_sha256"] != tree.lock["map_sha256"]:
        problems.append(
            Problem("I7", "pins", f"are for map {pins['map_sha256'][:12]}, not the map of this lock ({tree.lock['map_sha256'][:12]}): regenerate them")
        )
        return problems + labels
    problems += [Problem("I7", gap["table"], "is a gap (no queued output selects it) that is not pinned") for gap in unpinned]
    for table in sorted(set(tables) - set(computed)):
        problems.append(Problem("I7", table, "is pinned as a gap but a queued output selects it: change the pin in a reviewed commit"))
    for row in sorted(rows, key=lambda row: row["table"]):
        if row["table"] in computed and computed[row["table"]] != row:
            problems.append(Problem("I7", row["table"], f"differs from its pin: computed {computed[row['table']]}, pinned {row}"))
    return list(dict.fromkeys(problems + labels))


def marked_outputs(tree: Tree, outputs: Sequence[Output]) -> dict[str, int]:
    """The outputs the check cannot decide offline: archive members and outputs of collections whose tables select by dataset ID."""
    with_ids = {table["collection"] for table in tree.tables if "dataset_ids" in table}
    return {
        "archive_members": sum(1 for item in outputs if item.archive),
        "dataset_id_collections": sum(1 for item in outputs if not item.archive and item.collection in with_ids),
    }


def route_problems(tree: Tree, outputs: Sequence[Output], picked: Sequence[Sequence[str]], pinned: Collection[str]) -> tuple[list[Problem], dict[str, Any]]:
    """I4: no output is selected by two tables. I5: a declared feed selects exactly its table unless the table is a pinned gap. I10: a document output with no
    declared feed may select none, and the check counts those. Outputs the check cannot decide offline are counted as marked, never as passed."""
    problems: list[Problem] = []
    for item, tables in zip(outputs, picked, strict=True):
        if len(tables) > 1:
            problems.append(
                Problem("I4", item.snapshot_id, f"output {item.name!r} (role {item.role}) is selected by {len(tables)} tables: {', '.join(tables)}")
            )
    fed: set[int] = set()
    for feed in tree.collections["feeds"]:
        found = [number for number, item in enumerate(outputs) if item.source_id == feed["source_id"] and re.search(feed["name_regex"], item.name)]
        fed.update(found)
        if not found:
            problems.append(Problem("I5", feed["table"], f"declared feed {feed['source_id']} {feed['name_regex']!r} matches no output"))
            continue
        wrong = [number for number in found if picked[number] != [feed["table"]]]
        if wrong and feed["table"] not in pinned:
            seen = picked[wrong[0]]
            problems.append(
                Problem(
                    "I5",
                    feed["table"],
                    f"{len(wrong)} of {len(found)} declared outputs select {', '.join(seen) or 'no table'}, not {feed['table']}, and it is not a pinned gap",
                )
            )
    facts = {
        "marked_outputs": marked_outputs(tree, outputs),
        "document_outputs_unselected": sum(1 for number, item in enumerate(outputs) if item.role == "methodology" and not picked[number] and number not in fed),
        "feeds": len(tree.collections["feeds"]),
    }
    return problems, facts


def seat_membership() -> dict[str, frozenset[str]]:
    """The modes each existing seat list names."""
    return {
        "ADAPTERS (capture_mode)": frozenset(capture_mode.ADAPTERS),
        "ADAPTERS (redownload)": frozenset(redownload.ADAPTERS),
        "LINEAGE_ID": frozenset(capture_mode.LINEAGE_ID),
        "MODES (manual_content)": frozenset(manual_content.MODES),
        "PLAN_MODES": frozenset(stored_mirror.PLAN_MODES),
        "PLAN_PATHS": frozenset(startup_check.PLAN_PATHS),
        "PLANNED": frozenset(redownload.PLANNED),
        "SEED_ONLY_MODES": frozenset(storage_controls.SEED_ONLY_MODES),
        "URL_BOUND": frozenset(capture_mode.URL_BOUND),
    }


def computed_seat_gaps(tree: Tree, seats: Mapping[str, Iterable[str]]) -> dict[str, list[str]]:
    """For each queue mode the seat lists that should name it and do not."""
    modes = {redownload.handler_mode(unit) for unit in tree.units}
    gaps: dict[str, list[str]] = {}
    for mode in sorted(modes):
        missing = sorted(seat for seat in SEAT_RULES[SEAT_CLASS.get(mode, "plan")] if mode not in set(seats[seat]))
        if missing:
            gaps[mode] = missing
    return gaps


def seat_problems(tree: Tree, gaps: Mapping[str, list[str]]) -> list[Problem]:
    """I11: the seat lists agree for every queue mode except the gaps pinned for this base."""
    pinned = tree.lock["pins"]["seat_gaps"]
    problems = [
        Problem("I11", mode, f"is a queue mode missing from {', '.join(seats)} and the gap is not pinned")
        for mode, seats in sorted(gaps.items())
        if mode not in pinned
    ]
    problems += [
        Problem("I11", mode, "is a pinned seat gap but every seat now names it: remove the pin in a reviewed commit")
        for mode in sorted(pinned)
        if mode not in gaps
    ]
    problems += [
        Problem("I11", mode, f"seat gap differs from its pin: seats {gaps[mode]}, pinned {pinned[mode]}")
        for mode in sorted(set(gaps) & set(pinned))
        if gaps[mode] != pinned[mode]
    ]
    return problems


def closure_problems(root: Path) -> list[Problem]:
    """I6: each registered entry point's exact closure is among the entries of its own list at any position, and the table covers exactly the lists on disk."""
    problems: list[Problem] = []
    folder = root / ACQUISITION
    on_disk = {path.name.removesuffix("_code_versions.json") for path in folder.glob("*_code_versions.json")}
    for stem in sorted(on_disk - {entry.stem for entry in ENTRY_POINTS}):
        problems.append(Problem("I6", stem, f"{stem}_code_versions.json is a version list that no registered entry point checks"))
    for entry in ENTRY_POINTS:
        path = folder / f"{entry.stem}_code_versions.json"
        if not path.is_file():
            problems.append(Problem("I6", entry.name, f"has no version list {path.relative_to(root).as_posix()}"))
            continue
        try:
            listed = [version["code_sha256"] for version in read_code_versions(path)["versions"]]
            actual = closure_hashes(entry.module, root)
        except (RegistryError, OSError, KeyError, ValueError) as error:
            problems.append(Problem("I6", entry.name, f"cannot be checked: {error}"))
            continue
        if actual not in listed:
            problems.append(Problem("I6", entry.name, f"has a closure of {len(actual)} files that no version of {path.name} lists: relist after review"))
    return problems


def validate_documents(tree: Tree, schemas: records.Schemas) -> list[Problem]:
    """I2: the authored files validate against their schemas and hold what their readers need. The layout contracts are the I12 row, so their failures carry
    that name.

    The evidence file is not read here. It closes no readiness item: the lock binds its bytes and the readiness report judges its shape."""
    documents = {
        "collections": tree.collections,
        "manual_outcomes": tree.manual,
        "layout_contracts": tree.contracts,
        "unit_golden": tree.golden,
    }
    return [
        Problem("I12" if name == "layout_contracts" else "I2", AUTHORED[name], text)
        for name, document in documents.items()
        for text in schema_errors(schemas, name, document) or schemas.needs_errors(name, document)
    ]


def pattern_error(pattern: str) -> str | None:
    """Why a pattern of an authored file does not compile, or None. Deep nesting and a huge repeat count raise their own errors, not ``re.error``."""
    try:
        re.compile(pattern)
    except re.error as error:
        return str(error)
    except (RecursionError, OverflowError) as error:
        return type(error).__name__
    return None


def authored_problems(tree: Tree) -> list[Problem]:
    """I2 and I8: the authored files agree with the inputs they classify (map, layout, source registry, manual units) and with the lock."""
    problems: list[Problem] = []
    mapped = {table["table"] for table in tree.tables}
    sources_of: dict[str, set[str]] = defaultdict(set)
    for source, collection in tree.routes.items():
        sources_of[collection].add(source)
    wanted = {table["collection"] for table in tree.tables} | tree.leaving
    recorded = [collection["collection"] for collection in tree.collections["collections"]]
    problems += [
        Problem("I2", name, "is a collection of the table map or of the leaving record with no record in collections.json")
        for name in sorted(wanted - set(recorded))
    ]
    problems += [
        Problem("I2", name, "is recorded in collections.json but no table of the map or of the leaving record names it")
        for name in sorted(set(recorded) - wanted)
    ]
    effective = {row["source_id"]: row.get("effective_closeout_decision") for row in tree.source_registry["sources"]}
    for collection in tree.collections["collections"]:
        listed = [source["source_id"] for source in collection["sources"]]
        name = collection["collection"]
        problems += [
            Problem("I2", source, f"is a source of {name} in the collection layout with no record in collections.json")
            for source in sorted(sources_of[name] - set(listed))
        ]
        problems += [
            Problem("I2", source, f"is recorded under {name} but the collection layout does not route it there")
            for source in sorted(set(listed) - sources_of[name])
        ]
        for source in collection["sources"]:
            ref, decision = source["decision_ref"], effective.get(source["source_id"])
            if ref is not None and (ref["source_id"] != source["source_id"] or decision is None or ref["sha256"] != sha256_bytes(canonical(decision))):
                problems.append(
                    Problem("I2", source["source_id"], "decision_ref is not the hash of the registry's effective closeout decision for this source")
                )
            problems += [
                Problem("I2", source["source_id"], f"closure match pattern is not a regular expression: {error}")
                for error in map(pattern_error, source.get("match", {}).values())
                if error
            ]
            if source["acquisition"] == "closed_partial" and ref is None:
                problems.append(Problem("I2", source["source_id"], "is closed_partial and needs a decision_ref"))
    manual = {row["unit"] for row in tree.golden["units"] if row["class"] == "manual"}
    outcomes = [row["unit"] for row in tree.manual["outcomes"]]
    problems += [Problem("I2", unit, "is a manual unit with no outcome in manual_outcomes.json") for unit in sorted(manual - set(outcomes))]
    problems += [Problem("I2", unit, "has an outcome but is not a manual unit of the golden list") for unit in sorted(set(outcomes) - manual)]
    problems += [Problem("I2", unit, "has two outcomes") for unit in sorted({unit for unit in outcomes if outcomes.count(unit) > 1})]
    for feed in tree.collections["feeds"]:
        if feed["table"] not in mapped:
            problems.append(Problem("I2", feed["table"], "is the table of a declared feed but is not in the table map"))
        if feed["source_id"] not in tree.routes:
            problems.append(Problem("I2", feed["table"], f"declares a feed from {feed['source_id']}, which the collection layout does not route"))
        error = pattern_error(feed["name_regex"])
        if error:
            problems.append(Problem("I2", feed["table"], f"declared feed pattern is not a regular expression: {error}"))
    for table, gap in tree.collections["gaps"].items():
        if table not in mapped:
            problems.append(Problem("I2", table, "has a gap record but is not in the table map"))
        if not STEP_LIST.fullmatch(gap["step"]):
            problems.append(Problem("I8", table, f"has migration label {gap['step']!r} in collections.json, which is not a step of design v4 section 8"))
    recorded_decisions = tree.lock.get("decisions", {})
    for name, record in tree.collections["decisions"].items():
        if recorded_decisions.get(name) != {"value": record["value"], "record_sha256": sha256_bytes(canonical(record))}:
            problems.append(Problem("I2", name, "is recorded in the lock differently from collections.json: regenerate the lock and review it"))
    return problems


AUTOMATIC_FORBIDDEN = frozenset({"held_access", "not_scheduled", "excluded", "manual"})


def inside_scope(unit: Mapping[str, Any], spec: Mapping[str, Any], match: Mapping[str, str]) -> bool:
    """Whether a unit's URL or stored names fall inside a closed payload or vintage, by the patterns the closure record gives."""
    url, names = match.get("url_regex"), match.get("name_regex")
    return bool((url and re.search(url, unit["url"])) or (names and any(re.search(names, name) for name in spec.get("stored_data_files", []))))


def source_problems(tree: Tree) -> list[Problem]:
    """I9: a whole-source closure, a hold, a source never scheduled or a manual-only source has no automatic unit, and a payload or vintage closure refuses an
    automatic unit only inside its scope. A closed scope with no pattern cannot be tested here: the readiness block lists it for manual review."""
    problems: list[Problem] = []
    automatic: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for unit in tree.units:
        if unit["route"] != MANUAL_ROUTE:
            automatic[unit["source_id"]].append(unit)
    for collection in tree.collections["collections"]:
        for source in collection["sources"]:
            units = automatic.get(source["source_id"], [])
            closed_partial = source["acquisition"] == "closed_partial"
            if units and (source["acquisition"] in AUTOMATIC_FORBIDDEN or (closed_partial and source["scope"] == "whole_source")):
                problems.append(
                    Problem(
                        "I9", source["source_id"], f"is {source['acquisition']} but has {len(units)} automatic unit(s), for example {units[0]['snapshot_id']}"
                    )
                )
            elif units and closed_partial and source.get("match"):
                hit = [unit for unit in units if inside_scope(unit, tree.specs[unit["snapshot_id"]], source["match"])]
                if hit:
                    problems.append(
                        Problem(
                            "I9",
                            source["source_id"],
                            f"has {len(hit)} automatic unit(s) inside its closed {source['scope']}, for example {hit[0]['snapshot_id']}",
                        )
                    )
    return problems


def contract_problems(tree: Tree) -> list[Problem]:
    """I12: each layout contract names a mapped table, its accepted layouts are distinct and their column digests are the digests of their columns."""
    problems: list[Problem] = []
    mapped = {table["table"] for table in tree.tables}
    for name, contract in tree.contracts["tables"].items():
        if name not in mapped:
            problems.append(Problem("I12", name, "has a layout contract but is not in the table map"))
        consumer = contract["consumer_columns"]
        if consumer != "any" and len(set(consumer)) != len(consumer):
            problems.append(Problem("I12", name, "lists a consumer column twice"))
        layouts = contract["accepted_layouts"]
        if len({layout["id"] for layout in layouts}) != len(layouts) or len({layout["columns_sha256"] for layout in layouts}) != len(layouts):
            problems.append(Problem("I12", name, "lists an accepted layout twice (same id or same column digest)"))
        for layout in layouts:
            if layout["columns_sha256"] != sha256_bytes("\n".join(sorted(layout["columns"])).encode()):
                problems.append(Problem("I12", name, f"layout {layout['id']} has a column digest that is not the SHA-256 of its sorted columns"))
    return problems


def integrity(
    root: Path, report: Mapping[str, Any], schemas: records.Schemas, seats: Mapping[str, Iterable[str]] | None = None, skip: frozenset[str] = frozenset()
) -> Result:
    """Every integrity predicate over a tree whose lock and scope are bound. ``skip`` names predicates a test leaves out; the command never skips. A value no
    predicate can read is a named problem, and the problems found before it stay."""
    result = Result()
    try:
        run_predicates(result, root, default_scope(report), schemas, seat_membership() if seats is None else seats, skip)
    except INPUT_ERRORS as error:
        result.problems.append(Problem("I2", "inputs", f"cannot be read: {type(error).__name__}: {error}"))
    return result


def run_predicates(
    result: Result, root: Path, scope_tables: Mapping[str, str], schemas: records.Schemas, seats: Mapping[str, Iterable[str]], skip: frozenset[str]
) -> None:
    """Add what the predicates find to ``result``. The shape lines about the authored and supplied files are made once the authored files pass I2."""
    lock, problems = load_lock(root, schemas)
    if lock is None:
        result.problems += problems
        return
    tree = load_inputs(root, lock)
    outputs = unit_outputs(tree)
    picked = selections(tree, outputs)
    result.problems += [Problem("I2", schema_path(name), reason) for name, reason in sorted(schemas.unusable.items())]
    load_authored(tree)
    invalid = validate_documents(tree, schemas)
    result.problems += invalid
    if invalid:
        return
    result.notes = registry_readiness.observations(tree, scope_tables, schemas)
    result.problems += authored_problems(tree) + contract_problems(tree)
    if result.problems:
        return
    result.problems += unit_problems(tree) + source_problems(tree)
    result.gaps = computed_gaps(tree, scope_tables, picked)
    result.problems += gap_problems(tree, result.gaps)
    routing, routing_facts = route_problems(tree, outputs, picked, {gap["table"] for gap in tree.lock["pins"]["gaps"]})
    result.problems += routing
    result.seat_gaps = computed_seat_gaps(tree, seats)
    result.problems += seat_problems(tree, result.seat_gaps)
    if "I6" not in skip:
        result.problems += closure_problems(root)
    result.facts = {"outputs": len(outputs), "units": len(tree.units), **routing_facts}


def as_locked(root: Path, lock: Mapping[str, Any], name: str) -> bool:
    """Whether the lock lists a file and every row for it holds the file's digest now. The rows are read one by one, so a lock that is wrong elsewhere still
    binds the file."""
    rows = lock.get("files")
    digests = [row.get("sha256") for row in rows if isinstance(row, Mapping) and row.get("path") == name] if isinstance(rows, list) else []
    try:
        actual = sha256_bytes((root / name).read_bytes())
    except OSError:
        return False
    return bool(digests) and all(digest == actual for digest in digests)


def scope_problems(report: Mapping[str, Any], lock: Mapping[str, Any]) -> list[Problem]:
    """I1: the derived scope and the table map it was derived from are the ones the lock records."""
    if sha256_bytes(scope.report_bytes(report)) != lock.get("scope_report_sha256"):
        return [Problem("I1", "scope report", "differs from the lock: the default scope changed, regenerate the lock and review it")]
    if report["inputs"]["table_map_sha256"] != lock.get("map_sha256"):
        return [Problem("I1", "scope report", "was derived from another table map than the lock binds")]
    return []


def readiness_of(root: Path, report: Mapping[str, Any], lock: Mapping[str, Any]) -> registry_readiness.Assessment | None:
    """The readiness report of a tree whose queue, table map and derived scope are the locked ones, or None. No other file and no other failure removes it.

    The capture specifications and the collection layout give only the gaps and the marked outputs. Where they are not the locked ones or cannot be read, the
    report says that these are not computed and keeps every item."""
    if scope_problems(report, lock) or not all(as_locked(root, lock, name) for name in (QUEUE, TABLE_MAP)):
        return None
    try:
        units = read_json(root, QUEUE)["queue"]
        tables = scope.load_map(root / TABLE_MAP)["tables"]
        scope_tables = default_scope(report)
        gaps: list[str] | None = None
        marked: dict[str, int] | None = None
        if all(as_locked(root, lock, name) for name in (SPECS, LAYOUT)):
            with contextlib.suppress(*INPUT_ERRORS):
                tree = load_queue(root)
                outputs = unit_outputs(tree)
                gaps, marked = unselected(scope_tables, selections(tree, outputs)), marked_outputs(tree, outputs)
        return registry_readiness.obligations(units, tables, scope_tables, gaps, marked)
    except INPUT_ERRORS:
        return None


def check(
    root: Path,
    *,
    release: bool = False,
    scope_report: Any = NOT_SUPPLIED,
    seats: Mapping[str, Iterable[str]] | None = None,
    skip: frozenset[str] = frozenset(),
) -> Result:
    """Run the integrity predicates (and, with ``release``, the readiness predicates) over a tree. Reads files only. The predicates run over a tree the lock
    binds; the readiness report needs only what :func:`readiness_of` reads."""
    schemas = records.Schemas(root)
    unbound = lock_problems(root, schemas)
    try:
        report = derived_scope(root)
    except CANNOT_RUN:
        if not unbound:
            raise
        return Result(problems=unbound)
    try:
        lock = read_json(root, LOCK)
    except RegistryFileError:
        lock = None
    if not isinstance(lock, Mapping):
        lock = {}
    assessment = readiness_of(root, report, lock)
    mismatch = scope_problems(report, lock) if "scope_report_sha256" in lock else []
    result = Result(problems=unbound + mismatch) if unbound or mismatch else integrity(root, report, schemas, seats, skip)
    if assessment:
        result.readiness, result.release_problems = assessment.items, assessment.problems
    result.problems = supplied_scope_problems(report, scope_report) + result.problems
    if release:
        result.problems += result.release_problems
    return result


def build_lock(root: Path, report: Mapping[str, Any]) -> dict[str, Any]:
    """The lock of the tree as it is now: file digests, the scope report digest, the recorded decisions and the pinned gap and seat sets."""
    collections = read_json(root, AUTHORED["collections"])
    unreadable = records.Schemas(root).needs_errors("collections", collections)
    if unreadable:
        raise RegistryFileError(f"{AUTHORED['collections']} {unreadable[0]}")
    digest = map_digest(root)
    files: dict[str, str] = {}
    for name in bound_paths(root):
        try:
            files[name] = sha256_bytes((root / name).read_bytes())
        except (OSError, ValueError) as error:
            reason = getattr(error, "strerror", None) or type(error).__name__
            raise RegistryFileError(f"{name} is a bound file and cannot be read: {reason}") from error
    draft = {"map_sha256": digest, "pins": {"map_sha256": digest, "gaps": [], "seat_gaps": {}}}
    tree = load_authored(load_inputs(root, draft))
    gaps = computed_gaps(tree, default_scope(report), selections(tree, unit_outputs(tree)))
    for gap in gaps:
        if not STEP_LIST.fullmatch(gap["step"]):
            raise RegistryFileError(f"{AUTHORED['collections']}: gap table {gap['table']} has no valid migration step in its gaps record")
    return {
        "kind": "registry_lock",
        "schema_version": 1,
        "map_sha256": digest,
        "files": [{"path": name, "sha256": digest} for name, digest in sorted(files.items())],
        "scope_report_sha256": sha256_bytes(scope.report_bytes(report)),
        "parents": {"source_registry": {"path": SOURCE_REGISTRY, "sha256": files[SOURCE_REGISTRY]}},
        "decisions": {
            name: {"value": record["value"], "record_sha256": sha256_bytes(canonical(record))} for name, record in sorted(collections["decisions"].items())
        },
        "pins": {"map_sha256": digest, "gaps": gaps, "seat_gaps": computed_seat_gaps(tree, seat_membership())},
    }


def write_lock(root: Path, scope_report: Any = NOT_SUPPLIED) -> dict[str, Any]:
    """Generate the lock from the tree and write it, unless a supplied scope report is refused (LockRefused, nothing written)."""
    report = derived_scope(root)
    refused = supplied_scope_problems(report, scope_report)
    if refused:
        raise LockRefused(refused)
    lock = build_lock(root, report)
    (root / LOCK).write_bytes(encode(lock))
    return lock


def render(result: Result, release: bool) -> str:
    """The text the command prints: each problem on its own line, the READINESS block and the verdict."""
    lines = [str(problem) for problem in result.problems]
    lines.append("READINESS (open items; this block is not a statement of completion)")
    lines.append(f"  rule: {registry_readiness.RULE}")
    lines += [f"  open: {item}" for item in result.readiness] or ["  (not computed)"]
    lines += [f"  {note}" for note in result.notes]
    lines.append(f"{'check --release' if release else 'check'}: {'pass' if result.ok else 'fail'} ({len(result.problems)} problems)")
    # A JSON string may hold a lone surrogate, which no output encoding takes: it prints as its escape.
    return ("\n".join(lines) + "\n").encode("utf-8", "backslashreplace").decode()


def main(argv: Sequence[str] | None = None) -> int:
    """The command line: exit 0 for a pass, 1 for named problems, 2 when the check cannot run."""
    parser = argparse.ArgumentParser(description="Check or lock the ingestion registry (reads files only).")
    parser.add_argument("command", choices=("check", "lock", "golden"))
    parser.add_argument("--release", action="store_true", help="also require usable-input readiness")
    parser.add_argument("--root", type=Path, default=ROOT, help="the repository tree to read")
    parser.add_argument("--scope-report", type=Path, help="a scope report to compare with the one derived from the tree's dbt project")
    arguments = parser.parse_args(argv)
    arguments.root = arguments.root.resolve()
    try:
        if arguments.command == "golden":
            golden = write_golden(arguments.root)
            sys.stdout.write(f"golden list written: {golden['counts']['units']} units\n")
            return 0
        supplied = read_supplied(arguments.scope_report) if arguments.scope_report else NOT_SUPPLIED
        if arguments.command == "lock":
            try:
                lock = write_lock(arguments.root, supplied)
            except LockRefused as refused:
                sys.stdout.write("".join(f"{problem}\n" for problem in refused.problems) + "lock: refused, nothing written\n")
                return 1
            sys.stdout.write(f"lock written: {len(lock['files'])} files, {len(lock['pins']['gaps'])} pinned gaps\n")
            return 0
        result = check(arguments.root, release=arguments.release, scope_report=supplied)
    except CANNOT_RUN as error:
        sys.stderr.write(f"registry: cannot run: {error}\n")
        return 2
    sys.stdout.write(render(result, arguments.release))
    return 0 if result.ok else 1


if __name__ == "__main__":
    sys.exit(main())
