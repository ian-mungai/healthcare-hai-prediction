"""Bind, classify and check the ingestion registry (design v4 step S4).

``check`` tests that the registry files agree with each other and with the code and data they describe (integrity predicates I1 to I12).
``check --release`` adds the usable-input readiness predicates R1 to R5. Both always print a READINESS block. Nothing here reads S3 or the network, and
nothing in the live pipeline calls it: it reads files and writes only ``registry.lock.json`` (``lock``) and ``unit_golden.json`` (``golden``).

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
import json
import re
import sys
from collections import defaultdict
from collections.abc import Collection, Iterable, Mapping, Sequence
from pathlib import Path
from typing import Any, NamedTuple

from jsonschema import Draft202012Validator

from scripts.acquisition import (
    capture_mode,
    cms_owners_contract,
    hcai_util_contract,
    manual_content,
    onc_mu_contract,
    redownload,
    storage_controls,
    stored_mirror,
)
from scripts.acquisition.code_closure import closure_hashes
from scripts.acquisition.code_versions import read_code_versions
from scripts.acquisition.dataset_layout import source_folder
from scripts.acquisition.source_registry import RegistryError
from scripts.infrastructure import startup_check
from scripts.ingestion import registry_readiness, scope
from scripts.ingestion import registry_records as records
from scripts.ingestion.registry_model import MANUAL_ROUTE, Output, Problem, RegistryFileError, Result, Tree, canonical, default_scope, sha256_bytes
from scripts.lakehouse import bronze

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


def encode(document: Mapping[str, Any]) -> bytes:
    """A registry file as written: sorted keys, two-space indent, one trailing newline."""
    return (json.dumps(document, indent=2, sort_keys=True) + "\n").encode()


def read_json(root: Path, relative: str) -> Any:
    """A file of the tree parsed as JSON; a missing, unreadable or malformed file is a RegistryFileError naming it."""
    try:
        return json.loads((root / relative).read_bytes())
    except OSError as error:
        raise RegistryFileError(f"{relative} cannot be read: {error.strerror or type(error).__name__}") from error
    except (ValueError, RecursionError) as error:
        raise RegistryFileError(f"{relative} is not valid JSON: {error}") from error


def map_digest(root: Path) -> str:
    """The digest the scope report records for the table map: SHA-256 of the canonical JSON of the loaded map."""
    return sha256_bytes(json.dumps(scope.load_map(root / TABLE_MAP), sort_keys=True, separators=(",", ":")).encode())


def version_hashes(version: Mapping[str, Any]) -> dict[str, str]:
    """The path to SHA-256 map of one committed version, in either of the two forms the lists use."""
    listed = version["code_sha256"]
    return dict(listed) if isinstance(listed, dict) else {row["file"]: row["sha256"] for row in listed}


def bound_paths(root: Path) -> list[str]:
    """Every file the lock binds: the authored files and schemas, the frozen inputs, the baseline run and the classified plans."""
    found = {*AUTHORED.values(), *FROZEN}
    for folder in (SCHEMAS, BASELINE):
        found.update(path.relative_to(root).as_posix() for path in (root / folder).rglob("*") if path.is_file())
    try:
        for plan in read_json(root, AUTHORED["collections"]).get("plans", []):
            for name in (plan["path"], plan["path"].replace(".json", ".lock.json")):
                if (root / name).is_file():
                    found.add(name)
    except (RegistryFileError, AttributeError, KeyError, TypeError):
        pass
    return sorted(found)


def load_lock(root: Path) -> tuple[dict[str, Any] | None, list[Problem]]:
    """The lock document, or the problem that names why it cannot be used."""
    try:
        lock = read_json(root, LOCK)
    except RegistryFileError as error:
        text = f"{LOCK} is missing: run the lock command" if not (root / LOCK).exists() else str(error)
        return None, [Problem("lock", LOCK, text)]
    errors = schema_errors(root, "registry_lock", lock)
    return (None, [Problem("I2", LOCK, text) for text in errors]) if errors else (lock, [])


def schema_errors(root: Path, name: str, document: Any) -> list[str]:
    """The first few places a document breaks its JSON schema, as positions and keywords only: no input value reaches a diagnostic."""
    return records.describe_errors(Draft202012Validator(read_json(root, f"{SCHEMAS}/{name}.schema.json")), document, limit=5)


def lock_files(lock: Mapping[str, Any]) -> dict[str, str]:
    """The lock's bound files as a path to SHA-256 map. The lock lists them as path and digest records, sorted by path."""
    return {row["path"]: row["sha256"] for row in lock["files"]}


def lock_problems(root: Path) -> list[Problem]:
    """I1: each bound file's SHA-256 equals its lock entry, and the lock binds exactly the bound set."""
    lock, problems = load_lock(root)
    if lock is None:
        return problems
    wanted, bound = set(bound_paths(root)), lock_files(lock)
    for name in sorted(wanted - set(bound)):
        problems.append(Problem("I1", name, "is a bound file the lock does not list: regenerate the lock and review it"))
    for name in sorted(set(bound) - wanted):
        problems.append(Problem("I1", name, "is listed by the lock but is not a bound file"))
    for name in sorted(wanted & set(bound)):
        try:
            actual = sha256_bytes((root / name).read_bytes())
        except OSError:
            problems.append(Problem("I1", name, "is bound by the lock but cannot be read"))
            continue
        if actual != bound[name]:
            problems.append(Problem("I1", name, f"differs from the lock (lock {bound[name][:12]}, file {actual[:12]})"))
    return problems


def load_tree(root: Path, lock: dict[str, Any]) -> Tree:
    """Read every input once. A file that cannot be read raises RegistryFileError naming it."""
    queue, specs = read_json(root, QUEUE), read_json(root, SPECS)
    layout = read_json(root, LAYOUT)
    routes = {source: f"{group['publisher']}/{group['collection']}" for group in layout["groups"] for source in group["source_ids"]}
    return Tree(
        root=root,
        units=queue["queue"],
        specs=specs["specs"],
        routes=routes,
        tables=scope.load_map(root / TABLE_MAP)["tables"],
        collections=read_json(root, AUTHORED["collections"]),
        golden=read_json(root, AUTHORED["unit_golden"]),
        manual=read_json(root, AUTHORED["manual_outcomes"]),
        contracts=read_json(root, AUTHORED["layout_contracts"]),
        lock=lock,
        source_registry=read_json(root, SOURCE_REGISTRY),
        leaving=frozenset(row["collection"] for row in read_json(root, LEAVING_RECORD)["tables"]),
    )


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


def computed_gaps(tree: Tree, report: Mapping[str, Any], outputs: Sequence[Output], picked: Sequence[Sequence[str]]) -> list[dict[str, str]]:
    """The default-scope tables no queued output selects, each with its kind and its authored migration step."""
    selected = {table for tables in picked for table in tables}
    labels = tree.collections["gaps"]
    return [
        {"table": table, "kind": kind, "step": labels.get(table, {}).get("step", "")}
        for table, kind in sorted(default_scope(report).items())
        if table not in selected
    ]


def gap_problems(tree: Tree, gaps: list[dict[str, str]]) -> list[Problem]:
    """I7: the computed gap set equals the gap set pinned for this map digest, and I8: each migration label is a step of the design."""
    pins = tree.lock["pins"]
    problems: list[Problem] = []
    if pins["map_sha256"] != tree.lock["map_sha256"]:
        problems.append(
            Problem("I7", "pins", f"are for map {pins['map_sha256'][:12]}, not the map of this lock ({tree.lock['map_sha256'][:12]}): regenerate them")
        )
        return problems
    computed, pinned = {gap["table"]: gap for gap in gaps}, {gap["table"]: gap for gap in pins["gaps"]}
    for table in sorted(set(computed) - set(pinned)):
        problems.append(Problem("I7", table, "is a gap (no queued output selects it) that is not pinned"))
    for table in sorted(set(pinned) - set(computed)):
        problems.append(Problem("I7", table, "is pinned as a gap but a queued output selects it: change the pin in a reviewed commit"))
    for table in sorted(set(computed) & set(pinned)):
        if computed[table] != pinned[table]:
            problems.append(Problem("I7", table, f"differs from its pin: computed {computed[table]}, pinned {pinned[table]}"))
    for table, gap in sorted({**computed, **pinned}.items()):
        if not STEP_LIST.fullmatch(gap["step"]):
            problems.append(Problem("I8", table, f"has migration label {gap['step']!r}, which is not a step of design v4 section 8"))
    return problems


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
    with_ids = {table["collection"] for table in tree.tables if "dataset_ids" in table}
    facts = {
        "marked_outputs": {
            "archive_members": sum(1 for item in outputs if item.archive),
            "dataset_id_collections": sum(1 for item in outputs if not item.archive and item.collection in with_ids),
        },
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


def validate_documents(root: Path, tree: Tree) -> list[Problem]:
    """I2: the authored files validate against their schemas. The layout contracts are the I12 row, so their schema failures carry that name.

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
        for text in schema_errors(root, name, document)
    ]


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
            for pattern in source.get("match", {}).values():
                try:
                    re.compile(pattern)
                except re.error as error:
                    problems.append(Problem("I2", source["source_id"], f"closure match pattern is not a regular expression: {error}"))
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
        try:
            re.compile(feed["name_regex"])
        except re.error as error:
            problems.append(Problem("I2", feed["table"], f"declared feed pattern is not a regular expression: {error}"))
    for table, gap in tree.collections["gaps"].items():
        if table not in mapped:
            problems.append(Problem("I2", table, "has a gap record but is not in the table map"))
        if not STEP_LIST.fullmatch(gap["step"]):
            problems.append(Problem("I8", table, f"has migration label {gap['step']!r} in collections.json, which is not a step of design v4 section 8"))
    for plan in tree.collections["plans"]:
        if not (tree.root / plan["path"]).is_file():
            problems.append(Problem("I2", plan["path"], "is a classified plan that is not on disk"))
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


def integrity(root: Path, report: Mapping[str, Any], seats: Mapping[str, Iterable[str]] | None = None, skip: frozenset[str] = frozenset()) -> Result:
    """Every integrity predicate over a tree whose lock is bound. ``skip`` names predicates a test leaves out; the command never skips."""
    result = Result()
    lock, problems = load_lock(root)
    if lock is None:
        result.problems = problems
        return result
    try:
        tree = load_tree(root, lock)
    except (RegistryFileError, KeyError, TypeError, ValueError, scope.ScopeError) as error:
        result.problems = [Problem("I2", "inputs", f"cannot be read: {error}")]
        return result
    result.problems += validate_documents(root, tree)
    if result.problems:
        return result
    result.problems += authored_problems(tree) + contract_problems(tree)
    if result.problems:
        return result
    result.problems += unit_problems(tree) + source_problems(tree)
    outputs = unit_outputs(tree)
    picked = selections(tree, outputs)
    result.gaps = computed_gaps(tree, report, outputs, picked)
    result.problems += gap_problems(tree, result.gaps)
    routing, routing_facts = route_problems(tree, outputs, picked, {gap["table"] for gap in tree.lock["pins"]["gaps"]})
    result.problems += routing
    result.seat_gaps = computed_seat_gaps(tree, seat_membership() if seats is None else seats)
    result.problems += seat_problems(tree, result.seat_gaps)
    if "I6" not in skip:
        result.problems += closure_problems(root)
    result.facts = {"outputs": len(outputs), "units": len(tree.units), **routing_facts}
    assessment = registry_readiness.assess(tree, report, result.gaps, result.facts)
    result.readiness, result.release_problems, result.notes = assessment.items, assessment.problems, assessment.notes
    return result


def scope_report_for(root: Path, supplied: Mapping[str, Any] | None) -> dict[str, Any]:
    """The scope report of the tree: the supplied one, or a fresh pinned dbt parse of the tree's own project."""
    if supplied is not None:
        return dict(supplied)
    report, problems, _ = scope.scope_of_project(root / "dbt", root / TABLE_MAP, root)
    if problems:
        raise scope.ScopeError("; ".join(problems))
    return report


def check(
    root: Path,
    *,
    release: bool = False,
    scope_report: Mapping[str, Any] | None = None,
    seats: Mapping[str, Iterable[str]] | None = None,
    skip: frozenset[str] = frozenset(),
) -> Result:
    """Run the integrity predicates (and, with ``release``, the readiness predicates) over a tree. Reads files only."""
    result = Result()
    result.problems = lock_problems(root)
    if result.problems:
        return result
    report = scope_report_for(root, scope_report)
    lock = read_json(root, LOCK)
    if sha256_bytes(scope.report_bytes(report)) != lock["scope_report_sha256"]:
        result.problems.append(Problem("I1", "scope report", "differs from the lock: the default scope changed, regenerate the lock and review it"))
        return result
    if report["inputs"]["table_map_sha256"] != lock["map_sha256"]:
        result.problems.append(Problem("I1", "scope report", "was derived from another table map than the lock binds"))
        return result
    result = integrity(root, report, seats, skip)
    if release:
        result.problems += result.release_problems
    return result


def build_lock(root: Path, report: Mapping[str, Any]) -> dict[str, Any]:
    """The lock of the tree as it is now: file digests, the scope report digest, the recorded decisions and the pinned gap and seat sets."""
    collections = read_json(root, AUTHORED["collections"])
    digest = map_digest(root)
    files = {name: sha256_bytes((root / name).read_bytes()) for name in bound_paths(root)}
    draft = {"map_sha256": digest, "pins": {"map_sha256": digest, "gaps": [], "seat_gaps": {}}}
    tree = load_tree(root, draft)
    outputs = unit_outputs(tree)
    gaps = computed_gaps(tree, report, outputs, selections(tree, outputs))
    for gap in gaps:
        if not STEP_LIST.fullmatch(gap["step"]):
            raise RegistryFileError(f"{AUTHORED['collections']}: gap table {gap['table']} has no valid migration step in its gaps record")
    registry_path = (root / SOURCE_REGISTRY).relative_to(root).as_posix()
    return {
        "kind": "registry_lock",
        "schema_version": 1,
        "map_sha256": digest,
        "files": [{"path": name, "sha256": digest} for name, digest in sorted(files.items())],
        "scope_report_sha256": sha256_bytes(scope.report_bytes(report)),
        "parents": {"source_registry": {"path": registry_path, "sha256": files[SOURCE_REGISTRY]}},
        "decisions": {
            name: {"value": record["value"], "record_sha256": sha256_bytes(canonical(record))} for name, record in sorted(collections["decisions"].items())
        },
        "pins": {"map_sha256": digest, "gaps": gaps, "seat_gaps": computed_seat_gaps(tree, seat_membership())},
    }


def write_lock(root: Path, scope_report: Mapping[str, Any] | None = None) -> dict[str, Any]:
    """Generate the lock from the tree and write it. The only file this module writes."""
    lock = build_lock(root, scope_report_for(root, scope_report))
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
    return "\n".join(lines) + "\n"


def main(argv: Sequence[str] | None = None) -> int:
    """The command line: exit 0 for a pass, 1 for named problems, 2 when the check cannot run."""
    parser = argparse.ArgumentParser(description="Check or lock the ingestion registry (reads files only).")
    parser.add_argument("command", choices=("check", "lock", "golden"))
    parser.add_argument("--release", action="store_true", help="also require usable-input readiness")
    parser.add_argument("--root", type=Path, default=ROOT, help="the repository tree to read")
    parser.add_argument("--scope-report", type=Path, help="a scope report to use instead of a fresh dbt parse")
    arguments = parser.parse_args(argv)
    arguments.root = arguments.root.resolve()
    try:
        if arguments.command == "golden":
            golden = write_golden(arguments.root)
            sys.stdout.write(f"golden list written: {golden['counts']['units']} units\n")
            return 0
        supplied = json.loads(arguments.scope_report.read_bytes()) if arguments.scope_report else None
        if arguments.command == "lock":
            lock = write_lock(arguments.root, supplied)
            sys.stdout.write(f"lock written: {len(lock['files'])} files, {len(lock['pins']['gaps'])} pinned gaps\n")
            return 0
        result = check(arguments.root, release=arguments.release, scope_report=supplied)
    except (RegistryFileError, scope.ScopeError, OSError, ValueError) as error:
        sys.stderr.write(f"registry: cannot run: {error}\n")
        return 2
    sys.stdout.write(render(result, arguments.release))
    return 0 if result.ok else 1


if __name__ == "__main__":
    sys.exit(main())
