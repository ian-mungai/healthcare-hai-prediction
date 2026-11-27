"""Release readiness for the registry check (design v4 section 5.1), as a REPORT.

THE RULE OF STEP S4 (``RULE`` below): no file of any kind closes or reduces a readiness item here. Evidence, a record, a live check, a manual outcome,
a decision or state in ``collections.json``, a layout contract entry, a retention input and an IAM plan evidence file are at most reported as shape lines.
Every open item names the step it waits for. The evidence checks that would let a file close an item (selection, identity, manifest, layout, one fact set,
live coverage and closing status) are built later, in the steps that produce real records.

The split enforces the rule. :func:`obligations` makes every item and problem from the queue, the table map, the scope report and the computed facts,
and takes no authored registry file. :func:`observations` reads the authored and supplied files and returns only lines that start with ``shape:``.
``Assessment.open`` is the only way an item is added and nothing removes one.
"""

from __future__ import annotations

from collections.abc import Collection, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from scripts.ingestion import registry_records as records
from scripts.ingestion.registry_model import DECISIONS, MANUAL_ROUTE, Problem, Tree

RULE = (
    "Step S4 builds the integrity check and this report only. No file of any kind closes or reduces a readiness item: "
    "the checks that would let a supplied file close an item are built later, in the steps that produce real records."
)
INTERIM_FIELDS = ("trigger", "follow_up", "accountable", "due")
PENDING = "pending_owner"
COLLECTIONS_FILE = "config/ingestion/collections.json"
CONTRACTS_FILE = "config/ingestion/layout_contracts.json"
EVIDENCE_FILE = "config/ingestion/input_evidence.json"
RETENTION_INPUT = "config/lakehouse/retention_input.json"
IAM_EVIDENCE = "config/ingestion/iam_plan_evidence.json"
LIST_LIMIT = 30
WAIT_EVIDENCE = "L3 (the live check of exact object versions) and S9 (the evidence checks)"
WAIT_MANIFEST = "manifest bytes (L3)"
WAIT_RETENTION = "S7 and the lead's L5 export"
WAIT_IAM = "S11 (S12 for the shrink plan)"
WAIT_MANUAL = "S9 (the journaled manual path)"
WAIT_LAYOUT = "header facts (S9 inventory or L4)"


@dataclass
class Assessment:
    """The open readiness items, as READINESS lines, and the problems that make ``check --release`` fail."""

    items: list[str] = field(default_factory=list)
    problems: list[Problem] = field(default_factory=list)

    def open(self, item: str | None, *problems: Problem) -> None:
        """Add an open item and its problems. This is the only way an item enters the report, and there is no way to remove one."""
        if item:
            self.items.append(item)
        self.problems.extend(problems)


def names(values: Sequence[str]) -> str:
    return ", ".join(values) if len(values) <= LIST_LIMIT else f"{', '.join(values[:LIST_LIMIT])} and {len(values) - LIST_LIMIT} more"


def table_wait(table: Mapping[str, Any]) -> str:
    """The step an open table waits for: a selector by dataset ID or snapshot for manifest bytes, any other for L3 and S9."""
    if "dataset_ids" in table or "snapshot_pattern" in table:
        return f"{WAIT_MANIFEST}; the selector reads a dataset ID or snapshot the completion record does not carry"
    return WAIT_EVIDENCE


def decision_obligations() -> list[Problem]:
    """R3: the four decisions, the Q4 interim record, the source states and the plan lifecycles. A fixed set: no file settles one in this step."""
    owner = "stays open in this step: no file records the owner's decision here; waiting for the owner's decision record and the lead's confirmation of it"
    lead = "stays open in this step: no file records it here; waiting for the lead"
    return [
        *(Problem("R3", name, owner) for name in DECISIONS),
        *(Problem("R3", f"Q4 interim {name}", lead) for name in INTERIM_FIELDS),
        Problem("R3", "source states", f"no acquisition or admission state is settled in this step: {lead}"),
        Problem("R3", "plan lifecycles", f"no plan lifecycle is settled in this step: {lead}"),
    ]


def obligations(
    units: Sequence[Mapping[str, Any]],
    tables: Sequence[Mapping[str, Any]],
    scope: Mapping[str, str],
    gap_tables: Collection[str] | None,
    marked: Mapping[str, int] | None,
) -> Assessment:
    """Every open item and problem of a tree, from the queue, the table map, the default scope, the computed gaps and the marked outputs. No authored file.

    The gaps and the marked outputs are None when they cannot be computed: their items stay and say so."""
    result = Assessment()
    mapped = {table["table"]: table for table in tables}
    open_tables = sorted(scope)
    result.open(
        None,
        *(
            Problem("R1", table, f"no bound usable-input evidence: a selector match alone never closes R1; waiting for {table_wait(mapped[table])}")
            for table in open_tables
        ),
    )
    open_gaps = sorted(set(gap_tables or ()) & set(open_tables))
    if len(open_tables) <= LIST_LIMIT:
        listing = names(open_tables)
    elif gap_tables is None:
        listing = f"{len(open_tables)} tables, gaps not computed"
    else:
        gap_list = f" ({names(open_gaps)})" if open_gaps else ""
        listing = f"{len(open_gaps)} gaps with no selected queued output{gap_list}; {len(open_tables) - len(open_gaps)} with a selected queued output"
    by_selector = sum(1 for table in open_tables if "dataset_ids" in mapped[table] or "snapshot_pattern" in mapped[table])
    result.open(
        f"R1 usable-input evidence: {len(open_tables)} of {len(scope)} default-scope tables have none: {listing}; waiting for {WAIT_EVIDENCE}; "
        f"{by_selector} of them select by dataset ID or snapshot and wait for {WAIT_MANIFEST}"
    )
    result.open(
        "R2 evidence checks (selection, identity, manifest, layout, one fact set, live coverage, closing status): not built in step S4, so run for no table; "
        "waiting for S9 and L3"
    )
    result.open(
        f"R3 decisions and states: {', '.join(DECISIONS)}, the Q4 interim ({', '.join(INTERIM_FIELDS)}), the source states and the plan lifecycles stay open "
        "in this step; waiting for the owner (the decisions) and the lead (the Q4 interim, the source states and the plan lifecycles)",
        *decision_obligations(),
    )
    result.open(
        f"R4 retention input and IAM plan evidence are not usable in this step: {RETENTION_INPUT}: waiting for {WAIT_RETENTION}; "
        f"{IAM_EVIDENCE}: waiting for {WAIT_IAM}",
        Problem("R4", RETENTION_INPUT, f"is not usable in this step: waiting for {WAIT_RETENTION}"),
        Problem("R4", IAM_EVIDENCE, f"is not usable in this step: waiting for {WAIT_IAM}"),
    )
    layout = f"{len(scope)} of {len(scope)} default-scope tables have no usable layout facts"
    result.open(f"R5 layout: {layout}; waiting for {WAIT_LAYOUT}", Problem("R5", "layout contracts", f"{layout}: waiting for {WAIT_LAYOUT}"))
    result.open(
        "I13 consumer columns equal model reads: open (waiting for the accepted silver guard)",
        Problem("I13", "consumer columns", "are not yet compared with the columns the models read: waiting for the accepted silver guard"),
    )
    manual = sum(1 for unit in units if unit["route"] == MANUAL_ROUTE)
    if manual:
        text = f"{manual} of {manual} manual units have no usable closing record in this step: waiting for {WAIT_MANUAL}"
        result.open(f"R1 manual outcomes: {text} (pending_owner and owner_held are stops)", Problem("R1", "manual outcomes", text))
    result.open(
        "I9 closed scopes: none is confirmed in this step (the check tests only the scopes a pattern can test); waiting for the lead's review",
        Problem("I9", "closed scopes", "are not confirmed in this step: waiting for the lead's review"),
    )
    waits = f"waiting for {WAIT_MANIFEST} and S9"
    if marked is None:
        result.open(f"I4 outputs the check cannot decide offline: not counted, because the outputs of the queue cannot be computed; {waits}")
    elif marked:
        counts = f"{marked['archive_members']} archive members, {marked['dataset_id_collections']} in dataset-ID collections"
        result.open(f"I4 outputs the check cannot decide offline (marked, never passed): {counts}; {waits}")
    return result


def manual_review_sources(tree: Tree) -> list[str]:
    """Closed sources whose closed scope (a payload or a vintage) has no machine test, so a shape line lists them for review."""
    return sorted(
        source["source_id"]
        for collection in tree.collections["collections"]
        for source in collection["sources"]
        if source["acquisition"] == "closed_partial" and source["scope"] != "whole_source" and not source.get("match")
    )


def pending_states(tree: Tree) -> list[str]:
    """What ``collections.json`` itself shows as undecided or unrecorded: decisions, the Q4 interim record, source states and plan lifecycles."""
    found: list[str] = []
    decisions = tree.collections["decisions"]
    for name in DECISIONS:
        if decisions[name]["value"] == PENDING:
            found.append(f"{name} is {PENDING}")
        elif decisions[name]["recorded"] is None:
            found.append(f"{name} has a value and no record date")
    interim = decisions["Q4"].get("interim", {})
    found += [f"Q4 interim {name} is {PENDING}" for name in INTERIM_FIELDS if interim.get(name, PENDING) == PENDING]
    for collection in tree.collections["collections"]:
        for source in collection["sources"]:
            found += [f"{source['source_id']} {axis} is {PENDING}" for axis in ("acquisition", "admission") if source[axis] == PENDING]
    found += [f"{plan['path']} lifecycle is {PENDING}" for plan in tree.collections["plans"] if plan["lifecycle"] == PENDING]
    return found


def observations(tree: Tree, scope: Mapping[str, str], schemas: records.Schemas) -> list[str]:
    """Shape lines about the authored and supplied files. Each starts with ``shape:`` and none of them changes a readiness item."""
    review = manual_review_sources(tree)
    contracts = tree.contracts.get("tables", {})
    pending = pending_states(tree)
    authored = [
        f"shape: {COLLECTIONS_FILE}, reported only: pending or unrecorded in the file: {names(pending) if pending else 'nothing'}",
        f"shape: {COLLECTIONS_FILE}, reported only: closed scopes in the file with no machine test: {', '.join(review) if review else 'none'}",
        f"shape: {CONTRACTS_FILE}, reported only: default-scope tables with a contract, checked by I12 and not usable in this step: "
        f"{sum(1 for name in scope if name in contracts)}",
    ]
    mapped = frozenset(table["table"] for table in tree.tables)
    return authored + records.shape_notes(tree.root, EVIDENCE_FILE, tree.manual, mapped, RETENTION_INPUT, IAM_EVIDENCE, schemas)
