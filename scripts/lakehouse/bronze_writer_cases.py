"""Synthetic qualification cases for the bronze writer's dispatch and journal contracts."""

from __future__ import annotations

import ast
import contextlib
import copy
import hashlib
import io
import json
from dataclasses import asdict, replace
from pathlib import Path
from types import ModuleType
from typing import Any
from unittest.mock import patch

from scripts.infrastructure.bronze_policy_cases import storage_policy_scope
from scripts.lakehouse.bronze_namespace_cases import namespace_arguments, task_credentials

PRODUCTION = ("bronze", "bronze_dictionary", "smoke_check", "duckdb_view_check")
TWINS = ("bronze_e2e", "bronze_dictionary_e2e", "smoke_check_e2e", "duckdb_view_check_e2e")
MAP = {"namespace": "bronze", "tables": [{"table": "sample", "collection": "example/collection", "format": "text_lines"}]}


def require(condition: bool, message: str) -> None:
    """Fail a case with its exact violated contract."""
    if not condition:
        raise ValueError(message)


def binding(subject: ModuleType, *, mode: str = "aws_qualify", **changes: Any) -> Any:
    """Supply synthetic selector and authority inputs without granting any live rights."""
    testing = mode == "aws_qualify"
    values: dict[str, Any] = {
        "mode": mode,
        "target": "aws",
        "epoch": 42,
        "authority_state": "aws_writer",
        "run_id": "synthetic_run",
        "try_id": "synthetic_try",
        "database_id": "original_database",
        "bucket": "example-data-bucket",
        "source_collection": "example/collection",
        "namespaces": TWINS if testing else PRODUCTION,
        "principal": "e2e_writer" if testing else "bronze_writer",
        "task_role": "job_write_e2e_task" if testing else "job_write_bronze_task",
        "definition": "test_writer" if testing else "bronze_writer",
        "pair": "e2e" if testing else "bronze",
        "capacity_provider": "FARGATE",
        "layer_state": "off",
        "qualification_passed": False,
        "activation_approved": False,
        "run_approval": None,
        "scratch_approval": None,
        "removal_approval": None,
        "removal_grants": False,
        "recovery_hold": True,
        "namespace_creation_approved": True,
    }
    values.update(changes)
    return subject.Binding(**values)


def make_ledger(journal: ModuleType, out: Path) -> Any:
    """Create the storage directories required by the supplied journal interface."""
    result = journal.LocalJournal(out, "42")
    result.intents_path.mkdir(parents=True, exist_ok=False)
    result.completions_path.mkdir(parents=True, exist_ok=False)
    return result


class Catalog:
    """Supply fixed synthetic observations for dispatch and journal checks without running the loaders."""

    def __init__(self) -> None:
        self.tables: dict[str, dict[str, list[tuple[str, ...]]]] = {}
        self.calls: list[list[str]] = []

    def run(self, plan: Any, *, stop_after_bronze: bool = False, leave_scratch: bool = False) -> None:
        """Apply a planned command to synthetic table state, optionally stopping between commits."""
        self.calls.append(list(plan.argv))
        bronze, dictionary, smoke, fixture = plan.binding.namespaces
        if plan.job == "bronze":
            self.tables.setdefault(bronze, {})["sample"] = [("source_object", "010001"), ("source_object", "450056")]
            if stop_after_bronze:
                return
            self.tables.setdefault(dictionary, {})["sample"] = [("facility_id", "string")]
        elif plan.job == "dictionary":
            self.tables.setdefault(dictionary, {})["sample"] = [("facility_id", "string")]
        elif plan.job == "smoke":
            self.tables.setdefault(smoke, {}).pop("round_trip", None)
            self.tables[smoke]["round_trip"] = [("1", "010001"), ("2", "450056")]
            if not leave_scratch:
                del self.tables[smoke]
        elif plan.job == "checksum_fixture":
            if "create" in plan.argv:
                self.tables.setdefault(fixture, {})["edge_values"] = [("1", ""), ("2", "007")]
            else:
                self.tables.pop(fixture, None)
        elif plan.job == "removal":
            self.tables.get(bronze, {}).pop("sample", None)
            self.tables.get(dictionary, {}).pop("sample", None)

    def observation(self, names: tuple[str, ...], *, available: bool = True, kind: str = "live") -> dict[str, Any]:
        """Return complete synthetic catalog membership in the writes journal's wire format."""
        states = {}
        for namespace in names:
            tables = self.tables.get(namespace)
            states[namespace] = (
                {"state": "absent", "properties": {}, "tables": {}}
                if tables is None
                else {
                    "state": "present",
                    "properties": {},
                    "tables": {
                        table: {
                            "uuid": f"synthetic_{table}",
                            "metadata_location": f"s3://example-data-bucket/lakehouse/{namespace}/{table}/metadata.json",
                            "version_id": "synthetic_version",
                            "snapshot_id": 1,
                            "refs": {"main": {"snapshot_id": 1}},
                        }
                        for table in sorted(tables)
                    },
                }
            )
        return {"available": available, "kind": kind, "database_id": "original_database", "namespaces": states}


def planned(subject: ModuleType, job: str, **changes: Any) -> Any:
    """Resolve the same synthetic collection through the submitted production planner."""
    selected = binding(subject, **changes)
    table_job = job in {"bronze", "dictionary", "checksums"} or (job == "removal" and selected.mode == "aws_qualify")
    return subject.plan(selected, job, tables=["sample"] if table_job else [], table_map=MAP, authority_epoch=42)


def rejected(subject: ModuleType, job: str, reason: str, **changes: Any) -> None:
    """Require the intended admission failure rather than accepting any exception."""
    try:
        planned(subject, job, **changes)
    except subject.BindingError as error:
        require(reason in str(error), f"expected {reason!r}, received {str(error)!r}")
    else:
        raise ValueError(f"{job} accepted the forbidden binding: {reason}")


def repeated_load(subject: ModuleType, journal: ModuleType, out: Path) -> dict[str, Any]:
    """Check repeated-load dispatch and both namespace intents. Synthetic effects do not verify native convergence."""
    service = Catalog()
    ledger = make_ledger(journal, out)
    plan = planned(subject, "bronze")
    require(plan.mutated_namespaces == TWINS[:2], "load intent must include the bronze and dictionary twins")
    require("--task-role" in plan.argv and "--bucket" in plan.argv, "AWS load must use the task role and the supplied bucket")
    require(plan.argv[plan.argv.index("--namespace") + 1] == "bronze_e2e", "load target must be the bronze twin")
    require(plan.argv[plan.argv.index("--dictionary-namespace") + 1] == "bronze_dictionary_e2e", "load must bind the dictionary twin")
    subject.admit(plan, ledger)
    require(ledger.intents()["synthetic_try"]["namespaces"] == sorted(TWINS[:2]), "both namespace intents must exist before the first effect")
    service.run(plan)
    first = copy.deepcopy(service.tables)
    subject.complete(plan, ledger, service.observation(plan.mutated_namespaces), terminal=True, quiesced=True)
    repeated = subject.plan(replace(plan.binding, try_id="repeated_load"), "bronze", tables=["sample"], table_map=MAP, authority_epoch=42)
    subject.admit(repeated, ledger)
    service.run(repeated)
    require(service.tables == first, "repeated dispatch changed the synthetic journal fixture")
    result = subject.complete(repeated, ledger, service.observation(repeated.mutated_namespaces), terminal=True, quiesced=True)
    require(result["status"] == "confirmed", "full live terminal observation must confirm the intent")
    require(set(result["namespaces"]) == set(TWINS[:2]), "completion must contain both complete namespaces")
    return {"namespaces": list(plan.mutated_namespaces), "synthetic_checksum": hashlib.sha256(json.dumps(first, sort_keys=True).encode()).hexdigest()}


def repeated_dictionary(subject: ModuleType, journal: ModuleType, out: Path) -> dict[str, Any]:
    """Check dictionary dispatch and its complete journal observation. Synthetic effects do not verify native convergence."""
    service = Catalog()
    plan = planned(subject, "dictionary")
    require(plan.mutated_namespaces == ("bronze_dictionary_e2e",), "dictionary intent must name only the dictionary twin")
    require(plan.argv[plan.argv.index("--dictionary-namespace") + 1] == "bronze_dictionary_e2e", "dictionary output escaped the twin")
    ledger = make_ledger(journal, out)
    subject.admit(plan, ledger)
    service.run(plan)
    first = copy.deepcopy(service.tables)
    subject.complete(plan, ledger, service.observation(plan.mutated_namespaces), terminal=True, quiesced=True)
    repeated = subject.plan(replace(plan.binding, try_id="repeated_dictionary"), "dictionary", tables=["sample"], table_map=MAP, authority_epoch=42)
    subject.admit(repeated, ledger)
    service.run(repeated)
    require(first == service.tables, "repeated dictionary dispatch changed the synthetic journal fixture")
    result = subject.complete(repeated, ledger, service.observation(repeated.mutated_namespaces), terminal=True, quiesced=True)
    require(result["status"] == "confirmed", "dictionary completion must confirm its namespace")
    return {"namespace": "bronze_dictionary_e2e", "synthetic_dictionary_rows": 1}


def smoke_cleanup(subject: ModuleType, journal: ModuleType, out: Path) -> dict[str, Any]:
    """Check smoke dispatch and journal observations. Synthetic effects do not verify native scratch cleanup."""
    service = Catalog()
    plan = planned(subject, "smoke")
    require(plan.mutated_namespaces == ("smoke_check_e2e",), "smoke intent escaped its twin")
    require(plan.argv == ("--namespace", "smoke_check_e2e"), "smoke command must bind only its twin")
    ledger = make_ledger(journal, out)
    subject.admit(plan, ledger)
    service.run(plan, leave_scratch=True)
    require("smoke_check_e2e" in service.tables, "stopped smoke must leave observable scratch state")
    require(
        journal.replay({}, ledger.intents(), ledger.completions())["unresolved"] == ["smoke_check_e2e"],
        "a stopped smoke without a completion must stay unresolved",
    )
    stopped = subject.complete(plan, ledger, service.observation(plan.mutated_namespaces), terminal=True, quiesced=True, step="stopped")
    require(stopped["namespaces"]["smoke_check_e2e"]["state"] == "present", "the stopped smoke read must record its leftover table")
    repeated = subject.plan(replace(plan.binding, try_id="repeated_smoke"), "smoke", tables=[], table_map=MAP, authority_epoch=42)
    subject.admit(repeated, ledger)
    service.run(repeated)
    require("smoke_check_e2e" not in service.tables, "repeated smoke must remove scratch state")
    result = subject.complete(repeated, ledger, service.observation(repeated.mutated_namespaces), terminal=True, quiesced=True)
    require(result["namespaces"]["smoke_check_e2e"]["state"] == "absent", "clean smoke completion must record absence")
    return {"scratch_namespace_absent": True, "native_unreferenced_object_listing": "pending lead run"}


def checksum_fixture(subject: ModuleType, journal: ModuleType, out: Path) -> dict[str, Any]:
    """Check fixture create and drop dispatch and journal observations. Synthetic effects do not verify native cleanup."""
    service = Catalog()
    create = subject.plan(binding(subject), "checksum_fixture", tables=[], table_map=MAP, authority_epoch=42, fixture_action="create")
    drop = subject.plan(
        replace(create.binding, try_id="synthetic_drop"), "checksum_fixture", tables=[], table_map=MAP, authority_epoch=42, fixture_action="drop"
    )
    require(create.argv == ("--fixture", "create", "--fixture-namespace", "duckdb_view_check_e2e"), "fixture create escaped its twin")
    require(drop.mutated_namespaces == ("duckdb_view_check_e2e",), "fixture intent must name its twin")
    ledger = make_ledger(journal, out)
    subject.admit(create, ledger)
    service.run(create)
    require("duckdb_view_check_e2e" in service.tables, "fixture create did not produce scratch state")
    subject.complete(create, ledger, service.observation(create.mutated_namespaces), terminal=True, quiesced=True)
    subject.admit(drop, ledger)
    service.run(drop)
    result = subject.complete(drop, ledger, service.observation(drop.mutated_namespaces), terminal=True, quiesced=True)
    require(result["namespaces"]["duckdb_view_check_e2e"]["state"] == "absent", "fixture completion must record absence")
    return {"created_then_absent": True}


def interrupted_writer(subject: ModuleType, journal: ModuleType, out: Path) -> dict[str, Any]:
    """Keep interrupted namespace intents unresolved until a live read observes every namespace."""
    service = Catalog()
    plan = planned(subject, "bronze")
    ledger = make_ledger(journal, out)
    subject.admit(plan, ledger)
    service.run(plan, stop_after_bronze=True)
    require("bronze_dictionary_e2e" not in service.tables, "interruption must precede the dictionary commit")
    unavailable = service.observation(plan.mutated_namespaces, available=False)
    result = subject.complete(plan, ledger, unavailable, terminal=True, quiesced=True)
    require(result["status"] == "unconfirmed", "unavailable live read must leave intent unconfirmed")
    replay = journal.replay({}, ledger.intents(), ledger.completions())
    require(replay["unresolved"] == sorted(TWINS[:2]), "interrupted load must withhold both namespaces")
    rebuilt = subject.complete(plan, ledger, service.observation(plan.mutated_namespaces, kind="rebuild"), terminal=True, quiesced=True, step="rebuilt")
    require(rebuilt["status"] == "unconfirmed", "rebuilt database must not confirm a pending write")
    service.run(plan)
    result = subject.complete(plan, ledger, service.observation(plan.mutated_namespaces), terminal=True, quiesced=True, step="reconciled")
    require(result["status"] == "confirmed", "live reconciliation did not confirm the writer")
    require(not journal.replay({}, ledger.intents(), ledger.completions())["unresolved"], "confirmed reconciliation must resolve both namespaces")
    incomplete = service.observation(("bronze_e2e",))
    try:
        subject.complete(plan, ledger, incomplete, terminal=True, quiesced=True, step="incomplete")
    except ValueError as error:
        require("every namespace" in str(error), "missing namespace rejected for the wrong cause")
    else:
        raise ValueError("completion accepted an incomplete namespace observation")
    return {"interruption_points": ["between bronze and dictionary", "unavailable terminal read", "rebuilt database"], "reconciled": True}


def removal_and_recovery(subject: ModuleType, journal: ModuleType, out: Path) -> dict[str, Any]:
    """Require temporary removal grants, both namespace intents and the canonical recovery hold."""
    changes = {"removal_grants": True, "removal_approval": "synthetic_run"}
    plan = planned(subject, "removal", **changes)
    require(plan.mutated_namespaces == TWINS[:2], "removal must journal the bronze and dictionary twins")
    require(
        plan.argv == ("--drop-removed", "--namespace", "bronze_e2e", "--dictionary-namespace", "bronze_dictionary_e2e", "--qualification-tables", "sample"),
        "removal command must name the loaded twin table",
    )
    active = subject.grant_plan("aws_qualify", "active", namespace_creation_approved=True, removal_run_id="synthetic_run")
    removal = subject.grant_plan("aws_qualify", "removal", namespace_creation_approved=True, removal_run_id="synthetic_run")
    canonical_drop = [
        item for item in active if item["grant"]["privilege"] == "TABLE_DROP" and item["grant"].get("namespace") in [["bronze_e2e"], ["bronze_dictionary_e2e"]]
    ]
    require(not canonical_drop, "qualification active grants retain bronze or dictionary removal rights")
    production_active = subject.grant_plan("real", "active", namespace_creation_approved=True, removal_run_id="synthetic_run", recovery_hold=False)
    require(
        not any(
            item["grant"]["privilege"] == "TABLE_DROP" and item["grant"].get("namespace") in [["bronze"], ["bronze_dictionary"]] for item in production_active
        ),
        "production active grants retain bronze or dictionary removal rights",
    )
    require(len(removal) == len(active) + 2, "approved removal must add exactly two namespace removal grants")
    require(subject.grant_plan("aws_qualify", "active", namespace_creation_approved=True) == active, "revocation must restore exact active grants")
    subject.verify_grants(active, active)
    try:
        subject.verify_grants(removal, active)
    except subject.BindingError as error:
        require("grant parity" in str(error), "retained removal grants rejected for the wrong cause")
    else:
        raise ValueError("grant parity accepted retained removal rights")
    ready = {"mode": "real", "layer_state": "on", "qualification_passed": True, "activation_approved": True, "run_approval": "synthetic_run", **changes}
    rejected(subject, "removal", "recovery hold", **ready)
    rejected(subject, "removal", "aws_writer", **{**ready, "authority_state": "recovering", "recovery_hold": False})
    rejected(subject, "removal", "removal grant", removal_approval="synthetic_run")
    service = Catalog()
    service.run(planned(subject, "bronze"))
    ledger = make_ledger(journal, out)
    subject.admit(plan, ledger)
    service.run(plan)
    require(all(not tables for tables in service.tables.values()), "removal left a bronze or dictionary table")
    completion = subject.complete(plan, ledger, service.observation(plan.mutated_namespaces), terminal=True, quiesced=True)
    require(all(not state["tables"] for state in completion["namespaces"].values()), "removal completion must include the empty namespace membership")
    return {"removal_grants_revoked": True, "canonical_hold_refused": True}


def scope_denial(subject: ModuleType, journal: ModuleType, out: Path) -> dict[str, Any]:
    """Reject swapped identities, arbitrary namespaces, Mac execution and other source collections."""
    for names in (PRODUCTION, ("silver", *TWINS[1:]), ("gold", *TWINS[1:]), ("features", *TWINS[1:]), ("stray", *TWINS[1:])):
        rejected(subject, "bronze", "namespace", namespaces=names)
    rejected(subject, "bronze", "namespace", mode="real", namespaces=TWINS)
    for changes in ({"principal": "root"}, {"pair": "reader"}, {"task_role": "job_write_bronze_task"}, {"definition": "bronze_writer"}):
        rejected(subject, "bronze", "identity", **changes)
    rejected(subject, "bronze", "AWS", target="mac")
    rejected(subject, "bronze", "FARGATE", capacity_provider="FARGATE_SPOT")
    rejected(subject, "bronze", "source collection", source_collection="other/collection")
    rejected(subject, "bronze", "namespace creation", namespace_creation_approved=False)
    rejected(subject, "checksum_fixture", "fixture action")
    reader = binding(subject, principal="lakehouse_reader", pair="reader", task_role="job_read_task", definition="spark_read")
    read = subject.plan(reader, "checksums", tables=["sample"], table_map=MAP, authority_epoch=42)
    require(not read.mutated_namespaces, "read-only checksums must have no mutation intent")
    require(read.argv == ("--tables", "sample", "--namespace", "bronze_e2e"), "checksum reader must bind the bronze twin")
    return {"forbidden_targets_refused": [*PRODUCTION, "silver", "gold", "features", "stray"], "read_checksums_preserved": True}


def admission_gates(subject: ModuleType, journal: ModuleType, out: Path) -> dict[str, Any]:
    """Check production activation, run approval and scratch approval independently."""
    ready = {
        "mode": "real",
        "layer_state": "on",
        "qualification_passed": True,
        "activation_approved": True,
        "run_approval": "synthetic_run",
        "scratch_approval": "synthetic_run",
    }
    rejected(subject, "bronze", "off", **{**ready, "layer_state": "off"})
    rejected(subject, "bronze", "qualification", **{**ready, "qualification_passed": False})
    rejected(subject, "bronze", "activation", **{**ready, "activation_approved": False})
    rejected(subject, "bronze", "run approval", **{**ready, "run_approval": "another_run"})
    rejected(subject, "smoke", "scratch approval", **{**ready, "scratch_approval": None})
    for job in ("bronze", "dictionary", "smoke"):
        require(bool(planned(subject, job, **ready).mutated_namespaces), "approved production control must dispatch")
    return {"production_defaults_off": True, "approved_control_dispatches": True}


def worker_entrypoint(subject: ModuleType, journal: ModuleType, out: Path) -> dict[str, Any]:
    """Drive the actual AWS worker command through binding parsing, planning and module dispatch."""
    from scripts.lakehouse import aws_bronze

    out.mkdir(parents=True, exist_ok=False)
    path = out / "binding.json"
    path.write_text(json.dumps(asdict(binding(subject, source_collection="cms/hospitals"))))
    calls: list[list[str]] = []

    def worker_main(argv: list[str]) -> int:
        calls.append(argv)
        return 0

    require(hasattr(aws_bronze, "WORKERS"), "worker dispatch requires a literal entrypoint table")
    with patch.dict(aws_bronze.WORKERS, {"scripts.lakehouse.smoke": worker_main}, clear=True):
        status = aws_bronze.main(["--binding", str(path), "--job", "smoke", "--authority-epoch", "42"])
    require(status == 0 and calls == [["--namespace", "smoke_check_e2e"]], "worker entrypoint did not dispatch the closed smoke command")
    with contextlib.redirect_stderr(io.StringIO()) as errors:
        status = aws_bronze.main(["--binding", str(path), "--job", "checksum_fixture", "--authority-epoch", "42"])
    require(status == 1 and "fixture action" in errors.getvalue(), "worker accepted a checksum fixture without an action")
    path.write_text("42\n")
    with contextlib.redirect_stderr(io.StringIO()) as errors:
        status = aws_bronze.main(["--binding", str(path), "--job", "smoke", "--authority-epoch", "42"])
    require(status == 1 and "JSON object" in errors.getvalue(), "worker accepted a malformed run binding")
    return {"entrypoint": "scripts.lakehouse.aws_bronze.main", "bound_command_reached": True, "malformed_binding_refused": True}


def authority_epoch(subject: ModuleType, journal: ModuleType, out: Path) -> dict[str, Any]:
    """Require a numeric bound epoch and compare it with the independently supplied current authority epoch."""
    document = asdict(binding(subject))
    document.update(epoch=42, authority_state="aws_writer")
    selected = subject.Binding(**document)
    try:
        subject.plan(selected, "smoke", tables=[], table_map=MAP, authority_epoch=43)
    except subject.BindingError as error:
        require("stale authority epoch" in str(error), "stale binding rejected for the wrong cause")
    else:
        raise ValueError("writer accepted a stale numeric authority epoch")
    current = subject.plan(selected, "smoke", tables=[], table_map=MAP, authority_epoch=42)
    try:
        subject.admit(current, journal.LocalJournal(out, "43"))
    except subject.BindingError as error:
        require("journal epoch" in str(error), "wrong journal epoch rejected for the wrong cause")
    else:
        raise ValueError("writer admitted its intent into another authority epoch")
    return {"numeric_epoch": 42, "current_epoch": 43, "stale_binding_refused": True}


def worker_import_closure(subject: ModuleType, journal: ModuleType, out: Path) -> dict[str, Any]:
    """Require every repository import and worker configuration file in the declared bootstrap snapshot."""
    root = Path(__file__).resolve().parents[2]
    definitions = json.loads((root / "config/lakehouse/aws_bronze_templates.json").read_text())
    pending = ["scripts.lakehouse.aws_bronze", *(template["module"] for template in definitions["templates"].values())]
    required: set[str] = set()
    while pending:
        name = pending.pop()
        if not name.startswith("scripts."):
            continue
        base = root.joinpath(*name.split("."))
        source = next((candidate for candidate in (base.with_suffix(".py"), base / "__init__.py") if candidate.is_file()), None)
        if source is None or source.relative_to(root).as_posix() in required:
            continue
        relative = source.relative_to(root).as_posix()
        required.add(relative)
        package = source.parent.relative_to(root).parts
        for parent in source.parents:
            if parent == root:
                break
            initializer = parent / "__init__.py"
            if initializer.is_file():
                pending.append(".".join(parent.relative_to(root).parts))
        for node in ast.walk(ast.parse(source.read_text(), filename=relative)):
            if isinstance(node, ast.Import):
                pending.extend(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom):
                parts = [*(package[: len(package) - node.level + 1] if node.level else ()), *(node.module or "").split(".")]
                imported = ".".join(part for part in parts if part)
                pending.append(imported)
                pending.extend(f"{imported}.{alias.name}" for alias in node.names if alias.name != "*")
    configs = {
        "config/lakehouse/aws_bronze_templates.json",
        "config/lakehouse/bronze_tables.json",
        "config/lakehouse/removed_tables.json",
        "config/lakehouse/retired_objects.json",
    }
    staged = definitions.get("stage_files", [])
    require(isinstance(staged, list) and all(isinstance(path, str) for path in staged), "worker stage list must contain file paths")
    missing = sorted((required | configs) - set(staged))
    require(not missing, "worker snapshot omits import or configuration files: " + ", ".join(missing))
    require(len(staged) == len(set(staged)) and set(staged) == required | configs, "worker stage list must match its import and configuration closure")
    out.mkdir(parents=True, exist_ok=False)
    (out / "stage_files.json").write_text(json.dumps(sorted(staged), indent=2) + "\n")
    return {"repository_modules": sorted(required), "stage_files": sorted(staged), "third_party_runtime": "reviewed Spark image"}


CASES = {
    "repeated_load": repeated_load,
    "repeated_dictionary": repeated_dictionary,
    "smoke_cleanup": smoke_cleanup,
    "checksum_fixture": checksum_fixture,
    "interrupted_writer": interrupted_writer,
    "removal_and_recovery": removal_and_recovery,
    "scope_denial": scope_denial,
    "admission_gates": admission_gates,
    "namespace_arguments": namespace_arguments,
    "storage_policy_scope": storage_policy_scope,
    "task_credentials": task_credentials,
    "worker_entrypoint": worker_entrypoint,
    "authority_epoch": authority_epoch,
    "worker_import_closure": worker_import_closure,
}
