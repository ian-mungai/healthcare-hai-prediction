"""Resolve closed AWS bronze commands and launcher-owned journal inputs without activating a writer."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, fields
from pathlib import Path
from typing import Any, Protocol

ROOT = Path(__file__).resolve().parents[2]
TEMPLATES = ROOT / "config/lakehouse/aws_bronze_templates.json"
GRANTS = ROOT / "config/lakehouse/aws_bronze_grants.json"
PRODUCTION = ("bronze", "bronze_dictionary", "smoke_check", "duckdb_view_check")
QUALIFICATION = ("bronze_e2e", "bronze_dictionary_e2e", "smoke_check_e2e", "duckdb_view_check_e2e")
MUTATIONS = {
    "bronze": ("bronze", "dictionary"),
    "dictionary": ("dictionary",),
    "smoke": ("smoke",),
    "checksum_fixture": ("fixture",),
    "removal": ("bronze", "dictionary"),
    "checksums": (),
}
IDENTIFIER = re.compile(r"[a-z][a-z0-9_]*")
RUN_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]{0,127}")


class BindingError(ValueError):
    """A binding or authority record does not permit the requested bronze command."""


@dataclass(frozen=True)
class Binding:
    """Resolved selector, target and authority inputs supplied by the launcher, without credential values."""

    mode: str
    target: str
    epoch: int
    authority_state: str
    run_id: str
    try_id: str
    database_id: str
    bucket: str
    source_collection: str
    namespaces: tuple[str, ...]
    principal: str
    task_role: str
    definition: str
    pair: str
    capacity_provider: str
    layer_state: str = "off"
    qualification_passed: bool = False
    activation_approved: bool = False
    run_approval: str | None = None
    scratch_approval: str | None = None
    removal_approval: str | None = None
    removal_grants: bool = False
    recovery_hold: bool = True
    namespace_creation_approved: bool = False

    @classmethod
    def from_document(cls, value: dict[str, Any]) -> Binding:
        """Parse the immutable run binding, rejecting extra fields and invalid authority types."""
        if not isinstance(value, dict):
            raise BindingError("run binding must be a JSON object")
        known = {field.name for field in fields(cls)}
        if set(value) - known:
            raise BindingError("run binding contains unknown fields")
        document = dict(value)
        names = document.get("namespaces")
        if not isinstance(names, list) or len(names) != 4 or any(not isinstance(name, str) for name in names):
            raise BindingError("run binding requires all four namespace inputs")
        document["namespaces"] = tuple(names)
        try:
            result = cls(**document)
        except TypeError as error:
            raise BindingError("run binding is missing required selector or target inputs") from error
        _validate_types(result)
        return result


@dataclass(frozen=True)
class JobPlan:
    """One closed worker command and its complete set of possible namespace mutations."""

    binding: Binding
    job: str
    module: str
    job_class: str
    argv: tuple[str, ...]
    mutated_namespaces: tuple[str, ...]
    source_prefixes: tuple[str, ...]


class Journal(Protocol):
    """The writes lane's launcher-side journal interface."""

    epoch: str

    def intent(self, _intent_id: str, /, layer: str, namespaces: list[str], database_id: str) -> dict[str, Any]: ...

    def complete(self, _intent_id: str, /, step: str, observation: dict[str, Any], *, terminal: bool, quiesced: bool) -> dict[str, Any]: ...


def _validate_types(binding: Binding) -> None:
    boolean_fields = ("qualification_passed", "activation_approved", "removal_grants", "recovery_hold", "namespace_creation_approved")
    approval_fields = ("run_approval", "scratch_approval", "removal_approval")
    for field in fields(binding):
        value = getattr(binding, field.name)
        if field.name in boolean_fields:
            valid = type(value) is bool
        elif field.name in approval_fields:
            valid = value is None or isinstance(value, str)
        elif field.name == "epoch":
            valid = type(value) is int and value > 0
        elif field.name == "namespaces":
            valid = isinstance(value, tuple) and len(value) == 4 and all(isinstance(name, str) for name in value)
        else:
            valid = isinstance(value, str)
        if not valid:
            raise BindingError(f"run binding has an invalid {field.name} type")
    if not RUN_ID.fullmatch(binding.run_id) or not re.fullmatch(r"[A-Za-z0-9_]+", binding.try_id) or not binding.database_id:
        raise BindingError("run, try and original database identities are required")
    if not re.fullmatch(r"[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]", binding.bucket):
        raise BindingError("run binding requires a bucket name")


def _admission(binding: Binding, job: str, definitions: dict[str, Any], authority_epoch: int) -> None:
    _validate_types(binding)
    if type(authority_epoch) is not int or authority_epoch < 1:
        raise BindingError("the current numeric authority epoch is required")
    if binding.epoch != authority_epoch:
        raise BindingError("stale authority epoch refused before dispatch")
    if binding.target != "aws":
        raise BindingError("bronze templates require the AWS target")
    if binding.authority_state != "aws_writer":
        raise BindingError("bronze templates require the aws_writer epoch with no recovery in progress")
    if binding.mode not in {"real", "aws_qualify"}:
        raise BindingError("bronze templates require real or aws_qualify mode")
    expected_namespaces = QUALIFICATION if binding.mode == "aws_qualify" else PRODUCTION
    if binding.namespaces != expected_namespaces:
        raise BindingError("namespace inputs differ from the closed run mode")
    read_only = job == "checksums"
    identity_key = "reader_identity" if read_only else "qualification_identity" if binding.mode == "aws_qualify" else "production_identity"
    expected = definitions[identity_key]
    actual = {key: getattr(binding, key) for key in ("principal", "task_role", "definition", "pair")}
    if actual != expected:
        raise BindingError("catalog pair, principal, task role and definition must match the job identity")
    if binding.capacity_provider != "FARGATE":
        raise BindingError("bronze jobs require the FARGATE on-demand capacity provider")
    if read_only:
        return
    if not binding.namespace_creation_approved:
        raise BindingError("namespace creation scope must be verified and approved")
    if binding.mode == "real":
        if binding.layer_state != "on":
            raise BindingError("bronze layer is off")
        if not binding.qualification_passed:
            raise BindingError("bronze qualification must pass before production writes")
        if not binding.activation_approved:
            raise BindingError("bronze activation approval is absent")
        if job in {"smoke", "checksum_fixture"}:
            if binding.scratch_approval != binding.run_id:
                raise BindingError("production scratch approval must name this exact run and its cleanup")
        elif binding.run_approval != binding.run_id:
            raise BindingError("production run approval must name this exact run")
    if job == "removal":
        if not binding.removal_grants or binding.removal_approval != binding.run_id:
            raise BindingError("removal grant and approval must be bound to this exact run")
        if binding.mode == "real" and binding.recovery_hold:
            raise BindingError("canonical removal is refused during the recovery hold")


def plan(binding: Binding, job: str, *, tables: list[str], table_map: dict[str, Any], authority_epoch: int, fixture_action: str | None = None) -> JobPlan:
    """Validate a resolved run and return the worker command without performing any mutation."""
    definitions = json.loads(TEMPLATES.read_text())
    if job not in MUTATIONS or job not in definitions["templates"]:
        raise BindingError("unknown bronze job")
    _admission(binding, job, definitions, authority_epoch)
    mapped = {entry["table"]: entry for entry in table_map["tables"]}
    if len(tables) != len(set(tables)) or any(not IDENTIFIER.fullmatch(name) or name not in mapped for name in tables):
        raise BindingError("table inputs must be unique names from the table map")
    table_job = job in {"bronze", "dictionary", "checksums"} or (job == "removal" and binding.mode == "aws_qualify")
    if table_job and not tables:
        raise BindingError("the job requires explicit tables")
    if not table_job and tables:
        raise BindingError("this scratch or removal command does not accept table overrides")
    if fixture_action is not None and (job != "checksum_fixture" or fixture_action not in {"create", "drop"}):
        raise BindingError("fixture action is valid only for the checksum fixture")
    collection_set = {entry["collection"] for entry in mapped.values()}
    source_prefixes = tuple(sorted(f"{collection}/" for collection in collection_set))
    if binding.mode == "aws_qualify":
        if binding.source_collection not in collection_set or any(mapped[name]["collection"] != binding.source_collection for name in tables):
            raise BindingError("qualification source collection differs from the one approved collection")
        source_prefixes = (f"{binding.source_collection}/",)
    bronze, dictionary, smoke, fixture = binding.namespaces
    targets = dict(zip(("bronze", "dictionary", "smoke", "fixture"), binding.namespaces, strict=True))
    argv: list[str]
    if job == "bronze":
        argv = ["--tables", ",".join(tables), "--namespace", bronze, "--dictionary-namespace", dictionary]
        argv += ["--bucket", binding.bucket, "--task-role"]
    elif job == "dictionary":
        argv = ["--tables", ",".join(tables), "--namespace", bronze, "--dictionary-namespace", dictionary]
    elif job == "smoke":
        argv = ["--namespace", smoke]
    elif job == "checksum_fixture":
        if fixture_action is None:
            raise BindingError("checksum fixture action must be create or drop")
        argv = ["--fixture", fixture_action, "--fixture-namespace", fixture]
    elif job == "removal":
        argv = ["--drop-removed", "--namespace", bronze, "--dictionary-namespace", dictionary]
        if binding.mode == "aws_qualify":
            argv += ["--qualification-tables", ",".join(tables)]
    else:
        argv = ["--tables", *tables, "--namespace", bronze]
    template = definitions["templates"][job]
    return JobPlan(binding, job, template["module"], template["class"], tuple(argv), tuple(targets[name] for name in MUTATIONS[job]), source_prefixes)


def admit(job: JobPlan, journal: Journal) -> dict[str, Any]:
    """Fsync the launcher's intent before dispatch, naming every possible namespace mutation."""
    if not job.mutated_namespaces:
        raise BindingError("read-only checksums have no mutation intent")
    if journal.epoch != str(job.binding.epoch):
        raise BindingError("journal epoch differs from the admitted numeric authority epoch")
    return journal.intent(job.binding.try_id, "bronze", list(job.mutated_namespaces), job.binding.database_id)


def complete(job: JobPlan, journal: Journal, observation: dict[str, Any], *, terminal: bool, quiesced: bool, step: str = "terminal") -> dict[str, Any]:
    """Give the launcher's complete live observation to the writes journal after a terminal task state."""
    if journal.epoch != str(job.binding.epoch):
        raise BindingError("journal epoch differs from the admitted numeric authority epoch")
    if set(observation["namespaces"]) != set(job.mutated_namespaces):
        raise BindingError("completion must observe every namespace of its intent")
    return journal.complete(job.binding.try_id, step, observation, terminal=terminal, quiesced=quiesced)


def grant_plan(
    mode: str, phase: str, *, namespace_creation_approved: bool = False, removal_run_id: str | None = None, recovery_hold: bool = True
) -> list[dict[str, Any]]:
    """Return exact read, load, scratch or temporary removal grants for owner-controlled setup."""
    if mode not in {"real", "aws_qualify"} or phase not in {"prepared", "active", "removal"}:
        raise BindingError("unknown grant mode or phase")
    contract = json.loads(GRANTS.read_text())
    names = QUALIFICATION if mode == "aws_qualify" else PRODUCTION
    grants = [{"grant": {"type": "catalog", "privilege": privilege}} for privilege in contract["catalog_read"]]
    for name in names:
        grants.extend({"grant": {"type": "namespace", "namespace": [name], "privilege": privilege}} for privilege in contract["namespace_read"])
    if phase == "prepared":
        return grants
    if not namespace_creation_approved:
        raise BindingError("namespace creation scope must be verified and approved")
    grants.append({"grant": {key: value for key, value in contract["namespace_create"].items() if key != "approval_required"}})
    for name in names:
        privileges = [*contract["load"], *contract["scratch_extra"]] if name in names[2:] else contract["load"]
        grants.extend({"grant": {"type": "namespace", "namespace": [name], "privilege": privilege}} for privilege in privileges)
    if phase == "removal":
        if not removal_run_id or (mode == "real" and recovery_hold):
            raise BindingError("removal grants require an approved run outside the canonical recovery hold")
        grants.extend({"grant": {"type": "namespace", "namespace": [name], "privilege": privilege}} for name in names[:2] for privilege in contract["removal"])
    return grants


def verify_grants(actual: list[dict[str, Any]], expected: list[dict[str, Any]]) -> None:
    """Reject missing, extra or duplicate grants, including retained canonical removal rights."""
    if sorted(json.dumps(value, sort_keys=True) for value in actual) != sorted(json.dumps(value, sort_keys=True) for value in expected):
        raise BindingError("catalog grant parity differs from the approved phase")
