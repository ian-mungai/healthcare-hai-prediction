"""Exercise controller ownership, durable receipts and conditional dispatch with synthetic services."""

from __future__ import annotations

import argparse
import copy
import hashlib
import importlib
import json
import sys
import tempfile
import threading
import time
from collections.abc import Callable, Mapping
from dataclasses import replace
from datetime import UTC, datetime
from functools import partial
from pathlib import Path
from typing import Any
from unittest import mock

from orchestration import catalog_authority, ecs_target, launcher
from orchestration.interface import SubmissionRefused, SubmissionRequest
from scripts.lakehouse import run_lock
from scripts.lakehouse.catalog_journal import encoded
from scripts.orchestration.run_controller_runtime_e2e import RUN, CompleteEcs, MemoryTokens, configured, running
from scripts.process import run_command

ROOT = Path(__file__).resolve().parents[2]
BOOTSTRAP = ROOT / "services/hai/bootstrap/run.py"
DIGEST = "a" * 64
SOURCES = (
    "orchestration/controller_authority.py",
    "orchestration/controller_latch.py",
    "orchestration/launcher.py",
    "orchestration/ecs_target.py",
    "orchestration/ecs_stage.py",
    "scripts/orchestration/controller_owner.py",
    "scripts/orchestration/controller_runtime.py",
    "services/hai/bootstrap/integrity.py",
    "services/hai/bootstrap/run.py",
    "data_contracts/orchestration/execution.schema.json",
    "data_contracts/orchestration/ecs_job.schema.json",
    "scripts/orchestration/run_controller_authority_e2e.py",
    "scripts/orchestration/ecs_stage_runtime_cases.py",
    "scripts/orchestration/run_controller_tokens_e2e.py",
    "data_contracts/orchestration/ledger_line.schema.json",
    "data_contracts/orchestration/examples/ecs_job.json",
)


def authority() -> Any:
    return importlib.import_module("orchestration.controller_authority")


def require(value: bool, message: str) -> None:
    if not value:
        raise AssertionError(message)


def refuses(call: Callable[[], Any], code: str) -> None:
    try:
        call()
    except (ValueError, RuntimeError, launcher.ClientError) as error:
        require(code in str(error) or getattr(error, "code", None) == code, f"expected {code}, observed {type(error).__name__}: {error}")
    else:
        raise AssertionError(f"accepted operation requiring {code}")


def catalog() -> dict[str, Any]:
    return catalog_authority.authority_record(
        {
            "version": 1,
            "state": "aws_writer",
            "epoch": 7,
            "phase": "P4",
            "production_layers": dict.fromkeys(catalog_authority.LAYERS, "on"),
            "cutover_id": "catalog_move",
            "command_id": "catalog_active",
            "reason": "synthetic qualification",
            "time": "2026-10-25T00:00:00Z",
            "target": "aws",
            "frozen": False,
        }
    )


def command(action: str, epoch: int, *, identity: str | None = None, digest: str = DIGEST) -> dict[str, Any]:
    return {
        "command_id": identity or action,
        "action": action,
        "expected_epoch": epoch,
        "transfer_id": "synthetic_transfer",
        "state_digest": digest,
        "reason": "synthetic ownership case",
        "time": "2026-10-25T00:00:00Z",
    }


def evidence(state: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "state_digest": DIGEST,
        "catalog_sha256": hashlib.sha256(encoded(state["launcher"]["catalog_authority"])).hexdigest(),
        **dict.fromkeys(
            (
                "import_verified",
                "source_stopped",
                "ecs_reconciled",
                "no_pending_writes",
                "no_source_mutations",
                "iam_fenced",
                "no_admitted_tries",
                "copy_back_verified",
                "iam_reversed",
            ),
            True,
        ),
    }


def apply(state: dict[str, Any], folder: Path, request: dict[str, Any], **kwargs: Any) -> dict[str, Any]:
    return authority().owner_command(
        state,
        request,
        intent_directory=folder / "intents",
        owner=True,
        tty=True,
        confirmation=authority().confirmation_text(request),
        evidence=evidence(state),
        **kwargs,
    )


def seed(state: dict[str, Any], folder: Path, *, active: bool = True) -> dict[str, Any]:
    state["launcher"]["catalog_authority"] = catalog()
    apply(state, folder, command("initialize", 0))
    apply(state, folder, command("drain", 1))
    apply(state, folder, command("prepare", 1))
    if active:
        apply(state, folder, command("activate", 2))
    return authority().read_authority(state)


def fresh() -> dict[str, Any]:
    return run_lock.empty_state("synthetic_registry", run_lock.LAUNCHER_SCHEMA)


class ConditionalS3:
    """Store bytes using conditional creates and independently observed readback."""

    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self.puts = 0
        self.lose_put = False
        self.unavailable = False
        self.readback_unavailable = False
        self.observer: Callable[[], None] | None = None
        self.entered = threading.Event()
        self.release = threading.Event()
        self.block = False
        self.closed = False

    def call(self, operation: str, params: Mapping[str, Any], *, deadline: float) -> dict[str, Any]:
        self.calls.append((operation, dict(params)))
        if self.observer is not None:
            self.observer()
        if self.block:
            self.entered.set()
            self.release.wait(timeout=max(0, deadline - time.monotonic()))
        if self.unavailable or time.monotonic() >= deadline or self.readback_unavailable and self.puts and operation == "s3_get_small":
            return {"ok": False, "error": {"kind": "deadline", "code": "synthetic_unavailable"}}
        if operation == "s3_list":
            prefix = str(params["prefix"])
            return {"ok": True, "result": {"objects": [{"key": key, "size": len(body)} for key, body in self.objects.items() if key.startswith(prefix)]}}
        key = str(params["key"])
        if operation == "s3_put_small":
            require(params.get("if_none_match") == "*", "latch write omitted If-None-Match")
            if key in self.objects:
                return {"ok": False, "error": {"kind": "conflict", "code": "PreconditionFailed"}}
            raw = str(params["body_utf8"]).encode()
            require(hashlib.sha256(raw).hexdigest() == params["sha256"], "latch checksum differs")
            self.objects[key] = raw
            self.puts += 1
            if self.lose_put:
                self.lose_put = False
                return {"ok": False, "error": {"kind": "unknown", "code": "lost_response"}}
        elif operation != "s3_get_small":
            raise AssertionError(f"unexpected latch operation {operation}")
        if key not in self.objects:
            return {"ok": False, "error": {"kind": "service", "code": "NoSuchKey"}}
        body = self.objects[key]
        result = {"version_id": "synthetic_version", "size": len(body), "sha256": hashlib.sha256(body).hexdigest()}
        if operation == "s3_get_small":
            result["body_utf8"] = body.decode()
        return {"ok": True, "result": result}

    def close(self, *, deadline: float) -> dict[str, Any]:
        self.release.set()
        self.closed = True
        return {"confirmed": True}


def latch(client: ConditionalS3, *, seconds: float = 1.0) -> Any:
    return importlib.import_module("orchestration.controller_latch").DispatchLatch(client, "synthetic-controller-bucket", seconds=seconds)


def authority_validation(folder: Path) -> None:
    state = fresh()
    seed(state, folder)
    valid = authority().read_authority(state)
    for field, value in (
        ("controller_epoch", True),
        ("controller_epoch", 0),
        ("controller_owner", "other"),
        ("controller_phase", "unknown"),
        ("controller_state_digest", "bad"),
    ):
        refuses(partial(authority().controller_record, {**valid, field: value}), "controller_record")
    refuses(lambda: authority().read_authority(fresh()), "controller_record")


def owner_authorization(folder: Path) -> None:
    state = fresh()
    state["launcher"]["catalog_authority"] = catalog()
    request = command("initialize", 0)
    before = copy.deepcopy(state)
    for owner, tty, confirmation in (
        (False, True, authority().confirmation_text(request)),
        (True, False, authority().confirmation_text(request)),
        (True, True, "wrong"),
    ):
        refuses(
            partial(
                authority().owner_command,
                state,
                request,
                intent_directory=folder / "intents",
                owner=owner,
                tty=tty,
                confirmation=confirmation,
                evidence=evidence(state),
            ),
            "owner_confirmation",
        )
        require(state == before and not (folder / "intents").exists(), "refused owner command changed durable state")


def owner_receipts(folder: Path) -> None:
    state = fresh()
    seed(state, folder)
    original = state["launcher"]["controller_transfer_commands"]["prepare"]["outcome"]
    before = copy.deepcopy(state)
    require(apply(state, folder, command("prepare", 1)) == original, "retry returned current state instead of original receipt")
    require(state == before, "exact retry changed authority or receipts")
    refuses(lambda: apply(state, folder, {**command("prepare", 1), "reason": "different"}), "command_conflict")
    refuses(lambda: apply(state, folder, command("drain", 1, identity="stale_command")), "stale_controller_epoch")


def interrupted_command(folder: Path) -> None:
    state = fresh()
    state["launcher"]["catalog_authority"] = catalog()
    request = command("initialize", 0)
    module = authority()
    original = module.write_once

    def interrupted(path: Path, value: dict[str, Any]) -> None:
        original(path, value)
        raise OSError("synthetic interruption after intent fsync")

    before = copy.deepcopy(state)
    with mock.patch.object(module, "write_once", side_effect=interrupted):
        try:
            apply(state, folder, request)
        except OSError as error:
            require("interruption" in str(error), "wrong interrupted-command error")
        else:
            raise AssertionError("interruption did not reach the intent boundary")
    require(state == before and len(list((folder / "intents").glob("*.json"))) == 1, "intent interruption changed registry")
    require(apply(state, folder, request)["controller_epoch"] == 1, "interrupted command did not recover once")
    require(apply(state, folder, request)["controller_epoch"] == 1, "interrupted command replay incremented epoch")


def competing_commands(folder: Path) -> None:
    registry = run_lock.Registry(folder / "registry", "test")
    with run_lock.transaction(registry, create_schema=run_lock.LAUNCHER_SCHEMA) as state:
        seed(state, folder, active=False)
    with run_lock.transaction(registry) as state:
        first = apply(state, folder, command("activate", 2))
    with run_lock.transaction(registry) as state:
        refuses(lambda: apply(state, folder, command("activate", 2, identity="competing_activation")), "controller_transition")
        require(authority().read_authority(state) == first, "competing activation changed authority")


def transfer_gates(folder: Path) -> None:
    for field in ("import_verified", "source_stopped", "ecs_reconciled", "no_pending_writes", "no_source_mutations", "catalog_sha256"):
        state = fresh()
        state["launcher"]["catalog_authority"] = catalog()
        apply(state, folder / field, command("initialize", 0))
        apply(state, folder / field, command("drain", 1))
        proof = evidence(state)
        proof[field] = False
        request = command("prepare", 1)
        refuses(
            partial(
                authority().owner_command,
                state,
                request,
                intent_directory=folder / field / "intents",
                owner=True,
                tty=True,
                confirmation=authority().confirmation_text(request),
                evidence=proof,
            ),
            "controller_evidence",
        )
    state = fresh()
    seed(state, folder / "activation", active=False)
    proof = {**evidence(state), "iam_fenced": False}
    request = command("activate", 2)
    refuses(
        lambda: authority().owner_command(
            state,
            request,
            intent_directory=folder / "activation/intents",
            owner=True,
            tty=True,
            confirmation=authority().confirmation_text(request),
            evidence=proof,
        ),
        "controller_evidence",
    )


def catalog_preservation(folder: Path) -> None:
    state = fresh()
    seed(state, folder)
    require(state["launcher"]["catalog_authority"] == catalog(), "controller transfer changed catalog authority")
    record = catalog_authority.authority_record(state["launcher"]["catalog_authority"])
    require(
        catalog_authority.check_writer(
            record, epoch=7, layer="gold", mode="aws_qualify", target="ecs", run_id="qualification", namespaces=["gold_e2e"], approval=None
        )
        == {"epoch": 7, "layer": "gold"},
        "catalog API changed",
    )


def latch_protocol(folder: Path) -> None:
    state = fresh()
    seed(state, folder)
    binding = authority().dispatch_binding(state, owner="aws")
    client = ConditionalS3()
    service = latch(client)
    for _ in range(2):
        receipt = service.confirm(binding, time.monotonic() + 2)
        require(receipt["binding"] == binding and receipt["version_id"] == "synthetic_version", "latch readback receipt differs")
    require(client.puts == 1 and len(client.objects) == 1, "latch created more than once")
    require(client.calls[-1][0] == "s3_get_small", "latch success had no readback")
    client.readback_unavailable = True
    refuses(lambda: service.confirm(binding, time.monotonic() + 2), "controller_latch_unconfirmed")


def second_controller(folder: Path) -> None:
    state = fresh()
    seed(state, folder)
    binding = authority().dispatch_binding(state, owner="aws")
    client = ConditionalS3()
    service = latch(client)
    service.confirm(binding, time.monotonic() + 2)
    for field, value in (
        ("registry_id", "another_registry"),
        ("controller_epoch", 3),
        ("controller_state_digest", "b" * 64),
    ):
        refuses(partial(service.confirm, {**binding, field: value}, time.monotonic() + 2), "controller_latch_conflict")
    require(client.puts == 1, "second controller rewrote the latch")


def latch_key_scope(folder: Path) -> None:
    state = fresh()
    seed(state, folder)
    binding = authority().dispatch_binding(state, owner="aws")
    client = ConditionalS3()
    service = latch(client)
    receipt = service.confirm(binding, time.monotonic() + 2)
    expected = "lakehouse/catalog_migration/controller_transfers/synthetic_transfer/dispatch_started.json"
    require(receipt["key"] == expected, "latch key does not match the transfer contract")
    require(all(params.get("key") == expected for operation, params in client.calls if operation != "s3_list"), "latch read or write escaped its key")
    client.objects.clear()
    service.absence("synthetic_transfer", time.monotonic() + 2)
    require(client.calls[-1] == ("s3_list", {"bucket": "synthetic-controller-bucket", "prefix": expected}), "rollback listing escaped its transfer key")


def second_transfer(folder: Path) -> None:
    state = fresh()
    seed(state, folder)
    first = authority().dispatch_binding(state, owner="aws")
    second = {**first, "controller_transfer_id": "later_transfer", "controller_epoch": 6}
    client = ConditionalS3()
    service = latch(client)
    service.confirm(first, time.monotonic() + 2)
    original = dict(client.objects)
    receipt = service.confirm(second, time.monotonic() + 2)
    expected = "lakehouse/catalog_migration/controller_transfers/later_transfer/dispatch_started.json"
    require(receipt["key"] == expected and client.puts == 2, "later transfer did not create its own latch")
    require(all(client.objects[key] == body for key, body in original.items()), "later transfer rewrote the first latch")
    require(json.loads(client.objects[expected])["controller_epoch"] == 6, "later latch did not retain its controller binding")
    service.confirm(second, time.monotonic() + 2)
    require(client.puts == 2, "later transfer retry created another latch")
    service.absence("unused_transfer", time.monotonic() + 2)
    refuses(partial(service.absence, "later_transfer", time.monotonic() + 2), "controller_latch_exists")


def rollback(folder: Path) -> None:
    state = fresh()
    seed(state, folder)
    apply(state, folder, command("recover", 2))
    request = command("rollback", 2)
    refuses(lambda: apply(state, folder, request), "controller_latch_absence")
    proof = {**evidence(state), "latch_absent": True}
    record = authority().owner_command(
        state, request, intent_directory=folder / "intents", owner=True, tty=True, confirmation=authority().confirmation_text(request), evidence=proof
    )
    require(record["controller_owner"] == "none" and record["controller_epoch"] == 3, "rollback did not retain closed ownership")
    require(apply(state, folder, command("resume_mac", 3))["controller_epoch"] == 4, "Mac resume did not increase epoch")
    require(state["launcher"]["catalog_authority"] == catalog(), "controller rollback reopened Mac catalog")


def submit(controller: Any) -> tuple[dict[str, Any], dict[str, Any]]:
    controller.client.call("acquire", {"domain": "transform", "run_id": RUN, "mode": "publish_fixture"}, request_id="stable-acquire")
    principal = controller.client.principal_run(RUN)
    folder = controller.config.runs_root / RUN
    folder.mkdir(parents=True, exist_ok=True)
    database = folder / "staging.duckdb"
    database.write_bytes(b"synthetic database placeholder")
    request = {
        "template": "pipeline_validate_gold",
        "launch_id": "synthetic_validation",
        "params": {
            "database": str(database),
            "build_root": str(folder),
            "suites_host": str(ROOT / "data_contracts/great_expectations/gold"),
            "baselines_host": str(ROOT / "data_contracts/great_expectations/fixture_baselines.json"),
            "out_host": str(folder / "quality_gold"),
        },
    }
    result = controller.client.call("submit", request, principal, request_id="stable-submission")
    require(controller.client.call("submit", request, principal, request_id="stable-submission") == result, "submission replay changed identity")
    return result, principal


def await_value(read: Callable[[], Any], predicate: Callable[[Any], bool], label: str, seconds: float = 4) -> Any:
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        value = read()
        if predicate(value):
            return value
        time.sleep(0.01)
    raise AssertionError(f"deadline waiting for {label}")


def environment(folder: Path, client: ConditionalS3) -> launcher.Config:
    folder.mkdir(parents=True, exist_ok=True)
    config = configured(folder)
    with run_lock.transaction(config.registry, create_schema=run_lock.LAUNCHER_SCHEMA) as state:
        seed(state, folder)
    return replace(config, controller_latch=latch(client))


def dispatch_case(folder: Path, variant: str) -> None:
    client = ConditionalS3()
    client.lose_put = variant == "lost"
    client.block = variant in ("unavailable", "late")
    config = environment(folder, client)
    runtime = config.ecs_runtime
    require(isinstance(runtime, CompleteEcs), "missing task facade")
    if not isinstance(runtime, CompleteEcs):
        return

    def observe() -> None:
        saved = json.loads(config.registry.state_file.read_bytes())
        require(saved["launcher"]["controller_dispatch_latch"]["status"] == "pending", "S3 preceded durable latch intent")
        require(any(row.get("execution", {}).get("live_try") for row in saved["attempts"].values()), "S3 preceded dispatch intent")
        require(not runtime.calls, "task dispatched before latch readback")

    client.observer = observe
    with running(config) as controller:
        try:
            result, principal = submit(controller)
            if variant in ("unavailable", "late"):
                require(client.entered.wait(timeout=2), "latch worker did not start")
                require(controller.client.call("status")["status"] == "serving", "latch wait blocked status")
                if variant == "late":
                    request = command("recover", 2)
                    controller.client.call(
                        "controller_authority",
                        {
                            "command": request,
                            "evidence": evidence(json.loads(config.registry.state_file.read_bytes())),
                            "owner": True,
                            "tty": True,
                            "confirmation": authority().confirmation_text(request),
                        },
                    )
                else:
                    client.unavailable = True
                client.release.set()
                await_value(
                    lambda: json.loads(config.registry.state_file.read_bytes())["launcher"]["controller_dispatch_latch"],
                    lambda value: value["status"] != "pending",
                    "latch refusal",
                )
                if variant == "late":
                    await_value(
                        lambda: controller.client.call("status", {"what": "attempt", "id": result["attempt_id"]}, principal),
                        lambda attempt: attempt["phase"] == "ended" or bool(runtime.calls),
                        "late latch dispatch decision",
                    )
                require(not runtime.calls, "unconfirmed or stale latch reached task dispatch")
            else:
                await_value(lambda: runtime.calls, bool, "confirmed task dispatch")
                require(client.puts == 1 and len(runtime.calls) == 1, "first dispatch did not take the latch exactly once")
                stored = controller.client.call("status", {"what": "attempt", "id": result["attempt_id"]}, principal)
                require(stored["execution"]["controller_binding"]["controller_epoch"] == 2, "intent lacks controller epoch")
                require(stored["execution"]["placements"][0]["controller_binding"] == stored["execution"]["controller_binding"], "placement binding changed")
                deadline = stored["deadline_utc"]
                require(stored["execution"]["placements"][0]["deadline_utc"] == deadline, "controller binding reset the deadline")
                request = command("recover", 2)
                controller.client.call(
                    "controller_authority",
                    {
                        "command": request,
                        "evidence": evidence(json.loads(config.registry.state_file.read_bytes())),
                        "owner": True,
                        "tty": True,
                        "confirmation": authority().confirmation_text(request),
                    },
                )
                controller.client.call("cancel", {"attempt_id": result["attempt_id"]}, principal)
                await_value(
                    lambda: controller.client.call("status", {"what": "attempt", "id": result["attempt_id"]}, principal),
                    lambda value: value["phase"] == "ended",
                    "terminal reconciliation while authority closed",
                )
        finally:
            client.release.set()


def first_dispatch(folder: Path) -> None:
    dispatch_case(folder, "normal")


def lost_latch_response(folder: Path) -> None:
    dispatch_case(folder, "lost")


def latch_unavailable(folder: Path) -> None:
    dispatch_case(folder, "unavailable")


def late_latch_result(folder: Path) -> None:
    dispatch_case(folder, "late")


def latch_conflict_fenced(folder: Path) -> None:
    client = ConditionalS3()
    config = environment(folder, client)
    state = json.loads(config.registry.state_file.read_bytes())
    binding = authority().dispatch_binding(state, owner="aws")
    key = "lakehouse/catalog_migration/controller_transfers/synthetic_transfer/dispatch_started.json"
    conflicting = encoded({"version": 1, **binding, "controller_epoch": 3})
    client.objects[key] = conflicting
    runtime = config.ecs_runtime
    require(isinstance(runtime, CompleteEcs), "missing task facade")
    if not isinstance(runtime, CompleteEcs):
        return
    with running(config) as controller:
        submit(controller)
        await_value(
            lambda: json.loads(config.registry.state_file.read_bytes())["launcher"]["controller_dispatch_latch"],
            lambda gate: gate["status"] == "unconfirmed" and gate["last_error"] == "controller_latch_conflict",
            "proven latch conflict",
        )
        time.sleep(1)
        require(controller.client.call("status")["status"] == "serving", "conflict observation stopped the launcher")
        gate = json.loads(config.registry.state_file.read_bytes())["launcher"]["controller_dispatch_latch"]
        require(gate["status"] == "unconfirmed" and gate["last_error"] == "controller_latch_conflict", "proven latch conflict lost its fence")
        puts = sum(operation == "s3_put_small" for operation, _ in client.calls)
        require(puts == 1, f"proven latch conflict was retried: {puts} PUT attempts")
        require(sum(operation == "s3_get_small" for operation, _ in client.calls) == 1, "proven latch conflict repeated readback")
        require(not runtime.calls, "proven latch conflict reached task dispatch")
        require(client.objects == {key: conflicting}, "conflicting latch bytes changed")


def retry_dispatch(folder: Path, *, expire: bool) -> None:
    class RecoveringS3(ConditionalS3):
        def call(self, operation: str, params: Mapping[str, Any], *, deadline: float) -> dict[str, Any]:
            if expire and not self.entered.is_set():
                self.entered.set()
                require(self.release.wait(timeout=2), "expired latch worker was not released")
            return super().call(operation, params, deadline=deadline)

    client = RecoveringS3()
    client.unavailable = not expire
    config = environment(folder, client)
    if expire:
        config = replace(config, controller_latch=latch(client, seconds=0.12))
    runtime = config.ecs_runtime
    require(isinstance(runtime, CompleteEcs), "missing task facade")
    if not isinstance(runtime, CompleteEcs):
        return
    try:
        with running(config) as controller:
            result, principal = submit(controller)
            await_value(
                lambda: json.loads(config.registry.state_file.read_bytes())["launcher"]["controller_dispatch_latch"],
                lambda gate: gate["status"] == "unconfirmed",
                "retryable latch failure",
            )
            before = controller.client.call("status", {"what": "attempt", "id": result["attempt_id"]}, principal)
            require(not runtime.calls, "task dispatched during a retryable latch failure")
            if not expire:
                time.sleep(1)
                require(controller.client.call("status")["status"] == "serving", "latch outage stopped the launcher")
                puts = sum(operation == "s3_put_small" for operation, _ in client.calls)
                gets = sum(operation == "s3_get_small" for operation, _ in client.calls)
                require(1 <= puts <= 5, f"latch outage exceeded five PUT attempts: {puts}")
                require(1 <= gets <= 5, f"latch outage exceeded five GET calls: {gets}")
                require(not runtime.calls, "task dispatched during the latch outage")
            client.unavailable = False
            client.release.set()
            await_value(lambda: runtime.calls, bool, "same-process latch recovery")
            after = controller.client.call("status", {"what": "attempt", "id": result["attempt_id"]}, principal)
            gate = json.loads(config.registry.state_file.read_bytes())["launcher"]["controller_dispatch_latch"]
            require(gate["status"] == "confirmed" and client.puts == 1, "recovered dispatch lacks a confirmed latch")
            require(len(runtime.calls) == 1, "latch retry duplicated task dispatch")
            for field in ("id", "generation", "request", "deadline_utc"):
                require(after[field] == before[field], f"latch retry changed original {field}")
            require(after["execution"]["live_try"] == before["execution"]["live_try"], "latch retry changed the try identity")
    finally:
        client.release.set()


def latch_retry(folder: Path) -> None:
    retry_dispatch(folder, expire=False)


def latch_retry_deadline(folder: Path) -> None:
    retry_dispatch(folder, expire=True)


def placement_binding(folder: Path) -> None:
    client = ConditionalS3()
    client.block = True
    config = environment(folder, client)
    runtime = config.ecs_runtime
    require(isinstance(runtime, CompleteEcs), "missing task facade")
    if not isinstance(runtime, CompleteEcs):
        return
    with running(config) as controller:
        try:
            result, principal = submit(controller)
            require(client.entered.wait(timeout=2), "placement case did not reach latch confirmation")
            with run_lock.transaction(config.registry) as state:
                placement = state["attempts"][result["attempt_id"]]["execution"]["placements"][0]
                placement["controller_binding"]["controller_epoch"] = 3
            client.release.set()
            observed = await_value(
                lambda: controller.client.call("status", {"what": "attempt", "id": result["attempt_id"]}, principal),
                lambda attempt: attempt["phase"] == "ended" or bool(runtime.calls),
                "placement binding refusal",
            )
            require(not runtime.calls, "mismatched placement binding reached task dispatch")
            require(observed["execution"]["placements"][0]["state"] == "fenced", "mismatched placement was not fenced")
        finally:
            client.release.set()


def stale_epoch(folder: Path) -> None:
    state = fresh()
    seed(state, folder)
    binding = authority().dispatch_binding(state, owner="aws")
    state["launcher"]["controller_authority"]["controller_epoch"] = 3
    refuses(lambda: authority().check_dispatch(state, binding, owner="aws"), "stale_controller_epoch")


def owner_route(folder: Path) -> None:
    from scripts.orchestration import controller_owner

    client = ConditionalS3()
    config = environment(folder, client)
    request = command("recover", 2)
    path = folder / "owner_command.json"
    proof = evidence(json.loads(config.registry.state_file.read_bytes()))
    path.write_bytes(encoded({"command": request, "evidence": proof}))
    with running(config) as controller:
        refuses(lambda: controller.client.call("controller_authority", {"command": request}), "owner")
        with (
            mock.patch.object(controller_owner, "drop_identity"),
            mock.patch.object(sys.stdin, "isatty", return_value=True),
            mock.patch.object(sys.stdout, "isatty", return_value=True),
            mock.patch("builtins.input", return_value=authority().confirmation_text(request)),
        ):
            require(controller_owner.execute(config, ["controller-authority", "--request", str(path)]) == 0, "owner command route failed")
        stored = json.loads(config.registry.state_file.read_bytes())["launcher"]
        require(stored["controller_authority"]["controller_phase"] == "recovering", "owner route did not commit authority")


def job_binding(folder: Path) -> None:
    from orchestration.ecs_stage import stage_job

    verified_command = importlib.import_module("scripts.orchestration.ecs_stage_runtime_cases").verified_command

    verified_command(folder, command=BOOTSTRAP)
    job = json.loads((folder / "job.json").read_bytes())
    dispatch = {"controller_binding": job["controller_binding"], "catalog_epoch": job["catalog_epoch"]}
    changed = copy.deepcopy(job)
    changed["controller_binding"]["controller_epoch"] += 1
    refuses(lambda: stage_job(changed, mock.Mock(), (), {}, folder, time.monotonic() + 2, dispatch=dispatch), "controller_binding")


def bootstrap_binding(folder: Path) -> None:
    verified_command = importlib.import_module("scripts.orchestration.ecs_stage_runtime_cases").verified_command

    verified_command(folder, command=BOOTSTRAP)
    job = json.loads((folder / "job.json").read_bytes())
    binding = job["controller_binding"]
    common = [
        "--job",
        str(folder / "job.json"),
        "--job-sha256",
        hashlib.sha256((folder / "job.json").read_bytes()).hexdigest(),
        "--inputs",
        str(folder / "inputs"),
        "--run-id",
        job["run_id"],
        "--generation",
        str(job["generation"]),
        "--attempt-id",
        job["attempt_id"],
        "--try-id",
        job["try_id"],
        "--template",
        job["template"],
        "--controller-registry-id",
        binding["registry_id"],
        "--controller-owner",
        binding["controller_owner"],
        "--controller-epoch",
        str(binding["controller_epoch"] + 1),
        "--controller-transfer-id",
        binding["controller_transfer_id"],
        "--controller-state-digest",
        binding["controller_state_digest"],
        "--workspace",
        str(folder / "wrong_epoch"),
    ]
    result = run_command(sys.executable, ["-I", "-B", str(BOOTSTRAP), *common], cwd=folder, timeout=15)
    require(result.returncode == 71 and json.loads(result.stderr)["error"] == "controller_binding", "bootstrap accepted different controller epoch")
    require(not (folder / "wrong_epoch").exists(), "bootstrap prepared a workspace before epoch refusal")
    common[common.index("--controller-epoch") + 1] = str(binding["controller_epoch"])
    common.extend(("--catalog-epoch", "7"))
    result = run_command(sys.executable, ["-I", "-B", str(BOOTSTRAP), *common], cwd=folder, timeout=15)
    require(result.returncode == 71 and json.loads(result.stderr)["error"] == "catalog_binding", "bootstrap accepted a different catalog epoch")
    missing = common[: common.index("--controller-registry-id")] + ["--workspace", str(folder / "missing_binding")]
    result = run_command(sys.executable, ["-I", "-B", str(BOOTSTRAP), *missing], cwd=folder, timeout=15)
    require(result.returncode == 71 and json.loads(result.stderr)["error"] == "controller_binding", "bootstrap accepted a missing external controller binding")


def latch_restart(folder: Path) -> None:
    client = ConditionalS3()
    client.readback_unavailable = True
    config = environment(folder, client)
    tokens = config.token_provider
    runtime = config.ecs_runtime
    if not isinstance(tokens, MemoryTokens) or not isinstance(runtime, CompleteEcs):
        raise AssertionError("token or task fixture is absent")
    with running(config) as controller:
        result, principal = submit(controller)
        await_value(
            lambda: json.loads(config.registry.state_file.read_bytes())["launcher"]["controller_dispatch_latch"],
            lambda value: value["status"] == "unconfirmed",
            "unconfirmed latch before restart",
        )
        before = controller.client.call("status", {"what": "attempt", "id": result["attempt_id"]}, principal)
        saved_tokens = dict(tokens.values)
        require(client.puts == 1 and not runtime.calls, "ambiguous latch dispatched before restart")
    tokens.values.update(saved_tokens)
    client.readback_unavailable = False
    restarted = replace(config, controller_latch=latch(client))
    with running(restarted) as controller:
        await_value(lambda: runtime.calls, bool, "readback after restart")
        after = controller.client.call("status", {"what": "attempt", "id": result["attempt_id"]}, principal)
        require(client.puts == 1, "restart created another latch version")
        for field in ("id", "generation", "request", "deadline_utc"):
            require(before[field] == after[field], f"restart changed original {field}")
        require(before["execution"]["live_try"] == after["execution"]["live_try"], "restart created another try identity")


def rollback_route(folder: Path) -> None:
    for exists in (False, True):
        client = ConditionalS3()
        config = environment(folder / str(exists), client)
        state = json.loads(config.registry.state_file.read_bytes())
        if exists:
            latch(client).confirm(authority().dispatch_binding(state, owner="aws"), time.monotonic() + 2)
        with running(config) as controller:
            recovery = command("recover", 2)
            controller.client.call(
                "controller_authority",
                {"command": recovery, "evidence": evidence(state), "owner": True, "tty": True, "confirmation": authority().confirmation_text(recovery)},
                request_id="close_admission",
            )
            rollback_command = command("rollback", 2)
            params = {
                "command": rollback_command,
                "evidence": {**evidence(state), "latch_absent": True},
                "owner": True,
                "tty": True,
                "confirmation": authority().confirmation_text(rollback_command),
            }
            if exists:
                refuses(partial(controller.client.call, "controller_authority", params, request_id="rollback"), "controller_latch_exists")
                current = json.loads(config.registry.state_file.read_bytes())["launcher"]["controller_authority"]
                require(current["controller_owner"] == "none" and current["controller_epoch"] == 2, "existing latch reopened Mac ownership")
            else:
                client.unavailable = True
                refuses(partial(controller.client.call, "controller_authority", params, request_id="rollback_unavailable"), "controller_latch_unconfirmed")
                client.unavailable = False
                record = controller.client.call("controller_authority", params, request_id="rollback_retry")
                require(record["controller_owner"] == "none" and record["controller_epoch"] == 3, "confirmed absence did not retain closed rollback")
                resume = command("resume_mac", 3)
                record = controller.client.call(
                    "controller_authority",
                    {"command": resume, "evidence": evidence(state), "owner": True, "tty": True, "confirmation": authority().confirmation_text(resume)},
                )
                require(record["controller_owner"] == "mac" and record["controller_epoch"] == 4, "confirmed rollback did not resume Mac at a higher epoch")
            require(json.loads(config.registry.state_file.read_bytes())["launcher"]["catalog_authority"] == catalog(), "rollback changed catalog authority")


def local_latch_rollback(folder: Path) -> None:
    client = ConditionalS3()
    config = environment(folder, client)
    runtime = config.ecs_runtime
    require(isinstance(runtime, CompleteEcs), "missing task facade")
    if not isinstance(runtime, CompleteEcs):
        return
    with running(config) as controller:
        result, principal = submit(controller)
        await_value(lambda: runtime.calls, bool, "dispatch before local latch rollback")
        state = json.loads(config.registry.state_file.read_bytes())
        gate = state["launcher"]["controller_dispatch_latch"]
        require(gate["status"] == "confirmed", "local rollback case lacks a confirmed latch")
        recovery = command("recover", 2)
        controller.client.call(
            "controller_authority",
            {"command": recovery, "evidence": evidence(state), "owner": True, "tty": True, "confirmation": authority().confirmation_text(recovery)},
        )
        controller.client.call("cancel", {"attempt_id": result["attempt_id"]}, principal)
        await_value(
            lambda: controller.client.call("status", {"what": "attempt", "id": result["attempt_id"]}, principal),
            lambda attempt: attempt["phase"] == "ended",
            "task reconciliation before local rollback",
        )
        client.objects.clear()
        request = command("rollback", 2)
        refuses(
            partial(
                controller.client.call,
                "controller_authority",
                {"command": request, "evidence": evidence(state), "owner": True, "tty": True, "confirmation": authority().confirmation_text(request)},
            ),
            "controller_latch_exists",
        )
        saved = json.loads(config.registry.state_file.read_bytes())["launcher"]
        require(saved["controller_dispatch_latch"] == gate, "refused rollback changed the local latch")
        require(saved["controller_authority"]["controller_epoch"] == 2, "refused rollback advanced controller authority")


def interrupted_evidence_conflict(folder: Path) -> None:
    client = ConditionalS3()
    config = environment(folder, client)
    state = json.loads(config.registry.state_file.read_bytes())
    request = command("recover", 2)
    proof = evidence(state)
    module = authority()
    original = module.write_once

    def interrupted(path: Path, value: dict[str, Any]) -> None:
        original(path, value)
        raise OSError("synthetic interruption after command intent")

    before = copy.deepcopy(state)
    with mock.patch.object(module, "write_once", side_effect=interrupted):
        try:
            module.owner_command(
                state,
                request,
                intent_directory=config.registry.path / "controller_commands",
                owner=True,
                tty=True,
                confirmation=module.confirmation_text(request),
                evidence=proof,
            )
        except OSError as error:
            require("interruption" in str(error), "wrong command interruption")
        else:
            raise AssertionError("command intent did not reach the interruption boundary")
    require(state == before, "interrupted evidence case changed authority")
    intent = config.registry.path / "controller_commands/recover.json"
    frozen = intent.read_bytes()
    params = {
        "command": request,
        "evidence": {**proof, "import_verified": False},
        "owner": True,
        "tty": True,
        "confirmation": module.confirmation_text(request),
    }
    with running(config) as controller:
        try:
            controller.client.call("controller_authority", params, request_id="changed_evidence", budget=1)
        except launcher.ClientError as error:
            require(error.code == "command_conflict", f"interrupted evidence conflict returned {error.code} instead of command_conflict")
        else:
            raise AssertionError("interrupted command accepted changed evidence")
        require(intent.read_bytes() == frozen, "conflicting evidence replaced the durable intent")
        record = controller.client.call("controller_authority", {**params, "evidence": proof}, request_id="original_evidence")
        require(record["controller_phase"] == "recovering", "original interrupted evidence did not recover")


def never_started_cleanup(folder: Path) -> None:
    from scripts.orchestration.controller_runtime import Controller

    client = ConditionalS3()
    config = environment(folder, client)
    controller = Controller(config)
    tokens = config.token_provider
    if tokens is None:
        raise AssertionError("missing token provider")
    first = TimeoutError("synthetic token cleanup timeout")
    try:
        with (
            mock.patch.object(tokens, "close", side_effect=first) as close_tokens,
            mock.patch.object(config.controller_latch, "close", side_effect=TimeoutError("synthetic latch cleanup timeout")) as close_latch,
            mock.patch.object(controller.server.pool, "shutdown", wraps=controller.server.pool.shutdown) as stop_pool,
        ):
            try:
                controller.close()
            except TimeoutError as error:
                require(error is first, "never-started cleanup replaced the first failure")
            else:
                raise AssertionError("never-started cleanup hid the token timeout")
            require(close_latch.call_count == 1, "never-started token timeout skipped latch close")
            require(stop_pool.call_count == 1, "never-started token timeout skipped pool shutdown")
            require(close_tokens.call_args.kwargs["deadline"] == close_latch.call_args.kwargs["deadline"], "cleanup restarted its deadline")
    finally:
        controller.close()
    require(client.closed and controller.closed, "never-started cleanup retry did not close owned resources")


def broker_binding(folder: Path) -> None:
    config = environment(folder, ConditionalS3())
    run_folder = config.runs_root / RUN
    run_folder.mkdir(parents=True)
    (run_folder / "staging.duckdb").write_bytes(b"synthetic database placeholder")
    with running(config, broker=True) as controller:
        broker = controller.broker
        if broker is None:
            raise AssertionError("broker fixture is absent")
        request = SubmissionRequest("gold_validate", RUN, "publish_fixture", 1, "gold_dag", "validate", "manual")
        handle = broker.submit(request)
        require(broker.submit(request) == handle, "controller binding changed broker replay identity")
        status = await_value(lambda: broker.status(handle), lambda value: bool(value.execution and value.execution["placements"]), "broker execution")
        require(status.execution["controller_binding"]["controller_epoch"] == 2, "broker dropped the controller binding")
        lines = broker.ledger.read(RUN)
        require(
            any(row.get("execution", {}).get("controller_binding", {}).get("controller_epoch") == 2 for row in lines),
            "durable broker ledger dropped the controller epoch",
        )


def latch_cleanup(folder: Path) -> None:
    class ReturningS3(ConditionalS3):
        def __init__(self) -> None:
            super().__init__()
            self.returned = threading.Event()
            self.block = True

        def call(self, operation: str, params: Mapping[str, Any], *, deadline: float) -> dict[str, Any]:
            result = super().call(operation, params, deadline=deadline)
            if self.closed:
                time.sleep(min(0.2, max(0, deadline - time.monotonic())))
                self.returned.set()
            return result

    initial = set(threading.enumerate())
    client = ReturningS3()
    config = environment(folder, client)
    early = False
    try:
        with running(config) as controller:
            submit(controller)
            require(client.entered.wait(timeout=2), "latch call did not start before cleanup")
        early = not client.returned.is_set()
    finally:
        client.release.set()
        deadline = time.monotonic() + 3
        for thread in threading.enumerate():
            if thread not in initial:
                thread.join(timeout=max(0, deadline - time.monotonic()))
    require(not early, "controller stopped before the active latch caller returned")


def owner_command_restart(folder: Path) -> None:
    client = ConditionalS3()
    config = environment(folder, client)
    state = json.loads(config.registry.state_file.read_bytes())
    recovery = command("recover", 2)
    rollback_command = command("rollback", 2)
    params = {
        "command": rollback_command,
        "evidence": evidence(state),
        "owner": True,
        "tty": True,
        "confirmation": authority().confirmation_text(rollback_command),
    }
    failures: list[str] = []
    caller: threading.Thread | None = None
    try:
        with running(config) as controller:
            controller.client.call(
                "controller_authority",
                {"command": recovery, "evidence": evidence(state), "owner": True, "tty": True, "confirmation": authority().confirmation_text(recovery)},
                request_id="close_admission",
            )
            client.block = True

            def start_command() -> None:
                try:
                    controller.client.call("controller_authority", params, request_id="interrupted_rollback", timeout=2, budget=2)
                except launcher.ClientError as error:
                    failures.append(error.code)

            caller = threading.Thread(target=start_command, name="synthetic-owner-caller")
            caller.start()
            require(client.entered.wait(timeout=2), "owner rollback did not reach its read boundary")
    finally:
        client.release.set()
        if caller is not None:
            caller.join(timeout=3)
            require(not caller.is_alive(), "interrupted owner caller survived its cleanup deadline")
    require(bool(failures), "stopped controller completed an unconfirmed owner command")
    pending = json.loads(config.registry.state_file.read_bytes())["launcher"]["controller_owner_pending"]
    original_deadline = pending["deadline_utc"]
    client.block = False
    restarted = replace(config, controller_latch=latch(client))
    with running(restarted) as controller:
        result = controller.client.call("controller_authority", params, request_id="interrupted_rollback", timeout=0.5, budget=0.5)
        require(result["controller_epoch"] == 3 and result["controller_owner"] == "none", "interrupted owner command did not resume from its intent")
        saved = json.loads(config.registry.state_file.read_bytes())["launcher"]
        require(saved["controller_owner_pending"]["deadline_utc"] == original_deadline, "owner recovery extended the original operation deadline")
        require(
            controller.client.call("controller_authority", params, request_id="interrupted_rollback") == result,
            "interrupted owner command replay changed its receipt",
        )


def latch_permissions(folder: Path) -> None:
    class ScopedS3(ConditionalS3):
        def call(self, operation: str, params: Mapping[str, Any], *, deadline: float) -> dict[str, Any]:
            if operation == "s3_get_small" and params["key"] not in self.objects:
                self.calls.append((operation, dict(params)))
                return {"ok": False, "error": {"kind": "service", "code": "AccessDenied"}}
            return super().call(operation, params, deadline=deadline)

    client = ScopedS3()
    config = environment(folder / "first_dispatch", client)
    with running(config) as controller:
        submit(controller)
        calls = await_value(lambda: client.calls, bool, "first scoped latch call")
        require(calls[0][0] == "s3_put_small", "first dispatch required bucket listing permission before creating its latch")
        await_value(lambda: config.ecs_runtime.calls if isinstance(config.ecs_runtime, CompleteEcs) else [], bool, "scoped latch dispatch")
    client = ScopedS3()
    config = environment(folder / "rollback", client)
    with running(config) as controller:
        state = json.loads(config.registry.state_file.read_bytes())
        recovery = command("recover", 2)
        controller.client.call(
            "controller_authority",
            {"command": recovery, "evidence": evidence(state), "owner": True, "tty": True, "confirmation": authority().confirmation_text(recovery)},
            request_id="close_admission",
        )
        request = command("rollback", 2)
        result = controller.client.call(
            "controller_authority",
            {"command": request, "evidence": evidence(state), "owner": True, "tty": True, "confirmation": authority().confirmation_text(request)},
            request_id="rollback_scoped",
        )
        require(result["controller_epoch"] == 3, "scoped absence listing did not permit the closed rollback")
        require(bool(client.calls) and client.calls[0][0] == "s3_list", "rollback required an unscoped missing-object GET")


def drain_live_work(folder: Path) -> None:
    class DrainingEcs(CompleteEcs):
        def __init__(self, path: Path) -> None:
            super().__init__(path)
            self.draining_seen = 0

        def advance(self, state: dict[str, Any], attempt: dict[str, Any], now: float) -> None:
            if state["launcher"].get("controller_authority", {}).get("controller_phase") == "draining":
                self.draining_seen += 1
            super().advance(state, attempt, now)

    config = environment(folder, ConditionalS3())
    runtime = DrainingEcs(config.registry.state_file)
    config = replace(config, ecs_runtime=runtime, ecs_run_clear=runtime.run_clear)
    with running(config) as controller:
        result, principal = submit(controller)
        await_value(lambda: runtime.calls, bool, "admitted task before drain")
        try_id = runtime.calls[0]["try_id"]
        runtime.states[try_id] = "running"
        before = await_value(
            lambda: controller.client.call("status", {"what": "attempt", "id": result["attempt_id"]}, principal),
            lambda value: value["execution"]["placements"][0]["started"],
            "started task before drain",
        )
        state = json.loads(config.registry.state_file.read_bytes())
        request = command("drain", 2, identity="drain_live")
        controller.client.call(
            "controller_authority",
            {"command": request, "evidence": evidence(state), "owner": True, "tty": True, "confirmation": authority().confirmation_text(request)},
            request_id="drain_admission",
        )
        await_value(lambda: runtime.draining_seen, bool, "task observation under draining authority")
        after = controller.client.call("status", {"what": "attempt", "id": result["attempt_id"]}, principal)
        require(after["phase"] != "ended" and after["cancel_intent"] is None, "drain cancelled previously dispatched work")
        require(before["execution"]["live_try"] == after["execution"]["live_try"], "drain changed the live try")
        refuses(
            partial(
                controller.client.call,
                "submit",
                {"template": before["template"], "launch_id": "while_draining", "params": state["attempts"][result["attempt_id"]]["params"]},
                principal,
                request_id="new_during_drain",
            ),
            "controller_admission_closed",
        )
        controller.client.call("cancel", {"attempt_id": result["attempt_id"]}, principal)
        await_value(
            lambda: controller.client.call("status", {"what": "attempt", "id": result["attempt_id"]}, principal),
            lambda value: value["phase"] == "ended",
            "explicit cancellation while draining",
        )


CASES = {
    case.__name__: case
    for case in (
        authority_validation,
        owner_authorization,
        owner_receipts,
        interrupted_command,
        competing_commands,
        transfer_gates,
        catalog_preservation,
        latch_protocol,
        latch_key_scope,
        second_transfer,
        second_controller,
        rollback,
        first_dispatch,
        lost_latch_response,
        latch_unavailable,
        latch_conflict_fenced,
        latch_retry,
        latch_retry_deadline,
        placement_binding,
        late_latch_result,
        stale_epoch,
        owner_route,
        job_binding,
        bootstrap_binding,
        latch_restart,
        rollback_route,
        local_latch_rollback,
        interrupted_evidence_conflict,
        never_started_cleanup,
        broker_binding,
        latch_cleanup,
        owner_command_restart,
        latch_permissions,
        drain_live_work,
    )
}


def main() -> int:
    """Run the declared synthetic boundaries and retain failures as repeatable local evidence."""
    cli = argparse.ArgumentParser(description=__doc__)
    cli.add_argument("--output", type=Path, required=True)
    cli.add_argument("--case", action="append", choices=tuple(CASES))
    args = cli.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    started = datetime.now(UTC).isoformat()
    before = {name: hashlib.sha256((ROOT / name).read_bytes()).hexdigest() if (ROOT / name).exists() else "absent" for name in SOURCES}
    initial = set(threading.enumerate())
    results: dict[str, Any] = {}
    for name in args.case or CASES:
        start = time.monotonic()
        with tempfile.TemporaryDirectory(prefix="controller_owner_", dir="/private/tmp") as temporary:
            try:
                CASES[name](Path(temporary).resolve())
                results[name] = {"status": "pass"}
            except (
                AssertionError,
                ValueError,
                RuntimeError,
                ImportError,
                AttributeError,
                TypeError,
                KeyError,
                OSError,
                launcher.ClientError,
                launcher.LauncherError,
                ecs_target.PlacementError,
                run_lock.RegistryError,
                run_lock.Refused,
                SubmissionRefused,
            ) as error:
                results[name] = {"status": "fail", "error_type": type(error).__name__, "message": str(error)}
        results[name]["seconds"] = round(time.monotonic() - start, 3)
    after = {name: hashlib.sha256((ROOT / name).read_bytes()).hexdigest() if (ROOT / name).exists() else "absent" for name in SOURCES}
    remaining = [thread.name for thread in threading.enumerate() if thread not in initial and thread.is_alive()]
    report = {
        "started_utc": started,
        "classification": "Synthetic",
        "scope": "owner commands, registry, conditional S3 stand-in, dispatch facade and isolated bootstrap",
        "command": [sys.executable, "-m", "scripts.orchestration.run_controller_authority_e2e", *sys.argv[1:]],
        "source_sha256": before,
        "source_changed": before != after,
        "cases": results,
        "cleanup": "failed" if remaining else "done",
        "remaining_threads": remaining,
        "python_version": sys.version.split()[0],
        "limits": ["No AWS, Docker, image execution or deployed IAM proof"],
    }
    (args.output / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    (args.output / "retention.json").write_text(
        json.dumps({"class": 4, "purpose": report["scope"], "created": started, "end_condition": "No implementation or review cites this run"}) + "\n"
    )
    failures = [name for name, value in results.items() if value["status"] != "pass"]
    sys.stdout.write(json.dumps({"report": str(args.output / "report.json"), "failed": failures, "cleanup": report["cleanup"]}) + "\n")
    return int(bool(failures) or before != after or bool(remaining))


if __name__ == "__main__":
    sys.exit(main())
