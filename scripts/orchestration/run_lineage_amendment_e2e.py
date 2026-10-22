"""Light boundary checks for lineage service bindings and invocation-bound dbt artifacts."""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import io
import json
import os
import sys
import tempfile
import time
from collections.abc import Mapping
from contextlib import nullcontext
from datetime import UTC, datetime
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any
from unittest.mock import NonCallableMock, patch

from orchestration import dbt_lineage, launcher
from orchestration.interface import SubmissionRequest
from orchestration.lineage import build_events, canonical
from scripts.infrastructure import secret_store
from scripts.infrastructure.secret_store_stand_in import StandIn
from scripts.lakehouse import run_lock
from scripts.lakehouse.run_lock import Registry
from scripts.orchestration.broker import Broker, BrokerPaths, load_templates

ROOT = Path(__file__).resolve().parents[2]


def network_operation() -> None:
    """Use the actual operation producer and startup consumer without starting a worker or daemon."""
    subject = object.__new__(launcher.Launcher)
    subject.registry_id = "12345678-synthetic"
    subject.inflight = set()
    stack: dict[str, Any] = {"phase": "starting", "step": "network", "networks": {}, "services": {}}
    state: dict[str, Any] = {"operations": {}, "containers": {}, "launcher": {"lineage": stack}, "attempts": {}}

    def settle(_state: Any, op_id: str, _result: str, **_kwargs: Any) -> None:
        state["operations"][op_id]["state"] = "closed"

    for role in ("backend", "frontend"):
        actions: list[Any] = []
        subject.advance_lineage_start(state, stack, actions)
        if len(actions) != 1:
            raise AssertionError("missing network creation action")
        with patch.object(subject, "spawn") as spawn:
            actions[0]()
        if spawn.call_args.args[3] != f"hai-12345678-lineage-{role}":
            raise AssertionError("network startup lost the recorded role")
        operation = next(value for value in state["operations"].values() if value["state"] == "open")
        repeated: list[Any] = []
        subject.advance_lineage_start(state, stack, repeated)
        if len(state["operations"]) != len(stack["networks"]) + 1 or len(repeated) != 1:
            raise AssertionError("startup retry created a duplicate network operation")
        subject.inflight = {f"op:{operation['op_id']}"}
        waiting: list[Any] = []
        subject.advance_lineage_start(state, stack, waiting)
        if waiting:
            raise AssertionError("inflight network operation was dispatched again")
        subject.inflight = set()
        with patch.object(subject, "tx", return_value=nullcontext(state)), patch.object(subject, "settle_op", side_effect=settle):
            subject.on_lineagenetcreate(f"op:{operation['op_id']}", "ok", f"synthetic-{role}")
        if stack["networks"].get(role) != f"synthetic-{role}":
            raise AssertionError("network completion lost the recorded role")
    subject.advance_lineage_start(state, stack, [])
    if stack["step"] != launcher.LINEAGE_ORDER[0]:
        raise AssertionError("startup did not advance after both networks")
    stack["phase"] = "tearing_down"
    stack["networks"] = {}
    operation_id = subject.record_network_op(state, "lineage_network_create", name=subject.lineage_network_name("backend"))
    actions = []
    subject.advance_lineage_stop(state, stack, actions)
    with patch.object(subject, "spawn") as spawn:
        actions[0]()
    if spawn.call_args.args[3] != subject.lineage_network_name("backend"):
        raise AssertionError("teardown did not settle the recorded network creation")
    found = [
        {"Name": subject.lineage_network_name("backend"), "Id": "synthetic-backend", "Internal": True, "Labels": {launcher.LABEL_STACK: launcher.LINEAGE_STACK}}
    ]
    with patch.object(subject, "tx", return_value=nullcontext(state)), patch.object(subject, "settle_op", side_effect=settle):
        subject.settle_lineage_network_ops(found)
    if stack["networks"] != {"backend": "synthetic-backend"} or state["operations"][operation_id]["state"] != "closed":
        raise AssertionError("network scan recovery did not settle the operation")
    removed = subject.record_network_op(state, "lineage_network_remove", "synthetic-backend", name=subject.lineage_network_name("backend"))
    with patch.object(subject, "tx", return_value=nullcontext(state)), patch.object(subject, "settle_op", side_effect=settle):
        subject.on_lineagenetremove(f"op:{removed}", "ok", None)
    if stack["networks"]:
        raise AssertionError("network removal lost the recorded role")


STARTUP_DEADLINE_SECONDS = 600
PRE_START_PHASES = ("queued", "creating", "created", "starting")


UNREADY = (("degraded", False), ("degraded", True), ("starting", False))


def bare_launcher(status: str, docker_ok: bool, registry: Registry | None = None) -> launcher.Launcher:
    """A launcher without daemon, worker or Docker connection, in one readiness state, over a registry folder when it has one."""
    subject = object.__new__(launcher.Launcher)
    subject.registry_id = "12345678-synthetic"
    subject.inflight = set()
    subject.docker = NonCallableMock(spec_set=())
    subject.docker_ok = docker_ok
    subject.status = status
    subject.timings = launcher.Timings()
    subject.current = None
    subject.current_writes = False
    subject.changed = False
    if registry is not None:
        subject.registry = registry
    return subject


def startup_subject(
    step: str, deadline: float, service_phase: str | None = None, status: str = "serving", docker_ok: bool = True
) -> tuple[Any, dict[str, Any], dict[str, Any]]:
    """A launcher without daemon or Docker, and a recorded starting stack at one step with a recorded startup deadline."""
    subject = bare_launcher(status, docker_ok)
    stack: dict[str, Any] = {"phase": "starting", "step": step, "services": {}, "networks": {}, "health": {}, "error": "", "start_deadline": deadline}
    state: dict[str, Any] = {"operations": {}, "containers": {}, "attempts": {}, "services": {}, "launcher": {"lineage": stack}}
    if service_phase is not None:
        state["attempts"]["at-synthetic"] = {"phase": service_phase, "cancel_intent": None, "container_id": "c" * 64, "reason": None}
        stack["services"][step] = "at-synthetic"
    return subject, state, stack


def startup_cases() -> list[tuple[str, str | None]]:
    """Every phase that precedes a started service: the limits, the network and each service before it starts."""
    return [("limits", None), ("network", None), *((service, phase) for service in launcher.LINEAGE_ORDER for phase in PRE_START_PHASES)]


def startup_label(step: str) -> str:
    return step if step in ("limits", "network") else f"{step} start"


def expired_subject(step: str, service_phase: str | None, status: str = "serving", docker_ok: bool = True) -> tuple[Any, dict[str, Any], dict[str, Any]]:
    """A stack whose startup deadline passed a second ago, holding registered service limits and, at the network step, an open creation."""
    subject, state, stack = startup_subject(step, time.time() - 1, service_phase, status, docker_ok)
    state["services"] = {"lineage-postgres": 1}
    if step == "network":
        subject.record_network_op(state, "lineage_network_create", name=subject.lineage_network_name("backend"))
    return subject, state, stack


def expect_timeout(where: str, step: str, stack: Mapping[str, Any]) -> str:
    """The stack record shows the named startup timeout and is handed to teardown; returns the error."""
    if stack["phase"] != "tearing_down" or stack.get("after") != "down":
        raise AssertionError(f"{where}: startup stayed in {stack['phase']} past its deadline")
    for part in (f"{startup_label(step)} phase", f"{STARTUP_DEADLINE_SECONDS} s", "deadline"):
        if part not in stack["error"]:
            raise AssertionError(f"{where}: the timeout error does not name {part!r}: {stack['error']!r}")
    return str(stack["error"])


def startup_expired() -> None:
    """An expired startup deadline ends every pre-start phase with a named timeout and hands the stack to its owned teardown."""
    for step, service_phase in startup_cases():
        subject, state, stack = expired_subject(step, service_phase)
        subject.advance_lineage(state, [])
        where = f"{step} {service_phase or ''}".strip()
        timeout = expect_timeout(where, step, stack)
        subject.advance_lineage(state, [])
        if (stack["phase"], stack["after"], stack["error"]) != ("tearing_down", "down", timeout):
            raise AssertionError(f"{where}: a repeated advance rewrote the timeout record")
        if service_phase is not None and state["attempts"]["at-synthetic"]["cancel_intent"] is None:
            raise AssertionError(f"{where}: teardown left the pre-start service attempt running")


def startup_unready() -> None:
    """While Docker is unavailable, reconnecting or the launcher is restarting, the expired startup is recorded and nothing Docker-dependent runs."""
    for status, docker_ok in UNREADY:
        label = f"{status}, Docker {'ready' if docker_ok else 'unavailable'}"
        for step, service_phase in startup_cases():
            where = f"{label}: {step} {service_phase or ''}".strip()
            subject, state, stack = expired_subject(step, service_phase, status, docker_ok)
            actions: list[Any] = []
            subject.advance_lineage(state, actions)
            timeout = expect_timeout(where, step, stack)
            recorded = json.dumps(state, sort_keys=True)
            subject.advance_lineage(state, actions)
            if actions or json.dumps(state, sort_keys=True) != recorded:
                raise AssertionError(f"{where}: a later tick dispatched work or rewrote the timeout record")
            if service_phase is not None and state["attempts"]["at-synthetic"]["cancel_intent"] is not None:
                raise AssertionError(f"{where}: the service attempt was cancelled before Docker was ready")
            if state["services"] != {"lineage-postgres": 1}:
                raise AssertionError(f"{where}: the service limits were released before cleanup was confirmed")
            subject.status, subject.docker_ok = "serving", True
            subject.advance_lineage(state, [])
            if (stack["phase"], stack["after"], stack["error"]) != ("tearing_down", "down", timeout) or state["services"] != {"lineage-postgres": 1}:
                raise AssertionError(f"{where}: teardown lost the timeout or the retained limits when Docker returned")
            if service_phase is not None and state["attempts"]["at-synthetic"]["cancel_intent"] is None:
                raise AssertionError(f"{where}: teardown did not resume the pre-start service attempt when Docker returned")
        for step, service_phase in startup_cases():
            subject, state, stack = startup_subject(step, time.time() + 3600, service_phase, status, docker_ok)
            subject.advance_lineage(state, [])
            if stack["phase"] != "starting" or stack["error"]:
                raise AssertionError(f"{label}: {step} {service_phase}: a startup inside its deadline was failed: {stack['error']!r}")
        subject, state, stack = startup_subject("postgres", time.time() - 1, "started", status, docker_ok)
        subject.advance_lineage(state, [])
        if stack["phase"] != "starting" or stack["error"]:
            raise AssertionError(f"{label}: a started service was cut off by the startup deadline")
        subject, state, stack = startup_subject("web", time.time() - 1, "started", status, docker_ok)
        stack["phase"] = "up"
        subject.advance_lineage(state, [])
        if stack["phase"] != "up" or stack["error"]:
            raise AssertionError(f"{label}: an up stack was failed by the startup deadline")


def startup_restart() -> None:
    """A restarted launcher whose Docker does not answer records the expired startup in the saved registry, with no Docker use."""
    with tempfile.TemporaryDirectory() as folder:
        registry = Registry(Path(folder).resolve() / "registry", "test")
        for step, service_phase in startup_cases():
            where = f"restart: {step} {service_phase or ''}".strip()
            _, plain, stack = expired_subject(step, service_phase)
            with bare_launcher("starting", False, registry).tx() as saved:
                saved["launcher"]["lineage"] = stack
                for section in ("attempts", "services", "operations"):
                    saved[section] = plain[section]
            bare_launcher("starting", False, registry).advance_all()
            with bare_launcher("starting", False, registry).tx(write=False) as saved:
                persisted = json.loads(json.dumps(saved["launcher"]["lineage"]))
                kept = json.loads(json.dumps(saved["services"]))
                attempts = json.loads(json.dumps(saved["attempts"]))
            timeout = expect_timeout(where, step, persisted)
            if kept != {"lineage-postgres": 1} or any(attempt["cancel_intent"] for attempt in attempts.values()):
                raise AssertionError(f"{where}: the restart released limits or cancelled work before Docker was ready")
            recorded = registry.state_file.read_bytes()
            bare_launcher("starting", False, registry).advance_all()
            if registry.state_file.read_bytes() != recorded or persisted["error"] != timeout:
                raise AssertionError(f"{where}: a later restart tick rewrote the saved timeout")
        for name, record in (("down", {"phase": "down", "services": {}, "error": ""}), ("in-time", {**stack, "start_deadline": time.time() + 3600})):
            with bare_launcher("starting", False, registry).tx() as saved:
                saved["launcher"]["lineage"] = record
            recorded = registry.state_file.read_bytes()
            bare_launcher("starting", False, registry).advance_all()
            if registry.state_file.read_bytes() != recorded:
                raise AssertionError(f"restart: a {name} stack changed the saved registry")


def startup_in_time() -> None:
    """Inside the deadline nothing changes at any step, a started service stays under the health wait and an up stack ignores the deadline."""
    for step, service_phase in startup_cases():
        subject, state, stack = startup_subject(step, time.time() + 3600, service_phase)
        subject.advance_lineage(state, [])
        if stack["phase"] != "starting" or stack["error"]:
            raise AssertionError(f"{step} {service_phase}: startup failed inside its deadline: {stack['error']!r}")
    subject, state, stack = startup_subject("postgres", time.time() - 1, "started")
    actions: list[Any] = []
    subject.advance_lineage(state, actions)
    if stack["phase"] != "starting" or stack["error"] or len(actions) != 1:
        raise AssertionError("a started service was cut off by the startup deadline instead of the health wait")
    stack["waiting_since"] = {"postgres": time.time() - subject.timings.health_wait - 1}
    subject.advance_lineage(state, [])
    if stack["phase"] != "tearing_down" or "postgres was not healthy within 180 s" not in stack["error"]:
        raise AssertionError(f"the health wait no longer decides a started service: {stack['error']!r}")
    subject, state, stack = startup_subject("web", time.time() + 3600, "started")
    stack["health"] = {"web": "healthy"}
    subject.advance_lineage(state, [])
    if stack["phase"] != "up":
        raise AssertionError("a healthy startup did not reach up")
    stack["start_deadline"] = time.time() - 1
    subject.advance_lineage(state, [])
    if stack["phase"] != "up" or stack["error"]:
        raise AssertionError("an up stack was failed by the startup deadline")


def saved_deadline(registry: Registry) -> Any:
    """The startup deadline of the lineage record a fresh launcher object reads from the saved registry."""
    with bare_launcher("serving", True, registry).tx(write=False) as saved:
        return saved["launcher"].get("lineage", {}).get("start_deadline")


def startup_admission() -> None:
    """lineage_up through the real transaction saves one absolute deadline; reloads and repeated requests keep it and time past it ends in the named timeout."""
    with tempfile.TemporaryDirectory() as folder:
        registry = Registry(Path(folder).resolve() / "registry", "test")
        clock = [1000.0]
        saves: list[str] = []
        write_atomic = run_lock.write_atomic

        def counted_write(path: Path, text: str) -> None:
            saves.append(path.name)
            write_atomic(path, text)

        def request(subject: Any, state: dict[str, Any]) -> None:
            subject.foreign = False
            subject.cfg = SimpleNamespace(root=ROOT)
            params = {"state": "/synthetic/state", "api_port": 15000, "web_port": 13000, "checkout": str(ROOT)}
            with (
                patch.object(subject, "check_checkout", return_value=ROOT),
                patch.object(subject, "lineage_state_folder", return_value="/synthetic/state"),
                patch.object(subject, "initialize_lineage_config"),
                patch.object(subject, "growth_reserve_map", return_value={}),
                patch.object(subject, "spawn"),
            ):
                subject.m_lineage_up(SimpleNamespace(params=params, state=state))

        with patch.object(launcher.time, "time", side_effect=lambda: clock[0]), patch.object(run_lock, "write_atomic", counted_write):
            with bare_launcher("serving", True, registry).tx():
                pass
            created = len(saves)
            admitting = bare_launcher("serving", True, registry)
            with admitting.tx() as state:
                request(admitting, state)
            if "registry.json" not in saves[created:]:
                raise AssertionError("admission never reached the registry writer")
            expected = clock[0] + STARTUP_DEADLINE_SECONDS
            recorded = saved_deadline(registry)
            if recorded is None:
                raise AssertionError("the saved lineage record lost its startup deadline")
            if recorded != expected:
                raise AssertionError(f"admission saved a deadline of {recorded} instead of {expected}")
            clock[0] = 1100.0
            with bare_launcher("serving", True, registry).tx() as state:
                state["launcher"]["status"] = "serving"
            if saved_deadline(registry) != recorded:
                raise AssertionError(f"the reloaded startup deadline moved from {recorded} to {saved_deadline(registry)}")
            clock[0] = 1300.0
            repeating = bare_launcher("serving", True, registry)
            with repeating.tx() as state:
                request(repeating, state)
            if saved_deadline(registry) != recorded:
                raise AssertionError("a repeated lineage_up moved the startup deadline")
            clock[0] = recorded + 100.0
            late = bare_launcher("serving", True, registry)
            with late.tx() as state:
                request(late, state)
                actions: list[Any] = []
                late.advance_lineage(state, actions)
            if saved_deadline(registry) != recorded:
                raise AssertionError("a repeated lineage_up after the deadline moved the startup deadline")
            with bare_launcher("serving", True, registry).tx(write=False) as state:
                stack = state["launcher"]["lineage"]
                expect_timeout("admission after reload", "limits", stack)
                if actions:
                    raise AssertionError("the startup timeout dispatched work")


def startup_cleanup() -> None:
    """After a startup timeout a refused removal keeps the network, the limits and the error and reports nothing released, then cleanup completes."""
    subject, state, stack = startup_subject("network", time.time() - 1)
    stack["networks"] = {"backend": "synthetic-backend"}
    state["services"] = {"lineage-postgres": 1}
    subject.advance_lineage(state, [])
    timeout = stack["error"]
    if stack["phase"] != "tearing_down" or not timeout:
        raise AssertionError("the expired startup did not begin teardown")
    with patch.object(subject, "tx", return_value=nullcontext(state)):
        actions: list[Any] = []
        subject.advance_lineage(state, actions)
        removal = next(op for op in state["operations"].values() if op["kind"] == "lineage_network_remove")
        subject.on_lineagenetremove(f"op:{removal['op_id']}", "error", RuntimeError("synthetic refusal"))
        subject.on_lineageconfirm("lineage:confirm", "ok", False)
        retained = stack["phase"] == "tearing_down" and stack["networks"] == {"backend": "synthetic-backend"} and state["services"] == {"lineage-postgres": 1}
        if not retained or stack["error"] != timeout:
            raise AssertionError("a refused cleanup was reported as released or lost the timeout error")
        retry: list[Any] = []
        subject.advance_lineage(state, retry)
        if len(retry) != 1:
            raise AssertionError("teardown did not retry the refused network removal")
        retried = next(op for op in state["operations"].values() if op["state"] == "open")
        subject.on_lineagenetremove(f"op:{retried['op_id']}", "ok", None)
        subject.advance_lineage(state, [])
        subject.on_lineageconfirm("lineage:confirm", "ok", True)
    down = state["launcher"]["lineage"]
    if down["phase"] != "down" or down["error"] != timeout or state["services"]:
        raise AssertionError("completed cleanup did not record the release with the timeout error")


def startup_deadline() -> None:
    """The lineage stack startup is bounded from admission: a named timeout in every pre-start phase and an owned, honestly reported cleanup."""
    startup_expired()
    startup_unready()
    startup_restart()
    startup_in_time()
    startup_admission()
    startup_cleanup()


def dump_topology() -> None:
    """Render the finite dump on the internal backend with only its output writable."""
    synthetic = "synthetic-" + os.urandom(6).hex()
    with tempfile.TemporaryDirectory() as folder, StandIn({secret_store.MARQUEZ_DB_PASSWORD: synthetic}) as stand_in:
        root = Path(folder).resolve()
        for part in ("postgres", "private", "config"):
            (root / part).mkdir()
        config = launcher.Config(
            Registry(root / "registry", "test"), ROOT, "/unused", root / "tokens", root / "runs", allowed_roots=(root,), secrets_endpoint=stand_in.endpoint
        )
        if "lineage_dump" not in launcher.TEMPLATES:
            raise AssertionError("finite lineage dump template is not registered")
        renderer = launcher.Renderer(config, launcher.ComposeModel(ROOT / "services/lineage/docker-compose.checks.yaml"), ROOT, ROOT, b"synthetic")
        inp = launcher.RenderInput("lineage_dump", "lineage", "dump", 1, 1, "op-synthetic", {"JOB_MEMORY_LIMIT": str(1024**3)}, 1, [], {"state": str(root)}, {})
        render_dump = getattr(renderer, "render_lineage_dump", None)
        if not callable(render_dump):
            raise AssertionError("finite lineage dump renderer is not registered")
        rendered = render_dump(inp, launcher.TEMPLATES["lineage_dump"], "synthetic-dump", "12345678-synthetic")
        host = rendered.body["HostConfig"]
        if not rendered.finite or launcher.TEMPLATES["lineage_dump"].owners != ("alloc",):
            raise AssertionError("lineage dump is not a finite allocation-owned job")
        if host["NetworkMode"] != "hai-12345678-lineage-backend" or host.get("PortBindings"):
            raise AssertionError("lineage dump escaped the internal backend or published a port")
        mounts = host["Mounts"]
        if {row["Target"] for row in mounts if not row["ReadOnly"]} != {"/out"}:
            raise AssertionError("lineage dump has an unexpected writable mount")
        credentials = [row for row in rendered.body["Env"] if any(word in row.split("=", 1)[0] for word in ("PASSWORD", "SECRET", "TOKEN", "KEY"))]
        if credentials != [f"MARQUEZ_DB_PASSWORD={synthetic}"] or host.get("Privileged"):
            raise AssertionError("lineage dump passed a credential other than its database password from the secret store, or privileged mode")
        if any(row["Target"].startswith("/run/hai/private") for row in mounts):
            raise AssertionError("lineage dump still mounts the private password folder")


def dump_admission() -> None:
    """Drive the launcher's admission lookups for the finite dump: the memory plan check and the image the attempt runs."""
    with tempfile.TemporaryDirectory() as folder:
        root = Path(folder).resolve()
        subject = object.__new__(launcher.Launcher)
        subject.cfg = launcher.Config(Registry(root / "registry", "test"), ROOT, "/unused", root / "tokens", root / "runs", allowed_roots=(root,))
        subject.compose_models = {}
        subject.main_checkout = ROOT
        subject.key = b"synthetic"
        attempt = {"template": "lineage_dump", "checkout": str(ROOT)}
        if subject.prepare_attempt("at-synthetic-dump", attempt, False, {}) != {"plan": None, "image_error": None}:
            raise AssertionError("prepare_attempt did not admit the service-less template without an image lookup")
        expected = launcher.ComposeModel(ROOT / launcher.LINEAGE_CHECKS_COMPOSE).image("dump")
        if subject.image_reference(attempt) != expected:
            raise AssertionError("image_reference is not the checks Compose dump image")


class CopyConnection:
    """A stand-in psycopg2 connection whose ``copy_expert`` follows the library's file rule: ``str`` to a text file object, ``bytes`` to any other."""

    def __init__(self, tables: dict[str, list[bytes]]) -> None:
        self.tables = tables

    def cursor(self) -> CopyConnection:
        return self

    def __enter__(self) -> CopyConnection:
        return self

    def __exit__(self, *_exc: object) -> None:
        return None

    def copy_expert(self, statement: str, file: Any) -> None:
        for chunk in self.tables[statement.split('"')[1]]:
            file.write(chunk.decode() if isinstance(file, io.TextIOBase) else chunk)


def dump_module() -> ModuleType:
    """Load the shipped dump job against a minimal ``psycopg2`` and ``psycopg2.sql`` so its writer runs without a database."""
    sql = SimpleNamespace(
        Identifier=lambda name: f'"{name}"',
        SQL=lambda text: SimpleNamespace(format=lambda name: SimpleNamespace(as_string=lambda _connection: text.format(name))),
    )
    package = SimpleNamespace(sql=sql, Error=Exception)
    spec = importlib.util.spec_from_file_location("lineage_db_dump_probe", ROOT / "services/lineage/db_dump.py")
    if spec is None or spec.loader is None:
        raise AssertionError("the lineage dump job cannot be loaded")
    module = importlib.util.module_from_spec(spec)
    with patch.dict(sys.modules, {"psycopg2": package}):
        spec.loader.exec_module(module)
    return module


def dump_writer() -> None:
    """Write tables through the shipped bounded writer the way psycopg2's ``copy_expert`` does and compare the file with the bytes the server sent."""
    module = dump_module()
    tables = {"empty": [], "events": [b"1\t\xe2\x82", b"\xac\n", b"2\tplain\n"]}
    expected = b"-- table empty\n-- table events\n1\t\xe2\x82\xac\n2\tplain\n"
    with tempfile.TemporaryDirectory() as folder:
        target = Path(folder) / "dump.sql"
        output = module.BoundedDump(os.open(target, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600))
        module.copy_tables(CopyConnection(tables), list(tables), output)
        output.handle.close()
        if target.read_bytes() != expected or output.bytes != len(expected):
            raise AssertionError("the dump file differs from the bytes the server sent, or its byte count is wrong")
        for limit, accepted in ((len(expected), True), (len(expected) - 1, False)):
            bounded = Path(folder) / f"bound_{limit}.sql"
            writer = module.BoundedDump(os.open(bounded, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600))
            with patch.object(module, "MAX_BYTES", limit):
                try:
                    module.copy_tables(CopyConnection(tables), list(tables), writer)
                except ValueError as refusal:
                    if accepted or "exceeds" not in str(refusal):
                        raise AssertionError(f"a dump of the bound {limit} was refused for the wrong reason: {refusal}") from None
                else:
                    if not accepted:
                        raise AssertionError("a dump over the bound was accepted")
            writer.handle.close()
            if bounded.stat().st_size > limit:
                raise AssertionError("bytes beyond the bound reached the dump file")


def network_case(case: str) -> None:
    """Render the actual service specification and test its final ingress and membership boundaries."""
    with tempfile.TemporaryDirectory() as folder, StandIn({secret_store.MARQUEZ_DB_PASSWORD: "synthetic-" + os.urandom(6).hex()}) as stand_in:
        root = Path(folder).resolve()
        config = launcher.Config(
            Registry(root / "registry", "test"), ROOT, "/unused", root / "tokens", root / "runs", allowed_roots=(root,), secrets_endpoint=stand_in.endpoint
        )
        renderer = launcher.Renderer(config, launcher.ComposeModel(ROOT / launcher.LINEAGE_COMPOSE), ROOT, ROOT, b"synthetic")
        values = {"HAI_LINEAGE_STATE": str(root), "HAI_LINEAGE_API_PORT": "15000", "HAI_LINEAGE_WEB_PORT": "13000"}
        values.update({f"HAI_LINEAGE_MEM_{name}": str(1024**3) for name in ("POSTGRES", "API", "WEB")})
        original = renderer.assemble

        def altered(*args: Any, **kwargs: Any) -> Any:
            rendered = original(*args, **kwargs)
            body = rendered.body
            if case == "network_lan_binding":
                body["HostConfig"]["PortBindings"]["5000/tcp"][0]["HostIp"] = ".".join(["0"] * 4)
            elif case == "network_database_binding":
                body["HostConfig"]["PortBindings"] = {"5432/tcp": [{"HostIp": "127.0.0.1", "HostPort": "15432"}]}
            return rendered

        services = ("postgres", "api", "web") if case == "network_membership" else ("api",) if case == "network_lan_binding" else ("postgres",)
        for service in services:
            inp = launcher.RenderInput("lineage_service", "lineage", service, 1, 1, "op-synthetic", values, 1, [], {"service": service}, {})
            with patch.object(renderer, "assemble", altered):
                try:
                    rendered = renderer.render_lineage_service(inp, service, "12345678-synthetic")
                except launcher.LauncherError as error:
                    if case == "network_membership" or error.code != "spec":
                        raise
                    return
            if case != "network_membership":
                raise AssertionError(f"{case}: unsafe final binding was accepted")
            created_on = set(rendered.body.get("NetworkingConfig", {}).get("EndpointsConfig", {}))
            expected = {"hai-12345678-lineage-backend"} if service == "postgres" else {"hai-12345678-lineage-frontend"}
            joined = {"hai-12345678-lineage-backend"} if service == "api" else set()
            if created_on != expected or set(rendered.joins) != joined:
                raise AssertionError(f"{service}: wrong network membership")


def artifact_case(case: str) -> None:
    """Exercise the broker completion boundary with captured native dbt JSON and a recorded attempt digest."""
    run = "20261017T020000Z-abcdef12"
    with tempfile.TemporaryDirectory() as folder:
        root = Path(folder).resolve()
        registry = Registry(root / "registry", "test")
        config = launcher.Config(registry, ROOT, "/unused", root / "tokens", root / "runs")
        broker = Broker(BrokerPaths(registry, root), load_templates(), launcher.LauncherClient(config, start=False))
        request = SubmissionRequest("silver_build", run, "fixture", 1, "synthetic_dag", "silver_build", "manual__synthetic")
        row = broker.templates["silver_build"]["modes"]["fixture"]
        output = broker.placeholder_values(run, "fixture")
        from scripts.orchestration.broker import substitute

        envelope = substitute(row["launcher"]["lineage"], output)
        model = "model.synthetic.measure"
        source = "source.synthetic.bronze.measure"
        row["launcher"]["dbt_nodes"] = {model: envelope["outputs"][0], source: envelope["inputs"][0]}
        if case == "artifact_undeclared_edge":
            row["launcher"]["dbt_nodes"][source] = {"namespace": "file", "name": "fixture/undeclared.duckdb"}
        (root / "runs" / run).mkdir(parents=True)
        (root / "runs" / run / "fixture_lakehouse.duckdb").write_bytes(b"synthetic fixture bronze bytes")
        broker.write_intent(request, row, broker.payload_of(request, row))
        attempt_id = "at-synthetic-artifact"
        artifacts = root / "runs" / run / "attempts" / attempt_id / "dbt_artifacts"
        artifacts.mkdir(parents=True)
        invocation = "synthetic-dbt-invocation"
        documents: dict[str, Any] = {
            "manifest.json": {
                "metadata": {"invocation_id": invocation, "env": {"HAI_ATTEMPT_ID": attempt_id}},
                "nodes": {
                    model: {
                        "resource_type": "model",
                        "compiled_code": "synthetic_sql_secret_do_not_emit",
                        "config": {"materialized": "table"},
                        "depends_on": {"nodes": [source]},
                    }
                },
                "sources": {source: {"resource_type": "source"}},
            },
            "run_results.json": {
                "metadata": {"invocation_id": invocation, "env": {"HAI_ATTEMPT_ID": attempt_id}},
                "results": [{"unique_id": model, "status": "success"}],
            },
        }
        digests = {}
        for name, document in documents.items():
            body = json.dumps(document).encode()
            (artifacts / name).write_bytes(body)
            digests[name] = hashlib.sha256(body).hexdigest()
        if case == "artifact_hash_mismatch":
            (artifacts / "manifest.json").write_text(json.dumps({**documents["manifest.json"], "unexpected_change": True}))
        attempt = {
            "id": attempt_id,
            "template": "fixture_dbt",
            "phase": "ended",
            "reason": "exited",
            "exit_code": 0,
            "caller_identity": request.identity(),
            "container_id": "a" * 64,
            "dbt_artifacts": {"version": 1, "attempt_id": attempt_id, "invocation_id": invocation, "files": digests},
        }
        with (
            patch.object(broker, "attempt_of", return_value=attempt),
            patch.object(broker, "saved_logs", return_value=None),
            patch.object(broker, "validated_build", return_value={}),
        ):
            broker.apply_attempt(request, {})
        last = broker.lines_of(request)[-1]
        if case in ("artifact_undeclared_edge", "artifact_hash_mismatch"):
            if last["event"] != "failed_closed":
                raise AssertionError(f"{case}: invalid artifact was accepted by broker completion")
            expected = model if case == "artifact_undeclared_edge" else "hash"
            if expected not in last["reason"]:
                raise AssertionError(f"{case}: refusal lacks the required model or hash cause: {last['reason']}")
            if (broker.paths.records / run / f"{request.container_name}.json").exists():
                raise AssertionError("invalid artifact created a finished record")
        else:
            if last["event"] != "finished":
                raise AssertionError(f"valid model edges were not persisted: {last.get('reason')}")
            record = json.loads(Path(last["record_path"]).read_text())
            if record.get("dbt_lineage", {}).get("models", [{}])[0].get("model") != model:
                raise AssertionError("accepted dbt model edge missing from record")
            with patch.object(dbt_lineage, "read_artifact", side_effect=AssertionError("emitter read an artifact")):
                events = build_events(record)
            if len(events) != 4 or events[-1]["job"]["name"] != f"synthetic_dag.silver_build.{model}":
                raise AssertionError("persisted model edge did not become a model run")
            if events != build_events(record) or "synthetic_sql_secret_do_not_emit" in canonical(events):
                raise AssertionError("model event identity changed or arbitrary artifact text leaked")
            failed = build_events({**{key: value for key, value in record.items() if key != "input_snapshot_record"}, "exit_code": 1})
            if any(event["outputs"] for event in failed):
                raise AssertionError("a failed invocation asserted a model output")
            captured = root / "captured"
            first = dbt_lineage.capture(artifacts, captured, attempt_id)
            (artifacts / "manifest.json").write_text("not the first capture")
            if dbt_lineage.capture(artifacts, captured, attempt_id) != first:
                raise AssertionError("capture retry replaced frozen invocation bytes")


def main() -> int:
    global launcher
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--launcher-source", type=Path, help="a proposed launcher overlay for isolated checks")
    parser.add_argument(
        "--case",
        action="append",
        choices=(
            "network_lan_binding",
            "network_database_binding",
            "network_membership",
            "network_operation",
            "dump_topology",
            "dump_admission",
            "dump_writer",
            "startup_deadline",
            "artifact_undeclared_edge",
            "artifact_hash_mismatch",
            "artifact_accepted",
        ),
    )
    args = parser.parse_args()
    if args.launcher_source is not None:
        spec = importlib.util.spec_from_file_location("orchestration.launcher", args.launcher_source)
        if spec is None or spec.loader is None:
            parser.error("launcher overlay cannot be loaded")
        launcher = importlib.util.module_from_spec(spec)
        sys.modules["orchestration.launcher"] = launcher
        spec.loader.exec_module(launcher)
    output = ROOT / "data/e2e/lineage_amendment" / datetime.now(UTC).strftime("%Y%m%dT%H%M%S_%fZ")
    output.mkdir(parents=True)
    sources = [
        Path(__file__),
        Path(launcher.__file__),
        *(
            ROOT / path
            for path in (
                "orchestration/dbt_lineage.py",
                "orchestration/lineage.py",
                "orchestration/interface.py",
                "orchestration/snapshots.py",
                "scripts/orchestration/broker.py",
                "config/orchestration/broker_templates.json",
                "data_contracts/orchestration/dbt_lineage_record.schema.json",
                "data_contracts/orchestration/snapshot_record.schema.json",
                "data_contracts/orchestration/submission_request.schema.json",
                "services/lineage/docker-compose.lineage.yaml",
                "services/lineage/docker-compose.checks.yaml",
                "services/lineage/db_dump.py",
            )
        ),
    ]
    before = {str(path.relative_to(ROOT)): hashlib.sha256(path.read_bytes()).hexdigest() for path in sources}
    results = []
    for case in args.case or (
        "network_lan_binding",
        "network_database_binding",
        "network_membership",
        "network_operation",
        "dump_topology",
        "dump_admission",
        "dump_writer",
        "startup_deadline",
        "artifact_undeclared_edge",
        "artifact_hash_mismatch",
        "artifact_accepted",
    ):
        error = None
        try:
            if case == "dump_topology":
                dump_topology()
            elif case == "dump_admission":
                dump_admission()
            elif case == "dump_writer":
                dump_writer()
            elif case == "startup_deadline":
                startup_deadline()
            elif case == "network_operation":
                network_operation()
            else:
                (network_case if case.startswith("network_") else artifact_case)(case)
        except Exception as problem:
            error = f"{type(problem).__name__}: {problem}"
        results.append({"case": case, "pass": error is None, "error": error})
    unchanged = before == {str(path.relative_to(ROOT)): hashlib.sha256(path.read_bytes()).hexdigest() for path in sources}
    summary = {
        "boundary": "light: real render and validation, no daemon or container",
        "cases": results,
        "source_sha256": before,
        "source_unchanged": unchanged,
        "pass": unchanged and all(row["pass"] for row in results),
    }
    (output / "report.json").write_text(json.dumps(summary, indent=2) + "\n")
    sys.stdout.write(json.dumps({"pass": summary["pass"], "report": str(output / "report.json"), "cases": results}) + "\n")
    return 0 if summary["pass"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
