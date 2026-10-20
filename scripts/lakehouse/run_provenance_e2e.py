"""End-to-end runs of the provenance package and the sealed build (D1 sections 10 and 15 row 914, failure modes 920 to 924).

Run ``.venv/bin/python -m scripts.lakehouse.run_provenance_e2e [--launcher-patch] [--case NAME ...]``. Each case starts its own launcher process on its
own test registry and fake Docker daemon (the harness of ``run_launcher_e2e``), drives it through the real client over the real socket and asserts the
registry state, the files of the run folder and the create specifications the daemon received. The fake dbt container writes the build file and the
dbt artifacts the way a build does; the fake consumer container runs the package's own check on the mount source it resolves when the container starts.

The cases marked ``patched`` need the proposed launcher patch (``plans/wp/proposed/l1b_launcher.patch``: build state, write queue, seal step and the
consumer template); run them with ``--launcher-patch``. Without that flag the runner runs the cases that need only the two interface bodies, and
``interface_frozen``, which checks that the launcher diff touches nothing else. No real Docker, catalog, AWS service, bronze table or secret is used.
The macro cases run the pinned host dbt (``.tools/sqlfluff``) on the ``lint`` and ``lakehouse`` targets with an in-memory DuckDB: ``compile`` and
``run-operation`` only, no build.
"""

from __future__ import annotations

import argparse
import contextlib
import fcntl
import hashlib
import importlib
import inspect
import json
import os
import re
import shutil
import signal
import stat
import sys
import threading
import time
import traceback
import uuid
from collections.abc import Callable, Iterator
from datetime import UTC, datetime
from functools import partial
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest import mock

from orchestration import launcher
from scripts.lakehouse import pins as pins_module
from scripts.lakehouse import run_lock
from scripts.lakehouse.run_launcher_e2e import PINS_RUN, CaseFailure, Env, catalog_up, creates, expect_error, expect_error_of, refused, snapshot_request
from scripts.process import run_command

ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / "data" / "e2e" / "provenance"
TOOLS_PYTHON = ROOT / ".tools" / "sqlfluff" / "bin" / "python"
TOOLS_DBT = ROOT / ".tools" / "sqlfluff" / "bin" / "dbt"
INTERFACE_HASH = "1b2ddecae18c84d32d26669ac460a04d601b0a4557b8d9dfb115d878229fe3d7"
RELEASE_COPY_TEST = "release_copy_consistency_dim_release_"
TASKS = ("build", "run", "seed", "snapshot", "test")

Case = Callable[[Env], dict[str, bool]]
CASES: dict[str, tuple[Case, str]] = {}


def case(name: str, kind: str = "any") -> Callable[[Case], Case]:
    """Register a case. ``kind`` is ``any`` (runs with and without the launcher patch), ``unpatched``, ``patched`` or ``proposals`` (every proposed patch)."""

    def register(function: Case) -> Case:
        CASES[name] = (function, kind)
        return function

    return register


def provenance() -> Any:
    """The package module, imported when a case runs so a missing module fails that case only."""
    from scripts.lakehouse import provenance as module

    return module


def raises(function: Callable[[], Any]) -> str | None:
    """The message of the ``ProvenanceError`` the call raises, or None when it returns."""
    try:
        function()
    except provenance().ProvenanceError as error:
        return str(error)
    return None


def caught(function: Callable[[], Any]) -> Exception | None:
    """The exception a call raises, or None when it returns."""
    try:
        function()
    except Exception as error:
        return error
    return None


def has(message: str | None, text: str) -> bool:
    """True when a refusal message exists and names the text."""
    return message is not None and text in message


class E2E(Env):
    """The launcher harness, with read-only run files (snapshots and sealed copies) made removable before the scratch folder is deleted."""

    def close(self) -> list[str]:
        """Make every file and folder of the run folders removable (the token store keeps its modes), then close as the harness does.

        A launcher this case started is stopped even if the harness's own close fails.
        """
        pid = self.ping().get("pid")
        try:
            return self.close_folders()
        finally:
            if pid and pid != os.getpid() and self.alive(int(pid)):
                os.kill(int(pid), signal.SIGTERM)
                with contextlib.suppress(CaseFailure):
                    self.wait(lambda: not self.alive(int(pid)), 10, "the launcher stops")
                if self.alive(int(pid)):
                    os.kill(int(pid), signal.SIGKILL)

    def close_folders(self) -> list[str]:
        """Make the run folders removable, then the harness's close."""
        for current, folders, names in os.walk(self.temp / "runs"):
            for name in folders:
                with contextlib.suppress(OSError):
                    (Path(current) / name).chmod(0o755)
            for name in names:
                with contextlib.suppress(OSError):
                    (Path(current) / name).chmod(0o644)
        return super().close()


class Fakes:
    """What the fake daemon does for each template: pins, dbt builds and consumer checks. A build takes the next entry of ``builds``."""

    def __init__(self, env: Env) -> None:
        self.env = env
        self.tables = pins_module.mapped_tables(ROOT / pins_module.TABLE_MAP)
        self.builds: list[dict[str, Any]] = []
        self.started: list[dict[str, Any]] = []
        self.consumer: dict[str, Any] = {"proceed": threading.Event(), "attached": threading.Event(), "marker": "good", "hold": False}
        env.daemon.on_start = self.on_start
        env.daemon.script = self.script

    @staticmethod
    def mount(container: dict[str, Any], destination: str) -> str | None:
        """The host source of a mount, resolved now (a bind mount attaches when the container starts)."""
        return next((m["Source"] for m in container["Mounts"] if m["Destination"] == destination), None)

    def script(self, container: dict[str, Any]) -> tuple[float, int] | None:
        """How long a container runs and how it ends; services run until stopped."""
        labels = container["Config"]["Labels"]
        template = labels.get("hai.template")
        if template == "real_dbt":
            return container.get("_plan", (0.5, 0))
        if template == "sealed_consumer" or self.sealed_reader(container):
            return None
        if template == "spark_read":
            return 0.4, 0
        if labels.get("hai.service") == "polaris-bootstrap":
            return 0.3, 0
        return None if labels.get("hai.service") else (0.2, 0)

    def on_start(self, container: dict[str, Any]) -> None:
        """Run the template's side effects at the moment the daemon starts the container."""
        template = container["Config"]["Labels"].get("hai.template")
        if template == "spark_read":
            self.pins(container)
        elif template == "real_dbt":
            self.dbt(container)
        elif template == "sealed_consumer":
            self.consume(container)
        elif self.sealed_reader(container):
            self.reader(container)

    def pins(self, container: dict[str, Any]) -> None:
        """The pin job writes ``pins.json`` into the mounted run folder and prints its digest, like the real job."""
        source = next(m["Source"] for m in container["Mounts"] if m["Destination"].startswith("/workspace/data/analytics/dbt/runs/"))
        written = {table: str(1000 + index) for index, table in enumerate(self.tables)}
        text = json.dumps(written, indent=2, sort_keys=True) + "\n"
        (Path(source) / "pins.json").write_text(text)
        container["_out"] = json.dumps({"pinned": len(self.tables), "path": "x", "sha256": hashlib.sha256(text.encode()).hexdigest()}) + "\n"

    def dbt(self, container: dict[str, Any]) -> None:
        """A fake dbt launch: the build file and the artifacts the command writes (a run of tasks writes both; ``compile`` only the manifest; ``debug`` and
        ``ls`` neither). The mount source is the run folder."""
        step = self.builds.pop(0) if self.builds else {}
        mode = step.get("mode", "success")
        args = list(container["Config"].get("Cmd") or [])
        folder = Path(self.mount(container, "/workspace/out") or "")
        invocation = str(uuid.uuid4())
        selected = [args[i + 1] for i, a in enumerate(args[:-1]) if a in ("--select", "-s")]
        excluded = [args[i + 1] for i, a in enumerate(args[:-1]) if a == "--exclude"]
        environment = dict(item.split("=", 1) for item in container["Config"].get("Env") or [])
        self.started.append(
            {"name": container["Name"].lstrip("/"), "args": args, "mode": mode, "env": environment, "at": time.time(), "invocation": invocation}
        )
        command = args[0] if args else ""
        if command not in TASKS:
            if command in ("compile", "parse"):
                (folder / "target").mkdir(parents=True, exist_ok=True)
                (folder / "target" / "manifest.json").write_text(json.dumps({"metadata": {"invocation_id": invocation}}))
            container["_plan"] = (float(step.get("seconds", 0.3)), int(step.get("exit", 0)))
            return
        results: list[dict[str, str]] = [
            {"status": "success", "unique_id": "model.hai.int_silver_build_marker"},
            {"status": "pass", "unique_id": "test.hai.not_null_int_silver_build_marker_dbt_invocation_id.abc"},
        ]
        code = int(step.get("exit", 0))
        if mode == "failed":
            results.append({"status": "error", "unique_id": "model.hai.dim_hospital"})
            code = 1
        elif mode == "warn":
            results.append({"status": "warn", "unique_id": "test.hai.some_other_test.abc"})
        elif mode == "release_copy_warning":
            results.append({"status": "warn", "unique_id": f"test.hai.{RELEASE_COPY_TEST}.abc"})
        (folder / "target").mkdir(parents=True, exist_ok=True)
        if mode == "nodb":
            (folder / "staging.duckdb").unlink(missing_ok=True)
        else:
            (folder / "staging.duckdb").write_bytes(b"DUCKDB-FAKE-" + os.urandom(8192))
        if mode == "wal":
            (folder / "staging.duckdb.wal").write_bytes(b"wal")
        document = {
            "metadata": {"invocation_id": invocation, "dbt_version": "1.11.15"},
            "args": {
                "which": command,
                "select": selected,
                "exclude": excluded,
                "selector": None,
                "empty": False,
                "resource_types": [],
                "exclude_resource_types": [],
            },
            "results": results,
        }
        (folder / "target" / "run_results.json").write_text(json.dumps(document))
        other = str(uuid.uuid4()) if mode == "manifest" else invocation
        (folder / "target" / "manifest.json").write_text(json.dumps({"metadata": {"invocation_id": other}}))
        container["_plan"] = (float(step.get("seconds", 0.5)), code)

    @staticmethod
    def sealed_reader(container: dict[str, Any]) -> bool:
        """True for a reader container that attached the sealed copy (behind the consumer entrypoint or, in a faulty launcher, without it)."""
        return any(m["Destination"] == "/workspace/sealed/build.duckdb" for m in container["Mounts"]) and container["Config"]["Labels"].get("hai.template") in (
            "duckdb_helper",
            "quality_validate",
        )

    def marker_row(self, expected: dict[str, str]) -> dict[str, str]:
        """The marker row the fake build file holds: the expected values, or another run's when the case says so."""
        row = {**expected, "git_revision": "a" * 40}
        if self.consumer["marker"] != "good":
            row["run_id"] = "someone-else"
        row["pins_json"] = self.consumer.get("pins_json", "")
        return row

    def reader(self, container: dict[str, Any]) -> None:
        """A fake reader of the sealed copy: attach the file now; later, behind the consumer entrypoint, check it with the package's own check, print
        the check as the first line of standard error and run the reader (here: print two numbers) only if the check passed. A reader without the
        entrypoint runs on whatever it attached (the fault the entrypoint prevents)."""
        command = list(container["Config"].get("Cmd") or [])
        sealed = self.mount(container, "/workspace/sealed/build.duckdb")
        module = provenance()
        descriptor = os.open(str(sealed), os.O_RDONLY)
        self.consumer["attached"].set()

        def check() -> None:
            self.consumer["proceed"].wait(60)
            if command[:1] != ["/opt/hai/provenance.py"]:
                os.close(descriptor)
                container["_out"] = "1,2\n"
                self.env.daemon.finish(container, 0)
                return
            expected = json.loads(command[3])
            result = module.check_object(descriptor, command[2], expected, lambda: self.marker_row(expected))
            os.close(descriptor)
            container["_err"] = module.RESULT_PREFIX + json.dumps(result, sort_keys=True, separators=(",", ":")) + "\n"
            passed = result["matched"] and result["marker"]["matched"]
            if passed:
                container["_out"] = "1,2\n"
            self.env.daemon.finish(container, 0 if passed else (3 if not result["matched"] else 4))

        threading.Thread(target=check, name="fake-reader", daemon=True).start()

    def consume(self, container: dict[str, Any]) -> None:
        """A fake consumer: attach the sealed file now, check it later with the package's own check and write the result like the real entrypoint."""
        sealed = self.mount(container, "/workspace/sealed/build.duckdb")
        out = Path(self.mount(container, "/workspace/out") or "")
        command = list(container["Config"].get("Cmd") or [])
        environment = dict(item.split("=", 1) for item in container["Config"].get("Env") or [])
        module = provenance()
        descriptor = os.open(str(sealed), os.O_RDONLY)
        self.consumer["attached"].set()

        def check() -> None:
            self.consumer["proceed"].wait(60)
            expected = json.loads(environment["HAI_SEALED_EXPECTED"])
            row = self.marker_row(expected)

            def reader() -> dict[str, str]:
                return row

            result = module.check_object(descriptor, command[command.index("--expected-sha256") + 1], expected, reader)
            os.close(descriptor)
            module.write_result(out / Path(command[command.index("--result") + 1]).name, result)
            self.env.daemon.finish(container, 0 if result["matched"] and result["marker"]["matched"] else 3)

        threading.Thread(target=check, name="fake-consumer", daemon=True).start()


def prepare(
    env: Env, *, run_id: str = PINS_RUN, pinned: bool = True, snapshot: bool = True, builds: list[dict[str, Any]] | None = None
) -> tuple[dict[str, Any], Fakes]:
    """A real transform run with the catalog up, its pins recorded and its snapshot complete; returns the run principal and the daemon's fakes."""
    fakes = Fakes(env)
    fakes.builds += builds or []
    env.start()
    if catalog_up(env) != "up":
        raise CaseFailure("the catalog did not come up")
    run = env.acquire_run(run_id, "transform")
    client = env.client()
    if pinned:
        body = {"template": "spark_read", "launch_id": "pin", "args": ["pin", "--run", run_id], "params": {"job": "pins"}, "checkout": str(ROOT)}
        env.wait_ended(client.call("submit", body, run)["attempt_id"], 30)
        env.wait(lambda: env.state()["runs"][run_id].get("pins_sha256"), 10, "the pins digest is recorded")
    if snapshot:
        _, packages = env.case_folders("snap")
        (packages / "dbt_utils").mkdir(exist_ok=True)
        (packages / "dbt_utils" / "macro.sql").write_text("select 1")
        provenance().request_snapshot(client, run, run_id, ROOT, packages, 120)
    env.make_real(run_id)
    return run, fakes


def dbt_submit(env: Env, run: dict[str, Any], launch: str, args: list[str], template: str = "real_dbt") -> dict[str, Any]:
    """Submit a launch of the run."""
    body = {"template": template, "launch_id": launch, "args": args, "params": {}, "checkout": str(ROOT)}
    return dict(env.client().call("submit", body, run))


def run_record(env: Env, run_id: str = PINS_RUN) -> dict[str, Any]:
    """The run entry as the registry holds it."""
    return dict(env.state()["runs"][run_id])


def run_folder(env: Env, run_id: str = PINS_RUN) -> Path:
    """The run's folder under the test registry's runs root."""
    return env.temp / "runs" / run_id


def sealed_run(env: Env, builds: list[dict[str, Any]] | None = None) -> tuple[dict[str, Any], Fakes, dict[str, Any]]:
    """A run with one full successful build that the launcher sealed; returns the principal, the fakes and the run entry."""
    run, fakes = prepare(env, builds=builds or [{"mode": "success", "seconds": 0.4}])
    reply = dbt_submit(env, run, "full", ["build"])
    env.wait_ended(reply["attempt_id"], 30)
    entry = env.wait(lambda: (r := run_record(env)).get("build_state") == "sealed" and r, 30, "the run is sealed")
    return run, fakes, dict(entry)


def tree_digest_independent(folder: Path) -> str:
    """The launcher's snapshot hash by its own recipe (``rglob`` of files, relative name, content digest), written out again here."""
    digest = hashlib.sha256()
    for label in ("project", "packages"):
        root = folder / label
        for path in sorted(item for item in root.rglob("*") if item.is_file()):
            digest.update(f"{label}/{path.relative_to(root)}".encode() + b"\0" + hashlib.sha256(path.read_bytes()).digest())
    return digest.hexdigest()


@contextlib.contextmanager
def writable(*paths: Path) -> Iterator[None]:
    """Make read-only files and folders writable for one edit and restore their modes after."""
    modes = {path: stat.S_IMODE(path.lstat().st_mode) for path in paths}
    try:
        for path in paths:
            path.chmod(modes[path] | 0o200)
        yield
    finally:
        for path, mode in modes.items():
            if path.exists() or path.is_symlink():
                path.chmod(mode)


@contextlib.contextmanager
def traced_paths(*prefixes: str, before: Callable[[str, str], None] | None = None) -> Iterator[list[tuple[str, str]]]:
    """Record every call of ``os`` that names a path under a prefix. A call relative to a held descriptor (``dir_fd``) is not recorded.

    ``before`` runs just before each ``open`` that is recorded or relative to a descriptor; it receives the call and the name opened. The calls are restored
    on exit.
    """
    calls: list[tuple[str, str]] = []
    paused: list[bool] = []

    def wrap(name: str, original: Callable[..., Any]) -> Callable[..., Any]:
        def traced(path: Any, *args: Any, **kwargs: Any) -> Any:
            relative = kwargs.get("dir_fd") is not None or kwargs.get("src_dir_fd") is not None
            if isinstance(path, str | bytes | os.PathLike) and not paused:
                text = os.fsdecode(path)
                if name == "open" and before is not None and (relative or text.startswith(prefixes)):
                    paused.append(True)
                    try:
                        before(name, text)
                    finally:
                        paused.clear()
                if not relative and text.startswith(prefixes):
                    calls.append((name, text))
            return original(path, *args, **kwargs)

        return traced

    with contextlib.ExitStack() as stack:
        stack.enter_context(mock.patch.object(os, "open", wrap("open", os.open)))
        stack.enter_context(mock.patch.object(os, "lstat", wrap("lstat", os.lstat)))
        stack.enter_context(mock.patch.object(os, "stat", wrap("stat", os.stat)))
        stack.enter_context(mock.patch.object(os, "rename", wrap("rename", os.rename)))
        stack.enter_context(mock.patch.object(os, "mkdir", wrap("mkdir", os.mkdir)))
        stack.enter_context(mock.patch.object(os, "chmod", wrap("chmod", os.chmod)))
        stack.enter_context(mock.patch.object(os, "unlink", wrap("unlink", os.unlink)))
        stack.enter_context(mock.patch.object(os, "rmdir", wrap("rmdir", os.rmdir)))
        stack.enter_context(mock.patch.object(os, "listdir", wrap("listdir", os.listdir)))
        stack.enter_context(mock.patch.object(os, "scandir", wrap("scandir", os.scandir)))
        stack.enter_context(mock.patch.object(os, "readlink", wrap("readlink", os.readlink)))
        yield calls


@case("interface_frozen", "unpatched")
def interface_frozen(env: Env) -> dict[str, bool]:
    """Check the launcher's provenance and sealing signatures against the frozen interface digest."""
    signatures = f"provenance_vars{inspect.signature(launcher.provenance_vars)}\nseal{inspect.signature(launcher.seal)}\n"
    lines = (ROOT / "orchestration/launcher.py").read_text().splitlines()

    return {
        "the interface hash equals the frozen hash": hashlib.sha256(signatures.encode()).hexdigest() == INTERFACE_HASH,
        "each signature line exists once": all(sum(line.startswith(f"def {name}(") for line in lines) == 1 for name in ("provenance_vars", "seal")),
    }


@case("provenance_values")
def provenance_values(env: Env) -> dict[str, bool]:
    """920: a recorded run launches with exactly the six typed variables, each equal to an independent computation; a caller cannot set them."""
    run, fakes = prepare(env, builds=[{"mode": "selected", "seconds": 0.3}])
    entry = run_record(env)
    copy = env.state()["copies"][entry["snapshot"]]
    pins_text = (run_folder(env) / "pins.json").read_text()
    head = run_command("git", ["rev-parse", "HEAD"], cwd=ROOT).stdout.strip()
    reply = dbt_submit(env, run, "b1", ["build", "--select", "int_silver_build_marker"])
    env.wait_ended(reply["attempt_id"], 30)
    body = next(iter(creates(env, "-b1-")), None)
    sent = fakes.started[0]["env"] if fakes.started else {}
    module = provenance()
    wanted = {
        "HAI_BUILD_RUN_ID": PINS_RUN,
        "HAI_BUILD_DBT_TREE_SHA256": tree_digest_independent(Path(copy["folder"])),
        "HAI_BUILD_PINS_SHA256": hashlib.sha256(pins_text.encode()).hexdigest(),
        "HAI_BUILD_PINS_JSON": pins_text,
        "HAI_BUILD_GIT_REVISION": head,
        "HAI_PROVENANCE_REQUIRED": "1",
    }
    by_args = dbt_submit(env, run, "b2", ["build", "--vars", "{HAI_BUILD_RUN_ID: x}"])
    body = {"template": "real_dbt", "launch_id": "b3", "args": ["build"], "params": {"provenance": {"HAI_BUILD_RUN_ID": "x"}}, "checkout": str(ROOT)}
    by_params = env.client().call("submit", body, run)
    ended_args = env.wait_ended(by_args["attempt_id"], 30)
    ended_params = env.wait_ended(by_params["attempt_id"], 30)
    return {
        "a container was created for the launch": body is not None,
        "the container carries the six variables with the independent values": {k: v for k, v in sent.items() if k in wanted} == wanted,
        "the container carries no other build variable": {k for k in sent if k.startswith(("HAI_BUILD", "HAI_PROVENANCE"))} == set(wanted),
        "the module's variables equal the same values": module.variables(entry) == wanted,
        "the pins digest equals the registry's record": sent.get("HAI_BUILD_PINS_SHA256") == entry["pins_sha256"],
        "the tree hash equals the copy record": sent.get("HAI_BUILD_DBT_TREE_SHA256") == copy["dbt_tree_sha256"],
        "a dbt option that sets variables ends the attempt without a container": ended_args["reason"] == "create_failed" and not creates(env, "-b2-"),
        "a provenance parameter ends the attempt without a container": ended_params["reason"] == "create_failed" and not creates(env, "-b3-"),
    }


@case("tree_digest_parity")
def tree_digest_parity(env: Env) -> dict[str, bool]:
    """921: the tree hash the module computes again equals the launcher's own record of the copy and the independent recipe."""
    env.start()
    run = env.acquire_run(PINS_RUN, "transform")
    reply = snapshot_request(env, run)
    env.wait(lambda: env.state()["copies"][reply["copy_id"]]["phase"] == "complete", 120, "the snapshot copy completes")
    copy = env.state()["copies"][reply["copy_id"]]
    module = provenance()
    computed = module.tree_digest(Path(copy["folder"]))
    return {
        "the module equals the launcher's record": computed == copy["dbt_tree_sha256"],
        "the module equals the independent recipe": computed == tree_digest_independent(Path(copy["folder"])),
        "the digest has 64 hex digits": bool(re.fullmatch(r"[0-9a-f]{64}", computed)),
    }


@case("provenance_refusals")
def provenance_refusals(env: Env) -> dict[str, bool]:
    """920 and 921: every missing, changed, linked or unrecorded input refuses with the code ``provenance``; undoing the change accepts again."""
    run, fakes = prepare(env, builds=[{"mode": "selected", "seconds": 0.3}])
    module = provenance()
    entry = run_record(env)
    folder = Path(env.state()["copies"][entry["snapshot"]]["folder"])
    runs = run_folder(env)
    checks: dict[str, bool] = {"the recorded run is accepted": raises(lambda: module.variables(entry)) is None}

    def refusal(label: str, text: str, mapping: dict[str, Any] | None = None) -> None:
        checks[label] = has(raises(lambda: module.variables(mapping if mapping is not None else entry)), text)

    target = next(path for path in sorted((folder / "project").rglob("*.sql")))
    original = target.read_bytes()
    with writable(target):
        target.write_bytes(original + b"\n-- changed after the copy\n")
    refusal("a snapshot file changed after the copy refuses", "changed after it was copied")
    with writable(target):
        target.write_bytes(original)
    checks["the restored snapshot is accepted again"] = raises(lambda: module.variables(entry)) is None
    added = folder / "project" / "added_after_copy.sql"
    with writable(folder / "project"):
        added.write_text("select 1")
    refusal("a file added to the snapshot refuses", "changed after it was copied")
    with writable(folder / "project"):
        added.unlink()
    linked = folder / "project" / "linked.sql"
    with writable(folder / "project"):
        linked.symlink_to(target)
    refusal("a link in the snapshot refuses", "link")
    with writable(folder / "project"):
        linked.unlink()
    pins = runs / "pins.json"
    pins_original = pins.read_bytes()
    pins.write_text(json.dumps({"x": "1"}))
    refusal("pins replaced after the record refuse", "differs from the recorded digest")
    pins.write_bytes(pins_original)
    moved = runs / "pins.moved"
    pins.rename(moved)
    pins.symlink_to(moved)
    refusal("a link as the pins file refuses", "without following links")
    pins.unlink()
    moved.rename(pins)
    aside = runs.with_name(runs.name + ".aside")
    runs.rename(aside)
    runs.symlink_to(aside)
    refusal("a link as the run folder refuses", "without following links")
    runs.unlink()
    aside.rename(runs)
    checks["the restored files are accepted again"] = raises(lambda: module.variables(entry)) is None
    refusal("no recorded pins refuse", "no recorded pins", {k: v for k, v in entry.items() if k != "pins_sha256"})
    refusal("no snapshot pointer refuses", "no complete snapshot", {**entry, "snapshot": None})
    refusal("a fixture run refuses", "not a real transform run", {**entry, "mode": "fixture"})
    refusal("an acquisition run refuses", "not a real transform run", {**entry, "domain": "acquisition"})
    clone = env.temp / "registry_clone"
    clone.mkdir()
    for name in ("registry_id", "launcher_test.json"):
        shutil.copy2(env.registry / name, clone / name)
    state = json.loads((env.registry / "registry.json").read_text())
    state["copies"][entry["snapshot"]]["phase"] = "abandoned"
    (clone / "registry.json").write_text(json.dumps(state))
    refusal("a copy record that is not complete refuses", "not a complete copy", {**entry, "registry_path": str(clone)})
    del state["copies"][entry["snapshot"]]
    (clone / "registry.json").write_text(json.dumps(state))
    refusal("a missing copy record refuses", "has no record", {**entry, "registry_path": str(clone)})
    nogit = env.temp / "nogit"
    nogit.mkdir()
    refusal("a checkout without a Git revision refuses", "Git revision", {**entry, "checkout": str(nogit)})
    (nogit / ".git").mkdir()
    (nogit / ".git" / "HEAD").write_text("ref: refs/heads/main\n")
    refusal("a branch that names no commit refuses", "Git revision", {**entry, "checkout": str(nogit)})
    (nogit / ".git" / "HEAD").write_text("unrecorded\n")
    refusal("a revision that is the word unrecorded refuses", "Git revision", {**entry, "checkout": str(nogit)})
    commit = "a" * 40
    (nogit / ".git" / "packed-refs").write_text(f"# pack-refs\n{commit} refs/heads/main\n")
    (nogit / ".git" / "HEAD").write_text("ref: refs/heads/main\n")
    checks["a packed ref is read"] = raises(lambda: module.variables({**entry, "checkout": str(nogit)})) is None
    with writable(pins):
        pins.write_text(json.dumps({"x": "1"}))
    submit_error = expect_error(lambda: dbt_submit(env, run, "bad", ["build"]))
    with writable(pins):
        pins.write_bytes(pins_original)
    checks["the launcher refuses a launch with the code provenance"] = refused(submit_error, "provenance")
    checks["no container was created for the refused launch"] = not creates(env, "-bad-")
    return checks


@case("provenance_selection_bypass")
def provenance_selection_bypass(env: Env) -> dict[str, bool]:
    """920 (L1 round 2 finding 1): without recorded pins no real_dbt launch is admitted, whatever its command or selection; with pins they are."""
    run, fakes = prepare(env, pinned=False, builds=[{"mode": "selected", "seconds": 0.2}] * 2)
    commands = {
        "selected build": ["build", "--select", "int_hai_state_windows"],
        "run": ["run", "--select", "int_hai_state_windows"],
        "ls": ["ls"],
        "debug": ["debug"],
        "compile": ["compile"],
        "test": ["test"],
    }
    errors = {name: expect_error(partial(dbt_submit, env, run, f"x{index}", args)) for index, (name, args) in enumerate(commands.items())}
    checks = {f"{name} is refused without pins": refused(error, "provenance") for name, error in errors.items()}
    checks["no container was created for any of them"] = not any(creates(env, f"-x{index}-") for index in range(len(commands)))
    body = {"template": "spark_read", "launch_id": "pin", "args": ["pin", "--run", PINS_RUN], "params": {"job": "pins"}, "checkout": str(ROOT)}
    env.wait_ended(env.client().call("submit", body, run)["attempt_id"], 30)
    env.wait(lambda: env.state()["runs"][PINS_RUN].get("pins_sha256"), 10, "the pins digest is recorded")
    accepted = expect_error(lambda: dbt_submit(env, run, "after", ["build", "--select", "int_hai_state_windows"]))
    checks["the same selected build is accepted once pins are recorded"] = accepted is None
    return checks


@case("writer_serialized", "patched")
def writer_serialized(env: Env) -> dict[str, bool]:
    """922: one launch of a run that may write the build file is open at a time, in submit order (``debug`` and ``compile`` included); ``ls`` and ``parse``
    are not held back."""
    plan = {"mode": "selected", "seconds": 0.5}
    run, fakes = prepare(env, builds=[{**plan, "seconds": 1.5}, plan, plan, plan, plan])
    first = dbt_submit(env, run, "m1", ["build", "--select", "a"])
    inert = dbt_submit(env, run, "m0", ["ls"])
    second = dbt_submit(env, run, "m2", ["run", "--select", "b"])
    third = dbt_submit(env, run, "m3", ["seed"])
    fourth = dbt_submit(env, run, "m4", ["debug"])
    waits: list[str] = []
    inert_waits: list[str] = []
    deadline = time.monotonic() + 20
    while time.monotonic() < deadline and env.attempt(first["attempt_id"])["phase"] != "ended":
        waits.append(str(env.attempt(second["attempt_id"]).get("waiting_on")))
        inert_waits.append(str(env.attempt(inert["attempt_id"]).get("waiting_on")))
        time.sleep(0.05)
    for attempt in (first, inert, second, third, fourth):
        env.wait_ended(attempt["attempt_id"], 40)
    names = ("m0", "m1", "m2", "m3", "m4")
    starts = {name: next((e["at"] for e in env.daemon.event_log if e["action"] == "start" and f"-{name}-" in e["name"]), None) for name in names}
    ends = {name: next((e["at"] for e in env.daemon.event_log if e["action"] == "die" and f"-{name}-" in e["name"]), None) for name in names}
    ordered = all(v is not None for v in (*starts.values(), *ends.values()))
    start = {name: float(value or 0.0) for name, value in starts.items()}
    end = {name: float(value or 0.0) for name, value in ends.items()}
    return {
        "every launch ran": ordered,
        "ls does not wait in the write queue": not any("may write the build file" in wait for wait in inert_waits),
        "the second waits for the first": ordered and start["m2"] >= end["m1"],
        "the third waits for the second": ordered and start["m3"] >= end["m2"],
        "debug waits for the third": ordered and start["m4"] >= end["m3"],
        "the wait is named in the registry": any("launch of the run that may write the build file" in wait for wait in waits),
        "no launch ended in error": all(env.attempt(a["attempt_id"])["reason"] == "exited" for a in (first, inert, second, third, fourth)),
    }


def queued(name: str, args: list[str], phase: str = "queued", owner: str = "run:r", template: str = "real_dbt") -> dict[str, Any]:
    """An attempt record with the fields the write queue reads; ``name`` orders the submits."""
    return {"id": name, "owner": owner, "template": template, "phase": phase, "args": args, "created_at": f"2026-10-14T00:00:0{name[-1]}Z"}


@case("write_queue_rules")
def write_queue_rules(env: Env) -> dict[str, bool]:
    """922: the write queue's decision on records of runs and attempts: one launch that may write the build file at a time, in submit order; a sealed run
    stops a mutating launch only; a seal in progress holds every launch."""
    module = provenance()
    run = {"run_id": "r", "build_state": "open"}
    first, second = queued("a1", ["build"]), queued("a2", ["run"])
    blocked = module.dispatch_blocker
    return {
        "a lone launch may start": blocked(run, first, [first]) is None,
        "a launch waits for an open earlier launch": has(blocked(run, second, [queued("a1", ["build"], "started"), second]), "is open"),
        "a launch waits for an earlier queued launch": has(blocked(run, second, [first, second]), "is queued"),
        "the earlier queued launch may start": blocked(run, first, [first, second]) is None,
        "compile waits for an open build": has(blocked(run, queued("a2", ["compile"]), [queued("a1", ["build"], "started")]), "is open"),
        "an open ls does not hold a build": blocked(run, second, [queued("a1", ["ls"], "started"), second]) is None,
        "an ended launch does not hold": blocked(run, second, [queued("a1", ["build"], "ended"), second]) is None,
        "a launch of another run does not hold": blocked(run, second, [queued("a1", ["build"], "started", "run:other"), second]) is None,
        "a launch of another template does not hold": blocked(run, second, [queued("a1", ["build"], "started", "run:r", "sealed_consumer"), second]) is None,
        "a sealed run ends a mutating launch": blocked({**run, "build_state": "sealed"}, second, [second]) == "sealed",
        "a sealed run admits debug": blocked({**run, "build_state": "sealed"}, queued("a2", ["debug"]), [queued("a2", ["debug"])]) is None,
        "a seal in progress holds every launch": has(
            blocked({**run, "build_state": "sealing"}, queued("a2", ["debug"]), [queued("a2", ["debug"])]), "in progress"
        ),
    }


@case("seal_after_build", "patched")
def seal_after_build(env: Env) -> dict[str, bool]:
    """923: a full successful build is sealed as a read-only clone with its SHA-256, inode and device in the completion; a queued mutating launch ends."""
    run, fakes = prepare(env, builds=[{"mode": "success", "seconds": 0.8}, {"mode": "selected", "seconds": 0.3}])
    first = dbt_submit(env, run, "full", ["build"])
    queued = dbt_submit(env, run, "later", ["build", "--select", "x"])
    env.wait(lambda: run_record(env).get("build_state") == "open", 20, "the build state is open while the build runs")
    env.wait_ended(first["attempt_id"], 30)
    entry = dict(env.wait(lambda: (r := run_record(env)).get("build_state") == "sealed" and r, 30, "the run is sealed"))
    completion = entry["completion"]
    build = run_folder(env) / "staging.duckdb"
    sealed_file = run_folder(env) / "sealed" / f"{completion['sha256']}.duckdb"
    info = sealed_file.stat()
    ended = env.wait_ended(queued["attempt_id"], 20)
    return {
        "the completion records the file's SHA-256": completion["sha256"] == hashlib.sha256(sealed_file.read_bytes()).hexdigest(),
        "the sealed copy has the build file's bytes": sealed_file.read_bytes() == build.read_bytes(),
        "the completion records inode and device": completion["inode"] == info.st_ino and completion["device"] == info.st_dev,
        "the sealed copy is another inode than the build file": info.st_ino != build.stat().st_ino,
        "the copy is read-only": stat.S_IMODE(info.st_mode) & 0o222 == 0,
        "the sealed folder is read-only": stat.S_IMODE(sealed_file.parent.stat().st_mode) & 0o222 == 0,
        "the completion names the invocation of the build": completion["invocation_id"] == fakes.started[0]["invocation"],
        "the completion names the provenance values": completion["run_id"] == PINS_RUN and completion["bronze_pins_sha256"] == entry["pins_sha256"],
        "the queued mutating launch ended without a container": ended["reason"] == "cancelled" and not creates(env, "-later-"),
        "no write-ahead log or temporary clone is left": sorted(p.name for p in sealed_file.parent.iterdir()) == [sealed_file.name],
    }


@case("sealed_refusals", "patched")
def sealed_refusals(env: Env) -> dict[str, bool]:
    """923: a sealed run refuses every mutating launch, also after a launcher restart; non-mutating launches and the consumer are admitted."""
    run, fakes, entry = sealed_run(env, builds=[{"mode": "success", "seconds": 0.3}, {"mode": "selected", "seconds": 0.2}] * 4)
    mutating = {name: expect_error(partial(dbt_submit, env, run, f"r{name}", [name])) for name in provenance().HOOKED}
    checks = {f"{name} is refused on a sealed run": refused(error, "refused", "sealed") for name, error in mutating.items()}
    admitted = {"compile": ["compile"], "compile with a selection": ["compile", "--select", "x"], "debug": ["debug"], "ls": ["ls"], "parse": ["parse"]}
    replies: list[dict[str, Any]] = []
    for index, (label, arguments) in enumerate(admitted.items()):
        error = expect_error(partial(lambda launch, args: replies.append(dbt_submit(env, run, launch, args)), f"a{index}", arguments))
        checks[f"{label} is admitted"] = error is None
    for reply in replies:
        env.wait_ended(reply["attempt_id"], 30)
    now = run_record(env)
    checks["the admitted launches leave the run sealed with the same completion"] = (
        now.get("build_state") == "sealed" and now.get("completion") == entry["completion"]
    )
    env.kill_launcher()
    env.start()
    after = run_record(env)
    checks["the sealed state and completion survive a launcher restart"] = (
        after.get("build_state") == "sealed" and after.get("completion") == entry["completion"]
    )
    checks["a mutating launch is still refused after the restart"] = refused(
        expect_error(lambda: dbt_submit(env, run, "again", ["build"])), "refused", "sealed"
    )
    return checks


@case("seal_not_written", "patched")
def seal_not_written(env: Env) -> dict[str, bool]:
    """922 and 923 (item 7): a selected, failed, warned, mismatched, write-ahead-logged or fileless build writes no completion; a later clean build seals."""
    steps = [
        ("selected build", ["build", "--select", "x"], {"mode": "success"}, None),
        ("failed build", ["build"], {"mode": "failed"}, None),
        ("warning", ["build"], {"mode": "warn"}, "not success or pass"),
        ("a warning test", ["build"], {"mode": "release_copy_warning"}, "not success or pass"),
        ("manifest of another invocation", ["build"], {"mode": "manifest"}, "different invocations"),
        ("write-ahead log", ["build"], {"mode": "wal"}, "write-ahead log"),
        ("missing build file", ["build"], {"mode": "nodb"}, "the build file of run"),
    ]
    run, fakes = prepare(env, builds=[{**plan, "seconds": 0.3} for _, _, plan, _ in steps] + [{"mode": "success", "seconds": 0.3}])
    checks: dict[str, bool] = {}
    for index, (label, args, _, text) in enumerate(steps):
        reply = dbt_submit(env, run, f"s{index}", args)
        env.wait_ended(reply["attempt_id"], 30)
        entry = dict(env.wait(lambda: (r := run_record(env)).get("build_state") in ("open", "sealed") and r, 20, f"the build state settles after the {label}"))
        checks[f"{label}: no completion and the run stays open"] = entry.get("build_state") == "open" and not entry.get("completion")
        checks[f"{label}: no sealed copy"] = not (run_folder(env) / "sealed").exists() or not any((run_folder(env) / "sealed").iterdir())
        if text:
            checks[f"{label}: the reason is kept"] = text in str(entry.get("seal_error", ""))
        with contextlib.suppress(OSError):
            (run_folder(env) / "staging.duckdb.wal").unlink()
    reply = dbt_submit(env, run, "clean", ["build"])
    env.wait_ended(reply["attempt_id"], 30)
    sealed = dict(env.wait(lambda: (r := run_record(env)).get("build_state") == "sealed" and r, 20, "a later clean build seals"))
    checks["a later build of successes and passes seals and names its attempt"] = sealed["completion"]["build_attempt"] == reply["attempt_id"]
    checks["the earlier failure left no error behind"] = "seal_error" not in sealed
    return checks


@case("seal_direct", "unpatched")
def seal_direct(env: Env) -> dict[str, bool]:
    """923: the interface body seals one gold-eligible build, returns the same record for the same bytes and refuses when the inputs changed."""
    run, fakes = prepare(env, builds=[{"mode": "success", "seconds": 0.3}])
    reply = dbt_submit(env, run, "full", ["build"])
    env.wait_ended(reply["attempt_id"], 30)
    entry = bound(env, run_record(env), reply["attempt_id"])
    first = launcher.seal(entry)
    sealed_folder = run_folder(env) / "sealed"
    with writable(sealed_folder):
        (sealed_folder / ".0123456789abcdef0123456789abcdef.clone").write_bytes(b"interrupted")
    second = launcher.seal(entry)
    build = run_folder(env) / "staging.duckdb"
    sealed_file = run_folder(env) / "sealed" / f"{first['sha256']}.duckdb"
    pins = run_folder(env) / "pins.json"
    original = pins.read_bytes()
    pins.write_text(json.dumps({"x": "1"}))
    changed = expect_error_of(lambda: launcher.seal(entry))
    pins.write_bytes(original)
    return {
        "the copy hashes to the recorded digest": hashlib.sha256(sealed_file.read_bytes()).hexdigest() == first["sha256"],
        "the copy has the build file's bytes and another inode": sealed_file.read_bytes() == build.read_bytes()
        and sealed_file.stat().st_ino != build.stat().st_ino,
        "the record has the inode and the device": first["inode"] == sealed_file.stat().st_ino and first["device"] == sealed_file.stat().st_dev,
        "the copy and its folder are read-only": stat.S_IMODE(sealed_file.stat().st_mode) & 0o222 == 0
        and stat.S_IMODE(sealed_file.parent.stat().st_mode) & 0o222 == 0,
        "sealing the same bytes again gives the same record": {k: v for k, v in first.items() if k != "sealed_utc"}
        == {k: v for k, v in second.items() if k != "sealed_utc"},
        "one sealed copy exists and the interrupted temporary copy is gone": [p.name for p in sealed_file.parent.iterdir()] == [sealed_file.name],
        "changed pins refuse the seal with the code provenance": isinstance(changed, launcher.LauncherError) and changed.code == "provenance",
    }


def bound(env: Env, entry: dict[str, Any], attempt_id: str) -> dict[str, Any]:
    """The run entry as the launcher records it when a mutating launch ends: the attempt that wrote the build, with its generation and arguments."""
    attempt = env.attempt(attempt_id)
    binding = {"attempt": attempt_id, "generation": attempt["generation"], "args": attempt["args"]}
    return {**entry, "build_attempt": attempt_id, "build_binding": binding}


def results_invocation(env: Env) -> str:
    """The invocation ID in the run's ``run_results.json``, or an empty string when the file is absent."""
    path = run_folder(env) / "target" / "run_results.json"
    return str(json.loads(path.read_text())["metadata"]["invocation_id"]) if path.is_file() else ""


@case("warning_not_tolerated", "unpatched")
def warning_not_tolerated(env: Env) -> dict[str, bool]:
    """Require every build result to be success or pass and refuse a seal when any test warns."""
    run, fakes = prepare(env, builds=[{"mode": "release_copy_warning", "seconds": 0.3}, {"mode": "warn", "seconds": 0.3}, {"mode": "success", "seconds": 0.3}])
    checks: dict[str, bool] = {}
    sealed_folder = run_folder(env) / "sealed"
    for index, label in enumerate(("a warning test", "another warning")):
        reply = dbt_submit(env, run, f"w{index}", ["build"])
        env.wait_ended(reply["attempt_id"], 30)
        error = expect_error_of(partial(lambda attempt: launcher.seal(bound(env, run_record(env), attempt)), reply["attempt_id"]))
        checks[f"{label}: the seal is refused with the code provenance"] = isinstance(error, launcher.LauncherError) and error.code == "provenance"
        checks[f"{label}: the reason is a result that is not success or pass"] = error is not None and "not success or pass" in str(error)
        checks[f"{label}: no sealed copy was made"] = not sealed_folder.exists() or not any(sealed_folder.iterdir())
    reply = dbt_submit(env, run, "clean", ["build"])
    env.wait_ended(reply["attempt_id"], 30)
    record = launcher.seal(bound(env, run_record(env), reply["attempt_id"]))
    checks["control: a build of successes and passes seals"] = record["invocation_id"] == fakes.started[-1]["invocation"]
    return checks


def stale_artifacts(env: Env, second: list[str]) -> dict[str, bool]:
    """A full build whose seal failed (a write-ahead log) leaves its artifacts; the next launch is ``second``. Review round 1 finding 2."""
    plan: dict[str, Any] = {"seconds": 0.3}
    run, fakes = prepare(env, builds=[{**plan, "mode": "wal"}, {**plan, "mode": "success"}])
    first = dbt_submit(env, run, "w1", ["build"])
    env.wait_ended(first["attempt_id"], 30)
    opened = dict(env.wait(lambda: (r := run_record(env)).get("build_state") == "open" and r.get("seal_error") and r, 20, "the failed seal reopens the run"))
    stale = results_invocation(env)
    (run_folder(env) / "staging.duckdb.wal").unlink()
    reply = dbt_submit(env, run, "next", second)
    env.wait_ended(reply["attempt_id"], 30)
    deadline = time.monotonic() + 8
    while time.monotonic() < deadline and run_record(env).get("build_state") != "sealed":
        time.sleep(0.2)
    entry = run_record(env)
    full = second == ["build"]
    completion = entry.get("completion") or {}
    return {
        "the first build did not seal for its write-ahead log": "write-ahead log" in str(opened.get("seal_error")) and bool(stale),
        "the second launch ended normally": env.attempt(reply["attempt_id"])["reason"] == "exited",
        ("a full build of its own seals" if full else f"{second[0]} does not seal"): (entry.get("build_state") == "sealed") is full,
        ("the completion names the second build's invocation" if full else "no completion names the first build's invocation"): (
            completion.get("invocation_id") == fakes.started[-1]["invocation"] if full else completion.get("invocation_id") != stale and not completion
        ),
        "the first build's invocation is not the sealed one": completion.get("invocation_id") != stale,
    }


@case("stale_artifacts_control", "patched")
def stale_artifacts_control(env: Env) -> dict[str, bool]:
    """923 control for finding 2: after the failed seal, a second full build seals and the completion names that build."""
    return stale_artifacts(env, ["build"])


@case("stale_artifacts_debug", "patched")
def stale_artifacts_debug(env: Env) -> dict[str, bool]:
    """923 (review round 1 finding 2): after the failed seal, a successful ``debug`` writes no build artifacts and must not seal the older build."""
    return stale_artifacts(env, ["debug"])


@case("stale_artifacts_test", "patched")
def stale_artifacts_test(env: Env) -> dict[str, bool]:
    """923 (review round 1 finding 2): a successful ``test`` is a task that writes its own artifacts; it is not a build and must not seal."""
    return stale_artifacts(env, ["test"])


@case("seal_invocation_mismatch", "unpatched")
def seal_invocation_mismatch(env: Env) -> dict[str, bool]:
    """923 (L1 round 1 finding R5): a build whose results and manifest name different invocations is not sealed; a later build of one invocation is."""
    run, fakes = prepare(env, builds=[{"mode": "manifest", "seconds": 0.3}, {"mode": "success", "seconds": 0.3}])
    mismatched = dbt_submit(env, run, "m1", ["build"])
    env.wait_ended(mismatched["attempt_id"], 30)
    error = expect_error_of(lambda: launcher.seal(bound(env, run_record(env), mismatched["attempt_id"])))
    clean = dbt_submit(env, run, "m2", ["build"])
    env.wait_ended(clean["attempt_id"], 30)
    record = launcher.seal(bound(env, run_record(env), clean["attempt_id"]))
    return {
        "a manifest of another invocation: the seal is refused with the code provenance": isinstance(error, launcher.LauncherError)
        and error.code == "provenance",
        "a manifest of another invocation: the reason names the two invocations": error is not None and "different invocations" in str(error),
        "control: a build of one invocation seals": record["invocation_id"] == fakes.started[-1]["invocation"],
    }


@case("seal_binding", "unpatched")
def seal_binding(env: Env) -> dict[str, bool]:
    """923 (finding 2): the interface seals only the attempt it is told wrote the build: a full build of the run's current generation."""
    run, fakes = prepare(env, builds=[{"mode": "success", "seconds": 0.3}])
    reply = dbt_submit(env, run, "full", ["build"])
    env.wait_ended(reply["attempt_id"], 30)
    entry = run_record(env)
    good = bound(env, entry, reply["attempt_id"])

    def refusal(mapping: dict[str, Any]) -> BaseException | None:
        return expect_error_of(lambda: launcher.seal(mapping))

    not_build = {**good, "build_binding": {**good["build_binding"], "args": ["debug"]}}
    selected = {**good, "build_binding": {**good["build_binding"], "args": ["build", "--select", "x"]}}
    old_generation = {**good, "build_binding": {**good["build_binding"], "generation": good["build_binding"]["generation"] + 1}}
    other_attempt = {**good, "build_attempt": "at_other"}
    unbound = {k: v for k, v in good.items() if k not in ("build_attempt", "build_binding")}
    record = launcher.seal(good)
    module = provenance()
    same_binding = hasattr(module, "binding") and module.binding(env.attempt(reply["attempt_id"])) == good["build_binding"]
    problems = {
        "a debug launch": refusal(not_build),
        "a selected build": refusal(selected),
        "a build of another generation": refusal(old_generation),
        "a binding that names another attempt": refusal(other_attempt),
        "an entry without a binding": refusal(unbound),
    }
    checks = {
        f"{label}: refused with the code provenance": isinstance(error, launcher.LauncherError) and error.code == "provenance"
        for label, error in problems.items()
    }
    checks["the launcher's binding of an attempt is the one the seal reads"] = same_binding
    checks["control: the full build of the current generation seals"] = record["invocation_id"] == fakes.started[0]["invocation"]
    checks["control: the completion names the attempt and its generation"] = (
        record.get("build_attempt") == reply["attempt_id"] and record.get("build_generation") == good["build_binding"]["generation"]
    )
    return checks


@case("snapshot_link_swap")
def snapshot_link_swap(env: Env) -> dict[str, bool]:
    """921 and 920 (D1 section 10 item 2, review round 1 finding 4): the snapshot is read through held directory descriptors, so a folder replaced by a link
    after the walk is not followed."""
    run, fakes = prepare(env)
    module = provenance()
    entry = run_record(env)
    folder = Path(env.state()["copies"][entry["snapshot"]]["folder"])
    project = folder / "project"
    control = module.variables(entry)
    files = [path for path in sorted(project.rglob("*")) if path.is_file() and path.parent != project]
    names = [path.name for path in sorted(folder.rglob("*")) if path.is_file()]
    nested = next(path for path in files if names.count(path.name) == 1)
    directory = project / nested.relative_to(project).parts[0]
    moved = project / f"{directory.name}.moved"
    swapped: list[str] = []

    def swap(call: str, name: str) -> None:
        if not swapped and os.path.basename(name) == nested.name:
            swapped.append(name)
            with writable(project):
                directory.rename(moved)
                directory.symlink_to(moved.name)

    prefixes = (str(folder), str(folder.resolve()))
    outcome: dict[str, str] | None = None
    refusal = None
    try:
        with traced_paths(*prefixes, before=swap) as calls:
            try:
                outcome = module.variables(entry)
            except provenance().ProvenanceError as error:
                refusal = str(error)
    finally:
        if directory.is_symlink():
            with writable(project):
                directory.unlink()
                moved.rename(directory)
    named = [text for _, text in calls]
    checks = {
        "control: the recorded run is accepted": len(control) == 6,
        "the folder was replaced by a link while the snapshot was read": bool(swapped),
        "every component of the snapshot was opened relative to a held descriptor": not named,
        "the values are the control's or the read refused": refusal is not None or outcome == control,
        "the restored snapshot is accepted again": raises(lambda: module.variables(entry)) is None,
    }
    with writable(project):
        directory.rename(moved)
        directory.symlink_to(moved.name)
    try:
        checks["a folder that is already a link refuses"] = has(raises(lambda: module.variables(entry)), "link")
    finally:
        with writable(project):
            directory.unlink()
            moved.rename(directory)
    return checks


@case("seal_descriptor_traversal", "unpatched")
def seal_descriptor_traversal(env: Env) -> dict[str, bool]:
    """923 (D1 section 10 items 2 and 5, finding 4): sealing reads and writes the run folder only through held directory descriptors; the clone is the
    checked build file."""
    run, fakes = prepare(env, builds=[{"mode": "success", "seconds": 0.3}])
    reply = dbt_submit(env, run, "full", ["build"])
    env.wait_ended(reply["attempt_id"], 30)
    entry = bound(env, run_record(env), reply["attempt_id"])
    folder = run_folder(env)
    with traced_paths(str(folder), str(folder.resolve())) as calls:
        record = launcher.seal(entry)
    sealed_file = folder / "sealed" / f"{record['sha256']}.duckdb"
    checks = {
        "control: the seal makes the copy of the build file": sealed_file.read_bytes() == (folder / "staging.duckdb").read_bytes(),
        "no path under the run folder was used by name": not calls,
    }
    aside = folder.with_name(folder.name + ".aside")
    with writable(folder / "sealed"):
        shutil.rmtree(folder / "sealed", ignore_errors=True)
    (folder / "sealed").mkdir()
    (folder / "sealed").rename(aside)
    (folder / "sealed").symlink_to(aside)
    try:
        checks["a link in place of the sealed folder refuses"] = isinstance(expect_error_of(lambda: launcher.seal(entry)), launcher.LauncherError)
    finally:
        (folder / "sealed").unlink()
        aside.rename(folder / "sealed")
    return checks


@case("artifacts_cleared", "unpatched")
def artifacts_cleared(env: Env) -> dict[str, bool]:
    """923 (finding 2): before a mutating launch the results of every earlier launch are removed, through held descriptors and without following a link."""
    run, fakes = prepare(env, builds=[{"mode": "success", "seconds": 0.3}])
    reply = dbt_submit(env, run, "full", ["build"])
    env.wait_ended(reply["attempt_id"], 30)
    entry = run_record(env)
    module = provenance()
    folder = run_folder(env)
    target = folder / "target"
    checks = {"control: the build left both results": all((target / name).is_file() for name in module.ARTIFACTS)}
    with traced_paths(str(folder), str(folder.resolve())) as calls:
        module.clear_build_artifacts(entry)
    checks["both results are removed"] = not any((target / name).exists() for name in module.ARTIFACTS)
    checks["no path under the run folder was used by name"] = not calls
    checks["clearing again is harmless"] = raises(lambda: module.clear_build_artifacts(entry)) is None
    (target / "manifest.json").write_text("{}")
    other = folder / "target.moved"
    target.rename(other)
    target.symlink_to(other)
    try:
        checks["a link in place of the target folder refuses"] = has(raises(lambda: module.clear_build_artifacts(entry)), "without following links")
        checks["the file behind the link was not removed"] = (other / "manifest.json").is_file()
    finally:
        target.unlink()
        other.rename(target)
    return checks


@contextlib.contextmanager
def seal_held(env: Env) -> Iterator[Path]:
    """Hold the seal worker of the test registry's launcher (the registry's ``seal_hold`` file) until the block ends or the file is removed."""
    hold = env.registry / "seal_hold"
    hold.write_text("held\n")
    try:
        yield hold
    finally:
        hold.unlink(missing_ok=True)


def held_seal(env: Env, run: dict[str, Any], launch: str = "full") -> dict[str, Any]:
    """Submit a full build, wait until it ended and the run is ``sealing`` with the worker held; returns the run entry."""
    reply = dbt_submit(env, run, launch, ["build"])
    env.wait_ended(reply["attempt_id"], 30)
    return dict(env.wait(lambda: (r := run_record(env)).get("build_state") == "sealing" and r, 20, "the run is sealing"))


def settled(env: Env, states: tuple[str, ...], what: str) -> dict[str, Any]:
    """The run entry once its state is one of ``states``."""
    return dict(env.wait(lambda: (r := run_record(env)).get("state") in states and r, 30, what))


def poll_run(env: Env, predicate: Callable[[dict[str, Any]], bool], seconds: float = 20.0) -> dict[str, Any]:
    """The run entry once ``predicate`` holds, or an empty entry after ``seconds``: a regression then fails a check of the case instead of ending it."""
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        entry = run_record(env)
        if predicate(entry):
            return entry
        time.sleep(0.1)
    return {}


@case("seal_resume", "patched")
def seal_resume(env: Env) -> dict[str, bool]:
    """923: a launcher that died while a run was ``sealing`` resumes the same seal on restart with the same record (the step is idempotent)."""
    run, fakes = prepare(env, builds=[{"mode": "success", "seconds": 0.3}])
    with seal_held(env):
        held = held_seal(env, run)
        env.kill_launcher()
    env.start()
    again = poll_run(env, lambda r: r.get("build_state") == "sealed", 30)
    sealed_dir = run_folder(env) / "sealed"
    direct = launcher.seal(bound(env, run_record(env), held["build_attempt"]))
    return {
        "control: the first launcher left the run sealing": held.get("build_state") == "sealing" and "completion" not in held,
        "the restarted launcher seals the run again": bool(again),
        "the record is the one a direct seal of the build gives": bool(again)
        and {k: v for k, v in again["completion"].items() if k != "sealed_utc"} == {k: v for k, v in direct.items() if k != "sealed_utc"},
        "one sealed copy exists": len(list(sealed_dir.iterdir())) == 1,
        "the seal operation is closed": bool(again) and not again.get("seal"),
    }


@case("seal_fence_control", "patched")
def seal_fence_control(env: Env) -> dict[str, bool]:
    """923 control for finding 8: a seal that is held and then released publishes the generation-matching completion; the run stays sealing meanwhile."""
    run, fakes = prepare(env, builds=[{"mode": "success", "seconds": 0.3}])
    with seal_held(env):
        held = held_seal(env, run)
        time.sleep(1.0)
        still = run_record(env)
    entry = poll_run(env, lambda r: r.get("build_state") == "sealed", 30)
    completion = entry.get("completion") or {}
    return {
        "the run is sealing while the worker is held": held["build_state"] == "sealing" and still["build_state"] == "sealing" and "completion" not in still,
        "the released seal publishes a completion of the run's generation": bool(completion)
        and completion["build_generation"] == int(entry.get("generation", 1))
        and completion["build_attempt"] == entry["build_attempt"],
        "no seal operation is left open": bool(entry) and not entry.get("seal"),
    }


@case("seal_fence_recovery", "patched")
def seal_fence_recovery(env: Env) -> dict[str, bool]:
    """923 (review delta 1 finding 8): a seal is owner work until its worker returns. Recovery and release during a seal cannot finish before the worker
    does, and the older seal publishes nothing: no completion exists after the run is recovered and released, and no reader can use one."""
    run, fakes = prepare(env, builds=[{"mode": "success", "seconds": 0.3}])
    client = env.client()
    owner = client.principal_run(PINS_RUN, owner=True)
    checks: dict[str, bool] = {}
    with seal_held(env):
        held = held_seal(env, run)
        checks["control: the worker is held and the run is sealing"] = held["build_state"] == "sealing" and "completion" not in held
        checks["the seal is an operation recorded with the run (attempt, generation, binding)"] = bool(held.get("seal")) and all(
            held["seal"].get(key) is not None for key in ("id", "attempt", "generation", "binding")
        )
        recovered = client.call("recover", {"owner": True}, owner)
        checks["control: recovery moved the run to generation 2"] = recovered.get("generation") == 2
        time.sleep(2.0)
        checks["recovery waits for the seal: the run is still recovering"] = run_record(env)["state"] == "recovering"
        checks["release is refused while the seal is open"] = refused(expect_error(lambda: client.call("release", {"owner": True}, owner)), "refused")
    env.wait(lambda: run_record(env).get("build_state") != "sealing", 30, "the older seal has decided")
    after = settled(env, ("failed_retained", "released"), "the recovery completes once the seal returned")
    checks["the older seal published no completion"] = "completion" not in after and after.get("build_state") != "sealed"
    checks["the discarded seal leaves its reason and no open operation"] = "discarded" in str(after.get("seal_error")) and not after.get("seal")
    released = expect_error(lambda: client.call("release", {"owner": True}, owner))
    final = run_record(env)
    checks["control: the owner releases the recovered run once the seal is closed"] = released is None and final["state"] == "released"
    checks["the released run holds no completion"] = "completion" not in final and final.get("build_state") != "sealed"
    session = AllocSession(env)
    reader = {"template": "duckdb_helper", "launch_id": "late", "args": [], "params": {"sealed_run": PINS_RUN, "sql": "select 1"}, "checkout": str(ROOT)}
    checks["a reader of the released run is refused with the code provenance"] = refused(
        expect_error(lambda: client.call("submit", reader, session.principal)), "provenance"
    )
    return checks


@case("seal_fence_restart", "patched")
def seal_fence_restart(env: Env) -> dict[str, bool]:
    """923 (finding 8, restart): a launcher that died after the recovery of a run that was sealing leaves no completion; the recovered run can be released."""
    run, fakes = prepare(env, builds=[{"mode": "success", "seconds": 0.3}])
    client = env.client()
    owner = client.principal_run(PINS_RUN, owner=True)
    with seal_held(env):
        held = held_seal(env, run)
        client.call("recover", {"owner": True}, owner)
        env.kill_launcher()
    env.start()
    env.wait(lambda: run_record(env).get("build_state") != "sealing", 30, "the seal of the first launcher is closed")
    after = settled(env, ("failed_retained", "released"), "the recovery completes after the restart")
    released = expect_error(lambda: env.client().call("release", {"owner": True}, env.client().principal_run(PINS_RUN, owner=True)))
    final = run_record(env)
    return {
        "control: the first launcher left the run sealing": held["build_state"] == "sealing",
        "no completion exists after the restart": "completion" not in after and after.get("build_state") != "sealed",
        "no seal operation is left open": not after.get("seal"),
        "the recovered run is released": released is None and final["state"] == "released" and "completion" not in final,
    }


def lock_free(env: Env) -> bool:
    """True when no process holds the registry's launcher lock: a non-blocking exclusive lock succeeds (and is released at once)."""
    with env.config.lock_path.open("a") as handle:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return False
    return True


def sealed_names(env: Env) -> list[str]:
    """The names in the run's sealed folder (empty when the folder does not exist)."""
    folder = run_folder(env) / "sealed"
    return sorted(path.name for path in folder.iterdir()) if folder.is_dir() else []


@case("seal_graceful_stop", "patched")
def seal_graceful_stop(env: Env) -> dict[str, bool]:
    """923 (review delta 2 finding 9): a graceful stop keeps the registry lock and the seal's open-work protection until its worker has stopped.

    A held seal is recorded, recovery advances the generation, the owner sends ``shutdown`` (the original process stays alive while its worker is held), a
    replacement launcher is started and the owner tries to finish the recovery and release. The run must not be released, and the seal record must not be
    dropped, while the original worker runs; once it has exited the replacement settles the recovery, the run is released and its sealed folder no longer
    changes. A shutdown reply is not proof that the worker ended: the process id is what the case waits for.
    """
    run, fakes = prepare(env, builds=[{"mode": "success", "seconds": 0.3}])
    client = env.client()
    owner = client.principal_run(PINS_RUN, owner=True)
    original = int(env.ping()["pid"])
    checks: dict[str, bool] = {}

    def release() -> BaseException | None:
        """The owner's release through a client that never starts a launcher."""
        return expect_error(lambda: env.client(start=False).call("release", {"owner": True}, env.client(start=False).principal_run(PINS_RUN, owner=True)))

    try:
        with seal_held(env):
            held = held_seal(env, run)
            seal_id = (held.get("seal") or {}).get("id")
            checks["control: a valid full build ended and its seal is recorded with the worker held"] = (
                held["build_state"] == "sealing" and bool(seal_id) and "completion" not in held
            )
            recovered = client.call("recover", {"owner": True}, owner)
            checks["control: recovery moved the run to generation 2"] = recovered.get("generation") == 2
            with contextlib.suppress(launcher.ClientError, OSError):
                client.call("shutdown", {"owner": True}, {"kind": "any", "owner": True})
            env.wait(lambda: not env.ping(), 15, "the launcher answers no more")
            checks["control: the original process is alive after its shutdown reply"] = env.alive(original)
            checks["the registry lock is still held while the seal worker runs"] = not lock_free(env)
            env.client().start_launcher()
            deadline = time.monotonic() + 8
            replacement = 0
            while time.monotonic() < deadline and not replacement:
                replacement = int(env.ping().get("pid") or 0) if env.ping().get("status") == "serving" else 0
                time.sleep(0.2)
            checks["no replacement launcher serves while the old worker runs"] = not replacement
            window = poll_run(env, lambda r: not r.get("seal") or r.get("state") in ("failed_retained", "released"), 6 if not replacement else 10)
            accepted = bool(window and poll_run(env, lambda r: r["state"] in ("failed_retained", "released"), 8) and release() is None)
            now = run_record(env)
            checks["the seal record and the open recovery survive while the old worker runs"] = (
                not window and (now.get("seal") or {}).get("id") == seal_id and now["state"] == "recovering"
            )
            checks["no release is accepted while the old worker runs"] = not (accepted and env.alive(original))
            at_release: list[str] | None = sealed_names(env)
        env.wait(lambda: not env.alive(original), 40, "the original process ends")
        checks["control: the original worker ended and the process exited"] = not env.alive(original)
        if not replacement:
            env.start()
        after = settled(env, ("failed_retained", "released"), "the recovery completes once the old worker has ended")
        if after["state"] != "released":
            at_release = None if release() is not None else sealed_names(env)
        final = run_record(env)
        checks["control: the replacement settles the recovery and the owner releases the run"] = final["state"] == "released" and at_release is not None
        checks["no completion exists and the seal is closed"] = "completion" not in final and not final.get("seal") and final.get("build_state") != "sealed"
        time.sleep(1.0)
        checks["the sealed folder does not change after the release"] = at_release is not None and sealed_names(env) == at_release
    finally:
        (env.registry / "seal_hold").unlink(missing_ok=True)
        with contextlib.suppress(CaseFailure):
            env.wait(lambda: not env.alive(original), 30, "the original launcher ends")
        if env.alive(original):
            os.kill(original, signal.SIGKILL)
    return checks


@case("seal_graceful_stop_publishes", "patched")
def seal_graceful_stop_publishes(env: Env) -> dict[str, bool]:
    """923 (review delta 2 finding 9, control for the fix): a seal that still applies when the launcher is stopped gracefully is published by that launcher
    before it exits. The owner stops the launcher with a held seal; the case then lets the worker go and reads the registry the moment the process has ended,
    before any replacement exists."""
    run, fakes = prepare(env, builds=[{"mode": "success", "seconds": 0.3}])
    original = int(env.ping()["pid"])
    checks: dict[str, bool] = {}
    try:
        with seal_held(env):
            held = held_seal(env, run)
            checks["control: a valid full build ended and its seal is recorded with the worker held"] = (
                held["build_state"] == "sealing" and "completion" not in held
            )
            with contextlib.suppress(launcher.ClientError, OSError):
                env.client().call("shutdown", {"owner": True}, {"kind": "any", "owner": True})
            env.wait(lambda: not env.ping(), 15, "the launcher answers no more")
            checks["control: the original process is alive after its shutdown reply"] = env.alive(original)
            checks["the registry lock is still held while the seal worker runs"] = not lock_free(env)
            checks["the run is still sealing while the worker runs"] = run_record(env)["build_state"] == "sealing"
        env.wait(lambda: not env.alive(original), 40, "the original process ends")
        ended = run_record(env)
        checks["the stopped launcher published the seal before it exited"] = ended["build_state"] == "sealed" and not ended.get("seal")
        checks["its completion is of the run's generation and attempt"] = (ended.get("completion") or {}).get("build_attempt") == held["build_attempt"] and (
            ended.get("completion") or {}
        ).get("build_generation") == int(held.get("generation", 1))
        checks["the lock is free once the process has exited"] = lock_free(env)
        env.start()
        again = run_record(env)
        checks["a replacement keeps the sealed run and its one sealed copy"] = (
            again["build_state"] == "sealed" and again.get("completion") == ended.get("completion") and len(sealed_names(env)) == 1
        )
    finally:
        (env.registry / "seal_hold").unlink(missing_ok=True)
        with contextlib.suppress(CaseFailure):
            env.wait(lambda: not env.alive(original), 30, "the original launcher ends")
        if env.alive(original):
            os.kill(original, signal.SIGKILL)
    return checks


@case("completion_of_live_build", "patched")
def completion_of_live_build(env: Env) -> dict[str, bool]:
    """924 (finding 8): a reader refuses a completion that is not the one of the run's live build: a sealed run that the owner recovered is generation 2, its
    completion is of generation 1."""
    run, fakes, entry = sealed_run(env)
    fakes.consumer["pins_json"] = (run_folder(env) / "pins.json").read_text()
    fakes.consumer["proceed"].set()
    session = AllocSession(env)
    module = provenance()
    completion = entry["completion"]
    control, reason = read_with(session, completion)
    client = env.client()
    client.call("recover", {"owner": True}, client.principal_run(PINS_RUN, owner=True))
    settled(env, ("failed_retained", "released"), "the sealed run is recovered")
    reader = {"template": "duckdb_helper", "launch_id": "late", "args": [], "params": {"sealed_run": PINS_RUN, "sql": "select 1"}, "checkout": str(ROOT)}
    refusal = expect_error(lambda: client.call("submit", reader, session.principal))
    owner = client.principal_run(PINS_RUN, owner=True)
    helper = raises(lambda: module.sealed_completion(client, owner, PINS_RUN))
    consumer = caught(lambda: module.run_consumer(client, owner, PINS_RUN, "late", ROOT))
    return {
        "control: before the recovery the reader returns its rows": control == [["1", "2"]] and reason is None,
        "after the recovery the launcher refuses a reader with the code provenance": refused(refusal, "provenance"),
        "after the recovery the helper refuses the completion": has(helper, "live build"),
        "after the recovery the consumer helper stops before it asks the launcher": isinstance(consumer, module.ProvenanceError)
        and "live build" in str(consumer),
    }


@case("release_copy_warning_refused", "patched")
def release_copy_warning_refused(env: Env) -> dict[str, bool]:
    """Refuse a seal for a warning test in the stub and seal a build without that warning.

    No launch disables the production physical-copy invariant.
    """
    run, fakes = prepare(env, builds=[{"mode": "release_copy_warning", "seconds": 0.3}, {"mode": "success", "seconds": 0.3}])
    warned = dbt_submit(env, run, "warned", ["build"])
    env.wait_ended(warned["attempt_id"], 30)
    first = poll_run(env, lambda r: r.get("build_state") in ("open", "sealed") and r.get("build_attempt") == warned["attempt_id"], 20)
    sealed_folder = run_folder(env) / "sealed"
    no_copy = not sealed_folder.exists() or not any(sealed_folder.iterdir())
    reply: dict[str, Any] = {}
    error = expect_error(lambda: reply.update(dbt_submit(env, run, "clean", ["build"])))
    if reply:
        env.wait_ended(reply["attempt_id"], 30)
    last = poll_run(env, lambda r: r.get("build_state") == "sealed" and (r.get("completion") or {}).get("build_attempt") == reply.get("attempt_id"), 20)
    texts = [json.dumps(body) for body in env.daemon.create_bodies.values()]
    return {
        "a build with a warning test is refused a seal": first.get("build_state") == "open"
        and "not success or pass" in str(first.get("seal_error"))
        and "completion" not in first,
        "the refused build leaves no sealed copy": no_copy,
        "the run takes a later launch": error is None and bool(reply),
        "a build without the warning is sealed": bool(last),
        "the sealed build is the clean one, not the warned one": bool(last) and last["completion"]["invocation_id"] == fakes.started[-1]["invocation"],
        "no launch turns the release copy-consistency test off": bool(texts) and not any("check_release_copy_conflicts" in text for text in texts),
    }


@case("seal_concurrent", "unpatched")
def seal_concurrent(env: Env) -> dict[str, bool]:
    """WV3-015: two seals of one run at the same time (the launcher's seal worker and a direct caller) do not break each other. The second waits for the
    first and returns the same sealed copy. The first seal pauses between its copy and its rename: there an unlocked second seal removed the first's
    temporary copy or made the sealed folder read-only, so the first failed with FileNotFoundError or PermissionError."""
    run, fakes = prepare(env, builds=[{"mode": "success", "seconds": 0.3}])
    reply = dbt_submit(env, run, "full", ["build"])
    env.wait_ended(reply["attempt_id"], 30)
    entry = bound(env, run_record(env), reply["attempt_id"])
    folder = run_folder(env)
    digest = hashlib.sha256((folder / "staging.duckdb").read_bytes()).hexdigest()
    module = provenance()
    copy_of = module.copy_into
    paused = threading.Event()
    results: dict[str, Any] = {}

    def slow_copy(source: int, sealed_folder: int, name: str) -> int:
        """Pause the first seal once, after its temporary copy exists and before its rename."""
        descriptor = copy_of(source, sealed_folder, name)
        if threading.current_thread().name == "seal-first" and not paused.is_set():
            paused.set()
            time.sleep(1.0)
        return descriptor

    def seal_as(name: str) -> None:
        try:
            results[name] = launcher.seal(entry)
        except Exception as error:
            results[name] = error

    with mock.patch.object(module, "copy_into", slow_copy):
        first = threading.Thread(target=seal_as, args=("first",), name="seal-first")
        first.start()
        paused.wait(30)
        second = threading.Thread(target=seal_as, args=("second",), name="seal-second")
        second.start()
        first.join(60)
        second.join(60)
    sealed = folder / "sealed"
    records = [results.get(name) for name in ("first", "second")]
    return {
        "both threads ended": not first.is_alive() and not second.is_alive(),
        "the first seal succeeded": isinstance(records[0], dict),
        "the second seal succeeded": isinstance(records[1], dict),
        "both name the build file's bytes": all(isinstance(record, dict) and record.get("sha256") == digest for record in records),
        "no temporary copy is left": not any(item.name.endswith(".clone") for item in sealed.iterdir()),
        "the sealed folder is read-only": stat.S_IMODE(sealed.stat().st_mode) == 0o555,
    }


@case("seal_clone_replaced_path", "unpatched")
def seal_clone_replaced_path(env: Env) -> dict[str, bool]:
    """923 (D1 section 10 items 2 and 5, review delta 1 finding 4): once the seal holds its descriptors, a run folder or a sealed folder that is replaced by a
    link to another folder is not followed: nothing reads or writes through the link, and the other folder stays as it was. The swap happens when the seal
    starts to copy (the seal's own ``uuid`` call names the temporary copy), which is where a native copy would resolve both paths."""
    run, fakes = prepare(env, builds=[{"mode": "success", "seconds": 0.3}])
    reply = dbt_submit(env, run, "full", ["build"])
    env.wait_ended(reply["attempt_id"], 30)
    entry = bound(env, run_record(env), reply["attempt_id"])
    folder = run_folder(env)
    module = provenance()
    original = (folder / "staging.duckdb").read_bytes()
    digest = hashlib.sha256(original).hexdigest()
    checks = {"control: a seal that nothing disturbs certifies the build file's bytes": launcher.seal(entry)["sha256"] == digest}
    decoy_bytes = b"DECOY" * 2048
    for label, victim in {"run folder": folder, "sealed folder": folder / "sealed"}.items():
        decoy = folder.with_name("decoy_" + label.split()[0])
        (decoy / "sealed").mkdir(parents=True)
        (decoy / "staging.duckdb").write_bytes(decoy_bytes)
        aside = victim.with_name(victim.name + ".aside")
        target = decoy if victim == folder else decoy / "sealed"
        swapped: list[bool] = []

        def at_copy(victim: Path = victim, aside: Path = aside, target: Path = target, swapped: list[bool] = swapped) -> uuid.UUID:
            """Replace the folder by a link to another folder, once, then give the uuid the seal asked for."""
            if not swapped:
                swapped.append(True)
                victim.rename(aside)
                victim.symlink_to(target)
            return uuid.uuid4()

        record: dict[str, Any] | None = None
        error: BaseException | None = None
        try:
            with mock.patch.object(module, "uuid", SimpleNamespace(uuid4=at_copy)):
                try:
                    record = launcher.seal(entry)
                except Exception as exc:
                    error = exc
        finally:
            if victim.is_symlink():
                victim.unlink()
            if aside.exists():
                aside.rename(victim)
        checks[f"{label} replaced by a link: the swap happened while the seal copied"] = bool(swapped)
        checks[f"{label} replaced by a link: the seal still certifies the build file's bytes"] = (
            error is None and record is not None and record["sha256"] == digest
        )
        checks[f"{label} replaced by a link: the other folder was neither read nor written"] = (
            not list((decoy / "sealed").iterdir()) and (decoy / "staging.duckdb").read_bytes() == decoy_bytes
        )
    source = Path(module.__file__).read_text()
    checks["the seal module starts no process (it names no process API)"] = not any(name in source for name in ("scripts.process", "subprocess", "run_command"))
    checks["the sealed copy in the run folder holds the original bytes"] = (folder / "sealed" / f"{digest}.duckdb").read_bytes() == original
    return checks


def consumer_run(env: Env, fakes: Fakes, run: dict[str, Any], launch: str) -> dict[str, Any]:
    """Submit a consumer of the sealed run."""
    body = {"template": "sealed_consumer", "launch_id": launch, "args": ["verify"], "params": {}, "checkout": str(ROOT)}
    return dict(env.client().call("submit", body, run))


def replace_file(path: Path, data: bytes) -> None:
    """Replace a read-only sealed file the way a same-user process could: a new inode at the same name."""
    folder = path.parent
    with writable(folder):
        temporary = folder / ".replacement"
        temporary.write_bytes(data)
        os.replace(temporary, path)


@case("consumer_digest", "patched")
def consumer_digest(env: Env) -> dict[str, bool]:
    """924: the in-container digest decides: replaced between create and start fails; restored before start passes; a later replacement changes nothing."""
    run, fakes, entry = sealed_run(env)
    module = provenance()
    completion = entry["completion"]
    sealed_file = run_folder(env) / "sealed" / f"{completion['sha256']}.duckdb"
    original = sealed_file.read_bytes()
    pins_text = (run_folder(env) / "pins.json").read_text()
    fakes.consumer["pins_json"] = pins_text
    checks: dict[str, bool] = {}

    def result_of(launch: str) -> dict[str, Any]:
        found = list((run_folder(env) / "consumer").glob(f"{launch}-a1.json"))
        return dict(json.loads(found[0].read_text())) if found else {}

    gate = env.daemon.add_gate("start", "before", lambda context: context["labels"].get("hai.template") == "sealed_consumer")
    fakes.consumer["proceed"].set()
    reply = consumer_run(env, fakes, run, "c1")
    gate.wait_reached()
    created = len(creates(env, "-c1-"))
    replace_file(sealed_file, original + b"x")
    gate.release()
    ended = env.wait_ended(reply["attempt_id"], 30)
    result = result_of("c1")
    checks["replaced before start: the container was created before the replacement"] = created == 1
    checks["replaced before start: the in-container digest differs"] = result.get("matched") is False and result.get("sha256") != completion["sha256"]
    checks["replaced before start: the result is rejected and the container fails"] = not module.accepts_result(result, completion) and ended["exit_code"] == 3
    replace_file(sealed_file, original)
    fakes.consumer["proceed"].set()
    gate = env.daemon.add_gate("start", "before", lambda context: context["labels"].get("hai.template") == "sealed_consumer")
    reply = consumer_run(env, fakes, run, "c2")
    gate.wait_reached()
    replace_file(sealed_file, original + b"y")
    replace_file(sealed_file, original)
    gate.release()
    ended = env.wait_ended(reply["attempt_id"], 30)
    result = result_of("c2")
    checks["restored before start: the digest matches and the result is accepted"] = module.accepts_result(result, completion) and ended["exit_code"] == 0
    fakes.consumer["proceed"].clear()
    fakes.consumer["attached"].clear()
    reply = consumer_run(env, fakes, run, "c3")
    fakes.consumer["attached"].wait(30)
    replace_file(sealed_file, original + b"z")
    fakes.consumer["proceed"].set()
    ended = env.wait_ended(reply["attempt_id"], 30)
    result = result_of("c3")
    checks["replaced after attachment: the mounted object is unchanged and accepted"] = module.accepts_result(result, completion) and ended["exit_code"] == 0
    replace_file(sealed_file, original)
    fakes.consumer["marker"] = "wrong"
    reply = consumer_run(env, fakes, run, "c4")
    fakes.consumer["proceed"].set()
    ended = env.wait_ended(reply["attempt_id"], 30)
    result = result_of("c4")
    checks["a marker of another run is rejected"] = (
        not module.accepts_result(result, completion) and ended["exit_code"] == 3 and result["marker"]["matched"] is False
    )
    body = env.daemon.create_bodies[next(c["name"] for c in creates(env, "-c2-"))]
    mounts = {m["Target"]: m for m in body["HostConfig"]["Mounts"]}
    checks["the specification mounts the sealed copy read-only and nothing of the build folder"] = (
        mounts["/workspace/sealed/build.duckdb"]["ReadOnly"] is True
        and mounts["/workspace/sealed/build.duckdb"]["Source"] == str(sealed_file)
        and not any(target.startswith("/workspace/build") for target in mounts)
    )
    checks["the specification has no network"] = body["HostConfig"]["NetworkMode"] == "none"
    checks["the entrypoint is the fixed consumer"] = body["Cmd"][:1] == ["consume"] and "--expected-sha256" in body["Cmd"]
    extra = {"template": "sealed_consumer", "launch_id": "c6", "args": ["verify", "--x"], "params": {}, "checkout": str(ROOT)}
    checks["the consumer arguments are closed"] = refused(expect_error(lambda: env.client().call("submit", extra, run)), "args")
    return checks


@case("sealed_completion_checked", "patched")
def sealed_completion_checked(env: Env) -> dict[str, bool]:
    """924: the host hands a caller the completion record of a sealed run and checks that the copy exists as a regular file of the recorded size, through
    held descriptors; it hands out no path of the copy (a reader attaches it behind the consumer entrypoint)."""
    run, fakes, entry = sealed_run(env)
    module = provenance()
    completion = entry["completion"]
    sealed_file = run_folder(env) / "sealed" / f"{completion['sha256']}.duckdb"
    original = sealed_file.read_bytes()
    checks = {"the sealed copy is present": raises(lambda: module.check_sealed_file(entry)) is None}
    with traced_paths(str(run_folder(env)), str(run_folder(env).resolve())) as calls:
        module.check_sealed_file(entry)
    checks["the check names no path under the run folder"] = not calls
    replace_file(sealed_file, original + b"x")
    checks["another size is refused"] = has(raises(lambda: module.check_sealed_file(entry)), "completion records")
    replace_file(sealed_file, original)
    moved = sealed_file.parent / "moved"
    with writable(sealed_file.parent):
        sealed_file.rename(moved)
        sealed_file.symlink_to(moved)
    checks["a link in place of the copy is refused"] = has(raises(lambda: module.check_sealed_file(entry)), "without following links")
    with writable(sealed_file.parent):
        sealed_file.unlink()
        moved.rename(sealed_file)
    checks["an unsealed run has no sealed copy"] = has(raises(lambda: module.check_sealed_file({**entry, "build_state": "open"})), "is not sealed")
    client = env.client()
    checks["the helper returns the completion record"] = module.sealed_completion(client, run, PINS_RUN) == completion
    variables = module.consumer_variables(entry)
    checks["the consumer variables carry the sealed digest and the expected marker"] = (
        variables["HAI_SEALED_SHA256"] == completion["sha256"] and json.loads(variables["HAI_SEALED_EXPECTED"])["run_id"] == PINS_RUN
    )
    other_generation = {**entry, "generation": int(entry.get("generation", 1)) + 1}
    checks["a completion of another generation than the run's is refused"] = has(raises(lambda: module.consumer_variables(other_generation)), "live build")
    other_attempt = {**entry, "build_attempt": "at_other"}
    checks["a completion of another attempt than the run's is refused"] = has(raises(lambda: module.consumer_variables(other_attempt)), "live build")
    checks["the module hands out no path of the sealed copy"] = not any(hasattr(module, name) for name in ("sealed_path", "verify_sealed_copy", "sealed_copy"))
    fakes.consumer["proceed"].set()
    fakes.consumer["pins_json"] = (run_folder(env) / "pins.json").read_text()
    checks["the consumer helper accepts a good result"] = module.run_consumer(client, run, PINS_RUN, "h1", ROOT) is True
    fakes.consumer["marker"] = "wrong"
    checks["the consumer helper rejects a marker of another run"] = module.run_consumer(client, run, PINS_RUN, "h2", ROOT) is False
    return checks


class SealingClient:
    """A stand-in for the launcher client whose run is ``sealing`` for a number of polls and then ``sealed`` or ``open``."""

    def __init__(self, polls: int, final: str) -> None:
        self.polls = polls
        self.final = final
        self.calls = 0

    def call(self, method: str, params: dict[str, Any], principal: dict[str, Any]) -> dict[str, Any]:
        """The status of the run."""
        self.calls += 1
        state = "sealing" if self.calls <= self.polls else self.final
        return {"run_id": params["id"], "build_state": state, "seal_error": "selection" if state == "open" else None}


@case("helpers_wait_for_seal")
def helpers_wait_for_seal(env: Env) -> dict[str, bool]:
    """923: a caller that asks right after a build waits while the run is ``sealing`` and stops with the reason when the seal failed."""
    module = provenance()
    waiting = SealingClient(3, "sealed")
    entry = module.settled_run(waiting, {}, PINS_RUN, 5.0, 0.01)
    failed = SealingClient(2, "open")
    reason = raises(lambda: module.sealed_completion(failed, {}, PINS_RUN))
    stuck = raises(lambda: module.settled_run(SealingClient(10**6, "sealed"), {}, PINS_RUN, 0.05, 0.01))
    ready = SealingClient(0, "sealed")
    settled = module.settled_run(ready, {}, PINS_RUN, 5.0, 0.01)
    return {
        "control: a run that is not sealing returns at once": ready.calls == 1 and settled["build_state"] == "sealed",
        "the helper polls until the seal has decided": waiting.calls == 4 and entry["build_state"] == "sealed",
        "a failed seal stops the caller with the reason": has(reason, "selection") and failed.calls == 3,
        "a seal that never decides stops the caller": has(stuck, "still sealing"),
    }


@case("consumer_unsealed", "patched")
def consumer_unsealed(env: Env) -> dict[str, bool]:
    """924: a run that is not sealed has no consumer."""
    run, fakes = prepare(env)
    error = expect_error(lambda: consumer_run(env, fakes, run, "c0"))
    return {
        "a consumer of an unsealed run is refused with the code provenance": refused(error, "provenance"),
        "no container was created": not creates(env, "-c0-"),
    }


class AllocSession:
    """The kit's session: a launcher client and the principal of an allocation, with the ``run`` method the readers call."""

    def __init__(self, env: Env) -> None:
        self.client = env.client()
        self.allocation = env.allocate()
        self.principal = env.alloc_principal(self.allocation)
        self.calls = 0

    def run(self, template: str, launch: str, args: list[str], params: dict[str, Any]) -> Any:
        """One attempt of a template, to its end."""
        self.calls += 1
        return self.client.run_attempt(self.principal, template, launch, args, params, checkout=ROOT)


def read_with(session: AllocSession, completion: dict[str, Any], sql: str = "select 1") -> tuple[list[list[str]] | None, str | None]:
    """The rows of one sealed reader, or the reason the caller was stopped."""
    try:
        return provenance().sealed_duckdb(session, PINS_RUN, completion, sql), None
    except provenance().ProvenanceError as error:
        return None, str(error)


@case("reader_behind_consumer", "patched")
def reader_behind_consumer(env: Env) -> dict[str, bool]:
    """924 (D1 section 10 item 6, review round 1 finding 1): a reader of the sealed copy runs behind the fixed entrypoint's check of the object that reader
    attached; its digest is in the result; a rejected result stops the caller."""
    run, fakes, entry = sealed_run(env)
    completion = entry["completion"]
    sealed_file = run_folder(env) / "sealed" / f"{completion['sha256']}.duckdb"
    original = sealed_file.read_bytes()
    fakes.consumer["pins_json"] = (run_folder(env) / "pins.json").read_text()
    session = AllocSession(env)
    checks: dict[str, bool] = {}
    fakes.consumer["proceed"].set()
    rows, reason = read_with(session, completion)
    checks["control: the reader's rows come back for the untouched copy"] = rows == [["1", "2"]] and reason is None
    first = creates(env, "-sealed-")
    body: dict[str, Any] = env.daemon.create_bodies[first[0]["name"]] if first else {"HostConfig": {"Mounts": []}, "Cmd": [], "Entrypoint": []}
    mounts = {m["Target"]: m for m in body["HostConfig"]["Mounts"]}
    sealed_mount = mounts.get("/workspace/sealed/build.duckdb", {})
    checks["the reader runs behind the entrypoint that checks first"] = body["Entrypoint"][-4:-2] == ["sh", "-c"] and "consume" in body["Entrypoint"][-2]
    checks["the digest and the expected marker are in the command, from the launcher's record"] = (
        body["Cmd"][:1] == ["/opt/hai/provenance.py"]
        and completion["sha256"] in body["Cmd"]
        and any(item.startswith('{"bronze_pins_sha256"') for item in body["Cmd"])
    )
    checks["the reader is duckdb on the sealed copy, read-only"] = body["Cmd"][4:7] == ["duckdb", "/workspace/sealed/build.duckdb", "-readonly"]
    checks["the specification mounts the sealed copy read-only and not the build folder"] = (
        sealed_mount.get("ReadOnly") is True
        and sealed_mount.get("Source") == str(sealed_file)
        and not any("staging" in str(m.get("Source")) or m["Target"] == "/workspace/in" for m in body["HostConfig"]["Mounts"])
    )
    checks["the specification has no network"] = body["HostConfig"]["NetworkMode"] == "none"
    gate = env.daemon.add_gate("start", "before", lambda context: context["labels"].get("hai.template") == "duckdb_helper")
    outcome: list[Any] = []
    thread = threading.Thread(target=lambda: outcome.append(read_with(session, completion)), name="reader-replaced")
    thread.start()
    gate.wait_reached()
    replace_file(sealed_file, original + b"x")
    gate.release()
    thread.join(60)
    checks["replaced before start: the caller is stopped and gets no rows"] = bool(outcome) and outcome[0][0] is None
    checks["replaced before start: the check of the attached object is what rejected it"] = bool(outcome) and has(outcome[0][1], "attached hashes")
    replace_file(sealed_file, original)
    gate = env.daemon.add_gate("start", "before", lambda context: context["labels"].get("hai.template") == "duckdb_helper")
    outcome = []
    thread = threading.Thread(target=lambda: outcome.append(read_with(session, completion)), name="reader-restored")
    thread.start()
    gate.wait_reached()
    replace_file(sealed_file, original + b"y")
    replace_file(sealed_file, original)
    gate.release()
    thread.join(60)
    checks["restored before start: the rows come back"] = bool(outcome) and outcome[0][0] == [["1", "2"]]
    fakes.consumer["proceed"].clear()
    fakes.consumer["attached"].clear()
    outcome = []
    thread = threading.Thread(target=lambda: outcome.append(read_with(session, completion)), name="reader-attached")
    thread.start()
    fakes.consumer["attached"].wait(30)
    replace_file(sealed_file, original + b"z")
    fakes.consumer["proceed"].set()
    thread.join(60)
    checks["replaced after attachment: the rows come back from the attached object"] = bool(outcome) and outcome[0][0] == [["1", "2"]]
    replace_file(sealed_file, original)
    fakes.consumer["marker"] = "wrong"
    before = session.calls
    stopped: list[str | None] = []
    for _ in range(3):
        rows, reason = read_with(session, completion)
        if rows is None:
            stopped.append(reason)
            break
    fakes.consumer["marker"] = "good"
    checks["a marker of another run is rejected and the caller stops at once"] = len(stopped) == 1 and session.calls == before + 1
    client = env.client()

    sent = [0]

    def submit(params: dict[str, Any], template: str = "duckdb_helper") -> BaseException | None:
        sent[0] += 1
        body = {"template": template, "launch_id": f"x{sent[0]}", "args": [], "params": params, "checkout": str(ROOT)}
        return expect_error(lambda: client.call("submit", body, session.principal))

    sql = {"sql": "select 1", "flags": ["-csv"]}
    checks["a database next to sealed_run is refused"] = refused(submit({**sql, "sealed_run": PINS_RUN, "database": "/workspace/in/x.duckdb"}), "args")
    checks["a digest supplied by the caller is refused"] = refused(submit({**sql, "sealed_run": PINS_RUN, "expected_sha256": "a" * 64}), "args")
    checks["a mount over the sealed copy is refused"] = refused(
        submit({**sql, "sealed_run": PINS_RUN, "mounts": [{"host": str(env.work), "container": "/workspace/sealed/x"}]}), "args"
    )
    checks["a run that does not exist is refused"] = refused(submit({**sql, "sealed_run": "nope"}), "provenance")
    checks["another template does not take sealed_run"] = refused(submit({"sealed_run": PINS_RUN}, "fixture_dbt"), "args")
    return checks


@case("quality_reader_behind_consumer", "patched")
def quality_reader_behind_consumer(env: Env) -> dict[str, bool]:
    """924 (finding 1): the validation of the sealed copy runs behind the same check in its own container, without the build folder."""
    run, fakes, entry = sealed_run(env)
    completion = entry["completion"]
    sealed_file = run_folder(env) / "sealed" / f"{completion['sha256']}.duckdb"
    original = sealed_file.read_bytes()
    fakes.consumer["pins_json"] = (run_folder(env) / "pins.json").read_text()
    session = AllocSession(env)
    module = provenance()
    contracts = ROOT / "data_contracts" / "great_expectations"
    out = env.work / "ge_out"
    out.mkdir()
    params = {"sealed_run": PINS_RUN, "suites_host": str(contracts / "suites"), "baselines_host": str(contracts / "baselines.json"), "out_host": str(out)}
    fakes.consumer["proceed"].set()

    def check_accepted(outcome: Any) -> bool:
        """True when the attempt printed a check that accepts the object it attached."""
        try:
            return bool(module.accepts_result(module.reader_result(str(outcome.stderr)), completion))
        except module.ProvenanceError:
            return False

    accepted = session.run("quality_validate", "q1", [], params)
    checks = {
        "control: the validation container ran and exited 0": accepted.ok,
        "the validation printed a check that the completion accepts": check_accepted(accepted),
    }
    first = creates(env, "-q1-")
    body: dict[str, Any] = env.daemon.create_bodies[first[0]["name"]] if first else {"HostConfig": {"Mounts": []}, "Cmd": [], "Entrypoint": []}
    targets = {m["Target"]: m for m in body["HostConfig"]["Mounts"]}
    checks["the validation runs behind the entrypoint that checks first"] = (
        body["Cmd"][:1] == ["/opt/hai/provenance.py"] and "consume" in body["Entrypoint"][-2]
    )
    checks["the validation reads the sealed copy and not the build folder"] = (
        "/workspace/build" not in targets
        and targets.get("/workspace/sealed/build.duckdb", {}).get("ReadOnly") is True
        and "/workspace/sealed/build.duckdb" in body["Cmd"]
    )
    gate = env.daemon.add_gate("start", "before", lambda context: context["labels"].get("hai.template") == "quality_validate")
    holder: list[Any] = []
    thread = threading.Thread(target=lambda: holder.append(session.run("quality_validate", "q2", [], params)), name="quality-replaced")
    thread.start()
    gate.wait_reached()
    replace_file(sealed_file, original + b"x")
    gate.release()
    thread.join(60)
    rejected = holder[0] if holder else None
    checks["replaced before start: the check is rejected and the validation does not run"] = (
        rejected is not None and not check_accepted(rejected) and rejected.exit_code == 3 and not rejected.stdout
    )
    replace_file(sealed_file, original)
    client = env.client()
    body_db = {"template": "quality_validate", "launch_id": "q3", "args": [], "params": {**params, "database": "/x/staging.duckdb"}, "checkout": str(ROOT)}
    checks["a database next to sealed_run is refused"] = refused(expect_error(lambda: client.call("submit", body_db, session.principal)), "args")
    return checks


class StubSession:
    """A kit session whose attempts end with a scripted result: what the gold runner's readers receive from the launcher."""

    def __init__(self, completion: dict[str, Any], accepted: bool, stdout: str = "1,2\n") -> None:
        module = provenance()
        checked = {
            "sha256": completion["sha256"] if accepted else "0" * 64,
            "expected_sha256": completion["sha256"],
            "matched": accepted,
            "size": completion["size"],
            "marker": {"checked": True, "matched": accepted, "different": []},
        }
        line = module.RESULT_PREFIX + json.dumps(checked) + "\n"
        self.outcome = SimpleNamespace(ok=True, exit_code=0, stdout=stdout, stderr=line, reason="exited")
        self.calls: list[dict[str, Any]] = []

    def run(self, template: str, launch: str, args: list[str], params: dict[str, Any]) -> Any:
        """The scripted outcome; the request is kept."""
        self.calls.append({"template": template, "params": params})
        return self.outcome


@case("gold_wiring", "proposals")
def gold_wiring(env: Env) -> dict[str, bool]:
    """924 (finding 1): the proposed gold and silver changes read the sealed copy only through readers behind the entrypoint and stop at the first
    rejected check (needs every proposed patch applied: ``--proposals``)."""
    gold: Any = importlib.import_module("scripts.lakehouse.run_gold_e2e")
    silver: Any = importlib.import_module("scripts.lakehouse.silver")

    module = provenance()
    completion: dict[str, Any] = {"sha256": "a" * 64, "size": 10, "file": "sealed/x.duckdb", "inode": 1, "device": 2, "invocation_id": "i"}
    checks: dict[str, bool] = {}
    good: Any = StubSession(completion, True)
    backend = gold.SealedBackend(Path(completion["file"]), None, good, PINS_RUN, completion)
    big = "x" * gold.gold_checks.CHUNK_CHARACTERS
    rows = backend.run([big + "1", big + "2"])
    checks["control: every query of an accepted reader returns its rows"] = rows == [["1", "2"], ["1", "2"]] and len(good.calls) == 2
    checks["each query names the sealed run and no database or path"] = all(
        call["template"] == "duckdb_helper" and call["params"].get("sealed_run") == PINS_RUN and "database" not in call["params"] for call in good.calls
    )
    bad: Any = StubSession(completion, False)
    rejected = gold.SealedBackend(Path(completion["file"]), None, bad, PINS_RUN, completion)
    reason = raises(lambda: rejected.run([big + "1", big + "2", big + "3"]))
    checks["a rejected check raises"] = has(reason, "attached hashes")
    checks["the second chunk was never sent"] = len(bad.calls) == 1

    originals = (gold.staging.launcher_client, module.sealed_completion, module.run_consumer)
    state: dict[str, bool] = {"accepted": False}

    class Client:
        @staticmethod
        def principal_run(run_id: str) -> dict[str, str]:
            return {"run": run_id}

    try:
        gold.staging.launcher_client = lambda: Client()
        module.sealed_completion = lambda client, principal, run_id: dict(completion)
        module.run_consumer = lambda client, principal, run_id, launch, checkout: state["accepted"]
        outcome: dict[str, Any] = {"checks": {}}
        stopped = caught(lambda: gold.seal_check("real", PINS_RUN, outcome))
        state["accepted"] = True
        outcome = {"checks": {}}
        returned = gold.seal_check("real", PINS_RUN, outcome)
    finally:
        gold.staging.launcher_client = originals[0]
        module.sealed_completion, module.run_consumer = originals[1], originals[2]
    checks["a rejected consumer stops the stage"] = isinstance(stopped, RuntimeError)
    checks["an accepted consumer returns the completion record, not a path"] = (
        returned == completion and outcome["checks"]["real_sealed_consumer_accepts"] is True
    )

    manifest = env.work / "manifest.json"
    manifest.write_text(json.dumps({"nodes": {}}))
    contracts = ROOT / "data_contracts" / "great_expectations"

    def validate(accepted: bool) -> tuple[int, Path] | BaseException:
        scripted = StubSession(completion, accepted)

        class Link:
            @staticmethod
            def call(method: str, params: dict[str, Any], principal: Any = None) -> dict[str, Any]:
                return {"open_templates": []} if params.get("what") == "launcher" else {"record": {}}

            @staticmethod
            def allocate() -> str:
                return "al"

            @staticmethod
            def principal_alloc(allocation: str) -> dict[str, str]:
                return {"alloc": allocation}

            @staticmethod
            def release_allocation(allocation: str) -> None:
                return None

            @staticmethod
            def run_attempt(principal: Any, template: str, launch: str, args: list[str], params: dict[str, Any], **kwargs: Any) -> Any:
                scripted.run(template, launch, args, params)
                Path(params["out_host"], "result.json").write_text(json.dumps({"counts": {"fail": 0}, "expectations": [1]}))
                return scripted.outcome

        saved = silver.launcher.load_client
        silver.launcher.load_client = lambda registry=None, start=True: Link()
        try:
            outcome_or_error = caught(
                lambda: silver.validate(None, contracts / "gold", contracts / "baselines.json", manifest, sealed_run=PINS_RUN, completion=completion)
            )
            return outcome_or_error if outcome_or_error is not None else (0, Path())
        finally:
            silver.launcher.load_client = saved

    accepted_error = validate(True)
    time.sleep(1.1)
    rejected_error = validate(False)
    checks["control: the validation of an accepted sealed copy returns its record"] = not isinstance(accepted_error, BaseException)
    checks["a validation whose check is rejected stops with an error"] = isinstance(rejected_error, silver.SilverError)
    return checks


DUCKDB_SCRIPT = """
import duckdb, json, sys
con = duckdb.connect(sys.argv[1])
rows = json.loads(sys.argv[2])
if rows is None:
    con.execute("create table another_table (x varchar)")
else:
    con.execute(
        "create table int_silver_build_marker (dbt_invocation_id varchar, git_revision varchar, run_id varchar,"
        " dbt_tree_sha256 varchar, bronze_pins_sha256 varchar, pins_json varchar)"
    )
    con.executemany("insert into int_silver_build_marker values (?, ?, ?, ?, ?, ?)", rows)
con.close()
"""


def duckdb_file(path: Path, rows: list[tuple[str, ...]] | None) -> None:
    """A real DuckDB file with the marker table and the rows (or, for ``None``, only another table), made with the DuckDB of the tools environment."""
    result = run_command(str(TOOLS_PYTHON), ["-B", "-I", "-c", DUCKDB_SCRIPT, str(path), json.dumps(rows)], cwd=ROOT)
    if result.returncode != 0:
        raise CaseFailure(f"the DuckDB fixture could not be written: {result.stderr[-300:]}")


@case("consumer_cli")
def consumer_cli(env: Env) -> dict[str, bool]:
    """924: the consumer entrypoint on a real DuckDB file: match (0), digest differs (3), marker differs (4); the result file carries the digest."""
    module = provenance()
    folder = env.work / "consumer_cli"
    folder.mkdir()
    pins_text = '{\n  "t": "1"\n}\n'
    pins_sha = hashlib.sha256(pins_text.encode()).hexdigest()
    expected = {"run_id": PINS_RUN, "dbt_invocation_id": "inv-1", "dbt_tree_sha256": "a" * 64, "bronze_pins_sha256": pins_sha, "git_revision": "b" * 40}

    def row(**override: str) -> tuple[str, ...]:
        values = {**expected, **override}
        return (values["dbt_invocation_id"], values["git_revision"], values["run_id"], values["dbt_tree_sha256"], values["bronze_pins_sha256"], pins_text)

    def run_consume(name: str, database: Path, sha: str | None = None, marker: dict[str, str] | None = expected) -> tuple[int, dict[str, Any]]:
        digest = sha or hashlib.sha256(database.read_bytes()).hexdigest()
        result = folder / f"{name}.json"
        arguments = ["-m", "scripts.lakehouse.provenance", "consume", "--sealed", str(database), "--expected-sha256", digest, "--result", str(result)]
        if marker is not None:
            arguments += ["--expected-marker", json.dumps(marker)]
        completed = run_command(str(TOOLS_PYTHON), arguments, cwd=ROOT, env={**os.environ, "PYTHONPATH": str(ROOT)})
        return completed.returncode, (json.loads(result.read_text()) if result.exists() else {})

    good = folder / "good.duckdb"
    duckdb_file(good, [row()])
    code, found = run_consume("good", good)
    wrong_run = folder / "wrong_run.duckdb"
    duckdb_file(wrong_run, [row(run_id="other-run")])
    wrong_code, wrong = run_consume("wrong_run", wrong_run)
    other_invocation = folder / "other_invocation.duckdb"
    duckdb_file(other_invocation, [row(dbt_invocation_id="inv-2")])
    other_code, other_found = run_consume("other_invocation", other_invocation)
    two = folder / "two.duckdb"
    duckdb_file(two, [row(), row()])
    two_code, two_found = run_consume("two", two)
    missing = folder / "missing.duckdb"
    duckdb_file(missing, None)
    missing_code, missing_found = run_consume("missing", missing)
    digest_code, digest_found = run_consume("digest", good, sha="0" * 64)
    tampered = folder / "tampered.duckdb"
    duckdb_file(tampered, [row(dbt_tree_sha256="c" * 64)])
    tampered_code, tampered_found = run_consume("tampered", tampered)
    digest_only_code, digest_only = run_consume("digest_only", good, marker=None)
    without_revision = {k: v for k, v in expected.items() if k != "git_revision"}
    unrecorded = folder / "unrecorded.duckdb"
    duckdb_file(unrecorded, [row(git_revision="unrecorded")])
    unrecorded_code, unrecorded_found = run_consume("unrecorded", unrecorded, marker=without_revision)
    moved_on_code, moved_on = run_consume("moved_on", good, marker=without_revision)
    again_code, _ = run_consume("good", good)
    printed = run_command(
        str(TOOLS_PYTHON),
        [
            "-m",
            "scripts.lakehouse.provenance",
            "consume",
            "--sealed",
            str(good),
            "--expected-sha256",
            hashlib.sha256(good.read_bytes()).hexdigest(),
            "--print-result",
            "--expected-marker",
            json.dumps(expected),
        ],
        cwd=ROOT,
        env={**os.environ, "PYTHONPATH": str(ROOT)},
    )
    no_output = run_command(
        str(TOOLS_PYTHON),
        ["-m", "scripts.lakehouse.provenance", "consume", "--sealed", str(good), "--expected-sha256", "0" * 64],
        cwd=ROOT,
        env={**os.environ, "PYTHONPATH": str(ROOT)},
    )
    completion = {"sha256": hashlib.sha256(good.read_bytes()).hexdigest(), "size": good.stat().st_size}
    return {
        "a matching file and marker exit 0": code == 0 and found["matched"] is True and found["marker"]["matched"] is True,
        "the result carries the digest computed in the container": found["sha256"] == completion["sha256"] and found["size"] == completion["size"],
        "the launcher accepts that result": module.accepts_result(found, completion),
        "a marker of another run exits 4": wrong_code == 4 and "run_id" in wrong["marker"]["different"] and not module.accepts_result(wrong, completion),
        "a marker of another invocation than the sealed build's exits 4": other_code == 4 and "dbt_invocation_id" in other_found["marker"]["different"],
        "a marker with two rows exits 4": two_code == 4 and two_found["marker"]["matched"] is False,
        "a build file without the marker table exits 4": missing_code == 4 and missing_found["marker"]["matched"] is False,
        "a tree hash that differs exits 4": tampered_code == 4 and "dbt_tree_sha256" in tampered_found["marker"]["different"],
        "a digest that differs exits 3 and the result keeps both digests": digest_code == 3
        and digest_found["sha256"] == completion["sha256"]
        and digest_found["expected_sha256"] == "0" * 64,
        "a digest-only check exits 0": digest_only_code == 0 and digest_only["marker"]["checked"] is False,
        "a marker whose Git revision is unrecorded exits 4": unrecorded_code == 4 and "git_revision" in unrecorded_found["marker"]["different"],
        "a real revision the consumer was not told about is accepted": moved_on_code == 0 and moved_on["marker"]["matched"] is True,
        "a second result for the same name is refused": again_code != 0,
        "the check is printed as the first line of standard error and the launcher accepts it": printed.returncode == 0
        and module.accepts_result(module.reader_result(printed.stderr), {**completion, "size": good.stat().st_size})
        and printed.stderr.startswith(module.RESULT_PREFIX),
        "a check with neither a result file nor a printed result is refused": no_output.returncode == 2,
    }


@case("consumer_result_accepted")
def consumer_result_accepted(env: Env) -> dict[str, bool]:
    """924: the launcher's acceptance rule: the result must carry the completion's digest, computed inside the container, with a matching marker."""
    module = provenance()
    completion = {"sha256": "a" * 64, "size": 10}
    good = {"matched": True, "sha256": "a" * 64, "expected_sha256": "a" * 64, "size": 10, "marker": {"checked": True, "matched": True}}
    return {
        "a result that carries the digest and a matching marker is accepted": module.accepts_result(good, completion),
        "a result with another digest is rejected": not module.accepts_result({**good, "sha256": "b" * 64}, completion),
        "a result that was told another digest is rejected": not module.accepts_result({**good, "expected_sha256": "b" * 64}, completion),
        "a result with another size is rejected": not module.accepts_result({**good, "size": 11}, completion),
        "a result with an unchecked marker is rejected": not module.accepts_result({**good, "marker": {"checked": False, "matched": None}}, completion),
        "a result with a marker that differs is rejected": not module.accepts_result({**good, "marker": {"checked": True, "matched": False}}, completion),
        "a result without the matched flag is rejected": not module.accepts_result({k: v for k, v in good.items() if k != "matched"}, completion),
        "an empty result is rejected": not module.accepts_result({}, completion),
    }


@case("no_token_exposure", "patched")
def no_token_exposure(env: Env) -> dict[str, bool]:
    """924 (D1 section 11 item 3): the run token appears in no container specification, registry record, log or file of the run folder."""
    run, fakes, entry = sealed_run(env)
    token = str(run["token"])
    fakes.consumer["proceed"].set()
    fakes.consumer["pins_json"] = (run_folder(env) / "pins.json").read_text()
    reply = consumer_run(env, fakes, run, "t1")
    env.wait_ended(reply["attempt_id"], 30)
    bodies = json.dumps(env.daemon.create_bodies)
    calls = json.dumps(env.daemon.calls)
    registry = (env.registry / "registry.json").read_text()
    log = (env.registry / "launcher.log").read_text() if (env.registry / "launcher.log").exists() else ""
    found = [str(p) for p in run_folder(env).rglob("*") if p.is_file() and token.encode() in p.read_bytes()]
    return {
        "the token is long enough to mean something": len(token) >= 32,
        "no container specification holds it": token not in bodies,
        "no daemon call holds it": token not in calls,
        "the registry does not hold it": token not in registry,
        "the launcher log does not hold it": token not in log,
        "no file of the run folder holds it": not found,
        "the variables of the real build carry no token name": not any(
            re.search("TOKEN|SECRET|PASSWORD", name, re.IGNORECASE) for name in fakes.started[0]["env"] if name.startswith("HAI_")
        ),
    }


class Dbt:
    """The pinned host dbt on a minimal copy of the project in a scratch folder: ``compile`` and ``run-operation`` against in-memory DuckDB."""

    VALID = {
        "HAI_BUILD_RUN_ID": PINS_RUN,
        "HAI_BUILD_DBT_TREE_SHA256": "a" * 64,
        "HAI_BUILD_PINS_SHA256": "b" * 64,
        "HAI_BUILD_PINS_JSON": '{\n  "cms_hai_hospital": "1",\n  "file_metadata": "2"\n}\n',
        "HAI_BUILD_GIT_REVISION": "c" * 40,
        "HAI_PROVENANCE_REQUIRED": "1",
    }

    def __init__(self, folder: Path, marker_model: bool = True) -> None:
        self.folder = folder
        project = folder / "project"
        for name in ("macros", "models", "seeds", "snapshots", "tests"):
            (project / name).mkdir(parents=True)
        for name in ("build_provenance.sql", "build_marker_hooks.sql"):
            shutil.copy2(ROOT / "dbt" / "macros" / name, project / "macros" / name)
        if marker_model:
            shutil.copy2(ROOT / "dbt" / "models/silver/intermediate/int_silver_build_marker.sql", project / "models" / "int_silver_build_marker.sql")
            for stub in ("int_county_adjacency_edges", "int_change_of_ownership_holds"):
                (project / "models" / f"{stub}.sql").write_text("select 1 as x\n")
        (project / "models" / "stub_a.sql").write_text("select 1 as x\n")
        (project / "seeds" / "seed_x.csv").write_text("x\n1\n")
        (project / "snapshots" / "snap_x.sql").write_text(
            "{% snapshot snap_x %}{{ config(target_schema='main', unique_key='x', strategy='check', check_cols='all') }}select 1 as x{% endsnapshot %}\n"
        )
        (project / "tests" / "stub_ok.sql").write_text("select 1 as x where false\n")
        (project / "dbt_project.yml").write_text(
            "name: hai\nversion: '1.0.0'\nconfig-version: 2\nprofile: hai\nmodel-paths: [models]\nmacro-paths: [macros]\n"
            "flags: {send_anonymous_usage_stats: false, use_colors: false}\n"
            'on-run-start: ["{{ hai_drop_build_marker() }}"]\non-run-end: ["{{ hai_finalize_build_marker(results) }}"]\n'
        )
        (project / "macros" / "scratch_tests.sql").write_text(
            "{% macro show_provenance() %}{{ log('VALUES=' ~ tojson(hai_build_provenance()), info=True) }}{% endmacro %}\n"
            "{% macro show_required() %}{{ log('REQUIRED=' ~ hai_provenance_required(), info=True) }}{% endmacro %}\n"
            "{% macro show_eligible(results, which, selection) %}"
            "{{ log('ELIGIBLE=' ~ hai_build_eligible(results, which, selection), info=True) }}{% endmacro %}\n"
            "{% macro marker_cycle(finalize) %}\n"
            "  {%- do run_query('create or replace table ' ~ adapter.quote(target.schema) ~ '.int_silver_build_marker as select 1 as x') -%}\n"
            "  {%- if finalize -%}{%- do hai_finalize_build_marker([{'status': 'success'}]) -%}{%- else -%}{%- do hai_drop_build_marker() -%}{%- endif -%}\n"
            "  {%- set rows = run_query(\"select count(*) from information_schema.tables where table_name = 'int_silver_build_marker'\") -%}\n"
            "  {{ log('MARKER_ROWS=' ~ rows.columns[0].values()[0], info=True) }}\n"
            "{% endmacro %}\n"
            "{% macro show_untouched() %}{{ log('UNTOUCHED=' ~ tojson(hai_marker_untouched_commands()), info=True) }}{% endmacro %}\n"
            "{% macro stub_state() %}\n"
            "  {%- set rows = run_query(\"select count(*) from information_schema.tables where table_name = 'stub_a'\") -%}\n"
            "  {{ log('STUB_TABLES=' ~ rows.columns[0].values()[0], info=True) }}\n"
            "{% endmacro %}\n"
            "{% macro marker_create() %}\n"
            "  {%- do run_query('create or replace table int_silver_build_marker as select 1 as x') -%}\n"
            "{% endmacro %}\n"
            "{% macro marker_state() %}\n"
            "  {%- set rows = run_query(\"select count(*) from information_schema.tables where table_name = 'int_silver_build_marker'\") -%}\n"
            "  {{ log('MARKER_TABLES=' ~ rows.columns[0].values()[0], info=True) }}\n"
            "  {%- if rows.columns[0].values()[0] | int > 0 -%}\n"
            "    {%- set marker = run_query('select * from int_silver_build_marker') -%}\n"
            "    {{ log('MARKER_ROWS=' ~ (marker.rows | length), info=True) }}\n"
            "    {{ log('MARKER_COLUMNS=' ~ (marker.column_names | join(',')), info=True) }}\n"
            "    {%- if marker.rows | length == 1 and 'run_id' in marker.column_names -%}\n"
            "      {{ log('MARKER_ROW=' ~ tojson(dict(zip(marker.column_names, marker.rows[0].values()))), info=True) }}\n"
            "    {%- endif -%}\n"
            "  {%- endif -%}\n"
            "{% endmacro %}\n"
        )
        (folder / "profiles.yml").write_text(
            "hai:\n  target: lint\n  outputs:\n"
            + "".join(f"    {name}:\n      type: duckdb\n      path: ':memory:'\n      threads: 1\n" for name in ("lint", "lakehouse", "fixture"))
            + f"    scratch:\n      type: duckdb\n      path: '{folder / 'build.duckdb'}'\n      threads: 1\n"
        )

    def run(self, arguments: list[str], environment: dict[str, str] | None = None, target: str | None = None) -> tuple[int, str]:
        """One dbt invocation; returns the exit code and the output."""
        env = {k: v for k, v in os.environ.items() if not k.startswith(("HAI_", "DBT_"))}
        env |= {"DBT_PROFILES_DIR": str(self.folder), "DBT_TARGET_PATH": str(self.folder / "target"), "DBT_LOG_PATH": str(self.folder / "logs")}
        env |= environment or {}
        command = [*arguments, "--project-dir", str(self.folder / "project"), *(["--target", target] if target else [])]
        completed = run_command(str(TOOLS_DBT), command, cwd=self.folder, env=env, timeout=600)
        return completed.returncode, completed.stdout + completed.stderr

    def operation(
        self, macro: str, args: dict[str, Any] | None = None, environment: dict[str, str] | None = None, target: str | None = None
    ) -> tuple[int, str]:
        """``dbt run-operation``."""
        arguments = ["run-operation", macro] + (["--args", json.dumps(args)] if args is not None else [])
        return self.run(arguments, environment, target)

    def values(self, environment: dict[str, str] | None = None, target: str | None = None) -> tuple[int, dict[str, str] | None, str]:
        """The values ``hai_build_provenance`` returns, or None when dbt refused."""
        code, output = self.operation("show_provenance", None, environment, target)
        found = re.search(r"VALUES=(\{.*\})", output)
        return code, (json.loads(found.group(1)) if found else None), output


@case("marker_macros")
def marker_macros(env: Env) -> dict[str, bool]:
    """920 and 922: host dbt: the provenance macro stops on a missing or unrecorded value when required; the marker compiles; the hooks drop it."""
    dbt = Dbt(env.work / "dbt_macros")
    valid = dict(Dbt.VALID)
    checks: dict[str, bool] = {}
    code, values, _ = dbt.values()
    checks["a lint build needs no values and reads unrecorded"] = code == 0 and values is not None and set(values.values()) == {"unrecorded"}
    code, values, _ = dbt.values(valid)
    checks["required with every value returns the values"] = (
        code == 0
        and values is not None
        and values["run_id"] == PINS_RUN
        and values["git_revision"] == "c" * 40
        and values["pins_json"] == valid["HAI_BUILD_PINS_JSON"]
    )
    code, values, _ = dbt.values({}, target="lakehouse")
    checks["the lakehouse target is required without the flag and stops"] = code != 0 and values is None
    for name in [n for n in valid if n != "HAI_PROVENANCE_REQUIRED"]:
        code, values, output = dbt.values({k: v for k, v in valid.items() if k != name})
        checks[f"{name} missing stops compilation"] = code != 0 and values is None and name in output
        code, values, output = dbt.values({**valid, name: "unrecorded"})
        checks[f"{name} unrecorded stops compilation"] = code != 0 and values is None and "unrecorded" in output
    for name, bad in {
        "HAI_BUILD_RUN_ID": "../x",
        "HAI_BUILD_DBT_TREE_SHA256": "ABC",
        "HAI_BUILD_PINS_SHA256": "b" * 63,
        "HAI_BUILD_GIT_REVISION": "main",
        "HAI_BUILD_PINS_JSON": "[1]",
    }.items():
        code, values, _ = dbt.values({**valid, name: bad})
        checks[f"{name} malformed stops compilation"] = code != 0 and values is None
    code, output = dbt.run(["compile", "--select", "int_silver_build_marker"], valid)
    compiled = next((dbt.folder / "target" / "compiled").rglob("int_silver_build_marker.sql"), None)
    text = compiled.read_text() if compiled else ""
    checks["the marker compiles with every value as a column"] = code == 0 and all(
        f"'{value}' as {column}" in text
        for column, value in {"run_id": PINS_RUN, "dbt_tree_sha256": "a" * 64, "bronze_pins_sha256": "b" * 64, "git_revision": "c" * 40}.items()
    )
    checks["the marker keeps the pins text"] = '"cms_hai_hospital": "1"' in text
    code, output = dbt.run(["compile", "--select", "int_silver_build_marker"], {k: v for k, v in valid.items() if k != "HAI_BUILD_PINS_SHA256"})
    checks["the marker does not compile without the pins digest"] = code != 0
    code, output = dbt.run(["compile", "--select", "int_silver_build_marker"])
    checks["a lint compile needs no values"] = code == 0
    ok = [{"status": "success"}, {"status": "pass"}]
    cases = {
        "a full build of successes and passes is eligible": (ok, "build", [], True),
        "a selected build is not eligible": (ok, "build", ["x"], False),
        "a run is not eligible": (ok, "run", [], False),
        "an error is not eligible": ([*ok, {"status": "error"}], "build", [], False),
        "a skip is not eligible": ([*ok, {"status": "skipped"}], "build", [], False),
        "a failed test is not eligible": ([*ok, {"status": "fail"}], "build", [], False),
        "another warning is not eligible": ([*ok, {"status": "warn", "node": {"name": "some_test"}}], "build", [], False),
        "a warning test is not eligible": ([*ok, {"status": "warn", "node": {"name": RELEASE_COPY_TEST}}], "build", [], False),
        "no results are not eligible": ([], "build", [], False),
    }
    for label, (results, which, selection, expected) in cases.items():
        code, output = dbt.operation("show_eligible", {"results": results, "which": which, "selection": selection})
        checks[label] = code == 0 and f"ELIGIBLE={expected}" in output
    code, output = dbt.operation("marker_cycle", {"finalize": False}, valid)
    checks["the start hook drops the marker of a real build"] = code == 0 and "MARKER_ROWS=0" in output
    code, output = dbt.operation("marker_cycle", {"finalize": False}, {})
    checks["the start hook leaves the marker of a lint build"] = code == 0 and "MARKER_ROWS=1" in output
    code, output = dbt.operation("marker_cycle", {"finalize": True}, valid)
    checks["the end hook drops the marker after a launch that is not a build"] = code == 0 and "MARKER_ROWS=0" in output
    code, output = dbt.operation("marker_cycle", {"finalize": True}, {})
    checks["the end hook leaves the marker of a lint build"] = code == 0 and "MARKER_ROWS=1" in output
    code, output = dbt.operation("marker_cycle", {"finalize": False}, {k: v for k, v in valid.items() if k != "HAI_BUILD_PINS_SHA256"})
    checks["the start hook stops a real build without a value"] = code != 0
    return checks


@case("hook_command_coverage")
def hook_command_coverage(env: Env) -> dict[str, bool]:
    """922 (D1 section 10 items 4 and 5, review round 1 finding 3): for every dbt command the launcher admits, the launcher treats it as mutating exactly when
    dbt runs the hooks that drop the marker; the end hook writes the marker of an eligible build. Host dbt on a scratch project and a scratch DuckDB file
    (synthetic stub models; no real project, catalog or build)."""
    module = provenance()
    dbt = Dbt(env.work / "dbt_hooks")
    valid = dict(Dbt.VALID)
    checks: dict[str, bool] = {}
    commands = {
        "build": ["build", "--select", "stub_a"],
        "run": ["run", "--select", "stub_a"],
        "seed": ["seed"],
        "snapshot": ["snapshot"],
        "test": ["test"],
        "compile": ["compile"],
        "compile with a selection": ["compile", "--select", "stub_a"],
        "debug": ["debug"],
        "ls": ["ls"],
        "parse": ["parse"],
    }

    def marker(extra: str = "MARKER_TABLES") -> str | None:
        code, output = dbt.operation("marker_state", None, valid, "scratch")
        found = re.search(rf"{extra}=(.*)", output)
        return found.group(1).strip() if code == 0 and found else None

    def stub() -> str | None:
        """How many tables named ``stub_a`` the build file holds."""
        code, output = dbt.operation("stub_state", None, valid, "scratch")
        found = re.search(r"STUB_TABLES=(.*)", output)
        return found.group(1).strip() if code == 0 and found else None

    built, _ = dbt.run(["run", "--select", "stub_a"], valid, "scratch")
    checks["control: the stub model was built once"] = built == 0 and stub() == "1"
    for label, arguments in commands.items():
        created, _ = dbt.operation("marker_create", None, valid, "scratch")
        before = marker()
        code, output = dbt.run(arguments, valid, "scratch")
        after = marker()
        mutating = module.is_mutating(arguments)
        checks[f"{label}: the marker existed and the command ran"] = created == 0 and before == "1" and code == 0
        checks[f"{label}: dbt drops the marker if the launcher calls the command mutating"] = not mutating or after == "0"
        checks[f"{label}: dbt leaves the marker and the relations alone if the launcher calls the command non-mutating"] = mutating or (
            after == "1" and stub() == "1"
        )
    code, output = dbt.operation("show_untouched", None, valid, "scratch")
    listed = re.search(r"UNTOUCHED=(\[.*\])", output)
    checks["the hooks leave alone exactly the launcher's CONNECTING and INERT commands"] = (
        code == 0 and listed is not None and sorted(json.loads(listed.group(1))) == sorted([*module.CONNECTING, *module.INERT])
    )
    refused_operation = expect_error_of(lambda: launcher.validate_dbt_args("real_dbt", ["run-operation", "any_macro"]))
    checks["run-operation: the launcher admits no macro, so no change escapes the hooks"] = isinstance(refused_operation, launcher.LauncherError)
    checks["every command the launcher admits was tried"] = {arguments[0] for arguments in commands.values()} | {"run-operation"} == set(launcher.DBT_COMMANDS)
    try:
        classes = [module.HOOKED, module.OPERATION, module.CONNECTING, module.INERT]
    except AttributeError:
        classes = []
    checks["every admitted command is in exactly one class of the module"] = sorted(sum(classes, ())) == sorted(launcher.DBT_COMMANDS)
    hook_only = Dbt(env.work / "dbt_hook_only", marker_model=False)
    code, output = hook_only.run(["build"], valid, "scratch")
    state = hook_only.operation("marker_state", None, valid, "scratch")[1]
    row = re.search(r"MARKER_ROW=(\{.*\})", state)
    values = json.loads(row.group(1)) if row else {}
    checks["control: a full build of successes and passes ran"] = code == 0
    checks["the end hook wrote one marker row for the eligible build"] = "MARKER_ROWS=1" in state and bool(values)
    checks["the marker row carries the provenance values"] = (
        values.get("run_id") == PINS_RUN
        and values.get("dbt_tree_sha256") == "a" * 64
        and values.get("bronze_pins_sha256") == "b" * 64
        and values.get("git_revision") == "c" * 40
        and values.get("pins_json") == valid["HAI_BUILD_PINS_JSON"]
        and bool(re.fullmatch(r"[0-9a-f-]{36}", str(values.get("dbt_invocation_id", ""))))
    )
    selected, _ = hook_only.run(["build", "--select", "stub_a"], valid, "scratch")
    selected_state = hook_only.operation("marker_state", None, valid, "scratch")[1]
    checks["a selected build leaves no marker"] = selected == 0 and "MARKER_TABLES=0" in selected_state
    excluded, _ = hook_only.run(["build", "--exclude", "stub_a"], valid, "scratch")
    excluded_state = hook_only.operation("marker_state", None, valid, "scratch")[1]
    checks["a build with an exclusion leaves no marker"] = excluded == 0 and "MARKER_TABLES=0" in excluded_state
    again, _ = hook_only.run(["build"], valid, "scratch")
    checks["control: the full build writes the marker again"] = again == 0 and "MARKER_ROWS=1" in hook_only.operation("marker_state", None, valid, "scratch")[1]
    warning = hook_only.folder / "project" / "tests" / f"{RELEASE_COPY_TEST}.sql"
    warning.write_text("{{ config(severity='warn') }}\nselect 1 as x\n")
    warned, warned_output = hook_only.run(["build"], valid, "scratch")
    warned_state = hook_only.operation("marker_state", None, valid, "scratch")[1]
    checks["control: dbt ran the build with a warning test"] = warned == 0 and "WARN" in warned_output
    checks["a build with a warning test leaves no marker"] = "MARKER_TABLES=0" in warned_state
    warning.unlink()
    clean, _ = hook_only.run(["build"], valid, "scratch")
    checks["a build without the warning writes the marker"] = clean == 0 and "MARKER_ROWS=1" in hook_only.operation("marker_state", None, valid, "scratch")[1]
    return checks


@case("marker_project_compile")
def marker_project_compile(env: Env) -> dict[str, bool]:
    """920: the real project parses with the macros and compiles the marker with the values, and stops without them (host dbt, ``lint`` target, no build)."""
    folder = env.work / "dbt_project"
    shutil.copytree(ROOT / "dbt", folder / "project", ignore=shutil.ignore_patterns("target", "logs", "dbt_packages"))
    (folder / "profiles.yml").write_text((ROOT / "dbt" / "profiles.yml").read_text())
    packages = run_lock.main_checkout(ROOT) / "data" / "analytics" / "dbt" / "dbt_packages"
    base = {
        "DBT_PROFILES_DIR": str(folder),
        "DBT_TARGET_PATH": str(folder / "target"),
        "DBT_LOG_PATH": str(folder / "logs"),
        "DBT_PACKAGES_INSTALL_PATH": str(packages),
    }
    environment = {k: v for k, v in os.environ.items() if not k.startswith(("HAI_", "DBT_"))} | base

    def compile_marker(extra: dict[str, str]) -> tuple[int, str]:
        completed = run_command(
            str(TOOLS_DBT),
            ["compile", "--select", "int_silver_build_marker", "--project-dir", str(folder / "project"), "--target", "lint"],
            cwd=folder,
            env=environment | extra,
            timeout=900,
        )
        return completed.returncode, completed.stdout + completed.stderr

    code, _ = compile_marker(Dbt.VALID)
    compiled = next((folder / "target" / "compiled").rglob("int_silver_build_marker.sql"), None)
    text = compiled.read_text() if compiled else ""
    stopped, output = compile_marker({k: v for k, v in Dbt.VALID.items() if k != "HAI_BUILD_DBT_TREE_SHA256"})
    plain, _ = compile_marker({})
    return {
        "the project compiles the marker with the values": code == 0 and "'" + PINS_RUN + "' as run_id" in text,
        "the project stops on a missing value": stopped != 0 and "HAI_BUILD_DBT_TREE_SHA256" in output,
        "the project compiles the marker without values on lint": plain == 0,
    }


ALL_CASES = (
    interface_frozen,
    provenance_values,
    tree_digest_parity,
    provenance_refusals,
    provenance_selection_bypass,
    writer_serialized,
    write_queue_rules,
    seal_after_build,
    sealed_refusals,
    seal_not_written,
    seal_direct,
    warning_not_tolerated,
    stale_artifacts_control,
    stale_artifacts_debug,
    stale_artifacts_test,
    seal_invocation_mismatch,
    seal_binding,
    seal_concurrent,
    snapshot_link_swap,
    seal_descriptor_traversal,
    artifacts_cleared,
    seal_resume,
    seal_fence_control,
    seal_fence_recovery,
    seal_fence_restart,
    seal_graceful_stop,
    seal_graceful_stop_publishes,
    completion_of_live_build,
    release_copy_warning_refused,
    seal_clone_replaced_path,
    consumer_digest,
    sealed_completion_checked,
    helpers_wait_for_seal,
    consumer_unsealed,
    reader_behind_consumer,
    quality_reader_behind_consumer,
    gold_wiring,
    consumer_cli,
    consumer_result_accepted,
    no_token_exposure,
    marker_macros,
    hook_command_coverage,
    marker_project_compile,
)


def wanted(kind: str, patched: bool, proposals: bool) -> bool:
    """True when a case of this kind belongs to the run: ``any`` always, ``unpatched`` and ``patched`` by the launcher patch, ``proposals`` by the flag."""
    return {"any": True, "unpatched": not patched, "patched": patched, "proposals": proposals}[kind]


def run_cases(selected: list[str], patched: bool, proposals: bool = False) -> int:
    """Run the selected cases (the default set depends on ``patched``), write the report and return the exit code."""
    if {function for function, _ in CASES.values()} != set(ALL_CASES):
        raise SystemExit("ALL_CASES and the registry of @case differ")
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    report = OUT / stamp
    report.mkdir(parents=True)
    names = selected or [name for name, (_, kind) in CASES.items() if wanted(kind, patched, proposals)]
    sources = {
        name: hashlib.sha256((ROOT / name).read_bytes()).hexdigest()
        for name in ("orchestration/launcher.py", "scripts/lakehouse/provenance.py", "scripts/lakehouse/run_provenance_e2e.py")
        if (ROOT / name).is_file()
    }
    results: list[dict[str, Any]] = []
    for name in names:
        function, _ = CASES[name]
        env = E2E(name, report)
        started = time.monotonic()
        error = ""
        checks: dict[str, bool] = {}
        try:
            checks = function(env)
        except Exception as exc:
            error = f"{type(exc).__name__}: {exc}\n{traceback.format_exc()[-1800:]}"
        leftovers = env.close()
        checks["nothing of the case is left over"] = not leftovers
        status = "pass" if checks and all(checks.values()) and not error else "fail"
        results.append(
            {"case": name, "status": status, "checks": checks, "error": error, "leftovers": leftovers, "seconds": round(time.monotonic() - started, 1)}
        )
        sys.stdout.write(f"{'ok' if status == 'pass' else 'FAIL':<5} {name} ({results[-1]['seconds']} s)\n")
        for check, ok in checks.items():
            if not ok:
                sys.stdout.write(f"      failed: {check}\n")
        if error:
            sys.stdout.write(f"      error: {error.splitlines()[0]}\n")
    head = run_command("git", ["rev-parse", "HEAD"], cwd=ROOT).stdout.strip()
    summary = {
        "feature": "provenance and the sealed build, fake Docker, test registries",
        "command": ".venv/bin/python -m scripts.lakehouse.run_provenance_e2e" + (" --proposals" if proposals else " --launcher-patch" if patched else ""),
        "started_utc": stamp,
        "head": head,
        "launcher_patch_applied": patched,
        "proposals_applied": proposals,
        "sources": sources,
        "python": sys.version.split()[0],
        "cases": results,
        "passed": sum(1 for r in results if r["status"] == "pass"),
        "total": len(results),
    }
    (report / "report.json").write_text(json.dumps(summary, indent=2) + "\n")
    sys.stdout.write(f"{summary['passed']}/{summary['total']}: {report.relative_to(ROOT)}/report.json\n")
    return 0 if summary["passed"] == summary["total"] else 1


def main() -> int:
    """Parse arguments and run."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--case", action="append", default=[], choices=sorted(CASES))
    parser.add_argument("--launcher-patch", action="store_true", help="the proposed launcher patch is applied: run the patched cases")
    parser.add_argument("--proposals", action="store_true", help="every proposed patch is applied: run the patched cases and the cases of the proposals")
    args = parser.parse_args()
    return run_cases(args.case, args.launcher_patch or args.proposals, args.proposals)


if __name__ == "__main__":
    sys.exit(main())


__all__ = ["CASES", "Fakes", "run_cases"]
