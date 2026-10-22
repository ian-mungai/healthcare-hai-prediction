"""Host driver of the offline gold publish checks, through a launcher on a test registry.

Run ``.venv/bin/python -m scripts.lakehouse.run_gold_publish_e2e --mode <mode>``. Reports stay in data/e2e/gold_publish/<stamp>/.

Modes:

- ``launcher``: the gold publish templates against the fake Docker Engine (render, admission and the sentinel scan). No Docker.
- ``contract``: the contract's merge keys against the gold contract checks' own reading of grains, dimension keys and bridge grains. No Docker.
- ``writer_setup``: the ``gold_writer`` and ``silver_writer`` set-up requests against a recording stand-in for Polaris. No Docker, no catalog.
- ``runner``: the driver's own start-up, clean-up and final-report cases, against a stand-in for the launcher client. No launcher, no Docker.
- ``local_iceberg``: the project's Spark image on a local file-based Iceberg catalog proves a branch per table, ``MERGE INTO`` on a branch,
  candidates invisible on ``main``, the fast-forward and a read-back.
- ``suite``: only the case suite of ``publish_offline`` (the Spark image, a local catalog).
- ``publish_offline``: the whole sequence on a local catalog. A fixture sealed copy of every contract table is made by the pinned DuckDB image
  (``dim_release`` with null, empty and populated ``DATE[]`` and ``VARCHAR[]`` lists), exported by the consumer in the quality image (a replaced copy
  is refused), published step by step with one container per step, read back (rows, fingerprints and the exact lists), and then the case suite runs
  in the Spark image (interruptions, retries, tampering, a moved main, maintenance). A sentinel scan closes the run.

The launcher is a throwaway one on a test registry under the system temporary folder. It talks to the real Docker daemon and starts one container
at a time, each with network none. No Polaris, no S3, no AWS profile and no secret exist in these modes: the sentinel values the scan looks for are
planted in the test registry's own compose file, which no offline template reads.
"""

from __future__ import annotations

import argparse
import contextlib
import hashlib
import io
import json
import os
import shutil
import sys
import tempfile
import time
import traceback
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from unittest import mock

from orchestration import launcher
from orchestration.pipeline_protocol import PipelineError, safe_name
from scripts.infrastructure import secret_store
from scripts.infrastructure.secret_store_stand_in import StandIn
from scripts.lakehouse import gold_export, gold_publish, gold_writer_setup, polaris_writer, principal_credentials, run_lock, silver_writer_setup
from scripts.process import run_command

ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / "data" / "e2e" / "gold_publish"
MODES = ("launcher", "contract", "writer_setup", "runner", "local_iceberg", "suite", "publish_offline")
FULL_RUN = "20261014T040000Z-0000ab01"
ROW_SENTINEL = "ROWVALUE-7f3a91"
SUITE_SECONDS = 7000

EXIT_SECONDS = 30.0
CONTAINER_SECONDS = 90.0
# ``ps -o lstart=`` prints a start time as "Wed Oct 14 02:56:00 2026"; one second is its resolution.
LSTART = "%a %b %d %H:%M:%S %Y"
REDACTED = "[redacted]"


LAUNCHER_MODULE = "orchestration.launcher"
OPTIONS_WITH_VALUE = ("-W", "-X", "-Q")
CREATE_FIXTURE = "CREATE TABLE main.{table} AS SELECT {columns} FROM range(12) AS t(i);"
RELEASE_LISTS_SQL = (
    "CASE i % 4 WHEN 0 THEN NULL WHEN 1 THEN []::DATE[] WHEN 2 THEN [DATE '2026-01-01' + CAST(i AS INTEGER)] "
    "ELSE [DATE '2026-01-01' + CAST(i AS INTEGER), DATE '2026-02-01'] END AS \"publication_dates\"",
    "CASE i % 4 WHEN 0 THEN NULL WHEN 1 THEN []::VARCHAR[] WHEN 2 THEN ['run-' || i] ELSE ['run-' || i, 'run-x'] END AS \"download_run_ids\"",
)
RELEASE_LIST_TYPES = {"publication_dates": "DATE[]", "download_run_ids": "VARCHAR[]"}


@dataclass(frozen=True)
class Observed:
    """One look at a process. ``steady``: its start and session agree before and after its command was read, so start, session and command name one process.
    ``changed``: the process changed under the look (a PID reused). ``unreadable``: it exists but cannot be read (access refused, no command). ``gone``."""

    state: str
    start: str | None = None
    session: int | None = None
    command: str = ""


def process_session(pid: int) -> int | None:
    """The session ID of a process, or None where the system does not give it."""
    try:
        return os.getsid(pid)
    except OSError:
        return None


def launcher_arguments(command: str) -> list[str] | None:
    """The arguments after the module when ``command`` runs ``-m orchestration.launcher`` as a Python program, else None.

    The command line is parsed as arguments: the program is Python, only interpreter options stand between it and ``-m`` and the module name follows ``-m``
    itself. A name that appears after a script, a ``-c`` program or another module, or in another program's arguments, is not an invocation.
    """
    words = command.split()
    if not words or not words[0].rsplit("/", 1)[-1].lower().startswith("python"):
        return None
    index = 1
    while index < len(words):
        word = words[index]
        if word == "-m":
            return words[index + 2 :] if words[index + 1 : index + 2] == [LAUNCHER_MODULE] else None
        if word in OPTIONS_WITH_VALUE:
            index += 2
        elif word.startswith("-") and word != "-" and (word.startswith("--") or not {"c", "m"} & set(word[1:])):
            index += 1
        else:
            return None
    return None


class Session:
    """A throwaway test-registry launcher on the real Docker daemon with one allocation; every job runs as an attempt of that allocation.

    The constructor only makes the session's folder. ``start`` starts the launcher and takes the allocation; ``close`` is safe after any part of ``start``: it
    releases what was taken, asks the launcher this session started (by its start nonce) to stop, proves that it is gone and then removes the folder.

    The driver never signals a process by its ID: a PID can name another process between any look at it and the signal. The launcher stops
    itself on the shutdown request. The looks below only decide which processes of this session are still running: the launcher that answered with the
    session's nonce, while its saved start and session still name the PID, or a launcher on this session's test registry that began after the session was
    made. A launcher is an actual ``-m orchestration.launcher --test-registry <registry> serve`` invocation, parsed from the arguments. A process of this
    session that stays after the request, or one that cannot be looked at (access refused, no command) or changes under the look, is kept: the clean-up fails,
    names its PID and keeps the registry. A PID that names another process is left alone.
    """

    def __init__(self) -> None:
        # The resolved path: the launcher's command line shows it (on macOS /tmp is a link to /private/tmp), and ownership is proven from that line.
        self.temp = Path(tempfile.mkdtemp(prefix="p1gold")).resolve()
        self.registry = self.temp / "registry"
        self.nonce = os.urandom(8).hex()
        self.client: Any = None
        self.allocation = ""
        self.principal: Any = {}
        self.pid: int | None = None
        self.pid_start: str | None = None
        self.pid_session: int | None = None
        self.created = time.time()
        self.cleanup_errors: list[str] = []

        self.sentinels = {
            "POLARIS_CLIENT_SECRET": "root-" + os.urandom(8).hex(),
            "POLARIS_READER_CLIENT_SECRET": "reader-" + os.urandom(8).hex(),
            "POLARIS_GOLD_WRITER_CLIENT_SECRET": "writer-" + os.urandom(8).hex(),
        }

    def start(self) -> None:
        """Write the test registry's settings, start the launcher on demand, wait until it serves and take the allocation."""
        self.registry.mkdir(mode=0o700)
        self.stand_in = StandIn({launcher.COMPOSE_SECRETS[name]: value for name, value in self.sentinels.items() if name in launcher.COMPOSE_SECRETS})
        self.stand_in.__enter__()
        settings = {
            "docker_socket": launcher.docker_socket_path(),
            "timings": {"tick": 0.1, "backoff_start": 0.5, "backoff_cap": 4},
            "secrets_endpoint": self.stand_in.endpoint,
            "token_root": str(self.temp / "tokens"),
            "runs_root": str(self.temp / "runs"),
            "lakehouse_state": str(self.temp / "lakehouse"),
        }
        (self.registry / "launcher_test.json").write_text(json.dumps(settings, indent=2))
        self.config = launcher.resolve_config(str(self.registry))
        self.client = launcher.LauncherClient(self.config)
        self.client.launch_nonce = self.nonce
        self.client.connect().close()
        ready = self.wait(self.serving, 90, "the launcher serves the real daemon")
        self.pid = int(ready["pid"]) if ready.get("launch_nonce") == self.nonce and ready.get("pid") else None
        self.pid_start = self.launcher_start(self.pid)
        self.wait(lambda: not self.labelled(), 30, "the probe container is removed")
        self.allocation = self.client.allocate()
        self.principal = self.client.principal_alloc(self.allocation)
        self.sentinels["launcher token"] = self.token_text()

    def serving(self) -> dict[str, Any] | None:
        """The launcher's ping reply once it serves, else None."""
        reply: dict[str, Any] = dict(self.client.call("ping"))
        return reply if reply.get("status") == "serving" else None

    def token_text(self) -> str:
        """The allocation token the launcher issued to this client; the scan must find it in no record."""
        return str(self.principal.get("token", "")) if isinstance(self.principal, dict) else ""

    def wait(self, predicate: Any, seconds: float, what: str) -> Any:
        """Poll until the predicate is truthy."""
        deadline = time.monotonic() + seconds
        while time.monotonic() < deadline:
            value = predicate()
            if value:
                return value
            time.sleep(0.2)
        raise RuntimeError(f"timeout waiting for {what}")

    def labelled(self) -> list[str]:
        """Names of the containers that carry this registry's label, as the launcher reports them."""
        return [str(name) for name in dict(self.client.call("status", {"what": "launcher"}))["containers"]]

    def attempt(self, template: str, launch: str, args: list[str], params: dict[str, Any], seconds: float | None = None) -> launcher.AttemptResult:
        """One attempt of the allocation, waited for."""
        return self.client.run_attempt(self.principal, template, launch, args, params, checkout=ROOT, seconds=seconds)

    def job(self, name: str, launch: str, args: list[str], seconds: float | None = None) -> launcher.AttemptResult:
        """One Spark job of the offline template."""
        return self.attempt("spark_e2e", launch, args, {"job": name}, seconds)

    def runs_launcher(self, command: str) -> bool:
        """True when a command line is the launcher's own invocation on this session's test registry, parsed from its arguments."""
        tail = launcher_arguments(command)
        return tail is not None and " ".join(tail) == f"--test-registry {self.registry} serve"

    def observe(self, pid: int) -> Observed:
        """One look at a process: its start, session, command, session and start again. The look is steady only when both starts and both sessions agree and the
        command was read; a process that cannot be read or that changes under the look proves nothing, and an access refusal is never read as absence."""
        first = run_lock.process_start(pid)
        if first is None:
            try:
                os.kill(pid, 0)
            except ProcessLookupError:
                return Observed("gone")
            except PermissionError:
                pass
            return Observed("unreadable")
        session = process_session(pid)
        command = run_command("ps", ["-o", "command=", "-p", str(pid)], timeout=30).stdout.strip()
        if run_lock.process_start(pid) != first or process_session(pid) != session:
            return Observed("changed")
        return Observed("steady", first, session, command) if command else Observed("unreadable")

    def launcher_start(self, pid: int | None) -> str | None:
        """The start identity of the launcher that answered, saved only from a steady look at a launcher invocation on this registry; its session is saved
        with it (``pid_session``). A process that changes while it is looked at, or that is no launcher here, leaves no identity."""
        self.pid_session = None
        if not pid:
            return None
        seen = self.observe(pid)
        if seen.state != "steady" or not self.runs_launcher(seen.command):
            return None
        self.pid_session = seen.session
        return seen.start

    def classify(self, pid: int) -> str:
        """Who a process is, from one steady look: ``owned`` (this session started it), ``foreign`` (another process), ``unproven`` (it exists but cannot be
        read, or changed under the look) or ``gone``. The launcher that answered is owned while it is a launcher invocation here and its start and session equal
        the saved ones; any other process is owned only when it is a launcher invocation on this registry and began after the session was made."""
        seen = self.observe(pid)
        if seen.state != "steady":
            return "gone" if seen.state == "gone" else "unproven"
        launcher = self.runs_launcher(seen.command)
        if pid == self.pid:
            if self.pid_start is None:
                return "unproven" if launcher else "foreign"
            same = seen.start == self.pid_start and self.pid_session in (None, seen.session)
            return "owned" if same and launcher else "foreign"
        if not launcher:
            return "foreign"
        try:
            began = datetime.strptime(str(seen.start), LSTART).timestamp()
        except ValueError:
            return "unproven"
        return "owned" if began >= self.created - 2 else "foreign"

    def ownership(self) -> tuple[list[int], list[int]]:
        """The PIDs of the processes this session started (owned) and of those whose ownership cannot be proven (unproven); no other PID is signalled."""
        candidates = {self.pid} if self.pid else set()
        listing = run_command("ps", ["-axo", "pid=,command="], timeout=30).stdout
        for line in listing.splitlines():
            pid_text, _, command = line.strip().partition(" ")
            if pid_text.isdigit() and int(pid_text) != self.pid and f"--test-registry {self.registry} " in command + " ":
                candidates.add(int(pid_text))
        states = {pid: self.classify(pid) for pid in sorted(candidates)}
        return sorted(pid for pid, state in states.items() if state == "owned"), sorted(pid for pid, state in states.items() if state == "unproven")

    def launcher_alive(self) -> bool:
        """True while the launcher answers on its socket or a process of this session, or one that may be it, is still running."""
        if self.client is not None:
            try:
                self.client.open_socket(timeout=1).close()
                return True
            except OSError:
                pass
        return any(self.ownership())

    def stop_launcher(self) -> bool:
        """Ask the launcher this session started to stop and wait until it is gone. Nothing is signalled by its process ID.

        Returns True when gone. A process of this session that stays, or whose identity cannot be proven (access refused, no readable command, changed under
        the look), is kept, and the clean-up error names its PID.
        """
        if not self.launcher_alive():
            return True
        try:
            self.client.call("shutdown", {"owner": True, "launch_nonce": self.nonce}, {"kind": "any", "owner": True})
        except Exception as error:
            self.cleanup_errors.append(f"shutdown request: {type(error).__name__}")
        deadline = time.monotonic() + EXIT_SECONDS
        while time.monotonic() < deadline and self.launcher_alive():
            time.sleep(0.2)
        gone = not self.launcher_alive()
        if not gone:
            owned, unproven = self.ownership()
            self.cleanup_errors += [f"process {pid}: this session's launcher did not stop after the shutdown request; it was not signalled" for pid in owned]
            self.cleanup_errors += [f"process {pid}: its identity as this session's launcher cannot be proven, so it was not signalled" for pid in unproven]
        return gone

    def close(self) -> dict[str, Any]:
        """Release the allocation, remove the containers, stop the launcher and prove it is gone, then remove the folder.

        Returns the clean-up report. The folder, which holds the registry the launcher needs to finish its work, stays whenever anything is unresolved: a
        failed release, containers that cannot be read or are left, a launcher that cannot be stopped or is not proven gone.
        """
        self.cleanup_errors = []
        containers: list[str] = []
        if self.client is not None:
            if self.allocation:
                try:
                    self.client.release_allocation(self.allocation)
                except Exception as error:
                    self.cleanup_errors.append(f"release of the allocation: {type(error).__name__}")
            try:
                self.wait(lambda: not self.labelled(), CONTAINER_SECONDS, "the launcher removes the containers of the run")
            except RuntimeError as error:
                self.cleanup_errors.append(str(error))
            except Exception as error:
                self.cleanup_errors.append(f"the containers cannot be read: {type(error).__name__}")
            try:
                containers = self.labelled()
            except Exception as error:
                self.cleanup_errors.append(f"the containers cannot be listed: {type(error).__name__}")
        gone = self.stop_launcher()
        if hasattr(self, "stand_in"):
            self.stand_in.__exit__()
        resolved = gone and not containers and not self.cleanup_errors
        if resolved:
            shutil.rmtree(self.temp, ignore_errors=True)
        return {
            "launcher_left": not gone,
            "containers_left": containers,
            "errors": list(self.cleanup_errors),
            "launcher_identity": "proven" if self.pid_start else "not recorded",
            "registry_kept": None if resolved else str(self.registry),
        }


def step(name: str, ok: bool, **evidence: Any) -> dict[str, Any]:
    """One recorded step, echoed."""
    sys.stdout.write(f"{'ok' if ok else 'FAIL':<5} {name}\n")
    return {"step": name, "ok": bool(ok), **evidence}


def launcher_cases(report: Path, selected: list[str] | None = None) -> list[dict[str, Any]]:
    """The fake-Docker cases of the gold publish templates (all, or the selected ones); one step per case, with its checks."""
    from scripts.lakehouse import run_gold_publish_launcher_cases as cases

    results = cases.run(selected or [], report)
    (report / "launcher_cases.json").write_text(json.dumps(results, indent=2) + "\n")
    out = []
    for item in results:
        out.append(step(item["case"], item["status"] == "pass", checks=item["checks"], error=item["error"]))
        for check, ok in item["checks"].items():
            if not ok:
                sys.stdout.write(f"      failed: {check}\n")
    return out


def runner_cases(report: Path) -> list[dict[str, Any]]:
    """The driver's own start-up, clean-up and final-report cases; one step per case, with its checks."""
    from scripts.lakehouse import run_gold_publish_runner_cases as cases

    results = cases.run([], report)
    (report / "runner_cases.json").write_text(json.dumps(results, indent=2) + "\n")
    out = []
    for item in results:
        out.append(step(item["case"], item["status"] == "pass", checks=item["checks"], error=item["error"]))
        for check, ok in item["checks"].items():
            if not ok:
                sys.stdout.write(f"      failed: {check}\n")
    return out


def contract_check(report: Path) -> list[dict[str, Any]]:
    """The merge key of every table equals the contract's own grain, dimension key or bridge grain; every key column is a contract column."""
    from scripts.lakehouse import gold_checks

    contract = gold_checks.load_contract()
    plan = gold_publish.load_plan()
    expected: dict[str, tuple[str, ...]] = {fact.name: (fact.grain,) for fact in contract.facts}
    for table in contract.tables:
        expected[table.name] = (table.key,) if table.kind == "dimension" else tuple(table.grain_columns)
    columns = {fact.name: {c.name for c in fact.columns} for fact in contract.facts} | {t.name: {c.name for c in t.columns} for t in contract.bridges}
    missing = [n for n, key in plan.tables.items() if n in columns and not set(key) <= columns[n]]
    return [
        step("the plan names exactly the contract's 70 tables", set(plan.tables) == set(expected) and len(plan.tables) == 70),
        step("every merge key equals the contract's grain, dimension key or bridge grain", dict(plan.tables) == expected),
        step("every key column of a fact or bridge is one of the table's contract columns", not missing, missing=missing),
        step("the namespaces are gold_e2e for publish_fixture and gold for real", dict(plan.namespaces) == {"publish_fixture": "gold_e2e", "real": "gold"}),
        step("the backup prefix is the owner's folder under lakehouse/", plan.backup_prefix == "lakehouse/catalog_backups/"),
        *identifier_rule_steps(),
        *plan_boundary_steps(),
        *silver_read_steps(),
    ]


STRICTEST_NAME_LENGTH = 128
UNSAFE_NAMES = ("a`b", 'a"b', "a'b", "a b", "a-b", "a.b", "a;b", "a\nb", "a\n", "1a", "", "caf\u00e9", "a`; DROP TABLE x; --")


def synthetic_manifest(gold: dict[str, tuple[str, ...]], macros: tuple[str, ...] = ()) -> dict[str, Any]:
    """A dbt manifest of two silver models, one seed, one source and the given gold models; each gold model maps to the unique IDs it depends on."""

    def node(kind: str, name: str, layer: str, depends: tuple[str, ...] = ()) -> dict[str, Any]:
        path = [name] if kind == "seed" else [layer, name]
        return {
            "resource_type": kind,
            "name": name,
            "fqn": ["hai", *path],
            "raw_code": "select 'left_out' as left_out",
            "depends_on": {"nodes": list(depends), "macros": list(macros)},
        }

    nodes = {
        "model.hai.kept": node("model", "kept", "silver"),
        "model.hai.left_out": node("model", "left_out", "silver"),
        "seed.hai.lookup": node("seed", "lookup", "seeds"),
        **{f"model.hai.{name}": node("model", name, "gold", depends) for name, depends in gold.items()},
    }
    return {"nodes": nodes, "sources": {"source.hai.external.left_out": {"resource_type": "source", "name": "left_out"}}}


def silver_read_steps() -> list[dict[str, Any]]:
    """Gold reads only published silver models; the rule reads the dependencies of each gold model in the dbt manifest."""
    from scripts.lakehouse import gold_checks

    published = frozenset({"kept"})

    def problems(gold: dict[str, tuple[str, ...]], macros: tuple[str, ...] = (), names: frozenset[str] = published) -> list[str]:
        return gold_checks.unpublished_silver_problems(synthetic_manifest(gold, macros), names)

    refused = "g reads the silver model left_out, which the silver publish contract does not publish"
    contract = gold_checks.load_contract()
    dimension = contract.dimensions[0].name
    wired = gold_checks.lineage_problems(contract, synthetic_manifest({dimension: ("model.hai.left_out",)}), published)
    live = gold_checks.published_silver()
    return [
        step(
            "a gold model that depends on a published silver model has no problem, whatever names its SQL text holds",
            problems({"g": ("model.hai.kept",)}) == [],
        ),
        step("a gold model that depends on an unpublished silver model is named with that model", problems({"g": ("model.hai.left_out",)}) == [refused]),
        step(
            "a dependency that a macro resolves is named like a direct one",
            problems({"g": ("model.hai.left_out",)}, macros=("macro.hai.hidden_relation",)) == [refused],
        ),
        step(
            "a seed and a source() with the name of an unpublished model are not silver reads",
            problems({"g": ("seed.hai.lookup", "source.hai.external.left_out")}) == [],
        ),
        step("a gold model that depends on another gold model is not a silver read", problems({"g": ("model.hai.kept",), "h": ("model.hai.g",)}) == []),
        step(
            "a published name that no silver model has is named",
            problems({"g": ("model.hai.kept",)}, names=frozenset({"kept", "ghost"})) == ["ghost is published but no silver model has that name"],
        ),
        step(
            "a manifest without silver models fails instead of passing an empty scan",
            gold_checks.unpublished_silver_problems({"nodes": {}}, published) == ["the manifest holds no silver model, so no read was checked"],
        ),
        step(
            "the lineage check applies the rule to a contract model",
            any(item.startswith(f"{dimension} reads the silver model left_out") for item in wired),
            problems=wired,
        ),
        step("the silver publish contract lists tables", len(live) > 50, published=len(live)),
    ]


def plan_boundary_steps() -> list[dict[str, Any]]:
    """The publish plan loader accepts a table name and a merge-key column of the strictest length and refuses one character more."""
    document = json.loads(gold_publish.CONTRACT.read_text())

    def loads(table: str, column: str) -> bool:
        changed = json.loads(json.dumps(document))
        tables = changed["publish"]["tables"]
        first = next(iter(tables))
        tables[table] = {**tables.pop(first), "merge_key": [column]}
        with tempfile.TemporaryDirectory(prefix="publish_plan_") as folder:
            path = Path(folder) / "gold_contract.json"
            path.write_text(json.dumps(changed))
            try:
                plan = gold_publish.load_plan(path)
            except gold_publish.PublishError as error:
                if "not a plain identifier" not in str(error):
                    raise
                return False
            return plan.tables[table] == (column,)

    ordinary = "a"
    longest = "a" * STRICTEST_NAME_LENGTH
    over = "a" * (STRICTEST_NAME_LENGTH + 1)
    return [
        step("the plan loads with ordinary names", loads(ordinary, ordinary)),
        step(f"the plan loads a table name of {STRICTEST_NAME_LENGTH} characters", loads(longest, ordinary)),
        step(f"the plan refuses a table name of {STRICTEST_NAME_LENGTH + 1} characters", not loads(over, ordinary)),
        step(f"the plan loads a merge-key column of {STRICTEST_NAME_LENGTH} characters", loads(ordinary, longest)),
        step(f"the plan refuses a merge-key column of {STRICTEST_NAME_LENGTH + 1} characters", not loads(ordinary, over)),
    ]


def identifier_rule_steps() -> list[dict[str, Any]]:
    """The rule for a name in a statement accepts every contract name and refuses an unsafe name or one past the strictest limit."""
    from scripts.lakehouse import gold_checks

    def refused(name: str) -> bool:
        try:
            gold_publish.ident(name)
        except gold_publish.PublishError as error:
            return "not a plain identifier" in str(error)
        return False

    def import_refuses(name: str) -> bool:
        try:
            safe_name(name)
        except PipelineError:
            return True
        return False

    contract = gold_checks.load_contract()
    names = {table.name for table in contract.tables} | {fact.name for fact in contract.facts} | {bridge.name for bridge in contract.bridges}
    for fact in contract.facts:
        names |= (
            {name for name, _ in fact.expected_columns}
            | {column.silver_column for column in fact.columns}
            | {key.natural_key for key in fact.keys}
            | {fact.grain}
        )
    for table in contract.tables:
        names |= {column.name for column in table.columns} | {table.key, *table.grain_columns}
    silver = json.loads(gold_checks.SILVER_COLUMNS.read_text())["models"]
    for model, item in silver.items():
        names |= {model, *(column[0] for column in item["columns"])}
    over = sorted(name for name in names if refused(name))
    published = (
        set(json.loads((ROOT / "data_contracts/lakehouse/silver_publish.json").read_text())["publish"]["tables"])
        | {table.name for table in contract.tables}
        | {fact.name for fact in contract.facts}
    )
    return [
        step(
            "the contracts name more than 900 distinct tables and columns, and some are longer than 63 characters",
            len(names) > 900 and any(len(n) > 63 for n in names),
        ),
        step("every table and column of the gold and silver contracts is accepted", not over, refused=over),
        step(
            f"a name of exactly {STRICTEST_NAME_LENGTH} characters is accepted", gold_publish.ident("a" * STRICTEST_NAME_LENGTH) == "a" * STRICTEST_NAME_LENGTH
        ),
        step(f"a name of {STRICTEST_NAME_LENGTH + 1} characters is refused as not a plain identifier", refused("a" * (STRICTEST_NAME_LENGTH + 1))),
        step(
            "a name that is unsafe in a statement is refused as not a plain identifier",
            all(refused(name) for name in UNSAFE_NAMES),
            accepted=[n for n in UNSAFE_NAMES if not refused(n)],
        ),
        step("every published silver and gold table name is accepted when the build imports it", not [n for n in published if import_refuses(n)]),
        step(
            f"the import accepts a table name of {STRICTEST_NAME_LENGTH} characters and refuses one of {STRICTEST_NAME_LENGTH + 1}",
            not import_refuses("a" * STRICTEST_NAME_LENGTH) and import_refuses("a" * (STRICTEST_NAME_LENGTH + 1)),
        ),
        step(
            "the import refuses a table name with an uppercase letter, a leading digit, a hyphen or no characters",
            all(import_refuses(name) for name in ("Ab", "1a", "a-b", "")),
        ),
    ]


EXPECTED_GOLD = polaris_writer.Writer("gold_writer", ("gold", "gold_e2e"), ("POLARIS_GOLD_WRITER_CLIENT_ID", "POLARIS_GOLD_WRITER_CLIENT_SECRET"))
EXPECTED_SILVER = polaris_writer.Writer("silver_writer", ("silver", "silver_e2e"), ("POLARIS_SILVER_WRITER_CLIENT_ID", "POLARIS_SILVER_WRITER_CLIENT_SECRET"))
EXPECTED_CATALOG_GRANTS = ("CATALOG_READ_PROPERTIES",)
EXPECTED_NAMESPACE_GRANTS = (
    "NAMESPACE_LIST",
    "NAMESPACE_READ_PROPERTIES",
    "TABLE_LIST",
    "TABLE_CREATE",
    "TABLE_READ_PROPERTIES",
    "TABLE_WRITE_PROPERTIES",
    "TABLE_READ_DATA",
    "TABLE_WRITE_DATA",
)


@dataclass
class WriterRun:
    """What one writer's set-up did against the recording stand-in: how far its calls went and how it handled the credentials."""

    first: str
    second: str
    count_first: int
    count_second: int
    first_stored: bool
    second_stored_nothing: bool
    refuse: Callable[[Callable[..., tuple[int, Any]]], object]
    extra: list[dict[str, Any]]


Drive = Callable[[Callable[..., tuple[int, Any]], str, str, dict[str, Any], list[tuple[str, str, Any, str | None]]], WriterRun]


def drive_file_store(ensure: Callable[..., str], expected: polaris_writer.Writer) -> Drive:
    """Run a set-up that keeps the writer's pair in the catalog credentials file, against an in-memory store."""

    def drive(
        request: Callable[..., tuple[int, Any]], token: str, secret: str, state: dict[str, Any], calls: list[tuple[str, str, Any, str | None]]
    ) -> WriterRun:
        id_key, secret_key = expected.keys
        stored: list[dict[str, str]] = []
        first = ensure(token, "hai_lakehouse", {"POLARIS_CLIENT_ID": "root", "POLARIS_CLIENT_SECRET": "r"}, request, stored.append)
        count_first = len(calls)
        first_stored = len(stored) == 1 and stored[0][id_key] == "writer-id" and stored[0][secret_key] == secret
        second = ensure(token, "hai_lakehouse", stored[-1], request, stored.append)
        return WriterRun(
            first,
            second,
            count_first,
            len(calls),
            first_stored,
            len(stored) == 1,
            lambda denied: ensure(token, "hai_lakehouse", {}, denied, stored.append),
            [],
        )

    return drive


def drive_secrets_manager(report: Path) -> Drive:
    """Run the gold set-up against the Secrets Manager stand-in: one pair stored once, a half-written pair repaired and one update at a time."""
    id_name, secret_name = secret_store.POLARIS_GOLD_CLIENT_ID, secret_store.POLARIS_GOLD_CLIENT_SECRET
    setup = gold_writer_setup
    lock = report / "locks" / "gold_writer.lock"

    def drive(
        request: Callable[..., tuple[int, Any]], token: str, secret: str, state: dict[str, Any], calls: list[tuple[str, str, Any, str | None]]
    ) -> WriterRun:
        with StandIn() as stand_in:
            vault = secret_store.open_store(Path.cwd(), stand_in.endpoint)
            first = setup.ensure_writer(token, "hai_lakehouse", vault, request, lock)
            stored = dict(stand_in.values)
            changes_first = stand_in.changed()
            count_first = len(calls)
            second = setup.ensure_writer(token, "hai_lakehouse", vault, request, lock)
            changes_second = stand_in.changed()
            count_second = len(calls)
        with StandIn({secret_name: "old-secret"}) as stand_in:
            vault = secret_store.open_store(Path.cwd(), stand_in.endpoint)
            real_put = vault.put

            def interrupted(name: str, value: str) -> None:
                if name == secret_name:
                    raise secret_store.SecretStoreError("simulated failure between the two writes")
                real_put(name, value)

            state.update({"principal": True, "secret": secret + "-issued-before"})
            with mock.patch.object(vault, "put", interrupted):
                try:
                    setup.ensure_writer(token, "hai_lakehouse", vault, request, lock)
                    interrupted_failed = False
                except secret_store.SecretStoreError:
                    interrupted_failed = True
            half_written = dict(stand_in.values)
            resumed = setup.ensure_writer(token, "hai_lakehouse", vault, request, lock)
            repaired = dict(stand_in.values)
            settled = stand_in.changed()
            again = setup.ensure_writer(token, "hai_lakehouse", vault, request, lock)
            unchanged = stand_in.changed() == settled
        with StandIn() as stand_in:
            vault = secret_store.open_store(Path.cwd(), stand_in.endpoint)
            with principal_credentials.serialized(lock, "gold_writer", 1.0), mock.patch.object(principal_credentials, "LOCK_WAIT_SECONDS", 0.3):
                began = time.monotonic()
                try:
                    setup.ensure_writer(token, "hai_lakehouse", vault, request, lock)
                    waited_for_lock = False
                except principal_credentials.CredentialError:
                    waited_for_lock = time.monotonic() - began < 3
            nothing_while_locked = not stand_in.changed()

        def refuse(denied: Callable[..., tuple[int, Any]]) -> None:
            with StandIn() as refusing:
                setup.ensure_writer(token, "hai_lakehouse", secret_store.open_store(Path.cwd(), refusing.endpoint), denied, lock)

        extra = [
            step(
                "gold_writer: a second update of the principal waits for the lock, ends at its deadline and changes nothing",
                waited_for_lock and nothing_while_locked,
            ),
            step(
                "gold_writer: a failure between the two writes leaves a pair that a rerun tests, resets and stores again",
                interrupted_failed
                and half_written == {id_name: "writer-id", secret_name: "old-secret"}
                and resumed == "credentials reset"
                and repaired[secret_name] == state["secret"]
                and repaired[id_name] == "writer-id"
                and again == "already present"
                and unchanged,
            ),
        ]
        first_stored = stored == {id_name: "writer-id", secret_name: secret} and changes_first == [("CreateSecret", id_name), ("CreateSecret", secret_name)]
        return WriterRun(first, second, count_first, count_second, first_stored, changes_second == changes_first, refuse, extra)

    return drive


def writer_steps(
    expected: polaris_writer.Writer,
    drive: Drive,
    planned: Callable[[str], list[tuple[str, str, dict[str, Any] | None]]],
    main: Callable[[list[str]], int],
    refused: tuple[str, ...],
) -> list[dict[str, Any]]:
    """One writer's set-up requests against a recording stand-in, judged against ``expected``: exact scope and privileges, idempotence, failures, secrets."""
    secret = "writer-secret-" + os.urandom(6).hex()
    token = "access-token-" + os.urandom(6).hex()
    calls: list[tuple[str, str, Any, str | None]] = []
    state: dict[str, Any] = {"principal": False, "secret": secret, "resets": 0}
    own = list(expected.namespaces)

    def request(method: str, path: str, access: str | None = None, body: Any = None, form: Any = None) -> tuple[int, Any]:
        if path.endswith("/oauth/tokens"):
            accepted = form["client_id"] == "writer-id" and form["client_secret"] == state["secret"]
            return (200, {"access_token": "t"}) if accepted else (401, None)
        calls.append((method, path, body, access))
        if method == "GET" and "/principals/" in path:
            return (200, {}) if state["principal"] else (404, None)
        if method == "POST" and path.endswith("/principals"):
            state["principal"] = True
            return 201, {"credentials": {"clientId": "writer-id", "clientSecret": state["secret"]}}
        if method == "POST" and path.endswith("/reset"):
            state["resets"] += 1
            state["secret"] = f"{secret}-reset{state['resets']}"
            return 200, {"credentials": {"clientId": "writer-id", "clientSecret": state["secret"]}}
        if method == "POST" and path.endswith(("principal-roles", "catalog-roles", "/namespaces")):
            return (409, None) if any(c[0] == method and c[1] == path and c[2] == body for c in calls[:-1]) else (201, {})
        return 200, {}

    run = drive(request, token, secret, state, calls)
    grants = [c[2]["grant"] for c in calls[: run.count_first] if c[0] == "PUT" and c[1].endswith("/grants")]
    namespace_grants = [g for g in grants if g["type"] == "namespace"]
    created = [c[2]["namespace"] for c in calls[: run.count_first] if c[0] == "POST" and c[1].endswith("/namespaces")]
    count_first = run.count_first
    text = json.dumps([c[:3] for c in calls])

    def denied(method: str, path: str, access: str | None = None, body: Any = None, form: Any = None) -> tuple[int, Any]:
        return 403, {"detail": token}

    try:
        run.refuse(denied)
        refusal = ""
    except polaris_writer.SetupError as error:
        refusal = str(error)
    plan_lines = io.StringIO()
    with contextlib.redirect_stdout(plan_lines):
        dry_status = main(["--dry-run"])
    label = f"{expected.name}: "
    expected_grants = [{"type": "catalog", "privilege": p} for p in EXPECTED_CATALOG_GRANTS] + [
        {"type": "namespace", "namespace": [n], "privilege": p} for n in own for p in EXPECTED_NAMESPACE_GRANTS
    ]
    owned = {(n,) for n in own}
    per_namespace = {n: sorted(g["privilege"] for g in namespace_grants if g["namespace"] == [n]) for n in own}
    named = [
        c[2][kind]["name"]
        for c in calls[:count_first]
        if c[0] in ("POST", "PUT") and isinstance(c[2], dict)
        for kind in ("principalRole", "catalogRole", "principal")
        if kind in c[2]
    ]
    return [
        step(
            label + "first run creates the principal and stores its pair once",
            run.first == "created" and run.first_stored,
        ),
        *run.extra,
        step(
            label + f"the namespaces granted are exactly {' and '.join(own)}",
            bool(namespace_grants) and {tuple(g["namespace"]) for g in namespace_grants} == owned,
            granted=sorted(".".join(g["namespace"]) for g in namespace_grants),
        ),
        step(label + "the writer creates exactly its own namespaces and no other", sorted(created) == sorted([n] for n in own)),
        step(
            label + f"no grant or namespace names a refused namespace ({', '.join(refused)})",
            bool(namespace_grants)
            and not {n for g in namespace_grants for n in g["namespace"]} & set(refused)
            and not {n for ns in created for n in ns} & set(refused),
        ),
        step(
            label + "each namespace gets exactly the eight table privileges, none extra and none missing",
            all(privileges == sorted(EXPECTED_NAMESPACE_GRANTS) for privileges in per_namespace.values()),
            granted=per_namespace,
        ),
        step(
            label + "no forbidden privilege is granted (no management, namespace create or drop, table drop)",
            not {g["privilege"] for g in grants} & set(polaris_writer.FORBIDDEN_PRIVILEGES),
        ),
        step(
            label + f"the complete set of grants sent is exactly the expected {len(expected_grants)}, with no other type and no duplicate",
            sorted(json.dumps(g, sort_keys=True) for g in grants) == sorted(json.dumps(g, sort_keys=True) for g in expected_grants),
            sent=len(grants),
        ),
        step(
            label + "the catalog-wide grants are exactly the read of catalog properties",
            [g["privilege"] for g in grants if g["type"] == "catalog"] == list(EXPECTED_CATALOG_GRANTS),
        ),
        step(
            label + "the principal, principal role and catalog role carry the expected name",
            bool(named) and set(named) == {expected.name} and all(f"/catalog-roles/{expected.name}/grants" in c[1] for c in calls if c[1].endswith("/grants")),
            named=named,
        ),
        step(
            label + "the root token is sent as the bearer only and never in a body",
            all(token not in json.dumps(c[2]) for c in calls) and all(c[3] == token for c in calls),
        ),
        step(label + "the writer's secret is never sent in a request body or path", secret not in text),
        step(
            label + "a second run stores nothing and resets nothing",
            run.second == "already present" and run.second_stored_nothing and not any(c[1].endswith("/reset") for c in calls[count_first : run.count_second]),
        ),
        step(
            label + "the planned requests match the sent requests of a first run",
            {(m, p) for m, p, _ in planned("hai_lakehouse")} <= {(c[0], c[1]) for c in calls[:count_first]},
        ),
        step(
            label + "a refused request raises an error that names the action and status and holds no credential",
            "failed (HTTP 403)" in refusal and token not in refusal,
        ),
        step(
            label + "the dry run prints one line per planned request and sends nothing",
            dry_status == 0 and len(plan_lines.getvalue().splitlines()) == len(planned("hai_lakehouse")) and token not in plan_lines.getvalue(),
        ),
    ]


def writer_setup(report: Path) -> list[dict[str, Any]]:
    """The gold and silver writer set-ups against the recording stand-in; the two principals hold disjoint namespaces and credential keys."""
    gold = writer_steps(
        EXPECTED_GOLD, drive_secrets_manager(report), gold_writer_setup.planned_requests, gold_writer_setup.main, ("silver", "silver_e2e", "bronze")
    )
    silver = writer_steps(
        EXPECTED_SILVER,
        drive_file_store(lambda *args: polaris_writer.ensure_writer(silver_writer_setup.SILVER, *args), EXPECTED_SILVER),
        lambda catalog: polaris_writer.planned_requests(silver_writer_setup.SILVER, catalog),
        silver_writer_setup.main,
        ("gold", "gold_e2e", "bronze"),
    )
    pairs = (set(gold_writer_setup.GOLD.keys), set(silver_writer_setup.SILVER.keys))
    return [
        *gold,
        *silver,
        step(
            "the gold and silver writers share no principal name, namespace or credential key",
            gold_writer_setup.GOLD.name != silver_writer_setup.SILVER.name
            and not set(gold_writer_setup.GOLD.namespaces) & set(silver_writer_setup.SILVER.namespaces)
            and not pairs[0] & pairs[1],
        ),
    ]


def local_iceberg(session: Session, report: Path) -> list[dict[str, Any]]:
    """The Iceberg check job; the steps read its report."""
    result = session.job("gold_publish_e2e", "iceberg", ["--case", "iceberg_check", "--stamp", report.name], SUITE_SECONDS)
    cases = json.loads((report / "cases_iceberg_check.json").read_text()) if (report / "cases_iceberg_check.json").is_file() else []
    versions = json.loads((report / "iceberg_versions.json").read_text()) if (report / "iceberg_versions.json").is_file() else {}
    checks = cases[0]["checks"] if cases else {}
    return [
        step("the job ran", result.ok, exit=result.exit_code, reason=result.reason, tail=(result.stdout + result.stderr)[-300:]),
        step("Spark and Iceberg versions", bool(versions), **versions),
        *(step(name, ok) for name, ok in checks.items()),
    ]


def suite(session: Session, report: Path, selected: list[str] | None = None) -> list[dict[str, Any]]:
    """The case suite in the Spark image (all cases, or the selected ones); one step per case."""
    chosen = ["--case", *selected] if selected else []
    ran = session.job("gold_publish_e2e", "suite", [*chosen, "--stamp", report.name], SUITE_SECONDS)
    name = f"cases_{'_'.join(selected)}.json" if selected else "cases_all.json"
    cases = json.loads((report / name).read_text()) if (report / name).is_file() else []
    out = [step("the case suite ran in the Spark image", ran.ok, exit=ran.exit_code, tail=(ran.stdout + ran.stderr)[-300:])]
    out += [step(f"case {item['case']}", item["status"] == "pass", checks=item["checks"], error=item["error"]) for item in cases]
    return out


def fixture_sql(plan: gold_publish.Plan) -> str:
    """SQL that makes a DuckDB file holding every contract table with a few rows; keys unique, values of several types with nulls.

    ``dim_release`` also carries its two list columns with null, empty and populated lists.
    """
    statements = []
    for name, key in plan.tables.items():
        columns = [f"'k' || i AS {gold_export.duck_ident(key[0])}"]
        columns += [f"'g' || (i % 3) AS {gold_export.duck_ident(column)}" for column in key[1:]]
        columns += [
            f"CASE WHEN i % 7 = 0 THEN NULL ELSE '{ROW_SENTINEL}-' || i END AS \"text_a\"",
            'i * 1.5 AS "num_b"',
            'i % 2 = 0 AS "flag_c"',
            "DATE '2026-01-01' + CAST(i AS INTEGER) AS \"date_d\"",
        ]
        if name == "dim_release":
            columns += RELEASE_LISTS_SQL
        statements.append(CREATE_FIXTURE.format(table=gold_export.duck_ident(name), columns=", ".join(columns)))
    return "\n".join(statements) + "\n"


def planted_files(report: Path) -> list[Path]:
    """Text files of the run a scan reads: records, logs and reports; never data files, the warehouse or the fixture SQL (which holds the planted values)."""
    skip = {"warehouse", "exports", "case"}
    found = []
    for path in report.rglob("*"):
        if path.is_file() and path.suffix in (".json", ".log", ".txt", ".md") and not (skip & set(path.relative_to(report).parts)):
            found.append(path)
    return found


def scan(paths: list[Path], values: dict[str, str]) -> dict[str, list[str]]:
    """Which value appears in which file (names only)."""
    hits: dict[str, list[str]] = {}
    for path in paths:
        text = path.read_text(errors="replace")
        for label, value in values.items():
            if value and value in text:
                hits.setdefault(label, []).append(str(path.name))
    return hits


def publish_offline(session: Session, report: Path) -> list[dict[str, Any]]:
    """The whole sequence on a local catalog, one container per step, then the case suite and the sentinel scan."""
    steps: list[dict[str, Any]] = []
    case = report / "case"
    case.mkdir()
    plan = gold_publish.load_plan()
    (case / "fixture.sql").write_text(fixture_sql(plan))
    init = session.attempt(
        "fixture_init",
        "init",
        [],
        {
            "case_host": str(case),
            "case_container": "/workspace/out/rc",
            "database": "/workspace/out/rc/sealed.duckdb",
            "sql_file": "/workspace/out/rc/fixture.sql",
        },
    )
    sealed = case / "sealed.duckdb"
    steps.append(
        step(
            "fixture_init: a DuckDB file with every contract table is written",
            init.ok and sealed.is_file(),
            exit=init.exit_code,
            tail=(init.stdout + init.stderr)[-200:],
        )
    )
    if not init.ok or not sealed.is_file():
        return steps
    sha = gold_publish.sha256_file(sealed)

    replaced = case / "sealed_replaced.duckdb"
    shutil.copy2(sealed, replaced)
    with replaced.open("ab") as handle:
        handle.write(b"\0replaced")
    rejected_dir = report / "runs" / "rejected" / "publish"
    rejected_dir.mkdir(parents=True)
    refused = session.attempt(
        "gold_export", "replaced", [], {"sealed_host": str(replaced), "publish_host": str(rejected_dir), "run": FULL_RUN, "sealed_sha256": sha}
    )
    result_file = rejected_dir / "consumer_result.json"
    result = json.loads(result_file.read_text()) if result_file.is_file() else {}
    accepted = True
    try:
        gold_publish.accept_consumer(result, {"sha256": sha}, FULL_RUN)
    except gold_publish.PublishError:
        accepted = False
    steps.append(
        step(
            "a sealed copy replaced before the consumer starts is refused in the container (nonzero exit, digest of the mounted bytes, no export)",
            not refused.ok
            and refused.exit_code not in (None, 0)
            and result.get("exported") is False
            and result.get("in_container_sha256") == gold_publish.sha256_file(replaced) != sha
            and not (rejected_dir / "export").exists(),
            exit=refused.exit_code,
        )
    )
    steps.append(step("the launcher side rejects that consumer result", not accepted))

    publish_dir = report / "runs" / FULL_RUN / "publish"
    publish_dir.mkdir(parents=True)
    exported = session.attempt(
        "gold_export", "export", [], {"sealed_host": str(sealed), "publish_host": str(publish_dir), "run": FULL_RUN, "sealed_sha256": sha}
    )
    result = json.loads((publish_dir / "consumer_result.json").read_text()) if (publish_dir / "consumer_result.json").is_file() else {}
    accept_ok = True
    try:
        gold_publish.accept_consumer(result, {"sha256": sha}, FULL_RUN)
    except gold_publish.PublishError:
        accept_ok = False
    accepted_result = gold_publish.sha256_file(publish_dir / gold_publish.RESULT_NAME) if accept_ok else ""
    manifest = json.loads((publish_dir / "export" / "manifest.json").read_text()) if (publish_dir / "export" / "manifest.json").is_file() else {"tables": {}}
    steps.append(
        step(
            "gold_export: the consumer exports the 70 tables on the expected digest",
            exported.ok and accept_ok and len(manifest["tables"]) == 70,
            exit=exported.exit_code,
        )
    )
    release_types = dict(manifest["tables"].get("dim_release", {}).get("columns", []))
    steps.append(
        step(
            "gold_export: dim_release keeps publication_dates as DATE[] and download_run_ids as VARCHAR[]",
            all(release_types.get(name) == kind for name, kind in RELEASE_LIST_TYPES.items()),
            types={name: release_types.get(name) for name in RELEASE_LIST_TYPES},
        )
    )
    if not (exported.ok and accept_ok):
        return steps

    container_root = f"/workspace/data/e2e/gold_publish/{report.name}"
    warehouse = report / "multi" / "warehouse"
    warehouse.mkdir(parents=True)
    common = ["--run", FULL_RUN, "--mode", "publish_fixture", "--sealed-sha256", sha, "--accepted-result-sha256", accepted_result]
    common += ["--publish-dir", f"{container_root}/runs/{FULL_RUN}/publish"]
    common += ["--local-warehouse", f"{container_root}/multi/warehouse"]
    namespace_folder = warehouse / "gold_e2e"
    for index, name in enumerate(("candidates", "reconcile", "backup", "commit"), start=1):
        done = session.job("gold_publish_local", name, [name, *common], SUITE_SECONDS)
        steps.append(step(f"gold_publish {name}: one container, exit 0", done.ok, exit=done.exit_code, tail=(done.stdout + done.stderr)[-300:]))
        if not done.ok:
            return steps
        present = {p.name for p in namespace_folder.iterdir()} if namespace_folder.is_dir() else set()
        if index < 4:
            steps.append(
                step(f"after {name}: publish_runs does not exist in the catalog", "publish_runs" not in present and len(present) >= 70, tables=len(present))
            )
        else:
            steps.append(step("after commit: publish_runs exists", "publish_runs" in present))
    (report / "inputs.json").write_text(
        json.dumps(
            {
                "multi": {
                    "warehouse": f"{container_root}/multi/warehouse",
                    "publish_dir": f"{container_root}/runs/{FULL_RUN}/publish",
                    "run_id": FULL_RUN,
                    "sealed_sha256": sha,
                }
            }
        )
    )
    readback = session.job("gold_publish_e2e", "readback", ["--case", "readback", "--stamp", report.name], SUITE_SECONDS)
    readback_cases = json.loads((report / "cases_readback.json").read_text()) if (report / "cases_readback.json").is_file() else []
    steps.append(step("readback in the Spark image: the catalog the five containers wrote equals the export", readback.ok, exit=readback.exit_code))
    steps += [step(f"readback: {name}", ok) for name, ok in (readback_cases[0]["checks"] if readback_cases else {}).items()]
    suite = session.job("gold_publish_e2e", "suite", ["--stamp", report.name], SUITE_SECONDS)
    cases = json.loads((report / "cases_all.json").read_text()) if (report / "cases_all.json").is_file() else []
    steps.append(step("the case suite ran in the Spark image", suite.ok, exit=suite.exit_code, tail=(suite.stdout + suite.stderr)[-300:]))
    for item in cases:
        steps.append(step(f"case {item['case']}", item["status"] == "pass", checks=item["checks"], error=item["error"]))
    return steps


def sentinel_scan(session: Session, report: Path) -> list[dict[str, Any]]:
    """No credential value and no planted data value in any record, log or report; the scan finds a planted value (a positive control)."""
    evidence = report / "evidence"
    evidence.mkdir(exist_ok=True)
    for name in ("registry.json", "launcher.log", "launcher_test.json"):
        if (session.registry / name).exists():
            shutil.copy2(session.registry / name, evidence / name)
    values = {**session.sentinels, "planted row value": ROW_SENTINEL}
    hits = scan(planted_files(report), values)
    control = evidence / "positive_control.txt"
    control.write_text(session.sentinels["POLARIS_GOLD_WRITER_CLIENT_SECRET"])
    found = scan([control], values)
    control.unlink()
    return [
        step("sentinel scan: no credential, launcher token or planted row value in any record, log or report", not hits, hits=hits),
        step("sentinel scan: a planted credential is found by the same scan (positive control)", "POLARIS_GOLD_WRITER_CLIENT_SECRET" in found),
    ]


def places_of(node: Any, value: str, hidden: list[str] | None = None, path: str = "") -> list[str]:
    """The places (key paths, never values) in a JSON-shaped record where a string, a value or a key, holds the value.

    A key that holds the value or any of the ``hidden`` values is named by its position (``<key 0>``), so a place never repeats a planted value.
    """
    secret = [value, *(hidden or [])]
    found: list[str] = []
    if isinstance(node, str):
        return [path or "."] if value in node else []
    if isinstance(node, dict):
        for index, (key, child) in enumerate(node.items()):
            leaked = isinstance(key, str) and any(item and item in key for item in secret)
            label = f"<key {index}>" if leaked else str(key)
            here = f"{path}.{label}" if path else label
            if isinstance(key, str) and value in key:
                found.append(here)
            found += places_of(child, value, hidden, here)
    elif isinstance(node, list):
        for index, child in enumerate(node):
            found += places_of(child, value, hidden, f"{path}[{index}]")
    return found


def redacted(node: Any, values: dict[str, str]) -> Any:
    """The record with every planted value replaced, in keys as well as in values; two keys that become the same text keep both entries."""
    if isinstance(node, str):
        for value in values.values():
            if value:
                node = node.replace(value, REDACTED)
        return node
    if isinstance(node, dict):
        out: dict[Any, Any] = {}
        for key, child in node.items():
            name = redacted(key, values)
            while name in out:
                name = f"{name}#{len(out)}"
            out[name] = redacted(child, values)
        return out
    if isinstance(node, list):
        return [redacted(child, values) for child in node]
    return node


def seal_report(summary: dict[str, Any], values: dict[str, str]) -> dict[str, Any]:
    """Scan the final report as it will be written (every key and value: step, tail, case error, check name, run error) for the planted values.

    A hit fails the run, is recorded by label and place only (a leaked key is named by its position), and is redacted from the report, keys included, before it
    is written, so the persisted report never holds a value. Positive controls prove that the scan finds a value in a tail and in a key.
    """
    hidden = [value for value in values.values() if value]
    hits = {label: places for label, value in values.items() if value and (places := places_of(summary, value, hidden))}
    probe = "probe-" + os.urandom(4).hex()
    in_tail = places_of({"steps": [{"tail": "output " + probe}]}, probe) == ["steps[0].tail"]
    control = in_tail and bool(places_of({"checks": {probe: True}}, probe))
    scan_steps = [
        step("sentinel scan: the final report holds no credential, launcher token or planted row value in any key, step, tail or error", not hits, hits=hits),
        step("sentinel scan: a planted value in a result tail and in a key of the final report is found (positive control)", control),
    ]
    sealed = {**summary, "steps": [*summary["steps"], *scan_steps]}
    if hits or not control:
        sealed["status"] = "fail"
    return dict(redacted(sealed, values) if hits else sealed)


def main() -> int:
    """Run one mode and write its report."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--mode", required=True, choices=MODES)
    parser.add_argument("--case", nargs="+", default=[], help="suite and launcher modes: run these cases only")
    args = parser.parse_args()
    if args.case and args.mode not in ("suite", "launcher"):
        parser.error("--case applies to the suite and launcher modes only")
    report = OUT / f"{datetime.now(UTC).strftime('%Y%m%dT%H%M%SZ')}-{args.mode}"
    report.mkdir(parents=True)
    needs_session = args.mode in ("local_iceberg", "suite", "publish_offline")
    session = Session() if needs_session else None
    steps: list[dict[str, Any]] = []
    error = ""
    cleanup: dict[str, Any] = {}
    try:
        if session is not None:
            session.start()
        if args.mode == "launcher":
            steps = launcher_cases(report, args.case)
        elif session is None:
            steps = {"contract": contract_check, "writer_setup": writer_setup, "runner": runner_cases}[args.mode](report)
        elif args.mode == "suite":
            steps = suite(session, report, args.case)
        else:
            steps = {"local_iceberg": local_iceberg, "publish_offline": publish_offline}[args.mode](session, report)
            if args.mode == "publish_offline":
                steps += sentinel_scan(session, report)
    except Exception as exc:
        error = f"{type(exc).__name__}: {exc}\n{traceback.format_exc()[-1500:]}"
    finally:
        if session is not None:
            cleanup = session.close()
    unresolved = bool(cleanup.get("launcher_left") or cleanup.get("containers_left") or cleanup.get("errors"))
    status = "pass" if steps and all(s["ok"] for s in steps) and not error and not unresolved else "fail"
    summary = {
        "mode": args.mode,
        "command": f".venv/bin/python -m scripts.lakehouse.run_gold_publish_e2e --mode {args.mode}",
        "head": run_command("git", ["rev-parse", "HEAD"], cwd=ROOT).stdout.strip(),
        "status": status,
        "error": error,
        "cleanup": cleanup,
        "containers_left": cleanup.get("containers_left", []),
        "sha256_of_script": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "steps": steps,
    }
    values = {**(session.sentinels if session is not None else {}), "planted row value": ROW_SENTINEL}
    summary = seal_report(summary, values)
    (report / "report.json").write_text(json.dumps(summary, indent=2) + "\n")
    sys.stdout.write(f"{summary['status']}: {report.relative_to(ROOT)}/report.json\n")
    if summary["error"]:
        sys.stdout.write(str(summary["error"]).splitlines()[0] + "\n")
    if unresolved:
        sys.stdout.write(f"cleanup unresolved: registry kept at {cleanup.get('registry_kept')}\n")
    return 0 if summary["status"] == "pass" else 1


if __name__ == "__main__":
    sys.exit(main())
