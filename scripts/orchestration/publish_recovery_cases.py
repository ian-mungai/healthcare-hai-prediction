"""Host-only recovery and deadline scenarios for the publication pipeline: no Spark, launcher, Docker, network or credentials.

The commit scenario runs the real ``commit_publish``, ``record_committed`` and record readers against an in-memory stand-in for the few Spark
statements they issue; only the export verification (it reads the consumer result and Parquet files) is replaced. The deadline scenario runs the real
``wait_for``, ``Broker.call`` and the launcher client's ``call``, ``send`` and ``connect`` with a controlled clock and substituted socket I/O. Iceberg
behavior and a real stalled socket stay with the native runs.
"""

from __future__ import annotations

import contextlib
import math
import os
import re
import signal
import tempfile
from collections.abc import Callable
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast
from unittest import mock

from orchestration import launcher
from scripts.lakehouse import gold_publish
from scripts.lakehouse.gold_publish import RUNS_COLUMNS, Context, Faults, InjectedFault, Plan, backup_name, branch_name, commit_publish, write_record
from scripts.lakehouse.gold_publish_record import PublishError, read_record
from scripts.lakehouse.silver_publish import publish_plan, record_committed
from scripts.orchestration import pipeline_control
from scripts.orchestration.broker import Broker, LauncherUnavailable
from scripts.orchestration.run_control import ControlFailed

RUN = "20261019T120000Z-0000a3f1"
MODE = "publish_fixture"
SEALED, ACCEPTED, MANIFEST, BACKUP_DATA = "1" * 64, "2" * 64, "3" * 64, "4" * 64
CATALOG = "hai_lakehouse"
POINT = "commit:after_publish_runs"
CONFLICTS = {
    "mode": "real",
    "sealed_sha256": "5" * 64,
    "export_manifest_sha256": "6" * 64,
    "backup_name": "catalog_backup_20261019T120000Z-0000a3f2.json",
    "backup_sha256": "7" * 64,
    "snapshot_set": '{"int_acs_county_values": 1}',
}


class Frame:
    """The data frame that ``runs_view`` registers as a temporary view."""

    def __init__(self, spark: MemorySpark, rows: list[tuple[Any, ...]]) -> None:
        self.spark, self.rows = spark, rows

    def createOrReplaceTempView(self, name: str) -> None:
        self.spark.views[name] = self.rows


class Rows:
    """A result that only knows ``collect``."""

    def __init__(self, rows: list[tuple[Any, ...]]) -> None:
        self.rows = rows

    def collect(self) -> list[tuple[Any, ...]]:
        return list(self.rows)


class MemorySpark:
    """The statements of the commit step and the silver recorder over refs and a ``publish_runs`` row list; any other statement fails the scenario."""

    def __init__(self, plan: Plan, bases: dict[str, int], candidates: dict[str, int]) -> None:
        namespace = plan.namespaces[MODE]
        self.refs = {f"{CATALOG}.{namespace}.{name}": {"main": bases[name], branch_name(plan.branch_prefix, RUN): candidates[name]} for name in plan.tables}
        self.snapshots = {*bases.values(), *candidates.values()}
        self.runs_table = f"{CATALOG}.{namespace}.{plan.runs_table}"
        self.runs_exist = False
        self.runs: list[tuple[Any, ...]] = []
        self.views: dict[str, list[tuple[Any, ...]]] = {}
        self.catalog = self

    def tableExists(self, name: str) -> bool:
        return name in self.refs or (name == self.runs_table and self.runs_exist)

    def createDataFrame(self, rows: list[tuple[Any, ...]], schema: str) -> Frame:
        return Frame(self, rows)

    def sql(self, text: str) -> Rows:
        if match := re.fullmatch(r"SELECT name, type, snapshot_id FROM (\S+)\.refs", text):
            return Rows([(name, "BRANCH", snapshot) for name, snapshot in self.refs[match[1]].items()])
        if match := re.fullmatch(rf"CALL {CATALOG}\.system\.fast_forward\('([^']+)', 'main', '([^']+)'\)", text):
            refs = self.refs[f"{CATALOG}.{match[1]}"]
            refs["main"] = refs[match[2]]
            return Rows([])
        if match := re.fullmatch(r"ALTER TABLE (\S+) DROP BRANCH IF EXISTS (\S+)", text):
            self.refs[match[1]].pop(match[2], None)
            return Rows([])
        if re.match(r"CREATE TABLE IF NOT EXISTS \S+ \(", text):
            self.runs_exist = True
            return Rows([])
        if match := re.fullmatch(r"MERGE INTO (\S+) AS t USING (\S+) AS s ON t\.run_id = s\.run_id WHEN NOT MATCHED THEN INSERT \*", text):
            for row in self.views[match[2]]:
                if all(existing[0] != row[0] for existing in self.runs):
                    self.runs.append(row)
            return Rows([])
        if match := re.fullmatch(r"SELECT .+ FROM \S+ WHERE run_id = '([^']+)'", text):
            return Rows([row for row in self.runs if row[0] == match[1]])
        if re.fullmatch(r"SELECT .+ FROM \S+ ORDER BY committed_at ASC, run_id ASC", text):
            return Rows(sorted(self.runs, key=lambda row: (row[2], row[0])))
        if match := re.fullmatch(r"SELECT snapshot_id FROM \S+\.snapshots WHERE snapshot_id = (\d+)", text):
            return Rows([(int(match[1]),)] if int(match[1]) in self.snapshots else [])
        raise AssertionError(f"the in-memory catalog does not model this statement: {text[:80]}")

    def change_run(self, field: str, value: str) -> None:
        index = [name for name, _ in RUNS_COLUMNS].index(field)
        self.runs = [(*row[:index], value, *row[index + 1 :]) for row in self.runs]


def attempt(action: Callable[[], Any]) -> BaseException | None:
    """The exception an action raises, or None."""
    try:
        action()
    except Exception as error:
        return error
    return None


def export_substitute(ctx: Context) -> tuple[dict[str, Any], str, Path]:
    """The verified export of a run whose consumer result and Parquet files this scenario does not model."""
    return {"tables": {}}, MANIFEST, ctx.path("export")


def prepare(folder: Path) -> tuple[Context, MemorySpark, dict[str, int], str]:
    """A run with candidates, a passing reconcile record and a read-back backup record: its context, catalog, snapshot set and backup record hash."""
    plan = publish_plan(MODE)
    bases = {name: 1000 + index for index, name in enumerate(sorted(plan.tables))}
    candidates = {name: snapshot + 1000 for name, snapshot in bases.items()}
    spark = MemorySpark(plan, bases, candidates)
    document = {
        "schema": 1,
        "run_id": RUN,
        "mode": MODE,
        "namespace": plan.namespaces[MODE],
        "branch": branch_name(plan.branch_prefix, RUN),
        "sealed_sha256": SEALED,
        "export_manifest_sha256": MANIFEST,
        "bases": bases,
        "tables": {name: {"candidate_snapshot_id": candidates[name], "rows": 1, "base_snapshot_id": bases[name]} for name in sorted(plan.tables)},
        "complete": True,
    }
    candidates_sha = write_record(folder / "candidates.json", document)
    reconcile_sha = write_record(folder / "reconcile.json", {"schema": 1, "run_id": RUN, "candidates_sha256": candidates_sha, "tables": {}, "pass": True})
    backup = {"schema": 1, "run_id": RUN, "reconcile_sha256": reconcile_sha, "name": backup_name(RUN), "backup_sha256": BACKUP_DATA, "read_back": True}
    backup_sha = write_record(folder / "backup.json", backup)
    ctx = Context(spark, plan, RUN, MODE, folder, SEALED, accepted_result_sha256=ACCEPTED)
    return ctx, spark, candidates, backup_sha


def interrupted(root: Path, name: str) -> tuple[Context, MemorySpark, dict[str, int], str]:
    """A run whose commit stopped after the ``publish_runs`` row and before ``commit.json``."""
    ctx, spark, candidates, backup_sha = prepare(root / name)
    stopped = attempt(lambda: commit_publish(replace(ctx, faults=Faults(frozenset({POINT})))))
    if not isinstance(stopped, InjectedFault):
        raise AssertionError(f"the commit did not stop at {POINT}: {stopped!r}")
    return ctx, spark, candidates, backup_sha


def refused_without_receipt(root: Path, field: str, value: str) -> bool:
    """True when a retried commit refuses a catalog row whose ``field`` differs from the run's records and writes no receipt."""
    ctx, spark, _, _ = interrupted(root, f"conflict_{field}")
    spark.change_run(field, value)
    error = attempt(lambda: commit_publish(ctx))
    return isinstance(error, PublishError) and not ctx.path("commit.json").exists()


def receipt_recovery() -> dict[str, bool]:
    """After an interruption behind the ``publish_runs`` row, a retried commit restores the receipt and the silver recorder accepts the run."""
    checks: dict[str, bool] = {}
    with tempfile.TemporaryDirectory() as raw, mock.patch.object(gold_publish, "verified_export", export_substitute):
        root = Path(raw)
        whole_ctx, _, _, _ = prepare(root / "whole")
        commit_publish(whole_ctx)
        want = (root / "whole/commit.json").read_bytes()

        ctx, spark, candidates, backup_sha = interrupted(root, "retry")
        receipt = ctx.path("commit.json")
        checks["the interruption leaves one catalog row and no receipt"] = len(spark.runs) == 1 and not receipt.exists()
        retried = attempt(lambda: commit_publish(ctx))
        checks["the retry succeeds and reports the run as committed"] = retried is None
        checks["the retry restores the receipt of an uninterrupted commit"] = receipt.exists() and receipt.read_bytes() == want
        checks["the retry keeps one catalog row"] = len(spark.runs) == 1
        recorded = ctx.path("silver_snapshot.json")
        first = attempt(lambda: record_committed(ctx, recorded))
        checks["the silver snapshot is recorded after the retry"] = first is None and recorded.exists()
        if recorded.exists() and receipt.exists():
            snapshot_set = read_record(recorded)[0].get("snapshot_set")
            checks["the record, the receipt and the catalog row name identical snapshots"] = (
                snapshot_set == read_record(receipt)[0]["snapshot_set"] == candidates == gold_publish.published_runs(ctx)[0]["snapshot_set"]
            )
            checks["the recovered receipt names the backup record hash"] = read_record(receipt)[0].get("backup_sha256") == backup_sha
            before = recorded.read_bytes()
            again = attempt(lambda: record_committed(ctx, recorded))
            checks["recording twice leaves identical bytes"] = again is None and recorded.read_bytes() == before
        else:
            checks["the record, the receipt and the catalog row name identical snapshots"] = False
            checks["the recovered receipt names the backup record hash"] = False
            checks["recording twice leaves identical bytes"] = False
        retried_again = attempt(lambda: commit_publish(ctx))
        checks["a further retry leaves the receipt unchanged"] = retried_again is None and receipt.exists() and receipt.read_bytes() == want

        for field, value in CONFLICTS.items():
            checks[f"a row with another {field} is refused and gets no receipt"] = refused_without_receipt(root, field, value)

        stale_ctx, _, _, _ = interrupted(root, "stale_receipt")
        stale = write_record(
            stale_ctx.path("commit.json"), {"schema": 1, "run_id": RUN, "backup_sha256": BACKUP_DATA, "snapshot_set": {"x": 1}, "state": "committed"}
        )
        error = attempt(lambda: commit_publish(stale_ctx))
        checks["a receipt that records another commit is refused and keeps its bytes"] = (
            isinstance(error, PublishError) and read_record(stale_ctx.path("commit.json"))[1] == stale
        )
    return checks


class Clock:
    """A controlled replacement for the ``time`` module of ``pipeline_control``: ``sleep`` advances ``monotonic``."""

    def __init__(self) -> None:
        self.now = 0.0

    def monotonic(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.now += seconds


def deadline_cases() -> dict[str, bool]:
    """A reply after the absolute deadline is refused, every request receives only the time left, and an on-time reply passes.

    The request checks run the real launcher client, see ``client_budget_cases``.
    """
    checks: dict[str, bool] = {}
    budget = pipeline_control.WAIT_SECONDS

    def ready(view: dict[str, Any]) -> bool:
        return view.get("phase") == "ready"

    clock = Clock()

    def late(remaining: float | None = None) -> dict[str, Any]:
        clock.now = budget + 1
        return {"phase": "ready"}

    with mock.patch.object(pipeline_control, "time", clock):
        error = attempt(lambda: pipeline_control.wait_for("network startup", late, ready))
    checks["readiness received after the deadline is refused naming the phase"] = (
        isinstance(error, ControlFailed) and "network startup" in str(error) and "deadline" in str(error)
    )

    clock, seen = Clock(), []

    def stalled(remaining: float | None = None) -> dict[str, Any]:
        seen.append(remaining)
        clock.now += min(remaining if remaining is not None else 120.0, 300.0)
        return {"phase": "starting"}

    with mock.patch.object(pipeline_control, "time", clock):
        error = attempt(lambda: pipeline_control.wait_for("network startup", stalled, ready))
    checks["a stalled request is given the time left, which shrinks"] = (
        len(seen) >= 2
        and seen[0] == budget
        and all(isinstance(left, float) and 0 < left <= budget for left in seen)
        and seen[1] is not None
        and seen[1] < seen[0]
    )
    checks["the stalled wait ends at the budget with an error naming the phase"] = (
        isinstance(error, ControlFailed) and "network startup" in str(error) and clock.now <= budget + 0.25
    )

    clock, seen = Clock(), []

    def on_time(remaining: float | None = None) -> dict[str, Any]:
        seen.append(remaining)
        clock.now += 10.0
        return {"phase": "ready" if len(seen) == 2 else "starting"}

    with mock.patch.object(pipeline_control, "time", clock):
        result = pipeline_control.wait_for("network startup", on_time, ready)
    checks["readiness before the deadline passes"] = result == {"phase": "ready"} and seen[0] == budget

    unavailable = LauncherUnavailable("the launcher is not reachable: OSError")

    def gone(remaining: float) -> dict[str, Any]:
        raise unavailable

    clock = Clock()
    with mock.patch.object(pipeline_control, "time", clock):
        error = attempt(lambda: pipeline_control.wait_for("network startup", gone, ready))
    checks["a launcher error before the deadline is raised unchanged"] = error is unavailable
    checks.update(client_budget_cases())
    return checks


STARTING = "the launcher is starting"


class Socket:
    """The connection that the substituted ``open_socket`` returns."""

    def close(self) -> None:
        pass


MIDDLE = 424242


class Wire:
    """The socket and process I/O of one ``LauncherClient`` replaced: connecting, starting, the exchange and the intermediate process cost controlled
    time and record what they were given.

    ``spends_timeout`` makes every connection attempt after a launcher start use up the timeout it is given before it fails. ``parent_wait`` is how long the
    intermediate process of a real ``start_launcher`` runs after the fork (``math.inf`` for one that never ends); it ends at once on a signal in
    ``dies_on``. With ``deny_signals`` every signal raises ``PermissionError`` and ends nothing.
    """

    def __init__(
        self,
        clock: Clock,
        reply: Callable[[int], dict[str, Any]],
        connect_cost: float = 0.0,
        exchange_cost: float = 0.0,
        listening: bool = True,
        *,
        spends_timeout: bool = False,
        parent_wait: float = 0.0,
        dies_on: tuple[signal.Signals, ...] = (signal.SIGTERM, signal.SIGKILL),
        deny_signals: bool = False,
    ) -> None:
        self.clock, self.reply, self.connect_cost, self.exchange_cost, self.listening = clock, reply, connect_cost, exchange_cost, listening
        self.spends_timeout, self.parent_wait, self.dies_on, self.deny_signals = spends_timeout, parent_wait, dies_on, deny_signals
        self.connects: list[float | None] = []
        self.exchanges: list[tuple[float, float]] = []
        self.starts = 0
        self.start_budgets: list[float | None] = []
        self.forks = 0
        self.waits: list[tuple[int, int]] = []
        self.signals: list[int] = []
        self.exit_at: float | None = None

    def open_socket(self, timeout: float | None = None) -> Socket:
        self.connects.append(timeout)
        self.clock.now += self.connect_cost
        if self.spends_timeout and (self.starts or self.forks) and timeout is not None:
            self.clock.now += timeout
        if not self.listening:
            raise ConnectionRefusedError("nothing listens")
        return Socket()

    def start_launcher(self, budget: float | None = None) -> None:
        self.starts += 1
        self.start_budgets.append(budget)

    def fork(self) -> int:
        """The fork of ``start_launcher``: the parent side only, and the launcher listens from here on."""
        self.forks += 1
        self.listening = True
        self.exit_at = self.clock.now + self.parent_wait
        return MIDDLE

    def waitpid(self, pid: int, options: int) -> tuple[int, int]:
        self.waits.append((pid, options))
        if self.exit_at is None:
            raise AssertionError("waitpid before any fork")
        if options == 0:
            if self.exit_at == math.inf:
                raise AssertionError("a blocking wait on a process that never ends")
            self.clock.now = max(self.clock.now, self.exit_at)
            return pid, 0
        return (pid, 0) if self.clock.now >= self.exit_at else (0, 0)

    def kill(self, pid: int, sent: int) -> None:
        if self.deny_signals:
            raise PermissionError("synthetic denied signal")
        self.signals.append(sent)
        if sent in self.dies_on and self.exit_at is not None:
            self.exit_at = min(self.exit_at, self.clock.now)

    def exchange(self, sock: Socket, method: str, params: dict[str, Any], principal: dict[str, Any], request_id: str, timeout: float) -> dict[str, Any]:
        self.exchanges.append((self.clock.now, timeout))
        if self.exchange_cost > timeout:
            self.clock.now += timeout
            raise TimeoutError(f"{method}: no complete reply within {timeout} s")
        self.clock.now += self.exchange_cost
        return self.reply(len(self.exchanges))


def refusing(number: int) -> dict[str, Any]:
    """A launcher that has just started and refuses every request."""
    raise launcher.ClientError(STARTING, "degraded")


def ready_reply(number: int) -> dict[str, Any]:
    return {"phase": "ready"}


def is_ready(view: dict[str, Any]) -> bool:
    return view.get("phase") == "ready"


def drive(wire: Wire, run: Callable[[Broker], Any], start: bool = False, real_start: bool = False) -> tuple[Any, BaseException | None]:
    """Run an action on the real ``Broker.call`` and ``LauncherClient`` with the wire and the clock of ``orchestration.launcher`` replaced.

    With ``real_start`` the client's own ``start_launcher`` runs its parent side: the registry path is a mock, ``os.fork``, ``os.waitpid`` and
    ``os.kill`` are the wire's, and ``os.posix_spawn`` raises, so no process starts.
    """
    registry = SimpleNamespace(path=mock.MagicMock(), kind="test")
    client = launcher.LauncherClient(cast("launcher.Config", SimpleNamespace(registry=registry, root="/synthetic")), start=start)
    broker = object.__new__(Broker)
    broker.client = client
    result: Any = None
    error: BaseException | None = None
    patches: list[tuple[Any, str, Any]] = [
        (launcher, "time", wire.clock),
        (pipeline_control, "time", wire.clock),
        (client, "open_socket", wire.open_socket),
        (client, "exchange", wire.exchange),
        (client, "log_tail", lambda: "no log"),
    ]
    if real_start:
        patches += [(launcher.os, "fork", wire.fork), (launcher.os, "waitpid", wire.waitpid), (launcher.os, "kill", wire.kill)]
    else:
        patches.append((client, "start_launcher", wire.start_launcher))
    with contextlib.ExitStack() as stack:
        for target, name, value in patches:
            stack.enter_context(mock.patch.object(target, name, value))
        if real_start:
            stack.enter_context(mock.patch.object(launcher.os, "posix_spawn", side_effect=AssertionError("no process may start")))
        try:
            result = run(broker)
        except Exception as caught:
            error = caught
    return result, error


def lifecycle_wait(broker: Broker) -> dict[str, Any]:
    """The network startup wait of the pipeline control, as it calls the broker."""
    return pipeline_control.wait_for("network startup", lambda left: broker.call("pipeline_network_up", {}, timeout=left), is_ready)


def client_budget_cases() -> dict[str, bool]:
    """One budget covers connecting, the startup poll, every exchange and every retry pause of a launcher request, and no budget changes nothing.

    The real ``Broker.call``, ``LauncherClient.call``, ``send`` and ``connect`` run; only the socket opening, the exchange, the launcher start and the
    clock are replaced, so no socket opens and no launcher starts.
    """
    checks: dict[str, bool] = {}
    lifecycle = pipeline_control.WAIT_SECONDS
    slack = 1e-9

    wire = Wire(Clock(), refusing)
    _, error = drive(wire, lambda broker: broker.call("pipeline_network_up", {}, timeout=1.0))
    checks["a one second request is over within one second while the launcher keeps refusing"] = (
        isinstance(error, LauncherUnavailable) and wire.clock.now <= 1.0 + slack
    )
    checks["each exchange is given at most the budget left, the first all of it"] = (
        len(wire.exchanges) > 1
        and wire.exchanges[0][1] == 1.0
        and all(timeout <= 1.0 - at + slack for at, timeout in wire.exchanges)
        and wire.exchanges[-1][1] < wire.exchanges[0][1]
    )
    checks["every connection is given at most the budget left"] = bool(wire.connects) and all(left is not None and 0 < left <= 1.0 for left in wire.connects)

    clock = Clock()

    def starting_late(number: int) -> dict[str, Any]:
        if number > 1:
            return refusing(number)
        clock.now = lifecycle - 0.5
        return {"phase": "starting"}

    wire = Wire(clock, starting_late)
    _, error = drive(wire, lifecycle_wait)
    checks["a lifecycle wait whose last request meets a starting launcher ends within its 600 seconds"] = clock.now <= lifecycle + slack
    checks["that end names the awaited phase and the deadline"] = isinstance(error, ControlFailed) and "network startup" in str(error) and "600" in str(error)
    checks["the last request is given only the time that is left"] = (
        len(wire.exchanges) > 1 and max(timeout for _, timeout in wire.exchanges[1:]) <= 0.25 + slack
    )

    wire = Wire(Clock(), ready_reply, connect_cost=lifecycle + 1)
    _, error = drive(wire, lifecycle_wait)
    checks["a connection that uses the whole budget ends the wait naming the phase before any exchange"] = (
        isinstance(error, ControlFailed) and "network startup" in str(error) and not wire.exchanges
    )
    checks["the connection is given the budget as its timeout"] = wire.connects == [lifecycle]

    wire = Wire(Clock(), ready_reply)
    _, error = drive(wire, lambda broker: broker.call("pipeline_network_up", {}, timeout=0.0))
    checks["a spent budget opens no connection and sends nothing"] = isinstance(error, LauncherUnavailable) and not wire.connects and not wire.exchanges

    wire = Wire(Clock(), ready_reply, exchange_cost=300.0)
    _, error = drive(wire, lambda broker: broker.call("pipeline_network_up", {}, timeout=10.0))
    checks["a stalled exchange is cut off at the budget"] = (
        isinstance(error, LauncherUnavailable) and wire.clock.now == 10.0 and wire.exchanges == [(0.0, 10.0)]
    )

    wire = Wire(Clock(), ready_reply, listening=False)
    _, error = drive(wire, lambda broker: broker.call("pipeline_network_up", {}, timeout=5.0), start=True)
    checks["the startup poll ends at the budget, not at its own 45 seconds"] = (
        isinstance(error, LauncherUnavailable) and wire.starts == 1 and wire.clock.now <= 5.0 + slack and not wire.exchanges
    )
    checks["every startup attempt is given at most the budget left"] = len(wire.connects) > 2 and all(
        left is not None and 0 < left <= 5.0 for left in wire.connects
    )
    checks["the launcher start is given the budget left"] = len(wire.start_budgets) == 1 and (
        wire.start_budgets[0] is not None and 0 < wire.start_budgets[0] <= 5.0
    )

    wire = Wire(Clock(), ready_reply, listening=False)
    _, error = drive(wire, lambda broker: broker.call("pipeline_network_up", {}), start=True)
    checks["without a budget the startup poll still lasts 45 seconds and sets no connection timeout"] = (
        isinstance(error, LauncherUnavailable)
        and "did not start" in str(error)
        and 45.0 <= wire.clock.now < 45.2
        and set(wire.connects) == {None}
        and wire.start_budgets == [None]
    )

    wire = Wire(Clock(), refusing)
    _, error = drive(wire, lambda broker: broker.call("pipeline_network_up", {}))
    checks["without a budget the startup refusals still last 60 seconds and every exchange gets the original timeout"] = (
        isinstance(error, LauncherUnavailable)
        and 60.0 <= wire.clock.now < 60.5
        and {timeout for _, timeout in wire.exchanges} == {120.0}
        and set(wire.connects) == {None}
    )

    wire = Wire(Clock(), ready_reply)
    result, error = drive(wire, lambda broker: broker.call("pipeline_network_up", {}, timeout=5.0))
    checks["a ready reply inside the budget is returned after one connection and one exchange"] = (
        error is None and result == {"phase": "ready"} and len(wire.connects) == 1 and len(wire.exchanges) == 1 and wire.clock.now == 0.0
    )

    clock = Clock()
    wire = Wire(clock, lambda number: refusing(number) if number < 3 else {"phase": "ready"})
    result, error = drive(wire, lambda broker: broker.call("pipeline_network_up", {}, timeout=5.0))
    checks["a launcher that finishes starting inside the budget is waited for"] = (
        error is None and result == {"phase": "ready"} and len(wire.exchanges) == 3 and clock.now < 5.0
    )

    def refused(number: int) -> dict[str, Any]:
        raise launcher.ClientError("no such template", "refused")

    wire = Wire(Clock(), refused)
    _, error = drive(wire, lambda broker: broker.call("pipeline_network_up", {}, timeout=5.0))
    checks["a definite refusal is raised unchanged and not retried"] = (
        isinstance(error, launcher.ClientError) and error.code == "refused" and len(wire.exchanges) == 1
    )

    def asked(budget: float | None) -> Callable[[Broker], Any]:
        return lambda broker: broker.call("pipeline_network_up", {}, timeout=budget)

    def said(error: BaseException | None) -> str:
        """The message of the raised error, as a caller or a displayed traceback shows it."""
        return str(error) if error is not None else ""

    survival = "may still be running"
    unreachable = "the launcher is not reachable: "

    wire = Wire(Clock(), ready_reply, listening=False, spends_timeout=True)
    _, error = drive(wire, asked(1.0), start=True)
    checks["a startup attempt that uses its whole timeout is not followed by a pause past a one second budget"] = (
        isinstance(error, LauncherUnavailable)
        and said(error) == f"{unreachable}TimeoutError"
        and wire.starts == 1
        and len(wire.connects) == 2
        and wire.clock.now <= 1.0 + slack
    )

    clock = Clock()

    def starting_then_gone(number: int) -> dict[str, Any]:
        clock.now = lifecycle - 20.0
        wire.listening = False
        return {"phase": "starting"}

    wire = Wire(clock, starting_then_gone, spends_timeout=True)
    _, error = drive(wire, lifecycle_wait, start=True)
    checks["a startup attempt that uses the last of the lifecycle budget ends the wait within its 600 seconds"] = clock.now <= lifecycle + slack
    checks["that end names the awaited phase"] = isinstance(error, ControlFailed) and "network startup" in str(error) and wire.starts == 1

    wire = Wire(Clock(), ready_reply, listening=False, parent_wait=2.0)
    _, error = drive(wire, asked(1.0), start=True, real_start=True)
    checks["a launcher start whose parent wait outlasts a one second budget ends within it, before any exchange"] = (
        isinstance(error, LauncherUnavailable) and wire.clock.now <= 1.0 + slack and not wire.exchanges and wire.forks == 1
    )
    checks["the budgeted parent wait never blocks"] = bool(wire.waits) and all(options == os.WNOHANG for _, options in wire.waits)
    checks["the intermediate process that outlasted the budget is terminated and reaped"] = (
        wire.signals == [signal.SIGTERM] and wire.exit_at is not None and wire.exit_at <= wire.clock.now
    )
    checks["the broker error names the process, the signal sent and the reap, and that a spawned launcher may still be running"] = (
        isinstance(error, LauncherUnavailable)
        and said(error).startswith(f"{unreachable}the budget ended while the launcher was being started")
        and f"intermediate process {MIDDLE} " in said(error)
        and "SIGTERM sent" in said(error)
        and "SIGKILL" not in said(error)
        and "ended and was reaped" in said(error)
        and "not reaped" not in said(error)
        and survival in said(error)
    )

    wire = Wire(Clock(), ready_reply, listening=False, parent_wait=2.0, dies_on=(signal.SIGKILL,))
    _, error = drive(wire, asked(1.0), start=True, real_start=True)
    checks["an intermediate process that ignores the first signal is killed within a further half second"] = (
        isinstance(error, LauncherUnavailable)
        and wire.signals == [signal.SIGTERM, signal.SIGKILL]
        and "SIGTERM sent, SIGKILL sent" in said(error)
        and "ended and was reaped" in said(error)
        and "not reaped" not in said(error)
        and survival in said(error)
        and wire.clock.now <= 1.0 + 0.5 + slack
    )

    wire = Wire(Clock(), ready_reply, listening=False, parent_wait=math.inf, dies_on=())
    _, error = drive(wire, asked(1.0), start=True, real_start=True)
    checks["an intermediate process that cannot be reaped is reported as not reaped, within a second of the budget"] = (
        isinstance(error, LauncherUnavailable) and wire.signals == [signal.SIGTERM, signal.SIGKILL] and wire.clock.now <= 1.0 + 1.0 + slack
    )
    checks["the broker error names that process and its unreaped state and that a spawned launcher may still be running"] = (
        isinstance(error, LauncherUnavailable)
        and f"intermediate process {MIDDLE} " in said(error)
        and "SIGTERM sent, SIGKILL sent" in said(error)
        and "0.5 s after the last signal and was not reaped" in said(error)
        and survival in said(error)
    )

    wire = Wire(Clock(), ready_reply, listening=False, parent_wait=math.inf, dies_on=())
    _, error = drive(wire, lambda broker: broker.client.call("pipeline_network_up", {}, budget=1.0), start=True, real_start=True)
    checks["the client's own timeout names the process, its unreaped state and that a spawned launcher may still be running"] = (
        isinstance(error, TimeoutError) and f"intermediate process {MIDDLE} " in said(error) and "was not reaped" in said(error) and survival in said(error)
    )

    wire = Wire(Clock(), ready_reply, listening=False, parent_wait=1.2, deny_signals=True)
    _, error = drive(wire, asked(1.0), start=True, real_start=True)
    checks["a denied signal is reported as not sent and the child that ended on its own as reaped"] = (
        isinstance(error, LauncherUnavailable)
        and "SIGTERM not sent (PermissionError)" in said(error)
        and "SIGTERM sent" not in said(error)
        and "ended and was reaped" in said(error)
        and "synthetic denied signal" not in said(error)
        and survival in said(error)
    )

    gone = launcher.LauncherClient(cast("launcher.Config", SimpleNamespace()), start=False)
    with (
        mock.patch.object(launcher, "time", Clock()),
        mock.patch.object(launcher.os, "kill", side_effect=ProcessLookupError("synthetic missing process")),
        mock.patch.object(launcher.os, "waitpid", side_effect=ChildProcessError("synthetic no child")),
    ):
        text = gone.discard_child(MIDDLE)
    checks["a child that is already gone is reported with the signal that could not be sent and that a spawned launcher may still be running"] = (
        f"intermediate process {MIDDLE} " in text
        and "SIGTERM not sent (ProcessLookupError)" in text
        and "no such child" in text
        and "synthetic" not in text
        and survival in text
    )

    clock = Clock()

    def starting_near_the_end(number: int) -> dict[str, Any]:
        clock.now = lifecycle - 0.5
        wire.listening = False
        return {"phase": "starting"}

    wire = Wire(clock, starting_near_the_end, parent_wait=math.inf, dies_on=())
    _, error = drive(wire, lifecycle_wait, start=True, real_start=True)
    checks["a lifecycle wait whose last launcher start leaves a child unreaped fails within a second of its 600 seconds"] = clock.now <= lifecycle + 1.0 + slack
    checks["that failure names the awaited phase, the process, its unreaped state and that a spawned launcher may still be running"] = (
        isinstance(error, ControlFailed)
        and "network startup" in str(error)
        and f"intermediate process {MIDDLE} " in str(error)
        and "was not reaped" in str(error)
        and survival in str(error)
    )

    wire = Wire(Clock(), ready_reply, listening=False, parent_wait=0.25)
    result, error = drive(wire, asked(1.0), start=True, real_start=True)
    checks["a launcher start inside the budget is waited for and the exchange gets the time left"] = (
        error is None
        and result == {"phase": "ready"}
        and not wire.signals
        and len(wire.exchanges) == 1
        and wire.exchanges[0][1] <= 0.75 + slack
        and wire.clock.now <= 0.25 + 0.005 + slack
    )

    wire = Wire(Clock(), ready_reply, listening=False, parent_wait=2.0)
    result, error = drive(wire, lambda broker: broker.call("pipeline_network_up", {}), start=True, real_start=True)
    checks["without a budget the launcher start still waits for its process without end and the exchange gets the original timeout"] = (
        error is None
        and result == {"phase": "ready"}
        and wire.waits == [(MIDDLE, 0)]
        and not wire.signals
        and [timeout for _, timeout in wire.exchanges] == [120.0]
        and set(wire.connects) == {None}
    )
    return checks
