"""E2E of ``transform_dag`` through the real scheduler and ``LocalExecutor`` on real Docker, started and stopped only through a test-registry launcher.

Run ``.venv/bin/python -m scripts.orchestration.run_transform_e2e [--case NAME ...]`` from the repository root with Docker Desktop running, the pinned
PostgreSQL image present and the Airflow image built (the command is in ``services/orchestration/Dockerfile``). It makes no Docker call. One launcher on a test
registry under a new ``/tmp`` folder starts PostgreSQL, the migration and the four Airflow services (the supervisor's session), and the supervisor's broker
listens on the loopback. The stack mounts a copy of the DAG files, so no test file enters ``dags/``. Each case triggers manual runs of ``transform_dag`` (or of
a variant that fails inside one task) through the Airflow REST API, with a token the runner signs with the session's JWT secret, and asserts the terminal DAG
run state, the task states, the build file and the lock state. The work steps run ``fixture_dbt`` jobs on a tiny dbt project seeded into each run folder; the
control rows, the intentional no-ops and the other rows are the shipped ones (``transform_fixtures``). Faults are real file system faults or an exception raised
at a named seam of the broker (``HookedBroker``), and the manual launches are made at ledger lines. A final step runs the sentinel scan over Airflow's logs, its
metadata database dump and XCom rows, the broker's files and the launcher's registry.

``--shipped`` runs the shipped fixture rows on the package fixture kit's seed. ``--mutants`` runs each mutant with its probe cases. ``--cleanup-check``
proves that a failed teardown or launcher shutdown, a folder that cannot be removed and a launcher process that outlives its lock or whose
start time cannot be read fail the session and keep what finishes it (``--cleanup-mutant`` runs its synthetic checks with an earlier behavior in place).
``--finish-cleanup FOLDER`` finishes the cleanup of a folder that such a run kept.

Every DAG run writes ``data/e2e/orchestration/<run_id>/report.json``, failed and blocked runs included; the session summary is
``data/e2e/orchestration/session_<UTC>/report.json``. A session that ends with a cleanup error fails and keeps its temporary folder; otherwise the folder is
removed.
"""

from __future__ import annotations

import argparse
import contextlib
import errno
import fcntl
import hashlib
import http.client
import io
import json
import os
import runpy
import secrets
import shutil
import socket
import stat
import sys
import tempfile
import threading
import time
import traceback
import types
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from unittest import mock
from urllib.parse import parse_qs

import scripts.orchestration as orchestration_package
import scripts.orchestration.airflow_api as airflow_api_module
from orchestration import launcher
from scripts.infrastructure import secret_store
from scripts.infrastructure.secret_store_stand_in import StandIn, stack_seeds
from scripts.lakehouse import run_lock
from scripts.orchestration import broker as broker_module
from scripts.orchestration import run_control, sentinel, transform_fixtures, trigger_dag
from scripts.orchestration import supervisor as supervisor_module
from scripts.orchestration.airflow_api import AirflowApi, AirflowApiError
from scripts.orchestration.broker import Broker, BrokerPaths, Listener, load_templates
from scripts.orchestration.ledger import Ledger
from scripts.orchestration.supervisor import Supervisor, SupervisorError
from scripts.process import run_command

ROOT = Path(__file__).resolve().parents[2]
TEMP_ROOT = Path(os.sep, "tmp")
OUT = ROOT / "data" / "e2e" / "orchestration"
PLATFORM = json.loads((ROOT / "config" / "orchestration" / "platform.json").read_text())
SENTINELS = {
    "AWS_SECRET_ACCESS_KEY": "sentinel-aws-" + secrets.token_hex(8),
    "POLARIS_CLIENT_SECRET": "sentinel-polaris-" + secrets.token_hex(8),
    "RUN_LOCK_TOKEN": "sentinel-runlock-" + secrets.token_hex(8),
}
DAG = "transform_dag"
GATE_FAULT_DAG = "transform_dag_gate_fault"
PRESUBMIT_DAG = "transform_dag_presubmit"
RETRY_DAG = "transform_dag_retry"
PROBE_DAG = "refused_probe"
STEPS = (
    "acquire_run",
    "catalog_up",
    "pin_bronze",
    "silver_build",
    "silver_validate",
    "gold_build",
    "gold_validate",
    "cross_layer_checks",
    "verify_bronze",
    "publish_branch",
    "skip_publish",
    "publish_candidates",
    "reconcile_candidates",
    "catalog_backup",
    "commit_publish",
    "run_record",
    "catalog_release",
    "final_gate",
    "release_run",
)
SKIPPED_STEPS = ("publish_candidates", "reconcile_candidates", "catalog_backup", "commit_publish")
SUCCESS_TASKS = {name: "skipped" if name in SKIPPED_STEPS else "success" for name in STEPS}
NO_OP_TEMPLATES = ("catalog_up", "pin_bronze", "verify_bronze", "publish_branch", "skip_publish", "catalog_release")
JOB_TEMPLATES = ("silver_build", "silver_validate", "gold_build", "gold_validate", "cross_layer_checks")
LIMITS = [
    "The work steps run a tiny dbt project, not the shipped silver and gold fixture: run_transform_broker_e2e checks the shipped rows against the launcher.",
    "No second device was available for the listener checks (the broker and supervisor runners cover them).",
    "No real build, catalog, bronze, AWS or S3; modes publish_fixture and real are never used.",
]
RUN_WAIT_SECONDS = 900
RECONCILE_JOIN_SECONDS = 120
MUTANT_WAIT_SECONDS = 240
DUMP_SECONDS = 900
ORIGINAL_WAIT = AirflowApi.wait_for_run
ORIGINAL_REQUEST = AirflowApi.request
RECOVERY_INPUTS = ("launcher_test.json", "registry.json", "registry_id")
LAUNCHER_PROCESS_FILE = "launcher_process.json"
LAUNCHER_WAIT_SECONDS = 60.0
EXITED, RUNNING, UNOBSERVED = "exited", "running", "unobserved"
START_ATTEMPTS = 3
SHIPPED_WAIT_SECONDS = 5400
DAG_IDS = (DAG, GATE_FAULT_DAG, PRESUBMIT_DAG, RETRY_DAG, PROBE_DAG)
LOGS = ROOT / "plans" / "wp" / "a2_logs" / "failing_first"
GATE_FAULT_EDIT = [("        return verdict\n", '        raise RuntimeError("test: the gate fails after it wrote its result")\n')]
PRESUBMIT_EDIT = [
    (
        "    run_id, mode, attempt = transform_run()\n",
        "    run_id, mode, attempt = transform_run()\n"
        '    if template == "gold_validate":\n'
        '        raise RuntimeError("test: gold_validate fails before it submits anything")\n',
    )
]
RETRY_EDIT = [
    (
        "retries=0, execution_timeout=STEP_TIMEOUT)\n    def run()",
        "retries=1, retry_delay=timedelta(seconds=15), execution_timeout=STEP_TIMEOUT)\n    def run()",
    )
]


@dataclass(frozen=True)
class Probe:
    case: str
    check: str
    completes: str = ""


@dataclass(frozen=True)
class Mutant:
    what: str
    edits: tuple[tuple[str, str], ...]
    probes: tuple[Probe, ...]
    base: str = DAG
    module: str = ""
    patch: tuple[str, ...] = ()


HIDE_FAILURES = (
    (
        '    if status.state not in PASSING:\n        raise RuntimeError(f"{template} ended {status.state.value} with exit code {status.exit_code}")\n',
        "",
    ),
)
GATE_ACCEPTS = (('        verdict = "rejected" if problems else "accepted"\n', '        verdict = "accepted"\n'),)
RELEASE_IGNORES_GATE_TASK = (
    (
        '        verdict = get_current_context()["ti"].xcom_pull(task_ids="final_gate") if gate_state == "success" else None\n'
        '        accepted = gate_state == "success" and verdict == "accepted"\n',
        '        verdict = "accepted"\n        accepted = verdict == "accepted"\n',
    ),
)
RUN_CONTROL = "scripts/orchestration/run_control.py"
SUPERVISOR = "scripts/orchestration/supervisor.py"
BROKER_FILE = "scripts/orchestration/broker.py"
AIRFLOW_API = "scripts/orchestration/airflow_api.py"
MUTANTS: dict[str, Mutant] = {
    "release_ignores_gate_task_state": Mutant(
        "release_run takes the accepted path although the gate task failed",
        RELEASE_IGNORES_GATE_TASK,
        (Probe("gate_fails_after_accepted", "the lock is failed_retained"),),
        GATE_FAULT_DAG,
    ),
    "release_ignores_unrecorded_gate": Mutant(
        "release_run asks for the accepted release although the gate task failed before it wrote a result",
        RELEASE_IGNORES_GATE_TASK,
        (Probe("record_path_blocked", "the lock is failed_retained"),),
    ),
    "gate_always_accepts": Mutant(
        "final_gate writes `accepted` whatever the predecessors' states",
        GATE_ACCEPTS,
        (
            Probe("work_task_fails", "the gate rejected the run and names the failed submission"),
            Probe("acquire_run_fails", "the gate rejected the run and recorded why"),
            Probe("run_record_fails", "the gate rejected the run"),
        ),
    ),
    "branch_never_skips": Mutant(
        "publish_branch follows the publish path in mode fixture",
        (('        return "skip_publish" if transform_run()[1] == "fixture" else "publish_candidates"\n', '        return "publish_candidates"\n'),),
        (Probe("fixture_graph_success", "every task ended as the plan expects"),),
    ),
    "failures_hidden": Mutant(
        "a step that ended failed is reported as a success",
        HIDE_FAILURES,
        (
            Probe("work_task_fails", "the DAG run failed"),
            Probe("catalog_up_fails", "the DAG run failed"),
            Probe("gold_test_fails", "gold_build succeeded and cross_layer_checks did not run"),
            Probe("cross_layer_fails", "the gold validation succeeded and the branch did not run"),
        ),
    ),
    "gate_reason_dropped": Mutant(
        "the gate record names no reason for a run that never held the lock",
        (
            (
                '        else "lock not acquired"\n        if state is None\n        else "the DAG run\'s task states',
                "        else \"the DAG run's task states",
            ),
        ),
        (
            Probe("acquire_run_fails", "the gate rejected the run and recorded why"),
            Probe("concurrent_second_run", "the gate rejected run B with `lock not acquired`"),
        ),
        module=RUN_CONTROL,
    ),
    "deletion_error_swallowed": Mutant(
        "a build file that cannot be deleted does not stop the release",
        (
            (
                "        if target.is_symlink() or target.is_file():\n            target.unlink()\n",
                "        if target.is_symlink() or target.is_file():\n            with contextlib.suppress(OSError):\n                target.unlink()\n",
            ),
            ("import json\n", "import contextlib\nimport json\n"),
        ),
        (Probe("release_deletion_fails", "the first DAG run failed at release_run only"),),
        module=RUN_CONTROL,
    ),
    "release_refusal_ignored": Mutant(
        "a launcher refusal of the release is not an error",
        (('            raise ControlFailed(f"the launcher refused the release: {error} [{error.code}]") from None', "            pass"),),
        (Probe("release_lock_fails", "the first DAG run failed at release_run only"),),
        module=RUN_CONTROL,
    ),
    "interrupted_control_failed_closed": Mutant(
        "a control intent that survived a crash is failed closed instead of repeated",
        (
            (
                "        self.perform_control(request, row)\n\n    def perform_control(",
                "        self.fail_changed_row(request)\n\n    def perform_control(",
            ),
        ),
        (Probe("release_interrupted", "the reconciliation finished the submission and released the lock"),),
        module=BROKER_FILE,
        patch=("Broker.complete_control",),
    ),
    "held_not_reconciled": Mutant(
        "the reconciliation looks only at finalizing runs",
        (('held["state"] not in ("held", "finalizing")', 'held["state"] != "finalizing"'),),
        (Probe("dag_run_marked_failed", "the reconciliation reconciled exactly this run"),),
        module=SUPERVISOR,
        patch=("Supervisor.reconcile_runs",),
    ),
    "finalizing_not_reconciled": Mutant(
        "the reconciliation looks only at held runs",
        (('held["state"] not in ("held", "finalizing")', 'held["state"] != "held"'),),
        (Probe("dag_run_marked_failed_while_finalizing", "the reconciliation reconciled exactly this run"),),
        module=SUPERVISOR,
        patch=("Supervisor.reconcile_runs",),
    ),
    "accepted_finalization_retained": Mutant(
        "the reconciliation retains a finalizing run whose gate result is accepted",
        (('        elif held["state"] == "finalizing" and self.release_pending(run_id, mode, snapshot["tasks"], outcome):\n            pass\n', ""),),
        (Probe("release_deletion_fails", "the reconciliation does not retain the accepted finalization"),),
        module=SUPERVISOR,
        patch=("Supervisor.reconcile_runs",),
    ),
    "accepted_record_without_gate_task": Mutant(
        "the reconciliation leaves a finalizing run on its accepted record whatever the state of the gate task",
        (('        if gate_state == "success":\n', "        if True:\n"),),
        (Probe("dag_run_marked_failed_after_accepted", "the reconciliation reconciled exactly this run"),),
        module=SUPERVISOR,
        patch=("Supervisor.release_pending",),
    ),
    "gate_record_without_task_states": Mutant(
        "the gate writes no task states into the run record",
        (('    evidence = task_state_evidence(params["expected_states"], snapshot)\n', '    evidence = {"status": "unavailable", "reason": "not read"}\n'),),
        (
            Probe("task_fails_before_submission", "the gate record names gold_validate with its expected and observed state"),
            Probe("fixture_graph_success", "the gate record holds the DAG run's task states"),
        ),
        module=RUN_CONTROL,
        patch=("gate",),
    ),
    "reconcile_ignores_live": Mutant(
        "the reconciliation retains a run whose DAG run is still live",
        (('        elif snapshot["dag_run_state"] in LIVE_RUN_STATES:', "        elif False:"),),
        (Probe("nested_manual_launch", "the reconciliation leaves a run whose DAG run is live", "the probe's reconciliation call completed"),),
        module=SUPERVISOR,
        patch=("Supervisor.reconcile_runs",),
    ),
    "runs_first_page_only": Mutant(
        "the DAG run listing is read from its first page and taken as complete",
        (
            ("            if len(found) >= total:\n                break\n", "            break\n"),
            ("        if len(found) != total or len({item.get(", "        if False and len({item.get("),
        ),
        (Probe("task_fails_before_submission", "beyond page 1"),),
        module=AIRFLOW_API,
        patch=("AirflowApi.runs",),
    ),
    "snapshot_without_deadline": Mutant(
        "the API client ignores the read's deadline and each request waits for its own timeout",
        (("        self.deadline = deadline\n", "        self.deadline = None\n"),),
        (Probe("snapshot_deadline", "a slow listing stops at the deadline"),),
        module=AIRFLOW_API,
        patch=("AirflowApi.__init__",),
    ),
    "snapshot_deadline_only_before_a_request": Mutant(
        "the API client checks the deadline only before a request, so a response that keeps arriving runs past it",
        (
            (
                "threading.Timer(max(self.deadline - time.monotonic(), 0.0), abort_connection, (connection.sock, expired))",
                "threading.Timer(3600, abort_connection, (connection.sock, expired))",
            ),
        ),
        (Probe("snapshot_deadline", "a response body that keeps arriving in small groups stops at the deadline"),),
        module=AIRFLOW_API,
        patch=("AirflowApi.request",),
    ),
    "request_schema_off": Mutant(
        "the broker does not check a request body against the request schema",
        (("        errors = sorted(broker.requests_schema.iter_errors(payload), key=str)\n", "        errors = []\n"),),
        (Probe("refused_specifications", "the probe task saw 400 for every refused specification and 200 for the control"),),
        module=BROKER_FILE,
        patch=("dispatch",),
    ),
}


DAG_MUTANTS = {name: mutant for name, mutant in MUTANTS.items() if not mutant.module}
HOST_MODULES = {RUN_CONTROL: run_control, SUPERVISOR: supervisor_module, BROKER_FILE: broker_module, AIRFLOW_API: airflow_api_module}


@contextlib.contextmanager
def installed(mutant: Mutant) -> Iterator[None]:
    """Put a host-file mutant in place for the length of the block: a copy of the file's text with the edits made, run as a module of its own. The whole
    module replaces the original, or only the named ``Class.method`` members replace the original class's, with the module's other names taken from the
    original so that exceptions and constants are the same objects."""
    path = ROOT / mutant.module
    source = path.read_text()
    for old, new in mutant.edits:
        if source.count(old) != 1:
            raise SystemExit(f"mutant edit does not match {mutant.module}: {old[:60]!r} occurs {source.count(old)} times, not once")
        source = source.replace(old, new)
    original = HOST_MODULES[mutant.module]
    with tempfile.TemporaryDirectory(prefix="a2_mutant_", dir=TEMP_ROOT) as folder, contextlib.ExitStack() as stack:
        copy = Path(folder) / path.name
        copy.write_text(source)
        namespace = runpy.run_path(str(copy), run_name=f"{original.__name__}_mutant")
        if not mutant.patch:
            module = types.ModuleType(original.__name__)
            module.__dict__.update(namespace)
            stack.enter_context(mock.patch.object(orchestration_package, original.__name__.rpartition(".")[2], module))
            stack.enter_context(mock.patch.dict(sys.modules, {original.__name__: module}))
        else:
            replacements = {
                member: namespace[member.split(".")[0]].__dict__[member.split(".")[1]] if "." in member else namespace[member] for member in mutant.patch
            }
            for function in replacements.values():
                function.__globals__.update({key: value for key, value in vars(original).items() if not key.startswith("__")})
            for member, function in replacements.items():
                owner, _, attribute = member.rpartition(".")
                stack.enter_context(mock.patch.object(getattr(original, owner) if owner else original, attribute or member, function))
        yield


class InjectedInterruption(Exception):
    pass


def interrupt(message: str) -> Callable[[], None]:

    def raise_it() -> None:
        raise InjectedInterruption(message)

    return raise_it


class Reconciliation:
    def __init__(self) -> None:
        self.outcome: dict[str, Any] | None = None
        self.error = ""
        self.thread: threading.Thread | None = None

    def start(self, supervisor: Supervisor) -> None:

        def run() -> None:
            try:
                self.outcome = supervisor.reconcile_runs()
            except Exception as error:
                self.error = f"{type(error).__name__}: {error}"

        self.thread = threading.Thread(target=run, name="probe-reconcile", daemon=True)
        self.thread.start()

    def join(self, seconds: float) -> dict[str, Any] | None:
        if self.thread is not None:
            self.thread.join(seconds)
        return self.outcome if self.thread is not None and not self.thread.is_alive() and not self.error else None


class Hooks:
    def __init__(self) -> None:
        self.lines: list[tuple[Callable[[dict[str, Any]], bool], Callable[[dict[str, Any]], None]]] = []
        self.points: dict[str, Callable[[], None]] = {}
        self.lock = threading.Lock()

    def reset(self) -> None:
        with self.lock:
            self.lines.clear()
            self.points.clear()

    def on_line(self, template: str, event: str, action: Callable[[dict[str, Any]], None], *, attempt: int | None = None, failed: bool | None = None) -> None:

        def matches(line: dict[str, Any]) -> bool:
            if line["template"] != template or line["event"] != event or (attempt is not None and line["attempt"] != attempt):
                return False
            return failed is None or (line.get("exit_code", 0) != 0) == failed

        with self.lock:
            self.lines.append((matches, action))

    def at(self, point: str, action: Callable[[], None]) -> None:
        with self.lock:
            self.points[point] = action

    def fire_line(self, line: dict[str, Any]) -> None:
        with self.lock:
            found = next((item for item in self.lines if item[0](line)), None)
            if found is not None:
                self.lines.remove(found)
        if found is not None:
            found[1](line)

    def fire_point(self, point: str) -> None:
        with self.lock:
            action = self.points.pop(point, None)
        if action is not None:
            action()


HOOKS = Hooks()


class HookedBroker(Broker):
    def append(self, line: dict[str, Any]) -> dict[str, Any]:
        written = super().append(line)
        HOOKS.fire_line(written)
        return written

    def crash(self, point: str) -> None:
        HOOKS.fire_point(point)
        super().crash(point)


def free_port() -> int:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return int(probe.getsockname()[1])


def new_run_id() -> str:
    return f"{datetime.now(UTC):%Y%m%dT%H%M%SZ}-{secrets.token_hex(4)}"


class Stack:
    def __init__(self, work: Path) -> None:
        self.work = work
        self.registry = work / "registry"
        self.checkout = work / "checkout"
        self.seeds = work / "seeds"
        self.packages = work / "packages"
        self.runs_root = work / "runs"
        self.state = work / "airflow"
        self.shipped = False
        self.keep_work = False
        self.dag_map: dict[str, str] = {}
        self.evidence: dict[str, Any] = {}
        self.observed: dict[str, dict[str, Any]] = {}
        self.seen_runs: set[str] = set()
        self.launcher_process: dict[str, str] = {}
        self.client: launcher.LauncherClient
        self.supervisor: Supervisor
        self.api: AirflowApi
        self.config: launcher.Config

    def start(self) -> None:
        self.registry.mkdir(mode=0o700)
        for folder in (self.seeds, self.packages, self.runs_root):
            folder.mkdir()
        self.stand_in = StandIn(stack_seeds())
        self.stand_in.__enter__()
        settings = {
            "secrets_endpoint": self.stand_in.endpoint,
            "docker_socket": launcher.docker_socket_path(),
            "timings": {"health_wait": 300.0, "stop_grace": 30, "docker_call_timeout": 120.0},
            "allowed_roots": [str(self.state), str(self.runs_root), str(self.packages)],
            "token_root": str(self.registry / "run_tokens" / "t"),
            "runs_root": str(self.runs_root),
        }
        (self.registry / "launcher_test.json").write_text(json.dumps(settings, indent=2))
        extra = {
            "transform_dag_gate_fault.py": transform_fixtures.dag_variant(GATE_FAULT_DAG, GATE_FAULT_EDIT),
            "transform_dag_presubmit.py": transform_fixtures.dag_variant(PRESUBMIT_DAG, PRESUBMIT_EDIT),
            "transform_dag_retry.py": transform_fixtures.dag_variant(RETRY_DAG, RETRY_EDIT),
            "refused_probe.py": PROBE_DAG_TEXT,
            **{
                f"mutant_{name}.py": transform_fixtures.dag_variant(
                    f"mutant_{name}", [*(GATE_FAULT_EDIT if mutant.base == GATE_FAULT_DAG else []), *mutant.edits]
                )
                for name, mutant in DAG_MUTANTS.items()
            },
        }
        self.prepare_checkout(extra)
        self.write_inventory(self.base_inventory())
        os.environ.update(SENTINELS)
        self.config = launcher.resolve_config(str(self.registry))
        self.client = launcher.LauncherClient(self.config, start=True)
        self.client.launch_nonce = secrets.token_hex(6)
        self.client.connect().close()
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline and self.client.call("status", {"what": "launcher"})["status"] != "serving":
            time.sleep(0.5)
        view = self.client.call("status", {"what": "launcher"})
        self.evidence["launcher"] = {key: view[key] for key in ("status", "degraded_reason", "foreign_legacy_containers")}
        if view["status"] != "serving":
            raise SupervisorError(f"the launcher does not serve: {view['status']} {view['degraded_reason']}")
        self.launcher_process = record_launcher_process(self.client, self.registry, self.work)
        self.evidence["plan"] = {key: value for key, value in self.client.call("plan").items() if key in ("free", "threads", "total")}
        paths = BrokerPaths(run_lock.Registry(self.registry, "test"), self.work)
        started = time.monotonic()
        for attempt in range(1, START_ATTEMPTS + 1):
            self.supervisor = Supervisor(
                paths,
                PLATFORM,
                self.client,
                api_port=free_port(),
                broker_port=0,
                readiness_seconds=480,
                templates_file=self.work / "inventory.json",
                repo_root=self.checkout,
            )
            try:
                with mock.patch.object(supervisor_module, "Broker", HookedBroker):
                    self.evidence["start"] = {**self.supervisor.start(), "seconds": round(time.monotonic() - started, 1), "attempts": attempt}
                break
            except SupervisorError as error:
                self.evidence.setdefault("failed_starts", []).append(str(error)[:300])
                if "postgres ended" not in str(error) or attempt == START_ATTEMPTS:
                    raise
                shutil.rmtree(self.state / "postgres", ignore_errors=True)
        self.api = AirflowApi(self.supervisor.api_port, self.supervisor.jwt_secret)

    def prepare_checkout(self, extra: dict[str, str]) -> None:
        """Prepare the isolated combined-DAG regression checkout."""
        transform_fixtures.prepare_checkout(self.checkout, extra)

    def base_inventory(self) -> dict[str, Any]:
        if self.shipped:
            shipped: dict[str, Any] = json.loads(transform_fixtures.TEMPLATES_FILE.read_text())
            return shipped
        return transform_fixtures.test_inventory(self.seeds, self.packages)

    def seed_shipped_fixture(self) -> dict[str, Any]:
        from scripts.lakehouse import package_fixture

        if "shipped_seed" in self.evidence:
            return dict(self.evidence["shipped_seed"])
        package_fixture.TEST_REGISTRY = str(self.registry)
        started = time.monotonic()
        with package_fixture.reservation() as session:
            case = package_fixture.make_case("transform_seed", session, [])
        data = self.checkout / "data"
        shutil.copytree(case.folder, data / "orchestration" / "fixture_seed", ignore=shutil.ignore_patterns("target", "logs"))
        shutil.copytree(package_fixture.packages_from_main(), data / "analytics" / "dbt" / "dbt_packages")
        for contracts in ("great_expectations", "lakehouse"):
            shutil.copytree(ROOT / "data_contracts" / contracts, self.checkout / "data_contracts" / contracts, dirs_exist_ok=True)
        shutil.rmtree(case.folder, ignore_errors=True)
        return {"seed_seconds": round(time.monotonic() - started, 1)}

    def write_inventory(self, document: dict[str, Any]) -> Path:
        path = self.work / "inventory.json"
        path.write_text(json.dumps(document, indent=2))
        return path

    def swap_inventory(self, document: dict[str, Any]) -> None:
        supervisor = self.supervisor
        if supervisor.listener is None or supervisor.broker is None:
            raise SupervisorError("the session has no broker")
        port = supervisor.listener.port
        supervisor.listener.stop()
        supervisor.broker.close()
        broker = HookedBroker(
            supervisor.paths, load_templates(self.write_inventory(document)), supervisor.client, repo_root=self.checkout, dag_snapshot=supervisor.dag_snapshot
        )
        broker.start()
        listener = Listener(broker, "127.0.0.1", port, supervisor.token)
        listener.start()
        supervisor.broker, supervisor.listener = broker, listener

    def wait_for_dags(self, dag_ids: tuple[str, ...], seconds: float = 240) -> dict[str, Any]:
        deadline = time.monotonic() + seconds
        while time.monotonic() < deadline:
            if all(self.api.dag(dag_id) is not None for dag_id in dag_ids):
                break
            time.sleep(3)
        errors = {Path(item["filename"]).name: str(item["stack_trace"])[-300:] for item in self.api.import_errors()}
        present = {dag_id: self.api.dag(dag_id) is not None for dag_id in dag_ids}
        for dag_id, here in present.items():
            if here:
                self.api.unpause(dag_id)
        return {"present": present, "import_errors": errors}

    def registry_view(self) -> dict[str, Any]:
        view: dict[str, Any] = self.client.call("status", {"what": "registry"})
        return view

    def lock_state(self, run_id: str) -> str | None:
        run = self.registry_view()["runs"].get(run_id)
        return None if run is None else str(run["state"])

    def run_folder(self, run_id: str) -> Path:
        return self.runs_root / run_id

    def build_exists(self, run_id: str) -> bool:
        return (self.run_folder(run_id) / "staging.duckdb").exists()

    def record_file(self, run_id: str) -> Path:
        return self.checkout / "data" / "e2e" / "gold" / run_id / "run.json"

    def record(self, run_id: str) -> dict[str, Any] | None:
        path = self.record_file(run_id)
        try:
            loaded: dict[str, Any] | None = json.loads(path.read_text()) if path.is_file() else None
        except PermissionError:
            return {"unreadable": True}
        return loaded

    def ledger(self, run_id: str) -> dict[str, list[str]]:
        found = Ledger(self.supervisor.paths.ledger).submissions(run_id)
        return {key.split(":", 1)[1]: [line["event"] for line in lines] for key, lines in found.items()}

    def submissions(self, run_id: str) -> dict[str, dict[str, Any]]:
        result = {}
        for submission_id, lines in Ledger(self.supervisor.paths.ledger).submissions(run_id).items():
            last = lines[-1]
            result[submission_id.split(":", 1)[1]] = {
                "events": [line["event"] for line in lines],
                "no_op": last.get("no_op"),
                "exit_code": last.get("exit_code"),
                "control": last.get("control"),
                "reason": last.get("reason"),
                "build_file": last.get("build_file"),
            }
        return result

    def observe(self, dag_id: str, run_id: str, started: float) -> dict[str, Any]:
        record = self.api.run(dag_id, run_id)
        return {
            "dag_id": dag_id,
            "run_id": run_id,
            "dag_run_state": record["state"],
            "conf": record.get("conf"),
            "tasks": self.api.task_states(dag_id, run_id),
            "lock_state": self.lock_state(run_id),
            "build_file_exists": self.build_exists(run_id),
            "run_record": self.record(run_id),
            "submissions": self.submissions(run_id),
            "seconds": round(time.monotonic() - started, 1),
        }

    def run(self, dag_id: str, run_id: str, overlay: str = "good", conf: dict[str, Any] | None = None, wait: float = RUN_WAIT_SECONDS) -> dict[str, Any]:
        dag_id = self.dag_map.get(dag_id, dag_id)
        started = time.monotonic()
        transform_fixtures.seed_run(self.seeds, run_id, overlay)
        self.api.trigger(dag_id, run_id, conf if conf is not None else {"run_id": run_id, "mode": "fixture"})
        with contextlib.suppress(AirflowApiError):
            self.api.wait_for_run(dag_id, run_id, wait)
        return self.remember(self.observe(dag_id, run_id, started))

    def remember(self, observation: dict[str, Any]) -> dict[str, Any]:
        self.observed[observation["run_id"]] = observation
        return observation

    def resume(self, dag_id: str, run_id: str, task_ids: list[str], seconds: float = 300) -> dict[str, Any]:
        dag_id = self.dag_map.get(dag_id, dag_id)
        started = time.monotonic()
        self.api.clear(dag_id, run_id, task_ids)
        time.sleep(5)
        deadline = time.monotonic() + seconds
        while time.monotonic() < deadline and not self.settled(dag_id, run_id):
            time.sleep(3)
        return self.remember(self.observe(dag_id, run_id, started))

    def settled(self, dag_id: str, run_id: str) -> bool:
        if self.api.run(dag_id, run_id)["state"] not in ("success", "failed"):
            return False
        return all(state in ("success", "failed", "skipped", "upstream_failed", "removed") for state in self.api.task_states(dag_id, run_id).values())

    def open_attempts_of(self, run_id: str) -> list[str]:
        registry = json.loads((self.registry / "registry.json").read_text())
        return [item["id"] for item in registry["attempts"].values() if item["owner"] == f"run:{run_id}" and item["phase"] != "ended"]

    def run_tokens(self) -> list[str]:
        broker = self.supervisor.broker
        tokens: list[str] = []
        for run_id in sorted(self.seen_runs):
            if broker is not None:
                with contextlib.suppress(Exception):
                    tokens.append(str(broker.principal(run_id)["token"]))
        return tokens

    def manual_launch(self, label: str) -> dict[str, Any]:
        other = new_run_id()
        arguments = {"domain": "transform", "run_id": other, "mode": "fixture", "holder": "manual", "checkout": str(self.checkout)}
        try:
            self.client.call("acquire", arguments)
        except launcher.ClientError as error:
            return {"moment": label, "refused": True, "code": error.code, "reason": str(error)[:160]}
        self.release_manual(other)
        return {"moment": label, "refused": False}

    def hold_manually(self) -> str:
        other = new_run_id()
        self.client.call("acquire", {"domain": "transform", "run_id": other, "mode": "fixture", "holder": "manual", "checkout": str(self.checkout)})
        return other

    @staticmethod
    def dag_ids() -> tuple[str, ...]:
        return (*DAG_IDS, *(f"mutant_{name}" for name in DAG_MUTANTS))

    def wait_idle(self, seconds: float = 300) -> list[str]:
        deadline = time.monotonic() + seconds
        live: list[str] = []
        while time.monotonic() < deadline:
            live = [item["dag_run_id"] for dag_id in self.dag_ids() for item in self.api.runs(dag_id, ("queued", "running"))]
            if not live:
                return []
            time.sleep(3)
        for dag_id in self.dag_ids():
            for item in self.api.runs(dag_id, ("queued", "running")):
                self.api.mark_failed(dag_id, item["dag_run_id"])
        return live

    def release_leftovers(self) -> list[str]:
        released = []
        for run_id, run in self.registry_view()["runs"].items():
            if run["state"] == "released":
                continue
            principal = {**self.client.principal_run(run_id), "owner": True}
            record = self.work / f"{run_id}_owner.json"
            record.write_text(json.dumps({"run_id": run_id}))
            if run["state"] == "held":
                self.client.call("finalize", {}, principal)
            self.client.call("release", {"record": str(record), "owner": True}, principal, timeout=300)
            released.append(run_id)
        return released

    def release_manual(self, run_id: str) -> None:
        principal = self.client.principal_run(run_id)
        record = self.work / f"{run_id}_manual.json"
        record.write_text(json.dumps({"run_id": run_id}))
        self.client.call("finalize", {}, principal)
        self.client.call("release", {"record": str(record)}, principal, timeout=300)

    def run_sql_on_build(self, run_id: str, sql: str) -> bool:
        folder = self.run_folder(run_id)
        (folder / "mutate.sql").write_text(sql)
        allocation = self.client.allocate()
        try:
            principal = self.client.principal_alloc(allocation)
            params = {
                "case_host": str(folder),
                "case_container": "/workspace/out/case",
                "database": "/workspace/out/case/staging.duckdb",
                "sql_file": "/workspace/out/case/mutate.sql",
            }
            outcome = self.client.run_attempt(principal, "fixture_init", f"mutate-{run_id[-8:]}", [], params, checkout=self.checkout, seconds=300)
        finally:
            self.client.release_allocation(allocation)
        return bool(outcome.ok)

    def write_reports(self, case: str, checks: dict[str, bool]) -> list[str]:
        written = []
        for run_id, observation in self.observed.items():
            folder = OUT / run_id
            folder.mkdir(parents=True, exist_ok=True)
            report = {
                "feature": "transform_dag through the real scheduler and LocalExecutor on a test-registry launcher",
                "case": case,
                "command": ".venv/bin/python -m scripts.orchestration.run_transform_e2e",
                "head": run_command("git", ["rev-parse", "HEAD"], cwd=ROOT).stdout.strip(),
                "observation": observation,
                "case_checks": checks,
                "limits": LIMITS,
            }
            text = json.dumps(report, indent=2, default=str)
            if any(value in text for value in SENTINELS.values()):
                report = {**report, "observation": "withheld: a planted credential is in the report", "case_checks": "withheld"}
                text = json.dumps(report, indent=2, default=str)
            (folder / "report.json").write_text(text + "\n")
            written.append(str(folder.relative_to(ROOT)))
        self.observed.clear()
        return written


Case = Callable[[Stack], dict[str, bool]]
CASES: dict[str, Case] = {}


def case(function: Case) -> Case:
    CASES[function.__name__] = function
    return function


def file_digest(path: Path) -> str | None:
    try:
        return hashlib.sha256(path.read_bytes()).hexdigest()
    except OSError:
        return None


def failed_run(observation: dict[str, Any], expected_failed: tuple[str, ...], lock_state: str | None, build_kept: bool | None) -> dict[str, bool]:
    tasks = observation["tasks"]
    checks = {
        "the DAG run failed": observation["dag_run_state"] == "failed",
        f"the failed tasks are exactly {list(expected_failed)}": sorted(name for name, state in tasks.items() if state == "failed") == sorted(expected_failed),
        "catalog_release, final_gate and release_run ran after the failure": all(
            tasks[name] in ("success", "failed") for name in ("catalog_release", "final_gate", "release_run")
        ),
        f"the lock is {lock_state}": observation["lock_state"] == lock_state,
    }
    if build_kept is not None:
        checks["the build file is kept" if build_kept else "the build file does not exist"] = observation["build_file_exists"] is build_kept
    return checks


def gate_evidence_matches(record: dict[str, Any], run_id: str) -> bool:
    states = (record.get("gate") or {}).get("task_states") or {}
    predecessors = states.get("predecessors") or []
    return (
        states.get("status") == "read"
        and states.get("dag_run_id") == run_id
        and states.get("failed_tasks") == []
        and sorted(item["task"] for item in predecessors) == sorted(name for name in STEPS if name not in ("final_gate", "release_run"))
        and all(item["observed"] == item["expected"] == SUCCESS_TASKS[item["task"]] for item in predecessors)
    )


def validator_reported(stack: Stack, run_id: str) -> str | None:
    try:
        reported = json.loads((stack.run_folder(run_id) / "quality_silver" / "result.json").read_text()).get("build_file_sha256")
    except (OSError, ValueError, AttributeError):
        return None
    return reported if isinstance(reported, str) else None


def submissions_of(observation: dict[str, Any], template: str) -> dict[str, Any]:
    found: dict[str, Any] = observation["submissions"].get(f"{template}:fixture:1", {"events": [], "no_op": None, "exit_code": None})
    return found


@case
def fixture_graph_success(stack: Stack) -> dict[str, bool]:
    run_id = new_run_id()
    seen: dict[str, Any] = {}
    HOOKS.at("after_finalize", lambda: seen.update(build_during_finalization=stack.build_exists(run_id), lock_during_finalization=stack.lock_state(run_id)))
    HOOKS.on_line(
        "silver_validate",
        "finished",
        lambda line: seen.update(
            validated=line.get("build_file"),
            digest=file_digest(stack.run_folder(run_id) / "staging.duckdb"),
            reported=validator_reported(stack, run_id),
        ),
    )
    observation = stack.run(DAG, run_id)
    observation["during_finalization"] = dict(seen)
    submissions: dict[str, dict[str, Any]] = {name: submissions_of(observation, name) for name in (*NO_OP_TEMPLATES, *JOB_TEMPLATES)}
    record = observation["run_record"] or {}
    return {
        "the DAG run succeeded": observation["dag_run_state"] == "success",
        "every task ended as the plan expects (steps 10 to 13 skipped by the branch)": observation["tasks"] == SUCCESS_TASKS,
        "the build file existed while the run was finalizing": seen.get("build_during_finalization") is True,
        "the lock was finalizing while the run was recorded": seen.get("lock_during_finalization") == "finalizing",
        "the build file is deleted": observation["build_file_exists"] is False,
        "the lock is released": observation["lock_state"] == "released",
        "the intentional no-ops are recorded as no-ops without a container": all(
            submissions[name].get("no_op") is True and "started" not in submissions[name]["events"] for name in NO_OP_TEMPLATES
        ),
        "steps 5 to 8 ran jobs (silver and gold validation and gold build are not no-ops)": all(
            submissions[name]["events"] == ["intent", "started", "finished"] and submissions[name]["exit_code"] == 0 for name in JOB_TEMPLATES
        ),
        "the run record names an accepted gate": (record.get("gate") or {}).get("result") == "accepted" and record.get("state") == "finalizing",
        "the gate record holds the DAG run's task states, each as expected": gate_evidence_matches(record, run_id),
        "silver_validate recorded the SHA-256 and the run of the file it validated": (seen.get("validated") or {}).get("sha256") == seen.get("digest")
        and (seen.get("validated") or {}).get("run_id") == run_id,
        "the recorded SHA-256 is the one the validator reported for the copy it read": len(str(seen.get("reported") or "")) == 64
        and (seen.get("validated") or {}).get("sha256") == seen.get("reported"),
        "gold_build started after the recorded file was checked": submissions["gold_build"]["events"] == ["intent", "started", "finished"],
    }


@case
def acquire_run_fails(stack: Stack) -> dict[str, bool]:
    holder = stack.hold_manually()
    run_id = new_run_id()
    try:
        observation = stack.run(DAG, run_id)
        holder_after = stack.lock_state(holder)
    finally:
        stack.release_manual(holder)
    gate = (observation["run_record"] or {}).get("gate") or {}
    return {
        **failed_run(observation, ("acquire_run", "release_run"), None, None),
        "the gate rejected the run and recorded why": gate.get("result") == "rejected" and gate.get("reason") == "lock not acquired",
        "no lock was taken for the run and the build file never existed": observation["lock_state"] is None and observation["build_file_exists"] is False,
        "the manual holder kept its lock": holder_after == "held",
        "the work tasks did not run": all(observation["tasks"][name] == "upstream_failed" for name in ("catalog_up", "silver_build", "run_record")),
        "release_run failed the DAG run": observation["tasks"]["release_run"] == "failed",
    }


@case
def catalog_up_fails(stack: Stack) -> dict[str, bool]:
    stack.swap_inventory(transform_fixtures.test_inventory(stack.seeds, stack.packages, catalog_job=True))
    run_id = new_run_id()
    observation = stack.run(DAG, run_id, "bad_catalog")
    gate = (observation["run_record"] or {}).get("gate") or {}
    return {
        **failed_run(observation, ("catalog_up", "release_run"), "failed_retained", False),
        "the gate rejected the run": gate.get("result") == "rejected",
        "the later steps did not run": observation["tasks"]["silver_build"] == "upstream_failed",
    }


@case
def work_task_fails(stack: Stack) -> dict[str, bool]:
    run_id = new_run_id()
    observation = stack.run(DAG, run_id, "bad_gold_build")
    gate = (observation["run_record"] or {}).get("gate") or {}
    return {
        **failed_run(observation, ("gold_build", "release_run"), "failed_retained", True),
        "the gate rejected the run and names the failed submission": gate.get("result") == "rejected"
        and any("gold_build" in item for item in gate.get("failed", [])),
        "the gold validation did not run": observation["tasks"]["gold_validate"] == "upstream_failed",
        "the run record was not written as finalizing": (observation["run_record"] or {}).get("state") != "finalizing",
    }


@case
def gold_test_fails(stack: Stack) -> dict[str, bool]:
    observation = stack.run(DAG, new_run_id(), "bad_gold_test")
    return {
        **failed_run(observation, ("gold_validate", "release_run"), "failed_retained", True),
        "gold_build succeeded and cross_layer_checks did not run": observation["tasks"]["gold_build"] == "success"
        and observation["tasks"]["cross_layer_checks"] == "upstream_failed",
    }


@case
def cross_layer_fails(stack: Stack) -> dict[str, bool]:
    observation = stack.run(DAG, new_run_id(), "bad_cross_layer")
    return {
        **failed_run(observation, ("cross_layer_checks", "release_run"), "failed_retained", True),
        "the gold validation succeeded and the branch did not run": observation["tasks"]["gold_validate"] == "success"
        and observation["tasks"]["publish_branch"] == "upstream_failed",
    }


@case
def run_record_fails(stack: Stack) -> dict[str, bool]:
    HOOKS.at("after_record", interrupt("test: run_record stops before the run is finalizing"))
    observation = stack.run(DAG, new_run_id())
    gate = (observation["run_record"] or {}).get("gate") or {}
    return {
        **failed_run(observation, ("run_record", "release_run"), "failed_retained", True),
        "the gate rejected the run": gate.get("result") == "rejected",
        "the work steps before run_record succeeded": all(observation["tasks"][name] == "success" for name in ("silver_build", "gold_build", "skip_publish")),
    }


@case
def record_path_blocked(stack: Stack) -> dict[str, bool]:
    run_id = new_run_id()
    blocker = stack.record_file(run_id).parent
    blocker.parent.mkdir(parents=True, exist_ok=True)
    blocker.write_text("not a folder\n")
    observation = stack.run(DAG, run_id)
    return failed_run(observation, ("run_record", "final_gate", "release_run"), "failed_retained", True)


@case
def gate_fails_after_accepted(stack: Stack) -> dict[str, bool]:
    run_id = new_run_id()
    observation = stack.run(GATE_FAULT_DAG, run_id)
    gate = (observation["run_record"] or {}).get("gate") or {}
    return {
        **failed_run(observation, ("final_gate", "release_run"), "failed_retained", True),
        "the record holds the accepted result the gate wrote before it failed": gate.get("result") == "accepted",
        "the release was the rejected one": "release_rejected:fixture:1" in observation["submissions"]
        and "release_accepted:fixture:1" not in observation["submissions"],
    }


def attempt_of(call: Callable[[], Any]) -> dict[str, Any]:
    try:
        found: dict[str, Any] = call()
    except Exception as error:
        return {"unavailable": f"the probe raised {type(error).__name__}: {error}"}
    return found


def refused_runs(*_: Any, **__: Any) -> Any:
    raise AirflowApiError("injected: the DAG runs cannot be listed")


def refused_task_states(*_: Any, **__: Any) -> Any:
    raise AirflowApiError("injected: the task states cannot be read")


def filler_runs(count: int) -> list[dict[str, Any]]:
    return [{"dag_run_id": f"filler_{index}", "state": "success", "conf": {"run_id": f"filler_{index}", "mode": "fixture"}} for index in range(count)]


def paged_runs(rows: list[dict[str, Any]], total: int | None = -1) -> Callable[..., Any]:
    """An API transport that answers the DAG run listing from ``rows`` in pages of at most 100 and every other request as the real API does. ``total`` is
    the ``total_entries`` it reports: the row count by default and no entry for None."""
    original = AirflowApi.request

    def request(self: AirflowApi, method: str, path: str, body: dict[str, Any] | None = None, expect: tuple[int, ...] = (200,)) -> Any:
        if method != "GET" or "/dagRuns?" not in path:
            return original(self, method, path, body, expect)
        query = parse_qs(path.partition("?")[2])
        offset, limit = int(query.get("offset", ["0"])[0]), min(int(query["limit"][0]), 100)
        page: dict[str, Any] = {"dag_runs": rows[offset : offset + limit]}
        if total is not None:
            page["total_entries"] = len(rows) if total < 0 else total
        return page

    return request


def truncated_task_list(self: AirflowApi, method: str, path: str, body: dict[str, Any] | None = None, expect: tuple[int, ...] = (200,)) -> Any:
    answer = ORIGINAL_REQUEST(self, method, path, body, expect)
    if "/taskInstances" in path:
        answer["total_entries"] = len(answer["task_instances"]) + 5
    return answer


@case
def task_fails_before_submission(stack: Stack) -> dict[str, bool]:
    run_id = new_run_id()
    with mock.patch.object(stack.supervisor, "transform_dag", PRESUBMIT_DAG):
        observation = stack.run(PRESUBMIT_DAG, run_id)
        gate = (observation["run_record"] or {}).get("gate") or {}
        states = gate.get("task_states") or {}
        by_task = {item["task"]: item for item in states.get("predecessors", [])}
        exact = attempt_of(lambda: stack.supervisor.dag_snapshot(run_id, "fixture"))
        other_mode = attempt_of(lambda: stack.supervisor.dag_snapshot(run_id, "publish_fixture"))
        listed = stack.api.runs(PRESUBMIT_DAG)
        with mock.patch.object(AirflowApi, "runs", lambda *_, **__: [*listed, {**listed[0], "dag_run_id": "a_second_dag_run"}]):
            ambiguous = attempt_of(lambda: stack.supervisor.dag_snapshot(run_id, "fixture"))
        with mock.patch.object(AirflowApi, "task_states", refused_task_states):
            unreadable = attempt_of(lambda: stack.supervisor.dag_snapshot(run_id, "fixture"))
        match = listed[0]
        pages = {
            "duplicate": paged_runs([match, *filler_runs(99), {**match, "dag_run_id": "a_second_dag_run"}]),
            "sole": paged_runs([*filler_runs(100), match]),
            "ends_early": paged_runs(filler_runs(100), total=150),
            "no_total": paged_runs([match], total=None),
            "shifted": paged_runs([match, *filler_runs(99), match]),
        }
        beyond = {}
        for name, transport in pages.items():
            with mock.patch.object(AirflowApi, "request", transport):
                beyond[name] = attempt_of(lambda: stack.supervisor.dag_snapshot(run_id, "fixture"))
        with mock.patch.object(AirflowApi, "request", truncated_task_list):
            truncated = attempt_of(lambda: stack.supervisor.dag_snapshot(run_id, "fixture"))
    return {
        **failed_run(observation, ("gold_validate", "release_run"), "failed_retained", True),
        "gold_validate submitted nothing": not any("gold_validate" in name for name in observation["submissions"]),
        "the gate record names gold_validate with its expected and observed state": by_task.get("gold_validate")
        == {"task": "gold_validate", "expected": "success", "observed": "failed"},
        "the gate record names the tasks that did not run and their states": by_task.get("gold_build", {}).get("observed") == "success"
        and by_task.get("cross_layer_checks", {}).get("observed") == "upstream_failed"
        and "gold_validate" in states.get("failed_tasks", [])
        and "gold_build" not in states.get("failed_tasks", []),
        "the gate rejected the run on the DAG run named for the transform run": gate.get("result") == "rejected"
        and states.get("status") == "read"
        and states.get("dag_run_id") == run_id,
        "one DAG run bound to the run and mode gives its task states": exact.get("dag_run_id") == run_id
        and (exact.get("tasks") or {}).get("gold_validate") == "failed",
        "another mode selects no DAG run and is unavailable": "0 pipeline DAG runs" in str(other_mode.get("unavailable")),
        "two DAG runs for the run and mode are unavailable, not a guess": "2 pipeline DAG runs" in str(ambiguous.get("unavailable")),
        "an unreadable API is unavailable, not a snapshot": "cannot be read" in str(unreadable.get("unavailable")) and "tasks" not in unreadable,
        "a duplicate match beyond page 1 is unavailable, not the visible match": "2 pipeline DAG runs" in str(beyond["duplicate"].get("unavailable"))
        and "tasks" not in beyond["duplicate"],
        "a sole match beyond page 1 is selected": beyond["sole"].get("dag_run_id") == run_id
        and (beyond["sole"].get("tasks") or {}).get("gold_validate") == "failed",
        "a listing that ends before its total is unavailable": "ended at 100 of 150" in str(beyond["ends_early"].get("unavailable")),
        "a listing with no total is unavailable": "no total" in str(beyond["no_total"].get("unavailable")),
        "a run listed twice is unavailable": "distinct runs" in str(beyond["shifted"].get("unavailable")),
        "a truncated task list is unavailable, not a snapshot": "incomplete" in str(truncated.get("unavailable")) and "tasks" not in truncated,
    }


@case
def gate_lookup_unavailable(stack: Stack) -> dict[str, bool]:
    with mock.patch.object(AirflowApi, "runs", refused_runs):
        observation = stack.run(DAG, new_run_id(), "bad_gold_build")
    gate = (observation["run_record"] or {}).get("gate") or {}
    states = gate.get("task_states") or {}
    return {
        **failed_run(observation, ("gold_build", "release_run"), "failed_retained", True),
        "the gate rejected the run whatever the lookup": gate.get("result") == "rejected" and any("gold_build" in item for item in gate.get("failed", [])),
        "the record states the lookup as unavailable with its reason": states.get("status") == "unavailable" and "cannot be read" in str(states.get("reason")),
        "the record holds no invented snapshot": "predecessors" not in states and "failed_tasks" not in states and "dag_run_id" not in states,
    }


def trickle_send(connection: socket.socket, data: bytes, release: threading.Event, ended: list[str]) -> None:
    for offset in range(0, len(data), 2):
        if release.wait(0.1):
            return
        try:
            connection.sendall(data[offset : offset + 2])
        except OSError:
            ended.append("the client closed the connection")
            return


@dataclass
class AirflowFake:
    rows: list[dict[str, Any]]
    delay: float
    stall: bool
    trickle: str
    served: list[float]
    ended: list[str]
    release: threading.Event


def answer_airflow(connection: socket.socket, fake: AirflowFake) -> None:
    rows, delay, stall, served, release = fake.rows, fake.delay, fake.stall, fake.served, fake.release
    with connection:
        try:
            request = b""
            while b"\r\n\r\n" not in request:
                chunk = connection.recv(65536)
                if not chunk:
                    return
                request += chunk
            path = request.split(b" ", 2)[1].decode()
            if "/taskInstances" in path:
                body: dict[str, Any] = {"task_instances": [{"task_id": "gold_validate", "state": "failed"}], "total_entries": 1}
            else:
                query = parse_qs(path.partition("?")[2])
                offset, limit = int(query["offset"][0]), int(query["limit"][0])
                served.append(time.monotonic())
                release.wait(60 if stall else delay)
                body = {"dag_runs": rows[offset : offset + limit], "total_entries": len(rows)}
            payload = json.dumps(body).encode()
            header = b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nConnection: close\r\nContent-Length: %d\r\n\r\n" % len(payload)
            if "/taskInstances" in path and fake.trickle == "headers":
                trickle_send(connection, header, release, fake.ended)
                connection.sendall(payload)
            elif "/taskInstances" in path and fake.trickle == "body":
                connection.sendall(header)
                trickle_send(connection, payload, release, fake.ended)
            else:
                connection.sendall(header + payload)
        except OSError:
            return


@contextlib.contextmanager
def slow_airflow(rows: list[dict[str, Any]], delay: float, stall: bool = False, trickle: str = "") -> Iterator[tuple[int, list[float], list[str]]]:
    """A loopback server that answers the DAG run listing from ``rows`` in pages of at most 100 after ``delay`` seconds (a stalled answer for every page
    when ``stall`` is set) and the task list of one failed task, which ``trickle`` ("headers" or "body") sends two bytes every 0.1 second. It yields its
    port, the times at which listing requests arrived and a note for each trickled response that the client closed."""
    served: list[float] = []
    ended: list[str] = []
    release, stopped = threading.Event(), threading.Event()
    fake = AirflowFake(rows, delay, stall, trickle, served, ended, release)
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen()
    listener.settimeout(0.2)

    def accept() -> None:
        while not stopped.is_set():
            try:
                connection, _ = listener.accept()
            except TimeoutError:
                continue
            threading.Thread(target=answer_airflow, args=(connection, fake), daemon=True).start()

    thread = threading.Thread(target=accept, daemon=True)
    thread.start()
    try:
        yield listener.getsockname()[1], served, ended
    finally:
        release.set()
        stopped.set()
        thread.join(10)
        listener.close()


@case
def snapshot_deadline(stack: Stack) -> dict[str, bool]:
    match = {"dag_run_id": "deadline_run", "state": "failed", "conf": {"run_id": "deadline_run", "mode": "fixture"}}
    rows = [*filler_runs(4999), match]

    def snapshot_from(port: int, seconds: float) -> tuple[dict[str, Any], float]:
        with mock.patch.object(supervisor_module, "SNAPSHOT_TOTAL_SECONDS", seconds), mock.patch.object(stack.supervisor, "api_port", port):
            began = time.monotonic()
            found = attempt_of(lambda: stack.supervisor.dag_snapshot("deadline_run", "fixture"))
            return found, time.monotonic() - began

    def closed_by_client(ended: list[str]) -> bool:
        deadline = time.monotonic() + 3
        while not ended and time.monotonic() < deadline:
            time.sleep(0.05)
        return bool(ended)

    with slow_airflow(rows, 0.5) as (port, served, _):
        slow, slow_seconds = snapshot_from(port, 3.0)
        slow_pages = len(served)
    with slow_airflow(rows, 0.0, stall=True) as (port, _, _):
        stalled, stalled_seconds = snapshot_from(port, 3.0)
    with slow_airflow([match], 0.0, trickle="body") as (port, _, ended):
        slow_body, slow_body_seconds = snapshot_from(port, 2.0)
        slow_body_closed = closed_by_client(ended)
    with slow_airflow([match], 0.0, trickle="headers") as (port, _, ended):
        slow_headers, slow_headers_seconds = snapshot_from(port, 2.0)
        slow_headers_closed = closed_by_client(ended)
    with slow_airflow(rows, 0.0) as (port, served, _):
        fast, fast_seconds = snapshot_from(port, 20.0)
        fast_pages = len(served)
    return {
        "a slow listing stops at the deadline and is unavailable, not read to its end": "deadline" in str(slow.get("unavailable"))
        and "tasks" not in slow
        and slow_pages < 10
        and slow_seconds < 8,
        "a stalled request ends at the deadline, not at its own 20-second timeout": "unavailable" in stalled
        and "tasks" not in stalled
        and stalled_seconds < 10,
        "a response body that keeps arriving in small groups stops at the deadline and is unavailable": "deadline" in str(slow_body.get("unavailable"))
        and "tasks" not in slow_body
        and slow_body_seconds < 3.5,
        "response headers that keep arriving in small groups stop at the deadline and are unavailable": "deadline" in str(slow_headers.get("unavailable"))
        and "tasks" not in slow_headers
        and slow_headers_seconds < 3.5,
        "a read that passes its deadline closes its connection to the server": slow_body_closed and slow_headers_closed,
        "the control: a fast API serves all 50 pages inside the deadline and the sole match beyond page 1 is selected": fast_pages == 50
        and fast.get("dag_run_id") == "deadline_run"
        and (fast.get("tasks") or {}).get("gold_validate") == "failed"
        and fast_seconds < 20,
    }


@case
def build_file_changed_before_gold(stack: Stack) -> dict[str, bool]:
    run_id = new_run_id()
    seen: dict[str, Any] = {}

    def change(line: dict[str, Any]) -> None:
        if line["run_id"] == run_id:
            seen["recorded"] = line.get("build_file")
            with (stack.run_folder(run_id) / "staging.duckdb").open("ab") as handle:
                handle.write(b"changed after validation")

    HOOKS.on_line("silver_validate", "finished", change)
    observation = stack.run(DAG, run_id)
    gold = submissions_of(observation, "gold_build")
    recorded = seen.get("recorded") or {}
    return {
        **failed_run(observation, ("gold_build", "release_run"), "failed_retained", True),
        "silver_validate recorded a SHA-256 and the run before the file changed": len(str(recorded.get("sha256", ""))) == 64
        and recorded.get("run_id") == run_id,
        "gold_build was refused with exit code 125 before any container started": gold["events"] == ["intent", "finished"] and gold["exit_code"] == 125,
        "the refusal says the build file changed": "changed" in str(gold.get("reason")),
        "the later gold steps did not run": all(observation["tasks"][name] == "upstream_failed" for name in ("gold_validate", "cross_layer_checks")),
    }


def set_immutable(path: Path, flag: bool) -> None:
    run_command("chflags", ["uchg" if flag else "nouchg", str(path)], timeout=30)


@case
def release_deletion_fails(stack: Stack) -> dict[str, bool]:
    run_id = new_run_id()
    build = stack.run_folder(run_id) / "staging.duckdb"
    HOOKS.at("after_record", lambda: set_immutable(build, True))
    try:
        first = stack.run(DAG, run_id)
    finally:
        if build.exists():
            set_immutable(build, False)
    first_view = {key: first[key] for key in ("dag_run_state", "tasks", "lock_state", "build_file_exists", "run_record")}
    reconciled = stack.supervisor.reconcile_runs()
    kept = {"lock": stack.lock_state(run_id), "build": stack.build_exists(run_id)}
    resumed = stack.resume(DAG, run_id, ["release_run"])
    gate = (first_view["run_record"] or {}).get("gate") or {}
    return {
        "the reconciliation does not retain the accepted finalization": reconciled["reconciled"] == [] and kept == {"lock": "finalizing", "build": True},
        "the reconciliation names the run it left for its release task": run_id in reconciled["left"],
        "the first DAG run failed at release_run only": first_view["dag_run_state"] == "failed"
        and [n for n, s in first_view["tasks"].items() if s == "failed"] == ["release_run"],
        "the gate had accepted": first_view["tasks"]["final_gate"] == "success" and gate.get("result") == "accepted",
        "the lock stayed finalizing and the build file was kept": first_view["lock_state"] == "finalizing" and first_view["build_file_exists"] is True,
        "after clearing release_run the DAG run succeeded": resumed["dag_run_state"] == "success",
        "the build file is deleted and the lock released": resumed["build_file_exists"] is False and resumed["lock_state"] == "released",
    }


@case
def release_lock_fails(stack: Stack) -> dict[str, bool]:
    run_id = new_run_id()
    record = stack.record_file(run_id)
    HOOKS.at("after_delete", lambda: record.chmod(0))
    try:
        first = stack.run(DAG, run_id)
    finally:
        if record.exists():
            record.chmod(0o600)
    first_view = {key: first[key] for key in ("dag_run_state", "tasks", "lock_state", "build_file_exists")}
    resumed = stack.resume(DAG, run_id, ["release_run"])
    return {
        "the first DAG run failed at release_run only": first_view["dag_run_state"] == "failed"
        and [n for n, s in first_view["tasks"].items() if s == "failed"] == ["release_run"],
        "the build file was deleted and the lock stayed finalizing": first_view["build_file_exists"] is False and first_view["lock_state"] == "finalizing",
        "after clearing release_run the DAG run succeeded": resumed["dag_run_state"] == "success",
        "the lock is released": resumed["lock_state"] == "released",
    }


@case
def release_interrupted(stack: Stack) -> dict[str, bool]:
    run_id = new_run_id()
    HOOKS.at("after_delete", interrupt("test: the release stops after the deletion"))
    first = stack.run(DAG, run_id)
    events = stack.ledger(run_id).get("release_accepted:fixture:1", [])
    first_view = {key: first[key] for key in ("dag_run_state", "tasks", "lock_state", "build_file_exists")}
    if stack.supervisor.broker is None:
        raise SupervisorError("the session has no broker")
    summary = stack.supervisor.broker.reconcile()
    after_reconcile = {"lock": stack.lock_state(run_id), "events": stack.ledger(run_id).get("release_accepted:fixture:1", [])}
    resumed = stack.resume(DAG, run_id, ["release_run"])
    return {
        "the first DAG run failed at release_run only": first_view["dag_run_state"] == "failed"
        and [n for n, s in first_view["tasks"].items() if s == "failed"] == ["release_run"],
        "the interrupted submission has an intent and no finished line": events == ["intent"],
        "between the steps the build file was gone and the lock finalizing": first_view["build_file_exists"] is False
        and first_view["lock_state"] == "finalizing",
        "the reconciliation finished the submission and released the lock": f"{run_id}:release_accepted:fixture:1" in summary["updated"]
        and after_reconcile["lock"] == "released",
        "its ledger now ends with finished": after_reconcile["events"] == ["intent", "finished"],
        "clearing release_run afterwards succeeds without a second release": resumed["dag_run_state"] == "success" and resumed["lock_state"] == "released",
    }


@case
def dag_run_marked_failed(stack: Stack) -> dict[str, bool]:
    run_id = new_run_id()
    marked = threading.Event()

    def mark_now() -> None:
        stack.api.mark_failed(DAG, run_id)
        marked.set()

    def mark(line: dict[str, Any]) -> None:
        if line["run_id"] == run_id:
            threading.Thread(target=mark_now).start()

    HOOKS.on_line("silver_build", "started", mark)
    started = time.monotonic()
    transform_fixtures.seed_run(stack.seeds, run_id, "good")
    stack.api.trigger(DAG, run_id, {"run_id": run_id, "mode": "fixture"})
    marked.wait(300)
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline and stack.api.run(DAG, run_id)["state"] != "failed":
        time.sleep(2)
    time.sleep(20)
    before = stack.remember(stack.observe(DAG, run_id, started))
    outcome = stack.supervisor.reconcile_runs()
    after = stack.remember(stack.observe(DAG, run_id, started))
    again = stack.supervisor.reconcile_runs()
    open_attempts = stack.open_attempts_of(run_id)
    return {
        "the DAG run was marked failed while silver_build ran": marked.is_set() and before["dag_run_state"] == "failed",
        "the finalization tasks did not run and the lock is still held": before["tasks"].get("final_gate") != "success" and before["lock_state"] == "held",
        "the reconciliation reconciled exactly this run": [item["run_id"] for item in outcome["reconciled"]] == [run_id],
        "the lock moved to failed_retained and was not released": after["lock_state"] == "failed_retained",
        "no attempt of the run is open": open_attempts == [],
        "the build file is kept": after["build_file_exists"] is True,
        "a second reconciliation changes nothing": again["reconciled"] == [] and stack.lock_state(run_id) == "failed_retained",
    }


@case
def dag_run_marked_failed_while_finalizing(stack: Stack) -> dict[str, bool]:
    run_id = new_run_id()
    marked = threading.Event()

    def mark_now() -> None:
        stack.api.mark_failed(DAG, run_id)
        marked.set()

    def mark(line: dict[str, Any]) -> None:
        if line["run_id"] == run_id:
            threading.Thread(target=mark_now).start()

    HOOKS.on_line("run_record", "finished", mark)
    started = time.monotonic()
    transform_fixtures.seed_run(stack.seeds, run_id, "good")
    stack.api.trigger(DAG, run_id, {"run_id": run_id, "mode": "fixture"})
    marked.wait(300)
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline and stack.api.run(DAG, run_id)["state"] != "failed":
        time.sleep(2)
    time.sleep(20)
    before = stack.remember(stack.observe(DAG, run_id, started))
    outcome = stack.supervisor.reconcile_runs()
    after = stack.remember(stack.observe(DAG, run_id, started))
    again = stack.supervisor.reconcile_runs()
    return {
        "the DAG run was marked failed after the record step": marked.is_set() and before["dag_run_state"] == "failed",
        "the gate did not run and the lock is finalizing with no accepted result": before["tasks"].get("final_gate") != "success"
        and before["lock_state"] == "finalizing"
        and not (before["run_record"] or {}).get("gate"),
        "the reconciliation reconciled exactly this run": [item["run_id"] for item in outcome["reconciled"]] == [run_id],
        "the lock moved to failed_retained and was not released": after["lock_state"] == "failed_retained",
        "the build file is kept": after["build_file_exists"] is True,
        "no attempt of the run is open": stack.open_attempts_of(run_id) == [],
        "a second reconciliation changes nothing": again["reconciled"] == [] and stack.lock_state(run_id) == "failed_retained",
    }


@case
def dag_run_marked_failed_after_accepted(stack: Stack) -> dict[str, bool]:
    run_id = new_run_id()
    marked = threading.Event()

    def mark(line: dict[str, Any]) -> None:
        if line["run_id"] == run_id:
            stack.api.mark_failed(DAG, run_id)
            marked.set()

    HOOKS.on_line("final_gate_accepted", "finished", mark)
    started = time.monotonic()
    transform_fixtures.seed_run(stack.seeds, run_id, "good")
    stack.api.trigger(DAG, run_id, {"run_id": run_id, "mode": "fixture"})
    marked.wait(RUN_WAIT_SECONDS)
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline and not stack.settled(DAG, run_id):
        time.sleep(2)
    before = stack.remember(stack.observe(DAG, run_id, started))
    outcome = stack.supervisor.reconcile_runs()
    after = stack.remember(stack.observe(DAG, run_id, started))
    again = stack.supervisor.reconcile_runs()
    gate = (before["run_record"] or {}).get("gate") or {}
    return {
        "the DAG run was marked failed after the gate wrote its accepted result": marked.is_set()
        and before["dag_run_state"] == "failed"
        and gate.get("result") == "accepted",
        "the gate task did not succeed and the lock is finalizing": before["tasks"].get("final_gate") != "success" and before["lock_state"] == "finalizing",
        "the reconciliation reconciled exactly this run": [item["run_id"] for item in outcome["reconciled"]] == [run_id],
        "the lock moved to failed_retained and was not released": after["lock_state"] == "failed_retained",
        "the build file is kept": after["build_file_exists"] is True,
        "no attempt of the run is open": stack.open_attempts_of(run_id) == [],
        "a second reconciliation changes nothing": again["reconciled"] == [] and stack.lock_state(run_id) == "failed_retained",
    }


@case
def concurrent_second_run(stack: Stack) -> dict[str, bool]:
    run_a, run_b = new_run_id(), new_run_id()
    second: dict[str, Any] = {}

    def trigger_b(line: dict[str, Any]) -> None:
        def run() -> None:
            second.update(stack.run(DAG, run_b))

        thread = threading.Thread(target=run, name="second-run")
        thread.start()
        second["thread"] = thread

    HOOKS.on_line("silver_build", "started", trigger_b)
    first = stack.run(DAG, run_a)
    thread = second.pop("thread", None)
    if thread is not None:
        thread.join(RUN_WAIT_SECONDS)
    second_view = stack.remember(stack.observe(DAG, run_b, time.monotonic()))
    gate = (second_view["run_record"] or {}).get("gate") or {}
    return {
        "run A succeeded and released its lock": first["dag_run_state"] == "success" and first["lock_state"] == "released",
        "run B was triggered while A held the lock": thread is not None,
        "run B's acquire_run failed and the DAG run failed": second_view["dag_run_state"] == "failed" and second_view["tasks"]["acquire_run"] == "failed",
        "the gate rejected run B with `lock not acquired`": gate.get("result") == "rejected" and gate.get("reason") == "lock not acquired",
        "run B never held a lock": second_view["lock_state"] is None,
    }


@case
def nested_manual_launch(stack: Stack) -> dict[str, bool]:
    attempts: list[dict[str, Any]] = []
    probe = Reconciliation()

    def attempt(moment: str, run_id: str) -> None:
        attempts.append({**stack.manual_launch(moment), "lock_at_moment": stack.lock_state(run_id)})

    retry_run, final_run = new_run_id(), new_run_id()
    HOOKS.on_line("silver_build", "started", lambda line: attempt("during a run", retry_run))
    HOOKS.on_line("gold_build", "finished", lambda line: attempt("during a retry", retry_run), attempt=1, failed=True)
    first = stack.run(RETRY_DAG, retry_run, "bad_gold_build")
    stack.release_leftovers()

    def between_tasks(line: dict[str, Any]) -> None:
        attempt("between tasks", final_run)
        probe.start(stack.supervisor)

    HOOKS.on_line("silver_build", "finished", between_tasks)
    HOOKS.at("after_finalize", lambda: attempt("during finalization", final_run))
    second = stack.run(DAG, final_run)
    live = probe.join(RECONCILE_JOIN_SECONDS)
    moments = {item["moment"]: item for item in attempts}
    before_finalization = [moments.get(name, {}).get("lock_at_moment") for name in ("during a run", "during a retry", "between tasks")]
    return {
        "a launch was tried at each of the four moments": sorted(moments) == ["between tasks", "during a retry", "during a run", "during finalization"],
        "every launch was refused": all(item["refused"] for item in attempts),
        "the run was held before finalization and finalizing during it": before_finalization == ["held"] * 3
        and moments.get("during finalization", {}).get("lock_at_moment") == "finalizing",
        "the probe's reconciliation call completed": live is not None,
        "the reconciliation leaves a run whose DAG run is live": live is not None
        and live.get("reconciled") == []
        and "live" in " ".join(str(v) for v in live.get("left", {}).values()),
        "the retried run failed after its retry and was retained": first["dag_run_state"] == "failed" and first["lock_state"] == "failed_retained",
        "the second run succeeded and released its lock": second["dag_run_state"] == "success" and second["lock_state"] == "released",
    }


@case
def graph_validator(stack: Stack) -> dict[str, bool]:
    environment = {name: os.environ[name] for name in ("PATH", "HOME", "TMPDIR") if name in os.environ}
    arguments = ["-m", "scripts.orchestration.offline_checks", "--test-registry", str(stack.registry), "--only", "graph", "--only", "image"]
    process = run_command(sys.executable, arguments, cwd=ROOT, env=environment, timeout=1500)
    reports = sorted((ROOT / "data" / "e2e" / "orchestration_offline").glob("*/report.json"))
    results: dict[str, Any] = json.loads(reports[-1].read_text())["results"] if reports else {}
    stack.evidence["graph_validator"] = {
        "exit_code": process.returncode,
        "report": str(reports[-1].relative_to(ROOT)) if reports else None,
        "checks": len(results),
    }
    real = {name: results.get(f"{name} passes the graph validator (Airflow object)", {}) for name in supervisor_module.PIPELINE_DAGS}
    synthetic = {name: item["ok"] for name, item in results.items() if name.startswith("graph (Airflow objects): ")}
    return {
        "the offline checks with the image ran and passed": process.returncode == 0 and bool(results),
        "Airflow imported every DAG file in the image with no error": results.get("every DAG file imports in the image with no error or cycle", {}).get("ok")
        is True,
        "the validator passes on every real pipeline DAG object": all(
            item.get("ok") is True and item.get("present") is True and item.get("violations") == [] for item in real.values()
        ),
        "the synthetic rejection cases still fail with their rules": len(synthetic) >= 10 and all(synthetic.values()),
    }


PROBE_REQUEST = {"request_version": 2, "dag_id": PROBE_DAG, "task_id": "refused", "dag_run_id": "manual__2026-10-15T00:00:00+00:00"}
PROBE_BASE = {"template": "silver_build", "run_id": "20261015T000000Z-0a0a0a0a", "mode": "fixture", "attempt": 1, **PROBE_REQUEST}
PROBE_CONTROL = {"template": "catalog_up", "run_id": "20261015T000001Z-0b0b0b0b", "mode": "fixture", "attempt": 1, **PROBE_REQUEST}

PROBE_DAG_TEXT = '''"""Test-only DAG of the E2E: a task posts refused specifications straight to the broker with the session token (never part of dags/)."""

from __future__ import annotations

import http.client
import json
from datetime import UTC, datetime
from pathlib import Path

from airflow.sdk import DAG, task

BASE = __PROBE_BASE__
BODIES = {
    "extra field: image": {**BASE, "image": "evil@sha256:" + "0" * 64},
    "extra field: command": {**BASE, "command": ["sh", "-c", "id"]},
    "extra field: environment": {**BASE, "environment": {"AWS_SECRET_ACCESS_KEY": "x"}},
    "extra field: mounts": {**BASE, "mounts": [{"source": "/", "destination": "/host", "read_only": False}]},
    "extra field: privileged": {**BASE, "privileged": True},
    "extra field: user": {**BASE, "user": "root"},
    "extra field: network": {**BASE, "network": "host"},
    "extra field: args": {**BASE, "args": ["build", "--evil"]},
    "extra field on a control row": {**BASE, "template": "release_accepted", "build_files": ["/etc/passwd"]},
    "unknown template": {**BASE, "template": "not_a_template"},
    "template path escape": {**BASE, "template": "../silver_build"},
    "mode the template lacks": {**BASE, "template": "commit_publish"},
    "mode without a launcher row": {**BASE, "template": "sense_markers", "mode": "real"},
    "run ID path escape": {**BASE, "run_id": "../../etc"},
    "attempt out of range": {**BASE, "attempt": 0},
    "attempt not a number": {**BASE, "attempt": "1; id"},
}


def post(operation: str, body: object) -> int:
    settings = json.loads(Path("/opt/hai/broker/client.json").read_text())
    token = Path("/opt/hai/broker/broker_token").read_text().strip()
    host, port = settings["url"].removeprefix("http://").split(":")
    connection = http.client.HTTPConnection(host, int(port), timeout=60)
    try:
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        connection.request("POST", f"/v1/{operation}", body=json.dumps(body).encode(), headers=headers)
        return connection.getresponse().status
    finally:
        connection.close()


@task
def refused() -> str:
    """Every body is refused with 400; a valid request (a no-op) is accepted with 200 so that the poster is known to work."""
    results = {name: post("submit", body) for name, body in BODIES.items()}
    results["control: a valid no-op"] = post("submit", __PROBE_CONTROL__)
    wrong = {name: status for name, status in results.items() if status != (200 if name.startswith("control:") else 400)}
    if wrong:
        raise RuntimeError(f"unexpected statuses: {wrong}")
    return json.dumps(results, sort_keys=True)


with DAG(dag_id="refused_probe", schedule=None, start_date=datetime(2026, 1, 1, tzinfo=UTC), catchup=False):
    refused()
'''.replace("__PROBE_BASE__", repr(PROBE_BASE)).replace("__PROBE_CONTROL__", repr(PROBE_CONTROL))


@case
def refused_specifications(stack: Stack) -> dict[str, bool]:
    run_id = "refused_probe_" + secrets.token_hex(4)
    started = time.monotonic()
    stack.api.trigger(PROBE_DAG, run_id, {})
    with contextlib.suppress(AirflowApiError):
        stack.api.wait_for_run(PROBE_DAG, run_id, 300)
    observation = stack.api.run(PROBE_DAG, run_id)
    tasks = stack.api.task_states(PROBE_DAG, run_id)
    stack.observed[run_id] = {
        "dag_id": PROBE_DAG,
        "run_id": run_id,
        "dag_run_state": observation["state"],
        "tasks": tasks,
        "seconds": round(time.monotonic() - started, 1),
    }
    schema = broker_module.schema_validator("submission_request.schema.json")
    refused_ledger = Ledger(stack.supervisor.paths.ledger)
    refused_lines = refused_ledger.read("20261015T000000Z-0a0a0a0a")
    ok_lines = refused_ledger.read("20261015T000001Z-0b0b0b0b")
    created = [name for name in stack.client.call("status", {"what": "launcher"})["containers"] if "-run-" in name]
    return {
        "the probe task saw 400 for every refused specification and 200 for the control": observation["state"] == "success"
        and tasks.get("refused") == "success",
        "the base and control bodies satisfy the request schema, so each refusal comes from its own defect": not any(
            list(schema.iter_errors(body)) for body in (PROBE_BASE, PROBE_CONTROL)
        ),
        "the refused requests left no ledger line": refused_lines == [],
        "the control request did leave its ledger lines": [line["event"] for line in ok_lines] == ["intent", "finished"],
        "no job container exists": created == [],
    }


def run_trigger_command(stack: Stack, run_id: str, overlay: str) -> dict[str, Any]:
    """Run ``trigger_dag.main`` against the stack, with the runner's secret-store stand-in and the stack's API port; return its exit code and output.

    The fixture workflow and its mode are added to the command's closed lists for the call; nothing else of the command is replaced."""
    transform_fixtures.seed_run(stack.seeds, run_id, overlay)
    real_open = secret_store.open_store
    reads_before = token_secret_reads(stack)
    out, err = io.StringIO(), io.StringIO()
    argv = ["--dag", DAG, "--mode", "fixture", "--run-id", run_id, "--wait-seconds", str(RUN_WAIT_SECONDS)]
    with (
        mock.patch.object(trigger_dag, "DAGS", (*trigger_dag.DAGS, DAG)),
        mock.patch.object(trigger_dag, "MODES", (*trigger_dag.MODES, "fixture")),
        mock.patch.object(trigger_dag, "api_port", lambda: stack.supervisor.api_port),
        mock.patch.object(secret_store, "open_store", lambda root, endpoint=None: real_open(root, stack.stand_in.endpoint)),
        contextlib.redirect_stdout(out),
        contextlib.redirect_stderr(err),
    ):
        code = trigger_dag.main(argv)
    return {"exit_code": code, "stdout": out.getvalue(), "stderr": err.getvalue(), "token_secret_reads": token_secret_reads(stack) - reads_before}


def token_secret_reads(stack: Stack) -> int:
    return sum(1 for action, name in stack.stand_in.requests() if action == "GetSecretValue" and name == secret_store.AIRFLOW_JWT_SECRET)


def printed_result(text: str) -> dict[str, Any]:
    try:
        found = json.loads(text)
    except ValueError:
        return {}
    return found if isinstance(found, dict) else {}


def is_paused(stack: Stack) -> bool | None:
    record = stack.api.dag(DAG)
    return None if record is None else record.get("is_paused")


@case
def trigger_command(stack: Stack) -> dict[str, bool]:
    stack.api.request("PATCH", f"/dags/{DAG}", {"is_paused": True})
    paused_before = is_paused(stack)
    run_id = new_run_id()
    started = time.monotonic()
    success = run_trigger_command(stack, run_id, "good")
    paused_after = is_paused(stack)
    observation = stack.remember(stack.observe(DAG, run_id, started))
    observation["trigger_command"] = success
    shown = printed_result(success["stdout"])
    failed_id = new_run_id()
    failed_started = time.monotonic()
    failure = run_trigger_command(stack, failed_id, "bad_gold_build")
    failed_observation = stack.remember(stack.observe(DAG, failed_id, failed_started))
    failed_observation["trigger_command"] = failure
    failed_shown = printed_result(failure["stdout"])
    outputs = "".join(f"{item['stdout']}{item['stderr']}" for item in (success, failure))
    return {
        "the workflow was paused before the command ran": paused_before is True,
        "the command unpaused the workflow": paused_after is False,
        "the command read the session's token secret from the stand-in and was accepted by the API": success["token_secret_reads"] == 1 and bool(shown),
        "the submitted configuration is the run ID and the mode": observation["conf"] == {"run_id": run_id, "mode": "fixture"},
        "a successful run exits 0 and prints its run ID, the state success and the task states": success["exit_code"] == 0
        and shown.get("run_id") == run_id
        and shown.get("dag") == DAG
        and shown.get("state") == "success"
        and shown.get("tasks") == observation["tasks"] == SUCCESS_TASKS,
        "a failed run exits 1 and prints the state failed and the task states": failure["exit_code"] == 1
        and failed_shown.get("run_id") == failed_id
        and failed_shown.get("state") == "failed"
        and failed_shown.get("tasks") == failed_observation["tasks"]
        and failed_observation["tasks"].get("gold_build") == "failed",
        "the failed run is the one the fixture breaks (the same checks as work_task_fails)": all(
            failed_run(failed_observation, ("gold_build", "release_run"), "failed_retained", True).values()
        ),
        "neither run printed the signing secret or a bearer token": stack.supervisor.jwt_secret not in outputs
        and "Bearer" not in outputs
        and "eyJ" not in outputs,
    }


def dump_and_scan(stack: Stack, *, expected_runs: set[str] | None = None) -> dict[str, bool]:
    allocation = stack.client.allocate()
    results: dict[str, Any] = {}
    try:
        principal = stack.client.principal_alloc(allocation)
        for what in ("metadata", "xcom"):
            started = time.monotonic()
            outcome = stack.client.run_attempt(
                principal, "orchestration_dump", f"dump-{what}", [what], {"state": str(stack.state)}, checkout=stack.checkout, seconds=DUMP_SECONDS
            )
            results[what] = {"ok": outcome.ok, "exit_code": outcome.exit_code, "reason": outcome.reason, "seconds": round(time.monotonic() - started, 1)}
            if not outcome.ok:
                results[what]["stderr_tail"] = outcome.stderr[-400:]
    finally:
        stack.client.release_allocation(allocation)
    folder = stack.state / "dumps"
    run_ids = [run_id for run_id in stack.seen_runs if (folder / "metadata.sql").is_file() and run_id.encode() in (folder / "metadata.sql").read_bytes()]
    roots = [
        folder,
        stack.state / "logs",
        stack.state / "postgres",
        stack.work / "broker",
        stack.work / "ledger",
        stack.work / "records",
        stack.work / "job_logs",
        stack.work / "forwards",
        stack.work / "registry" / "registry.json",
        stack.checkout / "data",
    ]
    token = stack.supervisor.token
    scan = sentinel.scan(roots, [token, *SENTINELS.values(), *stack.run_tokens()])
    stack.evidence["sentinel"] = scan.record()
    stack.evidence["dumps"] = results
    required = {"dumps": 2, "logs": 1, "postgres": 1, "broker": 1, "ledger": 1, "records": 1, "job_logs": 1, "forwards": 1, "registry.json": 1, "data": 1}
    short = {name: scan.per_root.get(name, 0) for name, minimum in required.items() if scan.per_root.get(name, 0) < minimum}
    return {
        "the dump and the XCom query ran as launcher jobs and wrote their files": all(item["ok"] for item in results.values())
        and (folder / "metadata.sql").is_file(),
        "the dump holds the DAG runs of this session (the scan can see the data)": (
            len(run_ids) >= 2 if expected_runs is None else bool(expected_runs) and expected_runs == stack.seen_runs == set(run_ids)
        ),
        "the sentinel scan read every input and found no token or credential": scan.ok and not short,
    }


ALL_CASES = (
    fixture_graph_success,
    acquire_run_fails,
    catalog_up_fails,
    work_task_fails,
    gold_test_fails,
    cross_layer_fails,
    run_record_fails,
    record_path_blocked,
    gate_fails_after_accepted,
    task_fails_before_submission,
    gate_lookup_unavailable,
    snapshot_deadline,
    build_file_changed_before_gold,
    release_deletion_fails,
    release_lock_fails,
    release_interrupted,
    dag_run_marked_failed,
    dag_run_marked_failed_while_finalizing,
    dag_run_marked_failed_after_accepted,
    concurrent_second_run,
    nested_manual_launch,
    graph_validator,
    refused_specifications,
    trigger_command,
)
CASE_ORDER = tuple(function.__name__ for function in ALL_CASES)


def shipped_fixture_graph(stack: Stack) -> dict[str, bool]:
    stack.evidence["shipped_seed"] = stack.seed_shipped_fixture()
    run_id = new_run_id()
    seen: dict[str, Any] = {}
    HOOKS.on_line(
        "silver_validate",
        "finished",
        lambda line: seen.update(validated=line.get("build_file"), digest=file_digest(stack.run_folder(run_id) / "staging.duckdb")),
    )
    started = time.monotonic()
    stack.api.trigger(DAG, run_id, {"run_id": run_id, "mode": "fixture"})
    with contextlib.suppress(AirflowApiError):
        stack.api.wait_for_run(DAG, run_id, SHIPPED_WAIT_SECONDS)
    observation = stack.remember(stack.observe(DAG, run_id, started))
    record = observation["run_record"] or {}
    return {
        "the DAG run succeeded": observation["dag_run_state"] == "success",
        "every task ended as the plan expects": observation["tasks"] == SUCCESS_TASKS,
        "the build file is deleted and the lock released": observation["build_file_exists"] is False and observation["lock_state"] == "released",
        "the run record names an accepted gate": (record.get("gate") or {}).get("result") == "accepted",
        "the gate record holds the DAG run's task states, each as expected": gate_evidence_matches(record, run_id),
        "the recorded SHA-256 is the one the validator reported and the file had": len(str((seen.get("validated") or {}).get("sha256") or "")) == 64
        and (seen.get("validated") or {}).get("sha256") == validator_reported(stack, run_id) == seen.get("digest"),
    }


DRIFT = "UPDATE $table SET $column = $column + 1 WHERE rowid = (SELECT min(rowid) FROM $table WHERE $column IS NOT NULL);"


def drift_statements() -> list[str]:
    from scripts.lakehouse import gold_checks

    statements = []
    for fact in gold_checks.load_contract().facts:
        if fact.is_hold:
            continue
        fixed = {fact.grain, *fact.source_dates, *fact.text_columns, *(key.natural_key for key in fact.keys)}
        measures = [column.name for column in fact.columns if column.type.upper() in gold_checks.NUMERIC_TYPES and column.name not in fixed]
        if measures:
            statements.append(gold_checks.build(DRIFT, table=gold_checks.relation(fact.name), column=gold_checks.ident(measures[0])))
    return statements


def shipped_reconcile_mismatch(stack: Stack) -> dict[str, bool]:
    stack.evidence["shipped_seed"] = stack.seed_shipped_fixture()
    run_id = new_run_id()
    statements = drift_statements()
    mutated: dict[str, Any] = {}

    def drift(line: dict[str, Any]) -> None:
        if line["run_id"] == run_id:
            mutated["ok"] = stack.run_sql_on_build(run_id, "\n".join(statements))

    HOOKS.on_line("gold_validate", "finished", drift)
    started = time.monotonic()
    stack.api.trigger(DAG, run_id, {"run_id": run_id, "mode": "fixture"})
    with contextlib.suppress(AirflowApiError):
        stack.api.wait_for_run(DAG, run_id, SHIPPED_WAIT_SECONDS)
    observation = stack.remember(stack.observe(DAG, run_id, started))
    result_file = stack.run_folder(run_id) / "quality_cross" / "result.json"
    result = json.loads(result_file.read_text()) if result_file.is_file() else {}
    problems = (result.get("reconciliation") or {}).get("problems") or {}
    return {
        **failed_run(observation, ("cross_layer_checks", "release_run"), "failed_retained", True),
        "the facts were changed after the gold validation passed": bool(statements)
        and mutated.get("ok") is True
        and observation["tasks"]["gold_validate"] == "success",
        "the reconciliation reports a content difference and no row-count difference": bool(problems.get("reconcile_content"))
        and not problems.get("reconcile_rows"),
        "the gold suites and the silver suites passed in the same job": (result.get("counts") or {}).get("fail") == 0,
        "the branch did not run": observation["tasks"]["publish_branch"] == "upstream_failed",
    }


SHIPPED_CASES: dict[str, Case] = {"shipped_fixture_graph": shipped_fixture_graph, "shipped_reconcile_mismatch": shipped_reconcile_mismatch}


def run_session(selected: list[str], mutants: bool = False, shipped: bool = False, keep_work: bool = False, only: list[str] | None = None) -> int:
    if set(CASE_ORDER) != set(CASES):
        raise SystemExit("CASE_ORDER differs from the registered cases")
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    work = Path(tempfile.mkdtemp(prefix="a2_transform_", dir=TEMP_ROOT)).resolve()
    stack = Stack(work)
    stack.shipped = shipped
    stack.keep_work = keep_work
    results: list[dict[str, Any]] = []
    session_error = ""
    try:
        try:
            stack.start()
        except SupervisorError as error:
            session_error = f"the session did not start: {error}"
            sys.stdout.write(f"blocked: {session_error}\n")
        else:
            stack.evidence["dags"] = stack.wait_for_dags(stack.dag_ids())
            if mutants:
                results.extend(run_mutants(stack, only))
            else:
                for name in selected or list(CASE_ORDER):
                    results.append(run_case(stack, name))
                if not selected:
                    results.append(scan_case(stack))
    except Exception as error:
        session_error = f"{type(error).__name__}: {error}\n{traceback.format_exc()[-1500:]}"
        sys.stdout.write(f"error: {session_error.splitlines()[0]}\n")
    finally:
        teardown = finish(stack, work)
    passed = bool(results) and all(item["passed"] for item in results) and not session_error and not teardown["errors"]
    folder = OUT / f"session_{stamp}"
    folder.mkdir(parents=True)
    report = {
        "feature": "transform_dag through the real scheduler and LocalExecutor, real Docker, test registry",
        "command": ".venv/bin/python -m scripts.orchestration.run_transform_e2e",
        "started_utc": stamp,
        "status": "pass" if passed else "fail",
        "head": run_command("git", ["rev-parse", "HEAD"], cwd=ROOT).stdout.strip(),
        "error": session_error,
        "cases": results,
        "evidence": stack.evidence,
        "teardown": teardown,
        "limits": LIMITS,
    }
    text = json.dumps(report, indent=2, default=str)
    if any(value in text for value in SENTINELS.values()):
        report = {**report, "cases": "withheld: a planted credential is in the report", "evidence": "withheld"}
        text = json.dumps(report, indent=2, default=str)
    (folder / "report.json").write_text(text + "\n")
    sys.stdout.write(f"{report['status']}: {(folder / 'report.json').relative_to(ROOT)}\n")
    return 0 if passed else 1


def capped_wait(self: AirflowApi, dag_id: str, dag_run_id: str, seconds: float) -> dict[str, Any]:
    return ORIGINAL_WAIT(self, dag_id, dag_run_id, min(seconds, MUTANT_WAIT_SECONDS))


def run_mutants(stack: Stack, only: list[str] | None = None) -> list[dict[str, Any]]:
    LOGS.mkdir(parents=True, exist_ok=True)
    results = []
    for name, mutant in MUTANTS.items():
        for probe in mutant.probes:
            if only and f"{probe.case}/{name}" not in only and name not in only:
                continue
            stack.dag_map = {mutant.base: f"mutant_{name}"} if not mutant.module else {}
            with installed(mutant) if mutant.module else contextlib.nullcontext(), mock.patch.object(AirflowApi, "wait_for_run", capped_wait):
                result = run_case(stack, probe.case)
            stack.dag_map = {}
            failed = [check for check, ok in result["checks"].items() if not ok]
            unfinished = bool(probe.completes) and any(probe.completes in check for check in failed)
            verified = not result["error"] and not unfinished and any(probe.check in check for check in failed)
            verdict = (
                "failing-first verified"
                if verified
                else "NOT VERIFIED: the probe's call did not complete, so no named check is credited"
                if unfinished
                else "NOT VERIFIED"
            )
            lines = [
                f"case {probe.case}",
                f"mutant {name}: {mutant.what}",
                f"  intended check: {probe.check}",
                f"  verdict: {verdict}",
                f"  the code change{'' if not mutant.module else ' in ' + mutant.module}:",
            ]
            lines += [f"    - {old.strip()}\n    + {new.strip()}" for old, new in mutant.edits]
            lines += [f"  {'ok' if ok else 'FAILED'}: {check}" for check, ok in result["checks"].items()]
            if result["error"]:
                lines.append(f"  error: {result['error'].splitlines()[0]}")
            (LOGS / f"scheduler__{probe.case}__{name}.log").write_text("\n".join(lines) + "\n")
            results.append(
                {
                    "case": f"{probe.case} / {name}",
                    "passed": verified,
                    "checks": {"the intended check failed with the mutant in place": verified},
                    "error": result["error"],
                    "reports": result["reports"],
                }
            )
    return results


def run_case(stack: Stack, name: str) -> dict[str, Any]:
    HOOKS.reset()
    started = time.monotonic()
    checks: dict[str, bool] = {}
    error = ""
    try:
        stack.wait_idle()
        stack.release_leftovers()
        stack.swap_inventory(stack.base_inventory())
        checks = (CASES.get(name) or SHIPPED_CASES[name])(stack)
    except Exception as exc:
        error = f"{type(exc).__name__}: {exc}\n{traceback.format_exc()[-1500:]}"
    HOOKS.reset()
    stack.seen_runs.update(stack.observed)
    reports = stack.write_reports(name, checks)
    passed = bool(checks) and all(checks.values()) and not error
    sys.stdout.write(f"{'ok' if passed else 'FAIL':<5} {name} ({round(time.monotonic() - started, 1)} s)\n")
    for check, ok in checks.items():
        if not ok:
            sys.stdout.write(f"      failed: {check}\n")
    if error:
        sys.stdout.write(f"      error: {error.splitlines()[0]}\n")
    return {"case": name, "passed": passed, "checks": checks, "error": error, "reports": reports, "seconds": round(time.monotonic() - started, 1)}


def scan_case(stack: Stack) -> dict[str, Any]:
    started = time.monotonic()
    error = ""
    checks: dict[str, bool] = {}
    try:
        checks = dump_and_scan(stack)
    except Exception as exc:
        error = f"{type(exc).__name__}: {exc}\n{traceback.format_exc()[-1500:]}"
    passed = bool(checks) and all(checks.values()) and not error
    sys.stdout.write(f"{'ok' if passed else 'FAIL':<5} sentinel_scan ({round(time.monotonic() - started, 1)} s)\n")
    for check, ok in checks.items():
        if not ok:
            sys.stdout.write(f"      failed: {check}\n")
    if error:
        sys.stdout.write(f"      error: {error.splitlines()[0]}\n")
    return {"case": "sentinel_scan", "passed": passed, "checks": checks, "error": error, "seconds": round(time.monotonic() - started, 1)}


def finish(stack: Stack, work: Path) -> dict[str, Any]:
    result: dict[str, Any] = {}
    errors: list[str] = []
    try:
        result["supervisor"] = stack.supervisor.teardown()
        errors += result["supervisor"].get("errors", [])
    except Exception as error:
        errors.append(f"supervisor teardown: {type(error).__name__}: {error}"[:300])
    try:
        stack.client.call("shutdown", {"owner": True}, {"kind": "any", "owner": True})
    except Exception as error:
        errors.append(f"launcher shutdown: {type(error).__name__}: {error}"[:300])
    for name in SENTINELS:
        os.environ.pop(name, None)
    stack.stand_in.__exit__()
    if not stack.launcher_process:
        errors.append("the launcher process was never identified, so its exit cannot be confirmed")
    elif problem := launcher_exit_problem(stack.launcher_process):
        errors.append(problem)
    result["leftovers"] = leftovers(work)
    errors += result["leftovers"]
    if not errors and not stack.keep_work:
        result["removal"] = remove_work(work, stack.launcher_process)
        errors += result["removal"]
    result["errors"] = errors
    if errors:
        result["retained"] = str(work)
        result["finish_with"] = f".venv/bin/python -m scripts.orchestration.run_transform_e2e --finish-cleanup {work}"
    result["temporary_folder_removed"] = not work.exists()
    return result


def record_launcher_process(client: launcher.LauncherClient, registry: Path, work: Path) -> dict[str, str]:
    """Identify the launcher process this session started and save the identity beside the registry.

    The launcher names its own process ID over its socket. The process is the session's launcher when its start nonce is the one the client passed, the
    registry identity it reports is the registry's and the process is running.
    """
    ping = client.call("ping")
    view = client.call("status", {"what": "launcher"})
    pid = int(view["pid"])
    identity = run_lock.read_identity(run_lock.Registry(registry, "test"))
    started = run_lock.process_start(pid)
    if started is None or int(ping["pid"]) != pid or ping["launch_nonce"] != client.launch_nonce or view["registry_id"] != identity["registry_id"]:
        raise SupervisorError("the process that answers on the launcher socket is not the launcher this session started")
    record = {"pid": str(pid), "started": started, "registry_id": identity["registry_id"], "lock": str(registry / "launcher.lock")}
    (work / LAUNCHER_PROCESS_FILE).write_text(json.dumps(record, indent=2) + "\n")
    return record


def process_gone(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return True
    except PermissionError:
        return False
    return False


def observe_launcher(identity: dict[str, str]) -> str:
    pid = int(identity["pid"])
    started = run_lock.process_start(pid)
    if started is None:
        return EXITED if process_gone(pid) else UNOBSERVED
    return RUNNING if started == identity["started"] else EXITED


def launcher_exit(identity: dict[str, str], seconds: float) -> str:
    """``EXITED`` when the process the identity names is gone or shows another start time, ``RUNNING`` or ``UNOBSERVED`` at the end of ``seconds``.

    Only a PID that no longer exists or a start time that was read and differs counts as an exit. A ``ps`` read that fails or returns no start time
    while the PID exists is ``UNOBSERVED``. The launcher closes its lock while a worker thread of its pool can still keep the process alive, so a
    free lock does not say that the process has exited.
    """
    deadline = time.monotonic() + seconds
    while True:
        state = observe_launcher(identity)
        if state == EXITED or time.monotonic() > deadline:
            return state
        time.sleep(0.5)


def launcher_exit_problem(identity: dict[str, str]) -> str | None:
    state = launcher_exit(identity, LAUNCHER_WAIT_SECONDS)
    if state == RUNNING:
        return f"the launcher process {identity['pid']} still runs"
    if state == UNOBSERVED:
        return f"the launcher process {identity['pid']} cannot be observed, so its exit is not confirmed"
    return None


def cleanup_record_path(work: Path) -> Path:
    return work.with_name(f"{work.name}.cleanup.json")


def remove_entry(path: Path, failures: list[str]) -> None:
    def note(function: Callable[..., Any], failed: str, error: BaseException) -> None:
        failures.append(f"folder removal: {Path(failed).name}: {type(error).__name__}")

    if path.is_dir() and not path.is_symlink():
        shutil.rmtree(path, onexc=note)
        return
    try:
        path.unlink(missing_ok=True)
    except OSError as error:
        note(os.unlink, str(path), error)


def sync_folder(folder: Path) -> None:
    descriptor = os.open(folder, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def publish_record(record: Path, body: dict[str, Any]) -> None:
    """Make ``body`` durable at ``record``: write it under a temporary name, flush and synchronize it, rename it and synchronize the parent folder.

    ``OSError`` when any step fails; the temporary name is removed and the record is not replaced by a partial one.
    """
    pending = record.with_name(f"{record.name}.new")
    try:
        with pending.open("w") as handle:
            handle.write(json.dumps(body, indent=2) + "\n")
            handle.flush()
            os.fsync(handle.fileno())
        pending.replace(record)
    except OSError:
        with contextlib.suppress(OSError):
            pending.unlink(missing_ok=True)
        raise
    sync_folder(record.parent)


def remove_work(work: Path, saved: dict[str, str]) -> list[str]:
    """Remove the session folder and return the failures.

    The verified cleanup is recorded beside the folder first (the launcher process that exited and the folder). The record is durable before anything
    in the folder is deleted: a record or folder that cannot be synchronized fails the cleanup and leaves every original in place. The record goes last,
    after the folder is gone and the folder that held it is synchronized, so the removal is durable before the record can disappear. A failure while the
    folder is removed or synchronized leaves the record, and ``--finish-cleanup`` repeats the removal and the synchronization from it, also when the
    folder is already gone. The record's own deletion is not synchronized: a record that survives a crash for an absent folder is deleted by the next run.
    """
    record = cleanup_record_path(work)
    try:
        publish_record(record, {"work": str(work), "launcher_process": saved})
    except OSError as error:
        return [f"cleanup record: {record.name}: {type(error).__name__}"]
    failures: list[str] = []
    remove_entry(work, failures)
    if work.exists():
        return failures
    try:
        sync_folder(work.parent)
    except OSError as error:
        return [*failures, f"folder removal sync: {work.parent.name}: {type(error).__name__}"]
    remove_entry(record, failures)
    return failures


def leftovers(work: Path) -> list[str]:
    try:
        state = json.loads((work / "registry" / "registry.json").read_text())
    except (OSError, ValueError) as error:
        return [f"the registry file cannot be read: {type(error).__name__}"]
    found = [f"attempt {item['id']} is {item['phase']}" for item in state.get("attempts", {}).values() if item.get("phase") != "ended"]
    return found + [f"{section}: {', '.join(sorted(state[section]))}" for section in ("reservations", "readers", "services") if state.get(section)]


def finish_cleanup(work: Path) -> dict[str, Any]:
    result: dict[str, Any] = {"errors": []}
    record = cleanup_record_path(work)
    if record.is_file():
        try:
            recorded = json.loads(record.read_text())
            if Path(recorded["work"]).resolve() != work.resolve():
                raise ValueError
        except (OSError, ValueError, KeyError, TypeError):
            result["errors"].append(f"{record.name} cannot be read, so nothing was removed")
            return result
        result["removal"] = remove_work(work, recorded["launcher_process"])
        result["errors"] += result["removal"]
        result["folder_removed"] = not work.exists()
        return result
    registry = work / "registry"
    missing = [name for name in RECOVERY_INPUTS if not (registry / name).is_file()] + (
        [] if (work / LAUNCHER_PROCESS_FILE).is_file() else [LAUNCHER_PROCESS_FILE]
    )
    if missing:
        result["errors"].append(f"{work.name} lacks {', '.join(missing)}: the cleanup cannot be proven, and nothing was created in their place")
        return result
    try:
        saved = json.loads((work / LAUNCHER_PROCESS_FILE).read_text())
        int(saved["pid"])
        identity = run_lock.read_identity(run_lock.Registry(registry, "test"))
        config = launcher.resolve_config(str(registry))
    except (OSError, ValueError, KeyError, TypeError, run_lock.RegistryError) as error:
        result["errors"].append(f"the saved launcher process, the registry identity or the launcher settings cannot be read: {type(error).__name__}")
        return result
    if identity["registry_id"] != saved.get("registry_id"):
        result["errors"].append("the registry is not the one this session created")
        return result
    client = launcher.LauncherClient(config, start=False)
    try:
        client.call("status", {"what": "launcher"})
    except launcher.ClientError:
        result["launcher"] = "not running"
    else:
        try:
            supervisor = Supervisor(BrokerPaths(run_lock.Registry(registry, "test"), work), PLATFORM, client, repo_root=work / "checkout")
            result["supervisor"] = supervisor.teardown()
            result["errors"] += result["supervisor"].get("errors", [])
            client.call("shutdown", {"owner": True}, {"kind": "any", "owner": True})
            result["launcher"] = "stopped"
        except (SupervisorError, launcher.ClientError, OSError) as error:
            result["errors"].append(f"{type(error).__name__}: {error}"[:300])
    if problem := launcher_exit_problem(saved):
        result["errors"].append(problem)
    result["leftovers"] = leftovers(work)
    result["errors"] += result["leftovers"]
    if not result["errors"]:
        result["removal"] = remove_work(work, saved)
        result["errors"] += result["removal"]
    result["folder_removed"] = not work.exists()
    return result


def newest_session(before: set[Path]) -> tuple[Path | None, dict[str, Any]]:
    sessions = sorted(set(OUT.glob("session_*")) - before)
    return (sessions[-1], json.loads((sessions[-1] / "report.json").read_text())) if sessions else (None, {})


def linger(folder: Path, seconds: float) -> int:
    """The stand-in for a launcher whose lock is free while a worker keeps its process alive: take the folder's launcher lock, record the process ID,
    release the lock and stay for ``seconds``."""
    handle = (folder / "launcher.lock").open("a")
    fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
    (folder / "linger.tmp").write_text(json.dumps({"pid": os.getpid()}))
    (folder / "linger.tmp").replace(folder / "linger.json")
    handle.close()
    time.sleep(seconds)
    return 0


def start_stand_in(folder: Path, seconds: float) -> dict[str, str]:
    (folder / "linger.json").unlink(missing_ok=True)
    arguments = ["-m", "scripts.orchestration.run_transform_e2e", "--linger", str(folder), str(seconds)]

    def run() -> None:
        run_command(sys.executable, arguments, cwd=ROOT, timeout=120)

    threading.Thread(target=run, daemon=True).start()
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        if (folder / "linger.json").is_file():
            pid = int(json.loads((folder / "linger.json").read_text())["pid"])
            started = run_lock.process_start(pid)
            if started is not None:
                return {"pid": str(pid), "started": started, "lock": str(folder / "launcher.lock")}
        time.sleep(0.1)
    raise SupervisorError("the stand-in process did not start")


def lock_is_free(path: Path) -> bool:
    with path.open("a") as handle:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return False
        return True


def check_delayed_worker() -> dict[str, bool]:
    with tempfile.TemporaryDirectory(prefix="a2_linger_", dir=TEMP_ROOT) as name:
        folder = Path(name)
        delayed = start_stand_in(folder, 14)
        lock_free = lock_is_free(folder / "launcher.lock")
        still_running = run_lock.process_start(int(delayed["pid"])) == delayed["started"]
        began = time.monotonic()
        exited = launcher_exit(delayed, 90) == EXITED
        waited = time.monotonic() - began
        gone = run_lock.process_start(int(delayed["pid"])) != delayed["started"]
        longer = start_stand_in(folder, 25)
        reported_running = launcher_exit(longer, 1.0) == RUNNING
        outlasted = launcher_exit(longer, 90) == EXITED
        short = start_stand_in(folder, 3)
        began = time.monotonic()
        short_exited = launcher_exit(short, 90) == EXITED
        short_waited = time.monotonic() - began
    return {
        "the stand-in launcher's lock is free while its process still runs": lock_free and still_running,
        "the wait for the launcher process does not return before the process has exited": exited and gone and waited >= 5,
        "a process that outlives the wait is reported as still running": reported_running and outlasted,
        "the control: a process that exits soon ends the wait when it exits": short_exited and short_waited < 20,
    }


ENDED_PROCESS = {"pid": "99999999", "started": "a process that has ended"}


def synthetic_session(root: Path, name: str, process: dict[str, str]) -> Path:
    work = root / name
    registry = work / "registry"
    registry.mkdir(parents=True)
    (registry / "launcher_test.json").write_text(json.dumps({"docker_socket": launcher.docker_socket_path()}))
    identity = run_lock.read_identity(launcher.resolve_config(str(registry)).registry)
    (work / LAUNCHER_PROCESS_FILE).write_text(json.dumps({**process, "registry_id": identity["registry_id"]}))
    (registry / "discardable.txt").write_text("synthetic expendable file\n")
    return work


def refusing_ps(original: Callable[..., Any]) -> Callable[..., Any]:
    def run(program: str, args: Any, **options: Any) -> Any:
        if program == "ps":
            return types.SimpleNamespace(returncode=1, stdout="", stderr="injected: ps cannot read the process table")
        return original(program, args, **options)

    return run


def refusing_rmdir(folder: Path) -> Callable[..., None]:
    real = os.rmdir

    def rmdir(path: Any, *args: Any, **options: Any) -> None:
        if Path(os.fsdecode(path)) == folder:
            raise PermissionError(errno.EPERM, "injected: the folder cannot be removed", str(path))
        real(path, *args, **options)

    return rmdir


def failing_sync(kind: str) -> Callable[[int], None]:
    real = os.fsync

    def sync(descriptor: int) -> None:
        if stat.S_ISDIR(os.fstat(descriptor).st_mode) == (kind == "directory"):
            raise OSError(errno.EIO, f"injected: the {kind} cannot be synchronized")
        real(descriptor)

    return sync


def check_record_sync(root: Path) -> dict[str, bool]:
    this_module = sys.modules[__name__]
    outcomes: dict[str, dict[str, bool]] = {}
    for kind in ("record", "directory"):
        work = synthetic_session(root, f"{kind}_sync", ENDED_PROCESS)
        originals = [*(work / "registry" / name for name in RECOVERY_INPUTS), work / LAUNCHER_PROCESS_FILE]
        with mock.patch("os.fsync", failing_sync("directory" if kind == "directory" else "record")):
            faulted = finish_cleanup(work)
        kept = all(path.is_file() for path in originals)
        failed = any("cleanup record" in item for item in faulted["errors"])
        stray = any(path.name.endswith(".new") for path in root.iterdir())
        recovered = finish_cleanup(work)
        outcomes[kind] = {
            "failed": failed and kept and not stray,
            "recovered": not recovered["errors"] and not work.exists() and not cleanup_record_path(work).exists(),
        }
    events: list[str] = []
    real_sync, real_remove = os.fsync, remove_entry

    def traced_sync(descriptor: int) -> None:
        events.append("directory" if stat.S_ISDIR(os.fstat(descriptor).st_mode) else "file")
        real_sync(descriptor)

    def traced_remove(path: Path, failures: list[str]) -> None:
        events.append("remove record" if path.name.endswith(".cleanup.json") else "remove session")
        real_remove(path, failures)

    ordered = synthetic_session(root, "ordered", ENDED_PROCESS)
    with mock.patch("os.fsync", traced_sync), mock.patch.object(this_module, "remove_entry", traced_remove):
        control = finish_cleanup(ordered)
    before = events[: events.index("remove session")] if "remove session" in events else []

    late = synthetic_session(root, "late_sync", ENDED_PROCESS)

    def sync_until_removed(descriptor: int) -> None:
        if stat.S_ISDIR(os.fstat(descriptor).st_mode) and not late.exists():
            raise OSError(errno.EIO, "injected: the parent folder cannot be synchronized after the folder is removed")
        real_sync(descriptor)

    with mock.patch("os.fsync", sync_until_removed):
        late_fault = finish_cleanup(late)
    late_failed = any("removal sync" in item for item in late_fault["errors"]) and not late.exists() and cleanup_record_path(late).is_file()
    late_recovered = finish_cleanup(late)
    return {
        "a cleanup record that cannot be synchronized fails the cleanup and keeps every original recovery input": outcomes["record"]["failed"],
        "a parent directory that cannot be synchronized fails the cleanup and keeps every original recovery input": outcomes["directory"]["failed"],
        "after the record synchronization fault clears, finishing the cleanup succeeds and removes the folder and the record": outcomes["record"]["recovered"],
        "after the directory synchronization fault clears, finishing the cleanup succeeds and removes the folder and the record": outcomes["directory"][
            "recovered"
        ],
        "the control: the record is synchronized, then its directory, right before the first removal of an original": not control["errors"]
        and before[-2:] == ["file", "directory"],
        "the control: the folder is removed, its parent folder is synchronized, then the record is removed": events[-5:]
        == ["file", "directory", "remove session", "directory", "remove record"],
        "a parent folder that cannot be synchronized after the folder is removed fails the cleanup and keeps the record": late_failed,
        "after the post-removal fault clears, finishing the cleanup from the retained record succeeds and removes the record": not late_recovered["errors"]
        and not cleanup_record_path(late).exists(),
    }


def check_removal_faults() -> dict[str, bool]:
    with tempfile.TemporaryDirectory(prefix="a2_recover_", dir=TEMP_ROOT) as name:
        root = Path(name)
        control = synthetic_session(root, "control", ENDED_PROCESS)
        control_result = finish_cleanup(control)

        stuck_work = synthetic_session(root, "stuck_input", ENDED_PROCESS)
        stuck = stuck_work / "registry" / "registry.json"
        set_immutable(stuck, True)
        try:
            stuck_result = finish_cleanup(stuck_work)
        finally:
            set_immutable(stuck, False)
        earlier_input_deleted = not (stuck_work / "registry" / "launcher_test.json").exists()
        stuck_record = cleanup_record_path(stuck_work).is_file()
        stuck_kept = stuck_work.exists()
        stuck_recovered = finish_cleanup(stuck_work)

        final_work = synthetic_session(root, "final_directory", ENDED_PROCESS)
        with mock.patch("os.rmdir", refusing_rmdir(final_work)):
            final_result = finish_cleanup(final_work)
        final_record = cleanup_record_path(final_work).is_file()
        final_kept = final_work.exists()
        final_recovered = finish_cleanup(final_work)
        synced = check_record_sync(root)
        return {
            "control: a cleanup with no fault removes the folder and leaves no cleanup record": not control_result["errors"]
            and not control.exists()
            and not cleanup_record_path(control).exists(),
            "a recovery input that cannot be removed fails the cleanup and keeps the folder": bool(stuck_result["errors"]) and stuck_kept,
            "the removal fault leaves an earlier recovery input deleted": earlier_input_deleted,
            "a cleanup record beside the folder stays until the cleanup completes after a recovery input fault": stuck_record,
            "after the fault is cleared, finishing the cleanup succeeds without the deleted input and removes the folder and the record": not stuck_recovered[
                "errors"
            ]
            and not stuck_work.exists()
            and not cleanup_record_path(stuck_work).exists(),
            "a final directory removal that fails fails the cleanup and keeps the folder": any("folder removal" in item for item in final_result["errors"])
            and final_kept,
            "a cleanup record beside the folder stays until the cleanup completes after a final directory fault": final_record,
            "after the final directory can be removed, finishing the cleanup succeeds and removes the folder and the record": not final_recovered["errors"]
            and not final_work.exists()
            and not cleanup_record_path(final_work).exists(),
            **synced,
        }


def check_unobserved_process() -> dict[str, bool]:
    this_module = sys.modules[__name__]
    with tempfile.TemporaryDirectory(prefix="a2_unknown_", dir=TEMP_ROOT) as name:
        root = Path(name)
        delayed = start_stand_in(root, 20)
        unknown, running, exited = (synthetic_session(root, item, delayed) for item in ("unknown", "running", "exited"))
        with mock.patch.object(run_lock, "run_command", refusing_ps(run_lock.run_command)), mock.patch.object(this_module, "LAUNCHER_WAIT_SECONDS", 2.0):
            unknown_result = finish_cleanup(unknown)
        os.kill(int(delayed["pid"]), 0)
        evidence_kept = all((unknown / "registry" / item).is_file() for item in ("registry.json", "launcher_test.json", "registry_id"))
        with mock.patch.object(this_module, "LAUNCHER_WAIT_SECONDS", 1.0):
            running_result = finish_cleanup(running)
        replaced = synthetic_session(root, "replaced", {**delayed, "started": "a different start time"})
        replaced_result = finish_cleanup(replaced)
        exited_result = finish_cleanup(exited)
        return {
            "a launcher process whose start time cannot be read is not reported as exited and its evidence is kept": any(
                "cannot be observed" in item for item in unknown_result["errors"]
            )
            and evidence_kept
            and not cleanup_record_path(unknown).exists(),
            "the control: a launcher process that still runs keeps the folder": any("still runs" in item for item in running_result["errors"])
            and running.exists(),
            "a launcher process with another start time counts as exited": not replaced_result["errors"] and not replaced.exists(),
            "once the launcher process has exited, finishing the cleanup succeeds and removes the folder": not exited_result["errors"] and not exited.exists(),
        }


def reject_removal_in_registry(work: Path) -> Path:
    return work / "registry" / "undeletable.txt"


def reject_removal_beside_registry(work: Path) -> Path:
    return work / "undeletable.txt"


def removal_inputs_last(work: Path, saved: dict[str, str]) -> list[str]:
    failures: list[str] = []
    registry = work / "registry"
    last = [*(registry / name for name in RECOVERY_INPUTS), work / LAUNCHER_PROCESS_FILE]
    for folder in (work, registry):
        for child in sorted(folder.iterdir()) if folder.is_dir() else []:
            if child != registry and child not in last:
                remove_entry(child, failures)
                if failures:
                    return failures
    for path in last:
        remove_entry(path, failures)
        if failures:
            return failures
    for folder in (registry, work):
        try:
            folder.rmdir()
        except FileNotFoundError:
            pass
        except OSError as error:
            failures.append(f"folder removal: {folder.name}: {type(error).__name__}")
            return failures
    return failures


def retire_without_sync(work: Path, saved: dict[str, str]) -> list[str]:
    record = cleanup_record_path(work)
    try:
        publish_record(record, {"work": str(work), "launcher_process": saved})
    except OSError as error:
        return [f"cleanup record: {record.name}: {type(error).__name__}"]
    failures: list[str] = []
    remove_entry(work, failures)
    if not work.exists():
        remove_entry(record, failures)
    return failures


def publish_without_sync(record: Path, body: dict[str, Any]) -> None:
    pending = record.with_name(f"{record.name}.new")
    pending.write_text(json.dumps(body, indent=2) + "\n")
    pending.replace(record)


def exit_by_lock(identity: dict[str, str], seconds: float) -> str:
    deadline = time.monotonic() + seconds
    while "lock" in identity and not lock_is_free(Path(identity["lock"])):
        if time.monotonic() > deadline:
            return RUNNING
        time.sleep(0.5)
    return EXITED


def exit_when_start_unreadable(identity: dict[str, str], seconds: float) -> str:
    deadline = time.monotonic() + seconds
    while run_lock.process_start(int(identity["pid"])) == identity["started"]:
        if time.monotonic() > deadline:
            return RUNNING
        time.sleep(0.5)
    return EXITED


CLEANUP_MUTANTS: dict[str, tuple[str, Callable[..., Any], str, str]] = {
    "removal_inputs_last": (
        "remove_work",
        removal_inputs_last,
        "the removal deletes the recovery inputs one by one with no record beside the folder, so a failure at one of them loses the earlier ones",
        "after the fault is cleared, finishing the cleanup succeeds without the deleted input and removes the folder and the record",
    ),
    "record_without_sync": (
        "publish_record",
        publish_without_sync,
        "the cleanup record is renamed into place without a flush, a synchronization or a folder synchronization before the originals are deleted",
        "a cleanup record that cannot be synchronized fails the cleanup and keeps every original recovery input",
    ),
    "retire_without_sync": (
        "remove_work",
        retire_without_sync,
        "the cleanup record is deleted as soon as the folder is gone, with no synchronization of the folder that held it",
        "a parent folder that cannot be synchronized after the folder is removed fails the cleanup and keeps the record",
    ),
    "exit_by_lock": (
        "launcher_exit",
        exit_by_lock,
        "the launcher counts as gone when its lock is free",
        "the wait for the launcher process does not return before the process has exited",
    ),
    "unreadable_start_is_exit": (
        "launcher_exit",
        exit_when_start_unreadable,
        "a start time that cannot be read counts as a different process, so the launcher counts as gone",
        "a launcher process whose start time cannot be read is not reported as exited and its evidence is kept",
    ),
}


def synthetic_cleanup_checks() -> dict[str, bool]:
    return {**check_delayed_worker(), **check_removal_faults(), **check_unobserved_process()}


def check_cleanup_mutant(mutant: str) -> int:
    target, replacement, what, intended = CLEANUP_MUTANTS[mutant]
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    with mock.patch.object(sys.modules[__name__], target, replacement):
        checks = synthetic_cleanup_checks()
    folder_out = OUT / f"cleanup_check_{stamp}_{mutant}"
    folder_out.mkdir(parents=True)
    verified = checks.get(intended) is False
    body = {"mutant": {"name": mutant, "what": what, "intended_check": intended}, "checks": checks, "failing_first_verified": verified}
    (folder_out / "report.json").write_text(json.dumps(body, indent=2) + "\n")
    for name, ok in checks.items():
        sys.stdout.write(f"{'ok' if ok else 'FAIL':<5} {name}\n")
    sys.stdout.write(
        f'{"failing-first verified" if verified else "NOT VERIFIED"}: {mutant}: the check "{intended}" {"failed" if verified else "did not fail"}\n'
    )
    return 0 if verified else 1


def check_cleanup_verdict() -> int:
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    this_module = sys.modules[__name__]
    original_teardown, original_call, original_finish = Supervisor.teardown, launcher.LauncherClient.call, finish
    planted: list[Path] = []

    def failing_teardown(self: Supervisor) -> dict[str, Any]:
        result = original_teardown(self)
        return {**result, "errors": [*result.get("errors", []), "injected: the stack teardown reports a failure"]}

    def failing_call(self: launcher.LauncherClient, method: str, *args: Any, **kwargs: Any) -> Any:
        answer = original_call(self, method, *args, **kwargs)
        if method == "shutdown":
            raise OSError("injected: the reply to the launcher shutdown was lost")
        return answer

    def finish_with_undeletable_file(place: Callable[[Path], Path]) -> Callable[[Stack, Path], dict[str, Any]]:
        def run(stack: Stack, work: Path) -> dict[str, Any]:
            fault = place(work)
            fault.write_text("synthetic file that cannot be removed\n")
            set_immutable(fault, True)
            planted.append(fault)
            return original_finish(stack, work)

        return run

    def undeletable_session(place: Callable[[Path], Path]) -> tuple[int, dict[str, Any], Path | None, Path | None]:
        before = set(OUT.glob("session_*"))
        with mock.patch.object(this_module, "finish", finish_with_undeletable_file(place)):
            code = run_session(["refused_specifications"])
        session_folder, session = newest_session(before)
        retained = (session.get("teardown") or {}).get("retained")
        return code, session, session_folder, Path(str(retained)) if retained else None

    before = set(OUT.glob("session_*"))
    control_code = run_session(["refused_specifications"])
    control_folder, control = newest_session(before)
    control_teardown = control.get("teardown") or {}

    before = set(OUT.glob("session_*"))
    with mock.patch.object(Supervisor, "teardown", failing_teardown), mock.patch.object(launcher.LauncherClient, "call", failing_call):
        code = run_session(["refused_specifications"])
    folder, report = newest_session(before)
    teardown = report.get("teardown") or {}
    retained = Path(str(teardown.get("retained", ""))) if teardown.get("retained") else None
    missing_input: dict[str, Any] = {"errors": []}
    created_in_place = True
    if retained is not None and (retained / "registry" / "registry.json").is_file():
        state_file = retained / "registry" / "registry.json"
        aside = retained / "registry.json.aside"
        state_file.rename(aside)
        listing = sorted(path.name for path in (retained / "registry").iterdir())
        missing_input = finish_cleanup(retained)
        created_in_place = sorted(path.name for path in (retained / "registry").iterdir()) != listing
        aside.rename(state_file)
    finished = finish_cleanup(retained) if retained is not None and retained.exists() else {"errors": ["no folder was kept"]}

    try:
        removal_code, removal, removal_folder, kept = undeletable_session(reject_removal_beside_registry)
        removal_teardown = removal.get("teardown") or {}
        stuck = finish_cleanup(kept) if kept is not None and kept.exists() else {"errors": ["no folder was kept"]}
        record_beside = kept is not None and cleanup_record_path(kept).is_file()

        inside_code, inside, inside_folder, inside_kept = undeletable_session(reject_removal_in_registry)
        inside_teardown = inside.get("teardown") or {}
        record_inside = inside_kept is not None and cleanup_record_path(inside_kept).is_file()
        inside_stuck = finish_cleanup(inside_kept) if inside_kept is not None and inside_kept.exists() else {"errors": ["no folder was kept"]}
    finally:
        for fault in planted:
            if fault.exists():
                set_immutable(fault, False)
    freed = finish_cleanup(kept) if kept is not None and kept.exists() else {"errors": ["no folder was kept"]}
    inside_freed = finish_cleanup(inside_kept) if inside_kept is not None and inside_kept.exists() else {"errors": ["no folder was kept"]}
    synthetic = synthetic_cleanup_checks()
    checks = {
        "control: a session with no fault exits zero, passes and removes its folder": control_code == 0
        and control.get("status") == "pass"
        and control_teardown.get("temporary_folder_removed") is True
        and not control_teardown.get("errors"),
        "the session with failing cleanup exits non-zero": code != 0,
        "the session report is a failure that names the teardown and the shutdown errors": report.get("status") == "fail"
        and any("teardown" in item for item in teardown.get("errors", []))
        and any("shutdown" in item for item in teardown.get("errors", [])),
        "the session folder was kept for the cleanup": retained is not None and teardown.get("temporary_folder_removed") is False,
        "finishing the cleanup from the kept folder succeeded and removed it": not finished["errors"] and finished.get("folder_removed") is True,
        "a file that cannot be removed fails the session, which exits non-zero": removal_code != 0 and removal.get("status") == "fail",
        "the report names the folder removal failure and keeps the folder": any(
            "folder removal: undeletable.txt" in item for item in removal_teardown.get("errors", [])
        )
        and removal_teardown.get("temporary_folder_removed") is False,
        "a cleanup record beside the kept folder finishes the cleanup after a removal failure": record_beside,
        "finishing the cleanup fails while the file cannot be removed": any("folder removal" in item for item in stuck["errors"])
        and stuck.get("folder_removed") is False,
        "after the fault is cleared, finishing the cleanup succeeds, removes the folder and removes the record": not freed["errors"]
        and freed.get("folder_removed") is True
        and kept is not None
        and not cleanup_record_path(kept).exists(),
        "a file inside the registry that cannot be removed fails the session, which exits non-zero": inside_code != 0 and inside.get("status") == "fail",
        "the report names the removal failure inside the registry and keeps the folder": any(
            "folder removal: undeletable.txt" in item for item in inside_teardown.get("errors", [])
        )
        and inside_teardown.get("temporary_folder_removed") is False,
        "a cleanup record beside the kept folder finishes the cleanup after a removal failure inside the registry": record_inside,
        "finishing the cleanup fails with the removal error while the file inside the registry is stuck": any(
            "folder removal" in item for item in inside_stuck["errors"]
        )
        and inside_stuck.get("folder_removed") is False,
        "before the cleanup is verified, a missing recovery input fails the cleanup and creates nothing in its place": any(
            "cleanup cannot be proven" in item for item in missing_input["errors"]
        )
        and not created_in_place,
        "after the fault inside the registry is cleared, finishing the cleanup succeeds and removes the folder": not inside_freed["errors"]
        and inside_freed.get("folder_removed") is True,
        **synthetic,
    }
    folder_out = OUT / f"cleanup_check_{stamp}"
    folder_out.mkdir(parents=True)
    status = "pass" if all(checks.values()) else "fail"
    body = {
        "feature": "a failed cleanup, including a folder that cannot be removed and a launcher process that outlives its lock, fails the session",
        "status": status,
        "checks": checks,
        "sessions": {
            "control": str(control_folder.relative_to(ROOT)) if control_folder else None,
            "teardown_and_shutdown_errors": str(folder.relative_to(ROOT)) if folder else None,
            "undeletable_file_beside_the_registry": str(removal_folder.relative_to(ROOT)) if removal_folder else None,
            "undeletable_file_inside_the_registry": str(inside_folder.relative_to(ROOT)) if inside_folder else None,
        },
        "finish_cleanup": {
            "after_errors": finished,
            "while_file_stuck": stuck,
            "after_fault_cleared": freed,
            "while_file_inside_the_registry_stuck": inside_stuck,
            "with_an_input_missing": missing_input,
            "inside_after_fault_cleared": inside_freed,
        },
    }
    (folder_out / "report.json").write_text(json.dumps(body, indent=2, default=str) + "\n")
    for name, ok in checks.items():
        sys.stdout.write(f"{'ok' if ok else 'FAIL':<5} {name}\n")
    sys.stdout.write(f"{status}: {(folder_out / 'report.json').relative_to(ROOT)}\n")
    return 0 if status == "pass" else 1


OFFLINE_SIGNING_VALUE = "offline-signing-value"
OFFLINE_PORT = 8080
OFFLINE_RUN = "20261015T000000Z-0a0a0a0a"
OFFLINE_OTHER_RUN = "20261015T000001Z-0b0b0b0b"
OFFLINE_SILVER = ["--dag", "silver_transform_dag", "--mode", "publish_fixture"]
OFFLINE_GOLD = ["--dag", "gold_transform_dag", "--mode", "publish_fixture"]


class FakeClock:
    """Time that moves only when a stand-in answer or a sleep moves it."""

    def __init__(self) -> None:
        self.elapsed = 0.0

    def monotonic(self) -> float:
        return 1000.0 + self.elapsed

    def time(self) -> float:
        return 1_800_000_000.0 + self.elapsed

    def sleep(self, seconds: float) -> None:
        if seconds < 0:
            raise ValueError("sleep length must be non-negative")
        self.elapsed += seconds


class FakeAirflow:
    """A stand-in for the loopback Airflow API: a workflow that starts paused, a run that ends ``ends_after`` seconds after its start while unpaused,
    a fixed time per answer and optional rejection (the answer then echoes the Authorization header) or refusal of the connection."""

    def __init__(
        self,
        clock: FakeClock,
        *,
        ends_after: float | None = 5.0,
        final: str = "success",
        reject: int | None = None,
        refuse: bool = False,
        status_delay: float = 0.0,
        ignore_timeout: bool = False,
    ) -> None:
        self.clock = clock
        self.ends_after, self.final, self.reject, self.refuse = ends_after, final, reject, refuse
        self.status_delay, self.ignore_timeout = status_delay, ignore_timeout
        self.requests: list[dict[str, Any]] = []
        self.paused = True
        self.started_at: float | None = None

    def answer(self, method: str, path: str, body: dict[str, Any] | None, headers: dict[str, str], timeout: float) -> tuple[int, Any]:
        self.requests.append({"method": method, "path": path, "body": body, "timeout": timeout, "at": self.clock.elapsed})
        if self.reject is not None:
            return self.reject, {"detail": f"SECRET-BODY {headers.get('Authorization', '')}"}
        if method == "PATCH" and body is not None:
            self.paused = bool(body["is_paused"])
            return 200, {}
        if method == "POST":
            self.started_at = self.clock.elapsed
            return 200, {}
        if "taskInstances" in path:
            return 200, {"task_instances": [{"task_id": "build", "state": self.final}], "total_entries": 1}
        ended = not self.paused and self.started_at is not None and self.ends_after is not None and self.clock.elapsed - self.started_at >= self.ends_after
        return 200, {"state": self.final if ended else "queued" if self.paused else "running"}

    def connection(self) -> type[Any]:
        stand_in = self

        class Connection:
            def __init__(self, host: str, port: int, timeout: float) -> None:
                self.timeout = timeout
                self.sock = None
                self.pending: tuple[str, str, dict[str, Any] | None, dict[str, str]] | None = None

            def connect(self) -> None:
                if stand_in.refuse:
                    raise ConnectionRefusedError

            def request(self, method: str, url: str, body: bytes | None = None, headers: dict[str, str] | None = None) -> None:
                self.pending = (method, url, None if body is None else json.loads(body), headers or {})

            def getresponse(self) -> Any:
                if self.pending is None:
                    raise ValueError("no request was made")
                method, url, body, headers = self.pending
                status, document = stand_in.answer(method, url.removeprefix("/api/v2"), body, headers, self.timeout)
                delay = stand_in.status_delay if method == "GET" and "/dagRuns/" in url else 0.0
                if delay > self.timeout and not stand_in.ignore_timeout:
                    stand_in.clock.elapsed += self.timeout
                    raise TimeoutError("timed out")
                stand_in.clock.elapsed += delay
                return types.SimpleNamespace(status=status, read=lambda: json.dumps(document).encode())

            def close(self) -> None:
                return

        return Connection


def drive_trigger(argv: list[str], stand_in: FakeAirflow, secret_delay: float = 0.0) -> dict[str, Any]:
    """Run the real ``trigger_dag.main`` with the stand-in clock and HTTP transport; only the secret store's client is replaced, so the real bounded
    read runs. ``secret_delay`` is a real-time delay of the secret read."""
    store_calls: list[str] = []

    def read(name: str) -> str:
        time.sleep(secret_delay)
        return OFFLINE_SIGNING_VALUE

    def open_store(root: Path, endpoint: str | None = None) -> Any:
        store_calls.append("open_store")
        return types.SimpleNamespace(get=read)

    out, err = io.StringIO(), io.StringIO()
    started = time.monotonic()
    with (
        mock.patch.object(trigger_dag, "time", stand_in.clock),
        mock.patch.object(airflow_api_module, "time", stand_in.clock),
        mock.patch.object(http.client, "HTTPConnection", stand_in.connection()),
        mock.patch.object(trigger_dag, "api_port", lambda: OFFLINE_PORT),
        mock.patch.object(secret_store, "open_store", open_store),
        contextlib.redirect_stdout(out),
        contextlib.redirect_stderr(err),
    ):
        code = trigger_dag.main(argv)
    return {
        "real_seconds": time.monotonic() - started,
        "exit_code": code,
        "stdout": out.getvalue(),
        "stderr": err.getvalue(),
        "store_calls": store_calls,
        "requests": stand_in.requests,
        "elapsed": stand_in.clock.elapsed,
    }


def trigger_refusals() -> dict[str, bool]:
    refused = {
        "an empty run ID": [*OFFLINE_SILVER, "--run-id", ""],
        "a run ID with a final newline": [*OFFLINE_SILVER, "--run-id", OFFLINE_RUN + "\n"],
        "a run ID of the wrong form": [*OFFLINE_SILVER, "--run-id", "run-1"],
        "an empty silver snapshot run": [*OFFLINE_GOLD, "--silver-snapshot-run", ""],
        "a silver snapshot run with a final newline": [*OFFLINE_GOLD, "--silver-snapshot-run", OFFLINE_RUN + "\n"],
        "a silver snapshot run on a silver run": [*OFFLINE_SILVER, "--silver-snapshot-run", OFFLINE_RUN],
    }
    checks: dict[str, bool] = {}
    for name, argv in refused.items():
        result = drive_trigger([*argv, "--wait-seconds", "60"], FakeAirflow(FakeClock()))
        checks[f"{name} is refused before the secret is read and before any request"] = (
            result["exit_code"] == 1 and result["store_calls"] == [] and result["requests"] == [] and result["stderr"].startswith("trigger_dag: refused")
        )
    for seconds in ("0", "-1", "nan", str(trigger_dag.MAX_WAIT_SECONDS + 1)):
        result = drive_trigger([*OFFLINE_SILVER, "--wait-seconds", seconds], FakeAirflow(FakeClock()))
        checks[f"--wait-seconds {seconds} is refused before the secret is read"] = (
            result["exit_code"] == 1 and result["store_calls"] == [] and result["requests"] == []
        )
    return checks


def trigger_runs() -> dict[str, bool]:
    silver = drive_trigger([*OFFLINE_SILVER, "--wait-seconds", "60"], FakeAirflow(FakeClock()))
    posted = [item for item in silver["requests"] if item["method"] == "POST"]
    shown = printed_result(silver["stdout"])
    gold = drive_trigger([*OFFLINE_GOLD, "--wait-seconds", "60"], FakeAirflow(FakeClock()))
    pinned = drive_trigger(
        [*OFFLINE_GOLD, "--silver-snapshot-run", OFFLINE_OTHER_RUN, "--run-id", OFFLINE_RUN, "--wait-seconds", "60"], FakeAirflow(FakeClock())
    )
    failed = drive_trigger([*OFFLINE_SILVER, "--run-id", OFFLINE_RUN, "--wait-seconds", "60"], FakeAirflow(FakeClock(), final="failed"))
    return {
        "the workflow is unpaused before the run is posted": [(item["method"], item["body"]) for item in silver["requests"][:2]]
        == [("PATCH", {"is_paused": False}), ("POST", posted[0]["body"])],
        "a run on a workflow that starts paused ends in success and exits 0": silver["exit_code"] == 0 and shown.get("state") == "success",
        "the run ID is generated only when none is given": len(posted) == 1
        and trigger_dag.RUN_ID.fullmatch(posted[0]["body"]["dag_run_id"]) is not None
        and posted[0]["body"]["conf"] == {"run_id": posted[0]["body"]["dag_run_id"], "mode": "publish_fixture"},
        "a gold run without a snapshot reads latest": next(item for item in gold["requests"] if item["method"] == "POST")["body"]["conf"]["silver_snapshot_run"]
        == "latest",
        "a gold run keeps the pinned snapshot": next(item for item in pinned["requests"] if item["method"] == "POST")["body"]["conf"]["silver_snapshot_run"]
        == OFFLINE_OTHER_RUN,
        "a failed run exits 1 and prints the state failed and the task states": failed["exit_code"] == 1
        and printed_result(failed["stdout"]).get("state") == "failed"
        and printed_result(failed["stdout"]).get("tasks") == {"build": "failed"},
    }


def trigger_deadlines() -> dict[str, bool]:
    checks: dict[str, bool] = {}
    queued = drive_trigger([*OFFLINE_SILVER, "--run-id", OFFLINE_RUN, "--wait-seconds", "1"], FakeAirflow(FakeClock(), ends_after=1.5))
    checks["a run that would end after the wait exits 1 at the deadline, not after it"] = (
        queued["exit_code"] == 1 and queued["elapsed"] <= 1.0 and queued["stdout"] == "" and "not ended within 1 s" in queued["stderr"]
    )
    checks["the error names the workflow and the run ID"] = "silver_transform_dag" in queued["stderr"] and OFFLINE_RUN in queued["stderr"]
    checks["every request is limited to the time that remains"] = all(item["timeout"] <= 1.0 - item["at"] + 1e-9 for item in queued["requests"])
    late = drive_trigger(
        [*OFFLINE_SILVER, "--run-id", OFFLINE_RUN, "--wait-seconds", "5"],
        FakeAirflow(FakeClock(), ends_after=0.0, status_delay=10.0, ignore_timeout=True),
    )
    checks["a terminal answer that completes after the deadline is not accepted"] = (
        late["exit_code"] == 1 and late["stdout"] == "" and "wait deadline passed" in late["stderr"]
    )
    slow = drive_trigger([*OFFLINE_SILVER, "--run-id", OFFLINE_RUN, "--wait-seconds", "5"], FakeAirflow(FakeClock(), status_delay=10.0))
    checks["a status answer slower than the remaining time ends the wait at the deadline"] = slow["exit_code"] == 1 and slow["elapsed"] <= 5.0
    stalled = FakeAirflow(FakeClock())
    secret = drive_trigger([*OFFLINE_SILVER, "--run-id", OFFLINE_RUN, "--wait-seconds", "0.2"], stalled, secret_delay=1.0)
    checks["a secret read slower than the wait ends at the deadline with no API request"] = (
        secret["exit_code"] == 1
        and secret["real_seconds"] < 0.8
        and secret["requests"] == []
        and secret["stdout"] == ""
        and "did not finish within" in secret["stderr"]
    )
    return checks


def trigger_error_output() -> dict[str, bool]:
    checks: dict[str, bool] = {}
    for status in (401, 500):
        result = drive_trigger([*OFFLINE_SILVER, "--run-id", OFFLINE_RUN, "--wait-seconds", "60"], FakeAirflow(FakeClock(), reject=status))
        text = result["stdout"] + result["stderr"]
        checks[f"a {status} answer prints the workflow, the run ID and the status, and no response body, token or secret"] = (
            result["exit_code"] == 1
            and f"HTTP status {status}" in result["stderr"]
            and "silver_transform_dag" in result["stderr"]
            and OFFLINE_RUN in result["stderr"]
            and not any(marker in text for marker in ("SECRET-BODY", "Bearer", "eyJ", OFFLINE_SIGNING_VALUE))
        )
    refused = drive_trigger([*OFFLINE_SILVER, "--run-id", OFFLINE_RUN, "--wait-seconds", "60"], FakeAirflow(FakeClock(), refuse=True))
    checks["a refused connection prints the exception class and nothing more"] = (
        refused["exit_code"] == 1 and "ConnectionRefusedError" in refused["stderr"] and OFFLINE_SIGNING_VALUE not in refused["stderr"]
    )
    return checks


def check_trigger_offline() -> dict[str, bool]:
    return {**trigger_refusals(), **trigger_runs(), **trigger_deadlines(), **trigger_error_output()}


def check_trigger_verdict() -> int:
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    checks = check_trigger_offline()
    folder = OUT / f"trigger_check_{stamp}"
    folder.mkdir(parents=True)
    (folder / "report.json").write_text(
        json.dumps({"command": ".venv/bin/python -m scripts.orchestration.run_transform_e2e --trigger-check", "checks": checks}, indent=2) + "\n"
    )
    for name, ok in checks.items():
        sys.stdout.write(f"{'ok' if ok else 'FAIL':<5} {name}\n")
    sys.stdout.write(f"{'pass' if all(checks.values()) else 'fail'}: {(folder / 'report.json').relative_to(ROOT)}\n")
    return 0 if all(checks.values()) else 1


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--case", action="append", default=[], choices=sorted(CASES))
    parser.add_argument("--keep-work", action="store_true", help="keep the temporary folder (logs, ledger, job output) for a look after the run")
    parser.add_argument("--list", action="store_true", help="print the case names")
    parser.add_argument("--shipped", action="store_true", help="run the shipped fixture rows on the kit's seed (long; needs the dbt packages)")
    parser.add_argument("--mutants", action="store_true", help="run the mutants instead of the cases (failing-first evidence)")
    parser.add_argument("--mutant", action="append", default=[], help="with --mutants: only this mutant, or CASE/MUTANT")
    parser.add_argument(
        "--cleanup-check", action="store_true", help="prove that a failed teardown or launcher shutdown fails the session and keeps what finishes it"
    )
    parser.add_argument("--trigger-check", action="store_true", help="check trigger_dag with a stand-in clock, API and secret store (no Docker, no stack)")
    parser.add_argument("--finish-cleanup", type=Path, metavar="FOLDER", help="finish the cleanup of a session folder that a failed run kept")
    parser.add_argument(
        "--cleanup-mutant", choices=sorted(CLEANUP_MUTANTS), default="", help="with --cleanup-check: run the synthetic checks with an earlier behavior in place"
    )
    parser.add_argument("--linger", nargs=2, metavar=("FOLDER", "SECONDS"), help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.linger:
        return linger(Path(args.linger[0]), float(args.linger[1]))
    if args.list:
        sys.stdout.write("\n".join(CASE_ORDER) + "\n")
        return 0
    if args.trigger_check:
        return check_trigger_verdict()
    if args.cleanup_check:
        return check_cleanup_mutant(args.cleanup_mutant) if args.cleanup_mutant else check_cleanup_verdict()
    if args.finish_cleanup is not None:
        outcome = finish_cleanup(args.finish_cleanup.resolve())
        sys.stdout.write(json.dumps(outcome, indent=2, default=str) + "\n")
        return 1 if outcome["errors"] else 0
    if args.shipped:
        return run_session(list(SHIPPED_CASES), shipped=True, keep_work=args.keep_work)
    return run_session(args.case, args.mutants, keep_work=args.keep_work, only=args.mutant)


if __name__ == "__main__":
    sys.exit(main())
