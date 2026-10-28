"""E2E of the orchestration stack on real Docker, started and stopped only through the launcher, in a test registry (failure modes 925, 931 to 934; Airflow
plan modes 755, 763, 774 and 764).

Run ``.venv/bin/python -m scripts.orchestration.run_real_e2e`` from the repository root with Docker Desktop running, the pinned PostgreSQL image present
and the Airflow image built (the command is in ``services/orchestration/Dockerfile``). It makes no Docker call: a launcher on a test registry under a
new ``/tmp`` folder starts PostgreSQL, the migration and the four Airflow services (the supervisor's session), and a supervisor-owned broker listens on
the loopback. Two checks run inside Airflow: a test DAG (``broker_probe_auto``, in a copy of the DAG folder under the ``/tmp`` folder, never in ``dags/``)
runs once through the scheduler and LocalExecutor and calls the broker with the session token, with a wrong token and with a submission whose template
has no launcher row. Two more tasks submit fixture jobs through the broker (a test inventory in the ``/tmp`` folder maps ``silver_build`` and ``gold_build``
in mode ``fixture`` to ``fixture_dbt`` on a tiny dbt project: one parses, one is broken), so the run has a successful and a forced-failure job, and a third
task writes a harmless control marker to XCom. The run then makes a logical dump of the metadata database and a logical XCom query through the launcher's
``orchestration_dump`` template, and scans those, Airflow's task logs and raw database files, the broker's log, ledger, records, saved job logs and forward
cache, and the launcher's registry for the session token, the run token and synthetic credentials planted in this process's environment. The scan fails when
any input is unreadable, skipped or missing, or when the control marker is not found where it must be. It then checks that the listener and the API port
refuse every other address of this Mac, takes the session down and asks the launcher for the stack's state. The ``/tmp`` folder is removed at the end. The
report is ``data/e2e/orchestration_real/<UTC>/report.json``; it is withheld when a secret is found in it.
"""

from __future__ import annotations

import contextlib
import json
import os
import secrets
import shutil
import socket
import sys
import tempfile
import time
import traceback
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from orchestration import launcher
from scripts.infrastructure.secret_store_stand_in import StandIn, stack_seeds
from scripts.lakehouse import launcher_cleanup, run_lock
from scripts.orchestration import sentinel
from scripts.orchestration.broker import BrokerPaths
from scripts.orchestration.net_checks import ipv6_loopback_open, unreachable_elsewhere
from scripts.orchestration.supervisor import Supervisor, SupervisorError
from scripts.process import run_command

ROOT = Path(__file__).resolve().parents[2]
# The run's own folder is made inside this one with a random name and mode 0700.
TEMP_ROOT = Path(os.sep, "tmp")
TEARDOWN_SECONDS = 300.0
OUT = ROOT / "data" / "e2e" / "orchestration_real"
PLATFORM = json.loads((ROOT / "config" / "orchestration" / "platform.json").read_text())
SENTINELS = {
    "AWS_SECRET_ACCESS_KEY": "sentinel-aws-" + secrets.token_hex(8),
    "POLARIS_CLIENT_SECRET": "sentinel-polaris-" + secrets.token_hex(8),
    "RUN_LOCK_TOKEN": "sentinel-runlock-" + secrets.token_hex(8),
}
# A harmless value the control task writes to XCom: the scan must find it in the logical XCom rows and in the dump, or the scan could not see the data.
CONTROL_MARKER = "control-marker-" + secrets.token_hex(8)
RUN = "20261013T090000Z-0a1b2c3d"
TEST_DAG = '''"""Test-only DAG of the real E2E: it runs once when the scheduler first sees it (never part of dags/)."""

from __future__ import annotations

import json
import time
from datetime import UTC, datetime

from airflow.sdk import DAG, get_current_context, task

from orchestration.client import SESSION_FOLDER, BrokerClient, BrokerUnavailable
from orchestration.interface import Handle, State, Status, SubmissionRefused, SubmissionRequest

RUN = "__RUN__"
TERMINAL = (State.SUCCEEDED, State.FAILED, State.FAILED_CLOSED, State.CANCELLED, State.NO_OP)


def submission(template: str, mode: str) -> SubmissionRequest:
    """Use the scheduler task's actual caller identity in every broker request."""
    instance = get_current_context()["ti"]
    return SubmissionRequest(template, RUN, mode, 1, instance.dag_id, instance.task_id, instance.run_id)


def wait_terminal(client: BrokerClient, handle: Handle, seconds: float = 420.0) -> Status:
    """Poll the broker until the submission ends."""
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        status = client.status(handle)
        if status.state in TERMINAL:
            return status
        time.sleep(3)
    raise RuntimeError("FIXTURE_TIMEOUT")


@task
def with_token() -> str:
    """The broker answers a call that carries this session's token."""
    return "HEALTH " + json.dumps(BrokerClient.from_session().health(), sort_keys=True)


@task
def with_wrong_token() -> str:
    """The broker refuses a call with a token it did not issue."""
    settings = json.loads((SESSION_FOLDER / "client.json").read_text())
    try:
        BrokerClient(str(settings["url"]), "x" * 64).health()
    except BrokerUnavailable as error:
        return f"WRONG_TOKEN {error}"
    return "WRONG_TOKEN accepted"


@task
def unmapped_submission() -> str:
    """A template with no launcher row is refused by the broker with its reason."""
    try:
        BrokerClient.from_session().submit(submission("gold_build", "real"))
    except SubmissionRefused as error:
        return f"REFUSED {error}"
    return "REFUSED not refused"


@task
def fixture_success() -> str:
    """A fixture job that succeeds, submitted through the broker."""
    client = BrokerClient.from_session()
    status = wait_terminal(client, client.submit(submission("silver_build", "fixture")))
    if status.state != State.SUCCEEDED:
        raise RuntimeError(f"FIXTURE_UNEXPECTED state={status.state.value} exit={status.exit_code}")
    return f"FIXTURE_OK state={status.state.value} exit={status.exit_code}"


@task
def fixture_failure() -> str:
    """A fixture job that is forced to fail; the task fails with the job's end state."""
    client = BrokerClient.from_session()
    status = wait_terminal(client, client.submit(submission("gold_build", "fixture")))
    raise RuntimeError(f"FIXTURE_FAILED state={status.state.value} exit={status.exit_code}")


@task
def control_marker() -> str:
    """A harmless value in XCom: the scan has to find it."""
    return "CONTROL __CONTROL__"


with DAG(dag_id="broker_probe_auto", schedule="@once", start_date=datetime(2026, 1, 1, tzinfo=UTC), catchup=False, is_paused_upon_creation=False):
    with_token()
    with_wrong_token()
    unmapped_submission()
    fixture_success()
    fixture_failure()
    control_marker()
'''.replace("__RUN__", RUN).replace("__CONTROL__", CONTROL_MARKER)
TASKS = ("with_token", "with_wrong_token", "unmapped_submission", "fixture_success", "control_marker")
FAILING_TASKS = ("fixture_failure",)
# A tiny dbt project per case: the good one parses, the broken one names a model that does not exist (dbt exits 2).
DBT_PROFILE = "sentinel_probe:\n  target: fixture\n  outputs:\n    fixture:\n      type: duckdb\n      path: ':memory:'\n      threads: 1\n"
DBT_PROJECT = "name: sentinel_probe\nversion: '1.0.0'\nprofile: sentinel_probe\nmodel-paths: ['models']\n"
DBT_MODELS = {"ok": "select 1 as id\n", "bad": "select * from {{ ref('model_that_does_not_exist') }}\n"}


class Checks:
    """Named results with evidence."""

    def __init__(self) -> None:
        self.results: dict[str, dict[str, Any]] = {}

    def check(self, name: str, ok: bool, **evidence: Any) -> bool:
        """Record one result."""
        self.results[name] = {"ok": bool(ok), **evidence}
        sys.stdout.write(f"{'ok' if ok else 'FAIL':<5} {name}\n")
        sys.stdout.flush()
        return bool(ok)


def free_port() -> int:
    """A free loopback port."""
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return int(probe.getsockname()[1])


def prepare_checkout(folder: Path) -> Path:
    """A copy of what the stack mounts, so the test DAG never enters ``dags/``: Compose files, the DAG-side modules, the DAG folder and the files the jobs
    mount. Returns the test broker inventory: the real file with ``silver_build`` and ``gold_build`` in mode ``fixture`` mapped to ``fixture_dbt`` on a
    good and a broken dbt project (the real file keeps every other row)."""
    (folder / "services" / "orchestration").mkdir(parents=True)
    (folder / "services" / "analytics").mkdir(parents=True)
    (folder / "scripts" / "orchestration").mkdir(parents=True)
    (folder / "orchestration").mkdir()
    (folder / "dags").mkdir()
    shutil.copy2(ROOT / "docker-compose.yaml", folder / "docker-compose.yaml")
    shutil.copy2(ROOT / "services" / "analytics" / "resources.sql", folder / "services" / "analytics" / "resources.sql")
    shutil.copy2(ROOT / PLATFORM["compose"]["file"], folder / PLATFORM["compose"]["file"])
    shutil.copy2(ROOT / "scripts" / "orchestration" / "db_dump.py", folder / "scripts" / "orchestration" / "db_dump.py")
    for name in ("__init__.py", "interface.py", "client.py"):
        shutil.copy2(ROOT / "orchestration" / name, folder / "orchestration" / name)
    shutil.copy2(ROOT / "dags" / "broker_probe_dag.py", folder / "dags" / "broker_probe_dag.py")
    (folder / "dags" / "broker_probe_auto.py").write_text(TEST_DAG)
    cases = folder.parent / "cases"
    (folder.parent / "packages").mkdir()
    for name, model in DBT_MODELS.items():
        project = cases / name / "project"
        (project / "models").mkdir(parents=True)
        (project / "dbt_project.yml").write_text(DBT_PROJECT)
        (project / "profiles.yml").write_text(DBT_PROFILE)
        (project / "models" / "probe.sql").write_text(model)
    document = json.loads((ROOT / "config" / "orchestration" / "broker_templates.json").read_text())
    for template, case_name, seconds in (("silver_build", "ok", 300), ("gold_build", "bad", 300)):
        document["templates"][template]["modes"]["fixture"] = {
            "class": "offline",
            "no_op": False,
            "launcher": {
                "template": "fixture_dbt",
                "args": ["parse"],
                "params": {"case_host": str(cases / case_name), "case_container": "/workspace/out/case", "packages_host": str(folder.parent / "packages")},
                "timeout_seconds": seconds,
            },
        }
    inventory = folder.parent / "broker_templates_test.json"
    inventory.write_text(json.dumps(document, indent=2))
    return inventory


def write_registry_settings(registry: Path, work: Path, endpoint: str) -> None:
    """The launcher's settings for this test registry: the real Docker socket, a longer health wait and the state, case and package folders as allowed roots."""
    settings = {
        "docker_socket": launcher.docker_socket_path(),
        "timings": {"health_wait": 300.0, "stop_grace": 30, "docker_call_timeout": 120.0},
        "allowed_roots": [str(work / "airflow"), str(work / "cases"), str(work / "packages")],
        "token_root": str(registry / "run_tokens" / "t"),
        "runs_root": str(work / "runs"),
        "secrets_endpoint": endpoint,
    }
    (registry / "launcher_test.json").write_text(json.dumps(settings, indent=2))


def task_log(logs: Path, task_id: str) -> str:
    """The text of a task's first attempt log, or an empty string."""
    found = sorted(logs.glob(f"dag_id=broker_probe_auto/run_id=*/task_id={task_id}/attempt=1.log"))
    return found[0].read_text(errors="replace") if found else ""


def run(checks: Checks, work: Path, nonce: str) -> dict[str, Any]:
    """The whole scenario against a stand-in Parameter Store; the caller removes the folder and stops what is left."""
    with StandIn(stack_seeds()) as stand_in:
        return run_session(checks, work, stand_in.endpoint, nonce)


def run_session(checks: Checks, work: Path, endpoint: str, nonce: str) -> dict[str, Any]:
    """The scenario with the launcher's secrets read from the stand-in at ``endpoint``."""
    evidence: dict[str, Any] = {}
    registry = work / "registry"
    registry.mkdir(mode=0o700)
    write_registry_settings(registry, work, endpoint)
    checkout = work / "checkout"
    inventory = prepare_checkout(checkout)
    os.environ.update(SENTINELS)
    config = launcher.resolve_config(str(registry))
    client = launcher.LauncherClient(config, start=True)
    client.launch_nonce = nonce
    client.connect().close()
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline and client.call("status", {"what": "launcher"})["status"] != "serving":
        time.sleep(0.5)
    launcher_view = client.call("status", {"what": "launcher"})
    evidence["launcher"] = {key: launcher_view[key] for key in ("status", "degraded_reason", "foreign_legacy_containers")}
    if not checks.check("the launcher on the test registry serves real Docker", launcher_view["status"] == "serving", **evidence["launcher"]):
        return evidence
    plan = client.call("plan")
    evidence["plan"] = {key: plan.get(key) for key in ("free", "threads", "total") if key in plan}
    paths = BrokerPaths(run_lock.Registry(registry, "test"), work)
    supervisor = Supervisor(
        paths,
        PLATFORM,
        client,
        api_port=free_port(),
        broker_port=0,
        readiness_seconds=480,
        templates_file=inventory,
        repo_root=checkout,
    )
    started = time.monotonic()
    try:
        started_evidence = supervisor.start()
    except SupervisorError as error:
        checks.check("the supervisor started the stack through the launcher", False, error=str(error)[:600])
        evidence["stack"] = client.call("status", {"what": "orchestration"})
        supervisor.teardown()
        return evidence
    evidence["start"] = {"seconds": round(time.monotonic() - started, 1), **{key: started_evidence[key] for key in ("images", "settings", "health", "start")}}
    checks.check(
        "the stack is up and the API reports every component healthy",
        started_evidence["health"] == dict.fromkeys(("metadatabase", "scheduler", "triggerer", "dag_processor"), "healthy"),
        **evidence["start"],
    )
    listener_port = supervisor.listener.port if supervisor.listener else 0
    token = supervisor.token
    state = work / "airflow"
    try:
        specs(checks, config, token, evidence)
        probe(checks, state, evidence)
        ledger_of_jobs(checks, work, evidence)
        run_token = supervisor.broker.principal(RUN)["token"] if supervisor.broker else ""
        dumps(checks, client, state, checkout, evidence)
        networks(checks, listener_port, supervisor.api_port)
        sentinel_checks(checks, work, [token, run_token], evidence)
    finally:
        evidence["teardown"] = supervisor.teardown()
    view = client.call("status", {"what": "orchestration"})
    launcher_after = client.call("status", {"what": "launcher"})
    left_containers = [name for name in launcher_after["containers"] if "-svc-orch-" in name]
    left_networks = [name for name in launcher_after["networks"] if name.endswith("-orch-net")]
    checks.check(
        "the launcher confirms that nothing of the stack is left",
        view["phase"] == "down" and not left_containers and not left_networks and evidence["teardown"]["stack"]["phase"] == "down",
        phase=view["phase"],
        containers=left_containers,
        networks=left_networks,
    )
    checks.check(
        "the session token and client file are deleted", not (state / "session" / "broker_token").exists() and not (state / "session" / "client.json").exists()
    )
    checks.check("the listener port is closed", not port_accepts(listener_port))
    offline_with_image(checks, registry, evidence)
    with contextlib.suppress(launcher.ClientError, OSError):
        client.call("shutdown", {"owner": True}, {"kind": "any", "owner": True})
    evidence["catalog_phase"] = (launcher_after.get("catalog") or {}).get("phase", "down")
    return evidence


def offline_with_image(checks: Checks, registry: Path, evidence: dict[str, Any]) -> None:
    """The offline checks with the built image, through this launcher: the image's labels, user and layers, the DAG import in the image, the graph validator
    on real Airflow DAG objects and every installed distribution against the constraints file."""
    environment = {name: os.environ[name] for name in ("PATH", "HOME", "TMPDIR") if name in os.environ}
    args = ["-m", "scripts.orchestration.offline_checks", "--test-registry", str(registry)]
    result = run_command(sys.executable, args, cwd=ROOT, env=environment, timeout=1200)
    lines = result.stdout.splitlines()
    failed = [line for line in lines if line.startswith("FAIL")]
    evidence["offline_checks"] = {"exit_code": result.returncode, "last_line": lines[-1] if lines else "", "failed": failed[:10], "checks": len(lines) - 1}
    checks.check("the offline checks with the built image pass through the launcher", result.returncode == 0, **evidence["offline_checks"])


def port_accepts(port: int) -> bool:
    """True when something accepts a connection on the loopback port."""
    probe = socket.socket()
    probe.settimeout(1)
    try:
        probe.connect(("127.0.0.1", port))
    except OSError:
        return False
    finally:
        probe.close()
    return True


def specs(checks: Checks, config: launcher.Config, token: str, evidence: dict[str, Any]) -> None:
    """The effective specifications the launcher recorded for the real containers: no data credential, no socket, no token, loopback port only."""
    registry = json.loads((config.registry.path / "registry.json").read_text())
    creates = [op for op in registry["operations"].values() if op["kind"] == "create" and "-svc-orch-" in op["name"]]
    projections = {op["name"].rsplit("-svc-orch-", 1)[1]: op["projection"] for op in creates}
    env_names = {name for projection in projections.values() for name in projection["env"]}
    mounts = [mount for projection in projections.values() for mount in projection["mounts"]]
    ports = {name: projection["ports"] for name, projection in projections.items() if projection["ports"]}
    text = json.dumps(registry)
    evidence["specs"] = {"services": sorted(projections), "ports": ports}
    checks.check(
        "every service was created from a recorded specification",
        set(projections) == {"postgres", "airflow-migrate", "airflow-apiserver", "airflow-scheduler", "airflow-dag-processor", "airflow-triggerer"},
        services=sorted(projections),
    )
    checks.check(
        "no service has a data credential variable",
        not any(name.startswith(("AWS_", "POLARIS", "RUN_LOCK")) for name in env_names),
        env_names=sorted(name for name in env_names if name.startswith(("AWS", "POLARIS", "RUN_LOCK"))),
    )
    checks.check(
        "no mount is a socket, an AWS folder or a lakehouse secret",
        not any(m[0].endswith(".sock") or ".aws" in m[0] or "lakehouse/secrets" in m[0] for m in mounts),
    )
    checks.check(
        "only the API server publishes a port, on the loopback",
        list(ports) == ["airflow-apiserver"] and all(entry[1] == "127.0.0.1" for entry in ports["airflow-apiserver"]),
        ports=ports,
    )
    checks.check(
        "the launcher's registry holds neither the session token nor a planted credential",
        token not in text and not any(value in text for value in SENTINELS.values()),
    )
    checks.check(
        "the migration ran behind the deadline entrypoint",
        projections["airflow-migrate"]["entrypoint"][0] == launcher.DEADLINE_ENTRYPOINT,
        entrypoint=projections["airflow-migrate"]["entrypoint"],
    )


def probe(checks: Checks, state: Path, evidence: dict[str, Any]) -> None:
    """The test DAG runs once through the scheduler and LocalExecutor; each task's log shows what the broker answered. Five tasks succeed; the forced-failure
    fixture task fails with its job's end state."""
    logs = state / "logs"
    deadline = time.monotonic() + 600
    finished: dict[str, bool] = dict.fromkeys((*TASKS, *FAILING_TASKS), False)
    while time.monotonic() < deadline and not all(finished.values()):
        for name in TASKS:
            finished[name] = "Done. Returned value was" in task_log(logs, name)  # Airflow 3 logs this line only when the task body returned
        for name in FAILING_TASKS:
            finished[name] = "FIXTURE_FAILED state=" in task_log(logs, name).replace("\\", "")
        time.sleep(3)
    texts = {name: task_log(logs, name).replace("\\", "") for name in (*TASKS, *FAILING_TASKS)}  # the log is JSON lines: quotes inside a message are escaped
    markers = ("HEALTH", "WRONG_TOKEN", "REFUSED", "FIXTURE_OK", "FIXTURE_FAILED", "FIXTURE_UNEXPECTED")
    evidence["probe"] = {name: [line[-200:] for line in text.splitlines() if any(marker in line for marker in markers)][:3] for name, text in texts.items()}
    checks.check("the six test tasks ran through LocalExecutor: five succeeded and the forced-failure task failed", all(finished.values()), finished=finished)
    checks.check(
        "a task in the scheduler reached the broker with the session token",
        "HEALTH" in texts["with_token"] and '"broker": "ok"' in texts["with_token"],
        log=evidence["probe"]["with_token"],
    )
    checks.check(
        "the same task without the token was refused with 401",
        "WRONG_TOKEN" in texts["with_wrong_token"] and "401" in texts["with_wrong_token"],
        log=evidence["probe"]["with_wrong_token"],
    )
    checks.check(
        "a template with no launcher row was refused with its reason through Airflow",
        "REFUSED" in texts["unmapped_submission"] and "no launcher template runs gold_build" in texts["unmapped_submission"],
        log=evidence["probe"]["unmapped_submission"],
    )
    checks.check(
        "a fixture job submitted by an Airflow task through the broker succeeded",
        "FIXTURE_OK state=succeeded exit=0" in texts["fixture_success"],
        log=evidence["probe"]["fixture_success"],
    )
    failed_line = next((line for line in texts["fixture_failure"].splitlines() if "FIXTURE_FAILED state=" in line), "")
    checks.check(
        "a forced-failure fixture job failed with a non-zero exit code and failed its Airflow task",
        "FIXTURE_FAILED state=failed exit=" in failed_line and "exit=0" not in failed_line and "exit=None" not in failed_line,
        log=evidence["probe"]["fixture_failure"],
    )


def ledger_of_jobs(checks: Checks, work: Path, evidence: dict[str, Any]) -> None:
    """The broker's ledger holds both fixture submissions in order and each has its record, saved job log and forward cache."""
    ledger = work / "ledger" / f"{RUN}.jsonl"
    lines = [json.loads(line) for line in ledger.read_text().splitlines()] if ledger.is_file() else []
    by_submission: dict[str, list[str]] = {}
    for line in lines:
        by_submission.setdefault(line["submission_id"], []).append(line["event"])
    wanted = {f"{RUN}:silver_build:fixture:1", f"{RUN}:gold_build:fixture:1"}
    exits = {line["submission_id"]: line.get("exit_code") for line in lines if line["event"] == "finished"}
    evidence["jobs"] = {
        "events": {key.split(":")[1]: value for key, value in by_submission.items()},
        "exit_codes": {key.split(":")[1]: value for key, value in exits.items()},
    }
    checks.check(
        "the ledger holds both fixture jobs in order, one succeeded and one failed",
        wanted <= set(by_submission)
        and all(by_submission[key][0] == "intent" and by_submission[key][-1] == "finished" for key in wanted)
        and exits.get(f"{RUN}:silver_build:fixture:1") == 0
        and exits.get(f"{RUN}:gold_build:fixture:1") not in (0, None),
        **evidence["jobs"],
    )
    counts = {
        name: sum(1 for item in (work / name).rglob("*") if item.is_file()) if (work / name).is_dir() else 0 for name in ("records", "job_logs", "forwards")
    }
    checks.check("each fixture job has a record, a saved job log and a forward cache file", all(count >= 2 for count in counts.values()), files=counts)


def dumps(checks: Checks, client: launcher.LauncherClient, state: Path, checkout: Path, evidence: dict[str, Any]) -> None:
    """The logical dump of the metadata database and the logical XCom rows, made by the orchestration_dump template through the launcher, with the stack
    up (the allocation is made now, so its plan holds back the stack's growth reserve)."""
    folder = state / "dumps"
    results: dict[str, Any] = {}
    allocation = client.allocate()
    try:
        principal = client.principal_alloc(allocation)
        for what in ("metadata", "xcom"):
            outcome = client.run_attempt(principal, "orchestration_dump", f"dump-{what}", [what], {"state": str(state)}, checkout=checkout, seconds=300)
            results[what] = {"ok": outcome.ok, "reason": outcome.reason, "exit_code": outcome.exit_code, "output": outcome.stdout.strip()[-200:]}
    finally:
        client.release_allocation(allocation)
    sizes = {name: (folder / name).stat().st_size if (folder / name).is_file() else 0 for name in ("metadata.sql", "xcom.txt")}
    evidence["dumps"] = {"jobs": results, "bytes": sizes}
    checks.check(
        "the logical dump and the logical XCom query ran as launcher jobs and wrote their files",
        all(item["ok"] for item in results.values()) and all(sizes.values()),
        jobs=results,
        bytes=sizes,
    )
    control = CONTROL_MARKER.encode()
    found = {name: control in (folder / name).read_bytes() for name in sizes if (folder / name).is_file()}
    checks.check(
        "the control marker written to XCom by a task is found in the logical XCom rows and in the dump (the scan can see the data)",
        bool(found) and all(found.values()) and len(found) == 2,
        found=found,
    )


def networks(checks: Checks, listener_port: int, api_port: int) -> None:
    """The broker listener and the Airflow API port refuse every other address of this host; the second-device check is not run here."""
    for name, port in (("the broker listener", listener_port), ("the Airflow API port", api_port)):
        outcomes = unreachable_elsewhere(port)
        checks.check(
            f"{name} refuses a connection on every non-loopback address of this host",
            all(outcomes.values()),
            addresses_tried=len(outcomes),
            reached=len([a for a, ok in outcomes.items() if not ok]),
        )
    checks.check("the IPv6 loopback does not serve the broker", not ipv6_loopback_open(listener_port))


def sentinel_checks(checks: Checks, work: Path, tokens: list[str], evidence: dict[str, Any]) -> None:
    """Exact-match scan for the session token, the run token and the planted credentials in the logical dumps, Airflow's task logs and raw database files,
    the broker's log, ledger, records, saved job logs and forward cache, and the launcher's registry. Any input that is unreadable, skipped or missing fails
    the scan; the coverage is recorded."""
    state = work / "airflow"
    roots = [
        state / "dumps",
        state / "logs",
        state / "postgres",
        work / "broker",
        work / "ledger",
        work / "records",
        work / "job_logs",
        work / "forwards",
        work / "registry" / "registry.json",
    ]
    result = sentinel.scan(roots, [*tokens, *SENTINELS.values()])
    evidence["sentinel"] = result.record()
    required = {"dumps": 2, "logs": 1, "postgres": 1, "broker": 1, "ledger": 1, "records": 2, "job_logs": 2, "forwards": 2, "registry.json": 1}
    short = {name: result.per_root.get(name, 0) for name, minimum in required.items() if result.per_root.get(name, 0) < minimum}
    checks.check(
        "the sentinel scan read every input and found no match in the logical dump, the logical XCom rows, Airflow's logs and database files, the broker's "
        "log, ledger, records, saved job logs and forward cache, or the launcher's registry",
        result.ok and not short,
        short=short,
        coverage=result.record(),
    )


def clean_up(work: Path, nonce: str) -> list[str]:
    """Confirm stack teardown, independently stop the owned launcher and retain unresolved recovery state across retries."""
    unresolved: list[str] = []
    registry = work / "registry"
    saved = work / "cleanup_stack.json"
    with launcher_cleanup.defer_termination() as interrupted:
        try:
            if (registry / "launcher.sock").exists():
                run_lock.write_atomic(saved, json.dumps({"phase": "pending"}) + "\n")
                deadline = time.monotonic() + TEARDOWN_SECONDS
                client = launcher.LauncherClient(launcher.resolve_config(str(registry)), start=False)

                def remaining() -> float:
                    left = deadline - time.monotonic()
                    if left <= 0:
                        raise TimeoutError(f"the stack did not reach down within {TEARDOWN_SECONDS:g} s")
                    return left

                reply = client.call("ping", budget=remaining())
                if reply.get("launch_nonce") and reply["launch_nonce"] != nonce:
                    raise RuntimeError("a launcher with another nonce serves the test registry")
                client.call("orchestration_down", budget=remaining())
                while True:
                    left = remaining()
                    record = client.call(
                        "status",
                        {"what": "orchestration", "id": "", "wait": min(1.0, left), "until": {"field": "phase", "values": ["down"]}},
                        {"kind": "any"},
                        timeout=left,
                        budget=left,
                    )
                    if record.get("phase") == "down":
                        break
                run_lock.write_atomic(saved, json.dumps({"phase": "down"}) + "\n")
            elif saved.exists():
                if json.loads(saved.read_text()).get("phase") != "down":
                    raise RuntimeError("saved stack teardown remains unresolved and the launcher socket is absent")
            elif (registry / "registry.json").exists():
                state = json.loads((registry / "registry.json").read_text())
                if (state.get("launcher", {}).get("orchestration") or {}).get("phase", "down") != "down":
                    raise RuntimeError("the registry records an unresolved stack and the launcher socket is absent")
        except Exception as exc:
            unresolved.append(f"the stack was not brought down: {type(exc).__name__}: {str(exc)[:200]}")
        finally:
            try:
                outcome = launcher_cleanup.end_launcher(registry, nonce)
                if outcome not in ("none", "stopped"):
                    unresolved.append(f"the test-registry launcher this run started was not stopped ({outcome})")
            except Exception as exc:
                unresolved.append(f"launcher cleanup failed: {type(exc).__name__}: {str(exc)[:200]}")
        if interrupted:
            unresolved.append(f"termination signal {interrupted[0]} received during cleanup")
        if not unresolved:
            shutil.rmtree(work, ignore_errors=True)
    return unresolved


def main() -> int:
    """Run the scenario, clean up, write the report and return the exit code."""
    launcher_cleanup.exit_on_termination()
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    work = Path(tempfile.mkdtemp(prefix="a1_real_", dir=TEMP_ROOT)).resolve()  # /tmp is a link on this Mac; the launcher compares resolved paths
    nonce = secrets.token_hex(6)
    checks = Checks()
    evidence: dict[str, Any] = {}
    error = ""
    unresolved: list[str] = []
    try:
        evidence = run(checks, work, nonce)
    except (Exception, SystemExit) as exc:
        error = f"{type(exc).__name__}: {exc}\n{traceback.format_exc()[-1500:]}"
        sys.stdout.write(f"error: {error.splitlines()[0]}\n")
    finally:
        try:
            for name in SENTINELS:
                os.environ.pop(name, None)
        finally:
            unresolved = clean_up(work, nonce)
    passed = bool(checks.results) and all(result["ok"] for result in checks.results.values()) and not error and not unresolved
    report = {
        "feature": "orchestration stack on real Docker through the launcher, test registry",
        "command": ".venv/bin/python -m scripts.orchestration.run_real_e2e",
        "started_utc": stamp,
        "status": "pass" if passed else "fail",
        "head": run_command("git", ["rev-parse", "HEAD"], cwd=ROOT).stdout.strip(),
        "error": error,
        "checks": checks.results,
        "evidence": evidence,
        "temporary_folder_removed": not work.exists(),
        "cleanup_unresolved": unresolved,
        "registry_kept": str(work) if unresolved else "",
        "limits": [
            "No second device was available: the checks try every address of this Mac, not a connection from another machine.",
            "No real build, catalog, bronze, AWS or S3: the DAG run is a probe of the broker, not a transform.",
        ],
    }
    text = json.dumps(report, indent=2, default=str)
    if any(value in text for value in SENTINELS.values()):
        report = {**report, "checks": "withheld: a planted credential is in the report", "evidence": "withheld"}
        text = json.dumps(report, indent=2, default=str)
    folder = OUT / stamp
    folder.mkdir(parents=True)
    (folder / "report.json").write_text(text + "\n")
    sys.stdout.write(f"{report['status']}: {(folder / 'report.json').relative_to(ROOT)}\n")
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
