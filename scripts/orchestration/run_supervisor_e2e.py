"""End-to-end runs of the orchestration supervisor as a client of the launcher, against a fake Docker Engine, in test registries (failure modes 925
and 933; ``plans/wp/a1_failure_modes.md``).

Run ``.venv/bin/python -m scripts.orchestration.run_supervisor_e2e [--case NAME ...]``. It needs ``plans/wp/proposed/launcher_orchestration.patch``
applied to the launcher. Each case uses the harness of ``run_stack_e2e`` (its own test registry, fake daemon and launcher process) and a small
stand-in for Airflow's health endpoint on a free loopback port. A case ends by checking that nothing of it is left. The report is
``data/e2e/orchestration_supervisor/<UTC>/report.json``. No real Docker, catalog, AWS service or secret is used.
"""

from __future__ import annotations

import argparse
import contextlib
import fcntl
import io
import json
import socket
import stat
import sys
import threading
import time
import traceback
from collections.abc import Callable
from datetime import UTC, datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from unittest import mock

from orchestration import launcher
from scripts.infrastructure import secret_store
from scripts.lakehouse import run_lock
from scripts.lakehouse.run_launcher_e2e import CaseFailure, catalog_phase, catalog_script, catalog_up
from scripts.orchestration import supervisor as supervisor_module
from scripts.orchestration.broker import Broker, BrokerError, BrokerPaths, Listener
from scripts.orchestration.run_stack_e2e import POSTGRES_IMAGE, StackEnv, services_script
from scripts.orchestration.supervisor import Supervisor, SupervisorError
from scripts.process import run_command

ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / "data" / "e2e" / "orchestration_supervisor"
PLATFORM = json.loads((ROOT / "config" / "orchestration" / "platform.json").read_text())
HEALTHY = {name: {"status": "healthy"} for name in ("metadatabase", "scheduler", "triggerer", "dag_processor")}


class FakeAirflowApi:
    """A stand-in for Airflow's health endpoint on a free loopback port: it answers the health path with every component healthy."""

    def __init__(self) -> None:
        body = json.dumps(HEALTHY).encode()

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:
                self.send_response(200 if self.path == "/api/v2/monitor/health" else 404)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, format: str, *args: Any) -> None:
                return

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, name="fake-airflow-api", daemon=True)
        self.thread.start()

    @property
    def port(self) -> int:
        """The bound port."""
        return int(self.server.server_address[1])

    def stop(self) -> None:
        """Stop serving."""
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=10)


class SessionEnv(StackEnv):
    """A stack harness case whose state folder is the one the supervisor uses (``<test folder>/airflow``)."""

    def __init__(self, name: str, report: Path, **settings: Any) -> None:
        super().__init__(name, report, **settings)
        self.airflow = self.temp / "airflow"
        self.settings["allowed_roots"] = [str(self.airflow)]
        self.write_settings()
        self.api = FakeAirflowApi()
        self.supervisors: list[Supervisor] = []

    def supervisor(self, **extra: Any) -> Supervisor:
        """A supervisor on this case's registry, with its own ports."""
        paths = BrokerPaths(run_lock.Registry(self.registry, "test"), self.temp)
        supervisor = Supervisor(
            paths,
            PLATFORM,
            self.client(),
            api_port=extra.pop("api_port", self.api.port),
            broker_port=extra.pop("broker_port", 0),
            readiness_seconds=30,
            **extra,
        )
        self.supervisors.append(supervisor)
        return supervisor

    def session_files(self) -> list[str]:
        """The session files the supervisor writes for the scheduler."""
        folder = self.airflow / "session"
        return sorted(path.name for path in folder.iterdir()) if folder.is_dir() else []

    def close(self) -> list[str]:
        """Tear down any session a case left, then the case."""
        for supervisor in self.supervisors:
            with contextlib.suppress(SupervisorError, OSError):
                supervisor.teardown()
        self.api.stop()
        return super().close()


Case = Callable[[SessionEnv], dict[str, bool]]
CASES: dict[str, tuple[Case, dict[str, Any]]] = {}


def case(name: str, **settings: Any) -> Callable[[Case], Case]:
    """Register a case under a name, with the settings of its test registry."""

    def register(function: Case) -> Case:
        CASES[name] = (function, settings)
        return function

    return register


def settles(env: SessionEnv, predicate: Callable[[], Any], seconds: float) -> bool:
    """True when the predicate holds within the time; a case reports a state that does not settle as a failed check, not as an error."""
    try:
        env.wait(predicate, seconds, "the state settles")
    except CaseFailure:
        return False
    return True


def port_open(port: int) -> bool:
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


@case("supervisor_full_session")
def supervisor_full_session(env: SessionEnv) -> dict[str, bool]:
    """933: a full session through the launcher: images checked, the token file owner-only, the metadata secrets in Secrets Manager and in no file,
    the listener serving, the stack up and healthy, the lineage worker finishing a pass and failing none, then a teardown that leaves nothing."""
    services_script(env)
    env.start()
    supervisor = env.supervisor()
    evidence = supervisor.start()
    session = env.airflow / "session"
    token = (session / "broker_token").read_text().strip()
    port = supervisor.listener.port if supervisor.listener else 0
    modes = {path.name: stat.S_IMODE(path.stat().st_mode) for path in session.iterdir()}
    stored = env.stand_in.values
    client_url = json.loads((session / "client.json").read_text())["url"]
    during = {
        "the stack is up": env.stack_phase() == "up",
        "the images were checked through the launcher": set(evidence["images"]) == {"postgres", "airflow"},
        "the health check passed": evidence["health"] == dict.fromkeys(("metadatabase", "scheduler", "triggerer", "dag_processor"), "healthy"),
        "the token is 32 random bytes in hex": len(token) == 64 and all(c in "0123456789abcdef" for c in token),
        "the token and every session file are owner-only": all(mode == 0o600 for mode in modes.values()) and stat.S_IMODE(session.stat().st_mode) == 0o700,
        "no metadata file or folder exists": not (env.airflow / "metadata").exists(),
        "the rotated secrets are in Secrets Manager and the session holds the token secret in memory": bool(supervisor.jwt_secret)
        and stored[secret_store.AIRFLOW_JWT_SECRET] == supervisor.jwt_secret
        and bool(stored[secret_store.AIRFLOW_API_SECRET_KEY]),
        "the client file names the listener's port": client_url == f"http://host.docker.internal:{port}" and port > 0,
        "the listener serves on the loopback": port_open(port),
        "the evidence names the sizing the launcher computed": any(key.startswith("HAI_ORCH_MEM") for key in evidence["settings"]),
        "the catalog was not touched": catalog_phase(env) == "down",
    }
    worker = supervisor.emitter
    worker_passed = worker is not None and settles(env, lambda: bool(worker.outcome), 30)
    result = supervisor.teardown()
    last_pass = result.get("lineage", {}).get("last_pass", {})
    after = {
        "the lineage worker finished a pass that reached its end": worker_passed and "record_cycle" in last_pass,
        "the lineage worker recorded no failed pass over the session": result.get("lineage", {}).get("failures") == 0 and "error" not in last_pass,
        "teardown ends in phase down": result["stack"]["phase"] == "down" and env.stack_phase() == "down",
        "nothing of the stack is left": not env.stack_objects() and not env.daemon.networks,
        "the session token and client file are deleted": env.session_files() == [],
        "the listener is closed": not port_open(port),
        "no Docker restart was requested": not any("restart" in c["path"] for c in env.daemon.calls),
    }
    return {**during, **after}


@case("supervisor_partial_start")
def supervisor_partial_start(env: SessionEnv) -> dict[str, bool]:
    """933: a migration that fails leaves nothing running, no token and no open listener, and the supervisor says why."""
    services_script(env, migrate_code=1)
    env.start()
    supervisor = env.supervisor()
    error: SupervisorError | None = None
    try:
        supervisor.start()
    except SupervisorError as caught:
        error = caught
    settles(env, lambda: not env.stack_objects() and not env.daemon.networks, 30)
    return {
        "the start failed with the migration's cause": error is not None and "airflow-migrate" in str(error),
        "nothing of the stack is left": not env.stack_objects() and not env.daemon.networks,
        "the stack is down": env.stack_phase() == "down",
        "the session token is deleted": env.session_files() == [],
        "the listener is closed": supervisor.listener is None,
    }


@case("supervisor_listener_start_failure")
def supervisor_listener_start_failure(env: SessionEnv) -> dict[str, bool]:
    """933b: the listener's socket is bound and its thread fails to start. The supervisor must return that error, release the broker lock, close the
    socket and leave no session file and no stack; a later session then starts and stops as usual."""
    services_script(env)
    env.start()
    supervisor = env.supervisor()
    errors: list[str] = []
    ports: list[int] = []

    def start_session() -> None:
        try:
            supervisor.start()
        except Exception as exc:
            errors.append(f"{type(exc).__name__}: {exc}")

    def failing_start(self: Listener) -> None:
        ports.append(self.port)
        raise RuntimeError("synthetic listener thread start failure")

    with mock.patch.object(Listener, "start", failing_start):
        worker = threading.Thread(target=start_session, name="start-session", daemon=True)
        worker.start()
        worker.join(timeout=20)
        returned = not worker.is_alive()
        if not returned and supervisor.listener is not None:
            # Rescue for the case's own sake: let the blocked shutdown finish. The check above has already recorded the defect.
            supervisor.listener.thread.start()
            worker.join(timeout=30)
    lock = (env.temp / "broker" / "broker.lock").open("a")
    try:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        lock_free = True
    except BlockingIOError:
        lock_free = False
    finally:
        lock.close()
    nothing_created = not env.created_names() and not env.daemon.networks
    later = env.supervisor()
    later_error = ""
    try:
        later.start()
    except Exception as exc:
        later_error = f"{type(exc).__name__}: {exc}"
    later_up = env.stack_phase() == "up"
    later.teardown()
    return {
        "the start returned within 20 seconds": returned,
        "it returned the listener's own error": bool(errors) and "synthetic listener thread start failure" in errors[0],
        "the broker lock is free": lock_free,
        "the listener's socket is closed": bool(ports) and not port_open(ports[0]),
        "no session file is left": env.session_files() == [],
        "no container and no network of the stack were created": nothing_created,
        f"a later session starts ({later_error or 'ok'}) and stops": later_up and not later_error and not env.stack_objects(),
    }


@case("supervisor_teardown_independent")
def supervisor_teardown_independent(env: SessionEnv) -> dict[str, bool]:
    """933b: the listener and the broker each fail to stop; teardown still deletes the session files, takes the stack down through the launcher and
    reports both failures instead of raising; a second teardown finishes the work."""
    services_script(env)
    env.start()
    supervisor = env.supervisor()
    supervisor.start()
    raised = ""
    result: dict[str, Any] = {}
    with (
        mock.patch.object(Listener, "stop", side_effect=RuntimeError("synthetic listener stop failure")),
        mock.patch.object(Broker, "close", side_effect=RuntimeError("synthetic broker close failure")),
    ):
        try:
            result = supervisor.teardown()
        except Exception as exc:
            raised = f"{type(exc).__name__}: {exc}"
    if not raised:
        env.wait(lambda: env.stack_phase() == "down" and not env.stack_objects(), 40, "the stack is down")
    stack_down = env.stack_phase() == "down" and not env.stack_objects()
    session_gone = env.session_files() == []
    errors = " ".join(str(item) for item in result.get("errors", []))
    again = supervisor.teardown()
    return {
        f"teardown returned instead of raising ({raised or 'ok'})": not raised,
        "the session token and client file are deleted": session_gone,
        "the stack went down through the launcher": stack_down,
        "both failures are named in the result": "listener" in errors and "broker" in errors,
        "a second teardown, with nothing failing, ends clean": again["stack"]["phase"] == "down" and not again.get("errors") and env.session_files() == [],
    }


def run_down(env: SessionEnv, supervisor: Supervisor) -> tuple[int | None, dict[str, Any], str]:
    """The command line's ``down`` on a supervisor: its exit code (None when it raised), the JSON it printed and what it wrote to stderr."""
    output, errors = io.StringIO(), io.StringIO()
    argv = ["supervisor", "--test-registry", str(env.registry), "down"]
    code: int | None = None
    with (
        mock.patch.object(supervisor_module, "build", return_value=supervisor),
        mock.patch.object(sys, "argv", argv),
        contextlib.redirect_stdout(output),
        contextlib.redirect_stderr(errors),
        contextlib.suppress(Exception),  # an exception that escapes the command is judged by the exit code None
    ):
        code = supervisor_module.main()
    printed = output.getvalue()
    return code, json.loads(printed) if printed.strip().startswith("{") else {}, errors.getvalue()


def stack_is_down(env: SessionEnv) -> bool:
    """True once the launcher's record says down and nothing of the stack exists."""
    return settles(env, lambda: env.stack_phase() == "down" and not env.stack_objects() and not env.daemon.networks, 40)


@case("supervisor_teardown_unresolved")
def supervisor_teardown_unresolved(env: SessionEnv) -> dict[str, bool]:
    """933d: the command line's ``down`` returns nonzero until the launcher confirms the stack is down: when the launcher cannot be reached for the
    teardown request, when the wait returns a phase other than down and when the wait raises a launcher error. The other steps still run; a second ``down``
    with the launcher back ends clean; a confirmed teardown returns 0 with nothing on stderr."""
    services_script(env)
    env.start()
    first = env.supervisor()
    first.start()
    up_before = env.stack_phase() == "up"
    original = first.client.call

    def fail_down(method: str, *args: Any, **kwargs: Any) -> Any:
        if method == "orchestration_down":
            raise OSError("synthetic teardown transport outage")
        return original(method, *args, **kwargs)

    with mock.patch.object(first.client, "call", fail_down):
        outage_code, outage, outage_errors = run_down(env, first)
    still_up = env.stack_phase() == "up" and bool(env.stack_objects())
    released = first.listener is None and first.broker is None and env.session_files() == []
    again_code, again, again_errors = run_down(env, first)
    again_down = stack_is_down(env)
    second = env.supervisor()
    second.start()
    with mock.patch.object(second.client, "wait_for", return_value={"phase": "tearing_down", "error": ""}):
        phase_code, phase, phase_errors = run_down(env, second)
    stack_is_down(env)
    third = env.supervisor()
    third.start()
    raised = ""
    failed: dict[str, Any] = {}
    with mock.patch.object(third.client, "wait_for", side_effect=launcher.ClientError("synthetic wait failure", "degraded")):
        try:
            failed = third.teardown()
        except Exception as exc:
            raised = f"{type(exc).__name__}: {exc}"
    stack_is_down(env)
    fourth = env.supervisor()
    fourth.start()
    clean_code, clean, clean_errors = run_down(env, fourth)
    # A start that fails after the stack is up, with the launcher unable to take the stack down: the start error is the one raised, and the failed
    # cleanup is reported on stderr instead of being dropped.
    fifth = env.supervisor(fail_at="after_airflow_start")
    original_fifth = fifth.client.call

    def fail_down_again(method: str, *args: Any, **kwargs: Any) -> Any:
        if method == "orchestration_down":
            raise OSError("synthetic teardown transport outage")
        return original_fifth(method, *args, **kwargs)

    start_errors = io.StringIO()
    start_raised = ""
    with mock.patch.object(fifth.client, "call", fail_down_again), contextlib.redirect_stderr(start_errors):
        try:
            fifth.start()
        except SupervisorError as exc:
            start_raised = str(exc)
    fifth.teardown()
    stack_is_down(env)
    return {
        "the stack is up before the outage": up_before,
        "a teardown request the launcher cannot answer makes the command return nonzero": outage_code not in (0, None),
        "the command names the stack on stderr": "stack" in outage_errors,
        "the result lists the stack step under errors": any("stack" in item for item in outage.get("errors", [])),
        "the result still reports the stack phase as unknown": outage.get("stack", {}).get("phase") == "unknown",
        "the listener, the broker and the session files are released although the stack is unconfirmed": released,
        "the stack is still up, as the result says": still_up,
        "a second down with the launcher back returns 0 and the stack is down": again_code == 0 and not again_errors and again_down and "errors" not in again,
        "a wait that returns another phase makes the command return nonzero": phase_code not in (0, None),
        "the result names the phase that was not down": any("tearing_down" in item for item in phase.get("errors", [])) and "stack" in phase_errors,
        f"a launcher error raised by the wait does not escape teardown ({raised or 'ok'})": not raised,
        "that error is listed under errors": any("stack" in item for item in failed.get("errors", [])),
        "a failed start still raises its own error": "test failure point" in start_raised,
        "a failed start reports the cleanup that did not finish": "stack" in start_errors.getvalue() and "cleanup" in start_errors.getvalue(),
        "a confirmed teardown returns 0 with nothing on stderr": clean_code == 0 and not clean_errors and clean.get("stack", {}).get("phase") == "down",
    }


@case("supervisor_failure_after_start")
def supervisor_failure_after_start(env: SessionEnv) -> dict[str, bool]:
    """933: a failure after the stack is up (a test failure point) takes the stack down through the launcher and deletes the token."""
    services_script(env)
    env.start()
    supervisor = env.supervisor(fail_at="after_airflow_start")
    error: SupervisorError | None = None
    try:
        supervisor.start()
    except SupervisorError as caught:
        error = caught
    settles(env, lambda: env.stack_phase() == "down" and not env.stack_objects(), 40)
    return {
        "the start failed at the test failure point": error is not None and "after_airflow_start" in str(error),
        "the stack came up before the failure": any("orch-airflow-triggerer" in name for name in env.created_names()),
        "everything was taken down": env.stack_phase() == "down" and not env.stack_objects() and not env.daemon.networks,
        "the session token is deleted": env.session_files() == [],
    }


@case("supervisor_image_missing")
def supervisor_image_missing(env: SessionEnv) -> dict[str, bool]:
    """934: without the built Airflow image the supervisor stops before it writes a token or starts anything, and says how to build it."""
    env.daemon.images.pop(PLATFORM["airflow"]["image"])
    env.start()
    supervisor = env.supervisor()
    error: SupervisorError | None = None
    try:
        supervisor.start()
    except SupervisorError as caught:
        error = caught
    return {
        "the start was refused": error is not None and "not on this machine" in str(error) and "Dockerfile" in str(error),
        "no token or secret file was written": not (env.airflow / "session").exists(),
        "nothing was created": not env.created_names() and not env.daemon.networks,
    }


@case("supervisor_pulls_missing_postgres")
def supervisor_pulls_missing_postgres(env: SessionEnv) -> dict[str, bool]:
    """934: a PostgreSQL image that is absent is pulled by its digest through the launcher; the Airflow image is never pulled."""
    services_script(env)
    env.daemon.images.pop(POSTGRES_IMAGE, None)
    env.engine.pull_aliases = {"postgres@" + POSTGRES_IMAGE.partition("@")[2]: [POSTGRES_IMAGE]}
    env.start()
    supervisor = env.supervisor()
    error = ""
    try:
        supervisor.start()
    except (SupervisorError, BrokerError) as caught:
        error = str(caught)
    pulls = env.daemon.calls_matching("POST", "/images/create")
    supervisor.teardown()
    return {
        f"the session started ({error or 'ok'})": not error,
        "one pull was made": len(pulls) == 1,
        "it named PostgreSQL and its digest": len(pulls) == 1
        and pulls[0]["query"].get("fromImage") == "postgres"
        and pulls[0]["query"].get("tag", "").startswith("sha256:d74eeac9"),
        "the Airflow image was not pulled": not any("airflow" in c["query"].get("fromImage", "") for c in pulls),
    }


@case("supervisor_second_start_refused")
def supervisor_second_start_refused(env: SessionEnv) -> dict[str, bool]:
    """933: a second session while one runs is refused and changes nothing; teardown twice and teardown with no session are harmless."""
    services_script(env)
    env.start()
    first = env.supervisor()
    first.start()
    second = env.supervisor()
    error: SupervisorError | BrokerError | None = None
    try:
        second.start()
    except (SupervisorError, BrokerError) as caught:
        error = caught
    still_up = env.stack_phase() == "up"
    first.teardown()
    again = first.teardown()
    return {
        "the second start was refused": error is not None and "run down first" in str(error),
        "the first session still ran": still_up,
        "a second teardown reports down": again["stack"]["phase"] == "down",
        "nothing is left": not env.stack_objects() and not env.daemon.networks,
    }


@case("supervisor_leaves_the_catalog")
def supervisor_leaves_the_catalog(env: SessionEnv) -> dict[str, bool]:
    """933: a session starts and stops beside a running catalog without touching its containers or network, and asks for no Docker restart."""
    catalog_script(env)
    services_script(env)
    previous = env.daemon.script

    def script(container: dict[str, Any]) -> tuple[float, int] | None:
        return (0.3, 0) if container["Config"]["Labels"].get("hai.service") == "polaris-bootstrap" else previous(container)

    env.daemon.script = script
    env.start()
    up = catalog_up(env) == "up"
    before = sorted(c["Name"] for c in env.labelled() if c["Config"]["Labels"].get("hai.service"))
    mark = len(env.daemon.calls)
    supervisor = env.supervisor()
    supervisor.start()
    supervisor.teardown()
    after = sorted(c["Name"] for c in env.labelled() if c["Config"]["Labels"].get("hai.service"))
    touched = [c for c in env.daemon.calls[mark:] if c["method"] in ("POST", "DELETE") and "-svc-polaris" in str(c.get("target"))]
    networks_before_down = len(env.daemon.networks)
    env.client().call("catalog_down")
    env.wait(lambda: catalog_phase(env) == "down" and not env.daemon.networks, 40, "catalog down")
    return {
        "the catalog was up": up,
        "the catalog's containers are the same before and after": before == after and len(before) == 2,
        "no start, stop or remove was sent to a catalog container": not touched,
        "the catalog's network was still there": networks_before_down == 1,
        "no Docker restart was requested": not any("restart" in c["path"] for c in env.daemon.calls),
        "the stack is gone": not env.stack_objects(),
    }


# Every case, listed so that a case that loses its decorator cannot go unnoticed: the runner checks the list against the registry.
ALL_CASES = (
    supervisor_full_session,
    supervisor_partial_start,
    supervisor_listener_start_failure,
    supervisor_teardown_independent,
    supervisor_teardown_unresolved,
    supervisor_failure_after_start,
    supervisor_image_missing,
    supervisor_pulls_missing_postgres,
    supervisor_second_start_refused,
    supervisor_leaves_the_catalog,
)


def run_case(name: str, report: Path) -> dict[str, Any]:
    """Run one case in its own harness."""
    function, settings = CASES[name]
    env = SessionEnv(name, report, **dict(settings))
    started = time.monotonic()
    error = ""
    checks: dict[str, bool] = {}
    try:
        checks = function(env)
    except Exception as exc:
        error = f"{type(exc).__name__}: {exc}\n{traceback.format_exc()[-1500:]}"
    leftovers = env.close()
    checks["nothing of the case is left over"] = not leftovers
    passed = bool(checks) and all(checks.values()) and not error
    return {"case": name, "passed": passed, "checks": checks, "error": error, "leftovers": leftovers, "seconds": round(time.monotonic() - started, 1)}


def run_cases(selected: list[str], verbose: bool = False) -> int:
    """Run the selected cases (all by default), write the report and return the exit code."""
    if {function.__name__ for function in ALL_CASES} != {function.__name__ for function, _ in CASES.values()}:
        raise SystemExit("ALL_CASES differs from the registered cases")
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    report = OUT / stamp
    report.mkdir(parents=True)
    results: list[dict[str, Any]] = []
    for name in selected or list(CASES):
        result = run_case(name, report)
        status = "pass" if result["passed"] else "fail"
        results.append({**result, "status": status})
        sys.stdout.write(f"{'ok' if status == 'pass' else 'FAIL':<5} {name} ({result['seconds']} s)\n")
        for check, ok in result["checks"].items():
            if verbose:
                sys.stdout.write(f"      {'ok' if ok else 'FAILED'}: {check}\n")
            elif not ok:
                sys.stdout.write(f"      failed: {check}\n")
        if result["error"]:
            sys.stdout.write(f"      error: {result['error'].splitlines()[0]}\n")
    summary = {
        "feature": "orchestration supervisor as a launcher client, fake Docker, test registries",
        "command": ".venv/bin/python -m scripts.orchestration.run_supervisor_e2e",
        "started_utc": stamp,
        "head": run_command("git", ["rev-parse", "HEAD"], cwd=ROOT).stdout.strip(),
        "python": sys.version.split()[0],
        "cases": results,
        "passed": sum(1 for r in results if r["status"] == "pass"),
        "total": len(results),
        "limits": ["A fake daemon and a stand-in health endpoint: no real container or Airflow process runs here (see run_real_e2e)."],
    }
    (report / "report.json").write_text(json.dumps(summary, indent=2) + "\n")
    sys.stdout.write(f"{summary['passed']}/{summary['total']}: {report.relative_to(ROOT)}/report.json\n")
    return 0 if summary["passed"] == summary["total"] else 1


def main() -> int:
    """Parse arguments and run."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--case", action="append", default=[], choices=sorted(CASES))
    parser.add_argument("--checks", action="store_true", help="print every check of every case, not only the failed ones")
    args = parser.parse_args()
    return run_cases(args.case, args.checks)


if __name__ == "__main__":
    sys.exit(main())
