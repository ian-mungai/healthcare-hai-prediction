"""End-to-end runs of the orchestration stack lifecycle and the image pull in the launcher, against a fake Docker Engine, in test registries
(failure modes 925, 928, 933 and 934; ``plans/wp/a1_failure_modes.md``).

Run ``.venv/bin/python -m scripts.orchestration.run_stack_e2e [--case NAME ...]``. It needs ``plans/wp/proposed/launcher_orchestration.patch``
applied to ``orchestration/launcher.py`` (``git apply plans/wp/proposed/launcher_orchestration.patch``); without the patch every case fails with
the launcher's refusal of an unknown method. Each case uses the harness of ``scripts/lakehouse/run_launcher_e2e.py`` (its own test registry,
fake daemon and launcher process) and ends by checking that nothing of the case is left. No real Docker, catalog, AWS service or secret is
used. The report is ``data/e2e/orchestration_stack/<UTC>/report.json``.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import time
import traceback
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from unittest import mock

from orchestration import launcher
from scripts.lakehouse import run_launcher_e2e
from scripts.lakehouse.run_launcher_e2e import CaseFailure, Env, catalog_phase, catalog_script, catalog_up, refused
from scripts.orchestration.fake_engine import StackDocker
from scripts.process import run_command

ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / "data" / "e2e" / "orchestration_stack"
AIRFLOW_IMAGE = "hai-airflow:3.3.2-python3.12"
POSTGRES_IMAGE = "postgres:17@sha256:d74eeac9a635390a49bc21bd49fccd973de707e2a53a76ac49b552b8712ec46f"
UNPINNED_DIGEST = "sha256:" + "ab" * 32
STACK_SERVICES = ("postgres", "airflow-migrate", "airflow-apiserver", "airflow-scheduler", "airflow-dag-processor", "airflow-triggerer")
API_PORT = 18999
GIB = 1024**3


class StackEnv(Env):
    """A harness case that also holds an orchestration state folder and the images of the stack."""

    def __init__(self, name: str, report: Path, **settings: Any) -> None:
        with mock.patch.object(run_launcher_e2e, "FakeDocker", StackDocker):
            super().__init__(name, report, **settings)
        daemon = self.daemon
        if not isinstance(daemon, StackDocker):
            raise TypeError("the harness did not build the stack daemon")
        self.engine: StackDocker = daemon
        self.orch = self.temp / "orch"
        for folder in ("metadata", "session"):
            (self.orch / folder).mkdir(parents=True, mode=0o700)
        self.settings["allowed_roots"] = [str(self.orch)]
        self.write_settings()
        labels = {launcher.DEFAULT_IMAGE_LABEL: "1", launcher.DEFAULT_IMAGE_IMPLEMENTATION: "go"}
        self.daemon.images[AIRFLOW_IMAGE] = {
            "Id": "sha256:" + os.urandom(8).hex(),
            "Config": {
                "Labels": labels,
                "Env": ["PATH=/usr/local/bin:/usr/bin", "AIRFLOW_HOME=/opt/airflow"],
                "User": "airflow",
                "WorkingDir": "/opt/airflow",
                "Entrypoint": ["/usr/bin/dumb-init", "--", "/entrypoint"],
                "Cmd": [],
            },
        }

    def up_params(self, **extra: Any) -> dict[str, Any]:
        """The parameters of an ``orchestration_up`` request."""
        return {"state": str(self.orch), "api_port": API_PORT, "checkout": str(ROOT), **extra}

    def stack(self) -> dict[str, Any]:
        """The recorded stack."""
        return dict(self.state()["launcher"].get("orchestration") or {"phase": "down"})

    def stack_phase(self) -> str:
        """The recorded phase."""
        return str(self.stack().get("phase", "down"))

    def stack_up(self, seconds: float = 40) -> str:
        """Request the stack and wait for ``up`` or a failed start (``down``); returns the phase."""
        self.client().call("orchestration_up", self.up_params())
        time.sleep(0.2)
        return str(self.wait(lambda: self.stack_phase() in ("up", "down") and self.stack_phase(), seconds, "the stack is up or failed"))

    def stack_down(self, seconds: float = 40) -> None:
        """Request teardown and wait until it is down and nothing of the stack is left."""
        self.client().call("orchestration_down")
        self.wait(lambda: self.stack_phase() == "down" and not self.stack_objects(), seconds, "the stack is down")

    def stack_objects(self) -> list[str]:
        """Containers and networks of the stack."""
        with self.daemon.lock:
            found = [c["Name"] for c in self.daemon.containers.values() if c["Config"]["Labels"].get(launcher.LABEL_STACK) == "orchestration"]
            found += [n["Name"] for n in self.daemon.networks.values() if n["Labels"].get(launcher.LABEL_STACK) == "orchestration"]
        return found

    def created_names(self) -> list[str]:
        """Container names in the order the daemon received their creates."""
        return [str(c["name"]) for c in self.daemon.calls_matching("POST", "/containers/create") if "-svc-orch-" in str(c["name"])]

    def body_of(self, service: str) -> dict[str, Any]:
        """The create body of one stack service."""
        return dict(self.daemon.create_bodies[f"hai-{self.registry_id()[:8]}-svc-orch-{service}"])


Case = Callable[[StackEnv], dict[str, bool]]
CASES: dict[str, tuple[Case, dict[str, Any]]] = {}


def case(name: str, **settings: Any) -> Callable[[Case], Case]:
    """Register a case under a name, with the settings of its test registry."""

    def register(function: Case) -> Case:
        CASES[name] = (function, settings)
        return function

    return register


def services_script(env: StackEnv, migrate_code: int = 0) -> None:
    """Long-running services run until stopped; the migration ends with the given code after a short time."""

    def script(container: dict[str, Any]) -> tuple[float, int] | None:
        service = container["Config"]["Labels"].get("hai.orch_service")
        if service == "airflow-migrate":
            return 0.3, migrate_code
        if container["Config"]["Labels"].get("hai.service") == "polaris-bootstrap":
            return 0.3, 0
        return None

    env.daemon.script = script


def settles(env: StackEnv, predicate: Callable[[], Any], seconds: float) -> bool:
    """True when the predicate holds within the time; a case reports a state that does not settle as a failed check, not as an error."""
    try:
        env.wait(predicate, seconds, "the state settles")
    except CaseFailure:
        return False
    return True


def asked(env: StackEnv, method: str, params: dict[str, Any] | None = None) -> launcher.ClientError | None:
    """Send a launcher request and return the refusal, or None when it was accepted; a case judges a refusal by its text."""
    try:
        env.client().call(method, params)
    except launcher.ClientError as error:
        return error
    return None


def catalog_comes_up(env: StackEnv) -> bool:
    """True when ``catalog_up`` ends with the catalog up; a refusal or a catalog that does not come up is False, not an error."""
    try:
        return catalog_up(env) == "up"
    except (launcher.ClientError, CaseFailure):
        return False


def start_stack(env: StackEnv, seconds: float = 40) -> str:
    """The phase an ``orchestration_up`` settles in (``up`` or ``down``), or ``timeout``."""
    try:
        return env.stack_up(seconds)
    except CaseFailure:
        return "timeout"


def mount_pairs(body: dict[str, Any]) -> list[tuple[str, str, bool]]:
    """The mounts of a create body as (source, target, read_only)."""
    return [(m["Source"], m["Target"], bool(m.get("ReadOnly"))) for m in body["HostConfig"]["Mounts"]]


@case("stack_up_down")
def stack_up_down(env: StackEnv) -> dict[str, bool]:
    """933: orchestration_up creates the network, PostgreSQL, the migration and the four Airflow services in order, each with its own labels;
    orchestration_down removes them all in reverse order and the network, and the catalog is untouched."""
    services_script(env)
    env.start()
    phase = env.stack_up()
    state = env.state()
    rid = env.registry_id()[:8]
    stack = env.stack()
    bodies = {service: env.body_of(service) for service in STACK_SERVICES}
    apiserver, scheduler, postgres, migrate = bodies["airflow-apiserver"], bodies["airflow-scheduler"], bodies["postgres"], bodies["airflow-migrate"]
    api_env = dict(item.split("=", 1) for item in apiserver["Env"])
    networks = [n for n in env.daemon.networks.values() if n["Labels"].get(launcher.LABEL_STACK) == "orchestration"]
    all_mounts = [mount for body in bodies.values() for mount in mount_pairs(body)]
    labelled = [c for c in env.labelled() if c["Config"]["Labels"].get(launcher.LABEL_STACK) == "orchestration"]
    checks = {
        "the stack is up": phase == "up" and stack.get("error", "") == "",
        "the creates follow the start order": env.created_names() == [f"hai-{rid}-svc-orch-{service}" for service in STACK_SERVICES],
        "the five long-running services run (the migration has ended and is removed)": sorted(c["Config"]["Labels"]["hai.orch_service"] for c in labelled)
        == sorted(set(STACK_SERVICES) - {"airflow-migrate"}),
        "one labelled network of the stack exists": len(networks) == 1 and networks[0]["Name"] == f"hai-{rid}-orch-net",
        "every container is on that network": all(b["HostConfig"]["NetworkMode"] == f"hai-{rid}-orch-net" for b in bodies.values()),
        "no catalog attempt, catalog down": catalog_phase(env) == "down" and not any(a["owner"] == "service:catalog" for a in state["attempts"].values()),
        "the stack's containers carry no catalog service label": not any(launcher.LABEL_SERVICE in b["Labels"] for b in bodies.values()),
        "the migration runs behind the deadline entrypoint with a deadline": migrate["Entrypoint"][0] == launcher.DEADLINE_ENTRYPOINT
        and any(item.startswith("HAI_DEADLINE_UTC=") for item in migrate["Env"])
        and migrate["Cmd"] == ["db", "migrate"],
        "no long-running service has the deadline entrypoint": all("Entrypoint" not in bodies[s] for s in STACK_SERVICES if s != "airflow-migrate"),
        "the API server publishes only on the loopback": apiserver["HostConfig"]["PortBindings"]
        == {"8080/tcp": [{"HostIp": "127.0.0.1", "HostPort": str(API_PORT)}]},
        "no other service publishes a port": not any(b["HostConfig"].get("PortBindings") for s, b in bodies.items() if s != "airflow-apiserver"),
        "Airflow runs as the caller's UID with group 0, no capability, no new privileges": all(
            bodies[s]["User"] == f"{os.getuid()}:0"
            and bodies[s]["HostConfig"].get("CapDrop") == ["ALL"]
            and "no-new-privileges:true" in bodies[s]["HostConfig"].get("SecurityOpt", [])
            for s in STACK_SERVICES
            if s != "postgres"
        ),
        "PostgreSQL has no capability change but no new privileges": "no-new-privileges:true" in postgres["HostConfig"]["SecurityOpt"],
        "no mount is the Docker socket, an AWS folder or a lakehouse secret": not any(
            source.endswith(".sock") or ".aws" in source or "lakehouse/secrets" in source for source, _, _ in all_mounts
        ),
        "only the logs and the database folder are writable": sorted({target for _, target, ro in all_mounts if not ro})
        == ["/opt/airflow/logs", "/var/lib/postgresql/data"],
        "only the scheduler receives the broker session folder": [s for s, b in bodies.items() if any(t == "/opt/hai/broker" for _, t, _ in mount_pairs(b))]
        == ["airflow-scheduler"],
        "the launcher mounts only the three DAG-side modules, never launcher.py": sorted(
            t for _, t, _ in mount_pairs(scheduler) if t.startswith("/opt/hai/orchestration/")
        )
        == ["/opt/hai/orchestration/__init__.py", "/opt/hai/orchestration/client.py", "/opt/hai/orchestration/interface.py"],
        "every service has a memory limit from the plan and a registered growth reserve": all(b["HostConfig"]["Memory"] > 0 for b in bodies.values())
        and set(state["services"]) == {f"orch-{s}" for s in STACK_SERVICES if s != "airflow-migrate"},
        "the scheduler's limit leaves room for the LocalExecutor workers": scheduler["HostConfig"]["Memory"] >= 2 * GIB
        and int(api_env["AIRFLOW__CORE__PARALLELISM"]) >= 1,
        "the metadata secrets are read from files, never set in the environment": not any(
            name in api_env for name in ("AIRFLOW__CORE__FERNET_KEY", "AIRFLOW__API_AUTH__JWT_SECRET", "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN")
        ),
        "every create has a recorded effective-specification digest": all(
            op.get("spec_digest") for op in state["operations"].values() if op["kind"] == "create" and "-svc-orch-" in op["name"]
        ),
    }
    stops = len(env.daemon.calls_matching("POST", "/containers"))
    env.stack_down()
    removed = [c["target"] for c in env.daemon.calls_matching("DELETE", "/containers/") if c.get("target")]
    checks.update(
        {
            "PostgreSQL was removed after every Airflow service": [n for n in removed if n and "-svc-orch-" in n][-1:] == [f"hai-{rid}-svc-orch-postgres"],
            "orchestration_down removes every container and the network": not env.stack_objects() and not env.labelled() and not env.daemon.networks,
            "the registrations of the services are released": not env.state()["services"],
            "the stack record is down with no error": env.stack_phase() == "down" and env.stack().get("error", "") == "",
            "the daemon received start, stop and remove requests only through the launcher": stops > 0,
        }
    )
    return checks


@case("stack_partial_start")
def stack_partial_start(env: StackEnv) -> dict[str, bool]:
    """933: a migration that exits 1 tears everything down and leaves the stack down with the cause recorded."""
    services_script(env, migrate_code=1)
    env.start()
    phase = start_stack(env)
    settles(env, lambda: not env.stack_objects() and not env.daemon.networks, 30)
    stack = env.stack()
    names = env.created_names()
    return {
        "the stack is down": phase == "down" and stack["phase"] == "down",
        "the error names the migration and its exit code": "airflow-migrate ended" in stack.get("error", "") and "exit 1" in stack.get("error", ""),
        "no Airflow service was started after the failed migration": not any("apiserver" in n or "scheduler" in n for n in names),
        "PostgreSQL, started before it, was stopped and removed": any(n.endswith("postgres") for n in names) and not env.stack_objects(),
        "no network is left": not env.daemon.networks,
        "the registrations are released": not env.state()["services"],
    }


@case("stack_unhealthy_service")
def stack_unhealthy_service(env: StackEnv) -> dict[str, bool]:
    """933: a service that never turns healthy tears the stack down after the health wait."""
    services_script(env)
    env.engine.unhealthy_service = "airflow-scheduler"
    env.start()
    phase = start_stack(env, 60)
    settles(env, lambda: not env.stack_objects() and not env.daemon.networks, 30)
    return {
        "the stack is down": phase == "down",
        "the error names the scheduler and the health wait": "airflow-scheduler was not healthy" in env.stack().get("error", ""),
        "the services before it were stopped": not env.stack_objects() and not env.daemon.networks,
    }


@case("stack_teardown_barrier")
def stack_teardown_barrier(env: StackEnv) -> dict[str, bool]:
    """933: orchestration_up during a teardown is refused; orchestration_down during a start tears down what exists."""
    services_script(env)
    env.start()
    gate = env.daemon.add_gate("remove", "before", match=lambda c: str(c["name"]).endswith("orch-airflow-triggerer"))
    start_stack(env)
    asked(env, "orchestration_down")
    gate.wait_reached()
    up_during_teardown = asked(env, "orchestration_up", env.up_params())
    gate.release()
    settles(env, lambda: env.stack_phase() == "down" and not env.stack_objects(), 30)
    # a start interrupted by a teardown request
    slow = env.daemon.add_gate("start", "before", match=lambda c: str(c["name"]).endswith("orch-postgres"))
    asked(env, "orchestration_up", env.up_params())
    slow.wait_reached()
    asked(env, "orchestration_down")
    slow.release()
    settles(env, lambda: env.stack_phase() == "down" and not env.stack_objects() and not env.daemon.networks, 40)
    return {
        "orchestration_up is refused while the stack is being torn down": refused(up_during_teardown, "refused", "torn down"),
        "orchestration_down during a start leaves nothing": not env.stack_objects() and not env.daemon.networks,
        "the second teardown ended in phase down": env.stack_phase() == "down",
    }


@case("stack_resume_after_launcher_kill")
def stack_resume_after_launcher_kill(env: StackEnv) -> dict[str, bool]:
    """933: a launcher killed in the middle of a start, and again in the middle of a teardown, resumes the recorded phase."""
    services_script(env)
    env.start()
    gate = env.daemon.add_gate("create", "after", match=lambda c: str(c["name"]).endswith("orch-airflow-apiserver"))
    env.client().call("orchestration_up", env.up_params())
    gate.wait_reached()
    env.kill_launcher()
    env.start()
    gate.release()
    settles(env, lambda: env.stack_phase() == "up", 60)
    started_ok = env.stack_phase() == "up"
    unique = len({n for n in env.created_names() if n.endswith("orch-airflow-apiserver")}) == 1
    gate2 = env.daemon.add_gate("remove", "before", match=lambda c: str(c["name"]).endswith("orch-airflow-scheduler"))
    env.client().call("orchestration_down")
    gate2.wait_reached()
    env.kill_launcher()
    env.start()
    gate2.release()
    settles(env, lambda: env.stack_phase() == "down" and not env.stack_objects() and not env.daemon.networks, 60)
    return {
        "a start interrupted by a launcher kill resumes and comes up": started_ok,
        "the API server was created once": unique,
        "a teardown interrupted by a launcher kill resumes and ends": env.stack_phase() == "down",
        "nothing of the stack is left": not env.stack_objects() and not env.daemon.networks and not env.labelled(),
    }


@case("stack_independent_of_catalog")
def stack_independent_of_catalog(env: StackEnv) -> dict[str, bool]:
    """933: the catalog and the stack start and stop independently; each absence check ignores the other's containers and network."""
    catalog_script(env)
    services_script(env)
    previous = env.daemon.script

    def script(container: dict[str, Any]) -> tuple[float, int] | None:
        if container["Config"]["Labels"].get("hai.service") == "polaris-bootstrap":
            return 0.3, 0
        return previous(container)

    env.daemon.script = script
    env.start()
    checks: dict[str, bool] = {}
    checks["the catalog comes up"] = catalog_comes_up(env)
    checks["the stack comes up beside it"] = start_stack(env) == "up"
    networks = {n["Name"]: n["Labels"] for n in env.daemon.networks.values()}
    checks["two networks exist, one per project"] = len(networks) == 2
    asked(env, "catalog_down")
    settles(env, lambda: catalog_phase(env) == "down", 40)
    stack_left = [c for c in env.labelled() if c["Config"]["Labels"].get(launcher.LABEL_STACK) == "orchestration"]
    checks["catalog_down finishes while the stack runs, and leaves the stack's containers and network"] = (
        len(stack_left) == 5 and len(env.daemon.networks) == 1 and env.stack_phase() == "up"
    )
    checks["the catalog teardown barrier is cleared"] = env.state()["launcher"]["barrier"] is None
    checks["the catalog comes up again"] = catalog_comes_up(env)
    asked(env, "orchestration_down")
    settles(env, lambda: env.stack_phase() == "down" and not env.stack_objects(), 40)
    checks["orchestration_down leaves the catalog's containers and network"] = (
        catalog_phase(env) == "up" and len(env.daemon.networks) == 1 and any(c["Config"]["Labels"].get("hai.service") == "polaris" for c in env.labelled())
    )
    asked(env, "catalog_down")
    settles(env, lambda: catalog_phase(env) == "down" and not env.daemon.networks, 40)
    checks["everything is down"] = not env.labelled() and not env.daemon.networks
    return checks


@case("stack_request_validation")
def stack_request_validation(env: StackEnv) -> dict[str, bool]:
    """933: the request names only a state folder, a port and a checkout, and each is validated by the launcher."""
    services_script(env)
    env.start()
    client = env.client()

    def error_of(**override: Any) -> launcher.ClientError | None:
        params = env.up_params(**override)
        for name in [key for key, value in override.items() if value is None]:
            params.pop(name)
        try:
            client.call("orchestration_up", params)
        except launcher.ClientError as error:
            return error
        return None

    outside = env.temp / "elsewhere"
    outside.mkdir()
    (outside / "x").mkdir()
    secrets_folder = env.orch / "lakehouse" / "secrets"
    secrets_folder.mkdir(parents=True)
    bare = env.orch / "bare"
    bare.mkdir()
    results = {
        "a state folder outside the allowed folders is refused": refused(error_of(state=str(outside)), "args", "outside the allowed folders"),
        "a state folder inside a secrets folder is refused": refused(error_of(state=str(secrets_folder)), "mount"),
        "a relative state folder is refused": refused(error_of(state="orch"), "args"),
        "a missing state folder is refused": refused(error_of(state=str(env.orch / "missing")), "args", "does not exist"),
        "a state folder without the metadata and session folders is refused": refused(error_of(state=str(bare)), "args", "lacks"),
        "a port below 1024 is refused": refused(error_of(api_port=80), "args", "api_port"),
        "a port above 65535 is refused": refused(error_of(api_port=70000), "args", "api_port"),
        "a text port is refused": refused(error_of(api_port="18999"), "args", "api_port"),
        "an unknown parameter is refused": refused(error_of(image="evil"), "args", "image"),
        "a checkout that is not a repository folder is refused": refused(error_of(checkout=str(outside)), "args"),
        "nothing started": not env.created_names() and env.stack_phase() == "down" and not env.daemon.networks,
    }
    return results


@case("stack_no_docker_restart")
def stack_no_docker_restart(env: StackEnv) -> dict[str, bool]:
    """933: starting and stopping the stack sends no restart request and touches no container it did not label."""
    services_script(env)
    env.start()
    rogue = env.daemon.create_rogue("rogue-other-project", {"com.docker.compose.project": "elsewhere"}, running=True)
    env.stack_up()
    env.stack_down()
    paths = [f"{c['method']} {c['path']}" for c in env.daemon.calls]
    rogue_container = env.daemon.find(rogue)
    survived = rogue_container is not None and rogue_container["State"]["Running"]
    with env.daemon.lock:
        env.daemon.containers.pop(rogue, None)
    return {
        "no Docker restart was requested": not any("/_test/restart" in p or "desktop" in p for p in paths),
        "a container of another project still runs": survived,
        "no stop or remove targeted the other project's container": not any(
            c.get("target") == "rogue-other-project" for c in env.daemon.calls if c["method"] in ("POST", "DELETE")
        ),
    }


@case("stack_memory_floor")
def stack_memory_floor(env: StackEnv) -> dict[str, bool]:
    """933c: the stack starts only when the sum of its simultaneous service limits fits the plan's free memory, taken in one snapshot with the registered
    catalog services' growth reserve held back. A refusal happens in the limits step: no network, no container, no registered service."""
    catalog_script(env)
    services_script(env)
    previous = env.daemon.script

    def script(container: dict[str, Any]) -> tuple[float, int] | None:
        return (0.3, 0) if container["Config"]["Labels"].get("hai.service") == "polaris-bootstrap" else previous(container)

    env.daemon.script = script
    env.start()
    minimum = sum(floor for _, _, floor in launcher.ORCHESTRATION_MEMORY.values())
    headroom = 8 * GIB
    results: dict[str, bool] = {}

    def refused_at(label: str, total: int) -> None:
        env.daemon.mem_total = total
        created = len(env.created_names())
        phase = start_stack(env, 40)
        error = str(env.stack().get("error", ""))
        results[f"{label}: the stack is down with a memory cause"] = phase == "down" and "memory" in error
        stack_networks = [n for n in env.daemon.networks.values() if n["Labels"].get(launcher.LABEL_STACK) == "orchestration"]
        results[f"{label}: no container and no network of the stack were created"] = len(env.created_names()) == created and not stack_networks
        results[f"{label}: no service is registered"] = not any(name.startswith("orch-") for name in env.state()["services"])
        if env.stack_phase() == "up":
            env.stack_down()

    refused_at("4.0 GiB free", headroom + 4 * GIB)
    refused_at("one MiB below the minima", headroom + minimum - 1024**2)
    env.daemon.mem_total = headroom + minimum
    plan = env.client().call("plan")
    free = int(plan["environment"]["JOB_MEMORY_LIMIT"])
    phase = start_stack(env, 40)
    limits = {service: env.body_of(service)["HostConfig"]["Memory"] for service in launcher.ORCHESTRATION_MEMORY} if phase == "up" else {}
    results["exactly the minima free: the stack starts (control)"] = phase == "up" and free == minimum
    results["the service limits together do not exceed the plan"] = bool(limits) and sum(limits.values()) <= free
    env.stack_down()
    # A catalog service holds a growth reserve; the stack may not spend it.
    env.daemon.mem_total = 14 * GIB
    results["the catalog comes up on 6 GiB free (control)"] = catalog_up(env) == "up"
    refused_at("a catalog growth reserve on 6 GiB free", 14 * GIB)
    results["the catalog was not touched by the refusal"] = catalog_phase(env) == "up" and len(env.daemon.networks) == 1
    env.daemon.mem_total = 40 * GIB
    results["with 32 GiB free the stack starts beside the catalog (control)"] = start_stack(env, 60) == "up"
    env.stack_down()
    env.client().call("catalog_down")
    env.wait(lambda: catalog_phase(env) == "down" and not env.daemon.networks, 40, "catalog down")
    return results


@case("orchestration_dump_template")
def orchestration_dump_template(env: StackEnv) -> dict[str, bool]:
    """932b: the metadata database dump and the logical XCom query are finite jobs of the allocation-owned orchestration_dump template: the thin
    Airflow image behind the deadline entrypoint, the stack's network, a fixed program with one closed argument (metadata or xcom), the connection string
    only through the read-only metadata folder, one writable folder (the state folder's dumps), no capability. The stack must be up."""
    services_script(env)
    previous = env.daemon.script

    def script(container: dict[str, Any]) -> tuple[float, int] | None:
        """The dump jobs end after a moment, like any finite job."""
        return (0.3, 0) if (container.get("_body") or {}).get("Cmd", [])[:1] == ["/opt/hai/checks/db_dump.py"] else previous(container)

    env.daemon.script = script
    env.start()
    alloc = env.allocate()
    principal = env.alloc_principal(alloc)
    rid = env.registry_id()[:8]

    def submit(launch: str, args: list[str], **params: Any) -> tuple[dict[str, Any] | None, launcher.ClientError | None]:
        try:
            return env.submit(principal, template="orchestration_dump", launch=launch, args=args, params={"state": str(env.orch), **params}), None
        except launcher.ClientError as error:
            return None, error

    stack_down_error = submit("early", ["metadata"])[1]
    env.client().call("release_allocation", {}, principal)
    phase = env.stack_up()
    # The allocation is made with the stack running, so its plan holds back the stack's growth reserve as a real scan would.
    alloc = env.allocate()
    principal = env.alloc_principal(alloc)
    results: dict[str, bool] = {
        "a dump is refused while the stack is not up": refused(stack_down_error, "refused", "not up"),
        "the stack is up (control)": phase == "up",
    }
    for what in ("metadata", "xcom"):
        reply, error = submit(f"dump-{what}", [what])
        attempt = env.wait_ended(reply["attempt_id"]) if reply else {}
        body = env.daemon.create_bodies.get(attempt.get("container_name", ""), {})
        mounts = mount_pairs(body) if body else []
        environment = dict(item.split("=", 1) for item in body.get("Env", []))
        results.update(
            {
                f"{what}: the job was accepted and ended exited": error is None and attempt.get("reason") == "exited" and attempt.get("exit_code") == 0,
                f"{what}: it runs the thin Airflow image behind the deadline entrypoint": body.get("Image") == AIRFLOW_IMAGE
                and body.get("Entrypoint") == [launcher.DEADLINE_ENTRYPOINT, "python"],
                f"{what}: its command is the dump script with the one closed argument": body.get("Cmd") == ["/opt/hai/checks/db_dump.py", what],
                f"{what}: it joins the stack's network": body.get("HostConfig", {}).get("NetworkMode") == f"hai-{rid}-orch-net",
                f"{what}: no password or connection string is in the environment": not any(
                    "PASS" in name.upper() or "CONN" in name.upper() or "synthetic-password" in value for name, value in environment.items()
                ),
                f"{what}: the metadata folder and the script are read-only and only /out is writable": ("/run/hai/metadata", True)
                in {(t, ro) for _, t, ro in mounts}
                and ("/opt/hai/checks/db_dump.py", True) in {(t, ro) for _, t, ro in mounts}
                and sorted({t for _, t, ro in mounts if not ro}) == ["/out"],
                f"{what}: /out is the state folder's dumps folder": [s for s, t, _ in mounts if t == "/out"] == [str((env.orch / "dumps").resolve())],
                f"{what}: no mount is the Docker socket, an AWS folder or a lakehouse secret": not any(
                    source.endswith(".sock") or ".aws" in source or "lakehouse/secrets" in source for source, _, _ in mounts
                ),
                f"{what}: no capability, no new privileges, the caller's UID": body.get("HostConfig", {}).get("CapDrop") == ["ALL"]
                and "no-new-privileges:true" in body.get("HostConfig", {}).get("SecurityOpt", [])
                and str(body.get("User", "")).startswith(f"{os.getuid()}:"),
                f"{what}: the job has a memory limit from the allocation": int(body.get("HostConfig", {}).get("Memory", 0)) > 0,
            }
        )
    results["the dumps folder is owner-only"] = (env.orch / "dumps").is_dir() and ((env.orch / "dumps").stat().st_mode & 0o777) == 0o700
    results["an argument outside metadata and xcom is refused"] = refused(submit("bad-arg", ["--all"])[1], "args")
    results["no argument is refused"] = refused(submit("no-arg", [])[1], "args")
    results["two arguments are refused"] = refused(submit("two-args", ["metadata", "xcom"])[1], "args")
    results["a parameter other than the state folder is refused"] = refused(submit("bad-param", ["metadata"], mount="/")[1], "args")
    results["a state folder outside the allowed folders is refused"] = refused(submit("outside", ["metadata"], state=str(env.temp / "elsewhere"))[1], "args")
    env.client().call("release_allocation", {}, principal)
    env.stack_down()
    return results


@case("pull_rules")
def pull_rules(env: StackEnv) -> dict[str, bool]:
    """934: pull accepts only a digest-pinned reference that a template names; a tag, an unnamed digest and a local build are refused."""
    env.start()
    client = env.client()
    client2 = env.client()

    def attempt(reference: str) -> launcher.ClientError | None:
        try:
            client.call("pull", {"reference": reference})
        except launcher.ClientError as error:
            return error
        return None

    named = "apache/polaris:1.8.0@sha256:347615e736cab2c9be0d8c464552ac930be4755302bb0aeb745216bbae99ba5b"
    env.daemon.images.pop(named, None)
    env.daemon.images.pop(POSTGRES_IMAGE, None)
    results: dict[str, bool] = {
        "a tag-only reference is refused": refused(attempt("postgres:17"), "refused", "digest"),
        "a latest tag is refused": refused(attempt("apache/airflow:latest"), "refused", "digest"),
        "a digest that no template names is refused": refused(attempt(f"postgres@{UNPINNED_DIGEST}"), "refused", "no template names"),
        "a reference with an extra option is refused": refused(attempt(f"{POSTGRES_IMAGE} --platform=linux/arm64"), "args"),
        "the locally built image is refused (it is built, not pulled)": refused(attempt(AIRFLOW_IMAGE), "refused", "digest"),
        "nothing was pulled by the refused requests": not env.daemon.calls_matching("POST", "/images/create"),
    }
    reply = client.call("pull", {"reference": named})
    pulls = env.daemon.calls_matching("POST", "/images/create")
    results["a digest named by the Compose model of the lakehouse is pulled"] = bool(reply.get("pulled")) and len(pulls) == 1
    results["the daemon was asked for the name and the digest separately"] = pulls[0]["query"] == {
        "fromImage": "apache/polaris",
        "tag": "sha256:347615e736cab2c9be0d8c464552ac930be4755302bb0aeb745216bbae99ba5b",
    }
    again = client.call("pull", {"reference": named})
    results["a second request finds the image present and pulls nothing"] = (
        again.get("pulled") is False and len(env.daemon.calls_matching("POST", "/images/create")) == 1
    )
    reply2 = client2.call("pull", {"reference": POSTGRES_IMAGE})
    results["a digest named by the orchestration Compose model is pulled"] = bool(reply2.get("pulled"))
    env.daemon.images.pop(named.partition(":")[0] + "@" + named.partition("@")[2], None)
    env.engine.pull_error = "manifest unknown"
    results["a pull the daemon refuses is reported as a failure"] = refused(attempt(named), None, "manifest unknown")
    env.engine.pull_error = ""
    return results


@case("orchestration_check_template")
def orchestration_check_template(env: StackEnv) -> dict[str, bool]:
    """934: the in-image offline check is a finite attempt of the orchestration_check template: no network, read-only repository mounts, deadline."""
    env.start()
    alloc = env.allocate()
    principal = env.alloc_principal(alloc)
    reply = env.submit(principal, template="orchestration_check", launch="check", args=[], params={})
    attempt = env.wait_ended(reply["attempt_id"])
    body = env.daemon.create_bodies[attempt["container_name"]]
    mounts = mount_pairs(body)
    refused_args = None
    try:
        env.submit(principal, template="orchestration_check", launch="check2", args=["--extra"], params={})
    except launcher.ClientError as error:
        refused_args = error
    refused_params = None
    try:
        env.submit(principal, template="orchestration_check", launch="check3", args=[], params={"mount": "/"})
    except launcher.ClientError as error:
        refused_params = error
    return {
        "the attempt ended exited": attempt["reason"] == "exited",
        "it runs on no network": body["HostConfig"]["NetworkMode"] == "none",
        "it runs behind the deadline entrypoint": body["Entrypoint"][0] == launcher.DEADLINE_ENTRYPOINT and body["Entrypoint"][1] == "python",
        "its command is the in-image check": body["Cmd"] == ["/opt/hai/checks/in_image_checks.py"],
        "every mount is read-only": bool(mounts) and all(read_only for _, _, read_only in mounts),
        "the constraints file, the DAGs and the graph module are mounted": {t for _, t, _ in mounts}
        >= {"/opt/hai/constraints.txt", "/opt/hai/dags", "/opt/hai/orchestration/graph.py", "/opt/hai/checks/in_image_checks.py"},
        "it holds no capability and no new privileges": body["HostConfig"]["CapDrop"] == ["ALL"],
        "an argument is refused": refused(refused_args, "args"),
        "a parameter is refused": refused(refused_params, "args"),
    }


# Every case, listed so that a case that loses its decorator cannot go unnoticed: the runner checks the list against the registry.
ALL_CASES = (
    stack_up_down,
    stack_partial_start,
    stack_unhealthy_service,
    stack_teardown_barrier,
    stack_resume_after_launcher_kill,
    stack_independent_of_catalog,
    stack_request_validation,
    stack_no_docker_restart,
    stack_memory_floor,
    orchestration_dump_template,
    pull_rules,
    orchestration_check_template,
)


def run_case(name: str, report: Path) -> dict[str, Any]:
    """Run one case in its own harness."""
    function, settings = CASES[name]
    env = StackEnv(name, report, **dict(settings))
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
    launcher_source = hashlib.sha256((ROOT / "orchestration" / "launcher.py").read_bytes()).hexdigest()
    summary = {
        "feature": "orchestration stack lifecycle and image pull in the launcher, fake Docker, test registries",
        "command": ".venv/bin/python -m scripts.orchestration.run_stack_e2e",
        "started_utc": stamp,
        "head": run_command("git", ["rev-parse", "HEAD"], cwd=ROOT).stdout.strip(),
        "launcher_sha256": launcher_source,
        "python": sys.version.split()[0],
        "cases": results,
        "passed": sum(1 for r in results if r["status"] == "pass"),
        "total": len(results),
        "limits": ["A fake daemon: no real container, image pull or Airflow process runs here (see run_real_e2e)."],
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
