"""Real Docker runs of the fixture templates through the launcher, on a test registry.

Run ``.venv/bin/python -m scripts.lakehouse.run_launcher_real_fixture --image-suffix l1a-real`` with Docker running. Reports stay in
data/e2e/launcher_real/<stamp>/. It starts a launcher on a throwaway test registry that talks to the real Docker daemon, with the three
local images extended by the deadline entrypoint (named ``<repository>:<suffix>``; they are built from the pinned local images with no
network and removed after the run). It runs, one at a time: ``duckdb_helper``, ``fixture_init``, ``fixture_dbt`` (parse and a small
build), ``quality_validate``, ``spark_e2e`` (the pin check on its throwaway catalog) and ``dbt_deps`` (into an empty scratch folder),
then the deadline entrypoint inside a real container, a cancel, a launcher killed during an attempt and restarted, and an orphan.
It never starts the catalog, reads bronze, uses AWS or a secret.

This runner is a launcher client and nothing else: every container, network and removal is an operation of the launcher it starts, and what
it observes of the daemon it reads from that launcher's status (its inventory by registry label). It writes only its throwaway test
registry; it never reads or writes the main checkout's registry. The machine is shared with other jobs through the launcher's own
allocation inside this test registry only: running it while another package's job holds the machine is the lead's call (the cross-registry
admission of the former reservation no longer exists).
"""

from __future__ import annotations

import argparse
import contextlib
import json
import os
import shutil
import signal
import sys
import tempfile
import time
import traceback
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from orchestration import launcher
from scripts.lakehouse import package_fixture, run_lock
from scripts.process import run_command

ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / "data" / "e2e" / "launcher_real"
IMAGE_SERVICES = {"analytics-dbt": "hai-analytics", "quality": "hai-quality", "spark": "hai-lakehouse-spark"}


class Run:
    """The state of one real run: the registry, the launcher client, the recorded steps."""

    def __init__(self, suffix: str, report: Path) -> None:
        self.report = report
        self.steps: list[dict[str, Any]] = []
        self.temp = Path(tempfile.mkdtemp(prefix="hr"))
        self.registry = self.temp / "registry"
        self.registry.mkdir(mode=0o700)
        compose = launcher.ComposeModel(ROOT / "docker-compose.yaml")
        images = {compose.image(service): f"{repository}:{suffix}" for service, repository in IMAGE_SERVICES.items()}
        settings = {
            "docker_socket": launcher.docker_socket_path(),
            "timings": {"tick": 0.1, "backoff_start": 0.5, "backoff_cap": 4},
            "images": images,
            "token_root": str(self.temp / "tokens"),
            "runs_root": str(self.temp / "runs"),
            "lakehouse_state": str(self.temp / "lakehouse"),
        }
        (self.registry / "launcher_test.json").write_text(json.dumps(settings, indent=2))
        self.config = launcher.resolve_config(str(self.registry))
        self.client = launcher.LauncherClient(self.config)
        self.registry_id = str(json.loads((self.registry / "registry_id").read_text())["registry_id"])

    def step(self, name: str, ok: bool, **evidence: Any) -> bool:
        """Record one step."""
        self.steps.append({"step": name, "ok": bool(ok), "at": datetime.now(UTC).isoformat(), **evidence})
        sys.stdout.write(f"{'ok' if ok else 'FAIL':<5} {name}\n")
        return ok

    def state(self) -> dict[str, Any]:
        """The registry file."""
        return dict(json.loads((self.registry / "registry.json").read_text()))

    def status(self) -> dict[str, Any]:
        """The launcher's own status: its inventory of the containers and networks that carry this registry's label."""
        return dict(self.client.call("status", {"what": "launcher"}))

    def labelled(self) -> list[str]:
        """Names of the containers on the daemon that carry this registry's label, as the launcher reports them."""
        return [str(name) for name in self.status()["containers"]]

    def wait(self, predicate: Any, seconds: float, what: str) -> Any:
        """Poll until the predicate is truthy."""
        deadline = time.monotonic() + seconds
        while time.monotonic() < deadline:
            value = predicate()
            if value:
                return value
            time.sleep(0.2)
        raise RuntimeError(f"timeout waiting for {what}")


def scenarios(run: Run) -> None:
    """The real fixture runs."""
    client = run.client
    client.connect().close()
    run.wait(lambda: client.call("ping").get("status") == "serving", 90, "the launcher serves the real daemon")
    run.wait(lambda: not run.labelled(), 30, "the probe container is removed")
    state = run.state()
    run.step(
        "the launcher serves a real daemon: probe acknowledged and scan done",
        state["launcher"]["stream_epoch"] >= 1 and state["launcher"]["docker_state"] == "ok" and not run.labelled(),
        epoch=state["launcher"]["stream_epoch"],
    )
    allocation = client.allocate()
    principal = client.principal_alloc(allocation)
    plan = run.state()["allocations"][allocation]["environment"]
    run.step("the allocation holds a plan from the real daemon", int(plan["JOB_MEMORY_LIMIT"]) >= 4 * 1024**3, environment=plan)

    package_fixture.TEST_REGISTRY = str(run.registry)
    session = package_fixture.Session(client, allocation)
    package_fixture.ACTIVE.append(session)
    started = time.monotonic()
    answer = client.run_attempt(
        principal, "duckdb_helper", "answer", [], {"mounts": [], "sql": "SELECT 6 * 7 AS answer", "flags": ["-csv", "-noheader"]}, checkout=ROOT
    )
    run.step(
        "duckdb_helper: a query without a database runs in the pinned image",
        answer.ok and answer.stdout.strip() == "42",
        exit=answer.exit_code,
        stdout=answer.stdout.strip(),
    )

    case = package_fixture.make_case("l1a_real", session, [])
    run.step(
        "fixture_init: the fixture bronze database is written by the pinned image",
        (case.folder / "fixture_lakehouse.duckdb").is_file(),
        seconds=round(time.monotonic() - started, 1),
    )

    code, tail, statuses = package_fixture.dbt(case, ["parse"], session)
    run.step("fixture_dbt: parse on the case project", code == 0 and (case.folder / "target" / "manifest.json").is_file(), exit=code, tail=tail[-300:])
    code, tail, statuses = package_fixture.dbt(case, ["build", "--select", "+assert_occmix_values_cast", "--indirect-selection", "cautious"], session)
    failed = sorted(node for node, status in statuses.items() if status not in ("success", "pass"))
    run.step("fixture_dbt: a small build passes", code == 0 and bool(statuses) and not failed, nodes=len(statuses), failed=failed, tail=tail[-300:])
    ls = package_fixture.selection(case, ["+assert_occmix_values_cast"], session)
    run.step(
        "fixture_dbt: ls returns the node IDs on standard output",
        len(ls) > 0 and all(item.startswith(("model.", "test.", "seed.", "source.")) for item in ls),
        nodes=len(ls),
    )
    rows = package_fixture.duckdb_query(case.folder / "staging.duckdb", "SELECT count(*) FROM int_occmix_survey_rows;", session)
    run.step("duckdb_helper: a read-only query on the built file", bool(rows) and int(rows[0][0]) > 0, rows=rows)

    suites_out = case.folder / "ge_out"
    suites_out.mkdir(exist_ok=True)
    quality = client.run_attempt(
        principal,
        "quality_validate",
        "gx",
        [],
        {
            "database": str(case.folder / "staging.duckdb"),
            "build_root": str(case.folder),
            "suites_host": str(ROOT / "data_contracts/great_expectations/suites"),
            "baselines_host": str(ROOT / "data_contracts/great_expectations/baselines.json"),
            "out_host": str(suites_out),
        },
        checkout=ROOT,
    )
    run.step(
        "quality_validate: the quality image ran with no network and wrote its result",
        (suites_out / "result.json").is_file(),
        exit=quality.exit_code,
        reason=quality.reason,
    )

    pins = client.run_attempt(principal, "spark_e2e", "pins", [], {"job": "pins_e2e"}, checkout=ROOT)
    run.step("spark_e2e: the pin check ran on a throwaway catalog", pins.ok, exit=pins.exit_code, reason=pins.reason, tail=(pins.stdout + pins.stderr)[-300:])

    scratch = ROOT / "data" / "e2e" / "launcher_real" / "scratch_packages"
    shutil.rmtree(scratch, ignore_errors=True)
    scratch.mkdir(parents=True)
    deps = client.run_attempt(principal, "dbt_deps", "deps", ["deps"], {"packages_host": str(scratch)}, checkout=ROOT)
    run.step(
        "dbt_deps: packages from the lock file into an empty scratch folder",
        deps.ok and (scratch / "dbt_utils").is_dir(),
        exit=deps.exit_code,
        installed=sorted(p.name for p in scratch.iterdir()),
    )

    long_sql = "SELECT count(*) FROM range(100000000000) a, range(100000000000) b;"
    slow = {"mounts": [], "sql": long_sql, "flags": ["-csv", "-noheader"]}
    begun = time.time()
    limited = client.run_attempt(principal, "duckdb_helper", "limit", [], slow, checkout=ROOT, seconds=12)
    elapsed = time.time() - begun
    run.step(
        "the in-image deadline entrypoint stops a running job before the deadline (exit 124)",
        limited.exit_code == 124 and elapsed < 12 + 5,
        exit=limited.exit_code,
        reason=limited.reason,
        seconds=round(elapsed, 1),
    )

    reply = client.call("submit", {"template": "duckdb_helper", "launch_id": "cancel", "args": [], "params": slow, "checkout": str(ROOT)}, principal)
    run.wait(lambda: run.state()["attempts"][reply["attempt_id"]]["phase"] == "started", 60, "the long query starts")
    client.call("cancel", {"attempt_id": reply["attempt_id"]}, principal)
    ended = client.wait_for("attempt", reply["attempt_id"], "phase", ("ended",), principal, 90)
    run.step(
        "cancel: a started container is stopped and removed through cleanup",
        ended["reason"] == "cancelled" and not run.labelled(),
        reason=ended["reason"],
        exit=ended.get("exit_code"),
    )

    reply = client.call("submit", {"template": "duckdb_helper", "launch_id": "kill", "args": [], "params": slow, "checkout": str(ROOT)}, principal)
    run.wait(lambda: run.state()["attempts"][reply["attempt_id"]]["phase"] == "started", 60, "the long query starts again")
    before = run.state()["attempts"][reply["attempt_id"]]
    pid = int(client.call("ping")["pid"])
    os.kill(pid, signal.SIGKILL)
    run.wait(lambda: not _alive(pid), 15, "the launcher is gone")
    client.connect().close()
    run.wait(lambda: client.call("ping").get("status") == "serving", 90, "the launcher serves again")
    adopted = run.state()["attempts"][reply["attempt_id"]]
    still_running = run.labelled()
    run.step(
        "a launcher killed during an attempt finds the same container again",
        adopted["phase"] == "started" and adopted["container_id"] == before["container_id"] and str(before["container_name"]) in still_running,
        containers=still_running,
    )
    client.call("cancel", {"attempt_id": reply["attempt_id"]}, principal)
    ended = client.wait_for("attempt", reply["attempt_id"], "phase", ("ended",), principal, 90)
    run.step("the restarted launcher cleans the container up", ended["reason"] == "cancelled" and not run.labelled(), reason=ended["reason"])

    # An orphan on the real daemon without any actor outside the launcher: the registry loses a running attempt's records (as a lost
    # registry file would), and the restarted launcher finds a labelled container that no operation or attempt owns.
    reply = client.call("submit", {"template": "duckdb_helper", "launch_id": "orphan", "args": [], "params": slow, "checkout": str(ROOT)}, principal)
    run.wait(lambda: run.state()["attempts"][reply["attempt_id"]]["phase"] == "started", 60, "the long query starts for the orphan")
    victim = run.state()["attempts"][reply["attempt_id"]]
    name = str(victim["container_name"])
    pid = int(client.call("ping")["pid"])
    os.kill(pid, signal.SIGKILL)
    run.wait(lambda: not _alive(pid), 15, "the launcher is gone before the orphan")
    with run_lock.transaction(run_lock.Registry(run.registry, "test")) as state:
        state["attempts"].pop(reply["attempt_id"])
        for op_id in [o for o, op in state["operations"].items() if op["attempt"] == reply["attempt_id"]]:
            state["operations"].pop(op_id)
        state["containers"].pop(str(victim["container_id"]), None)
    client.connect().close()
    run.wait(lambda: client.call("ping").get("status") == "serving", 90, "the launcher serves again after the orphan")
    run.wait(lambda: name not in run.labelled(), 60, "the orphan is removed")
    orphans = [str(item["name"]) for item in run.state()["launcher"].get("orphans", [])]
    run.step("a labelled container no operation owns is removed as an orphan by the launcher", name in orphans and name not in run.labelled(), orphan=name)
    client.release_allocation(allocation)
    package_fixture.ACTIVE.clear()
    package_fixture.TEST_REGISTRY = None
    run.wait(lambda: run.status()["open_operations"] == 0, 60, "the launcher settles its operations")
    final = run.status()
    run.step(
        "nothing of the run is left on the daemon (the launcher's inventory)",
        not final["containers"] and not final["networks"] and final["open_operations"] == 0,
    )


def _alive(pid: int) -> bool:
    """True while the process exists."""
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    return True


def main() -> int:
    """Admit the machine, run the scenarios, write the report and clean up whatever happens."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--image-suffix", required=True, help="the tag suffix of the extended images (hai-analytics:<suffix>, ...)")
    args = parser.parse_args()
    report = OUT / datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    report.mkdir(parents=True)
    run: Run | None = None
    error = ""
    try:
        run = Run(args.image_suffix, report)
        scenarios(run)
    except Exception as exc:
        error = f"{type(exc).__name__}: {exc}\n{traceback.format_exc()[-1500:]}"
    finally:
        left: list[str] = []
        if run is not None:
            try:
                package_fixture.TEST_REGISTRY = str(run.registry)
                client = launcher.LauncherClient(run.config, start=False)
                for attempt_id, attempt in run.state()["attempts"].items():
                    if attempt["phase"] != "ended" and attempt["owner"].startswith("alloc:"):
                        client.call("cancel", {"attempt_id": attempt_id}, client.principal_alloc(attempt["owner"].partition(":")[2]))
                with contextlib.suppress(RuntimeError, launcher.ClientError, OSError):
                    run.wait(lambda: not run.labelled(), 90, "the launcher removes the containers of the run")
                with contextlib.suppress(launcher.ClientError, OSError):
                    left = [str(name) for name in run.labelled()]
                client.call("shutdown", {"owner": True}, {"kind": "any", "owner": True})
            except Exception as exc:
                sys.stderr.write(f"cleanup: the launcher did not stop on request ({type(exc).__name__}: {exc})\n")
            time.sleep(1)
            evidence = report / "evidence"
            evidence.mkdir()
            for name in ("registry.json", "launcher.log", "launcher_test.json"):
                if (run.registry / name).exists():
                    shutil.copy2(run.registry / name, evidence / name)
            if (run.registry / "allocations").exists():
                shutil.copytree(run.registry / "allocations", evidence / "allocations")
            shutil.rmtree(run.temp, ignore_errors=True)
    steps = run.steps if run is not None else []
    status = "pass" if steps and all(s["ok"] for s in steps) and not error and not left else "fail"
    summary = {
        "feature": "single launcher on the real Docker daemon, fixture templates (no catalog, no bronze, no AWS)",
        "command": ".venv/bin/python -m scripts.lakehouse.run_launcher_real_fixture --image-suffix " + args.image_suffix,
        "head": run_command("git", ["rev-parse", "HEAD"], cwd=ROOT).stdout.strip(),
        "status": status,
        "error": error,
        "containers_left": left,
        "steps": steps,
    }
    (report / "report.json").write_text(json.dumps(summary, indent=2) + "\n")
    sys.stdout.write(f"{status}: {report.relative_to(ROOT)}/report.json\n")
    if error:
        sys.stdout.write(error.splitlines()[0] + "\n")
    return 0 if status == "pass" else 1


if __name__ == "__main__":
    sys.exit(main())
