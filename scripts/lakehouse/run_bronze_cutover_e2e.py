"""E2E of the bronze cutover: the launcher's authority method and Mac write gate on a test registry with fake Docker.

Run ``.venv/bin/python -m scripts.lakehouse.run_bronze_cutover_e2e [--case NAME ...]``. Each case starts its own launcher on its own test
registry (the stand-ins of ``run_launcher_e2e``); nothing calls AWS or Docker. The report is written under ``data/e2e/bronze_cutover/<stamp>/``.
Failure IDs (F1 and the rest) refer to the bronze registration failure modes of the cutover design.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
import traceback
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from orchestration import catalog_authority
from scripts.lakehouse import run_launcher_e2e as launcher_e2e
from scripts.lakehouse.run_launcher_e2e import Env, catalog_script, catalog_up, expect_error, lifecycle_host, real_run, record_file, refused, run_lock
from scripts.process import run_command

ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / "data" / "e2e" / "bronze_cutover"
Case = Callable[[Env], dict[str, bool]]
CASES: dict[str, Case] = {}


def case(function: Case) -> Case:
    CASES[function.__name__] = function
    return function


def command(command_id: str, epoch: int, state: str, cutover: str, action: str = "transition") -> dict[str, Any]:
    return {
        "command_id": command_id,
        "expected_epoch": epoch,
        "action": action,
        "state": state,
        "cutover_id": cutover,
        "reason": f"bronze cutover E2E {command_id}",
        "time": datetime.now(UTC).isoformat(),
    }


def owner_call(env: Env, body: dict[str, Any], *, tty: bool = True, confirmation: str | None = None) -> dict[str, Any]:
    text = catalog_authority.confirmation_text(body) if confirmation is None else confirmation
    return dict(env.client().call("catalog_authority", {"command": body, "confirmation": text, "tty": tty, "owner": True}))


def authority(env: Env) -> dict[str, Any]:
    return dict(env.state()["launcher"].get("catalog_authority") or {})


def writer_submit(env: Env, principal: dict[str, Any], launch: str) -> dict[str, Any]:
    return env.submit(principal, "spark_write", launch, ["--group", "hai"], {"job": "bronze"})


def end_run(env: Env, principal: dict[str, Any], run_id: str) -> None:
    client = env.client()
    for attempt in env.state()["attempts"].values():
        if attempt["owner"] == f"run:{run_id}" and attempt["phase"] != "ended":
            client.call("cancel", {"attempt_id": attempt["id"]}, principal)
            env.wait_ended(attempt["id"])
    client.call("finalize", {}, principal)
    client.call("release", {"record": str(record_file(env, run_id))}, principal, timeout=60)


@case
def freeze_gate_and_fence(env: Env) -> dict[str, bool]:
    """F1, F2, F3, F4: the freeze refuses Mac writes, the fence waits for the running writer, a repeat returns the stored record."""
    catalog_script(env)
    env.start()
    catalog_up(env)
    writer = real_run(env, "writer", "acquisition", write_prefixes=["lakehouse/bronze/"])
    checks: dict[str, bool] = {}
    checks["Mac authority initializes at epoch 1"] = owner_call(env, command("c_init", 0, "mac_writer", "mac_initial", "initialize"))["epoch"] == 1
    running = writer_submit(env, writer, "w1")
    env.wait_phase(running["attempt_id"], ("started",))
    checks["a Mac write starts under mac_writer"] = True
    frozen = owner_call(env, command("c_freeze", 1, "freezing", "cut1"))
    checks["freeze records freezing at P1 with every layer off"] = frozen["state"] == "freezing" and set(frozen["production_layers"].values()) == {"off"}
    checks["F1: a Mac write submit after the freeze is refused"] = refused(
        expect_error(lambda: writer_submit(env, writer, "w2")), "refused", "catalog authority"
    )
    fence = command("c_fence", 2, "source_fenced", "cut1")
    busy = expect_error(lambda: owner_call(env, fence))
    checks["F2: the fence is refused while a Mac write runs and the state stays freezing"] = (
        refused(busy, "refused", "Mac catalog writes are still open") and authority(env)["state"] == "freezing"
    )
    checks["F2: the running writer is not stopped by the freeze"] = env.phase(running["attempt_id"]) == "started"
    env.client().call("cancel", {"attempt_id": running["attempt_id"]}, writer)
    env.wait_ended(running["attempt_id"])
    fenced = owner_call(env, fence)
    checks["the same fence command passes once no Mac write is open"] = fenced["state"] == "source_fenced" and fenced["epoch"] == 3
    checks["F3: a repeated command returns the stored record without a new epoch"] = owner_call(env, fence) == fenced and authority(env)["epoch"] == 3
    checks["F3: a stale epoch is refused"] = refused(expect_error(lambda: owner_call(env, command("c_stale", 1, "refreshing", "cut1"))), "refused", "stale")
    refresh = command("c_refresh", 3, "refreshing", "cut1")
    checks["F4: a command without a terminal is refused"] = refused(expect_error(lambda: owner_call(env, refresh, tty=False)), "refused", "terminal")
    checks["F4: a wrong typed confirmation is refused"] = refused(expect_error(lambda: owner_call(env, refresh, confirmation="yes")), "refused", "typed")
    checks["refused commands left the record unchanged"] = authority(env)["epoch"] == 3
    intents = env.config.registry.path / "catalog_authority_intents"
    checks["every applied command left one intent file"] = sorted(path.name for path in intents.iterdir()) == ["c_fence.json", "c_freeze.json", "c_init.json"]
    end_run(env, writer, "writer")
    return checks


@case
def queued_write_fenced(env: Env) -> dict[str, bool]:
    """F1: a Mac write queued before the freeze ends as fenced instead of starting after it."""
    catalog_script(env)
    instance = lifecycle_host(env, target="q1", held_phase="queued")
    catalog_up(env)
    writer = real_run(env, "writer", "acquisition", write_prefixes=["lakehouse/bronze/"])
    owner_call(env, command("c_init", 0, "mac_writer", "mac_initial", "initialize"))
    queued = writer_submit(env, writer, "q1")
    was_queued = env.phase(queued["attempt_id"]) == "queued"
    owner_call(env, command("c_freeze", 1, "freezing", "cut1"))
    instance.held_phase = ""
    ended = env.wait_ended(queued["attempt_id"])
    checks = {
        "the write was queued before the freeze": was_queued,
        "the queued write ends as fenced": ended["reason"] == "fenced",
        "no container was created for it": not launcher_e2e.creates(env, "-q1-"),
        "the fence then passes": owner_call(env, command("c_fence", 2, "source_fenced", "cut1"))["state"] == "source_fenced",
    }
    end_run(env, writer, "writer")
    return checks


@case
def rollback_paths(env: Env) -> dict[str, bool]:
    """F5, B1, B2, B4: rollback from P1 and P3, a reused cutover ID, and the refusal after a writer was admitted (P5)."""
    env.start()
    checks: dict[str, bool] = {}
    owner_call(env, command("c_init", 0, "mac_writer", "mac_initial", "initialize"))
    owner_call(env, command("p1_freeze", 1, "freezing", "cut1"))
    owner_call(env, command("p1_back", 2, "rollback_pending", "cut1"))
    rollback = command("p1_mac", 3, "mac_writer", "cut1")
    back = owner_call(env, rollback)
    checks["B2: rollback at P1 returns to mac_writer"] = back["state"] == "mac_writer" and back["phase"] == "P1"
    checks["B4: a repeated rollback returns the stored record"] = owner_call(env, rollback) == back
    checks["F3: the same command ID with other content is refused"] = refused(
        expect_error(lambda: owner_call(env, {**rollback, "reason": "changed"})), "refused", "identity conflict"
    )
    checks["F5: a new freeze that reuses the cutover ID is refused"] = refused(
        expect_error(lambda: owner_call(env, command("reuse", 4, "freezing", "cut1"))), "refused", "new cutover identity"
    )
    epoch = 4
    for name, state in (("p3_freeze", "freezing"), ("p3_fence", "source_fenced"), ("p3_refresh", "refreshing"), ("p3_back", "rollback_pending")):
        owner_call(env, command(name, epoch, state, "cut2"))
        epoch += 1
    direct = expect_error(lambda: owner_call(env, command("p3_direct", 8, "mac_writer", "cut2")))
    checks["B2: rollback from P3 cannot skip source verification"] = refused(direct, "refused", "source verification")
    owner_call(env, command("p3_verify", 8, "source_verify", "cut2"))
    restored = owner_call(env, command("p3_mac", 9, "mac_writer", "cut2"))
    checks["B2: rollback from P3 returns to mac_writer through source_verify"] = restored["state"] == "mac_writer" and restored["epoch"] == 10
    epoch = 10
    for name, state in (("p5_freeze", "freezing"), ("p5_fence", "source_fenced"), ("p5_refresh", "refreshing"), ("p5_switch", "aws_writer")):
        owner_call(env, command(name, epoch, state, "cut3"))
        epoch += 1
    checks["the switch records aws_writer at P4 with every layer off"] = authority(env)["phase"] == "P4" and set(
        authority(env)["production_layers"].values()
    ) == {"off"}
    with run_lock.transaction(run_lock.Registry(env.registry, "test")) as registry_state:
        registry_state["launcher"]["catalog_authority"]["phase"] = "P5"
    late = expect_error(lambda: owner_call(env, command("p5_back", 14, "rollback_pending", "cut3")))
    checks["B1: rollback after an admitted AWS writer (P5) is refused"] = refused(late, "refused", "admitted writer")
    return checks


def run_cases(selected: list[str]) -> int:
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    report = OUT / stamp
    report.mkdir(parents=True)
    results: list[dict[str, Any]] = []
    for name in selected or list(CASES):
        env = Env(name, report)
        started = time.monotonic()
        error = ""
        checks: dict[str, bool] = {}
        try:
            checks = CASES[name](env)
        except Exception as exc:
            error = f"{type(exc).__name__}: {exc}\n{traceback.format_exc()[-1500:]}"
        leftovers = env.close()
        checks["nothing of the case is left over"] = not leftovers
        status = "pass" if checks and all(checks.values()) and not error else "fail"
        results.append({"case": name, "status": status, "checks": checks, "error": error, "seconds": round(time.monotonic() - started, 1)})
        sys.stdout.write(f"{'ok' if status == 'pass' else 'FAIL':<5} {name} ({results[-1]['seconds']} s)\n")
        for check, ok in checks.items():
            if not ok:
                sys.stdout.write(f"      failed: {check}\n")
        if error:
            sys.stdout.write(f"      error: {error.splitlines()[0]}\n")
    head = run_command("git", ["rev-parse", "HEAD"], cwd=ROOT).stdout.strip()
    passed = sum(1 for result in results if result["status"] == "pass")
    summary = {
        "feature": "bronze cutover: launcher authority, inventory, registration and owner CLI with stand-ins",
        "command": ".venv/bin/python -m scripts.lakehouse.run_bronze_cutover_e2e",
        "started_utc": stamp,
        "head": head,
        "cases": results,
        "passed": passed,
        "total": len(results),
    }
    (report / "report.json").write_text(json.dumps(summary, indent=2) + "\n")
    sys.stdout.write(f"{passed}/{len(results)}: {report.relative_to(ROOT)}/report.json\n")
    return 0 if passed == len(results) else 1


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--case", action="append", default=[], choices=sorted(CASES))
    return run_cases(parser.parse_args().case)


if __name__ == "__main__":
    sys.exit(main())
