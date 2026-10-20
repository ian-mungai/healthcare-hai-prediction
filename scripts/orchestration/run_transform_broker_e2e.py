"""End-to-end runs of the broker's run-control rows, the build file checkpoint and the shipped fixture rows, against a fake Docker Engine, in test registries.

Run ``.venv/bin/python -m scripts.orchestration.run_transform_broker_e2e [--case NAME ...] [--checks]``. Each case uses the harness of
``scripts/orchestration/run_broker_e2e.py`` (its own test registry, fake daemon and launcher process); the broker runs in the case's process, or as a
``python -m scripts.orchestration.broker submit-once`` child where the case kills it between two steps of an action. The control rows (``acquire``, ``record``,
``gate_accepted``, ``gate_rejected``, ``release_accepted`` and ``release_rejected``) act on the launcher's run methods; the cases assert the launcher's lock
state, the files and the ledger. ``shipped_rows_render`` submits the shipped fixture rows to the launcher's renderer on a copy of the checkout and reads the
effective specification of every container. No real Docker, catalog, AWS service or secret is used. The report is
``data/e2e/orchestration_transform_broker/<UTC>/report.json``.
"""

from __future__ import annotations

import argparse
import contextlib
import copy
import hashlib
import json
import os
import shutil
import stat
import sys
import tempfile
import time
import traceback
import uuid
from collections.abc import Callable, Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from orchestration import dbt_lineage
from orchestration.interface import Handle, State, Status, SubmissionRequest
from scripts.orchestration.broker import Broker, BrokerError, load_templates
from scripts.orchestration.run_broker_e2e import (
    RUN,
    BrokerEnv,
    drift_inventory,
    events,
    ledger_lines,
    lose_submit_reply,
    map_to_control,
    refusal_of,
    soft_wait,
    state_of,
    stop_attempt,
    wire,
)
from scripts.orchestration.transform_fixtures import CASE_CONTAINER, FIXTURE_BRONZE_BYTES, FIXTURE_BRONZE_NAME
from scripts.process import run_command

ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / "data" / "e2e" / "orchestration_transform_broker"
OTHER = "20261013T090100Z-0b1b2b3b"
SHIPPED = ROOT / "config" / "orchestration" / "broker_templates.json"
CHECKPOINT_KEYS = ("records_build_file", "build_file_result", "requires_build_file")


def reported(data: bytes) -> dict[str, Any] | None:
    return {"build_file_sha256": hashlib.sha256(data).hexdigest()}


class Env(BrokerEnv):
    def __init__(self, name: str, report: Path, **settings: Any) -> None:
        super().__init__(name, report, **settings)
        self.settings["allowed_roots"] = [str(self.temp / "runs")]
        self.write_settings()
        self.validator()

    def build_path(self, run_id: str = RUN) -> Path:
        return self.temp / "runs" / run_id / "staging.duckdb"

    def result_path(self, run_id: str = RUN) -> Path:
        return self.temp / "runs" / run_id / "quality_silver" / "result.json"

    def validator(self, during: bytes | None = None, report: Callable[[bytes], dict[str, Any] | None] = reported) -> None:
        """Make the fake daemon's validation jobs (``dbt test`` or the quality validator) write the result file of a real validator. The job reads the
        build file, which holds the bytes ``during`` while it runs when they are given, and ``report`` turns what it read into the result (None writes no
        file)."""

        def script(container: dict[str, Any]) -> tuple[float, int]:
            command = container["Config"].get("Cmd") or [""]
            if command[0] != "test" and "--database" not in command:
                return 0.2, 0
            build, result = self.build_path(), self.result_path()
            if during is not None:
                build.write_bytes(during)
            body = report(build.read_bytes())
            result.parent.mkdir(parents=True, exist_ok=True)
            if body is None:
                result.unlink(missing_ok=True)
            else:
                result.write_text(json.dumps(body))
            return 0.2, 0

        self.daemon.script = script

    def plays_dbt(self, resources: list[str]) -> None:
        """Make the fake daemon's dbt jobs leave the native artifacts a real dbt run leaves in the run folder's ``target`` folder: a manifest and run results
        that name the job's attempt and one new invocation. Each resource ID (``model.``, ``seed.`` or ``source.`` and a name) is in the manifest as its
        kind; a model or a seed also has one successful result, a source has none. Only a container that carries the attempt variable plays dbt; every
        other container keeps the script it had."""
        other = self.daemon.script

        def script(container: dict[str, Any]) -> tuple[float, int] | None:
            entries = dict(item.partition("=")[::2] for item in container["Config"].get("Env") or [])
            attempt = entries.get(dbt_lineage.ATTEMPT_ENV)
            if attempt is None:
                return other(container)
            case = next(Path(mount["Source"]) for mount in container["Mounts"] if mount["Destination"] == CASE_CONTAINER)
            target = case / "target"
            target.mkdir(exist_ok=True)
            metadata = {"invocation_id": str(uuid.uuid4()), "env": {"HAI_ATTEMPT_ID": attempt}}
            described = {
                name: {"resource_type": name.partition(".")[0], "config": {"materialized": "table"}, "depends_on": {"nodes": []}} for name in resources
            }
            sources = {name: node for name, node in described.items() if node["resource_type"] == "source"}
            nodes = {name: node for name, node in described.items() if name not in sources}
            (target / "manifest.json").write_text(json.dumps({"metadata": metadata, "nodes": nodes, "sources": sources}))
            (target / "run_results.json").write_text(
                json.dumps({"metadata": metadata, "results": [{"unique_id": name, "status": "success"} for name in nodes]})
            )
            return 0.2, 0

        self.daemon.script = script

    def records(self, run_id: str) -> Path:
        return self.temp / "records" / run_id / "run.json"

    def inventory(self) -> dict[str, Any]:
        document = copy.deepcopy(load_templates(SHIPPED))
        templates = document["templates"]
        shipped_rows = {name: templates[name]["modes"]["fixture"] for name in ("silver_validate", "gold_build")}
        templates["acquire_run"]["modes"]["fixture"]["control"].pop("params")
        for name in ("run_record", "final_gate_accepted", "final_gate_rejected", "release_accepted"):
            templates[name]["modes"]["fixture"]["control"]["params"]["record_file"] = str(self.temp / "records" / "{run_id}" / "run.json")
        templates["silver_build"]["modes"]["fixture"] = self.fixture_row(["build"])
        templates["gold_build"]["modes"]["fixture"] = self.fixture_row(["run"])
        templates["silver_validate"]["modes"]["fixture"] = self.fixture_row(["test"])
        for name in ("silver_validate", "gold_build"):
            templates[name]["modes"]["fixture"]["launcher"].update(
                {key: shipped_rows[name]["launcher"][key] for key in CHECKPOINT_KEYS if key in shipped_rows[name]["launcher"]}
            )
        return document

    def checkout_copy(self) -> Path:
        checkout = self.temp / "checkout"
        (checkout / "services" / "analytics").mkdir(parents=True)
        shutil.copy2(ROOT / "docker-compose.yaml", checkout / "docker-compose.yaml")
        shutil.copy2(ROOT / "services" / "analytics" / "resources.sql", checkout / "services" / "analytics" / "resources.sql")
        for contracts in ("great_expectations", "lakehouse"):
            shutil.copytree(ROOT / "data_contracts" / contracts, checkout / "data_contracts" / contracts)
        (checkout / "data" / "analytics" / "dbt" / "dbt_packages").mkdir(parents=True)
        (checkout / "data" / "orchestration" / "fixture_seed" / "project").mkdir(parents=True)
        (checkout / "data" / "orchestration" / "fixture_seed" / "project" / "dbt_project.yml").write_text("name: seed\n")
        (checkout / "data" / "orchestration" / "fixture_seed" / FIXTURE_BRONZE_NAME).write_bytes(FIXTURE_BRONZE_BYTES)
        return checkout


CASES: dict[str, tuple[Callable[[Env], dict[str, bool]], dict[str, Any]]] = {}


def case(name: str, **settings: Any) -> Callable[[Callable[[Env], dict[str, bool]]], Callable[[Env], dict[str, bool]]]:

    def register(function: Callable[[Env], dict[str, bool]]) -> Callable[[Env], dict[str, bool]]:
        CASES[name] = (function, settings)
        return function

    return register


def step(broker: Broker, template: str, attempt: int = 1, run_id: str = RUN) -> Status:
    return broker.status(broker.submit(SubmissionRequest(template, run_id, "fixture", attempt, "lineage_fixture", str(template), "manual__" + str(run_id))))


def lock_of(env: Env, run_id: str = RUN) -> str | None:
    run = env.state()["runs"].get(run_id)
    return None if run is None else str(run["state"])


def record_of(env: Env, run_id: str = RUN) -> dict[str, Any]:
    path = env.records(run_id)
    loaded: dict[str, Any] = json.loads(path.read_text()) if path.is_file() else {}
    return loaded


def finished(env: Env, template: str, attempt: int = 1, run_id: str = RUN) -> dict[str, Any]:
    return next(line for line in ledger_lines(env, run_id) if line["submission_id"] == f"{run_id}:{template}:fixture:{attempt}" and line["event"] == "finished")


def prepare_finalizing(env: Env, broker: Broker, run_id: str = RUN) -> Path:
    step(broker, "acquire_run", 1, run_id)
    build = env.temp / "runs" / run_id / "staging.duckdb"
    build.write_text("build\n")
    (env.temp / "runs" / run_id / "staging.duckdb.wal").write_text("wal\n")
    step(broker, "run_record", 1, run_id)
    step(broker, "final_gate_accepted", 1, run_id)
    return build


@case("control_rows_schema")
def control_rows_schema(env: Env) -> dict[str, bool]:
    shipped = load_templates(SHIPPED)
    results = {
        "the shipped inventory loads and holds six control templates": sum(
            1 for t in shipped["templates"].values() if any("control" in m for m in t["modes"].values())
        )
        == 6
    }

    def mutated(change: Callable[[dict[str, Any]], None], fragment: str) -> bool:
        document = copy.deepcopy(shipped)
        change(document["templates"])
        path = env.work / "mutant.json"
        path.write_text(json.dumps(document))
        error = refusal_of(lambda: load_templates(path))
        return isinstance(error, BrokerError) and fragment in str(error)

    def row(templates: dict[str, Any], name: str = "run_record") -> dict[str, Any]:
        found: dict[str, Any] = templates[name]["modes"]["fixture"]
        return found

    results["an unknown action is refused"] = mutated(lambda t: row(t)["control"].update(action="launch"), "is not one of")
    results["an extra parameter is refused"] = mutated(lambda t: row(t)["control"]["params"].update(image="x"), "Additional properties")
    results["a missing parameter is refused"] = mutated(lambda t: row(t)["control"].update(params={}), "missing ['record_file']")
    results["a gate row without its expected states is refused"] = mutated(
        lambda t: row(t, "final_gate_accepted")["control"]["params"].pop("expected_states"), "missing ['expected_states']"
    )
    results["an expected state other than success or skipped is refused"] = mutated(
        lambda t: row(t, "final_gate_rejected")["control"]["params"]["expected_states"].update(gold_build="failed"), "is not one of"
    )
    results["a launcher beside the control is refused"] = mutated(
        lambda t: row(t).update(launcher={"template": "fixture_dbt", "args": [], "params": {}}), "is not valid under any of the given schemas"
    )
    results["a no-op flag with a control is refused"] = mutated(
        lambda t: row(t).update(no_op=True, reason="not a real reason"), "is not valid under any of the given schemas"
    )
    results["a class other than offline is refused"] = mutated(lambda t: row(t).update({"class": "lakehouse_write"}), "the broker runs only the offline class")
    results["a placeholder outside the five is refused"] = mutated(lambda t: row(t)["control"]["params"].update(record_file="{home}/x.json"), "does not match")
    return results


@case("control_acquire")
def control_acquire(env: Env) -> dict[str, bool]:
    broker = env.new_broker()
    broker.start()
    request = SubmissionRequest("acquire_run", RUN, "fixture", 1, "lineage_fixture", "acquire_run", "manual__" + str(RUN))
    first, again = broker.submit(request), broker.submit(request)
    status = broker.status(first)
    second = step(broker, "acquire_run", 1, OTHER)
    refusal = finished(env, "acquire_run", 1, OTHER)
    return {
        "the first acquire succeeded and the lock is held by the broker": status.state == State.SUCCEEDED
        and lock_of(env) == "held"
        and env.state()["runs"][RUN]["holder"] == "broker",
        "the run folder exists": (env.temp / "runs" / RUN).is_dir(),
        "a repeated request returns the same handle with one intent and one launcher acquire": first == again
        and events(env, first.submission_id) == ["intent", "finished"]
        and wire(broker).methods.count("acquire") == 2,
        "a second run is refused by the launcher with exit code 125": second.state == State.FAILED
        and second.exit_code == 125
        and "launcher refused the lock" in str(refusal.get("reason")),
        "the refused run holds no lock and no folder": lock_of(env, OTHER) is None and not (env.temp / "runs" / OTHER).exists(),
        "the held run is unchanged": lock_of(env) == "held",
    }


@case("control_acquire_seed")
def control_acquire_seed(env: Env) -> dict[str, bool]:
    seed = env.work / "seed"
    (seed / "project").mkdir(parents=True)
    (seed / "project" / "model.sql").write_text("select 1\n")
    document = env.inventory()
    control = document["templates"]["acquire_run"]["modes"]["fixture"]["control"]
    control["params"] = {"seed": str(seed), "folders": ["out_a", "out_b"]}
    env.write_inventory(document)
    broker = env.new_broker()
    broker.start()
    first = step(broker, "acquire_run", 1)
    folder = env.temp / "runs" / RUN
    (folder / "project" / "model.sql").write_text("changed\n")
    again = step(broker, "acquire_run", 2)
    link_seed = env.work / "seed_with_link"
    link_seed.mkdir()
    os.symlink(seed / "project" / "model.sql", link_seed / "model.sql")
    outside_seed = Path(tempfile.mkdtemp(prefix="a2_outside_seed_"))
    (outside_seed / "note.txt").write_text("outside\n")
    outcomes = {}
    refused_seeds = (
        ("a seed with a link", link_seed, "holds a link"),
        ("a seed outside the data folder", outside_seed, "inside the data folder"),
        ("a seed that is a file", seed / "project" / "model.sql", "inside the data folder"),
    )
    broker.close()
    for number, (label, path, expected) in enumerate(refused_seeds):
        control["params"] = {"seed": str(path)}
        env.write_inventory(document)
        other = env.new_broker()
        other.start()
        run_id = f"2026101{number}T090000Z-0c1c2c3c"
        status = step(other, "acquire_run", 1, run_id)
        outcomes[label] = status.state == State.FAILED and expected in str(finished(env, "acquire_run", 1, run_id).get("reason"))
        other.close()
    shutil.rmtree(outside_seed, ignore_errors=True)
    return {
        "the first acquire copied the seed and made the folders": first.state == State.SUCCEEDED
        and (folder / ".seeded").is_file()
        and (folder / "out_a").is_dir()
        and (folder / "out_b").is_dir(),
        "a later attempt does not copy the seed again": again.state == State.SUCCEEDED and (folder / "project" / "model.sql").read_text() == "changed\n",
        **{f"{label} is refused": ok for label, ok in outcomes.items()},
    }


@case("control_record")
def control_record(env: Env) -> dict[str, bool]:
    broker = env.new_broker()
    broker.start()
    early = step(broker, "run_record", 1)
    step(broker, "acquire_run", 1)
    ok = step(broker, "run_record", 2)
    first = record_of(env)
    lock_state_after_first = lock_of(env)
    again = step(broker, "run_record", 3)
    second = record_of(env)
    env.client().call("mark", {"state": "failed_retained"}, broker.principal(RUN))
    late = step(broker, "run_record", 4)
    return {
        "a record without the lock is refused": early.state == State.FAILED and "lock is not held" in str(finished(env, "run_record", 1).get("reason", "")),
        "the first record names the run and finalizing, and the lock is finalizing": ok.state == State.SUCCEEDED
        and first.get("run_id") == RUN
        and first.get("state") == "finalizing"
        and lock_state_after_first == "finalizing",
        "the record lists the run's submissions": any(item["template"] == "acquire_run" for item in first.get("submissions", [])),
        "a repeat rewrites the record and the run stays finalizing": again.state == State.SUCCEEDED and second["written_at"] >= first["written_at"],
        "a retained run is not recorded": late.state == State.FAILED and late.exit_code == 125,
    }


@case("control_gate")
def control_gate(env: Env) -> dict[str, bool]:
    env.daemon.script = lambda container: (0.2, 7 if (container["Config"].get("Cmd") or [""])[0] == "run" else 0)
    broker = env.new_broker()
    broker.start()
    step(broker, "acquire_run", 1)
    early = step(broker, "final_gate_accepted", 1)
    work = broker.submit(SubmissionRequest("gold_build", RUN, "fixture", 1, "lineage_fixture", "gold_build", "manual__" + str(RUN)))
    env.wait_state(broker, work, (State.FAILED, State.SUCCEEDED))
    rejected = step(broker, "final_gate_rejected", 1)
    after_rejected = record_of(env)
    lock_after_rejection = lock_of(env)
    held_only = step(broker, "final_gate_accepted", 2)
    step(broker, "run_record", 1)
    accepted = step(broker, "final_gate_accepted", 3)
    final = record_of(env)
    no_lock = step(broker, "final_gate_rejected", 1, OTHER)
    unlocked = record_of(env, OTHER)
    return {
        "an accepted result before the record step is refused": early.state == State.FAILED and early.exit_code == 125,
        "an accepted result while the run is only held is refused even with a record": held_only.state == State.FAILED
        and "needs a finalizing run" in str(finished(env, "final_gate_accepted", 2).get("reason")),
        "a rejected result is recorded while the run is held": rejected.state == State.SUCCEEDED
        and after_rejected["gate"]["result"] == "rejected"
        and lock_after_rejection == "held",
        "the rejected result names the failed submission": any("gold_build" in item for item in after_rejected["gate"]["failed"]),
        "an accepted result is recorded after the record step": accepted.state == State.SUCCEEDED
        and final["gate"]["result"] == "accepted"
        and final["state"] == "finalizing",
        "a rejection without a lock creates the record with the reason": no_lock.state == State.SUCCEEDED
        and unlocked["gate"]["reason"] == "lock not acquired"
        and unlocked["state"] == "failed"
        and lock_of(env, OTHER) is None,
    }


def gate_snapshot(expected: dict[str, str], **observed: str) -> dict[str, Any]:
    return {"dag_run_id": "manual__run", "dag_run_state": "running", "tasks": {**expected, **observed}}


@case("control_gate_task_states")
def control_gate_task_states(env: Env) -> dict[str, bool]:
    expected = load_templates(SHIPPED)["templates"]["final_gate_accepted"]["modes"]["fixture"]["control"]["params"]["expected_states"]
    asked: list[tuple[str, str]] = []
    reading: dict[str, Any] = {}

    def snapshot(run_id: str, mode: str) -> dict[str, Any]:
        asked.append((run_id, mode))
        found: dict[str, Any] = reading["value"]
        return found

    broker = env.new_broker()
    broker.start()
    broker.dag_snapshot = snapshot
    step(broker, "acquire_run", 1)
    step(broker, "run_record", 1)
    reading["value"] = gate_snapshot(expected)
    matched = step(broker, "final_gate_accepted", 1)
    accepted = record_of(env)["gate"]
    failing = {"gold_validate": "failed", "cross_layer_checks": "upstream_failed", "verify_bronze": "upstream_failed", "run_record": "upstream_failed"}
    reading["value"] = gate_snapshot(expected, **failing)
    rejected_step = step(broker, "final_gate_rejected", 1)
    rejected = record_of(env)
    reading["value"] = gate_snapshot(expected, gold_build="failed")
    contradicted = step(broker, "final_gate_accepted", 2)
    contradicted_record = record_of(env)["gate"]
    reading["value"] = {"unavailable": "2 DAG runs of transform_dag name the run, not one"}
    unavailable_accepted = step(broker, "final_gate_accepted", 3)
    after_unavailable_accepted = record_of(env)["gate"]
    unavailable_rejected = step(broker, "final_gate_rejected", 2)
    after_unavailable_rejected = record_of(env)["gate"]
    states = rejected["gate"].get("task_states", {})
    by_task = {item["task"]: item for item in states.get("predecessors", [])}
    return {
        "every gate request asked for the snapshot of its run and mode": asked == [(RUN, "fixture")] * 5,
        "a matching snapshot is recorded in full with the DAG run and no failed task": matched.state == State.SUCCEEDED
        and accepted["result"] == "accepted"
        and accepted["task_states"]["status"] == "read"
        and accepted["task_states"].get("dag_run_id") == "manual__run"
        and accepted["task_states"].get("failed_tasks") == []
        and sorted(item["task"] for item in accepted["task_states"].get("predecessors", [])) == sorted(expected)
        and all(item["observed"] == item["expected"] for item in accepted["task_states"].get("predecessors", [])),
        "a task that failed before any submission is named with its expected and observed state": rejected_step.state == State.SUCCEEDED
        and rejected["gate"]["result"] == "rejected"
        and by_task.get("gold_validate") == {"task": "gold_validate", "expected": "success", "observed": "failed"}
        and by_task.get("cross_layer_checks", {}).get("observed") == "upstream_failed"
        and sorted(states.get("failed_tasks", [])) == sorted(failing)
        and not any("gold_validate" in item["submission_id"] for item in rejected["submissions"]),
        "an accepted request against a failed predecessor is refused and the record holds the rejection": contradicted.state == State.FAILED
        and contradicted.exit_code == 125
        and contradicted_record["result"] == "rejected"
        and contradicted_record["task_states"].get("failed_tasks") == ["gold_build"],
        "an unavailable lookup is recorded as unavailable with its reason and no predecessors": after_unavailable_rejected["task_states"]
        == {"status": "unavailable", "reason": "2 DAG runs of transform_dag name the run, not one"},
        "the rejection stands when the lookup is unavailable": unavailable_rejected.state == State.SUCCEEDED
        and after_unavailable_rejected["result"] == "rejected",
        "an acceptance with an unavailable lookup is recorded with the lookup as unavailable, not as a complete snapshot": unavailable_accepted.state
        == State.SUCCEEDED
        and after_unavailable_accepted["result"] == "accepted"
        and after_unavailable_accepted["task_states"].get("status") == "unavailable"
        and "predecessors" not in after_unavailable_accepted["task_states"],
    }


@case("control_release_accepted")
def control_release_accepted(env: Env) -> dict[str, bool]:
    broker = env.new_broker()
    broker.start()
    build = prepare_finalizing(env, broker)
    wal = build.with_name("staging.duckdb.wal")
    before = build.exists() and wal.exists() and lock_of(env) == "finalizing"
    released = step(broker, "release_accepted", 1)
    again = step(broker, "release_accepted", 2)
    outside = env.work / "outside.txt"
    outside.write_text("not a build file\n")
    document = env.inventory()
    document["templates"]["release_accepted"]["modes"]["fixture"]["control"]["params"]["build_files"] = [str(outside)]
    env.write_inventory(document)
    other = env.new_broker()
    other.close()
    broker.close()
    other.start()
    outside_build = prepare_finalizing(env, other, OTHER)
    escaped = step(other, "release_accepted", 1, OTHER)
    return {
        "the run was finalizing with its build files before the release": before,
        "the release succeeded: both build files are deleted and the lock is released": released.state == State.SUCCEEDED
        and not build.exists()
        and not wal.exists()
        and lock_of(env) == "released",
        "a repeat of the release succeeds": again.state == State.SUCCEEDED and lock_of(env) == "released",
        "the finished line names what was deleted": sorted(finished(env, "release_accepted", 1)["deleted"]) == ["staging.duckdb", "staging.duckdb.wal"],
        "a build file outside the run folder is refused and nothing is released": escaped.state == State.FAILED
        and escaped.exit_code == 125
        and lock_of(env, OTHER) == "finalizing"
        and outside.exists()
        and outside_build.exists(),
    }


@case("control_release_needs_gate")
def control_release_needs_gate(env: Env) -> dict[str, bool]:
    broker = env.new_broker()
    broker.start()
    step(broker, "acquire_run", 1)
    build = env.temp / "runs" / RUN / "staging.duckdb"
    build.write_text("build\n")
    step(broker, "run_record", 1)
    without = step(broker, "release_accepted", 1)
    step(broker, "final_gate_rejected", 1)
    after_rejection = step(broker, "release_accepted", 2)
    return {
        "a release with no recorded gate result is refused": without.state == State.FAILED and without.exit_code == 125,
        "a release after a rejected result is refused": after_rejection.state == State.FAILED
        and "not accepted" in str(finished(env, "release_accepted", 2).get("reason")),
        "the build file is kept and the lock stays finalizing": build.exists() and lock_of(env) == "finalizing",
    }


@case("control_release_rejected")
def control_release_rejected(env: Env) -> dict[str, bool]:
    broker = env.new_broker()
    broker.start()
    step(broker, "acquire_run", 1)
    build = env.temp / "runs" / RUN / "staging.duckdb"
    build.write_text("build\n")
    first = step(broker, "release_rejected", 1)
    again = step(broker, "release_rejected", 2)
    release = refusal_of(lambda: broker.release_run(RUN, str(env.records(RUN))))
    nothing = step(broker, "release_rejected", 1, OTHER)
    return {
        "the lock is failed_retained and the build file is kept": first.state == State.SUCCEEDED and lock_of(env) == "failed_retained" and build.exists(),
        "a repeat succeeds and changes nothing": again.state == State.SUCCEEDED and lock_of(env) == "failed_retained",
        "the broker cannot release a retained run": release is not None and "failed_retained" in str(release),
        "a run that never held the lock succeeds without a lock": nothing.state == State.SUCCEEDED and lock_of(env, OTHER) is None,
    }


@case("control_release_stops_open")
def control_release_stops_open(env: Env) -> dict[str, bool]:
    env.hold_jobs()
    broker = env.new_broker()
    broker.start()
    step(broker, "acquire_run", 1)
    job = broker.submit(SubmissionRequest("silver_build", RUN, "fixture", 1, "lineage_fixture", "silver_build", "manual__" + str(RUN)))
    env.wait_state(broker, job, (State.RUNNING,))
    stopped = step(broker, "release_rejected", 1)
    ended = soft_wait(lambda: state_of(broker, job) in (State.CANCELLED, State.FAILED), 10)
    return {
        "the control stopped one attempt": stopped.state == State.SUCCEEDED and len(finished(env, "release_rejected")["stopped_attempts"]) == 1,
        "the job's submission ended cancelled": bool(ended) and state_of(broker, job) == State.CANCELLED,
        "the lock is failed_retained and no container of the run remains": lock_of(env) == "failed_retained" and not env.labelled(),
    }


@case("control_release_stops_forwarded_after_row_drift")
def control_release_stops_forwarded_after_row_drift(env: Env) -> dict[str, bool]:
    env.hold_jobs()
    request = SubmissionRequest("silver_build", RUN, "fixture", 1, "lineage_fixture", "silver_build", "manual__" + str(RUN))
    accepted, cache_missing, submit_code = lose_submit_reply(env, request, "f" * 64)
    drift_inventory(env, map_to_control)
    broker = env.new_broker()
    broker.start()
    stopped = step(broker, "release_rejected", 1)
    ended = env.attempt(accepted["id"])
    stop_attempt(env, accepted["id"])
    return {
        "the submit reply was lost and the broker has no forward cache": submit_code == 503 and cache_missing,
        "the release stopped the forwarded attempt": stopped.state == State.SUCCEEDED
        and finished(env, "release_rejected")["stopped_attempts"] == [accepted["id"]],
        "the launcher's attempt received the cancel and ended": ended.get("cancel_intent") is not None and ended["phase"] == "ended",
        "the lock is failed_retained and no container of the run remains": lock_of(env) == "failed_retained" and not env.labelled(),
    }


def run_job(env: Env, broker: Broker, template: str, attempt: int) -> Status:
    handle = broker.submit(SubmissionRequest(template, RUN, "fixture", attempt, "lineage_fixture", str(template), "manual__" + str(RUN)))
    return env.wait_state(broker, handle, (State.SUCCEEDED, State.FAILED, State.CANCELLED, State.FAILED_CLOSED))


@case("build_file_checkpoint")
def build_file_checkpoint(env: Env) -> dict[str, bool]:
    broker = env.new_broker()
    broker.start()
    step(broker, "acquire_run", 1)
    build = env.temp / "runs" / RUN / "staging.duckdb"
    build.write_text("validated build\n")
    unvalidated = run_job(env, broker, "gold_build", 1)
    validated = run_job(env, broker, "silver_validate", 1)
    recorded = finished(env, "silver_validate").get("build_file") or {}
    unchanged = run_job(env, broker, "gold_build", 2)
    build.write_text("changed after validation\n")
    changed = run_job(env, broker, "gold_build", 3)
    created = [op["name"] for op in env.state()["operations"].values() if op["kind"] == "create" and RUN in op["name"]]
    env.daemon.script = lambda container: (0.2, 7 if (container["Config"].get("Cmd") or [""])[0] == "test" else 0)
    failed = run_job(env, broker, "silver_validate", 2)
    return {
        "gold_build before any validation is refused with exit code 125": unvalidated.state == State.FAILED
        and unvalidated.exit_code == 125
        and "recorded no hash" in str(finished(env, "gold_build", 1).get("reason")),
        "silver_validate records the SHA-256, the run and the attempt of the file": validated.state == State.SUCCEEDED
        and recorded.get("sha256") == hashlib.sha256(b"validated build\n").hexdigest()
        and recorded.get("run_id") == RUN
        and bool(recorded.get("attempt_id")),
        "gold_build runs while the file is the validated one": unchanged.state == State.SUCCEEDED,
        "a changed build file ends gold_build with exit code 125 and no container": changed.state == State.FAILED
        and changed.exit_code == 125
        and "changed after silver_validate validated it" in str(finished(env, "gold_build", 3).get("reason"))
        and events(env, f"{RUN}:gold_build:fixture:3") == ["intent", "finished"]
        and len(created) == 2,
        "a validation that fails records no hash": failed.state == State.FAILED and "build_file" not in finished(env, "silver_validate", 2),
    }


def validate_until_exit(env: Env, broker: Broker, attempt: int) -> tuple[Handle, SubmissionRequest]:
    request = SubmissionRequest("silver_validate", RUN, "fixture", attempt, "lineage_fixture", "silver_validate", "manual__" + str(RUN))
    handle = broker.submit(request)
    env.wait_ended(str((broker.read_forward(request) or {})["attempt_id"]))
    return handle, request


def intent_inputs(env: Env, template: str, attempt: int = 1) -> dict[str, Any]:
    lines = [line for line in ledger_lines(env) if line["submission_id"] == f"{RUN}:{template}:fixture:{attempt}" and line["event"] == "intent"]
    inputs: dict[str, Any] = lines[0]["inputs"] if lines else {}
    return inputs


@case("build_file_changed_before_completion")
def build_file_changed_before_completion(env: Env) -> dict[str, bool]:
    broker = env.new_broker()
    broker.start()
    step(broker, "acquire_run", 1)
    build = env.temp / "runs" / RUN / "staging.duckdb"
    build.write_text("validated build\n")
    digest = hashlib.sha256(b"validated build\n").hexdigest()
    control_handle, _ = validate_until_exit(env, broker, 1)
    control = env.wait_state(broker, control_handle, (State.SUCCEEDED, State.FAILED, State.CANCELLED))
    gold_after_control = run_job(env, broker, "gold_build", 1)
    handle, _ = validate_until_exit(env, broker, 2)
    build.write_text("changed after the validator exited\n")
    changed = env.wait_state(broker, handle, (State.SUCCEEDED, State.FAILED, State.CANCELLED))
    recorded = finished(env, "silver_validate", 2)
    gold_after_change = run_job(env, broker, "gold_build", 2)
    return {
        "the control: a file that stays the validated one is recorded with its SHA-256": control.state == State.SUCCEEDED
        and (finished(env, "silver_validate", 1).get("build_file") or {}).get("sha256") == digest,
        "the control: gold_build runs after it": gold_after_control.state == State.SUCCEEDED,
        "the intent line of the validation carries the build file and the validator's result file": intent_inputs(env, "silver_validate", 2)
        == {"records_build_file": str(build), "build_file_result": str(env.result_path())},
        "a file that changed after the validator exited is not recorded as validated": changed.state == State.FAILED
        and changed.exit_code == 1
        and "build_file" not in recorded
        and "changed" in str(recorded.get("reason")),
        "gold_build is then refused with exit code 125 and no new container": gold_after_change.state == State.FAILED
        and gold_after_change.exit_code == 125
        and events(env, f"{RUN}:gold_build:fixture:2") == ["intent", "finished"]
        and len(env.creates()) == 3,
    }


@case("build_file_replaced_during_validation")
def build_file_replaced_during_validation(env: Env) -> dict[str, bool]:
    broker = env.new_broker()
    broker.start()
    step(broker, "acquire_run", 1)
    build = env.build_path()
    original, replaced = b"validated build\n", b"replaced while the validator ran\n"
    build.write_bytes(original)
    env.validator(during=replaced)
    handle, _ = validate_until_exit(env, broker, 1)
    build.write_bytes(original)
    returned = env.wait_state(broker, handle, (State.SUCCEEDED, State.FAILED, State.CANCELLED))
    ended = finished(env, "silver_validate", 1)
    gold_after_return = run_job(env, broker, "gold_build", 1)
    created = len(env.creates())
    env.validator()
    stable = run_job(env, broker, "silver_validate", 2)
    gold_after_stable = run_job(env, broker, "gold_build", 2)
    env.validator(during=replaced)
    kept = run_job(env, broker, "silver_validate", 3)
    gold_after_kept = run_job(env, broker, "gold_build", 3)
    return {
        "a file replaced while the validator read it and restored before the end is not recorded as validated": returned.state == State.FAILED
        and returned.exit_code == 1
        and "build_file" not in ended
        and "validator read" in str(ended.get("reason")),
        "gold_build is then refused with exit code 125 and no new container": gold_after_return.state == State.FAILED
        and gold_after_return.exit_code == 125
        and "recorded no hash" in str(finished(env, "gold_build", 1).get("reason"))
        and events(env, f"{RUN}:gold_build:fixture:1") == ["intent", "finished"]
        and created == 1,
        "the control: a stable file is recorded with the SHA-256 the validator reported": stable.state == State.SUCCEEDED
        and (finished(env, "silver_validate", 2).get("build_file") or {}).get("sha256") == hashlib.sha256(original).hexdigest(),
        "the control: gold_build runs after it": gold_after_stable.state == State.SUCCEEDED,
        "a file replaced before the validator read it and kept is recorded as the bytes the validator read": kept.state == State.SUCCEEDED
        and (finished(env, "silver_validate", 3).get("build_file") or {}).get("sha256") == hashlib.sha256(replaced).hexdigest()
        and gold_after_kept.state == State.SUCCEEDED,
    }


@case("build_file_result_unusable")
def build_file_result_unusable(env: Env) -> dict[str, bool]:
    broker = env.new_broker()
    broker.start()
    step(broker, "acquire_run", 1)
    env.build_path().write_bytes(b"validated build\n")
    reports: list[Callable[[bytes], dict[str, Any] | None]] = [
        lambda data: None,
        lambda data: {"counts": {}},
        lambda data: {"build_file_sha256": "not a digest"},
    ]
    outcomes = []
    for attempt, report in enumerate(reports, start=1):
        env.validator(report=report)
        outcomes.append((run_job(env, broker, "silver_validate", attempt), finished(env, "silver_validate", attempt)))
    env.validator()
    control = run_job(env, broker, "silver_validate", 4)
    return {
        "a job that wrote no result file ends with exit code 1 and records nothing": outcomes[0][0].exit_code == 1
        and "build_file" not in outcomes[0][1]
        and "cannot be read" in str(outcomes[0][1].get("reason")),
        "a result without a SHA-256 ends with exit code 1 and records nothing": outcomes[1][0].exit_code == 1
        and "build_file" not in outcomes[1][1]
        and "reports no SHA-256" in str(outcomes[1][1].get("reason")),
        "a result with a SHA-256 that is not a digest ends with exit code 1 and records nothing": outcomes[2][0].exit_code == 1
        and "build_file" not in outcomes[2][1]
        and "reports no SHA-256" in str(outcomes[2][1].get("reason")),
        "the control: a usable result is recorded": control.state == State.SUCCEEDED
        and bool((finished(env, "silver_validate", 4).get("build_file") or {}).get("sha256")),
    }


@case("build_file_checkpoint_from_intent")
def build_file_checkpoint_from_intent(env: Env) -> dict[str, bool]:
    broker = env.new_broker()
    broker.start()
    step(broker, "acquire_run", 1)
    build = env.temp / "runs" / RUN / "staging.duckdb"
    build.write_text("validated build\n")
    digest = hashlib.sha256(b"validated build\n").hexdigest()
    validate_until_exit(env, broker, 1)
    env.stop_brokers()

    def stop_recording(document: dict[str, Any]) -> None:
        launcher_row = document["templates"]["silver_validate"]["modes"]["fixture"]["launcher"]
        launcher_row.pop("records_build_file")
        launcher_row.pop("build_file_result")

    drift_inventory(env, stop_recording)
    revived = env.new_broker()
    summary = revived.start()
    completed = finished(env, "silver_validate", 1)
    revived.close()
    crash_code = crash_child(env, "gold_build", "after_intent")
    build.write_text("changed while the gold intent waited\n")

    def stop_requiring(document: dict[str, Any]) -> None:
        document["templates"]["gold_build"]["modes"]["fixture"]["launcher"].pop("requires_build_file")

    drift_inventory(env, stop_requiring)
    again = env.new_broker()
    resumed = again.start()
    gold = SubmissionRequest("gold_build", RUN, "fixture", 1, "lineage_fixture", "gold_build", "manual__" + str(RUN))
    env.wait_state(again, Handle(gold.submission_id, gold.container_name), (State.SUCCEEDED, State.FAILED, State.CANCELLED))
    ended = finished(env, "gold_build", 1)
    return {
        "the validation ended after its row stopped recording the file": f"{RUN}:silver_validate:fixture:1" in summary["updated"]
        and completed["exit_code"] == 0,
        "its completion followed the intent: the SHA-256 of the validated file is recorded": (completed.get("build_file") or {}).get("sha256") == digest,
        "the gold_build intent carries the checkpoint and the validated SHA-256": crash_code == 70
        and intent_inputs(env, "gold_build") == {"requires_build_file": str(build), "validated_by": "silver_validate", "validated_sha256": digest},
        "after its row stopped requiring the file, gold_build is still refused for the changed file": f"{RUN}:gold_build:fixture:1" in resumed["updated"]
        and ended["exit_code"] == 125
        and "changed" in str(ended.get("reason")),
        "only the validation created a container": len(env.creates()) == 1,
    }


@contextlib.contextmanager
def undeletable_entries(folder: Path) -> Iterator[None]:
    """Make the entries of ``folder`` undeletable for one step by removing its write bits; restore its mode after. Works on macOS and Linux (``chflags``
    exists only on macOS). Root ignores folder modes, so a root run refuses instead of passing for the wrong reason."""
    if os.geteuid() == 0:
        raise RuntimeError("folder modes do not stop root from deleting files; run this case as a regular user")
    mode = stat.S_IMODE(folder.stat().st_mode)
    folder.chmod(mode & ~0o222)
    try:
        yield
    finally:
        folder.chmod(mode)


@case("control_release_resume")
def control_release_resume(env: Env) -> dict[str, bool]:
    broker = env.new_broker()
    broker.start()
    build = prepare_finalizing(env, broker)
    with undeletable_entries(build.parent):
        first = step(broker, "release_accepted", 1)
        state_after = lock_of(env)
    second = step(broker, "release_accepted", 2)
    return {
        "the first release failed with exit code 1 and names the permission error": first.state == State.FAILED
        and first.exit_code == 1
        and "PermissionError" in str(finished(env, "release_accepted", 1).get("reason")),
        "the run stayed finalizing with its build file": state_after == "finalizing",
        "the next attempt deleted the file and released the lock": second.state == State.SUCCEEDED and not build.exists() and lock_of(env) == "released",
    }


def crash_child(env: Env, template: str, point: str, run_id: str = RUN) -> int:
    result = env.child("submit-once", "--template", template, "--run", run_id, "--mode", "fixture", "--attempt", "1", "--crash-at", point)
    return int(result.returncode)


@case("control_crash_after_delete")
def control_crash_after_delete(env: Env) -> dict[str, bool]:
    broker = env.new_broker()
    broker.start()
    build = prepare_finalizing(env, broker)
    broker.close()
    code = crash_child(env, "release_accepted", "after_delete")
    midway = {"build": build.exists(), "lock": lock_of(env), "events": events(env, f"{RUN}:release_accepted:fixture:1")}
    revived = env.new_broker()
    summary = revived.start()
    return {
        "the child died at the seam with exit code 70": code == 70,
        "between the steps the file was gone, the lock finalizing and the ledger held only the intent": midway
        == {"build": False, "lock": "finalizing", "events": ["intent"]},
        "the reconciliation finished the submission": f"{RUN}:release_accepted:fixture:1" in summary["updated"],
        "the lock is released and the ledger ends with finished": lock_of(env) == "released"
        and events(env, f"{RUN}:release_accepted:fixture:1") == ["intent", "finished"],
    }


@case("control_crash_after_record")
def control_crash_after_record(env: Env) -> dict[str, bool]:
    broker = env.new_broker()
    broker.start()
    step(broker, "acquire_run", 1)
    broker.close()
    code = crash_child(env, "run_record", "after_record")
    midway = {"record": env.records(RUN).is_file(), "lock": lock_of(env)}
    revived = env.new_broker()
    summary = revived.start()
    return {
        "the child died at the seam with exit code 70": code == 70,
        "between the steps the record existed and the lock was still held": midway == {"record": True, "lock": "held"},
        "the reconciliation finished the submission and the run is finalizing": f"{RUN}:run_record:fixture:1" in summary["updated"]
        and lock_of(env) == "finalizing",
    }


@case("control_crash_after_finalize")
def control_crash_after_finalize(env: Env) -> dict[str, bool]:
    broker = env.new_broker()
    broker.start()
    step(broker, "acquire_run", 1)
    broker.close()
    code = crash_child(env, "run_record", "after_finalize")
    midway = {"lock": lock_of(env), "events": events(env, f"{RUN}:run_record:fixture:1")}
    revived = env.new_broker()
    summary = revived.start()
    return {
        "the child died at the seam with exit code 70": code == 70,
        "between the steps the lock was finalizing and the ledger held only the intent": midway == {"lock": "finalizing", "events": ["intent"]},
        "the reconciliation finished the submission and the run is still finalizing": f"{RUN}:run_record:fixture:1" in summary["updated"]
        and lock_of(env) == "finalizing"
        and events(env, f"{RUN}:run_record:fixture:1") == ["intent", "finished"]
        and finished(env, "run_record").get("control") == "record"
        and finished(env, "run_record")["exit_code"] == 0,
    }


@case("control_idempotent_retry")
def control_idempotent_retry(env: Env) -> dict[str, bool]:
    broker = env.new_broker()
    broker.start()
    request = SubmissionRequest("acquire_run", RUN, "fixture", 1, "lineage_fixture", "acquire_run", "manual__" + str(RUN))
    handles: list[Handle] = [broker.submit(request) for _ in range(3)]
    step(broker, "run_record", 1)
    step(broker, "run_record", 2)
    step(broker, "run_record", 3)
    return {
        "three requests are one handle": len(set(handles)) == 1,
        "the ledger holds one intent and one finished line for the acquire": events(env, handles[0].submission_id) == ["intent", "finished"],
        "three record attempts are three submissions with one run record": len([p for p in env.temp.glob("records/*/run.json")]) == 1
        and all(events(env, f"{RUN}:run_record:fixture:{n}") == ["intent", "finished"] for n in (1, 2, 3)),
        "the run is finalizing once": lock_of(env) == "finalizing",
    }


@case("shipped_rows_render")
def shipped_rows_render(env: Env) -> dict[str, bool]:
    checkout = env.checkout_copy()
    shipped = load_templates(SHIPPED)
    bound = {name: sorted(shipped["templates"][name]["modes"]["fixture"]["launcher"]["dbt_nodes"]) for name in ("silver_build", "gold_build")}
    env.plays_dbt(bound["silver_build"])
    env.write_inventory(shipped)
    broker = Broker(env.paths, load_templates(env.inventory_path), env.client_for_broker(), repo_root=checkout)
    env.brokers.append(broker)
    broker.start()
    acquired = step(broker, "acquire_run", 1)
    folder = env.temp / "runs" / RUN
    names = ("silver_build", "silver_validate", "gold_build", "gold_validate", "cross_layer_checks")
    (folder / "staging.duckdb").write_text("build\n")
    states = {name: run_job(env, broker, name, 1).state for name in names}
    creates = [op["projection"] for op in env.state()["operations"].values() if op["kind"] == "create" and RUN in op["name"]]
    commands = [" ".join(spec["cmd"]) for spec in creates]
    mounts = {mount[1]: mount[2] for spec in creates for mount in spec["mounts"]}
    build_dataset = {"namespace": "file", "name": f"data/analytics/dbt/runs/{RUN}/staging.duckdb"}
    edges = {
        name: json.loads(Path(finished(env, name)["record_path"]).read_text()).get("dbt_lineage", {}).get("models", [])
        if states[name] == State.SUCCEEDED
        else []
        for name in bound
    }
    bronze_intent = next(line for line in ledger_lines(env) if line["submission_id"] == f"{RUN}:silver_build:fixture:1" and line["event"] == "intent")
    results = {
        "acquire_run copied the seed into the run folder and made the quality folders": acquired.state == State.SUCCEEDED
        and (folder / "project" / "dbt_project.yml").is_file()
        and (folder / FIXTURE_BRONZE_NAME).is_file()
        and all((folder / name).is_dir() for name in ("quality_silver", "quality_gold", "quality_cross")),
        "silver_build pinned the seed's bronze file by the hash of its bytes": [pin["snapshot"] for pin in bronze_intent.get("input_pins", [])]
        == [{"kind": "file_sha256", "id": hashlib.sha256(FIXTURE_BRONZE_BYTES).hexdigest()}],
        "every shipped fixture step succeeded on the fake daemon": all(state == State.SUCCEEDED for state in states.values()),
        "each dbt step recorded one edge for every model and seed its row binds (no source), on the build file": all(
            [edge["model"] for edge in edges[name]] == [resource for resource in bound[name] if not resource.startswith("source.")]
            and all(edge["status"] == "success" and edge["outputs"] == [build_dataset] for edge in edges[name])
            for name in bound
        ),
        "five containers were created, each on no network": len(creates) == 5 and all(spec["network"] == "none" for spec in creates),
        "silver_build builds every seed and the silver models with cautious indirect selection": any(
            "build --select resource_type:seed +path:models/silver --indirect-selection cautious" in text for text in commands
        ),
        "gold_build builds the gold models": any("build --select path:models/gold --indirect-selection cautious" in text for text in commands),
        "the dbt jobs mount the run folder writable and the packages read-only": mounts.get("/workspace/out/case") is False
        and mounts.get("/workspace/out/dbt_packages") is True,
        "the quality jobs mount the suites and baselines read-only and their output folders writable": mounts.get("/workspace/suites") is True
        and mounts.get("/workspace/baselines.json") is True
        and mounts.get("/workspace/out") is False,
        "cross_layer_checks validates the silver and gold suites together and reconciles the facts": any(
            "scripts.lakehouse.gold_reconcile" in " ".join(spec["entrypoint"]) and "--suites /workspace/suites/s0 /workspace/suites/s1" in text
            for spec, text in zip(creates, commands, strict=True)
        ),
    }
    return results


ALL_CASES = (
    control_rows_schema,
    control_acquire,
    control_acquire_seed,
    control_record,
    control_gate,
    control_gate_task_states,
    control_release_accepted,
    control_release_needs_gate,
    control_release_rejected,
    control_release_stops_open,
    control_release_stops_forwarded_after_row_drift,
    build_file_checkpoint,
    build_file_changed_before_completion,
    build_file_replaced_during_validation,
    build_file_result_unusable,
    build_file_checkpoint_from_intent,
    control_release_resume,
    control_crash_after_delete,
    control_crash_after_record,
    control_crash_after_finalize,
    control_idempotent_retry,
    shipped_rows_render,
)


def run_case(name: str, report: Path) -> dict[str, Any]:
    function, settings = CASES[name]
    started = time.monotonic()
    try:
        env = Env(name, report, **dict(settings))
    except Exception as exc:
        error = f"the harness did not start: {type(exc).__name__}: {exc}"
        return {"case": name, "passed": False, "checks": {}, "error": error, "leftovers": [], "seconds": round(time.monotonic() - started, 1)}
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
        "feature": "broker run-control rows and the shipped fixture rows, fake Docker, test registries",
        "command": ".venv/bin/python -m scripts.orchestration.run_transform_broker_e2e",
        "started_utc": stamp,
        "head": run_command("git", ["rev-parse", "HEAD"], cwd=ROOT).stdout.strip(),
        "python": sys.version.split()[0],
        "cases": results,
        "passed": sum(1 for r in results if r["status"] == "pass"),
        "total": len(results),
        "limits": ["A fake daemon: no real container runs here; the real scheduler cases are in run_transform_e2e."],
    }
    (report / "report.json").write_text(json.dumps(summary, indent=2) + "\n")
    sys.stdout.write(f"{summary['passed']}/{summary['total']}: {report.relative_to(ROOT)}/report.json\n")
    return 0 if summary["passed"] == summary["total"] else 1


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    if {function.__name__ for function in ALL_CASES} != {function.__name__ for function, _ in CASES.values()}:
        raise SystemExit("ALL_CASES differs from the registered cases")
    parser.add_argument("--case", action="append", default=[], choices=sorted(CASES))
    parser.add_argument("--checks", action="store_true", help="print every check of every case, not only the failed ones")
    args = parser.parse_args()
    return run_cases(args.case, args.checks)


if __name__ == "__main__":
    sys.exit(main())
