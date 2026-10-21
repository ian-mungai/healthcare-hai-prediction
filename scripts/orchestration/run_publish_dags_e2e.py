"""Run shipped silver, gold and maintenance DAGs through Airflow on a test registry and local Iceberg."""

from __future__ import annotations

import argparse
import contextlib
import hashlib
import json
import shutil
import sys
import tempfile
import threading
import time
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from unittest import mock

from orchestration import launcher
from orchestration.pipeline_protocol import read_record
from scripts.orchestration import run_transform_e2e as harness
from scripts.orchestration.airflow_api import AirflowApi, AirflowApiError
from scripts.orchestration.ledger import Ledger
from scripts.orchestration.publish_dag_fixtures import DAGS, PipelineStack, finish_publication, make_seed, verifier_accepting_mismatch, work_steps

ROOT = Path(__file__).resolve().parents[2]
INTERRUPTED = {f"interrupt_{step}": step for step in ("publish_candidates", "reconcile_candidates", "catalog_backup", "commit_publish")}
CASES = (
    "no_silver",
    "silver",
    "selected_silver",
    "bronze_head_changed",
    *INTERRUPTED,
    "gold",
    "maintenance",
    "maintenance_reader",
    "tampered_build",
    "tampered_result",
)
NEEDS = {**{name: ("silver", "gold") for name in (*INTERRUPTED, "maintenance_reader")}, "bronze_head_changed": ("silver",)}
BARRIERS = {
    "publish_candidates": "candidates:after_merge:",
    "reconcile_candidates": "reconcile:after_table:",
    "catalog_backup": "backup:after_write",
    "commit_publish": "commit:after_ff:",
}
MUTANTS = (
    "asset_on_build",
    "verifier_accepts_mismatch",
    "no_interruption",
    "candidates_visible",
    "row_before_fast_forward",
    "maintenance_ignores_registrations",
    "maintenance_ignores_pending",
)
BARRIER_SECONDS = 900
EVIDENCE_SECONDS = 30
MARK_SECONDS = 30
JOIN_SECONDS = 45
SETTLE_SECONDS = 120
RUN_SECONDS = 5400
NOT_RUNNING = ("creating", "created", "exiting", "ended")
SILVER_PUBLISH_STEPS = ("seal_silver", "silver_export", "silver_candidates", "silver_reconcile", "silver_backup", "silver_commit", "record_silver_snapshot")
ANY = {"kind": "any"}
FENCE = "maintenance fence"


class InterruptionControlError(RuntimeError):
    """The harness could not mark the DAG run failed, a phase of the interruption passed its deadline, or the marking worker did not end."""


def bounded_api(stack: PipelineStack, deadline: float) -> AirflowApi:
    """An Airflow client of its own whose every request ends at the absolute deadline; the stack's shared client is left as it is."""
    secret = (stack.state / "metadata" / "jwt_secret").read_text().strip()
    return AirflowApi(stack.supervisor.api_port, secret, timeout=10, deadline=deadline)


def wait_phase(stack: PipelineStack, phase: str, seconds: float, ready: Callable[[AirflowApi], bool], interval: float) -> bool:
    """Poll `ready` with a client bounded by the phase deadline; an Airflow error or a request cut by the deadline is a named control error."""
    deadline = time.monotonic() + seconds
    api = bounded_api(stack, deadline)
    while time.monotonic() < deadline:
        try:
            if ready(api):
                return True
        except AirflowApiError as error:
            raise InterruptionControlError(f"{phase}: {error} (phase deadline {seconds:g} s)") from None
        time.sleep(interval)
    return False


class Marker:
    """Mark a DAG run failed from a worker with an absolute request deadline; the worker is joined within a named deadline."""

    def __init__(self, stack: PipelineStack, dag: str, run_id: str) -> None:
        self.stack, self.dag, self.run_id = stack, dag, run_id
        self.marked = False
        self.error = ""
        self.thread = threading.Thread(target=self.work, name=f"a3-mark-{run_id}", daemon=True)

    def work(self) -> None:
        try:
            bounded_api(self.stack, time.monotonic() + MARK_SECONDS).mark_failed(self.dag, self.run_id)
            self.marked = True
        except (AirflowApiError, OSError) as error:
            self.error = f"{type(error).__name__}: {error}"[:300]

    def finish(self) -> dict[str, Any]:
        self.thread.join(JOIN_SECONDS)
        result = {"marked": self.marked, "error": self.error, "worker_alive": self.thread.is_alive()}
        if result["worker_alive"] or result["error"] or not result["marked"]:
            raise InterruptionControlError(f"marking {self.dag} run {self.run_id} failed: {json.dumps(result)}")
        return result


def step_evidence(stack: PipelineStack, run_id: str, template: str, live: bool) -> dict[str, Any]:
    """The ledger lines of one step's submission and, when live, the launcher's phase of its attempt."""
    found = [lines for lines in Ledger(stack.supervisor.paths.ledger).submissions(run_id).values() if lines[0]["template"] == template]
    lines = found[-1] if found else []
    started = next((line for line in lines if line["event"] == "started"), {})
    evidence: dict[str, Any] = {
        "template": template,
        "events": [line["event"] for line in lines],
        "attempt_id": started.get("launcher_attempt"),
        "container_id": started.get("container_id"),
        "ledger_container_state": started.get("container_state"),
        "exit_code": lines[-1].get("exit_code") if lines else None,
    }
    if live and evidence["attempt_id"]:
        evidence["phase"] = stack.client.call("status", {"what": "attempt", "id": evidence["attempt_id"]}, stack.client.principal_run(run_id)).get("phase")
    return evidence


def progress_lines(stack: PipelineStack, run_id: str) -> list[str]:
    """The points the step's container reached, as the private-checkout barrier wrote them."""
    log = stack.run_folder(run_id) / "publish/a3_progress.log"
    return log.read_text().splitlines() if log.is_file() else []


def untouched() -> dict[str, Any]:
    """The interruption record of a run in which no interruption was made."""
    marker = {"marked": False, "error": "", "worker_alive": False}
    return {"progress_point": "", "at_mark": {}, "marker": marker, "reconciled": [], "open_attempts": [], "progress_after": [], "post": {}}


def interrupt_run(stack: PipelineStack, dag: str, run_id: str, step: str) -> dict[str, Any]:
    """Wait until the step's container reached its barrier point, prove it is running, mark the DAG run failed and reconcile its lock."""
    prefix = BARRIERS[step]
    result = untouched()
    ended = False

    def reached(api: AirflowApi) -> bool:
        nonlocal ended
        result["progress_point"] = next((line for line in progress_lines(stack, run_id) if line.startswith(prefix)), "")
        ended = not result["progress_point"] and api.run(dag, run_id)["state"] in ("success", "failed")
        return bool(result["progress_point"]) or ended

    if not wait_phase(stack, f"{step} reaching its barrier point", BARRIER_SECONDS, reached, 0.5):
        raise InterruptionControlError(f"{step} did not reach its barrier point within {BARRIER_SECONDS} s")
    if ended:
        return result
    deadline = time.monotonic() + EVIDENCE_SECONDS
    result["at_mark"] = step_evidence(stack, run_id, step, live=True)
    while time.monotonic() < deadline and not result["at_mark"]["attempt_id"]:
        time.sleep(1)
        result["at_mark"] = step_evidence(stack, run_id, step, live=True)
    marker = Marker(stack, dag, run_id)
    marker.thread.start()
    result["marker"] = marker.finish()
    if not wait_phase(stack, "the DAG run turning failed", SETTLE_SECONDS, lambda api: api.run(dag, run_id)["state"] == "failed", 2):
        raise InterruptionControlError(f"the DAG run {run_id} was not failed within {SETTLE_SECONDS} s of the marking request")
    result["reconciled"] = [item["run_id"] for item in stack.supervisor.reconcile_runs()["reconciled"]]
    deadline = time.monotonic() + SETTLE_SECONDS
    while time.monotonic() < deadline and stack.open_attempts_of(run_id):
        time.sleep(2)
    result["open_attempts"] = stack.open_attempts_of(run_id)
    result["progress_after"] = progress_lines(stack, run_id)
    result["post"] = step_evidence(stack, run_id, step, live=False)
    return result


def execute(stack: PipelineStack, dag: str, run_id: str, artifact: Path, producer: str = "latest", interrupt_at: str | None = None) -> dict[str, Any]:
    """Trigger the actual DAG and retain its terminal task, lock, record and asset-event evidence; interrupt `interrupt_at` at its barrier point."""
    started = time.monotonic()
    conf = {"run_id": run_id, "mode": "publish_fixture"}
    if dag == "gold_transform_dag":
        conf["silver_snapshot_run"] = producer
    stack.seen_runs.add(run_id)
    interrupting = interrupt_at is not None and stack.mutant != "no_interruption"
    if interrupt_at is not None and interrupting:
        prefix = BARRIERS[interrupt_at]

        def arm(line: dict[str, Any]) -> None:
            if line["run_id"] == run_id:
                (stack.run_folder(run_id) / "publish/a3_barrier.txt").write_text(prefix)

        harness.HOOKS.on_line("acquire_run", "finished", arm)
    sys.stdout.write(json.dumps({"phase": "run_start", "dag": dag, "run_id": run_id}) + "\n")
    sys.stdout.flush()
    stack.api.trigger(dag, run_id, conf)
    interruption = None if interrupt_at is None else interrupt_run(stack, dag, run_id, interrupt_at) if interrupting else untouched()
    try:
        bounded_api(stack, time.monotonic() + RUN_SECONDS).wait_for_run(dag, run_id, RUN_SECONDS)
    except AirflowApiError as error:
        if interrupt_at is None:
            raise
        raise InterruptionControlError(f"the final wait for {dag} run {run_id}: {error}") from None
    observed = stack.remember(stack.observe(dag, run_id, started))
    if interruption is not None:
        observed["interruption"] = interruption
    (artifact / f"{run_id}_observation.json").write_text(json.dumps(observed, indent=2) + "\n")
    shutil.copytree(stack.state / "logs", artifact / "task_logs", dirs_exist_ok=True)
    for kind in ("silver", "gold", "cross"):
        relative = f"quality_{kind}/result.json"
        if (stack.run_folder(run_id) / relative).is_file():
            quality, digest = read_record(stack.run_folder(run_id), relative)
            (artifact / f"{run_id}_quality_{kind}.json").write_text(json.dumps({"source_sha256": digest, "result": quality}, indent=2) + "\n")
    attempts = stack.run_folder(run_id) / "attempts"
    for log in attempts.glob("*/log_tail.txt"):
        shutil.copyfile(log, artifact / f"{run_id}_{log.parent.name}.log")
    observed["asset_events"] = stack.api.request("GET", f"/assets/events?source_dag_id={dag}&source_run_id={run_id}")["asset_events"]
    sys.stdout.write(json.dumps({"phase": "run_end", "dag": dag, "state": observed["dag_run_state"], "seconds": round(time.monotonic() - started, 1)}) + "\n")
    sys.stdout.flush()
    return observed


def terminal(observed: dict[str, Any], succeeded: bool) -> dict[str, bool]:
    """Check the scheduler result together with durable lock and build-file outcomes."""
    events = observed["asset_events"]
    return {
        "dag_state": observed["dag_run_state"] == ("success" if succeeded else "failed"),
        "lock_state": observed["lock_state"] == ("released" if succeeded else "failed_retained"),
        "release_leaf": observed["tasks"].get("release_run") == ("success" if succeeded else "failed"),
        "asset_after_commit_only": all(event["source_task_id"] == "commit_publish" for event in events),
        "asset_count": len(events) == (1 if succeeded and observed["dag_id"] == "gold_transform_dag" else 0),
        "build_cleanup": not observed["build_file_exists"] if succeeded else True,
    }


def read_json(path: Path) -> dict[str, Any]:
    """A run-folder JSON record, or an empty record when the step never wrote it."""
    found: dict[str, Any] = json.loads(path.read_text()) if path.is_file() else {}
    return found


def bronze_head_changed(stack: PipelineStack, run_id: str, artifact: Path, control: dict[str, Any]) -> tuple[dict[str, Any], dict[str, bool]]:
    """Change the pinned bronze file after silver validation: `verify_bronze` must refuse and no publish step may run.

    `control` is the observation of the unchanged `silver` run of the same invocation, whose `verify_bronze` must have succeeded.
    """
    before = stack.visible("silver")
    changed: dict[str, str | None] = {}

    def change(line: dict[str, Any]) -> None:
        path = stack.run_folder(line["run_id"]) / "fixture_lakehouse.duckdb"
        changed["before"] = harness.file_digest(path)
        try:
            path.chmod(0o600)
            with path.open("ab") as stream:
                stream.write(b"a3-bronze-head-changed")
        except OSError as error:
            changed["error"] = f"{type(error).__name__}: {error}"
        changed["after"] = harness.file_digest(path)

    harness.HOOKS.on_line("silver_validate", "finished", change, failed=False)
    observed = execute(stack, "silver_transform_dag", run_id, artifact)
    tasks = observed["tasks"]
    submissions = observed["submissions"]
    sent = {key.split(":", 1)[0] for key in submissions}
    verify: dict[str, Any] = next((value for key, value in submissions.items() if key.startswith("verify_bronze:")), {})
    pin_lines = [lines for lines in Ledger(stack.supervisor.paths.ledger).submissions(run_id).values() if lines[0]["template"] == "pin_bronze"]
    pin_digest = pin_lines[-1][-1].get("sha256") if pin_lines else None
    observed["tamper_evidence"] = {**changed, "pin_digest": pin_digest}
    unchanged = [value for key, value in control["submissions"].items() if key.startswith("verify_bronze:")]
    checks = {
        **terminal(observed, False),
        "unchanged_control_verified": control["tasks"].get("verify_bronze") == "success" and bool(unchanged) and unchanged[-1]["exit_code"] == 0,
        "tamper_applied": "error" not in changed,
        "tampered_head": bool(changed.get("after")) and changed.get("after") != changed.get("before"),
        "pin_digest_is_the_untampered_digest_when_exposed": pin_digest is None or (pin_digest == changed.get("before") and pin_digest != changed.get("after")),
        "healthy_until_tamper": all(tasks.get(name) == "success" for name in ("pin_bronze", "silver_build", "silver_validate")),
        "verify_refused_the_changed_head": tasks.get("verify_bronze") == "failed",
        "verify_refusal_reason": "fixture_verify refused by the launcher" in str(verify.get("reason")),
        "no_publish_step_ran": all(tasks.get(name) == "upstream_failed" for name in SILVER_PUBLISH_STEPS),
        "no_publish_submission": not sent & set(SILVER_PUBLISH_STEPS),
        "no_silver_snapshot_record": not (stack.run_folder(run_id) / "silver_snapshot.json").exists(),
        "silver_published_unchanged": stack.visible("silver") == before,
    }
    return observed, checks


def interrupted_publish(stack: PipelineStack, step: str, run_id: str, artifact: Path, producer: str) -> tuple[dict[str, Any], dict[str, bool]]:
    """Interrupt one publish step of the gold DAG while its container is blocked at a named point after a named effect.

    The published set of `gold_e2e` must stay as it was, the step must not have completed and its record must be absent.
    """
    before = stack.visible("gold")
    branches_before = stack.branches("gold")
    observed = execute(stack, "gold_transform_dag", run_id, artifact, producer, interrupt_at=step)
    steps = work_steps("gold_transform_dag")
    interruption = observed["interruption"]
    tasks = observed["tasks"]
    publish = stack.run_folder(run_id) / "publish"
    after = stack.visible("gold")
    prefix = BARRIERS[step]
    lines = interruption["progress_after"]
    at_mark = interruption["at_mark"]
    post = interruption["post"]
    moved = after["mains"] != before["mains"]
    candidates_complete = read_json(publish / "candidates.json").get("complete")
    effects = {
        "publish_candidates": {
            "named_effect_candidate_branch_exists": stack.branches("gold") != branches_before,
            "candidates_record_incomplete": candidates_complete is not True,
            "mains_unchanged": not moved,
        },
        "reconcile_candidates": {
            "candidates_complete_before_the_step": candidates_complete is True,
            "step_record_absent": not (publish / "reconcile.json").exists(),
            "mains_unchanged": not moved,
        },
        "catalog_backup": {
            "named_effect_backup_file_written": any((stack.registry.parent / "pipeline_catalog/catalog_backup").glob(f"*{run_id}*")),
            "step_record_absent": not (publish / "backup.json").exists(),
            "mains_unchanged": not moved,
        },
        "commit_publish": {"named_effect_first_table_fast_forwarded": moved},
    }
    checks = {
        "progress_point_reached": bool(interruption["progress_point"]),
        "attempt_identity": at_mark.get("template") == step and bool(at_mark.get("attempt_id")),
        "attempt_running_at_mark": at_mark.get("phase") not in (None, *NOT_RUNNING) and bool(at_mark.get("container_id")),
        "ledger_open_at_mark": bool(at_mark) and not {"finished", "failed_closed"} & set(at_mark.get("events", [])),
        "stopped_at_barrier": bool(lines) and lines[-1].startswith(prefix) and sum(line.startswith(prefix) for line in lines) == 1,
        "interruption_control": interruption["marker"]["marked"] and not interruption["marker"]["worker_alive"],
        "step_not_recorded_as_succeeded": "finished" not in post.get("events", []) or post.get("exit_code") != 0,
        "dag_run_failed": observed["dag_run_state"] == "failed",
        "earlier_steps_healthy": all(tasks.get(name) == "success" for name in steps[: steps.index(step)]),
        "step_not_completed": tasks.get(step) != "success" and tasks.get("commit_publish") != "success",
        "no_asset_event": observed["asset_events"] == [],
        "reconciled_to_retained": interruption["reconciled"] == [run_id] and observed["lock_state"] == "failed_retained",
        "attempts_closed": interruption["open_attempts"] == [],
        "build_kept": observed["build_file_exists"] is True,
        "published_set_unchanged": after["published_runs"] == before["published_runs"],
        "no_commit_record": not (publish / "commit.json").exists(),
        **effects[step],
    }
    return observed, checks


def maintenance_reader(stack: PipelineStack, artifact: Path, producer: str, committed: list[str]) -> tuple[dict[str, Any], dict[str, bool]]:
    """A reader reserves, a second set commits, maintenance runs beside the reader's discovery, then beside its registered set."""
    if not committed:
        raise RuntimeError("maintenance_reader needs a gold set committed by the gold case")
    first = committed[0]
    reader = f"a3-reader-{harness.new_run_id()[-8:]}"
    late = f"{reader}-late"
    fence: dict[str, str] = {}
    registered_after = ""
    late_after = ""
    late_left = ""

    def reader_call(method: str, params: dict[str, str]) -> str:
        try:
            stack.client.call(method, params, ANY)
        except launcher.ClientError as error:
            return str(error)
        return "accepted"

    def reserve(name: str) -> str:
        return reader_call("reserve", {"reader": name, "domain": "transform", "publish_run": "latest"})

    def register() -> str:
        return reader_call("register_set", {"reader": reader, "snapshot_set": first})

    def leave(name: str) -> str:
        return reader_call("deregister_set", {"reader": name})

    def probe(_: dict[str, Any]) -> None:
        fence["reserve"] = reserve(late)
        fence["register"] = register()

    def maintain() -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
        run_id = harness.new_run_id()
        observed = execute(stack, "gold_maintenance_dag", run_id, artifact)
        folder = stack.run_folder(run_id) / "publish"
        return observed, read_json(folder / "protected.json"), read_json(folder / f"maintenance_{run_id}.json")

    checks: dict[str, bool] = {}
    published_before = stack.visible("gold")["published_runs"]
    try:
        reserved = reserve(reader)
        if reserved != "accepted":
            raise RuntimeError(f"the reader reservation was refused: {reserved[:200]}")
        second_id = harness.new_run_id()
        second = execute(stack, "gold_transform_dag", second_id, artifact, producer)
        committed.append(second_id)
        checks["second_publish_committed"] = all(terminal(second, True).values()) and stack.visible("gold")["published_runs"] == published_before + 1
        harness.HOOKS.on_line("gold_maintenance", "started", probe)
        pending_run, pending_protected, pending_result = maintain()
        harness.HOOKS.reset()
        registered_after = register()
        late_after = reserve(late)
        late_left = leave(late)
        if late_after != "accepted" or late_left != "accepted":
            raise RuntimeError(f"the late reservation control failed: reserve {late_after[:120]!r}, leave {late_left[:120]!r}")
        registered_run, registered_protected, registered_result = maintain()
        leave(reader)
        control_run, _, control_result = maintain()
    finally:
        harness.HOOKS.reset()
        for name in (reader, late):
            leave(name)
    both = sorted([first, second_id])
    checks |= {
        "maintenance_runs_succeeded": all(all(terminal(run, True).values()) for run in (pending_run, registered_run, control_run)),
        "fence_probe_ran": set(fence) == {"reserve", "register"},
        "fence_refused_reservation": FENCE in fence.get("reserve", ""),
        "fence_refused_registration": FENCE in fence.get("register", ""),
        "pending_listed_alone": [(entry.get("kind"), entry.get("id")) for entry in pending_protected.get("items", [])] == [("pending", reader)],
        "pending_set_kept": pending_result.get("kept_runs") == both,
        "registration_accepted_after_attempt": registered_after == "accepted",
        "late_reservation_accepted_and_left_after_attempt": late_after == "accepted" and late_left == "accepted",
        "protected_lists_only_the_registration": registered_protected.get("items") == [{"kind": "registration", "id": reader, "run_id": first}],
        "registered_set_kept": registered_result.get("kept_runs") == both,
        "control_keeps_only_newest": control_result.get("kept_runs") == [second_id],
    }
    return control_run, checks


def main() -> int:
    """Run the selected scheduler cases and return failure for any check or cleanup error."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seed", action="store_true")
    parser.add_argument("--case", action="append", choices=CASES)
    parser.add_argument("--mutant", choices=MUTANTS)
    args = parser.parse_args()
    selected = set(args.case or CASES)
    for name in sorted(selected & NEEDS.keys()):
        lacking = set(NEEDS[name]) - selected
        if lacking:
            parser.error(f"{name} needs {', '.join(sorted(lacking))} in the same run")
    if not getattr(launcher, "NETWORK_CLASSES_ENFORCED", ()):
        parser.error("apply the proposed launcher patch in a private review copy first")
    if args.seed:
        sys.stdout.write(json.dumps(make_seed()) + "\n")
        return 0
    folder = ROOT / "data/e2e/publish_dags" / datetime.now(UTC).strftime("%Y%m%dT%H%M%S_%fZ")
    folder.mkdir(parents=True)
    work = Path(tempfile.mkdtemp(prefix="a3dag_", dir=harness.TEMP_ROOT)).resolve()
    stack = PipelineStack(work)
    stack.mutant = args.mutant
    stack.retain_sets = 1 if "maintenance_reader" in selected else None
    stack.barrier = bool(selected & INTERRUPTED.keys())
    report: dict[str, Any] = {
        "scope": "synthetic, real scheduler, local Iceberg; no AWS",
        "cases": [],
        "mutant": args.mutant,
        "test_parameters": {"retain_sets": stack.retain_sets, "barrier_overlay": stack.barrier},
        "pending": [],
    }
    if selected & INTERRUPTED.keys():
        report["pending"].append("retained_run_resubmission: an interrupted run is not resumed; the retry is a new run after the owner's release")
    if "maintenance_reader" in selected:
        report["pending"].append(
            "expiry_preservation: proven are the protected file and the keep decision; an eligible old snapshot surviving actual expiry is not"
        )
    host_mutation = contextlib.ExitStack()
    if args.mutant == "verifier_accepts_mismatch":
        host_mutation.enter_context(verifier_accepting_mismatch())
    committed: list[str] = []
    silver_control: dict[str, Any] = {}
    interrupted_runs: list[str] = []
    try:
        with mock.patch.object(harness, "START_ATTEMPTS", 1):
            stack.start()
        report["mounted_sources_sha256"] = {
            str(path.relative_to(stack.checkout)): hashlib.sha256(path.read_bytes()).hexdigest()
            for path in [stack.checkout / "orchestration/dag_runtime.py", *(stack.checkout / f"dags/{name}.py" for name in DAGS)]
        }
        readiness = stack.wait_for_dags(DAGS)
        report["readiness"] = readiness
        if not all(readiness["present"].values()) or readiness["import_errors"]:
            raise RuntimeError("the shipped DAGs did not import")
        producer = "latest"
        for name in CASES:
            if name not in selected:
                continue
            run_id = harness.new_run_id()
            checks: dict[str, bool]
            if name == "no_silver":
                observed = execute(stack, "gold_transform_dag", run_id, folder)
                checks = {
                    **terminal(observed, False),
                    "pin_refused": observed["tasks"].get("pin_silver") == "failed",
                    "no_publish": observed["tasks"].get("commit_publish") == "upstream_failed",
                }
            elif name in ("silver", "selected_silver"):
                before = stack.visible("silver")
                observed = execute(stack, "silver_transform_dag", run_id, folder)
                checks = terminal(observed, True)
                snapshot_path = stack.run_folder(run_id) / "silver_snapshot.json"
                snapshot = json.loads(snapshot_path.read_text()) if snapshot_path.is_file() else {}
                checks["complete_silver_set"] = len(snapshot.get("snapshot_set", {})) == len(
                    json.loads((ROOT / "data_contracts/lakehouse/silver_publish.json").read_text())["publish"]["tables"]
                )
                after = stack.visible("silver")
                checks["catalog_view_sees_silver_commit"] = bool(after["mains"]) and after["published_runs"] == before["published_runs"] + 1
                if name == "silver":
                    producer = run_id
                    silver_control = observed
            elif name == "bronze_head_changed":
                observed, checks = bronze_head_changed(stack, run_id, folder, silver_control)
            elif name in INTERRUPTED:
                interrupted_runs.append(run_id)
                observed, checks = interrupted_publish(stack, INTERRUPTED[name], run_id, folder, producer)
            elif name == "maintenance":
                observed = execute(stack, "gold_maintenance_dag", run_id, folder)
                checks = {**terminal(observed, True), "maintenance_ran": observed["tasks"].get("gold_maintenance") == "success"}
            elif name == "maintenance_reader":
                observed, checks = maintenance_reader(stack, folder, producer, committed)
            else:
                if name == "tampered_build":

                    def corrupt(line: dict[str, Any]) -> None:
                        with (stack.run_folder(line["run_id"]) / "staging.duckdb").open("ab") as stream:
                            stream.write(b"a3-after-validation")

                    harness.HOOKS.on_line("cross_layer_checks", "finished", corrupt, failed=False)
                if name == "tampered_result":

                    def replace_result(line: dict[str, Any]) -> None:
                        path = stack.run_folder(line["run_id"]) / "quality_cross/result.json"
                        body = json.loads(path.read_text())
                        body["replaced_after_completion"] = True
                        path.write_text(json.dumps(body) + "\n")

                    harness.HOOKS.on_line("cross_layer_checks", "finished", replace_result, failed=False)
                before = stack.visible("gold")
                observed = execute(stack, "gold_transform_dag", run_id, folder, producer)
                checks = terminal(observed, name == "gold")
                if name == "gold":
                    pin = json.loads((stack.run_folder(run_id) / "silver_pins.json").read_text())
                    checks["explicit_old_set_selected"] = pin["source_run_id"] == producer
                    checks["iceberg_input_used"] = observed["tasks"].get("read_silver") == "success"
                    after = stack.visible("gold")
                    checks["view_sees_commit"] = bool(after["mains"]) and after["published_runs"] >= 1
                    checks["publish_runs_once"] = after["published_runs"] == before["published_runs"] + 1 and (
                        before["published_runs"] == 0 or not interrupted_runs
                    )
                    committed.append(run_id)
                else:
                    checks["seal_refused_changed_build_or_result"] = observed["tasks"].get("seal_gold") == "failed"
                    checks["retained_build"] = observed["build_file_exists"]
            report["cases"].append({"case": name, "checks": checks, "observation": observed, "ok": all(checks.values())})
            (folder / f"{name}.json").write_text(json.dumps(report["cases"][-1], indent=2) + "\n")
            harness.HOOKS.reset()
            if not all(checks.values()) and args.mutant != "no_interruption":
                raise RuntimeError(f"{name} failed its named checks")
            if observed["lock_state"] == "failed_retained":
                stack.release_leftovers()
        report["sentinel_scan"] = harness.dump_and_scan(stack, expected_runs=set(stack.seen_runs))
        if not all(report["sentinel_scan"].values()):
            raise RuntimeError("the sentinel scan failed")
    except (Exception, KeyboardInterrupt) as error:
        report["error"] = f"{type(error).__name__}: {error}"[:500]
    finally:
        harness.HOOKS.reset()
        host_mutation.close()
        report["cleanup"] = finish_publication(stack, work)
    report["source_sha256"] = {
        name: hashlib.sha256((ROOT / name).read_bytes()).hexdigest()
        for name in (
            "orchestration/launcher.py",
            "config/orchestration/broker_templates.json",
            "orchestration/dag_runtime.py",
            "scripts/orchestration/run_publish_dags_e2e.py",
            "scripts/orchestration/publish_dag_fixtures.py",
        )
    }
    report["ok"] = not report.get("error") and not report["cleanup"]["errors"] and bool(report["cases"]) and all(case["ok"] for case in report["cases"])
    (folder / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    sys.stdout.write(json.dumps({"ok": report["ok"], "report": str(folder / "report.json")}) + "\n")
    return 0 if report["ok"] else 1


if __name__ == "__main__":
    sys.exit(main())
