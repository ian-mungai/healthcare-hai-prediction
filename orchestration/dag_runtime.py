"""Build manual pipeline tasks that launch their jobs through the direct ECS launch library and retain failed runs.

A DAG of one job is one chain of four tasks (``build_job_chain``): ``preflight`` (the gates, no launch), ``job`` (the one ECS task of the job, in the
launch pool), ``evidence`` (the durable launch record says the job and its acceptance checks passed) and ``gate`` (runs after a failure too, fails the DAG
run unless the three passed and carries the asset event). The features and training DAGs still have one task per step (``build_tasks``): ``run_step`` runs
a task's fixed template through ``orchestration/airflow_steps.py``, where a job step launches or joins its job's one ECS task and a control step (acquire,
seal, release, the gates) launches nothing.
There is no other path: the DAGs hold no broker client.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import http.client
import importlib
import json
import logging
import math
import os
import re
import time
from collections.abc import Callable, Collection, Mapping, Sequence
from datetime import timedelta
from pathlib import Path
from typing import Any

from airflow.sdk import Asset, TriggerRule, get_current_context, task

from orchestration.ingestion_schedule import RETRY_KIND, pipeline_after_retry, resolve_kind, resolve_retry, resolve_run_id, retry_run_id
from orchestration.interface import SubmissionRequest
from orchestration.training_contract import (
    DAG_PARAM_DEFAULTS,
    DAGS,
    FEATURE_DAGS,
    LAUNCH_LANES,
    MODELS,
    PIPELINE_DAG,
    drift_attempt_of,
    metadata_run_for_base,
    model_drift_run_id,
    model_features_run_id,
    model_training_run_id,
    parameters_for,
    research_dag_run_id,
    route_run_conf,
    serving_parameters_for,
)

LOG = logging.getLogger(__name__)
RESEARCH_DAG = "research_dag"
STEP_TIMEOUT = timedelta(hours=26)
# The DAG parameter that selects the producer run a pin reads ("latest" when absent), and the asset each committing task emits.
SELECTIONS = {"pin_silver": "silver_snapshot_run", "pin_gold": "gold_snapshot_run", "metadata_run": "features_run"}
# A model set's features DAG builds from the base run that the base DAG published: its form names that run, not a gold run.
MODEL_SELECTIONS = {"pin_gold": "base_snapshot_run"}
# The serving deployment's form names the research run whose record names the candidate of a canary.
SERVING_DAG = "serving_deploy_dag"
SERVING_SELECTIONS = {"serve_deploy": "research_run"}
# The drift check reads one model run's published set: its form names that run, and the check's job needs the features job of that run to have passed.
DRIFT_DAG = "drift_monitoring_dag"
DRIFT_SELECTIONS = {"drift_run": "features_run"}
# The on-demand live check of a baseline: its form names the model and the HAI type, and it reads no producer run.
LIVE_DRIFT_DAG = "drift_live_check_dag"
ASSETS = {"features_commit": "features"}
# The time limits of the one-job chain's tasks that start no job.
PREFLIGHT_TIMEOUT = timedelta(minutes=30)
CHECK_TIMEOUT = timedelta(minutes=15)
CHAIN_TASKS = ("preflight", "job", "evidence")
# The step of the Run 3 publisher: its DAG form names the run kind and the capture root, and the launch checks them.
PUBLISHER_TEMPLATE = "publisher_run"
RETRY_DAG = "ingestion_retry_dag"
RESEARCH_TASKS = ("ready", "research", "chain")
# The module that launches a step, and the environment setting that names the pool of host jobs.
LAUNCH_MODULE = "orchestration.airflow_steps"
POOL_SETTING = "HAI_LAUNCH_POOL"
POOL_NAME = re.compile(r"[A-Za-z0-9_.-]{1,200}")
DEFAULT_POOL = "default_pool"
# The pool has one slot for each POOL_SLOT_CPU_UNITS of CPU the host leaves for jobs (the services stack computes both and renders them into every Airflow
# container). A job takes the slots of its share of the host: the larger of its CPU share and its memory share of the host job cap.
POOL_SLOTS_SETTING = "HAI_LAUNCH_POOL_SLOTS"
POOL_SLOT_CPU_SETTING = "HAI_LAUNCH_POOL_SLOT_CPU_UNITS"
JOB_CAP_SETTING = "HAI_LAUNCH_HOST_JOB_CAP_MIB"
# The host family that every model set is like (its row in the host table says ``like``).
MODEL_SET_PARENT = "features"
HOST_TABLE = Path(__file__).resolve().parents[1] / "config/orchestration/host_jobs.json"


TRIGGER_TIMEOUT_SECONDS = 10
# The poll of a wait on another DAG's run, the time a run that a DAG must have created may take to appear, and the clock and the wait that these read (the
# cases replace both, so that a wait of hours takes no time).
POLL_SECONDS = 60
CREATION_GRACE = timedelta(minutes=10)
CLOCK: Callable[[], float] = time.monotonic
SLEEP: Callable[[float], None] = time.sleep


class ChildRunFailed(RuntimeError):
    """A DAG run that a pipeline waits for failed, is paused, never appeared or did not end in time: the message names the DAG, the run and the cause."""


def _b64(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode()


def airflow_call(method: str, path: str, body: dict[str, Any] | None = None) -> tuple[int, Any]:
    """One request to the Airflow 3 REST API on the loopback port: the status and the decoded answer (an empty object when the body is not JSON).

    The token is signed with the session's JWT secret as the host client does, and the call ends after ``TRIGGER_TIMEOUT_SECONDS``. A refused or lost
    connection raises ``OSError``: it is never an answer.
    """
    secret = os.environ.get("AIRFLOW__API_AUTH__JWT_SECRET", "")
    if not secret:
        raise RuntimeError("the Airflow JWT secret is not in the task environment")
    now = int(time.time())
    header = _b64(json.dumps({"alg": "HS512", "typ": "JWT"}, separators=(",", ":")).encode())
    claims = _b64(
        json.dumps(
            {"sub": "admin", "role": "ADMIN", "teams": [], "aud": "apache-airflow", "iat": now, "nbf": now - 5, "exp": now + 120}, separators=(",", ":")
        ).encode()
    )
    signature = _b64(hmac.new(secret.encode(), f"{header}.{claims}".encode(), hashlib.sha512).digest())
    headers = {"Authorization": f"Bearer {header}.{claims}.{signature}"}
    connection = http.client.HTTPConnection("127.0.0.1", int(os.environ.get("AIRFLOW_API_PORT", "8080")), timeout=TRIGGER_TIMEOUT_SECONDS)
    try:
        if body is not None:
            headers["Content-Type"] = "application/json"
        connection.request(method, path, None if body is None else json.dumps(body), headers)
        response = connection.getresponse()
        status, raw = response.status, response.read()
    finally:
        connection.close()
    try:
        answer = json.loads(raw) if raw else {}
    except ValueError:
        answer = {}
    return status, answer


def create_dag_run(dag_id: str, dag_run_id_value: str, conf: dict[str, str]) -> bool:
    """Create a manual DAG run through the Airflow 3 REST API. Return True when it was created and False when it already existed. Any other answer raises."""
    status, _ = airflow_call("POST", f"/api/v2/dags/{dag_id}/dagRuns", {"dag_run_id": dag_run_id_value, "conf": conf, "logical_date": None})
    if status == 409:
        return False
    if status not in (200, 201):
        raise RuntimeError(f"the Airflow API answered {status} to the trigger of {dag_id}")
    return True


def dag_run(dag_id: str, run_id: str) -> dict[str, Any] | None:
    """The record of a DAG run (its state and the settings it was started with), or None when Airflow holds no such run. A record of another run raises."""
    status, answer = airflow_call("GET", f"/api/v2/dags/{dag_id}/dagRuns/{run_id}")
    if status == 404:
        return None
    if status != 200 or not isinstance(answer, dict) or not isinstance(answer.get("state"), str):
        raise RuntimeError(f"the Airflow API answered {status} to the read of {dag_id} run {run_id}")
    if answer.get("dag_id", dag_id) != dag_id or answer.get("dag_run_id", run_id) != run_id:
        raise RuntimeError(f"the Airflow API answered with the run {answer.get('dag_id')} {answer.get('dag_run_id')} to the read of {dag_id} run {run_id}")
    return answer


def dag_run_state(dag_id: str, run_id: str) -> str | None:
    """The state of a DAG run (``queued``, ``running``, ``success`` or ``failed``), or None when Airflow holds no such run."""
    record = dag_run(dag_id, run_id)
    return None if record is None else str(record["state"])


# The Airflow OpenLineage provider adds this setting to the conf of every run that a TriggerDagRunOperator starts (apache-airflow-providers-standard 1.21.0,
# ``openlineage_inject_parent_info``, on by default): the parent run's lineage facets and nothing else. It is for lineage only and no DAG reads it as a setting.
LINEAGE_CONF_KEY = "openlineage"


def conf_problem(expected: Mapping[str, Any], found: Any, defaults: Mapping[str, Any] | None = None, free: Collection[str] = ()) -> str | None:
    """Every way the settings a run was started with (``found``, as Airflow stores them: only what was named) differ from the settings it would have been
    started with (``expected``), or None when they are the same. A parameter the form defaults takes its default when neither names it, so a run started by
    hand with the defaults left out is the same run. A parameter that is not in ``expected`` and not defaulted, and a parameter that ``expected`` names and the
    run lacks, are differences too. ``free`` parameters are not compared: their values come from data the caller does not read, and the form checks them.
    The provider's lineage setting (``LINEAGE_CONF_KEY``) is not compared either: runs that a trigger task started carry it. Every other one is compared."""
    named = found if isinstance(found, dict) else {}
    wanted, seen = {**(defaults or {}), **expected}, {**(defaults or {}), **named}
    problems = []
    for name in sorted(set(wanted) | set(seen)):
        if name in free or name == LINEAGE_CONF_KEY:
            continue
        if name not in seen:
            problems.append(f"{name}: expected {wanted[name]!r}, found none")
        elif name not in wanted:
            problems.append(f"{name}: expected none (the pipeline sets no such parameter), found {seen[name]!r}")
        elif seen[name] != wanted[name]:
            problems.append(f"{name}: expected {wanted[name]!r}, found {seen[name]!r}")
    return "; ".join(problems) or None


def dag_is_paused(dag_id: str) -> bool:
    """Whether the DAG is paused. A DAG that Airflow does not hold (not parsed, or removed) raises: it is neither paused nor active."""
    status, answer = airflow_call("GET", f"/api/v2/dags/{dag_id}")
    if status == 404:
        raise RuntimeError(f"{dag_id} is not registered in Airflow (its file is not parsed or was removed)")
    if status != 200 or not isinstance(answer, dict) or not isinstance(answer.get("is_paused"), bool):
        raise RuntimeError(f"the Airflow API answered {status} to the read of {dag_id}")
    return bool(answer["is_paused"])


OPEN_TASK_STATES = frozenset({"queued", "running", "scheduled", "deferred", "up_for_retry", "up_for_reschedule", "restarting"})


def task_outcome(dag_id: str, run_id: str, task_id: str) -> str | None:
    """What a task of a DAG run did, from its task instances (a mapped task has one for each mapped request): ``failed`` when any failed, ``running`` while
    any is open, ``success`` when any succeeded and none failed. Otherwise every instance was skipped, and the map index says why: a mapped request that
    was skipped (map index 0 or more) is a trigger that found its child run already there (``skip_when_already_exists``), so ``duplicate``; the one
    instance of a task that mapped no request (map index -1) is ``skipped``. None when the run has no such task. An incomplete listing, or a skipped
    instance that does not give its map index, raises."""
    status, answer = airflow_call("GET", f"/api/v2/dags/{dag_id}/dagRuns/{run_id}/taskInstances?limit=100")
    rows = answer.get("task_instances") if isinstance(answer, dict) else None
    total = answer.get("total_entries") if isinstance(answer, dict) else None
    if status != 200 or not isinstance(rows, list) or not isinstance(total, int) or isinstance(total, bool):
        raise RuntimeError(f"the Airflow API answered {status} to the task list of {dag_id} run {run_id}")
    if total != len(rows):
        raise RuntimeError(f"the task list of {dag_id} run {run_id} is incomplete: {len(rows)} of {total}")
    mine = [row for row in rows if row.get("task_id") == task_id]
    states = [row.get("state") for row in mine]
    if not states:
        return None
    if any(state in ("failed", "upstream_failed") for state in states):
        return "failed"
    if any(state is None or state in OPEN_TASK_STATES for state in states):
        return "running"
    if "success" in states:
        return "success"
    indexes = [row.get("map_index") for row in mine if row.get("state") == "skipped"]
    if any(not isinstance(index, int) or isinstance(index, bool) for index in indexes):
        raise RuntimeError(
            f"the task list of {dag_id} run {run_id} does not give the map index of a skipped {task_id}: a duplicate trigger cannot be told from none"
        )
    return "duplicate" if any(index >= 0 for index in indexes) else "skipped"


def require_active(dag_ids: Sequence[str]) -> str:
    """Refuse unless every DAG is registered and not paused, and name each one that is not: a run created under a paused DAG waits for ever."""
    problems = []
    for dag_id in dag_ids:
        try:
            if dag_is_paused(dag_id):
                problems.append(f"{dag_id} is paused")
        except RuntimeError as error:
            problems.append(str(error))
    if problems:
        raise ChildRunFailed("the pipeline cannot run: " + "; ".join(problems) + ". Activate them: trigger_dag --dag pipeline_dag unpauses the whole inventory")
    return f"{len(dag_ids)} DAGs are registered and active"


def join_child(dag_id: str, run_id: str, conf: dict[str, str], limit: timedelta) -> str:
    """Start one child DAG run and wait for its own outcome, or join the run that already exists under the same ID: the one place a pipeline waits for a stage.

    A run that exists is joined only when it was started with the settings this pipeline would start it with (``conf``, the form's defaults filled in): another
    logical run, producer run, mode or other parameter fails here, naming the DAG, the run, the parameter and both values, before any wait. A run that exists is
    never created again, reset or cleared. A run that succeeded is adopted, a running one is waited for and a failed one fails here, so the pipeline never goes
    on past a failed stage. A paused DAG fails at once (its run would never start). The wait ends at ``limit``.
    """
    deadline = CLOCK() + limit.total_seconds()
    record = dag_run(dag_id, run_id)
    if record is None:
        if dag_is_paused(dag_id):
            raise ChildRunFailed(f"{dag_id} is paused, so its run {run_id} would never start")
        create_dag_run(dag_id, run_id, conf)  # a run that appeared in the meantime is the same run: the join below reads and checks it
        record = dag_run(dag_id, run_id)
    checked = False
    while True:
        state = None if record is None else str(record["state"])
        if record is not None and not checked:
            problem = conf_problem(conf, record.get("conf"), DAG_PARAM_DEFAULTS.get(dag_id))
            if problem is not None:
                raise ChildRunFailed(f"{dag_id} run {run_id} exists but was started with other settings than this pipeline's, so it is not reused: {problem}")
            checked = True
        if state == "success":
            return f"{dag_id} run {run_id} succeeded"
        if state == "failed":
            raise ChildRunFailed(f"{dag_id} run {run_id} failed")
        if state == "queued" and dag_is_paused(dag_id):
            raise ChildRunFailed(f"{dag_id} is paused, so its queued run {run_id} would never start")
        if CLOCK() >= deadline:
            raise ChildRunFailed(f"{dag_id} run {run_id} is still {state or 'not visible'} after {limit}")
        SLEEP(POLL_SECONDS)
        record = dag_run(dag_id, run_id)


# What a trigger task did when its run exists: it created the child run (``success``) or found it there already (``duplicate``). The route follows the child
# either way: it checks the settings of a run that exists and waits for its end, so a child that failed, is running or was started otherwise is not passed over.
STARTED = ("success", "duplicate")


class ModelRoute:
    """One model's route after the features base: its features run, the drift run it starts, the training run the drift gate may start and, for a promotion
    model, the research run that training starts. Each run is created by the DAG before it, so a route only reads them."""

    def __init__(self, model: str, base_run: str) -> None:
        self.model = model
        self.step = "features"
        self.dag = next(dag for dag, name in FEATURE_DAGS.items() if name == model)
        self.run = model_features_run_id(base_run, model)
        self.outcome: str | None = None
        self.detail = ""
        self.seen: float | None = None
        self.base = base_run
        self.checked: str | None = None

    def finish(self, outcome: str, detail: str) -> None:
        self.outcome, self.detail = outcome, f"{self.model} {self.step}: {detail}"

    def move(self, step: str, dag: str, run: str) -> None:
        self.step, self.dag, self.run, self.seen = step, dag, run, None

    def advance(self) -> None:
        """Read the run this route waits for once, check once that it was started with the settings the DAG before it gives it, and go on to the next run
        when it succeeded."""
        record = dag_run(self.dag, self.run)
        if record is None:
            now = CLOCK()
            if self.seen is None:
                self.seen = now
            elif now - self.seen >= CREATION_GRACE.total_seconds():
                self.finish("failed", f"run {self.run} of {self.dag} was never created")
            return
        if self.checked != self.run:
            fixed, free = route_run_conf(self.step, self.base, self.model)
            problem = conf_problem(fixed, record.get("conf"), DAG_PARAM_DEFAULTS.get(self.dag), free)
            if problem is not None:
                self.finish("failed", f"run {self.run} of {self.dag} was started with other settings than its route's: {problem}")
                return
            self.checked = self.run
        state = str(record["state"])
        if state == "failed":
            self.finish("failed", f"run {self.run} of {self.dag} failed")
        elif state == "success":
            self.after_success()

    def after_success(self) -> None:
        if self.step == "features":
            self.move("drift", DRIFT_DAG, model_drift_run_id(self.run))
        elif self.step == "drift":
            started = task_outcome(self.dag, self.run, "trigger_training")
            if started in STARTED:
                self.move("training", "training_dag", model_training_run_id(self.run, self.model))
            elif started == "skipped":
                self.finish("done", "the drift gate trained no type, so no training run exists")
            else:
                self.finish("failed", f"drift run {self.run} did not say whether it started training (trigger_training is {started})")
        elif self.step == "training":
            started = task_outcome(self.dag, self.run, "trigger_research")
            if started in STARTED:
                self.move("research", RESEARCH_DAG, research_dag_run_id(self.run))
            elif started == "skipped":
                self.finish("done", "the model starts no research")
            else:
                self.finish("failed", f"training run {self.run} did not say whether it started research (trigger_research is {started})")
        else:
            self.finish("done", "researched")


def join_model_routes(base_run: str, limit: timedelta) -> str:
    """Wait for every model's route after the features base to end, in one task (so one executor slot), and fail when any route failed.

    The routes run side by side, so all of them are followed until each has ended: a failed route is reported with the others' outcomes, and no route is
    left running when the pipeline ends. A run that a route needs and that never appears, or a route that does not end by ``limit``, fails. A model whose drift
    gate trained no type has no training run, and a model that is not compared has no research run: those routes end there, and that is a success. The
    catalog run is not part of a route.
    """
    routes = [ModelRoute(model, base_run) for model in MODELS]
    deadline = CLOCK() + limit.total_seconds()
    while True:
        for route in routes:
            if route.outcome is None:
                route.advance()
        open_routes = [route for route in routes if route.outcome is None]
        if not open_routes:
            break
        if CLOCK() >= deadline:
            raise ChildRunFailed(
                f"the model routes still running after {limit}: "
                + ", ".join(f"{route.model} ({route.step} run {route.run} of {route.dag})" for route in open_routes)
            )
        SLEEP(POLL_SECONDS)
    failed = [route.detail for route in routes if route.outcome == "failed"]
    if failed:
        raise ChildRunFailed("a model route failed: " + "; ".join(failed))
    return "; ".join(route.detail for route in routes)


def launch_pool() -> str:
    """The pool that serializes host jobs where the deployment names one (``HAI_LAUNCH_POOL``), else Airflow's default pool."""
    name = os.environ.get(POOL_SETTING)
    if name is None:
        return DEFAULT_POOL
    if not POOL_NAME.fullmatch(name):
        raise ValueError(f"{POOL_SETTING} must be a pool name of letters, digits, dots, dashes and underscores")
    return name


def whole_setting(name: str) -> int:
    """The positive whole number in the setting ``name``, or a refusal that names the setting."""
    raw = os.environ.get(name, "")
    if not raw.isdecimal() or int(raw) < 1:
        raise ValueError(f"{name} must be a whole number above 0 while {POOL_SLOTS_SETTING} is set")
    return int(raw)


def host_row(family: str | None) -> dict[str, Any]:
    """The row of ``family`` in the host table (an empty row for ``None``, as the steps of the other DAGs name no family)."""
    return json.loads(HOST_TABLE.read_text())["jobs"][family] if family is not None else {}


def pool_slots_of(family: str | None) -> int:
    """The pool slots of a job in ``family``: all of them for a family that reserves the whole job room (``None`` too, as the steps of the other DAGs
    do), so such a job runs alone; the larger of its CPU slots and its memory slots for a family that reserves its own size, so several run together
    (``config/orchestration/host_jobs.json``). A deployment that sets no slot count has the one-slot pool: one slot each."""
    if POOL_SLOTS_SETTING not in os.environ:
        return 1
    slots = whole_setting(POOL_SLOTS_SETTING)
    need = host_row(family)
    if not need.get("reserve"):
        return slots
    by_cpu = math.ceil(int(need["cpu_units"]) / whole_setting(POOL_SLOT_CPU_SETTING))
    by_memory = math.ceil(int(need["memory_mib"]) * slots / whole_setting(JOB_CAP_SETTING))
    return min(slots, max(by_cpu, by_memory))


def pool_settings(family: str | None = None) -> dict[str, Any]:
    """The task settings that put a launching task in the host pool: its name, and its slots where the deployment sets a slot count. A model set (a host
    family that is like the features job) also takes its slots as its priority weight, so the scheduler starts the large sets first and the small ones fill
    the room beside them."""
    settings: dict[str, Any] = {"pool": launch_pool()}
    if POOL_SLOTS_SETTING in os.environ:
        settings["pool_slots"] = pool_slots_of(family)
        if host_row(family).get("like") == MODEL_SET_PARENT:
            settings["priority_weight"] = settings["pool_slots"]
    return settings


def selections_of(dag_id: str) -> dict[str, str]:
    """The DAG parameter that names the producer run of each template's job in ``dag_id``."""
    if dag_id == SERVING_DAG:
        return SERVING_SELECTIONS
    if dag_id == DRIFT_DAG:
        return DRIFT_SELECTIONS
    return MODEL_SELECTIONS if dag_id in FEATURE_DAGS else SELECTIONS


def step_request(template: str, context: dict[str, Any]) -> SubmissionRequest:
    """The request of the current task's fixed template."""
    params = context["params"]
    instance = context["ti"]
    selections = selections_of(instance.dag_id)
    selected = params.get(selections[template], "latest") if template in selections else "latest"
    training = None
    mode = str(params["mode"])
    if instance.dag_id == RESEARCH_DAG:
        raw = {name: params[name] for name in ("base_run", "population", "hai_types", "mlflow")}
        if params.get("research_attempt", 1) != 1:  # attempt 1 is the default and is never sent, so the first research run keeps its parameters
            raw["research_attempt"] = params["research_attempt"]
        training = parameters_for(RESEARCH_DAG, raw, LAUNCH_LANES)  # the research step runs on AWS: mlflow=local is refused by name
    elif instance.dag_id == SERVING_DAG:
        training = serving_parameters_for({name: params[name] for name in ("hai_type", "deploy_mode")})  # a closed request: no version and no share
    elif instance.dag_id in DAGS:
        names = (
            ("features_run", "population", "hai_types", "trigger", "mlflow", "model")
            if instance.dag_id == "training_dag"
            else ("hai_type", "model_version", "mlflow")
        )
        raw = {name: params[name] for name in names}
        if instance.dag_id == "model_promotion_dag":
            raw["promotion_mode"] = mode
            mode = "real"
        training = parameters_for(instance.dag_id, raw, LAUNCH_LANES)  # every step runs on AWS: mlflow=local is refused by name
    elif instance.dag_id == DRIFT_DAG:
        training = {"model": params["model"]}  # the model whose set the drift job checks; the preflight refuses a model outside the contract
        if params.get("drift_attempt", 1) != 1:  # attempt 1 is the default and is never sent, so the first drift run keeps its parameters
            training["drift_attempt"] = drift_attempt_of(params["drift_attempt"])
    elif instance.dag_id == LIVE_DRIFT_DAG:
        training = {"model": params["model"], "hai_type": params["hai_type"]}  # the preflight refuses a model or type outside the contract
    publisher = None
    if template == PUBLISHER_TEMPLATE and instance.dag_id == RETRY_DAG:
        original, day = resolve_retry(str(params["original_run"]), int(params["day"]), context)
        run_id = retry_run_id(original, day)
        publisher = {"kind": RETRY_KIND, "capture_root": params["capture_root"], "original_run": original, "day": day}
    elif template == PUBLISHER_TEMPLATE:
        run_id = resolve_run_id(str(params["run_id"]), context)
        publisher = {"kind": resolve_kind(str(params["run_kind"]), context), "capture_root": params["capture_root"]}
    else:
        run_id = str(params["run_id"])
    return SubmissionRequest(
        template,
        run_id,
        mode,
        int(instance.try_number),
        instance.dag_id,
        instance.task_id,
        instance.run_id,
        None if selected == "latest" else str(selected),
        training=training,
        publisher=publisher,
    )


def launch_module() -> Any:
    """The module that launches a step. It is imported by name when a step runs, so a container without it still parses the DAGs and the step names it."""
    try:
        airflow_steps = importlib.import_module(LAUNCH_MODULE)  # dynamic: the launch library's step, mounted where the DAGs launch jobs
    except ImportError as error:
        raise RuntimeError(f"{LAUNCH_MODULE} is not in this container's mounts, so the step cannot launch its job: {error}") from None
    return airflow_steps


def run_step(template: str) -> str:
    """Run the current task's fixed template through the direct ECS launch library and return its successful state."""
    context = get_current_context()
    return str(launch_module().launch_step(step_request(template, context), context, selections_of(context["ti"].dag_id)))


def collapsed_step(kind: str, template: str, built: Callable[[Any], None] | None = None) -> Any:
    """Run one task of the one-job chain whose job ``template`` names: its preflight, its launch or its evidence. The launch passes ``built`` the launch
    operator before it runs."""
    context = get_current_context()
    steps = launch_module()
    request = step_request(template, context)
    selections = selections_of(context["ti"].dag_id)
    if kind == "job":
        return steps.job_step(request, context, selections, built)
    return {"preflight": steps.preflight_step, "evidence": steps.evidence_step}[kind](request, context, selections)


class JobLineage:
    """The lineage hooks of a job task. The OpenLineage listener asks the operator it was given, here the task that runs the launch operator inside it, for
    ``get_openlineage_facets_on_complete`` and ``..._on_failure``, so the task forwards them to the launch operator once the launch has built it. A task
    that failed before it launched has no launch operator and gives the listener nothing."""

    def __init__(self) -> None:
        self.launch: Any = None

    def built(self, operator: Any) -> None:
        self.launch = operator

    def get_openlineage_facets_on_complete(self, task_instance: Any) -> Any:
        return None if self.launch is None else self.launch.get_openlineage_facets_on_complete(task_instance)

    def get_openlineage_facets_on_failure(self, task_instance: Any) -> Any:
        return None if self.launch is None else self.launch.get_openlineage_facets_on_failure(task_instance)


def build_job_chain(template: str, asset: str | None = None, family: str | None = None) -> Any:
    """Create preflight, job, evidence and gate for the job that ``template`` names and return the gate operator. The gate carries ``asset``. ``family`` is
    the host family the DAG launches in when it is not the job's own (a model set's); it sets the slots of the job task."""

    @task(task_id="preflight", retries=0, execution_timeout=PREFLIGHT_TIMEOUT)
    def preflight() -> str:
        return str(collapsed_step("preflight", template))

    lineage = JobLineage()

    @task(task_id="job", retries=0, execution_timeout=STEP_TIMEOUT, **pool_settings(family))
    def job() -> str:
        return str(collapsed_step("job", template, lineage.built))

    @task(task_id="evidence", retries=0, execution_timeout=CHECK_TIMEOUT)
    def evidence() -> Any:
        return collapsed_step("evidence", template)

    @task(task_id="gate", trigger_rule=TriggerRule.ALL_DONE, retries=0, execution_timeout=CHECK_TIMEOUT, outlets=[Asset(asset)] if asset else [])
    def gate() -> str:
        states = task_states(CHAIN_TASKS)
        failures = [name for name, state in states.items() if state != "success"]
        if failures:
            raise RuntimeError(f"the job gate rejected the run: {', '.join(f'{name} {states[name]}' for name in failures)}")
        return "accepted"

    first, launched, found, last = preflight().operator, job().operator, evidence().operator, gate().operator
    launched.get_openlineage_facets_on_complete = lineage.get_openlineage_facets_on_complete
    launched.get_openlineage_facets_on_failure = lineage.get_openlineage_facets_on_failure
    first >> launched >> found >> last
    return last


def build_pipeline_start() -> Any:
    """Create ``start_pipeline`` behind the gate of ``ingestion_retry_dag`` and return its operator.

    The task starts the first DAG (``PIPELINE_DAG``) under the pipeline run ID of the ingestion run (``pipeline_<hex>``, also its Airflow run ID) from the
    bronze stage, because ingestion and its retry period are over. The last retry day of a scheduled period starts it whatever that day's own result was,
    because the files still missing then are carried over. A hand-started retry and a test capture root start none (a hand-started ingestion run starts
    none either: the first DAG runs ingestion itself). A start that fails (a paused or missing first DAG, a refused request) fails the task and so the DAG run,
    in plain sight: nothing else would start the later stages. A pipeline run that exists counts as started.

    The task is the last one of the DAG and runs after a failure too, and Airflow ends a DAG run by its last tasks, where a skipped one counts as
    success. So the task fails itself, after the handoff it owes, when the gate or a task before it did not succeed: a failed retry day never ends as a
    successful run.
    """

    @task(task_id="start_pipeline", trigger_rule=TriggerRule.ALL_DONE, retries=0, execution_timeout=CHECK_TIMEOUT)
    def start_pipeline() -> str:
        from airflow.sdk.exceptions import AirflowSkipException

        states = task_states((*CHAIN_TASKS, "gate"))
        failures = [f"{name} {state}" for name, state in states.items() if state != "success"]
        try:
            started = hand_over_to_pipeline()
        except AirflowSkipException:
            if not failures:
                raise
            started = "no pipeline run started"
        if failures:
            raise RuntimeError(f"the retry day failed ({', '.join(failures)}). Pipeline handoff: {started}")
        return started

    def hand_over_to_pipeline() -> str:
        from airflow.sdk.exceptions import AirflowSkipException

        context = get_current_context()
        params = context["params"]
        if params["capture_root"] != "raw/":
            raise AirflowSkipException("a test capture root is never read by bronze")
        original, day = resolve_retry(str(params["original_run"]), int(params["day"]), context)
        pipeline = pipeline_after_retry(original, day, context)
        if pipeline is None:
            raise AirflowSkipException("this run does not end an ingestion run's retry period, so it starts no pipeline run")
        if dag_is_paused(PIPELINE_DAG):
            raise RuntimeError(f"{PIPELINE_DAG} is paused, so the run {pipeline} would never start: activate it (trigger_dag --dag {PIPELINE_DAG} does)")
        conf = {"run_id": pipeline, "mode": "real", "start_from": "bronze"}
        if create_dag_run(PIPELINE_DAG, pipeline, conf):
            return f"pipeline run {pipeline} started"
        record = dag_run(PIPELINE_DAG, pipeline)
        problem = conf_problem(conf, None if record is None else record.get("conf"), DAG_PARAM_DEFAULTS[PIPELINE_DAG])
        if problem is not None:
            raise RuntimeError(f"the pipeline run {pipeline} exists but was started with other settings than this retry day's, so it is not reused: {problem}")
        return f"pipeline run {pipeline} already exists"

    return start_pipeline().operator


def build_metadata_start() -> Any:
    """Create ``start_metadata`` and return its operator: the catalog start after a training run.

    The task creates ``metadata_ingest_dag`` under ``metadata_<hex>`` of the training run's base run, with the base run as the features run the catalog
    ingests, once the run's release passed. A hand-started run with no base run starts none. Six models and their types give one catalog run (the later starts
    find it, which counts as done). It creates the DAG run through the Airflow REST API with a bounded call. A trigger operator reports success by raising an
    exception, which this handler would swallow. An error of its own (a missing or paused catalog DAG, a refused request, no answer in time) is logged at
    error level and returned in the task result, never raised, so the training result does not depend on the catalog.
    """

    @task(task_id="start_metadata", retries=0, execution_timeout=CHECK_TIMEOUT)
    def start_metadata() -> str:
        from airflow.sdk.exceptions import AirflowSkipException

        params = get_current_context()["params"]
        base = str(params["base_run"])
        if not base:
            raise AirflowSkipException("a training run with no base run starts no catalog run")
        run = metadata_run_for_base(base)
        conf = {"run_id": run, "features_run": base, "mode": params["mode"]}
        try:
            created = create_dag_run("metadata_ingest_dag", run, conf)
            record = None if created else dag_run("metadata_ingest_dag", run)
            problem = None if created or record is None else conf_problem(conf, record.get("conf"), DAG_PARAM_DEFAULTS["metadata_ingest_dag"])
        except Exception as error:
            LOG.error("the catalog start for %s failed: %s: %s", run, type(error).__name__, error)
            return f"catalog start failed: {type(error).__name__}: {error}"
        if problem is not None:
            LOG.error("the catalog run %s exists but was started with other settings than this training run's: %s", run, problem)
            return f"catalog start refused: {run} exists but was started with other settings than this training run's: {problem}"
        return f"catalog start requested: {run} ({'created' if created else 'already exists'})"

    return start_metadata().operator


def research_request() -> SubmissionRequest:
    """The research DAG's request: its research run, with the form's closed parameters. A form that names the local lane is refused here, by name."""
    context = get_current_context()
    return step_request("research", context)


def build_research_tasks() -> Any:
    """Create ready, research, chain and gate for ``research_dag`` and return the gate operator.

    ``ready`` launches nothing: it ends skipped, not failed, until m1, m2 and m4 each have a passed training run on the base run, and every task after it
    is then skipped too. ``research`` is the one-job step of the promote family (its preflight and launch run in the one task, joined by a retry). ``chain``
    runs the promotion chain of each infection type that has a challenger (``promotion_chain``). ``gate`` runs after a failure too and fails the DAG run unless
    the three passed, or every one was skipped.
    """

    @task(task_id="ready", retries=0, execution_timeout=CHECK_TIMEOUT)
    def ready() -> str:
        from airflow.sdk.exceptions import AirflowSkipException

        missing = launch_module().research_missing_models(research_request())
        if missing:
            raise AirflowSkipException(f"research waits for a passed training run of {', '.join(missing)} on this base run")
        return "ready"

    @task(task_id="research", retries=0, execution_timeout=STEP_TIMEOUT, pool=launch_pool())
    def research() -> str:
        request = research_request()
        context = get_current_context()
        steps = launch_module()
        steps.job_step(request, context, {})
        return str(steps.evidence_step(request, context, {})["run_id"])

    @task(task_id="chain", retries=0, execution_timeout=STEP_TIMEOUT, pool=launch_pool())
    def chain() -> str:
        return str(launch_module().research_chain_step(research_request(), get_current_context()))

    @task(task_id="gate", trigger_rule=TriggerRule.ALL_DONE, retries=0, execution_timeout=CHECK_TIMEOUT)
    def gate() -> str:
        states = task_states(RESEARCH_TASKS)
        if all(state == "skipped" for state in states.values()):
            return "skipped"
        failures = [name for name, state in states.items() if state != "success"]
        if failures:
            raise RuntimeError(f"the research gate rejected the run: {', '.join(f'{name} {states[name]}' for name in failures)}")
        return "accepted"

    first, researched, chained, last = ready().operator, research().operator, chain().operator, gate().operator
    first >> researched >> chained >> last
    return last


def task_states(task_ids: tuple[str, ...]) -> dict[str, str | None]:
    """Read this DAG run's named task states from Airflow's task-instance API."""
    ti = get_current_context()["ti"]
    found = ti.get_task_states(dag_id=ti.dag_id, task_ids=list(task_ids), run_ids=[ti.run_id])
    states = found.get(ti.run_id, {})
    return {name: states.get(name) for name in task_ids}


def build_tasks(work_steps: tuple[str, ...], family: str) -> Any:
    """Create an ordered work chain, an always-run catalog release and a failure-preserving release leaf, and return the release operator."""

    def step(name: str, rule: TriggerRule = TriggerRule.ALL_SUCCESS) -> Any:
        outlets = [Asset(ASSETS[name])] if name in ASSETS else []

        @task(task_id=name, trigger_rule=rule, retries=0, execution_timeout=STEP_TIMEOUT, outlets=outlets, **pool_settings())
        def run() -> str:
            return run_step(name)

        return run().operator

    work = [step(name) for name in work_steps]
    for before, after in zip(work, work[1:], strict=False):
        before >> after
    catalog_release = step("catalog_release", TriggerRule.ALL_DONE)
    for operation in work:
        operation >> catalog_release
    predecessors = (*work_steps, "catalog_release")

    @task(task_id="final_gate", trigger_rule=TriggerRule.ALL_DONE, retries=0, execution_timeout=STEP_TIMEOUT)
    def final_gate() -> str:
        states = task_states(predecessors)
        failures = [name for name, state in states.items() if state != "success"]
        verdict = "rejected" if failures else "accepted"
        run_step(f"{family}_final_gate_{verdict}")
        if failures:
            LOG.warning("Pipeline gate rejected tasks: %s", ", ".join(failures))
        return verdict

    @task(task_id="release_run", trigger_rule=TriggerRule.ALL_DONE, retries=0, execution_timeout=STEP_TIMEOUT)
    def release_run() -> str:
        state = task_states(("final_gate",))["final_gate"]
        verdict = get_current_context()["ti"].xcom_pull(task_ids="final_gate") if state == "success" else None
        accepted = state == "success" and verdict == "accepted"
        run_step("release_accepted" if accepted else "release_rejected")
        if not accepted:
            raise RuntimeError("The pipeline failed its final gate; its lock and recovery inputs are retained")
        return "released"

    gate = final_gate().operator
    release = release_run().operator
    for operation in [*work, catalog_release]:
        operation >> gate
    gate >> release
    return release
