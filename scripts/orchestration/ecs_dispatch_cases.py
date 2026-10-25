"""Exercise renderer-bound dispatch and authority admission with fake ECS and STS APIs."""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
import sys
import tempfile
from collections.abc import Callable
from contextlib import contextmanager
from datetime import UTC, datetime
from email.utils import format_datetime
from importlib.metadata import version
from pathlib import Path
from typing import Any

from orchestration import ecs_admission, ecs_dispatch, ecs_target, launcher
from scripts.infrastructure.render_ecs_target import build_target
from scripts.lakehouse import run_lock

ROOT = Path(__file__).resolve().parents[2]
NOW = 1792886400
SOURCES = (
    "orchestration/ecs_admission.py",
    "orchestration/ecs_dispatch.py",
    "orchestration/ecs_reconcile.py",
    "orchestration/ecs_target.py",
    "data_contracts/orchestration/execution.schema.json",
    "data_contracts/orchestration/ecs_job.schema.json",
    "data_contracts/orchestration/ledger_line.schema.json",
    "data_contracts/orchestration/sdk_worker.schema.json",
    "orchestration/catalog_authority.py",
    "orchestration/ecs_stage.py",
    "scripts/orchestration/ecs_submission_cases.py",
    "scripts/orchestration/ecs_fake_api.py",
    "scripts/orchestration/broker.py",
    "orchestration/interface.py",
    "scripts/orchestration/ecs_dispatch_cases.py",
    "scripts/infrastructure/render_ecs_target.py",
    "orchestration/launcher.py",
    "data_contracts/orchestration/examples/ecs_target_inputs.json",
)
ENVIRONMENT = {"HAI_ATTEMPT_ID", "HAI_TRY_ID", "HAI_RUN_ID", "HAI_GENERATION", "HAI_STAGE_PREFIX", "HAI_JOB_SPEC_SHA256", "HAI_DEADLINE_UTC"}


def authority() -> dict[str, Any]:
    """Return complete synthetic AWS authority with production disabled."""
    return {
        "version": 1,
        "state": "aws_writer",
        "epoch": 1,
        "phase": "P4",
        "production_layers": dict.fromkeys(("bronze", "silver", "gold", "features"), "off"),
        "cutover_id": "synthetic_cutover",
        "command_id": "synthetic_command",
        "reason": "synthetic acceptance",
        "time": "2026-10-25T00:00:00+00:00",
        "target": "aws",
        "frozen": False,
    }


def initial_attempt() -> dict[str, Any]:
    """Return a launcher-compatible attempt with a fixed deadline."""
    attempt = {
        "id": "synthetic_attempt",
        "generation": 1,
        "deadline_utc": NOW + 600,
        "template": "pipeline_validate_gold",
        "params": {},
        "owner": "run:20261025T000000Z-01234567",
    }
    ecs_target.initialize(attempt, "ecs", NOW, None)
    return attempt


class Clock:
    """Supply wall and monotonic readings without a sleep or random source."""

    def __init__(self) -> None:
        self.now = float(NOW)

    def time(self) -> float:
        return self.now

    def monotonic(self) -> float:
        return self.now - NOW + 100


class Store:
    """Commit registry state to a file that the fake ECS reads before every effect."""

    def __init__(self, folder: Path) -> None:
        folder.mkdir(parents=True)
        self.path = folder / "registry.json"
        self.on_enter: Callable[[dict[str, Any]], None] | None = None
        self.on_commit: Callable[[dict[str, Any]], None] | None = None
        self.write({"launcher": {"catalog_authority": authority()}, "attempts": {"synthetic_attempt": initial_attempt()}})

    def read(self) -> dict[str, Any]:
        return json.loads(self.path.read_text())

    def write(self, state: dict[str, Any]) -> None:
        temporary = self.path.with_suffix(".new")
        temporary.write_text(json.dumps(state, sort_keys=True))
        temporary.replace(self.path)

    @contextmanager
    def tx(self) -> Any:
        state = self.read()
        if self.on_enter is not None:
            self.on_enter(state)
        yield state
        self.write(state)
        if self.on_commit is not None:
            self.on_commit(state)


class FakeApi:
    """Enforce the request and paging contracts while recording exact calls."""

    def __init__(self, store: Store, clock: Clock, target: dict[str, Any], captures: dict[str, Any]) -> None:
        self.store, self.clock, self.target = store, clock, target
        self.calls: list[tuple[str, dict[str, Any], float]] = []
        self.run_requests: list[dict[str, Any]] = []
        self.tasks: dict[str, dict[str, Any]] = {}
        self.lost_reply = False
        self.date_offset = 0
        self.account = target["account_id"]
        self.incomplete = False
        self.repeated_token = False
        self.describe_delay = 0
        self.after_definition: Callable[[], None] | None = None
        self.no_effect_error = False
        name = "quality_offline"
        self.description = {
            "task_definition_arn": target["definitions"][name]["task_definition_arn"],
            "status": "ACTIVE",
            "container": json.loads(captures["jobs_outputs"]["container_definitions"]["value"][name])[0],
            "task": captures["jobs_outputs"]["task_definition_settings"]["value"][name],
        }

    def task(self, request: dict[str, Any], suffix: str = "task_1") -> dict[str, Any]:
        """Create the independently specified described task for a single accepted request."""
        arn = self.target["cluster_arn"].replace(":cluster/", ":task/") + "/" + suffix
        return {
            "task_arn": arn,
            "cluster_arn": request["cluster"],
            "task_definition_arn": request["taskDefinition"],
            "started_by": request["startedBy"],
            "group": "family:" + request["taskDefinition"].rsplit("/", 1)[1].rsplit(":", 1)[0],
            "last_status": "PENDING",
            "tags": [{"key": "Project", "value": self.target["project_tag"]}],
            "containers": [{"name": "job", "image": self.description["container"]["image"]}],
            "security_groups": request["networkConfiguration"]["awsvpcConfiguration"]["securityGroups"],
            "started": False,
        }

    def call(self, operation: str, params: Any, *, deadline: float) -> dict[str, Any]:
        """Return normalized closed-operation results and fail visibly on an expired budget."""
        self.calls.append((operation, copy.deepcopy(dict(params)), deadline))
        if self.clock.monotonic() >= deadline:
            raise TimeoutError("fake_api_deadline")
        if operation == "caller_identity":
            result: dict[str, Any] = {
                "account": self.account,
                "date_utc": format_datetime(datetime.fromtimestamp(self.clock.time() + self.date_offset, UTC), usegmt=True),
            }
        elif operation == "describe_task_definition":
            self.clock.now += self.describe_delay
            result = copy.deepcopy(self.description)
            if self.after_definition is not None:
                self.after_definition()
        elif operation == "run_task":
            state = self.store.read()
            attempts = [attempt for attempt in state["attempts"].values() if attempt.get("execution", {}).get("live_try") == params["startedBy"]]
            if len(attempts) != 1:
                raise ValueError("run_has_no_unique_persisted_try")
            placement = attempts[0]["execution"]["placements"][-1]
            if not placement["dispatch"]["sent"] or placement["dispatch"]["request"] != params or placement["state"] != "unknown":
                raise ValueError("run_preceded_durable_intent")
            self.run_requests.append(copy.deepcopy(params))
            if self.no_effect_error:
                return {"ok": False, "error": {"kind": "service", "code": "AccessDenied", "effect_unknown": False}}
            task = self.task(params)
            self.tasks[task["task_arn"]] = task
            if self.lost_reply:
                return {"ok": False, "error": {"kind": "network", "code": "reply_lost", "effect_unknown": True}}
            result = {"task_arns": [task["task_arn"]], "failures": []}
        elif operation == "list_tasks":
            if "startedBy" in params or set(params) - {"cluster", "family", "desiredStatus", "nextToken"}:
                raise ValueError("illegal_list_filters")
            selected = sorted(arn for arn, row in self.tasks.items() if (row["last_status"] == "STOPPED") == (params["desiredStatus"] == "STOPPED"))
            offset = int(params.get("nextToken", "0"))
            result = {"task_arns": selected[offset : offset + 1]}
            if offset + 1 < len(selected) or self.repeated_token:
                result["next_token"] = str(offset + 1) if not self.repeated_token else "1"
        elif operation == "describe_tasks":
            if params.get("include") != ["TAGS"]:
                raise ValueError("missing_tag_read")
            result = {"tasks": [copy.deepcopy(self.tasks[arn]) for arn in params["tasks"]], "failures": []}
            if self.incomplete:
                result["tasks"] = []
        else:
            raise ValueError("unexpected_fake_operation")
        return {"ok": True, "result": result}


def fixture(folder: Path, *, job_class: str = "offline") -> tuple[Store, FakeApi, ecs_dispatch.Dispatcher, dict[str, Any], dict[str, Any], dict[str, Any]]:
    """Build the real target and launcher render before entering the dispatch facade."""
    store, clock = Store(folder), Clock()
    captures = json.loads((ROOT / "data_contracts/orchestration/examples/ecs_target_inputs.json").read_text())
    for name, raw in captures["jobs_outputs"]["container_definitions"]["value"].items():
        container = json.loads(raw)[0]
        task = captures["jobs_outputs"]["task_definition_settings"]["value"][name]
        subject = {
            "container": {key: value for key, value in container.items() if key not in ("memory", "memoryReservation")},
            "task": {
                "settings": {key: value for key, value in task.items() if key != "volume"},
                "volumes": {
                    volume["name"]: volume for volume in task["volume"] if volume["name"] in {mount["sourceVolume"] for mount in container["mountPoints"]}
                },
            },
        }
        text = (
            json.dumps(subject, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
            .replace("<", "\\u003c")
            .replace(">", "\\u003e")
            .replace("&", "\\u0026")
        )
        captures["jobs_outputs"]["execution_digests"]["value"][name + "/job"] = hashlib.sha256(text.encode()).hexdigest()
    target = build_target(
        *(captures[key] for key in ("storage_outputs", "platform_outputs", "iam_outputs", "jobs_outputs", "image_measurements", "catalog_binding")),
        enabled=True,
    )
    with store.tx() as state:
        attempt = state["attempts"]["synthetic_attempt"]
        ecs_admission.bind(state, attempt, job_class=job_class, registry_kind="rehearsal", run={"id": "20261025T000000Z-01234567", "mode": "real"})
        placement = ecs_target.reserve(
            attempt, ecs_target.Size(1024, 4096, 21), ecs_target.Decision("dispatch", "ecs", "aws_first", "FARGATE_SPOT"), NOW, ecs_target.Usage()
        )
    config = launcher.Config(
        run_lock.Registry(folder / "unused_registry", "test"), ROOT, str(folder / "unused.sock"), folder / "tokens", folder / "runs", allowed_roots=(folder,)
    )
    renderer = launcher.Renderer(config, launcher.ComposeModel(ROOT / "docker-compose.yaml"), ROOT, ROOT, b"synthetic-key")
    inp = launcher.RenderInput(
        "pipeline_validate_gold",
        "synthetic",
        "validate",
        1,
        1,
        "op_synthetic",
        {"JOB_MEMORY_LIMIT": str(4 * 1024**3), "DUCKDB_MEMORY_LIMIT": "1GB", "JOB_THREADS": "1"},
        NOW + 600,
        [],
        {
            "database": str(folder / "staging.duckdb"),
            "build_root": str(folder),
            "suites_host": str(folder),
            "baselines_host": str(folder / "baseline.json"),
            "out_host": str(folder),
        },
        {},
    )
    work = folder / "work"
    work.mkdir()
    inp.params = {
        **inp.params,
        "build_root": str(work),
        "database": str(work / "staging.duckdb"),
        "suites_host": str(work),
        "baselines_host": str(work / "baseline.json"),
        "out_host": str(work),
    }
    rendered = renderer.render(inp, "synthetic_registry")
    job = json.loads((ROOT / "data_contracts/orchestration/examples/ecs_job.json").read_text())
    job.update(
        run_id="20261025T000000Z-01234567", template=inp.template, try_id=placement["try_id"], task_role_id=target["job_roles"]["offline_task"]["unique_id"]
    )
    job = ecs_dispatch.job_from_rendered(job, rendered, store.read()["attempts"]["synthetic_attempt"])
    reference = {
        "key": f"lakehouse/run_stage/{job['run_id']}/g1/tries/{placement['try_id']}/job.json",
        "version_id": "synthetic_version",
        "sha256": hashlib.sha256(ecs_dispatch.canonical(job)).hexdigest(),
        "size": len(ecs_dispatch.canonical(job)),
    }
    api = FakeApi(store, clock, target, captures)
    dispatcher = ecs_dispatch.Dispatcher(api, store.tx, registry_kind="rehearsal", clock=clock)
    return store, api, dispatcher, target, job, reference


def submit(data: tuple[Any, ...]) -> dict[str, Any]:
    """Dispatch the existing persisted live try through the production facade."""
    store, _api, dispatcher, target, job, reference = data
    live = store.read()["attempts"]["synthetic_attempt"]["execution"]["live_try"]
    return dispatcher.submit("synthetic_attempt", live, target=target, definition_name="quality_offline", job=job, job_reference=reference)


class LauncherStore(Store):
    """Expose the actual launcher's registry file only as the fake API's read witness."""

    def __init__(self, path: Path) -> None:
        self.path = path


class SubmissionRuntime:
    """Drive the real canonical facade from the launcher's existing runtime hook."""

    def __init__(self, server: launcher.Launcher, api: FakeApi, target: dict[str, Any]) -> None:
        self.server, self.api, self.target = server, api, target
        self.error: str | None = None
        self.dispatcher = ecs_dispatch.Dispatcher(api, server.tx, registry_kind="test", clock=api.clock)

    def availability(self, attempt: Any, now: float) -> ecs_target.Availability:
        return ecs_target.Availability()

    def size(self, attempt: Any) -> ecs_target.Size:
        return ecs_target.Size(1024, 4096, 21)

    def dispatch(self, attempt: Any, placement: Any) -> None:
        run_id = attempt["owner"].partition(":")[2]
        inp = launcher.RenderInput(
            attempt["template"],
            run_id,
            attempt["launch_id"],
            attempt["n"],
            attempt["generation"],
            "synthetic_operation",
            {"JOB_MEMORY_LIMIT": str(4 * 1024**3), "DUCKDB_MEMORY_LIMIT": "1GB", "JOB_THREADS": "1"},
            attempt["deadline_utc"],
            attempt["args"],
            attempt["params"],
            {},
        )
        rendered = self.server.renderer_of(attempt["template"], ROOT).render(inp, "synthetic_registry")
        job = json.loads((ROOT / "data_contracts/orchestration/examples/ecs_job.json").read_text())
        job.update(
            attempt_id=attempt["id"],
            run_id=run_id,
            generation=attempt["generation"],
            template=attempt["template"],
            try_id=placement["try_id"],
            task_role_id=self.target["job_roles"]["offline_task"]["unique_id"],
        )
        job = ecs_dispatch.job_from_rendered(job, rendered, attempt)
        reference = {
            "key": f"lakehouse/run_stage/{run_id}/g{attempt['generation']}/tries/{placement['try_id']}/job.json",
            "version_id": "synthetic_version",
            "sha256": hashlib.sha256(ecs_dispatch.canonical(job)).hexdigest(),
            "size": len(ecs_dispatch.canonical(job)),
        }
        try:
            self.dispatcher.submit(attempt["id"], placement["try_id"], target=self.target, definition_name="quality_offline", job=job, job_reference=reference)
        except ecs_target.PlacementError as error:
            self.error = str(error)
            raise

    def advance(self, state: Any, attempt: Any, now: float) -> None:
        return None


def submission_case(folder: Path) -> bool:
    """Pass through the real broker, launcher, renderer, dispatcher and broker record."""
    from scripts.orchestration.ecs_submission_cases import NOW as submission_now
    from scripts.orchestration.ecs_submission_cases import submission_environment

    _store, _api, _dispatcher, target, _job, _reference = fixture(folder / "captures")
    payloads = ROOT / "data/e2e"
    payloads.mkdir(parents=True, exist_ok=True)
    caller_root = Path(tempfile.mkdtemp(prefix="dispatch_submission_", dir=payloads))
    (caller_root / "retention.json").write_text(
        json.dumps(
            {
                "class": 3,
                "purpose": "Synthetic real-caller dispatch evidence",
                "created": datetime.now(UTC).isoformat(),
                "end_condition": "Candidate accepted or superseded",
            },
            indent=2,
        )
        + "\n"
    )
    with submission_environment(ROOT, caller_root) as (server, broker, request):
        clock = Clock()
        clock.now = submission_now
        api = FakeApi(
            LauncherStore(server.cfg.registry.state_file),
            clock,
            target,
            json.loads((ROOT / "data_contracts/orchestration/examples/ecs_target_inputs.json").read_text()),
        )
        api.description["task"] = copy.deepcopy(_api.description["task"])
        runtime = SubmissionRuntime(server, api, target)
        server.ecs_runtime = runtime
        server.status, server.docker_ok = "degraded", False
        handle = broker.submit(request)
        server.advance_all()
        outcome = broker.status(handle)
        (folder / "caller_observed.json").write_text(
            json.dumps(
                {
                    "operations": [op for op, _p, _d in api.calls],
                    "error": runtime.error,
                    "execution": outcome.execution,
                    "cloud_ready": server.ecs_ready(),
                    "caller_root": str(caller_root),
                    "attempts": [
                        {key: value.get(key) for key in ("id", "template", "phase", "waiting_on", "reason", "execution")}
                        for value in json.loads(server.cfg.registry.state_file.read_text())["attempts"].values()
                    ],
                },
                indent=2,
            )
            + "\n"
        )
        return (
            len(api.run_requests) == 1
            and outcome.execution is not None
            and outcome.execution["placements"][0]["state"] == "pending"
            and outcome.execution["placements"][0]["dispatch"]["request"] == api.run_requests[0]
        )


def review_cases(folder: Path) -> dict[str, bool]:
    """Exercise the review's retry identity, authority independence and transaction races."""
    checks: dict[str, bool] = {}
    run = {"id": "20261025T000000Z-01234567", "mode": "aws_qualify"}
    authorization = {"qualification": "synthetic_qualification", "layer_activation": None, "publish_approval": None}
    for changed_field, replacement_value in (("template", "pipeline_publish"), ("params", {"step": "commit"})):
        prior = initial_attempt()
        state = {"launcher": {"catalog_authority": authority()}, "attempts": {prior["id"]: prior}}
        ecs_admission.bind(state, prior, job_class="lakehouse_write", registry_kind="rehearsal", run=run, layer="gold", authorization=authorization)
        placed = ecs_target.reserve(
            prior, ecs_target.Size(1024, 4096, 21), ecs_target.Decision("dispatch", "ecs", "aws_first", "FARGATE"), NOW, ecs_target.Usage()
        )
        placed.update(state="running", started=True)
        replacement = initial_attempt()
        replacement["id"] = "replacement_attempt"
        replacement[changed_field] = replacement_value
        try:
            ecs_admission.bind(state, replacement, job_class="lakehouse_write", registry_kind="rehearsal", run=run, layer="gold", authorization=authorization)
        except ecs_target.PlacementError as error:
            checks["writer_changed_" + changed_field + "_refused"] = "writer operation requires reconciliation" in str(error)
        else:
            checks["writer_changed_" + changed_field + "_refused"] = False
    for mode in ("real", "aws_qualify"):
        for reference in ("qualification", "layer_activation"):
            prior = initial_attempt()
            record = authority()
            record["production_layers"]["gold"] = "on"
            state = {"launcher": {"catalog_authority": record}, "attempts": {prior["id"]: prior}}
            writer_run = {"id": "synthetic_run", "mode": mode}
            writer_authorization: dict[str, Any] = {
                "qualification": "synthetic_qualification",
                "layer_activation": "synthetic_activation" if mode == "real" else None,
                "publish_approval": {"run_id": "synthetic_run", "layer": "gold", "epoch": 1, "reference": "synthetic_publish", "action": "commit"}
                if mode == "real"
                else None,
            }
            ecs_admission.bind(
                state, prior, job_class="lakehouse_write", registry_kind="rehearsal", run=writer_run, layer="gold", authorization=writer_authorization
            )
            placed = ecs_target.reserve(
                prior, ecs_target.Size(1024, 4096, 21), ecs_target.Decision("dispatch", "ecs", "aws_first", "FARGATE"), NOW, ecs_target.Usage()
            )
            placed.update(state="running", started=True)
            saved_state = copy.deepcopy(state)
            changed_authorization = copy.deepcopy(writer_authorization)
            changed_authorization[reference] = "synthetic_changed_reference"
            replacement = initial_attempt()
            replacement["id"] = "replacement_attempt"
            saved_execution = copy.deepcopy(replacement["execution"])
            expected_error = (
                "qualification cannot inherit production authorization"
                if mode == "aws_qualify" and reference == "layer_activation"
                else "writer operation requires reconciliation before another attempt"
            )
            case_name = f"writer_changed_{reference}_{mode}_refused"
            try:
                ecs_admission.bind(
                    state,
                    replacement,
                    job_class="lakehouse_write",
                    registry_kind="rehearsal",
                    run=writer_run,
                    layer="gold",
                    authorization=changed_authorization,
                )
            except ecs_target.PlacementError as error:
                checks[case_name] = str(error) == expected_error and state == saved_state and replacement["execution"] == saved_execution
            else:
                checks[case_name] = False
    for job_class in ("offline", "lakehouse_read"):
        attempt = initial_attempt()
        state = {"launcher": {"catalog_authority": authority()}}
        saved_execution = copy.deepcopy(attempt["execution"])
        try:
            ecs_admission.bind(state, attempt, job_class=job_class, registry_kind="rehearsal", run=run, authorization=authorization)
        except ecs_target.PlacementError as error:
            checks[job_class + "_writer_authorization_refused"] = (
                str(error) == "non-write admission cannot carry writer authorization" and attempt["execution"] == saved_execution
            )
        else:
            checks[job_class + "_writer_authorization_refused"] = False
    for job_class in ("offline", "seal"):
        for state_name, launcher_record in (("absent", {}), ("malformed", {"catalog_authority": {"unreadable": "synthetic"}})):
            attempt = initial_attempt()
            state = {"launcher": launcher_record}
            try:
                ecs_admission.bind(state, attempt, job_class=job_class, registry_kind="real", run=run)
                state["launcher"]["catalog_authority"] = {"unreadable": "synthetic_after_bind"}
                ecs_admission.current(state, attempt, registry_kind="real")
                checks[job_class + "_authority_" + state_name] = (
                    attempt["execution"]["catalog_authority"] is None and attempt["execution"]["catalog_epoch"] is None
                )
            except ecs_target.PlacementError:
                checks[job_class + "_authority_" + state_name] = False

    frozen = fixture(folder / "authority_inside_intent", job_class="lakehouse_read")

    def freeze_after_definition() -> None:
        with frozen[0].tx() as state:
            state["launcher"]["catalog_authority"].update(state="refreshing", phase="P3", target=None, frozen=True)

    frozen[1].after_definition = freeze_after_definition
    try:
        submit(frozen)
    except ecs_target.PlacementError as error:
        saved = frozen[0].read()["attempts"]["synthetic_attempt"]["execution"]["placements"][0]
        checks["authority_rechecked_inside_intent"] = (
            "catalog admission is closed" in str(error) and "dispatch" not in saved and not any(op == "run_task" for op, _p, _d in frozen[1].calls)
        )
    else:
        checks["authority_rechecked_inside_intent"] = False

    for boundary in ("enter", "commit"):
        expired = fixture(folder / ("deadline_" + boundary))

        def expire_at_boundary(state: dict[str, Any], at: str = boundary, clock_api: FakeApi = expired[1]) -> None:
            placed = state["attempts"]["synthetic_attempt"]["execution"]["placements"][0]
            if at == "enter" and clock_api.calls and clock_api.calls[-1][0] == "describe_task_definition" or at == "commit" and "dispatch" in placed:
                clock_api.clock.now = NOW + 600

        if boundary == "enter":
            expired[0].on_enter = expire_at_boundary
        else:
            expired[0].on_commit = expire_at_boundary
        try:
            submit(expired)
        except ecs_target.PlacementError as error:
            saved = expired[0].read()["attempts"]["synthetic_attempt"]["execution"]["placements"][0]
            expected_intent = "dispatch" not in saved if boundary == "enter" else saved.get("dispatch_attempted") is True and saved["state"] == "unknown"
            checks["deadline_at_intent_" + boundary] = (
                "ECS operation deadline expired" in str(error) and expected_intent and not any(op == "run_task" for op, _p, _d in expired[1].calls)
            )
        else:
            checks["deadline_at_intent_" + boundary] = False

    refused_run = fixture(folder / "known_service_refusal")
    refused_run[1].no_effect_error = True
    first = submit(refused_run)
    second = submit(refused_run)
    saved_attempt = refused_run[0].read()["attempts"]["synthetic_attempt"]
    checks["known_service_refusal_retains_unknown"] = (
        first["state"] == "unknown"
        and second["state"] == "unknown"
        and len(refused_run[1].run_requests) == 1
        and not refused_run[1].tasks
        and ecs_target.active_usage([saved_attempt]) == ecs_target.Usage(1, 1024, 4096)
        and saved_attempt["execution"]["live_try"] is not None
    )
    return checks


def cases(folder: Path) -> dict[str, bool]:
    """Check observable outgoing requests, durable authority and recovery results."""
    checks: dict[str, bool] = {}
    data = fixture(folder / "canonical")
    store, api, dispatcher, target, job, reference = data
    result = submit(data)
    sent = api.run_requests[0]
    placement = store.read()["attempts"]["synthetic_attempt"]["execution"]["placements"][0]
    checks["canonical_request"] = (
        result["state"] == "pending"
        and set(sent)
        == {
            "cluster",
            "taskDefinition",
            "count",
            "capacityProviderStrategy",
            "networkConfiguration",
            "overrides",
            "startedBy",
            "clientToken",
            "propagateTags",
            "enableECSManagedTags",
            "enableExecuteCommand",
        }
        and set(sent["overrides"]) == {"cpu", "memory", "ephemeralStorage", "containerOverrides"}
        and len(sent["overrides"]["containerOverrides"]) == 1
        and set(sent["overrides"]["containerOverrides"][0]) == {"name", "environment"}
        and all(set(row) == {"name", "value"} for row in sent["overrides"]["containerOverrides"][0]["environment"])
        and sent["taskDefinition"] == target["definitions"]["quality_offline"]["task_definition_arn"]
        and sent["capacityProviderStrategy"] == [{"capacityProvider": "FARGATE_SPOT", "weight": 1}]
        and sent["networkConfiguration"]["awsvpcConfiguration"]
        == {
            "subnets": [target["job_network"]["subnet_id"]],
            "securityGroups": [target["job_network"]["security_group_ids"]["offline"]],
            "assignPublicIp": "DISABLED",
        }
        and {row["name"] for row in sent["overrides"]["containerOverrides"][0]["environment"]} == ENVIRONMENT
        and len(sent["overrides"]["containerOverrides"][0]["environment"]) == 7
        and {row["name"]: row["value"] for row in sent["overrides"]["containerOverrides"][0]["environment"]}
        == {
            "HAI_ATTEMPT_ID": "synthetic_attempt",
            "HAI_TRY_ID": placement["try_id"],
            "HAI_RUN_ID": "20261025T000000Z-01234567",
            "HAI_GENERATION": "1",
            "HAI_STAGE_PREFIX": "lakehouse/run_stage/20261025T000000Z-01234567/g1/",
            "HAI_JOB_SPEC_SHA256": reference["sha256"],
            "HAI_DEADLINE_UTC": str(NOW + 600),
        }
        and (sent["overrides"]["cpu"], sent["overrides"]["memory"], sent["overrides"]["ephemeralStorage"]) == ("1024", "4096", {"sizeInGiB": 21})
        and sent["clientToken"] == hashlib.sha256(("hai-try|" + placement["try_id"]).encode()).hexdigest()[:48]
        and (sent["propagateTags"], sent["enableECSManagedTags"], sent["enableExecuteCommand"], sent["count"]) == ("TASK_DEFINITION", True, False, 1)
        and placement["dispatch"]["job_reference"] == reference
        and job["command"][:3] == ["python", "-m", "scripts.lakehouse.pipeline_quality"]
        and placement["dispatch"]["sent"] is True
        and placement["catalog_authority"] is None
    )
    submit(data)
    checks["repeat_submission_reconciles"] = len(api.run_requests) == 1
    with store.tx() as state:
        state["launcher"]["catalog_authority"].update(state="refreshing", phase="P3", target=None, frozen=True)
    frozen_result = dispatcher.reconcile("synthetic_attempt", placement["try_id"], deadline=100 + 600)
    checks["read_only_reconciliation_during_freeze"] = frozen_result["state"] == "pending" and len(api.run_requests) == 1
    checks["fixed_budget"] = len({deadline for _op, _p, deadline in api.calls}) == 1 and placement["deadline_utc"] == NOW + 600

    def refused(name: str, alter: Any, cause: str, *, read: bool = False) -> None:
        candidate = fixture(folder / name, job_class="lakehouse_read" if read else "offline")
        alter(candidate)
        try:
            submit(candidate)
        except ecs_target.PlacementError as error:
            checks[name] = cause in str(error) and not candidate[1].run_requests
        else:
            checks[name] = False

    def edit_authority(data: tuple[Any, ...], **changes: Any) -> None:
        with data[0].tx() as state:
            state["launcher"]["catalog_authority"].update(changes)

    refused("stale_epoch", lambda d: edit_authority(d, epoch=2), "stale catalog epoch", read=True)
    refused("frozen_catalog", lambda d: edit_authority(d, state="refreshing", phase="P3", target=None, frozen=True), "catalog admission is closed", read=True)
    refused("mac_catalog", lambda d: edit_authority(d, state="mac_writer", phase="P1", target="mac"), "catalog admission is closed", read=True)
    refused("inactive_definition", lambda d: d[1].description.update(status="INACTIVE"), "active definition")
    refused("wrong_revision", lambda d: d[1].description.update(task_definition_arn=d[1].description["task_definition_arn"] + "1"), "fixed revision")
    refused("wrong_image", lambda d: d[1].description["container"].update(image=d[1].description["container"]["image"][:-1] + "c"), "image digest")
    refused("wrong_project_tag", lambda d: d[1].description["task"]["tags"].update(Project="other_project"), "project tag")
    refused("changed_definition_pin", lambda d: d[1].description["container"].update(workingDirectory="/other"), "execution pin")
    refused("mismatched_class", lambda d: d[3]["class_networks"].update(offline="lakehouse"), "class network")
    refused("wrong_definition_class", lambda d: d[4].update(job_class="lakehouse_read"), "job class")
    refused("job_digest_changed", lambda d: d[4]["command"].append("unexpected"), "job reference")
    refused("skewed_sts_date", lambda d: setattr(d[1], "date_offset", 3), "clock skew")
    refused("wrong_sts_account", lambda d: setattr(d[1], "account", "1" * 12), "caller account")
    refused("stale_sts_measurement", lambda d: setattr(d[1], "describe_delay", 61), "clock observation")

    changed = fixture(folder / "persisted_request_changed")
    submit(changed)
    with changed[0].tx() as state:
        placed = state["attempts"]["synthetic_attempt"]["execution"]["placements"][0]
        placed["dispatch"]["request"]["clientToken"] = "f" * 48
    try:
        submit(changed)
    except ecs_target.PlacementError as error:
        checks["persisted_request_changed_refused"] = "dispatch request changed" in str(error)
    else:
        checks["persisted_request_changed_refused"] = False

    lost = fixture(folder / "lost_reply")
    lost[1].lost_reply = True
    submit(lost)
    unknown = lost[0].read()["attempts"]["synthetic_attempt"]["execution"]["placements"][0]
    recovered = submit(lost)
    checks["lost_reply_reconciles_exact_task"] = unknown["state"] == "unknown" and recovered["state"] == "pending" and len(lost[1].run_requests) == 1

    empty = fixture(folder / "empty_after_lost_reply")
    empty[1].lost_reply = True
    submit(empty)
    empty[1].tasks.clear()
    empty_result = submit(empty)
    checks["empty_scan_retains_unknown"] = (
        empty_result["state"] == "unknown"
        and len(empty[1].run_requests) == 1
        and empty[0].read()["attempts"]["synthetic_attempt"]["execution"]["live_try"] is not None
    )
    duplicate = fixture(folder / "duplicate_try")
    submit(duplicate)
    other_task = duplicate[1].task(duplicate[1].run_requests[0], "duplicate_task")
    duplicate[1].tasks[other_task["task_arn"]] = other_task
    try:
        submit(duplicate)
    except ecs_target.PlacementError as error:
        checks["duplicate_try_refused"] = "more than one task" in str(error) and len(duplicate[1].run_requests) == 1
    else:
        checks["duplicate_try_refused"] = False

    paged = fixture(folder / "paged")
    submit(paged)
    live_request = paged[1].run_requests[0]
    for index in range(3):
        noise = paged[1].task(live_request, f"a_noise_{index}")
        noise.update(started_by="another_try", last_status="STOPPED" if index == 2 else "PENDING")
        paged[1].tasks[noise["task_arn"]] = noise
    submit(paged)
    lists = [params for op, params, _deadline in paged[1].calls if op == "list_tasks"]
    checks["paginated_exact_reconciliation"] = (
        any("nextToken" in row for row in lists) and {row["desiredStatus"] for row in lists} == {"RUNNING", "STOPPED"} and len(paged[1].run_requests) == 1
    )
    paged[1].incomplete = True
    try:
        submit(paged)
    except ecs_target.PlacementError as error:
        checks["incomplete_describe_refused"] = "incomplete task describe" in str(error)
    else:
        checks["incomplete_describe_refused"] = False

    for field, value, cause in (
        ("group", "family:wrong", "task identity"),
        ("tags", [], "project tag"),
        ("security_groups", ["sg-wrong"], "task network"),
        ("task_definition_arn", "other_revision", "task identity"),
    ):
        other = fixture(folder / ("task_" + field))
        submit(other)
        next(iter(other[1].tasks.values()))[field] = value
        try:
            submit(other)
        except ecs_target.PlacementError as error:
            checks["task_" + field] = cause in str(error) and len(other[1].run_requests) == 1
        else:
            checks["task_" + field] = False
    repeated = fixture(folder / "repeated_token")
    submit(repeated)
    repeated[1].repeated_token = True
    try:
        submit(repeated)
    except ecs_target.PlacementError as error:
        checks["repeated_page_token_refused"] = "repeated task page" in str(error)
    else:
        checks["repeated_page_token_refused"] = False

    for state_name in ("pending", "unknown", "fenced"):
        state = {"launcher": {"catalog_authority": authority()}}
        attempt = initial_attempt()
        run = {"id": "20261025T000000Z-01234567", "mode": "aws_qualify"}
        authorization = {"qualification": "synthetic_qualification", "layer_activation": None, "publish_approval": None}
        ecs_admission.bind(state, attempt, job_class="lakehouse_write", registry_kind="rehearsal", run=run, layer="gold", authorization=authorization)
        decision = ecs_target.select(attempt, "lakehouse_write", ecs_target.Size(1024, 4096, 21), ecs_target.Availability(), NOW)
        checks["writer_on_demand"] = decision.target == "ecs" and decision.provider == "FARGATE"
        attempt["execution"]["requested_target"] = "docker"
        checks["writer_never_local"] = (
            ecs_target.select(attempt, "lakehouse_write", ecs_target.Size(1024, 4096, 21), ecs_target.Availability(), NOW).action == "refuse"
        )
        attempt["execution"]["requested_target"] = "ecs"
        placed = ecs_target.reserve(attempt, ecs_target.Size(1024, 4096, 21), decision, NOW, ecs_target.Usage())
        placed.update(state=state_name, dispatch_attempted=True)
        if state_name == "fenced":
            placed.update(fenced_at=NOW + 1, no_start=True)
            attempt["execution"]["live_try"] = None
        next_decision = ecs_target.select(attempt, "lakehouse_write", ecs_target.Size(1024, 4096, 21), ecs_target.Availability(), NOW + 2)
        checks["write_no_retry_" + state_name] = next_decision.action != "dispatch"
        if state_name == "fenced":
            try:
                ecs_target.reserve(attempt, ecs_target.Size(1024, 4096, 21), decision, NOW + 2, ecs_target.Usage())
            except ecs_target.PlacementError as error:
                checks["write_reservation_cannot_retry"] = "writer requires reconciliation" in str(error)
            else:
                checks["write_reservation_cannot_retry"] = False
        checks["exact_qualification_persisted"] = placed["writer_authorization"] == authorization and placed["catalog_authority"]["epoch"] == 1
    prior = initial_attempt()
    prior_run = {"id": "20261025T000000Z-01234567", "mode": "aws_qualify"}
    prior_authorization = {"qualification": "synthetic_qualification", "layer_activation": None, "publish_approval": None}
    prior_state = {"launcher": {"catalog_authority": authority()}, "attempts": {prior["id"]: prior}}
    ecs_admission.bind(
        prior_state, prior, job_class="lakehouse_write", registry_kind="rehearsal", run=prior_run, layer="gold", authorization=prior_authorization
    )
    previous = ecs_target.reserve(
        prior, ecs_target.Size(1024, 4096, 21), ecs_target.Decision("dispatch", "ecs", "aws_first", "FARGATE"), NOW, ecs_target.Usage()
    )
    previous.update(state="unknown", dispatch_attempted=True)
    replacement = initial_attempt()
    replacement["id"] = "replacement_attempt"
    try:
        ecs_admission.bind(
            prior_state, replacement, job_class="lakehouse_write", registry_kind="rehearsal", run=prior_run, layer="gold", authorization=prior_authorization
        )
    except ecs_target.PlacementError as error:
        checks["write_no_retry_fresh_attempt"] = "writer operation requires reconciliation" in str(error)
    else:
        checks["write_no_retry_fresh_attempt"] = False

    state = {"launcher": {"catalog_authority": authority()}}
    attempt = initial_attempt()
    try:
        ecs_admission.bind(
            state,
            attempt,
            job_class="lakehouse_write",
            registry_kind="rehearsal",
            run={"id": "synthetic_run", "mode": "real"},
            layer="gold",
            authorization={
                "qualification": "qualification",
                "layer_activation": "activation",
                "publish_approval": {"run_id": "synthetic_run", "layer": "gold", "epoch": 1, "reference": "publish"},
            },
        )
    except ecs_target.PlacementError as error:
        checks["production_layer_off_refused"] = "production layer is off" in str(error)
    else:
        checks["production_layer_off_refused"] = False
    state["launcher"]["catalog_authority"]["production_layers"]["gold"] = "on"
    run = {"id": "synthetic_run", "mode": "real"}
    approval = {"run_id": "synthetic_run", "layer": "gold", "epoch": 1, "reference": "synthetic_publish", "action": "commit"}
    production_authorization: dict[str, Any] = {
        "qualification": "synthetic_qualification",
        "layer_activation": "synthetic_activation",
        "publish_approval": approval,
    }
    approved = initial_attempt()
    ecs_admission.bind(state, approved, job_class="lakehouse_write", registry_kind="rehearsal", run=run, layer="gold", authorization=production_authorization)
    checks["exact_production_authorization_persisted"] = approved["execution"]["writer_authorization"] == production_authorization
    for approval_field, approval_value in (("run_id", "other_run"), ("layer", "silver"), ("epoch", 2)):
        wrong = copy.deepcopy(production_authorization)
        wrong["publish_approval"][approval_field] = approval_value
        try:
            ecs_admission.bind(state, initial_attempt(), job_class="lakehouse_write", registry_kind="rehearsal", run=run, layer="gold", authorization=wrong)
        except ecs_target.PlacementError as error:
            checks["publish_approval_" + approval_field] = "per-run approval does not match" in str(error)
        else:
            checks["publish_approval_" + approval_field] = False
    checks["real_submission_canonical_dispatch"] = submission_case(folder / "real_submission")
    checks.update(review_cases(folder / "review"))
    return checks


def main() -> int:
    """Save the tested source identities and every observed stand-in assertion."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    hashes = {path: hashlib.sha256((ROOT / path).read_bytes()).hexdigest() for path in SOURCES}
    case_root = Path(tempfile.mkdtemp(prefix="cases_", dir=args.output.parent))
    error_record = None
    try:
        checks = cases(case_root)
    except (ecs_target.PlacementError, launcher.LauncherError, OSError, KeyError, TypeError, ValueError) as error:
        checks = {"case_completion": False}
        error_record = {"type": type(error).__name__, "message": str(error)}
    checks["source_snapshot_unchanged"] = hashes == {path: hashlib.sha256((ROOT / path).read_bytes()).hexdigest() for path in SOURCES}
    report = {
        "time": datetime.now(UTC).isoformat(),
        "command": [sys.executable, "-m", "scripts.orchestration.ecs_dispatch_cases", *sys.argv[1:]],
        "boundary": "real target renderer and launcher renderer, durable stand-in registry, fake ECS and STS",
        "source_sha256": hashes,
        "checks": checks,
        "exception": error_record,
        "case_root": str(case_root),
        "environment": {"python": sys.version.split()[0], "jsonschema": version("jsonschema")},
        "failed": [name for name, ok in checks.items() if not ok],
        "cleanup": "done",
        "limits": ["No AWS, Docker, grant issuance or native worker execution.", "Live executor and worker protocol extensions remain lead integration gates."],
    }
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    (case_root / "retention.json").write_text(
        json.dumps(
            {"class": 3, "purpose": "Synthetic dispatch case state", "created": report["time"], "end_condition": "Candidate accepted or superseded"}, indent=2
        )
        + "\n"
    )
    sys.stdout.write(json.dumps({"report": str(args.output), "failed": report["failed"]}) + "\n")
    return int(bool(report["failed"]))


if __name__ == "__main__":
    sys.exit(main())
