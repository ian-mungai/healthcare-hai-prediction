"""Synthetic regressions for training mounts, approval admission and interrupted work."""

from __future__ import annotations

import ast
import hashlib
import json
import os
import tempfile
from dataclasses import replace
from functools import partial
from pathlib import Path, PurePosixPath
from types import SimpleNamespace
from typing import Any
from unittest import mock

from orchestration import code_digest, launcher, training_contract, training_render
from orchestration.interface import SubmissionRequest
from scripts.lakehouse import run_lock
from scripts.orchestration import pipeline_control, transform_fixtures
from scripts.orchestration.broker import Broker, BrokerPaths, load_templates
from scripts.training.lane_b_cases import GOLD, PARAMETERS, ROOT, RUN, Cases, fixture, fixture_git, require
from scripts.training.step_records import write_done


def local_imports(path: Path) -> set[str]:
    """Resolve DAG-side orchestration imports without importing Airflow or host-only modules."""
    found: set[str] = set()
    for node in ast.walk(ast.parse(path.read_bytes())):
        modules: list[str] = []
        if isinstance(node, ast.Import):
            modules = [item.name for item in node.names]
        elif isinstance(node, ast.ImportFrom) and node.module:
            modules = [node.module]
            if node.module == "orchestration":
                modules += [f"orchestration.{item.name}" for item in node.names if (ROOT / "orchestration" / f"{item.name}.py").is_file()]
        for module in modules:
            if module.startswith("orchestration."):
                found.add(module.replace(".", "/") + ".py")
    return found


def dag_mount_cases(cases: Cases, folder: Path) -> None:
    """Check actual transitive imports against every DAG service's effective read-only mounts."""
    required = {"orchestration/__init__.py"}
    pending = set().union(*(local_imports(path) for path in (ROOT / "dags").glob("*.py")))
    while pending:
        name = pending.pop()
        if name in required:
            continue
        required.add(name)
        pending.update(local_imports(ROOT / name) - required)
    model = launcher.ComposeModel(ROOT / "services/orchestration/docker-compose.orchestration.yaml")
    for service, settings in model.services.items():
        volumes = [str(volume).split(":") for volume in settings.get("volumes", [])]
        if not any(parts[0] == "${HAI_ORCH_REPO}/dags" for parts in volumes):
            continue
        mounted = {
            parts[0].removeprefix("${HAI_ORCH_REPO}/")
            for parts in volumes
            if len(parts) == 3 and parts[-1] == "ro" and parts[1] == "/opt/hai/" + parts[0].removeprefix("${HAI_ORCH_REPO}/")
        }
        cases.check(
            f"dag_imports_mounted_{service.replace('-', '_')}",
            partial(require, not required - mounted, f"DAG imports unmounted modules: {sorted(required - mounted)}"),
        )
    manifest = json.loads((ROOT / "config/quality/dependency_runtimes.json").read_bytes())
    cases.check(
        "dag_contract_in_runtime_manifest",
        lambda: require("orchestration/training_contract.py" in manifest["runtimes"]["orchestration"]["code"], "DAG contract lacks its image runtime"),
    )

    def fixture_contains_imports() -> None:
        transform_fixtures.prepare_checkout(folder)
        require(all((folder / name).is_file() for name in required), "DAG fixture omits an imported module")

    cases.check("dag_fixture_contains_imports", fixture_contains_imports)
    cases.check(
        "extract_image_matches_feature_store_build",
        lambda: require(
            launcher.ComposeModel(ROOT / "services/training/docker-compose.training.yaml").image("feature-store") == "hai-feature-store:feast0.66.0",
            "training extraction selects a tag absent from the feature-store build",
        ),
    )


def snapshot_cases(cases: Cases, folder: Path) -> None:
    """Interrupt the real snapshot after its pending record, then retry twice."""
    source, run = folder / "source", folder / RUN
    fixture(source)
    replace_file = os.replace

    def interrupt(before: Any, after: Any) -> None:
        if Path(after) == run / "code":
            raise OSError("synthetic interruption before code publication")
        replace_file(before, after)

    def retry() -> None:
        with mock.patch.object(code_digest.os, "replace", side_effect=interrupt):
            cases.refuses(
                "snapshot_interruption_reaches_pending_record",
                lambda: code_digest.snapshot_training(source, run, RUN, git_reader=fixture_git),
                "synthetic interruption",
            )
        require((run / code_digest.PENDING).is_file() and not (run / "code").exists(), "snapshot did not reach the pending-only crash point")
        first = code_digest.snapshot_training(source, run, RUN, git_reader=fixture_git)
        second = code_digest.snapshot_training(source, run, RUN, git_reader=fixture_git)
        require(first == second and not (run / code_digest.PENDING).exists(), "snapshot retry changed its identity or left the pending record")

    cases.check("snapshot_pending_without_code_retries", retry)


def rendered_cases(cases: Cases, folder: Path) -> None:
    """Render every action and check mount nesting and the exact later-step input boundaries."""
    source, run = folder / "source", folder / RUN
    fixture(source)
    record = code_digest.snapshot_training(source, run, RUN, git_reader=fixture_git)
    (run / "training_parameters.json").write_text(json.dumps(PARAMETERS))
    state = {
        "registry_kind": "test",
        "training": {"dag_id": "training_dag", "parameters": PARAMETERS},
        "training_snapshot": {"phase": "complete", "record_sha256": hashlib.sha256((run / code_digest.RECORD).read_bytes()).hexdigest()},
        "training_mlflow": {"phase": "ready", "name": "isolated-mlflow"},
        "training_publication": {"local_catalog": str(folder / "catalog"), "registry": str(folder / "registry.pb")},
    }
    for step in ("guard", "extract", "version_extract", "fit", "mediation", "hac_validate", "log_run"):
        output = run / "outputs" / step
        output.mkdir(parents=True)
        names = [f"{step}.json"]
        if step == "extract":
            names += [f"{prefix}_{hai}.{suffix}" for hai in training_contract.HAI_TYPES for prefix, suffix in (("extract", "parquet"), ("manifest", "json"))]
            names += ["mediator.parquet", "hac.parquet", "adjacency.parquet"]
        for name in names:
            (output / name).write_text("synthetic step output\n")
        write_done(output, step, record["code_digest"], names)
    inp = launcher.RenderInput(
        "training_job",
        RUN,
        "contract",
        1,
        1,
        "op",
        {"JOB_MEMORY_LIMIT": str(1024**3), "JOB_THREADS": "1"},
        2**31,
        [],
        {},
        {},
        run_folder=run,
        run_mode="fixture",
        pipeline=state,
    )
    specs = {}
    for action in ("guard", "extract", "version_extract", "fit", "mediation", "hac_validate", "log_run", "promotion_check", "promote", "smoke"):
        template = "training_extract" if action == "extract" else "training_mlflow" if action in training_contract.WRITER_ACTIONS else "training_job"
        selected = replace(inp, template=template, params={"action": action, "parameters": PARAMETERS})
        spec = training_render.build(selected, {}, None)
        specs[action] = spec
        targets = [PurePosixPath(target) for _, target, _ in spec.mounts] + [PurePosixPath("/workspace/tmp")]
        parents = [PurePosixPath(target) for _, target, readonly in spec.mounts if readonly]
        cases.check(
            f"{action}_mounts_outside_readonly_roots",
            partial(
                require,
                not any(parent in target.parents for parent in parents for target in targets),
                "a runtime mount is nested under a read-only bind",
            ),
        )
    for action, allowed in (
        ("fit", {"adjacency.parquet"}),
        ("mediation", {"mediator.parquet"}),
        ("hac_validate", {"hac.parquet"}),
        ("log_run", set()),
        ("promotion_check", set()),
    ):
        mounts = specs[action].mounts
        auxiliary = {
            name
            for name in ("adjacency.parquet", "mediator.parquet", "hac.parquet")
            if any(Path(host).name == name or Path(host).is_dir() and any(Path(host).rglob(name)) for host, _, _ in mounts)
        }
        cases.check(
            f"{action}_only_declared_auxiliary_inputs",
            partial(require, auxiliary == allowed, "step received another auxiliary input"),
        )
    for action, allowed in (("mediation", {"extract"}), ("hac_validate", {"extract", "fit"})):
        inputs = {PurePosixPath(target).relative_to("/workspace/in").parts[0] for _, target, _ in specs[action].mounts if target.startswith("/workspace/in/")}
        cases.check(
            f"{action}_only_declared_predecessor_inputs",
            partial(require, inputs == allowed, "step received another predecessor output"),
        )
    cases.check(
        "hac_validation_has_no_predictor_extracts",
        lambda: require(
            not any(Path(host).name.startswith(("extract_HAI_", "manifest_HAI_")) for host, _, _ in specs["hac_validate"].mounts),
            "HAC validation received predictor extracts",
        ),
    )
    for action, prerequisite in (("mediation", "fit"), ("hac_validate", "mediation")):
        path = run / "outputs" / prerequisite / f"{prerequisite}.done.json"
        original = path.read_bytes()
        path.write_text("{}")
        try:
            cases.refuses(
                f"{action}_still_checks_{prerequisite}_completion",
                partial(training_render.build, replace(inp, params={"action": action, "parameters": PARAMETERS}), {}, None),
                "another step or schema",
            )
        finally:
            path.write_bytes(original)


def broker_for(folder: Path) -> Broker:
    """Use the real broker with only synthetic registry and run folders."""
    config = launcher.Config(run_lock.Registry(folder / "registry", "test"), folder, "", folder / "tokens", folder / "runs", allowed_roots=(folder,))
    return Broker(
        BrokerPaths(config.registry, folder / "state"), load_templates(), launcher.LauncherClient(config), repo_root=folder, runs_root=folder / "runs"
    )


def approval_cases(cases: Cases, folder: Path) -> None:
    """Submit real-mode requests with missing and exact approvals, keeping launcher calls synthetic."""
    broker = broker_for(folder)
    request = SubmissionRequest("acquire_run", RUN, "real", 1, "training_dag", "acquire_run", "manual", training=PARAMETERS)
    calls: list[tuple[str, dict[str, Any]]] = []

    def call(method: str, params: dict[str, Any], *arguments: Any, **keywords: Any) -> dict[str, Any]:
        calls.append((method, params))
        if method == "status" and params == {"what": "run", "id": RUN}:
            return {"state": "held"}
        if method == "training_prepare":
            return {"prepared": True}
        raise ValueError(f"unexpected synthetic launcher call: {method}")

    with mock.patch.object(broker, "principal", return_value={"kind": "run", "id": RUN}), mock.patch.object(broker, "call", side_effect=call):
        cases.refuses("real_training_without_approval_refused", lambda: broker.submit(request), "training approval required: approval is missing")
        cases.check(
            "unapproved_training_has_no_intent_or_calls",
            lambda: require(not broker.lines_of(request) and not calls, "unapproved run reached intent or launcher work"),
        )
        unadmitted = replace(request, template="release_rejected", task_id="release_run", run_id=GOLD)
        cases.refuses("unadmitted_training_release_refused", lambda: broker.submit(unadmitted), "training approval required: approval is missing")
        approval = {"run_id": RUN, "dag_id": "training_dag", "scope": "real_training", "approved_on": "2026-10-23"}
        path = broker.paths.state / "approvals/training_dag" / f"{RUN}.json"
        path.parent.mkdir(parents=True)
        path.write_text(json.dumps(approval))
        path.chmod(0o600)

        def admitted() -> None:
            broker.submit(request)
            line = broker.lines_of(request)[-1]
            require(line["event"] == "finished" and line["exit_code"] == 0, "approved training acquire did not complete")
            require(
                ("training_prepare", {"dag_id": "training_dag", "parameters": PARAMETERS}) in calls, "approved acquire did not bind its training parameters"
            )
            require(json.loads((broker.runs_root / RUN / "promotion_approval.json").read_bytes()) == approval, "acquire did not persist its exact approval")

        cases.check("real_training_exact_approval_admitted", admitted)
        path.unlink()
        release = replace(request, template="release_rejected", task_id="release_run")
        with mock.patch.object(broker, "perform_control", side_effect=lambda request, row: broker.finish(request, 0)):
            cases.check("admitted_training_can_release_without_approval", lambda: broker.submit(release))
        changed = replace(release, attempt=2, training={**PARAMETERS, "population": "sensitivity"})
        cases.refuses("training_cleanup_requires_admitted_parameters", lambda: broker.submit(changed), "training approval required: approval is missing")


def mlflow_failure_case(folder: Path, *, late: bool) -> None:
    broker = broker_for(folder)
    request = SubmissionRequest("log_run", RUN, "fixture", 1, "training_dag", "log_run", "manual", training=PARAMETERS)
    clock = [0.0]
    calls: list[str] = []

    def call(method: str, *arguments: Any, **keywords: Any) -> dict[str, Any]:
        calls.append(method)
        require(method == "training_mlflow_up", "a failed startup forwarded a container")
        require(keywords.get("timeout") == 300.0, "MLflow startup did not carry its remaining deadline")
        if late:
            clock[0] = 301.0
            return {"phase": "ready"}
        return {"phase": "failed", "error": "synthetic startup failure"}

    with (
        mock.patch.object(broker, "require_serving"),
        mock.patch.object(broker, "ensure_run", return_value={"kind": "run", "id": RUN}),
        mock.patch.object(broker, "accepted_forward", return_value=None),
        mock.patch.object(broker, "call", side_effect=call),
        mock.patch.object(pipeline_control, "time", SimpleNamespace(monotonic=lambda: clock[0], sleep=lambda seconds: None)),
    ):
        first = broker.submit(request)
        line = broker.lines_of(request)[-1]
        require(line["event"] == "finished" and line["exit_code"] == 125 and line.get("refused") is True, "startup failure has no terminal refusal")
        expected = "deadline" if late else "failed before readiness"
        require("local MLflow startup" in line.get("reason", "") and expected in line["reason"], "startup refusal lost its cause")
        require(broker.submit(request) == first and calls == ["training_mlflow_up"], "retry repeated failed MLflow startup")
    with broker.ledger.locked(RUN):
        require(broker.lines_of(request)[-1] == line, "terminal startup refusal changed after releasing the run lock")


def preparation_cases(cases: Cases, folder: Path) -> None:
    """Queue each training template and require its preparation to inspect the training image, never the root Compose model."""
    config = launcher.Config(run_lock.Registry(folder / "registry", "test"), ROOT, "", folder / "tokens", folder / "runs", allowed_roots=(ROOT,))
    for template in ("training_job", "training_mlflow", "training_extract"):

        def prepared(template: str = template) -> None:
            subject = launcher.Launcher.__new__(launcher.Launcher)
            subject.cfg, subject.main_checkout, subject.key, subject.compose_models = config, ROOT, b"", {}
            subject.docker = mock.NonCallableMock(spec_set=["image"])
            subject.docker.image.return_value = None
            attempt = {"template": template, "checkout": str(ROOT), "params": {}}
            result = subject.prepare_attempt("at", attempt, False, {})
            service = launcher.TEMPLATES[template].service
            require(service is not None, "a training template has no service")
            expected = subject.renderer_of(template, ROOT).image(str(service))
            require(subject.docker.image.call_args_list == [mock.call(expected)], "queued preparation did not inspect the training image")
            require(result["image_error"] == f"image {expected} is not present", "queued preparation lost the missing-image refusal")

        cases.check(f"{template}_preparation_inspects_training_image", prepared)


def run() -> list[dict[str, Any]]:
    """Run fix regressions without a launcher process, engine request or real dataset."""
    cases = Cases([])
    with tempfile.TemporaryDirectory(prefix="hai_training_fix_") as temporary:
        folder = Path(temporary)
        cases.check("dag_mount_case_setup", lambda: dag_mount_cases(cases, folder / "dag_copy"))
        cases.check("snapshot_case_setup", lambda: snapshot_cases(cases, folder / "snapshot"))
        cases.check("rendered_case_setup", lambda: rendered_cases(cases, folder / "render"))
        cases.check("approval_case_setup", lambda: approval_cases(cases, folder / "approval"))
        cases.check("preparation_case_setup", lambda: preparation_cases(cases, folder / "preparation"))
        cases.check("mlflow_failed_start_records_refusal", lambda: mlflow_failure_case(folder / "mlflow_failed", late=False))
        cases.check("mlflow_late_start_records_refusal", lambda: mlflow_failure_case(folder / "mlflow_late", late=True))
    return cases.results
