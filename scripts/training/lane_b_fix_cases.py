"""Synthetic regressions for training mounts, snapshots and preparation."""

from __future__ import annotations

import ast
import hashlib
import json
import os
import tempfile
from collections.abc import Callable
from dataclasses import replace
from functools import partial
from pathlib import Path, PurePosixPath
from typing import Any
from unittest import mock

from orchestration import code_digest, launcher, training_contract, training_render
from scripts.lakehouse import run_lock
from scripts.orchestration import transform_fixtures
from scripts.training.lane_b_cases import PARAMETERS, ROOT, RUN, Cases, fixture, fixture_git, require
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
            launcher.ComposeModel(ROOT / "services/training/docker-compose.training.yaml").image("feature-store") == "hai-feature-store:feast0.67.0",
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

    def interrupt_record(before: Any, after: Any) -> None:
        if Path(after).name == code_digest.RECORD:
            raise OSError("synthetic interruption before the record")
        replace_file(before, after)

    def interrupted(name: str, saved: Callable[[Path, Path], None] | None = None) -> Path:
        """Interrupt the real snapshot after it published its code and before it published its record, then let ``saved`` change the saved tree."""
        target = folder / name
        with mock.patch.object(code_digest.os, "replace", side_effect=interrupt_record):
            try:
                code_digest.snapshot_training(source, target, RUN, git_reader=fixture_git)
            except OSError as error:
                require("before the record" in str(error), "the snapshot stopped for another cause")
        require((target / "code").is_dir() and (target / code_digest.PENDING).is_file() and not (target / code_digest.RECORD).exists(), "not the crash point")
        if saved is not None:
            saved(target / "code", target / code_digest.PENDING)
        return target

    def as_old_writer(code: Path, pending: Path) -> None:
        """Save a file outside the current training roots and list it in the pending record, as an older writer did."""
        (code / "legacy").mkdir()
        (code / "legacy/saved_input.md").write_text("saved input\n")
        (code / "legacy/saved_input.md").chmod(0o444)
        entries = code_digest.file_entries(code, (".",))
        record = json.loads(pending.read_bytes())
        code_digest._write(pending, {**record, "code_digest": code_digest.entries_digest(entries), "files": entries})

    def resumes(target: Path, outside_roots: bool) -> None:
        pending = json.loads((target / code_digest.PENDING).read_bytes())
        record = code_digest.snapshot_training(source, target, RUN, git_reader=fixture_git)
        require(record == pending and (target / code_digest.RECORD).is_file() and not (target / code_digest.PENDING).exists(), "the retry changed the record")
        require(
            any(entry["path"] == "legacy/saved_input.md" for entry in record["files"]) is outside_roots,
            "the saved tree and the record disagree on the saved file",
        )
        require(code_digest.verify_snapshot(target, RUN)["code_digest"] == pending["code_digest"], "the resumed snapshot has another digest")

    cases.check("snapshot_interrupted_after_code_resumes", lambda: resumes(interrupted("after_code"), False))
    cases.check("snapshot_interrupted_with_a_saved_file_outside_the_roots_resumes", lambda: resumes(interrupted("old_outside_roots", as_old_writer), True))

    def changed_file(code: Path, pending: Path) -> None:
        as_old_writer(code, pending)
        (code / "legacy/saved_input.md").chmod(0o644)
        (code / "legacy/saved_input.md").write_text("changed input\n")

    def added_file(code: Path, pending: Path) -> None:
        (code / "scripts/extra.py").write_text("unexpected staged code\n")

    def removed_file(code: Path, pending: Path) -> None:
        (code / "scripts/sample.py").unlink()

    for name, saved in (("changed_saved_file", changed_file), ("added_saved_file", added_file), ("removed_saved_file", removed_file)):
        target = interrupted(name, saved)
        cases.refuses(
            f"snapshot_interrupted_with_{name}_refused",
            partial(code_digest.snapshot_training, source, target, RUN, git_reader=fixture_git),
            "interrupted snapshot does not match its pending record",
        )


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
    for action in ("log_run", "promotion_check"):
        mounted = {PurePosixPath(target).relative_to("/workspace/in").parts[0] for _, target, _ in specs[action].mounts if target.startswith("/workspace/in/")}
        cases.check(
            f"{action}_mounts_the_fit_output_that_the_outcome_check_reads",
            partial(
                require, "fit" in mounted and "fit" in training_contract.INPUT_STEPS[action], "the last action of a model cannot read the saved fit results"
            ),
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


def preparation_cases(cases: Cases, folder: Path) -> None:
    """Queue each training template and require its preparation to inspect the training image, never the root Compose model."""
    config = launcher.Config(run_lock.Registry(folder / "registry", "test"), ROOT, "", folder / "tokens", folder / "runs", allowed_roots=(ROOT,))
    for template in ("training_job", "training_mlflow", "training_extract"):

        def prepared(template: str = template) -> None:
            subject = launcher.Launcher.__new__(launcher.Launcher)
            subject.cfg, subject.main_checkout, subject.key, subject.compose_models = config, ROOT, b"", {}
            subject.docker = mock.NonCallableMock(spec_set=["image"])
            subject.docker.configure_mock(**{"image.return_value": None})
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
        cases.check("preparation_case_setup", lambda: preparation_cases(cases, folder / "preparation"))
    return cases.results
