"""Synthetic E2E cases of the AWS task runner (decision 56): the phases of one training run inside Fargate containers, through ``aws_run.main``."""

from __future__ import annotations

import contextlib
import hashlib
import io
import json
import os
import tarfile
from collections.abc import Iterator, Sequence
from pathlib import Path
from typing import Any
from unittest import mock

from orchestration import code_digest
from scripts.features import training_extract
from scripts.training import guards, promotion_check
from scripts.training.job_action_cases import GUARD, HAI_TYPE, SAVED, Fixture, require, standins
from scripts.training.job_cases import COMMIT, DIGEST
from scripts.training.lane_b_cases import RUN
from scripts.training.step_records import verify_done, write_done

PARAMETERS = {"features_run": "latest", "population": "primary", "hai_types": [HAI_TYPE], "trigger": "manual", "mlflow": "aws"}
TRACKING = "http://10.0.1.20:5100"
BUCKET = "example-project-data-dev"
TRAIN_STEPS = ["version_extract", "fit", "mediation", "hac_validate", "log_run", "promotion_check"]


def call(*arguments: str) -> tuple[int, str, str]:
    """Run the runner in this process and return its exit code, standard output and standard error."""
    from scripts.training import aws_run

    out, err = io.StringIO(), io.StringIO()
    with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
        try:
            code = aws_run.main(list(arguments))
        except SystemExit as stop:
            code = int(stop.code or 0)
    return code, out.getvalue(), err.getvalue()


def lines_of(text: str) -> list[dict[str, Any]]:
    """Parse the one-line JSON records the runner prints."""
    return [json.loads(line) for line in text.splitlines() if line.startswith("{")]


@contextlib.contextmanager
def environment(**changes: str | None) -> Iterator[None]:
    """Give the runner the variables a task definition and a `run-task` override give it, with no ambient AWS value."""
    values = {"DATA_BUCKET_NAME": BUCKET, "MLFLOW_TRACKING_URI": TRACKING, "TRAINING_PARAMETERS": json.dumps(PARAMETERS), "AWS_REGION": "us-west-2"}
    values.update({name: value for name, value in changes.items() if value is not None})
    with mock.patch.dict(os.environ, values):
        for name, value in changes.items():
            if value is None:
                os.environ.pop(name, None)
        yield


def code_archive(folder: Path, commit: str | None = COMMIT) -> tuple[Path, str]:
    """Write a git-archive-like tarball with a file under every declared code root, unpack it as the fetch container does, and return it with its hash."""
    tree = folder / "tree"
    for root in code_digest.TRAINING_ROOTS:
        path = tree / root.replace("*", "sample")
        path = path if path.suffix else path / "sample.py"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("synthetic input\n")
    archive = folder / "code.tar.gz"
    with tarfile.open(archive, "w:gz", format=tarfile.PAX_FORMAT, pax_headers={"comment": commit} if commit else {}) as tar:
        for path in sorted(tree.rglob("*")):
            if path.is_file():
                tar.add(path, arcname=path.relative_to(tree).as_posix())
    with tarfile.open(archive) as tar:
        tar.extractall(folder / "code", filter="data")
    return archive, hashlib.sha256(archive.read_bytes()).hexdigest()


def prepare_cases(cases: Any, folder: Path) -> None:
    """The prepare phase records the code identity and the validated parameters, then runs the guard."""

    def arguments(work: Path) -> tuple[str, ...]:
        return ("prepare", "--run", RUN, "--mode", "real", "--work", str(work))

    def prepared() -> None:
        work = folder / "prepared"
        work.mkdir(parents=True)
        _, sha = code_archive(work)
        with environment(CODE_SHA256=sha), mock.patch.object(guards, "check", return_value=GUARD):
            code, out, error = call(*arguments(work))
        require(code == 0, f"prepare failed: {error}")
        record = json.loads((work / "snapshot_training.json").read_text())
        entries = code_digest.file_entries(work / "code")
        require(
            record["schema_version"] == 1
            and record["run_id"] == RUN
            and record["git_commit"] == COMMIT
            and record["code_digest"] == code_digest.entries_digest(entries),
            f"snapshot record: {record}",
        )
        require(json.loads((work / "training_parameters.json").read_text()) == PARAMETERS, "the parameter file differs from the validated parameters")
        verify_done(work / "in/guard", "guard", record["code_digest"])
        require([item["step"] for item in lines_of(out) if "step" in item] == ["guard"], f"steps: {out}")

    def rerun_is_stable() -> None:
        work = folder / "rerun"
        work.mkdir(parents=True)
        _, sha = code_archive(work)
        with environment(CODE_SHA256=sha), mock.patch.object(guards, "check", return_value=GUARD):
            require(call(*arguments(work))[0] == 0, "the first prepare failed")
            first = {name: (work / name).read_bytes() for name in ("snapshot_training.json", "training_parameters.json", "in/guard/guard.json")}
            code, _, error = call(*arguments(work))
        require(code == 0, f"the second prepare failed: {error}")
        require(first == {name: (work / name).read_bytes() for name in first}, "a second prepare changed a record")

    def refuses(name: str, reason: str, **changes: str | None) -> None:
        def case() -> None:
            work = folder / name
            work.mkdir(parents=True)
            _, sha = code_archive(work, commit=None if name.endswith("no_commit") else COMMIT)
            with (
                environment(CODE_SHA256=sha if name != "prepare_refuses_changed_archive" else "0" * 64, **changes),
                mock.patch.object(guards, "check", return_value=GUARD),
            ):
                code, _, error = call(*arguments(work))
            require(code != 0 and reason in error, f"{code} {error}")
            require(not (work / "snapshot_training.json").exists(), "a refused prepare wrote a snapshot record")

        cases.check(f"aws_run_{name}", case)

    cases.check("aws_run_prepare_records_identity_and_runs_guard", prepared)
    cases.check("aws_run_prepare_rerun_is_stable", rerun_is_stable)
    refuses("prepare_refuses_local_lane", "mlflow=aws", TRAINING_PARAMETERS=json.dumps({**PARAMETERS, "mlflow": "local"}))
    refuses("prepare_refuses_unnamed_parameters", "TRAINING_PARAMETERS", TRAINING_PARAMETERS=None)
    refuses("prepare_refuses_changed_archive", "CODE_SHA256")
    refuses("prepare_refuses_archive_with_no_commit", "commit")


def staged_train(folder: Path) -> Fixture:
    """A fixture of the AWS lane with the guard and the extract complete, the state the train phase starts from."""
    fixture = Fixture(folder)
    (fixture.folder / "training_parameters.json").write_text(json.dumps(PARAMETERS) + "\n")
    (fixture.folder / "in/guard").mkdir(parents=True)
    (fixture.folder / "in/guard/guard.json").write_text(json.dumps(GUARD, sort_keys=True) + "\n")
    for step, directory in (("guard", fixture.folder / "in/guard"), ("extract", fixture.extract)):
        write_done(directory, step, DIGEST, sorted(path.name for path in directory.iterdir() if not path.name.endswith(".done.json")))
    return fixture


def train(fixture: Fixture, **changes: str | None) -> tuple[int, str, str]:
    """Run the train phase of the fixture's run with the job stand-ins and the AWS task environment."""

    def loader(uri: str) -> Any:
        return SAVED[(Path(uri) / "model.bin").read_text()]

    with standins(fixture), environment(**changes), mock.patch.object(promotion_check, "load_pipeline", side_effect=loader):
        os.environ.pop("DVC_REMOTE_URL", None)
        os.environ["DVC_REMOTE_URL"] = str(fixture.folder / "dvc_remote")
        return call("train", "--run", RUN, "--mode", "real", "--work", str(fixture.folder))


def train_cases(cases: Any, folder: Path) -> None:
    """The train phase runs the six actions in order, names the failed step, resumes after a rerun and refuses an unsafe tracking address."""

    def ran() -> None:
        fixture = staged_train(folder / "ran")
        code, out, error = train(fixture)
        require(code == 0, f"train failed: {error}")
        require([item["step"] for item in lines_of(out) if "step" in item] == TRAIN_STEPS, f"steps: {out}")
        for step in TRAIN_STEPS:
            verify_done(fixture.folder / "in" / step, step, DIGEST)
        require(len(fixture.tracking.parents()) == 1, f"parent runs: {len(fixture.tracking.parents())}")
        require(any(path.is_file() for path in (fixture.folder / "dvc_remote").rglob("*")), "nothing reached the DVC remote")

    def resumed() -> None:
        fixture = staged_train(folder / "resumed")
        require(train(fixture)[0] == 0, "the first train failed")
        calls = len(fixture.tracking.calls)
        code, out, error = train(fixture)
        require(code == 0, f"the rerun failed: {error}")
        require(all(item.get("resumed") for item in lines_of(out) if "step" in item), f"the rerun did not resume every step: {out}")
        require(len(fixture.tracking.calls) == calls, "a completed run called the tracking store again")

    def refuses_changed_extract() -> None:
        fixture = staged_train(folder / "changed_extract")
        (fixture.extract / "mediator.parquet").unlink()
        code, out, error = train(fixture)
        require(code != 0 and not out.strip() and "completed step extract" in error, f"{code} {out} {error}")
        require(not (fixture.folder / "in/version_extract").exists() and not fixture.tracking.calls, "an action ran on a changed extract")

    def stops_after_progress() -> None:
        fixture = staged_train(folder / "progress")
        with mock.patch.object(fixture.tracking, "create_experiment", side_effect=RuntimeError("the tracking service is down")):
            code, out, error = train(fixture)
        require(code != 0, "an unreachable tracking service did not stop the run")
        require([item["step"] for item in lines_of(out) if "step" in item] == TRAIN_STEPS[:4], f"steps before the failure: {out}")
        require(lines_of(error)[-1]["step"] == "log_run", f"the failure names another step: {error}")
        require(not (fixture.folder / "in/promotion_check").exists(), "promotion_check ran after log_run failed")

    def refuses_prerequisite() -> None:
        fixture = staged_train(folder / "prerequisite")
        (fixture.extract / "extract.done.json").unlink()
        code, out, error = train(fixture)
        require(code != 0 and "extract" in error and not out.strip(), f"{code} {out} {error}")

    def refuses_uri(name: str, uri: str | None) -> None:
        def case() -> None:
            fixture = staged_train(folder / name)
            code, out, error = train(fixture, MLFLOW_TRACKING_URI=uri)
            require(code != 0 and "MLFLOW_TRACKING_URI" in error and not out.strip(), f"{code} {out} {error}")
            require(not (fixture.folder / "in/version_extract").exists(), "an action ran before the address was checked")

        cases.check(f"aws_run_train_refuses_{name}", case)

    cases.check("aws_run_train_runs_six_actions_in_order", ran)
    cases.check("aws_run_train_rerun_resumes_every_step", resumed)
    cases.check("aws_run_train_refuses_changed_extract_output", refuses_changed_extract)
    cases.check("aws_run_train_stops_after_progress_and_names_the_step", stops_after_progress)
    cases.check("aws_run_train_refuses_missing_prerequisite", refuses_prerequisite)
    refuses_uri("missing_tracking_address", None)
    refuses_uri("loopback_tracking_address", "http://127.0.0.1:5100")
    refuses_uri("named_tracking_address", "http://mlflow:5100")
    refuses_uri("public_tracking_address", "http://203.0.113.9:5100")
    refuses_uri("wrong_tracking_port", "http://10.0.1.20:5000")


def extract_cases(cases: Any, folder: Path) -> None:
    """The extract phase reads the real features namespace and the Feast registry of the data bucket, after the guard."""

    def namespace() -> None:
        fixture = staged_train(folder / "namespace")
        (fixture.extract / "extract.done.json").unlink()
        seen: list[tuple[list[str], dict[str, str]]] = []

        def recorder(argv: Sequence[str] | None = None) -> int:
            seen.append((list(argv or []), dict(os.environ)))
            return 0

        with environment(POLARIS_URI="http://10.0.1.20:8181/api/catalog"), mock.patch.object(training_extract, "main", recorder):
            code, out, error = call("extract", "--run", RUN, "--mode", "real", "--work", str(fixture.folder))
        require(code == 0, f"extract failed: {error}")
        arguments, env = seen[0]
        require(arguments[arguments.index("--run-dir") + 1] == str(fixture.folder / "in/extract"), f"arguments: {arguments}")
        require(env["FEAST_NAMESPACE"] == "features" and env["FEAST_CATALOG_TYPE"] == "rest", f"namespace: {env.get('FEAST_NAMESPACE')}")
        require(env["FEAST_REGISTRY_PATH"] == f"s3://{BUCKET}/lakehouse/feast/registry.pb", f"registry: {env.get('FEAST_REGISTRY_PATH')}")
        require([item["step"] for item in lines_of(out) if "step" in item] == ["extract"], f"steps: {out}")

    def needs_guard() -> None:
        fixture = staged_train(folder / "needs_guard")
        (fixture.folder / "in/guard/guard.done.json").unlink()
        with (
            environment(POLARIS_URI="http://10.0.1.20:8181/api/catalog"),
            mock.patch.object(training_extract, "main", side_effect=AssertionError("ran without the guard")),
        ):
            code, _, error = call("extract", "--run", RUN, "--mode", "real", "--work", str(fixture.folder))
        require(code != 0 and "guard" in error, f"{code} {error}")

    def needs_catalog() -> None:
        fixture = staged_train(folder / "needs_catalog")
        with environment(POLARIS_URI=None), mock.patch.object(training_extract, "main", side_effect=AssertionError("ran without a catalog address")):
            code, _, error = call("extract", "--run", RUN, "--mode", "real", "--work", str(fixture.folder))
        require(code != 0 and "POLARIS_URI" in error, f"{code} {error}")

    cases.check("aws_run_extract_reads_the_real_namespace_and_registry", namespace)
    cases.check("aws_run_extract_needs_the_guard", needs_guard)
    cases.check("aws_run_extract_needs_the_catalog_address", needs_catalog)


def scenarios(cases: Any, folder: Path) -> None:
    """Register every AWS runner case."""
    prepare_cases(cases, folder / "prepare")
    train_cases(cases, folder / "train")
    extract_cases(cases, folder / "extract")
