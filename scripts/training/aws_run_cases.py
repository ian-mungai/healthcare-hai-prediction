"""Synthetic E2E cases of the AWS task runner: the phases of one training run inside Fargate containers, through ``aws_run.main``."""

from __future__ import annotations

import configparser
import contextlib
import hashlib
import importlib
import importlib.util
import io
import json
import os
import re
import shutil
import stat
import sys
import tarfile
import time
import tomllib
from collections.abc import Callable, Iterator, Sequence
from pathlib import Path
from typing import Any
from unittest import mock

from orchestration import code_digest
from scripts.features import publication_reads, training_extract
from scripts.orchestration.ecs_broker_fake import client_error
from scripts.orchestration.ecs_launch_fake import FakeLaunchBucket
from scripts.process import run_command
from scripts.training import guards, mlflow_alias_stand_in, promote_receipt, promotion_check
from scripts.training.job_action_cases import GUARD, HAI_TYPE, REAL, SAVED, Fixture, require, standins
from scripts.training.job_cases import COMMIT, DIGEST
from scripts.training.lane_b_cases import RUN
from scripts.training.mlflow_service_stand_in import CLUSTER, PROFILE, REGION, SERVICE, MlflowService
from scripts.training.step_records import verify_done, write_done

PARAMETERS = {"features_run": "latest", "population": "primary", "hai_types": [HAI_TYPE], "trigger": "manual", "mlflow": "aws"}
TRACKING = "http://10.0.1.20:5100"
BUCKET = "example-project-data-dev"
LOGIN = "-".join(("synthetic", "login", "value", "0123456789"))
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
    values = {
        "DATA_BUCKET_NAME": BUCKET,
        "MLFLOW_TRACKING_URI": TRACKING,
        "MLFLOW_TRACKING_USERNAME": "admin",
        "MLFLOW_TRACKING_PASSWORD": LOGIN,
        "TRAINING_PARAMETERS": json.dumps(PARAMETERS),
        "AWS_REGION": "us-west-2",
    }
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

    def real_tree() -> None:
        work = folder / "real_tree"
        work.mkdir(parents=True)
        archive = work / "code.tar.gz"
        done = run_command("git", ["archive", "--format=tar.gz", f"--output={archive}", "HEAD"], cwd=REAL, timeout=300)
        require(done.returncode == 0, f"git archive failed: {done.stderr}")
        with tarfile.open(archive) as tar:
            commit = str(tar.pax_headers.get("comment", ""))
            tar.extractall(work / "code", filter="data")
        with environment(CODE_SHA256=hashlib.sha256(archive.read_bytes()).hexdigest()):
            code, out, error = call(*arguments(work))
        require(code == 0, f"prepare refused the committed code tree: {error}")
        record = json.loads((work / "snapshot_training.json").read_text())
        require(record["git_commit"] == commit and len(record["files"]) > 100, f"snapshot record: {record['git_commit']} {len(record['files'])}")
        guard = json.loads((work / "in/guard/guard.json").read_text())
        require(guard["git_commit"] == commit and guard["master_seed"] == 42, f"guard record: {guard}")

    cases.check("aws_run_prepare_accepts_the_committed_code_tree_and_runs_the_real_guard", real_tree)
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
        require(LOGIN not in out + error, "the MLflow login reached the output")

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

    def retry_in_a_new_task() -> None:
        first = staged_train(folder / "retry_first")
        code, _, error = train(first)
        require(code == 0, f"the first task failed: {error}")
        runs = len(first.tracking.active())
        second = staged_train(folder / "retry_second")
        second.tracking = first.tracking
        shutil.copytree(first.folder / "dvc_remote", second.folder / "dvc_remote")
        code, out, error = train(second)
        require(code == 0, f"a retry in a new task did not resume: {error}")
        require([item["step"] for item in lines_of(out) if "step" in item] == TRAIN_STEPS, f"steps of the retry: {out}")
        require(
            len(second.tracking.active()) == runs and len(second.tracking.parents()) == 1, f"the retry added runs: {len(second.tracking.active())} of {runs}"
        )

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
    cases.check("aws_run_train_retry_in_a_new_task_resumes_the_logged_run", retry_in_a_new_task)
    cases.check("aws_run_train_refuses_changed_extract_output", refuses_changed_extract)
    cases.check("aws_run_train_stops_after_progress_and_names_the_step", stops_after_progress)
    cases.check("aws_run_train_refuses_missing_prerequisite", refuses_prerequisite)

    def refuses_login(name: str, variable: str) -> None:
        def case() -> None:
            fixture = staged_train(folder / name)
            code, out, error = train(fixture, **{variable: None})
            require(code != 0 and variable in error and not out.strip() and LOGIN not in error, f"{code} {out} {error}")
            require(not (fixture.folder / "in/version_extract").exists(), "an action ran before the login was checked")

        cases.check(f"aws_run_train_refuses_{name}", case)

    refuses_login("missing_mlflow_username", "MLFLOW_TRACKING_USERNAME")
    refuses_login("missing_mlflow_password", "MLFLOW_TRACKING_PASSWORD")
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
        require(env["FEAST_PROFILE"] == "aws", f"profile: {env.get('FEAST_PROFILE')}")
        require(env["FEAST_REGISTRY_PATH"] == f"s3://{BUCKET}/lakehouse/feast/registry.pb", f"registry: {env.get('FEAST_REGISTRY_PATH')}")
        # Feast's offline reader opens its own PyIceberg catalog hai, which reads only these variables: no credential vending and the task's region.
        delegation = env.get("PYICEBERG_CATALOG__HAI__HEADER__X_ICEBERG_ACCESS_DELEGATION")
        require(delegation == "", f"delegation override: {delegation!r}")
        region = env.get("PYICEBERG_CATALOG__HAI__S3__REGION")
        require(region == "us-west-2", f"PyIceberg region: {region!r}")
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

    def reader_environment(**changes: str) -> dict[str, str]:
        values = {
            "FEAST_NAMESPACE": "features",
            "FEAST_CATALOG_TYPE": "rest",
            "FEAST_PROFILE": "aws",
            "POLARIS_URI": "http://10.0.1.20:8181/api/catalog",
            "POLARIS_CLIENT_ID": "reader-id",
            "POLARIS_CLIENT_SECRET": "reader-secret",
            "AWS_REGION": "us-west-2",
            "FEAST_RUN_FOLDER": str(folder / "reader_tmp"),
        }
        return {**values, **changes}

    def aws_reader() -> None:
        exchanged: list[str] = []
        opened: list[Any] = []
        issued = "issued-value"
        environ = reader_environment()

        def exchange(endpoint: str, client: str, secret: str) -> str:
            exchanged.append(endpoint)
            return issued

        def open_catalog(settings: Any, loader: Any = None) -> Any:
            opened.append(settings)
            return object()

        with (
            mock.patch.object(publication_reads, "polaris_token", side_effect=exchange),
            mock.patch.object(publication_reads, "open_catalog", side_effect=open_catalog),
            training_extract.live_reader(REAL, environ, "latest") as reader,
        ):
            require(reader.namespace == "features", f"namespace: {reader.namespace}")
        require(exchanged == ["http://10.0.1.20:8181/api/catalog"], f"the token came from another endpoint: {exchanged}")
        require(
            len(opened) == 1 and opened[0].endpoint == exchanged[0] and opened[0].token == issued and opened[0].catalog_type == "rest",
            f"catalog settings: {opened}",
        )
        require("POLARIS_CLIENT_ID" not in environ and "POLARIS_CLIENT_SECRET" not in environ, "the reader pair stayed in the environment")
        require("PYICEBERG_HOME" not in environ, "the Mac egress proxy settings file was written")

    def local_profile_keeps_the_docker_endpoint() -> None:
        environ = reader_environment(FEAST_PROFILE="local")
        with (
            mock.patch.object(publication_reads, "polaris_token", return_value="token"),
            mock.patch.object(publication_reads, "private_pyiceberg_home"),
            mock.patch.object(publication_reads, "open_catalog", side_effect=AssertionError("a private address opened the Docker-profile catalog")),
        ):
            try:
                with training_extract.live_reader(REAL, environ, "latest"):
                    raise ValueError("the Docker profile accepted a private catalog address")
            except publication_reads.ReadError as error:
                require("POLARIS_URI" in str(error), str(error))

    cases.check("aws_run_extract_reader_uses_the_private_catalog_address", aws_reader)
    cases.check("aws_run_extract_reader_keeps_the_docker_endpoint_for_the_local_profile", local_profile_keeps_the_docker_endpoint)
    cases.check("aws_run_extract_reads_the_real_namespace_and_registry", namespace)
    cases.check("aws_run_extract_needs_the_guard", needs_guard)
    cases.check("aws_run_extract_needs_the_catalog_address", needs_catalog)


FETCH = Path(__file__).resolve().parents[2] / "services/training/fetch_code.py"
START = Path(__file__).resolve().parents[2] / "services/mlflow/start_server.py"


class FakeObjects:
    """A stand-in for the S3 client call the fetch script makes: it serves one local file under one key and counts the downloads."""

    def __init__(self, key: str, source: Path | None) -> None:
        self.key, self.source, self.downloads = key, source, 0

    def download_file(self, Bucket: str, Key: str, Filename: str) -> None:
        if Bucket != BUCKET or Key != self.key or self.source is None:
            raise OSError(f"no such object: {Bucket}/{Key}")
        self.downloads += 1
        Path(Filename).write_bytes(self.source.read_bytes())


DERIVED_RUN = "20261030T000000Z-0d371ed1"


def fetch(work: Path, sha: str, objects: FakeObjects, *, run: str = RUN, tools: Path | None = None) -> tuple[int, str]:
    """Run the fetch script in this process against the stand-in client and return its exit code and standard error."""
    spec = importlib.util.spec_from_file_location("fetch_code", FETCH)
    if spec is None or spec.loader is None:
        raise ValueError("the fetch script is missing")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    err = io.StringIO()
    # RUN_ID is the run's own ID and differs from the code run on purpose: the archive is read from the code run's prefix.
    with environment(CODE_SHA256=sha, CODE_RUN_ID=run, RUN_ID=DERIVED_RUN), contextlib.redirect_stderr(err):
        return int(module.main(objects, work, tools or work / "no_tools")), err.getvalue()


def fetch_cases(cases: Any, folder: Path) -> None:
    """The fetch step downloads the run's code archive, checks its hash and unpacks it once."""
    key = f"lakehouse/run_stage/{RUN}/inputs/code.tar.gz"

    def source(name: str) -> tuple[Path, str]:
        folder.joinpath(name).mkdir(parents=True)
        archive, sha = code_archive(folder / name)
        return archive, sha

    def fetched() -> None:
        archive, sha = source("fetched")
        work = folder / "fetched_work"
        work.mkdir()
        objects = FakeObjects(key, archive)
        code, error = fetch(work, sha, objects)
        require(code == 0 and objects.downloads == 1, f"fetch failed: {code} {error}")
        require(hashlib.sha256((work / "code.tar.gz").read_bytes()).hexdigest() == sha, "the archive on the workspace differs")
        require(code_digest.file_entries(work / "code") == code_digest.file_entries(folder / "fetched/code"), "the unpacked code differs from the archive")

    def rerun() -> None:
        archive, sha = source("rerun")
        work = folder / "rerun_work"
        work.mkdir()
        objects = FakeObjects(key, archive)
        require(fetch(work, sha, objects)[0] == 0, "the first fetch failed")
        code, error = fetch(work, sha, objects)
        require(code == 0 and objects.downloads == 1, f"the second fetch downloaded again or failed: {code} {error}")

    def changed_archive() -> None:
        archive, _ = source("changed")
        work = folder / "changed_work"
        work.mkdir()
        code, error = fetch(work, "0" * 64, FakeObjects(key, archive))
        require(code != 0 and "CODE_SHA256" in error, f"{code} {error}")
        require(not (work / "code").exists() and not (work / "code.tar.gz").exists(), "a refused fetch left code on the workspace")

    def absent_object() -> None:
        work = folder / "absent_work"
        work.mkdir(parents=True)
        code, error = fetch(work, "a" * 64, FakeObjects(key, None))
        require(code != 0 and "code.tar.gz" in error and not (work / "code").exists(), f"{code} {error}")

    def unsafe_run() -> None:
        archive, sha = source("unsafe")
        work = folder / "unsafe_work"
        work.mkdir()
        objects = FakeObjects(key, archive)
        code, error = fetch(work, sha, objects, run="../../other")
        require(code != 0 and "RUN_ID" in error and objects.downloads == 0, f"{code} {error}")

    def traversal() -> None:
        work = folder / "traversal_work"
        work.mkdir(parents=True)
        hostile = folder / "hostile.tar.gz"
        with tarfile.open(hostile, "w:gz") as tar:
            data = io.BytesIO(b"x")
            info = tarfile.TarInfo("../escaped.txt")
            info.size = 1
            tar.addfile(info, data)
        sha = hashlib.sha256(hostile.read_bytes()).hexdigest()
        code, error = fetch(work, sha, FakeObjects(key, hostile))
        require(code != 0 and "unpack" in error and not (folder / "escaped.txt").exists() and not (work.parent / "escaped.txt").exists(), f"{code} {error}")

    def links_dvc() -> None:
        archive, sha = source("linked")
        work = folder / "linked_work"
        work.mkdir()
        tools = folder / "image_dvc"
        (tools / "bin").mkdir(parents=True)
        (tools / "bin/dvc").write_text("#!/bin/sh\n")
        code, error = fetch(work, sha, FakeObjects(key, archive), tools=tools)
        require(code == 0, f"fetch failed: {error}")
        require(
            (work / "code/.tools/dvc/bin/dvc").is_file() and (work / "code/.tools/dvc").resolve() == tools.resolve(),
            "the image's DVC is not where the job looks",
        )
        require(code_digest.file_entries(work / "code") == code_digest.file_entries(folder / "linked/code"), "the tool link changed the code digest")
        code, error = fetch(work, sha, FakeObjects(key, archive), tools=tools)
        require(code == 0, f"the rerun failed: {error}")

    cases.check("aws_fetch_links_the_image_dvc_where_the_job_looks", links_dvc)
    cases.check("aws_fetch_downloads_verifies_and_unpacks", fetched)
    cases.check("aws_fetch_rerun_downloads_nothing", rerun)
    cases.check("aws_fetch_refuses_changed_archive", changed_archive)
    cases.check("aws_fetch_names_an_absent_object", absent_object)
    cases.check("aws_fetch_refuses_a_run_id_with_a_path", unsafe_run)
    cases.check("aws_fetch_refuses_a_path_leaving_the_workspace", traversal)


def start_cases(cases: Any, folder: Path) -> None:
    """The MLflow container starts the server with basic authentication, no default access and the administrator password only in a private file."""
    password = "A" * 8 + "b1-" + "C" * 8
    all_addresses = ".".join(["0"] * 4)
    options = ["--host", all_addresses, "--port", "5100", "--workers", "1", "--serve-artifacts"]

    def start(work: Path, argv: list[str], **changes: str | None) -> tuple[int, str, list[list[str]], dict[str, str]]:
        spec = importlib.util.spec_from_file_location("start_server", START)
        if spec is None or spec.loader is None:
            raise ValueError("the start script is missing")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        environ = {"MLFLOW_ADMIN_PASSWORD": password, "MLFLOW_AUTH_DATABASE_URI": "postgresql+psycopg2://mlflow@127.0.0.1:5432/mlflow_auth", "PATH": "/usr/bin"}
        environ.update({name: value for name, value in changes.items() if value is not None})
        for name, value in changes.items():
            if value is None:
                environ.pop(name, None)
        launched: list[list[str]] = []
        err = io.StringIO()
        with contextlib.redirect_stderr(err):
            code = int(module.main(argv, environ, lambda arguments: launched.append(list(arguments)), work))
        return code, err.getvalue(), launched, environ

    def started() -> None:
        work = folder / "started"
        work.mkdir(parents=True)
        code, error, launched, env = start(work, options)
        require(code == 0 and len(launched) == 1, f"start failed: {code} {error}")
        require(launched[0] == ["server", "--app-name", "basic-auth", *options], f"command: {launched[0]}")
        ini = work / "mlflow_auth.ini"
        require(stat.S_IMODE(ini.stat().st_mode) == 0o600, f"mode of the auth file: {oct(ini.stat().st_mode)}")
        require(env["MLFLOW_AUTH_CONFIG_PATH"] == str(ini) and "MLFLOW_ADMIN_PASSWORD" not in env, "the server process kept the password in its environment")
        parsed = configparser.ConfigParser(interpolation=None)
        parsed.read(ini)
        found = dict(parsed["mlflow"])
        wanted = {
            "default_permission": "NO_PERMISSIONS",
            "database_uri": "postgresql+psycopg2://mlflow@127.0.0.1:5432/mlflow_auth",
            "authorization_function": "mlflow.server.auth:authenticate_request_basic_auth",
            "admin_username": "admin",
            "admin_password": password,
        }
        require(found == wanted, f"auth file: { {key: value for key, value in found.items() if key != 'admin_password'} }")
        key = env.get("MLFLOW_FLASK_SERVER_SECRET_KEY", "")
        require(re.fullmatch(r"[0-9a-f]{64}", key) is not None, "the server process has no random 32-byte CSRF secret key")
        require(key not in ini.read_text() and key not in error, "the CSRF secret key was written to the auth file or the error output")

    def restarted() -> None:
        work = folder / "restarted"
        work.mkdir(parents=True)
        require(start(work, options)[0] == 0, "the first start failed")
        before = (work / "mlflow_auth.ini").read_bytes()
        require(start(work, options)[0] == 0 and (work / "mlflow_auth.ini").read_bytes() == before, "a second start changed the auth file")

    def new_key_each_start() -> None:
        work = folder / "new_key"
        work.mkdir(parents=True)
        preset = "-".join(("preset", "key"))
        keys = {start(work, options, MLFLOW_FLASK_SERVER_SECRET_KEY=preset)[3].get("MLFLOW_FLASK_SERVER_SECRET_KEY") for _ in range(2)}
        require(len(keys) == 2 and preset not in keys, f"each start must make its own key: {len(keys)} distinct, preset kept: {preset in keys}")

    def jobs_off() -> None:
        # MLflow copies the server environment, including the CSRF key, into the job runner it starts while job execution is on (the default).
        # This service runs no server-side MLflow jobs, so every start switches job execution off, whatever the outside value was.
        switch = "MLFLOW_SERVER_ENABLE_JOB_EXECUTION"
        for index, outside in enumerate((None, "true", "false")):
            work = folder / f"jobs_off_{index}"
            work.mkdir(parents=True)
            env = start(work, options, **{switch: outside})[3]
            require(env.get(switch) == "false", f"job execution stays on for an outside value of {outside}: {env.get(switch)}")

    def refuses(name: str, reason: str, argv: list[str] | None = None, **changes: str | None) -> None:
        def case() -> None:
            work = folder / name
            work.mkdir(parents=True)
            code, error, launched, _ = start(work, argv or options, **changes)
            require(code != 0 and reason in error and not launched, f"{code} {error} {launched}")
            require(not (work / "mlflow_auth.ini").exists(), "a refused start wrote the auth file")

        cases.check(f"mlflow_start_{name}", case)

    def lock_holds_auth() -> None:
        # The basic-auth app refuses to import without Flask-WTF, which only the mlflow[auth] extra brings, so the image lock must hold it.
        group = tomllib.loads((START.parents[2] / "pyproject.toml").read_text())["dependency-groups"]["mlflow"]
        require(
            '"basic-auth"' not in START.read_text() or any(entry.startswith("mlflow[auth]==") for entry in group),
            "the mlflow group of pyproject.toml lacks the mlflow[auth] extra for the basic-auth app",
        )

    cases.check("mlflow_start_image_lock_holds_the_basic_auth_packages", lock_holds_auth)
    cases.check("mlflow_start_runs_the_server_with_basic_auth_and_a_private_password_file", started)
    cases.check("mlflow_start_restart_keeps_the_same_file", restarted)
    cases.check("mlflow_start_makes_a_new_csrf_key_at_each_start", new_key_each_start)
    cases.check("mlflow_start_switches_server_job_execution_off", jobs_off)
    refuses("refuses_a_missing_password", "MLFLOW_ADMIN_PASSWORD", MLFLOW_ADMIN_PASSWORD=None)
    refuses("refuses_a_short_password", "MLFLOW_ADMIN_PASSWORD", MLFLOW_ADMIN_PASSWORD="-".join(("short", "1")))
    refuses("refuses_a_password_with_a_percent_sign", "MLFLOW_ADMIN_PASSWORD", MLFLOW_ADMIN_PASSWORD="%".join(("abcdefgh", "(ijklmnop)s")))
    refuses("refuses_a_missing_database_address", "MLFLOW_AUTH_DATABASE_URI", MLFLOW_AUTH_DATABASE_URI=None)
    refuses("refuses_a_command_that_names_its_own_app", "--app-name", argv=[*options, "--app-name", "other"])


def scenarios(cases: Any, folder: Path) -> None:
    """Register every AWS runner case."""
    prepare_cases(cases, folder / "prepare")
    train_cases(cases, folder / "train")
    extract_cases(cases, folder / "extract")
    fetch_cases(cases, folder / "fetch")
    start_cases(cases, folder / "start")
    promote_cases(cases, folder / "promote")


PROMOTION = {"hai_type": HAI_TYPE, "model_version": "2", "promotion_mode": "promote", "mlflow": "aws"}
MODEL = f"hai_model_{HAI_TYPE}"
RUN_B = "20261024T000001Z-5678abcd"
INTENT_KEY = f"promote/{RUN}/intent.json"
ATTEMPT_KEY = f"promote/{RUN}/attempt.json"
LEASE_PREFIX = f"promote/models/{MODEL}/"
REGISTRY_WRITES = ("set_registered_model_alias", "set_model_version_tag")


def registry_fixture(folder: Path) -> Fixture:
    """A registry with version 1 in production after an earlier promotion and version 2 as a passed challenger."""
    fixture = Fixture(folder)
    store = fixture.tracking
    store.models[MODEL] = {"tags": {}, "versions": [], "aliases": {}}
    for tags in (
        {"promotion_verdict": "pass", "promoted_run": "20250101T000000Z-aaaaaaaa"},
        {"promotion_verdict": "pass"},
        {"promotion_verdict": "fail:fairness"},
    ):
        store.create_model_version(MODEL, "runs:/x/model", run_id="x", tags=tags)
    store.set_registered_model_alias(MODEL, "production", "1")
    store.set_registered_model_alias(MODEL, "challenger", "2")
    store.calls.clear()
    return fixture


class ListingBucket(FakeLaunchBucket):
    """The write-once bucket stand-in plus the list call the promotion grant allows: a prefix inside the receipt prefix only, ``page_size`` keys a page."""

    def __init__(self, bucket: str, allowed_prefix: str) -> None:
        super().__init__(bucket, allowed_prefix)
        self.deny_list = False
        self.page_size = 1000

    def list_objects_v2(self, **arguments: Any) -> dict[str, Any]:
        self._enter("list_objects_v2", arguments, str(arguments.get("Prefix", "")))
        if self.deny_list:
            raise client_error("AccessDenied", "list_objects_v2")
        keys = sorted(key for key in self.objects if key.startswith(str(arguments["Prefix"])))
        start = int(arguments.get("ContinuationToken", 0))
        page = keys[start : start + self.page_size]
        reply: dict[str, Any] = {"Contents": [{"Key": key} for key in page], "KeyCount": len(page), "IsTruncated": start + self.page_size < len(keys)}
        if reply["IsTruncated"]:
            reply["NextContinuationToken"] = str(start + self.page_size)
        return reply


def receipt_bucket() -> ListingBucket:
    """A bucket that answers like the train task role: a create-only write, a read and a list under the receipt prefix, and nothing else."""
    return ListingBucket(BUCKET, f"{promote_receipt.PREFIX}/")


@contextlib.contextmanager
def mlflow_restart(service: MlflowService) -> Iterator[None]:
    """Give the owner command the stand-in ECS service in place of Terraform, the AWS CLI and the clock."""
    from scripts.infrastructure import host_session

    clock = host_session.Clock
    names = host_session.Names(CLUSTER, "stand-in-group", "stand-in-provider", "stand-in-project", {"mlflow": SERVICE}, {})
    with (
        mock.patch.object(host_session, "load_configuration", return_value=({"aws_profile": PROFILE, "aws_region": REGION}, None)),
        mock.patch.object(host_session, "load_names", return_value=names),
        mock.patch.object(host_session, "tolerant_runner", service.runner),
        mock.patch.object(host_session, "Clock", side_effect=lambda: clock(lambda: service.now, service.advance)),
    ):
        yield


class MlflowBlocker:
    """A meta-path finder that makes every ``import mlflow`` fail, as in the host environment, and records each attempt."""

    def __init__(self) -> None:
        self.attempts: list[str] = []

    def find_spec(self, name: str, path: Any = None, target: Any = None) -> None:
        if name == "mlflow" or name.startswith("mlflow."):
            self.attempts.append(name)
            raise ModuleNotFoundError(f"No module named {name!r}", name=name)


@contextlib.contextmanager
def without_mlflow() -> Iterator[MlflowBlocker]:
    """Run a block in an interpreter where the mlflow package cannot be imported."""
    blocker, kept = MlflowBlocker(), {name: module for name, module in sys.modules.items() if name == "mlflow" or name.startswith("mlflow.")}
    for name in kept:
        del sys.modules[name]
    sys.meta_path.insert(0, blocker)
    try:
        yield blocker
    finally:
        sys.meta_path.remove(blocker)
        sys.modules.update(kept)


OWNER_LOGIN = "-".join(("synthetic", "owner", "login", "9876543210"))


def alias_lookup(fixture: Fixture) -> Callable[[str, str], str | None]:
    """The version a registered model alias names in the registry stand-in, read without counting as a registry call."""

    def lookup(model: str, alias: str) -> str | None:
        found = fixture.tracking.models.get(model, {"aliases": {}})["aliases"].get(alias)
        return None if found is None else str(found)

    return lookup


def owner_run(
    fixture: Fixture,
    bucket: ListingBucket,
    service: MlflowService,
    arguments: Sequence[str],
    *,
    fault: str = "",
    login: str | None = OWNER_LOGIN,
    timeout: float | None = None,
    lookup: Callable[[str, str], str | None] | None = None,
) -> tuple[int, str, str, list[dict[str, Any]], list[str]]:
    """Run ``promote_owner`` as the owner does, in an interpreter without mlflow.

    The receipt bucket and the MLflow ECS service are the stand-ins. The registry is the local alias endpoint stand-in. ``login`` is the password the command is
    given, and the stand-in accepts ``OWNER_LOGIN`` only. Returns the exit code, standard output and standard error, the alias requests and the failed imports.
    """
    from scripts.training import mlflow_alias_read, promote_owner

    out, err = io.StringIO(), io.StringIO()
    with (
        mlflow_alias_stand_in.AliasServer(lookup or alias_lookup(fixture), "admin", OWNER_LOGIN, lambda model: model in fixture.tracking.models) as server,
        without_mlflow() as blocker,
    ):
        server.fault = fault
        environ = {"DATA_BUCKET_NAME": BUCKET, "MLFLOW_TRACKING_URI": server.uri, "MLFLOW_TRACKING_USERNAME": "admin"}
        if login is not None:
            environ["MLFLOW_TRACKING_PASSWORD"] = login
        with (
            mock.patch.object(promote_receipt, "s3_client", return_value=bucket),
            mock.patch.object(mlflow_alias_read, "TIMEOUT_SECONDS", timeout if timeout is not None else mlflow_alias_read.TIMEOUT_SECONDS),
            mlflow_restart(service),
            contextlib.redirect_stdout(out),
            contextlib.redirect_stderr(err),
        ):
            code = promote_owner.main(list(arguments), environ)
    return code, out.getvalue(), err.getvalue(), server.requests, blocker.attempts


def owner_command(
    fixture: Fixture, bucket: ListingBucket, service: MlflowService, *arguments: str, lookup: Callable[[str, str], str | None] | None = None
) -> tuple[int, str, str]:
    """Run ``promote_owner`` as the owner does and return its exit code, standard output and standard error."""
    return owner_run(fixture, bucket, service, arguments, lookup=lookup)[:3]


def promote_in(
    fixture: Fixture, work: Path, bucket: ListingBucket | None, parameters: Any = PROMOTION, run: str | None = None, **changes: str | None
) -> tuple[int, str, str]:
    """Run the promote phase of one run in a work folder that holds the unpacked code of the run, with the receipt bucket stand-in."""
    work.mkdir(parents=True, exist_ok=True)
    if not (work / "code.tar.gz").exists():
        code_archive(work)
    sha = hashlib.sha256((work / "code.tar.gz").read_bytes()).hexdigest()
    client = bucket if bucket is not None else receipt_bucket()
    with (
        standins(fixture),
        mock.patch.object(promote_receipt, "s3_client", return_value=client),
        environment(CODE_SHA256=sha, TRAINING_PARAMETERS=json.dumps(parameters), **changes),
    ):
        return call("promote", "--run", run or RUN, "--mode", "real", "--work", str(work))


def promote_call(fixture: Fixture, work: Path, parameters: Any = PROMOTION, **changes: str | None) -> tuple[int, str, str]:
    """Run the promote phase with a receipt bucket of its own."""
    return promote_in(fixture, work, None, parameters, **changes)


def promote_cases(cases: Any, folder: Path) -> None:
    """The promote phase moves the production alias for an approved, passed version and refuses everything else without a registry change."""

    def alias(fixture: Fixture) -> str:
        return str(fixture.tracking.models[MODEL]["aliases"]["production"])

    def moved() -> None:
        fixture = registry_fixture(folder / "moved")
        code, out, error = promote_call(fixture, folder / "moved_work")
        require(code == 0, f"promote failed: {error}")
        require(alias(fixture) == "2", "production did not move to version 2")
        require(fixture.tracking.find_version(MODEL, "2")["tags"].get("promoted_run") == RUN, "the version does not record the promotion run")
        record = json.loads((folder / "moved_work/in/promote/promote.json").read_text())
        require(record["previous_version"] == "1" and record["version"] == "2" and record["mode"] == "promote", f"record: {record}")
        require([item["step"] for item in lines_of(out) if "step" in item] == ["promote"], f"steps: {out}")
        require(LOGIN not in out + error, "the MLflow login reached the output")

    def rerun_changes_nothing() -> None:
        fixture = registry_fixture(folder / "again")
        work = folder / "again_work"
        require(promote_call(fixture, work)[0] == 0, "the first promote failed")
        calls = len(fixture.tracking.calls)
        code, out, error = promote_call(fixture, work)
        require(code == 0 and all(item.get("resumed") for item in lines_of(out) if "step" in item), f"the rerun did not resume: {out} {error}")
        require(len(fixture.tracking.calls) == calls, "a completed promotion called the registry again")
        require(promote_call(fixture, folder / "again_new_task")[0] == 0 and alias(fixture) == "2", "a promotion in a new task changed the registry")

    def rolled_back() -> None:
        fixture = registry_fixture(folder / "rollback")
        require(promote_call(fixture, folder / "rollback_first")[0] == 0 and alias(fixture) == "2", "the forward promotion failed")
        code, _, error = promote_call(fixture, folder / "rollback_work", {**PROMOTION, "model_version": "1", "promotion_mode": "rollback"})
        require(code == 0 and alias(fixture) == "1", f"the rollback did not restore version 1: {error}")

    def refuses(name: str, parameters: Any = PROMOTION, **changes: str | None) -> None:
        def case() -> None:
            fixture = registry_fixture(folder / name)
            code, _, error = promote_call(fixture, folder / f"{name}_work", parameters, **changes)
            require(code != 0 and error.strip(), f"{name} did not fail")
            require(alias(fixture) == "1", f"{name} moved production")
            require("set_registered_model_alias" not in fixture.tracking.calls, f"{name} wrote to the registry")

        cases.check(name, case)

    def stores_a_durable_receipt() -> None:
        fixture, bucket = registry_fixture(folder / "durable"), receipt_bucket()
        work = folder / "durable_work"
        code, out, error = promote_in(fixture, work, bucket)
        require(code == 0, f"promote failed: {error}")
        stored = bucket.objects.get(promote_receipt.key_of(RUN))
        require(stored is not None, f"no durable receipt after a successful promote: {sorted(bucket.objects)}")
        require(stored == (work / "in/promote/promote.json").read_bytes(), "the durable receipt differs from the workspace receipt")
        require(json.loads(stored or b"{}")["previous_version"] == "1", "the durable receipt lost the previous version")
        puts = [arguments for operation, arguments in bucket.calls if operation == "put_object"]
        require(len(puts) == 5 and all(item.get("IfNoneMatch") == "*" for item in puts), f"a write is not create-only: {puts}")
        require(any(item.get("step") == "promote" for item in lines_of(out)), "success was not reported")

    def receipt_rerun_is_idempotent() -> None:
        fixture, bucket = registry_fixture(folder / "receipt_again"), receipt_bucket()
        work = folder / "receipt_again_first"
        require(promote_in(fixture, work, bucket)[0] == 0, "the first promote failed")
        first = dict(bucket.objects)
        require(promote_in(fixture, work, bucket)[0] == 0 and bucket.objects == first, "a resumed task changed the durable receipt")
        code, _, error = promote_in(fixture, folder / "receipt_again_new_task", bucket)
        require(code == 0 and bucket.objects == first, f"a rerun in a new task failed or replaced the durable receipt: {error}")
        require(json.loads(first[promote_receipt.key_of(RUN)])["previous_version"] == "1", "the first receipt lost the previous version")

    def receipt_conflict_refused() -> None:
        fixture, bucket = registry_fixture(folder / "receipt_conflict"), receipt_bucket()
        bucket.objects[promote_receipt.key_of(RUN)] = b'{"schema":1,"run_id":"other"}\n'
        code, out, error = promote_in(fixture, folder / "receipt_conflict_work", bucket)
        require(code != 0 and "promotion_conflict" in error, f"{code} {error}")
        require(alias(fixture) == "1" and not writes(fixture), "the registry changed although the receipt of the run was another one")
        require(not any(item.get("step") == "promote" for item in lines_of(out)), "success was reported with a conflicting receipt")

    def failed_upload_fails_the_job() -> None:
        fixture, bucket = registry_fixture(folder / "receipt_down"), receipt_bucket()
        bucket.put_faults["promote.json"] = [client_error("AccessDenied", "put_object")]
        code, out, error = promote_in(fixture, folder / "receipt_down_work", bucket)
        require(code != 0 and "receipt_upload_failed" in error, f"a failed upload did not fail the job: {code} {error}")
        require(not any(item.get("step") == "promote" for item in lines_of(out)), "success was reported after a failed upload")
        require(promote_receipt.key_of(RUN) not in bucket.objects, "a receipt was stored by a failed write")

    def lost_reply_is_adopted() -> None:
        fixture, bucket = registry_fixture(folder / "receipt_lost"), receipt_bucket()
        bucket.write_hook = lambda key: setattr(bucket, "lose_reply_once", key.endswith("promote.json"))
        code, _, error = promote_in(fixture, folder / "receipt_lost_work", bucket)
        require(code != 0 and "receipt_upload_failed" in error, f"a lost reply was not reported: {code} {error}")
        require(promote_in(fixture, folder / "receipt_lost_retry", bucket)[0] == 0, "a retry after a lost reply did not adopt the stored receipt")

    def writes(fixture: Fixture) -> list[str]:
        return [name for name in fixture.tracking.calls if name in REGISTRY_WRITES]

    def stored_receipt(bucket: ListingBucket) -> dict[str, Any]:
        return dict(json.loads(bucket.objects[promote_receipt.key_of(RUN)]))

    def records_the_intent_first() -> None:
        fixture, bucket = registry_fixture(folder / "intent_first"), receipt_bucket()
        seen: list[tuple[str, str]] = []
        bucket.write_hook = lambda key: seen.append((key, alias(fixture)))
        code, _, error = promote_in(fixture, folder / "intent_first_work", bucket)
        require(code == 0, f"promote failed: {error}")
        lease, closing = f"{LEASE_PREFIX}lease-0000000001.json", f"{LEASE_PREFIX}lease-0000000001.done.json"
        require(
            seen == [(lease, "1"), (INTENT_KEY, "1"), (ATTEMPT_KEY, "1"), (promote_receipt.key_of(RUN), "2"), (closing, "2")],
            f"writes and the alias they saw (lease, intent, attempt, receipt, then the lease closes): {seen}",
        )
        intent = json.loads(bucket.objects[INTENT_KEY])
        require(intent["previous_version"] == "1" and intent["version"] == "2" and intent["run_id"] == RUN, f"intent: {intent}")

    def interrupted_after_the_move(name: str, champion: bool) -> None:
        def case() -> None:
            fixture, bucket = registry_fixture(folder / name), receipt_bucket()
            if not champion:
                del fixture.tracking.models[MODEL]["aliases"]["production"]
            bucket.put_faults["promote.json"] = [client_error("AccessDenied", "put_object")]
            code, _, error = promote_in(fixture, folder / f"{name}_first", bucket)
            require(code != 0 and "receipt_upload_failed" in error and alias(fixture) == "2", f"the first task did not stop after the move: {code} {error}")
            code, _, error = promote_in(fixture, folder / f"{name}_retry", bucket)
            require(code == 0, f"the retry in a new task failed: {error}")
            require(
                stored_receipt(bucket)["previous_version"] == ("1" if champion else None),
                f"the receipt lost the original predecessor: {stored_receipt(bucket)}",
            )
            require(
                json.loads((folder / f"{name}_retry/in/promote/promote.json").read_text()) == stored_receipt(bucket),
                "the workspace receipt differs from the stored one",
            )
            require(alias(fixture) == "2", "the retry changed production")

        cases.check(name, case)

    def stale_retry_moves_nothing(name: str, later: str) -> None:
        def case() -> None:
            fixture, bucket = registry_fixture(folder / name), receipt_bucket()
            require(promote_in(fixture, folder / f"{name}_first", bucket)[0] == 0, "the first promote failed")
            fixture.tracking.set_registered_model_alias(MODEL, "production", later)
            fixture.tracking.calls.clear()
            first = dict(bucket.objects)
            for attempt in ("one", "two"):
                code, _, error = promote_in(fixture, folder / f"{name}_{attempt}", bucket)
                require(code == 0, f"the retry {attempt} failed: {error}")
                require(alias(fixture) == later and not writes(fixture), f"the retry {attempt} wrote to the registry: {alias(fixture)} {writes(fixture)}")
                require(bucket.objects == first, f"the retry {attempt} changed the durable records")
                require(
                    json.loads((folder / f"{name}_{attempt}/in/promote/promote.json").read_text()) == stored_receipt(bucket),
                    "the retry returned another receipt",
                )

        cases.check(name, case)

    def changed_request_is_refused() -> None:
        fixture, bucket = registry_fixture(folder / "collision"), receipt_bucket()
        require(promote_in(fixture, folder / "collision_first", bucket)[0] == 0, "the first promote failed")
        fixture.tracking.calls.clear()
        first = dict(bucket.objects)
        code, out, error = promote_in(fixture, folder / "collision_work", bucket, {**PROMOTION, "model_version": "1", "promotion_mode": "rollback"})
        require(code != 0 and "promotion_conflict" in error, f"a changed request was not refused: {code} {error}")
        require(alias(fixture) == "2" and not writes(fixture) and bucket.objects == first, "a refused request changed the registry or the records")
        require(not any(item.get("step") == "promote" for item in lines_of(out)), "success was reported for a conflicting request")

    def intent_failure_stops_the_move() -> None:
        fixture, bucket = registry_fixture(folder / "intent_down"), receipt_bucket()
        bucket.put_faults["intent.json"] = [client_error("AccessDenied", "put_object")]
        code, out, error = promote_in(fixture, folder / "intent_down_work", bucket)
        require(code != 0 and "promotion_intent_failed" in error, f"a failed intent write did not fail the job: {code} {error}")
        require(alias(fixture) == "1" and not writes(fixture), "the alias moved without a stored intent")
        require(sorted(bucket.objects) == [f"{LEASE_PREFIX}lease-0000000001.json"], f"records after a failed intent: {sorted(bucket.objects)}")
        require(not any(item.get("step") == "promote" for item in lines_of(out)), "success was reported without an intent")
        require(promote_in(fixture, folder / "intent_down_retry", bucket)[0] == 0 and stored_receipt(bucket)["previous_version"] == "1", "the retry failed")

    def retry_does_not_send_the_alias_request_again() -> None:
        fixture, bucket = registry_fixture(folder / "before_move"), receipt_bucket()
        fixture.tracking.fail_on = "set_registered_model_alias"
        code, _, error = promote_in(fixture, folder / "before_move_first", bucket)
        require(code != 0 and alias(fixture) == "1", f"the first task did not stop at the alias request: {code} {error}")
        require(INTENT_KEY in bucket.objects and ATTEMPT_KEY in bucket.objects, f"the intent and the attempt were not stored first: {sorted(bucket.objects)}")
        fixture.tracking.fail_on = None
        fixture.tracking.calls.clear()
        before = dict(bucket.objects)
        for attempt in ("one", "two"):
            code, out, error = promote_in(fixture, folder / f"before_move_retry_{attempt}", bucket)
            require(
                code != 0 and "promotion_needs_resolution" in error and "promote_owner resolve" in error,
                f"the retry {attempt} did not ask for the owner: {code} {error}",
            )
            require(alias(fixture) == "1" and not writes(fixture), f"the retry {attempt} wrote to the registry: {alias(fixture)} {writes(fixture)}")
            require(bucket.objects == before, f"the retry {attempt} changed the records: {sorted(bucket.objects)}")
            require(not any(item.get("step") == "promote" for item in lines_of(out)), f"success was reported by the retry {attempt}")
        code, _, error = run_b(fixture, folder / "before_move_b", bucket, "1", "rollback")
        require(code != 0 and "promotion_busy" in error and not writes(fixture), f"run B was not refused while run A needs the owner: {code} {error}")

    def unknown_completion_fails_closed() -> None:
        fixture, bucket = registry_fixture(folder / "cannot_tell"), receipt_bucket()
        fixture.tracking.fail_on = "set_registered_model_alias"
        require(promote_in(fixture, folder / "cannot_tell_first", bucket)[0] != 0, "the first task did not stop before the move")
        fixture.tracking.fail_on = None
        fixture.tracking.calls.clear()
        bucket.deny_list = True
        code, _, error = promote_in(fixture, folder / "cannot_tell_retry", bucket)
        require(code != 0 and "promotion_state_unknown" in error, f"records that cannot be listed were not refused: {code} {error}")
        require(alias(fixture) == "1" and not writes(fixture), "the retry moved production without knowing whether the run had completed")
        bucket.deny_list = False
        bucket.absent_reads_denied = True
        code, _, error = promote_in(fixture, folder / "cannot_tell_again", bucket)
        require(
            code != 0 and "promotion_needs_resolution" in error and "promotion_state_unknown" not in error, f"a retry needed an absent read: {code} {error}"
        )
        require(alias(fixture) == "1" and not writes(fixture), "the retry sent the alias request again")

    def first_promotion_needs_no_list_grant() -> None:
        fixture, bucket = registry_fixture(folder / "no_list"), receipt_bucket()
        bucket.absent_reads_denied = True
        code, _, error = promote_in(fixture, folder / "no_list_work", bucket)
        require(
            code == 0 and alias(fixture) == "2" and stored_receipt(bucket)["previous_version"] == "1",
            f"a first promotion needed an absent read: {code} {error}",
        )

    def intent_lost_reply_is_adopted() -> None:
        fixture, bucket = registry_fixture(folder / "intent_lost"), receipt_bucket()
        bucket.write_hook = lambda key: setattr(bucket, "lose_reply_once", key.endswith("intent.json"))
        code, _, error = promote_in(fixture, folder / "intent_lost_work", bucket)
        require(code == 0 and alias(fixture) == "2", f"a lost intent reply was not adopted: {code} {error}")
        require(
            writes(fixture).count("set_registered_model_alias") == 1 and stored_receipt(bucket)["previous_version"] == "1",
            "the promotion was repeated or lost its predecessor",
        )

    def allow_three(fixture: Fixture) -> None:
        fixture.tracking.find_version(MODEL, "3")["tags"]["promotion_verdict"] = "pass"

    def run_b(fixture: Fixture, work: Path, bucket: ListingBucket, version: str = "3", mode: str = "promote") -> tuple[int, str, str]:
        return promote_in(fixture, work, bucket, {**PROMOTION, "model_version": version, "promotion_mode": mode}, run=RUN_B)

    def receipt_of(bucket: ListingBucket, run: str) -> dict[str, Any]:
        return dict(json.loads(bucket.objects[promote_receipt.key_of(run)]))

    def lease_keys(bucket: ListingBucket) -> list[str]:
        return sorted(key.removeprefix(LEASE_PREFIX) for key in bucket.objects if key.startswith(LEASE_PREFIX))

    def lease_closed(generation: int) -> str:
        return f"lease-{generation:010d}.done.json"

    def first_write_runs(bucket: ListingBucket, other: Callable[[], None]) -> None:
        """Run ``other`` once, at the start of the first write of the run under test: the point where a delayed run is overtaken."""
        fired: list[bool] = []

        def hook(_: str) -> None:
            if not fired:
                fired.append(True)
                other()

        bucket.write_hook = hook

    def rolled_back_run_cannot_repeat_the_move() -> None:
        fixture, bucket = registry_fixture(folder / "overtaken"), receipt_bucket()
        bucket.put_faults["promote.json"] = [client_error("AccessDenied", "put_object")]
        code, _, error = promote_in(fixture, folder / "overtaken_a", bucket)
        require(code != 0 and "receipt_upload_failed" in error and alias(fixture) == "2", f"run A did not stop after the move: {code} {error}")
        fixture.tracking.calls.clear()
        second, _, refusal = run_b(fixture, folder / "overtaken_b", bucket, "1", "rollback")
        blocked = alias(fixture) == "2" and not writes(fixture)
        fixture.tracking.calls.clear()
        code, _, error = promote_in(fixture, folder / "overtaken_a_retry", bucket)
        require(
            "set_registered_model_alias" not in writes(fixture),
            f"run A moved production again after run B restored its predecessor: now {alias(fixture)} (run B: {second} {refusal[:80]})",
        )
        require(code == 0 and alias(fixture) == "2", f"run A did not finish: {code} {error}")
        require(receipt_of(bucket, RUN)["previous_version"] == "1", "run A lost its predecessor")
        require(second != 0 and "promotion_busy" in refusal and RUN in refusal, f"a second run was not refused while run A was unresolved: {second} {refusal}")
        require(blocked and not any(key.startswith(f"promote/{RUN_B}/") for key in bucket.objects), "the refused run changed the registry or stored records")
        code, _, error = run_b(fixture, folder / "overtaken_b_again", bucket, "1", "rollback")
        require(code == 0 and alias(fixture) == "1", f"the rollback did not run once the lease was closed: {code} {error}")
        require(receipt_of(bucket, RUN_B)["previous_version"] == "2", "the rollback receipt does not show the version it replaced")
        require(
            lease_keys(bucket) == ["lease-0000000001.done.json", "lease-0000000001.json", "lease-0000000002.done.json", "lease-0000000002.json"],
            f"{lease_keys(bucket)}",
        )

    def run_that_finishes_first_is_not_overwritten() -> None:
        fixture, bucket = registry_fixture(folder / "overlap"), receipt_bucket()
        allow_three(fixture)
        outcome: list[tuple[int, str]] = []

        def other() -> None:
            code, _, error = run_b(fixture, folder / "overlap_b", bucket)
            outcome.append((code, error))

        first_write_runs(bucket, other)
        code, _, error = promote_in(fixture, folder / "overlap_a", bucket)
        require(outcome == [(0, "")] and receipt_of(bucket, RUN_B)["previous_version"] == "1", f"run B did not finish first: {outcome}")
        require(code == 0 and alias(fixture) == "2", f"the delayed run A failed or did not move production: {code} {error}")
        require(receipt_of(bucket, RUN)["previous_version"] == "3", f"the delayed run published a predecessor it never replaced: {receipt_of(bucket, RUN)}")
        require(lease_keys(bucket)[-2:] == ["lease-0000000002.done.json", "lease-0000000002.json"], f"{lease_keys(bucket)}")

    def run_that_is_unresolved_blocks_the_delayed_run() -> None:
        fixture, bucket = registry_fixture(folder / "overlap_held"), receipt_bucket()
        allow_three(fixture)
        bucket.put_faults["promote.json"] = [client_error("AccessDenied", "put_object")]
        after: list[tuple[str, list[str]]] = []

        def other() -> None:
            require(run_b(fixture, folder / "overlap_held_b", bucket)[0] != 0, "run B should have stopped after its move")
            after.append((alias(fixture), writes(fixture)))

        first_write_runs(bucket, other)
        code, _, error = promote_in(fixture, folder / "overlap_held_a", bucket)
        require(after == [("3", ["set_registered_model_alias", "set_model_version_tag"])], f"run B did not stop after its move: {after}")
        require(code != 0 and "promotion_busy" in error and RUN_B in error, f"the delayed run was not refused: {code} {error}")
        require(alias(fixture) == "3" and writes(fixture) == after[0][1], f"the delayed run changed the registry: {alias(fixture)} {writes(fixture)}")
        require(not any(key.startswith(f"promote/{RUN}/") for key in bucket.objects), "the refused run stored records")
        require(
            run_b(fixture, folder / "overlap_held_b_retry", bucket)[0] == 0 and receipt_of(bucket, RUN_B)["previous_version"] == "1", "run B did not finish"
        )
        code, _, error = promote_in(fixture, folder / "overlap_held_a_retry", bucket)
        require(code == 0 and alias(fixture) == "2" and receipt_of(bucket, RUN)["previous_version"] == "3", f"run A did not follow run B: {code} {error}")

    def lease_survives_a_failure_before_the_intent() -> None:
        fixture, bucket = registry_fixture(folder / "lease_kept"), receipt_bucket()
        bucket.put_faults["intent.json"] = [client_error("AccessDenied", "put_object")]
        require(promote_in(fixture, folder / "lease_kept_a", bucket)[0] != 0, "run A did not stop at its intent")
        fixture.tracking.calls.clear()
        code, _, error = run_b(fixture, folder / "lease_kept_b", bucket, "1", "rollback")
        require(code != 0 and "promotion_busy" in error and alias(fixture) == "1" and not writes(fixture), f"run B was not refused: {code} {error}")
        require(promote_in(fixture, folder / "lease_kept_a_retry", bucket)[0] == 0 and alias(fixture) == "2", "run A did not resume its lease")
        require(lease_keys(bucket) == ["lease-0000000001.done.json", "lease-0000000001.json"], f"{lease_keys(bucket)}")

    def lease_lost_reply_is_resumed() -> None:
        fixture, bucket = registry_fixture(folder / "lease_lost"), receipt_bucket()
        bucket.write_hook = lambda key: setattr(bucket, "lose_reply_once", key.endswith("lease-0000000001.json"))
        code, _, error = promote_in(fixture, folder / "lease_lost_work", bucket)
        require(code == 0 and alias(fixture) == "2", f"a lease whose reply was lost was not resumed: {code} {error}")
        require(lease_keys(bucket) == ["lease-0000000001.done.json", "lease-0000000001.json"], f"{lease_keys(bucket)}")

    def lease_that_cannot_be_read_stops_the_run() -> None:
        fixture, bucket = registry_fixture(folder / "lease_down"), receipt_bucket()
        bucket.deny_list = True
        code, out, error = promote_in(fixture, folder / "lease_down_work", bucket)
        require(code != 0 and "promotion_state_unknown" in error and alias(fixture) == "1" and not writes(fixture), f"{code} {error}")
        require(not bucket.objects and not any(item.get("step") == "promote" for item in lines_of(out)), "records were stored without a readable lease")
        bucket.deny_list = False
        bucket.objects[f"{LEASE_PREFIX}lease-0000000001.json"] = promote_receipt.canonical({"schema": 1, "model": MODEL, "generation": 1, "run_id": RUN_B})
        bucket.deny_keys.add(f"{LEASE_PREFIX}lease-0000000001.json")
        code, _, error = promote_in(fixture, folder / "lease_down_held", bucket)
        require(code != 0 and "promotion_lease_failed" in error and alias(fixture) == "1" and not writes(fixture), f"{code} {error}")

    def lease_generations_span_listing_pages() -> None:
        def seeded(name: str, held: bool) -> tuple[Fixture, ListingBucket]:
            fixture, bucket = registry_fixture(folder / name), receipt_bucket()
            bucket.page_size = 2
            for generation in range(1, 5 + held):
                holder = canonical_lease(generation, RUN_B)
                bucket.objects[f"{LEASE_PREFIX}lease-{generation:010d}.json"] = holder
                if generation < 5:
                    bucket.objects[f"{LEASE_PREFIX}lease-{generation:010d}.done.json"] = holder
            return fixture, bucket

        def canonical_lease(generation: int, run: str) -> bytes:
            return promote_receipt.canonical({"schema": 1, "model": MODEL, "generation": generation, "run_id": run})

        fixture, bucket = seeded("pages_free", False)
        code, _, error = promote_in(fixture, folder / "pages_free_work", bucket)
        require(code == 0 and alias(fixture) == "2", f"{code} {error}")
        require(lease_keys(bucket)[-2:] == ["lease-0000000005.done.json", "lease-0000000005.json"], f"{lease_keys(bucket)}")
        fixture, bucket = seeded("pages_held", True)
        code, _, error = promote_in(fixture, folder / "pages_held_work", bucket)
        require(code != 0 and "promotion_busy" in error and alias(fixture) == "1" and not writes(fixture), f"{code} {error}")

    def release_failure_keeps_the_lease_until_a_retry() -> None:
        fixture, bucket = registry_fixture(folder / "release"), receipt_bucket()
        bucket.put_faults[lease_closed(1)] = [client_error("AccessDenied", "put_object")]
        code, out, error = promote_in(fixture, folder / "release_a", bucket)
        require(code != 0 and "lease_release_failed" in error and alias(fixture) == "2", f"{code} {error}")
        require(promote_receipt.key_of(RUN) in bucket.objects, "the receipt was not stored before the lease was closed")
        require(not any(item.get("step") == "promote" for item in lines_of(out)), "success was reported with the lease open")
        fixture.tracking.calls.clear()
        code, _, error = run_b(fixture, folder / "release_b", bucket, "1", "rollback")
        require(code != 0 and "promotion_busy" in error and not writes(fixture), f"the lease was not held until the retry: {code} {error}")
        code, _, error = promote_in(fixture, folder / "release_a_retry", bucket)
        require(code == 0 and not writes(fixture) and lease_closed(1) in lease_keys(bucket), f"the retry did not close the lease: {code} {error}")
        require(
            run_b(fixture, folder / "release_b_again", bucket, "1", "rollback")[0] == 0 and alias(fixture) == "1", "the rollback did not run after the close"
        )

    def completed_run_returns_its_receipt_while_another_holds_the_lease() -> None:
        fixture, bucket = registry_fixture(folder / "completed_busy"), receipt_bucket()
        require(promote_in(fixture, folder / "completed_busy_a", bucket)[0] == 0, "run A failed")
        bucket.put_faults["promote.json"] = [client_error("AccessDenied", "put_object")]
        require(run_b(fixture, folder / "completed_busy_b", bucket, "1", "rollback")[0] != 0 and alias(fixture) == "1", "run B did not stop after its move")
        fixture.tracking.calls.clear()
        before = dict(bucket.objects)
        code, _, error = promote_in(fixture, folder / "completed_busy_a_retry", bucket)
        require(code == 0 and alias(fixture) == "1" and not writes(fixture) and bucket.objects == before, f"{code} {error}")

    def owner_resolution_closes_a_stuck_lease_and_fences_the_run() -> None:
        fixture, bucket = registry_fixture(folder / "resolve"), receipt_bucket()
        bucket.put_faults["promote.json"] = [client_error("AccessDenied", "put_object")]
        require(promote_in(fixture, folder / "resolve_a", bucket)[0] != 0 and alias(fixture) == "2", "run A did not stop after the move")
        service = MlflowService(fixture.tracking)

        def lease_command(*arguments: str) -> tuple[int, str, str]:
            return owner_command(fixture, bucket, service, *arguments)

        resolve = ("resolve", "--model", MODEL, "--owner", "owner", "--reason", "the task was lost")
        code, out, error = lease_command("show", "--model", MODEL)
        shown = json.loads(out)["held"]
        require(code == 0 and shown["run_id"] == RUN and shown["intent"]["previous_version"] == "1", f"show: {code} {out} {error}")
        code, _, error = lease_command(*resolve, "--run", RUN_B)
        require(code == 2 and RUN in error and not any(key.endswith("abandoned.json") for key in bucket.objects), f"another run was resolved: {code} {error}")
        require(service.restarts == 0, "MLflow was restarted for a refused resolution")
        bucket.put_faults[lease_closed(1)] = [client_error("AccessDenied", "put_object")]
        code, _, error = lease_command(*resolve, "--run", RUN)
        require(code == 2 and promote_receipt.key_of(RUN, "abandoned.json") in bucket.objects, f"a failed close was not reported: {code} {error}")
        fixture.tracking.calls.clear()
        code, _, error = promote_in(fixture, folder / "resolve_a_fenced", bucket)
        require(code != 0 and "promotion_abandoned" in error and not writes(fixture), f"the fenced run started: {code} {error}")
        code, _, error = run_b(fixture, folder / "resolve_b_early", bucket, "1", "rollback")
        require(code != 0 and "promotion_busy" in error, f"the lease was released before its close: {code} {error}")
        code, out, error = lease_command(*resolve, "--run", RUN)
        require(code == 0 and "abandoned" in out and lease_closed(1) in lease_keys(bucket), f"the repeat did not close the lease: {code} {out} {error}")
        require(service.restarts == 2, f"each resolution that passed its checks restarts MLflow once: {service.restarts}")
        code, _, error = lease_command(*resolve, "--run", RUN)
        require(code == 2 and "not held" in error and service.restarts == 2, f"a free model was resolved: {code} {error}")
        code, _, error = run_b(fixture, folder / "resolve_b", bucket, "1", "rollback")
        require(code == 0 and alias(fixture) == "1" and receipt_of(bucket, RUN_B)["previous_version"] == "2", f"the next run was not served: {code} {error}")

    def owner_cannot_resolve_a_run_that_stored_its_receipt() -> None:
        from scripts.training import promote_owner

        fixture, bucket = registry_fixture(folder / "resolve_done"), receipt_bucket()
        bucket.put_faults[lease_closed(1)] = [client_error("AccessDenied", "put_object")]
        require(promote_in(fixture, folder / "resolve_done_a", bucket)[0] != 0, "run A did not stop before closing its lease")
        with mock.patch.object(promote_receipt, "s3_client", return_value=bucket), contextlib.redirect_stderr(io.StringIO()) as error:
            code = promote_owner.main(["resolve", "--model", MODEL, "--run", RUN, "--owner", "owner", "--reason", "x"], {"DATA_BUCKET_NAME": BUCKET})
        require(code == 2 and "stored its receipt" in error.getvalue(), f"{code} {error.getvalue()}")
        require(promote_receipt.key_of(RUN, "abandoned.json") not in bucket.objects, "a completed run was fenced")

    def stuck_run(name: str) -> tuple[Fixture, ListingBucket, MlflowService]:
        """A registry where run A moved production to version 2 and stopped before it closed its lease, and the owner's stand-ins."""
        fixture, bucket = registry_fixture(folder / name), receipt_bucket()
        bucket.put_faults["promote.json"] = [client_error("AccessDenied", "put_object")]
        require(promote_in(fixture, folder / f"{name}_a", bucket)[0] != 0 and alias(fixture) == "2", "run A did not stop after the move")
        return fixture, bucket, MlflowService(fixture.tracking)

    resolve = ("resolve", "--model", MODEL, "--run", RUN, "--owner", "owner", "--reason", "the task was lost")

    def owner_reads_the_alias_without_the_mlflow_package() -> None:
        fixture, bucket, service = stuck_run("rest_read")
        with without_mlflow():
            try:
                importlib.import_module("mlflow")
            except ImportError:
                pass
            else:
                raise ValueError("the blocker let mlflow import")
        code, out, error, requests, imports = owner_run(fixture, bucket, service, resolve)
        require(code == 0 and "abandoned" in out and lease_closed(1) in lease_keys(bucket), f"the owner could not resolve without mlflow: {code} {out} {error}")
        require(not imports, f"the command tried to import mlflow: {imports}")
        require(len(requests) == 1 and requests[0]["path"] == mlflow_alias_stand_in.ENDPOINT, f"the alias was not read once from the REST endpoint: {requests}")
        require(requests[0]["query"] == {"name": MODEL, "alias": "production"} and requests[0]["authorized"], f"the request is wrong: {requests}")
        fence = json.loads(bucket.objects[promote_receipt.key_of(RUN, "abandoned.json")])
        require(fence["production_alias"] == "2", f"the fence does not record version 2: {fence}")
        require(OWNER_LOGIN not in out + error + json.dumps(fence), "the MLflow login reached the output or the fence")

    def owner_reads_a_missing_alias_as_none() -> None:
        fixture, bucket, service = stuck_run("rest_none")
        del fixture.tracking.models[MODEL]["aliases"]["production"]
        code, out, error, requests, _ = owner_run(fixture, bucket, service, resolve)
        require(code == 0 and "abandoned" in out and lease_closed(1) in lease_keys(bucket), f"a missing alias was not read as none: {code} {out} {error}")
        require(len(requests) == 1 and requests[0]["authorized"], f"the alias was not read once with the login: {requests}")
        fence = json.loads(bucket.objects[promote_receipt.key_of(RUN, "abandoned.json")])
        require(fence["production_alias"] == "none", f"the fence does not record no production alias: {fence}")

    def owner_reads_a_missing_model_as_none() -> None:
        fixture, bucket, service = stuck_run("rest_no_model")
        del fixture.tracking.models[MODEL]
        code, out, error, requests, _ = owner_run(fixture, bucket, service, resolve)
        require(code == 0 and "abandoned" in out and lease_closed(1) in lease_keys(bucket), f"a missing model was not read as none: {code} {out} {error}")
        require(len(requests) == 1, f"the alias was not read once: {requests}")
        fence = json.loads(bucket.objects[promote_receipt.key_of(RUN, "abandoned.json")])
        require(fence["production_alias"] == "none", f"the fence does not record no production alias: {fence}")

    def owner_alias_read_failures_keep_the_lease_held() -> None:
        fixture, bucket, service = stuck_run("rest_failures")
        wrong = "-".join(("synthetic", "wrong", "login", "1357924680"))
        failures = (
            ("wrong_login", {"login": wrong}, "HTTP 401", 1),
            ("no_login", {"login": None}, "MLFLOW_TRACKING_PASSWORD", 0),
            ("server_error", {"fault": "server_error"}, "HTTP 500", 1),
            ("other_404", {"fault": "other_404"}, "HTTP 404", 1),
            ("invalid_alias_400", {"fault": "invalid_alias_400"}, "HTTP 400", 1),
            ("other_code_400", {"fault": "other_code_400"}, "HTTP 400", 1),
            ("other_alias_400", {"fault": "other_alias_400"}, "HTTP 400", 1),
            ("redirect", {"fault": "redirect"}, "HTTP 302", 1),
            ("malformed", {"fault": "malformed"}, "not JSON", 1),
            ("wrong_shape", {"fault": "wrong_shape"}, "no version", 1),
            ("stall", {"fault": "stall", "timeout": 0.5}, "did not answer within", 1),
        )
        for name, options, expected, sent in failures:
            code, out, error, requests, _ = owner_run(fixture, bucket, service, resolve, **options)
            require(code == 2 and expected in error, f"{name}: the failure was not reported with its cause: {code} {error}")
            require(len(requests) == sent, f"{name}: {len(requests)} alias requests: {requests}")
            require("The lease is still held" in error, f"{name}: the lease state is not reported: {error}")
            require(
                promote_receipt.key_of(RUN, "abandoned.json") not in bucket.objects and lease_closed(1) not in lease_keys(bucket), f"{name}: the run was closed"
            )
            require(wrong not in out + error and OWNER_LOGIN not in out + error, f"{name}: a MLflow login reached the output")
        require(
            service.restarts == sum(sent for *_, sent in failures), f"a resolution restarts MLflow once, and one without a login does not: {service.restarts}"
        )
        code, out, error, _, _ = owner_run(fixture, bucket, service, resolve)
        require(code == 0 and lease_closed(1) in lease_keys(bucket), f"the repeat after the failures did not close the lease: {code} {out} {error}")

    def owner_alias_read_deadline_covers_every_byte() -> None:
        fixture, bucket, service = stuck_run("rest_deadline")
        limit, margin = 0.6, 1.2
        slow = mlflow_alias_stand_in.DRIP_SECONDS * mlflow_alias_stand_in.DRIP_BYTES
        require(slow > limit + margin, f"the dripped answer takes {slow} seconds, which is not longer than the deadline and its margin")
        for fault in ("drip_headers", "drip_body", "drip_error_body"):
            started = time.monotonic()
            code, out, error, requests, _ = owner_run(fixture, bucket, service, resolve, fault=fault, timeout=limit)
            elapsed = time.monotonic() - started
            require(code == 2 and f"did not answer within {limit:g} seconds" in error, f"{fault}: the late answer was not refused as a timeout: {code} {error}")
            require(elapsed < limit + margin, f"{fault}: the call took {elapsed:.2f} seconds for a deadline of {limit} seconds")
            require("The lease is still held" in error and len(requests) == 1, f"{fault}: the lease state or request count is wrong: {error} {requests}")
            require(
                promote_receipt.key_of(RUN, "abandoned.json") not in bucket.objects and lease_closed(1) not in lease_keys(bucket),
                f"{fault}: the run was closed",
            )
            require(OWNER_LOGIN not in out + error, f"{fault}: a MLflow login reached the output")

    def owner_alias_error_body_timeout_keeps_the_lease_held() -> None:
        fixture, bucket, service = stuck_run("rest_error_body")
        for status in (400, 404):
            code, out, error, requests, _ = owner_run(fixture, bucket, service, resolve, fault=f"withhold_{status}", timeout=0.5)
            require(
                code == 2 and "did not answer within 0.5 seconds" in error, f"HTTP {status}: the withheld body was not refused as a timeout: {code} {error}"
            )
            require("The lease is still held" in error and len(requests) == 1, f"HTTP {status}: the lease state or request count is wrong: {error} {requests}")
            require(
                promote_receipt.key_of(RUN, "abandoned.json") not in bucket.objects and lease_closed(1) not in lease_keys(bucket),
                f"HTTP {status}: the run was closed",
            )
        code, out, error, _, _ = owner_run(fixture, bucket, service, resolve)
        require(code == 0 and lease_closed(1) in lease_keys(bucket), f"the repeat after the timeouts did not close the lease: {code} {out} {error}")

    def attempt_reply_lost_fails_closed() -> None:
        fixture, bucket = registry_fixture(folder / "attempt_lost"), receipt_bucket()
        bucket.write_hook = lambda key: setattr(bucket, "lose_reply_once", key.endswith("attempt.json"))
        code, _, error = promote_in(fixture, folder / "attempt_lost_work", bucket)
        require(code != 0 and "promotion_needs_resolution" in error, f"a lost attempt reply was not refused: {code} {error}")
        require(alias(fixture) == "1" and not writes(fixture) and ATTEMPT_KEY in bucket.objects, "the alias request was sent without a known attempt record")

    def pending_alias_request_cannot_revert_a_later_promotion() -> None:
        fixture, bucket = registry_fixture(folder / "pending"), receipt_bucket()
        allow_three(fixture)
        service, resolve = MlflowService(fixture.tracking), ("resolve", "--model", MODEL, "--run", RUN, "--owner", "owner", "--reason", "the task was lost")
        fixture.tracking.hold_alias = True
        code, _, error = promote_in(fixture, folder / "pending_a", bucket)
        require(
            code != 0 and alias(fixture) == "1" and fixture.tracking.pending == [(MODEL, "production", "2")],
            f"run A did not leave a pending request: {code} {error}",
        )
        fixture.tracking.calls.clear()
        code, _, error = promote_in(fixture, folder / "pending_a_retry", bucket)
        require(code != 0 and "promotion_needs_resolution" in error, f"the retry in a fresh workspace was not stopped: {code} {error}")
        require("set_registered_model_alias" not in writes(fixture) and alias(fixture) == "1", f"the retry sent the alias request again: {writes(fixture)}")
        require(lease_keys(bucket) == ["lease-0000000001.json"], f"the lease was closed or replaced: {lease_keys(bucket)}")
        code, _, error = run_b(fixture, folder / "pending_b_early", bucket)
        require(code != 0 and "promotion_busy" in error and alias(fixture) == "1" and not writes(fixture), f"run B took the model: {code} {error}")
        code, out, error = owner_command(fixture, bucket, service, "show", "--model", MODEL)
        require(code == 0 and "attempt.json" in json.loads(out)["held"]["records"], f"show does not list the attempt: {code} {out} {error}")
        seen: list[bool] = []
        original = alias_lookup(fixture)

        def read_after_restart(name: str, alias_name: str) -> str | None:
            seen.append(service.restart_done)
            return original(name, alias_name)

        code, out, error = owner_command(fixture, bucket, service, *resolve, lookup=read_after_restart)
        require(code == 0 and service.restarts == 1 and not fixture.tracking.pending, f"the owner resolution did not restart MLflow: {code} {out} {error}")
        require(seen == [True], f"the alias was read before the restart finished: {seen}")
        fence = json.loads(bucket.objects[promote_receipt.key_of(RUN, "abandoned.json")])
        require(fence["production_alias"] == "1" and lease_closed(1) in lease_keys(bucket), f"the resolution did not record the alias and close: {fence}")
        code, _, error = run_b(fixture, folder / "pending_b", bucket)
        require(code == 0 and alias(fixture) == "3", f"run B did not promote after the resolution: {code} {error}")
        fixture.tracking.deliver_pending()
        require(alias(fixture) == "3", f"the old alias request reverted production to {alias(fixture)}")
        require(receipt_of(bucket, RUN_B)["previous_version"] == "1", "run B lost its predecessor")
        code, _, error = promote_in(fixture, folder / "pending_a_late", bucket)
        require(code != 0 and "promotion_abandoned" in error and alias(fixture) == "3", f"the abandoned run started again: {code} {error}")

    def resolution_needs_a_finished_restart() -> None:
        fixture, bucket = registry_fixture(folder / "restart"), receipt_bucket()
        allow_three(fixture)
        fixture.tracking.hold_alias = True
        require(promote_in(fixture, folder / "restart_a", bucket)[0] != 0, "run A did not stop after its alias request")
        resolve = ("resolve", "--model", MODEL, "--run", RUN, "--owner", "owner", "--reason", "the task was lost")
        for fault, reason in (("update_error", "AccessDenied"), ("never_stop", "not reached within"), ("rollout_failed", "rollout FAILED")):
            service = MlflowService(fixture.tracking, {fault})
            code, _, error = owner_command(fixture, bucket, service, *resolve)
            require(code == 2 and reason in error and "lease is still held" in error, f"{fault}: the failed restart was not reported: {code} {error}")
            require(
                not any(key.endswith("abandoned.json") for key in bucket.objects) and lease_closed(1) not in lease_keys(bucket),
                f"{fault}: the lease was closed",
            )
            code, _, error = run_b(fixture, folder / f"restart_b_{fault}", bucket)
            require(code != 0 and "promotion_busy" in error and alias(fixture) == "1", f"{fault}: run B took the model: {code} {error}")
        code, _, error = owner_command(
            fixture, bucket, MlflowService(fixture.tracking), "resolve", "--model", MODEL, "--run", RUN, "--owner", "owner", "--reason", "x"
        )
        require(code == 0 and lease_closed(1) in lease_keys(bucket), f"a healthy restart did not resolve: {code} {error}")
        code, _, error = owner_command(
            fixture, bucket, MlflowService(fixture.tracking), "resolve", "--model", MODEL, "--run", RUN, "--owner", "owner", "--reason", "x"
        )
        require(code == 2 and "not held" in error, f"a second resolution was not refused: {code} {error}")

    cases.check("promote_refuses_a_second_run_while_an_interrupted_run_is_unresolved", rolled_back_run_cannot_repeat_the_move)
    cases.check("promote_pending_alias_request_cannot_revert_a_later_promotion", pending_alias_request_cannot_revert_a_later_promotion)
    cases.check("promote_owner_resolution_needs_a_finished_mlflow_restart", resolution_needs_a_finished_restart)
    cases.check("promote_attempt_record_whose_reply_was_lost_fails_closed", attempt_reply_lost_fails_closed)
    cases.check("promote_concurrent_run_that_finishes_first_is_not_overwritten", run_that_finishes_first_is_not_overwritten)
    cases.check("promote_concurrent_run_is_refused_while_the_first_is_unresolved", run_that_is_unresolved_blocks_the_delayed_run)
    cases.check("promote_lease_survives_a_failure_before_the_intent", lease_survives_a_failure_before_the_intent)
    cases.check("promote_lease_whose_reply_was_lost_is_resumed", lease_lost_reply_is_resumed)
    cases.check("promote_lease_that_cannot_be_read_stops_the_run", lease_that_cannot_be_read_stops_the_run)
    cases.check("promote_lease_generations_span_listing_pages", lease_generations_span_listing_pages)
    cases.check("promote_release_failure_keeps_the_lease_until_a_retry", release_failure_keeps_the_lease_until_a_retry)
    cases.check("promote_completed_run_returns_its_receipt_while_another_holds_the_lease", completed_run_returns_its_receipt_while_another_holds_the_lease)
    cases.check("promote_owner_resolution_closes_a_stuck_lease_and_fences_the_run", owner_resolution_closes_a_stuck_lease_and_fences_the_run)
    cases.check("promote_owner_cannot_resolve_a_run_that_stored_its_receipt", owner_cannot_resolve_a_run_that_stored_its_receipt)
    cases.check("promote_owner_reads_the_alias_without_the_mlflow_package", owner_reads_the_alias_without_the_mlflow_package)
    cases.check("promote_owner_reads_a_missing_production_alias_as_none", owner_reads_a_missing_alias_as_none)
    cases.check("promote_owner_reads_a_missing_model_as_none", owner_reads_a_missing_model_as_none)
    cases.check("promote_owner_alias_read_failures_keep_the_lease_held", owner_alias_read_failures_keep_the_lease_held)
    cases.check("promote_owner_alias_read_deadline_covers_every_byte", owner_alias_read_deadline_covers_every_byte)
    cases.check("promote_owner_alias_error_body_timeout_keeps_the_lease_held", owner_alias_error_body_timeout_keeps_the_lease_held)
    cases.check("promote_records_the_intent_before_the_alias_moves", records_the_intent_first)
    interrupted_after_the_move("promote_keeps_the_predecessor_after_a_crash_with_a_champion", True)
    interrupted_after_the_move("promote_keeps_the_predecessor_after_a_crash_without_a_champion", False)
    stale_retry_moves_nothing("promote_stale_retry_after_a_later_promotion_moves_nothing", "3")
    stale_retry_moves_nothing("promote_stale_retry_after_a_return_to_the_predecessor_moves_nothing", "1")
    cases.check("promote_refuses_a_changed_request_for_a_recorded_run", changed_request_is_refused)
    cases.check("promote_stops_before_the_move_when_the_intent_is_not_stored", intent_failure_stops_the_move)
    cases.check("promote_retry_does_not_send_a_recorded_alias_request_again", retry_does_not_send_the_alias_request_again)
    cases.check("promote_retry_fails_closed_when_completion_cannot_be_read", unknown_completion_fails_closed)
    cases.check("promote_first_run_needs_no_list_grant", first_promotion_needs_no_list_grant)
    cases.check("promote_adopts_an_intent_whose_reply_was_lost", intent_lost_reply_is_adopted)

    cases.check("promote_moves_production", moved)
    cases.check("promote_stores_a_durable_receipt_before_success", stores_a_durable_receipt)
    cases.check("promote_receipt_rerun_changes_nothing", receipt_rerun_is_idempotent)
    cases.check("promote_receipt_refuses_another_receipt", receipt_conflict_refused)
    cases.check("promote_fails_visibly_when_the_receipt_upload_fails", failed_upload_fails_the_job)
    cases.check("promote_adopts_a_receipt_whose_reply_was_lost", lost_reply_is_adopted)
    cases.check("promote_rerun_changes_nothing", rerun_changes_nothing)
    cases.check("promote_rolls_back_to_an_earlier_production_version", rolled_back)
    refuses("promote_refuses_a_failed_version", {**PROMOTION, "model_version": "3"})
    refuses("promote_refuses_a_missing_version", {**PROMOTION, "model_version": "9"})
    refuses("promote_refuses_a_rollback_to_a_never_promoted_version", {**PROMOTION, "model_version": "2", "promotion_mode": "rollback"})
    refuses("promote_refuses_a_training_parameter_set", PARAMETERS)
    refuses("promote_refuses_the_local_lane", {**PROMOTION, "mlflow": "local"})
    refuses("promote_refuses_an_unsafe_tracking_address", MLFLOW_TRACKING_URI="http://203.0.113.9:5100")
    refuses("promote_refuses_a_missing_login", MLFLOW_TRACKING_PASSWORD=None)
