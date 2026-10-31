"""Synthetic E2E cases of the AWS task runner: the phases of one training run inside Fargate containers, through ``aws_run.main``."""

from __future__ import annotations

import configparser
import contextlib
import hashlib
import importlib.util
import io
import json
import os
import shutil
import stat
import tarfile
from collections.abc import Iterator, Sequence
from pathlib import Path
from typing import Any
from unittest import mock

from orchestration import code_digest
from scripts.features import publication_reads, training_extract
from scripts.process import run_command
from scripts.training import guards, promotion_check
from scripts.training.job_action_cases import GUARD, HAI_TYPE, REAL, SAVED, Fixture, require, standins
from scripts.training.job_cases import COMMIT, DIGEST
from scripts.training.lane_b_cases import RUN
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

    def restarted() -> None:
        work = folder / "restarted"
        work.mkdir(parents=True)
        require(start(work, options)[0] == 0, "the first start failed")
        before = (work / "mlflow_auth.ini").read_bytes()
        require(start(work, options)[0] == 0 and (work / "mlflow_auth.ini").read_bytes() == before, "a second start changed the auth file")

    def refuses(name: str, reason: str, argv: list[str] | None = None, **changes: str | None) -> None:
        def case() -> None:
            work = folder / name
            work.mkdir(parents=True)
            code, error, launched, _ = start(work, argv or options, **changes)
            require(code != 0 and reason in error and not launched, f"{code} {error} {launched}")
            require(not (work / "mlflow_auth.ini").exists(), "a refused start wrote the auth file")

        cases.check(f"mlflow_start_{name}", case)

    cases.check("mlflow_start_runs_the_server_with_basic_auth_and_a_private_password_file", started)
    cases.check("mlflow_start_restart_keeps_the_same_file", restarted)
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


def promote_call(fixture: Fixture, work: Path, parameters: Any = PROMOTION, **changes: str | None) -> tuple[int, str, str]:
    """Run the promote phase in a work folder that holds the unpacked code of the run."""
    work.mkdir(parents=True, exist_ok=True)
    if not (work / "code.tar.gz").exists():
        code_archive(work)
    sha = hashlib.sha256((work / "code.tar.gz").read_bytes()).hexdigest()
    with standins(fixture), environment(CODE_SHA256=sha, TRAINING_PARAMETERS=json.dumps(parameters), **changes):
        return call("promote", "--run", RUN, "--mode", "real", "--work", str(work))


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

    cases.check("promote_moves_production", moved)
    cases.check("promote_rerun_changes_nothing", rerun_changes_nothing)
    cases.check("promote_rolls_back_to_an_earlier_production_version", rolled_back)
    refuses("promote_refuses_a_failed_version", {**PROMOTION, "model_version": "3"})
    refuses("promote_refuses_a_missing_version", {**PROMOTION, "model_version": "9"})
    refuses("promote_refuses_a_rollback_to_a_never_promoted_version", {**PROMOTION, "model_version": "2", "promotion_mode": "rollback"})
    refuses("promote_refuses_a_training_parameter_set", PARAMETERS)
    refuses("promote_refuses_the_local_lane", {**PROMOTION, "mlflow": "local"})
    refuses("promote_refuses_an_unsafe_tracking_address", MLFLOW_TRACKING_URI="http://203.0.113.9:5100")
    refuses("promote_refuses_a_missing_login", MLFLOW_TRACKING_PASSWORD=None)
