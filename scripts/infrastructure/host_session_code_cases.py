"""Host session cases for the code operation that run the real installer scripts against a real folder instead of a simulated digest.

The stand-in host here executes the command of the code one-shot: the command that a RunTask override carries, or, without one, the command of the
registered task definition. The registered command is the installer and the image fetcher as they were before the verified installer (copies in
``code_installer_before``), so the cases show what ``start`` does while the services stack still registers the old one-shot.
"""

from __future__ import annotations

import contextlib
import importlib.util
import io
import json
import os
import shutil
import sys
import tarfile
import tempfile
from collections.abc import Callable
from pathlib import Path
from typing import Any
from unittest import mock

from scripts.acquisition.s3_store import AwsCli
from scripts.infrastructure.host_session import Clock, HostSessionError, Names, Session
from scripts.infrastructure.host_session_stand_in import (
    CLUSTER,
    CODE_FETCH_FAMILY,
    FAMILIES,
    GROUP,
    OVERRIDES_LIMIT,
    PROFILE,
    PROJECT,
    PROVIDER,
    REGION,
    SERVICES,
    StandInHost,
)
from scripts.infrastructure.run_code_fetch_e2e import BUCKET, RUN_A, RUN_B, StandInS3, archive, archive_files

REPO_ROOT = Path(__file__).resolve().parents[2]
BEFORE = REPO_ROOT / "scripts" / "infrastructure" / "code_installer_before"
REVIEWED_SCRIPT = REPO_ROOT / "infra" / "services" / "scripts" / "code_fetch.py"
IMAGE_FETCHER = REPO_ROOT / "services" / "training" / "fetch_code.py"
CASE_ERRORS = (AssertionError, LookupError, StopIteration, TypeError, ValueError, AttributeError, NameError, OSError, HostSessionError)


class ScriptedHost(StandInHost):
    """A stand-in host whose code one-shot runs a real script against ``root``, the host data folder (/host in the task).

    ``registered`` is the command of the registered task definition and ``fetch_dir`` the folder of the image's own fetch script (/opt/hai in the task).
    ``after_install`` runs on the folder after every successful install task, to damage what the install left. ``commands`` lists where each task's
    command came from: ``override`` or ``registered``, and ``modes`` the CODE_MODE of each task. A task whose mode is in ``never_starts`` fails the way
    ECS reports a container that cannot start: no script runs, the container has no exit code and the stop code is TaskFailedToStart.
    """

    def __init__(self, root: Path, registered: str, fetch_dir: Path, client: StandInS3, faults: set[str] | None = None) -> None:
        super().__init__(faults, code_fetch=True)
        self.root, self.registered, self.fetch_dir, self.client = root, registered, fetch_dir, client
        self.after_install: Callable[[Path], None] | None = None
        self.commands: list[str] = []
        self.modes: list[str] = []
        self.never_starts: set[str] = set()
        self.unstarted: set[str] = set()
        self.next_unstarted = False
        self.output = io.StringIO()

    def ecs_run_task(self, options: dict[str, list[str]]) -> dict[str, Any]:
        launched = super().ecs_run_task(options)
        if self.next_unstarted:
            self.next_unstarted = False
            self.unstarted.add(launched["tasks"][0]["taskArn"])
        return launched

    def ecs_describe_tasks(self, options: dict[str, list[str]]) -> dict[str, Any]:
        described = super().ecs_describe_tasks(options)
        for task in described["tasks"]:
            if task["taskArn"] in self.unstarted and task["lastStatus"] == "STOPPED":
                task["containers"][0].pop("exitCode", None)
                task["containers"][0]["reason"] = "CannotPullContainerError"
                task["stopCode"], task["stoppedReason"] = "TaskFailedToStart", "Task failed to start"
        return described

    def code_fetch_exit(self, options: dict[str, list[str]], code: int) -> int:
        containers = json.loads(options["--overrides"][0])["containerOverrides"] if "--overrides" in options else []
        entry: dict[str, Any] = next((item for item in containers if item.get("name") == "code_fetch"), {})
        variables = {item["name"]: item["value"] for item in entry.get("environment", [])}
        self.commands.append("override" if entry.get("command") else "registered")
        self.modes.append(variables.get("CODE_MODE", "install"))
        text = entry["command"][0] if entry.get("command") else self.registered
        if code != 0:
            return code
        if self.modes[-1] in self.never_starts:
            self.next_unstarted = True
            return 1
        exit_code = self.execute(text, {"DATA_BUCKET_NAME": BUCKET, **variables})
        if exit_code == 0 and "CODE_MODE" not in variables and self.after_install is not None:
            self.after_install(self.root)
        return exit_code

    def execute(self, text: str, variables: dict[str, str]) -> int:
        """Run the one-shot script ``text`` as its container does, with ``variables`` as its environment, and return its exit code."""
        folder = Path(tempfile.mkdtemp(prefix="code_one_shot_"))
        (folder / "one_shot.py").write_text(text, encoding="utf-8")
        spec = importlib.util.spec_from_file_location("code_one_shot", folder / "one_shot.py")
        if spec is None or spec.loader is None:
            raise ValueError("the one-shot script cannot be loaded")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        sys.modules.pop("fetch_code", None)
        saved = list(sys.path)
        try:
            with mock.patch.dict(os.environ, variables, clear=True), contextlib.redirect_stdout(self.output), contextlib.redirect_stderr(self.output):
                return int(module.run(self.root, self.fetch_dir, self.client))
        except KeyboardInterrupt:
            return 130
        finally:
            sys.path[:] = saved
            sys.modules.pop("fetch_code", None)


def names() -> Names:
    return Names(
        CLUSTER, GROUP, PROVIDER, PROJECT, dict(sorted(SERVICES.items(), key=lambda item: item[0] != "postgres")), {**FAMILIES, "code_fetch": CODE_FETCH_FAMILY}
    )


def old_image(work: Path) -> Path:
    """A folder holding the image's fetch script as it was before the verified installer."""
    folder = work / "old_image"
    folder.mkdir()
    (folder / "fetch_code.py").write_text((BEFORE / "fetch_code.py.txt").read_text(encoding="utf-8"), encoding="utf-8")
    return folder


def session_on(host: ScriptedHost) -> tuple[Session, list[str]]:
    lines: list[str] = []
    cli = AwsCli({"aws_profile": PROFILE, "aws_region": REGION}, host.runner)
    return Session(cli, names(), Clock(lambda: host.now, host.advance), Path(tempfile.mkdtemp(prefix="host_session_code_")) / "launched", lines.append), lines


def release(root: Path, sha: str, path: Path, members: int | None = None, select: bool = True) -> None:
    """Put the archive ``path`` under repo_releases/<sha>, unpack ``members`` of its members (all when None) into code and show it as repo when ``select``."""
    folder = root / "repo_releases" / sha
    (folder / "code").mkdir(parents=True)
    shutil.copy(path, folder / "code.tar.gz")
    with tarfile.open(path) as tar:
        for index, member in enumerate(tar):
            if members is None or index < members:
                tar.extract(member, folder / "code", filter="data")
    if select:
        link = root / "repo"
        if link.is_dir() and not link.is_symlink():
            link.rmdir()
        link.symlink_to(f"repo_releases/{sha}/code")


def shown(root: Path) -> dict[str, bytes]:
    """The files that repo shows."""
    repo = root / "repo"
    return {item.relative_to(repo).as_posix(): item.read_bytes() for item in sorted(repo.rglob("*")) if item.is_file()} if repo.is_dir() else {}


def deployments(host: StandInHost) -> int:
    return sum(entry["operation"] == "update-service" for entry in host.log)


def scene(registered: str, image: Path | None = None) -> tuple[ScriptedHost, Session, list[str], dict[str, tuple[Path, str]], Path]:
    """A new host data folder with an empty repo folder, the two synthetic archives and a session over a scripted host."""
    work = Path(tempfile.mkdtemp(prefix="code_scene_"))
    archives = {RUN_A: archive(work, "a"), RUN_B: archive(work, "b")}
    root = work / "host"
    (root / "repo").mkdir(parents=True)
    fetch_dir = image or old_image(work)
    host = ScriptedHost(root, registered, fetch_dir, StandInS3({run: path for run, (path, _) in archives.items()}))
    session, lines = session_on(host)
    return host, session, lines, archives, work


def start_for(session: Session, run: str, sha: str) -> str | None:
    """The message of the error that ``start`` for the code ``(run, sha)`` raised, or None."""
    try:
        session.start(code=(run, sha))
    except HostSessionError as error:
        return str(error)
    return None


def registered_old() -> str:
    return (BEFORE / "code_fetch.py.txt").read_text(encoding="utf-8")


def old_installer_with_an_interrupted_cache_selects_no_partial_checkout() -> dict[str, bool]:
    host, session, _, archives, _ = scene(registered_old())
    (path_a, sha_a), (path_b, sha_b) = archives[RUN_A], archives[RUN_B]
    release(host.root, sha_a, path_a)
    release(host.root, sha_b, path_b, members=1, select=False)
    message = start_for(session, RUN_B, sha_b)
    now = shown(host.root)
    return {
        "the first code task runs the reviewed installer as its command, not the registered one": host.commands[:1] == ["override"],
        "repo never shows an incomplete release": now in (archive_files(path_a), archive_files(path_b)),
        "start ends with the complete release selected": message is None and now == archive_files(path_b),
        "the interrupted folder was completed": (host.root / "repo_releases" / sha_b / "code").is_dir(),
    }


def verification_failure_restores_the_previous_checkout() -> dict[str, bool]:
    host, session, lines, archives, _ = scene(REVIEWED_SCRIPT.read_text(encoding="utf-8"), IMAGE_FETCHER.parent)
    (path_a, sha_a), (_, sha_b) = archives[RUN_A], archives[RUN_B]
    release(host.root, sha_a, path_a)

    def damage(root: Path) -> None:
        (root / "repo_releases" / sha_b / "code" / "dags" / "second.py").unlink()

    host.after_install = damage
    message = start_for(session, RUN_B, sha_b)
    return {
        "start fails": message is not None,
        "the message states that repo shows a verified checkout": message is not None and "verified checkout" in message,
        "repo shows the previous checkout again": shown(host.root) == archive_files(path_a),
        "no service is deployed before a verified checkout is selected": deployments(host) == 0,
        "the host is turned off again": host.desired == 0,
        "the message names no ARN or account ID": message is not None and "arn:aws" not in message and not any("arn:aws" in line for line in lines),
    }


def failed_install_states_the_actual_state() -> dict[str, bool]:
    host, session, _, archives, _ = scene(REVIEWED_SCRIPT.read_text(encoding="utf-8"), IMAGE_FETCHER.parent)
    (path_a, sha_a), (_, sha_b) = archives[RUN_A], archives[RUN_B]
    release(host.root, sha_a, path_a)
    host.client.fail = True
    message = start_for(session, RUN_B, sha_b)
    return {
        "start fails": message is not None,
        "the message states that repo shows a verified checkout": message is not None and "verified checkout" in message,
        "repo shows the previous checkout": shown(host.root) == archive_files(path_a),
        "no service is deployed": deployments(host) == 0,
    }


def failed_install_without_a_previous_checkout_reports_none() -> dict[str, bool]:
    host, session, _, archives, _ = scene(REVIEWED_SCRIPT.read_text(encoding="utf-8"), IMAGE_FETCHER.parent)
    _, sha_b = archives[RUN_B]
    host.client.fail = True
    message = start_for(session, RUN_B, sha_b)
    return {
        "start fails": message is not None,
        "the message says that repo has no verified checkout": message is not None and "no verified checkout" in message,
        "repo is the empty folder host_prepare leaves": (host.root / "repo").is_dir() and not (host.root / "repo").is_symlink() and shown(host.root) == {},
        "no service is deployed": deployments(host) == 0,
    }


def start_without_code_refuses_a_selected_release_that_is_incomplete() -> dict[str, bool]:
    host, session, _, archives, _ = scene(REVIEWED_SCRIPT.read_text(encoding="utf-8"), IMAGE_FETCHER.parent)
    path_a, sha_a = archives[RUN_A]
    release(host.root, sha_a, path_a, members=1)
    try:
        session.start()
        message = None
    except HostSessionError as error:
        message = str(error)
    return {
        "start without code fails when the selected release is incomplete": message is not None,
        "the message says that repo has no verified checkout": message is not None and "no verified checkout" in message,
        "repo no longer shows the incomplete release": not (host.root / "repo").is_symlink(),
        "no service is deployed": deployments(host) == 0,
    }


def complete_install_with_the_reviewed_installer_and_the_old_image() -> dict[str, bool]:
    host, session, _, archives, _ = scene(REVIEWED_SCRIPT.read_text(encoding="utf-8"))
    path_b, sha_b = archives[RUN_B]
    message = start_for(session, RUN_B, sha_b)
    return {
        "start succeeds with the image's older fetch script": message is None,
        "repo shows every file of the archive": shown(host.root) == archive_files(path_b),
        "every code task carried the reviewed installer as its command": host.commands != [] and set(host.commands) == {"override"},
        "every service is deployed": deployments(host) == len(SERVICES),
    }


def the_override_of_every_code_task_fits_the_run_task_limit() -> dict[str, bool]:
    host, session, _, archives, _ = scene(REVIEWED_SCRIPT.read_text(encoding="utf-8"), IMAGE_FETCHER.parent)
    _, sha_b = archives[RUN_B]
    host.client.fail = True
    start_for(session, RUN_B, sha_b)
    sizes = [len(entry["options"]["--overrides"][0]) for entry in host.log if entry["operation"] == "run-task" and "--overrides" in entry["options"]]
    return {
        "install, verification and restore all launched with their overrides": len(sizes) >= 2,
        "no override is longer than RunTask accepts": all(size <= OVERRIDES_LIMIT for size in sizes),
    }


def a_task_that_never_starts_leaves_the_healthy_selected_checkout() -> dict[str, bool]:
    host, session, _, archives, _ = scene(REVIEWED_SCRIPT.read_text(encoding="utf-8"), IMAGE_FETCHER.parent)
    (path_a, sha_a), (path_b, sha_b) = archives[RUN_A], archives[RUN_B]
    release(host.root, sha_b, path_b, select=False)
    release(host.root, sha_a, path_a)
    (host.root / "repo.previous").symlink_to(f"repo_releases/{sha_b}/code")
    host.never_starts.add("install")
    message = start_for(session, RUN_A, sha_a)
    return {
        "start fails": message is not None,
        "the message says that what repo shows is not known": message is not None and "not known" in message,
        "no task runs the restore mode": host.modes == ["install"],
        "repo still shows the requested healthy release": shown(host.root) == archive_files(path_a),
        "no service is deployed": deployments(host) == 0,
    }


def a_verification_task_that_never_starts_is_reported_as_unknown_state() -> dict[str, bool]:
    host, session, _, archives, _ = scene(REVIEWED_SCRIPT.read_text(encoding="utf-8"), IMAGE_FETCHER.parent)
    (path_a, sha_a), (_, sha_b) = archives[RUN_A], archives[RUN_B]
    release(host.root, sha_a, path_a)
    host.never_starts.add("verify")
    message = start_for(session, RUN_B, sha_b)
    return {
        "start fails": message is not None,
        "the message says that what repo shows is not known": message is not None and "not known" in message,
        "no task runs the restore mode": host.modes == ["install", "verify"],
        "no service is deployed": deployments(host) == 0,
    }


CASES: dict[str, Callable[[], dict[str, bool]]] = {
    "old_installer_with_an_interrupted_cache_selects_no_partial_checkout": old_installer_with_an_interrupted_cache_selects_no_partial_checkout,
    "verification_failure_restores_the_previous_checkout": verification_failure_restores_the_previous_checkout,
    "failed_install_states_the_actual_state": failed_install_states_the_actual_state,
    "failed_install_without_a_previous_checkout_reports_none": failed_install_without_a_previous_checkout_reports_none,
    "start_without_code_refuses_a_selected_release_that_is_incomplete": start_without_code_refuses_a_selected_release_that_is_incomplete,
    "complete_install_with_the_reviewed_installer_and_the_old_image": complete_install_with_the_reviewed_installer_and_the_old_image,
    "the_override_of_every_code_task_fits_the_run_task_limit": the_override_of_every_code_task_fits_the_run_task_limit,
    "a_task_that_never_starts_leaves_the_healthy_selected_checkout": a_task_that_never_starts_leaves_the_healthy_selected_checkout,
    "a_verification_task_that_never_starts_is_reported_as_unknown_state": a_verification_task_that_never_starts_is_reported_as_unknown_state,
}
