"""Run the code one-shot's script against a local stand-in of the host data folder and keep the report under data/e2e/code_fetch/.

Run from the repository root: ``.venv/bin/python -m scripts.infrastructure.run_code_fetch_e2e``. The script is the one the services stack puts in the
task definition (``infra/services/scripts/code_fetch.py``), and it calls the training image's own ``services/training/fetch_code.py`` unchanged. A stand-in
S3 client serves the archive from a local file, so no AWS service is contacted and no container starts.
"""

from __future__ import annotations

import hashlib
import importlib.util
import io
import json
import os
import sys
import tarfile
import tempfile
import threading
from collections.abc import Callable, Iterator
from contextlib import contextmanager, redirect_stderr, redirect_stdout
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from unittest import mock

from scripts.infrastructure.installer_gates import GATE_SECONDS, Worker, after_first_member, joined, reached

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = REPO_ROOT / "infra" / "services" / "scripts" / "code_fetch.py"
FETCH_DIR = REPO_ROOT / "services" / "training"
OUTPUT = REPO_ROOT / "data" / "e2e" / "code_fetch"
IDENTIFY = REPO_ROOT / "infra" / "services" / "scripts" / "code_identify.py"
BUCKET = "example-project-data-dev"
RUN_A = "20261108T120000Z-aaaaaaaa"
RUN_B = "20261108T130000Z-bbbbbbbb"


def load_script() -> Any:
    """The one-shot script as a module, loaded from the file the task definition embeds."""
    spec = importlib.util.spec_from_file_location("code_fetch_one_shot", SCRIPT)
    if spec is None or spec.loader is None:
        raise FileNotFoundError(SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def archive(folder: Path, marker: str) -> tuple[Path, str]:
    """A code archive of three files, ``dags/marker.py`` holding ``marker``; returns its path and SHA-256."""
    path = folder / f"code_{marker}.tar.gz"
    files = {"dags/marker.py": f"MARKER = {marker!r}\n", "dags/second.py": f"SECOND = {marker!r}\n", "config/third.json": f'{{"third": "{marker}"}}\n'}
    with tarfile.open(path, "w:gz") as tar:
        for name, text in files.items():
            data = text.encode()
            info = tarfile.TarInfo(name)
            info.size, info.mode = len(data), 0o644
            tar.addfile(info, io.BytesIO(data))
    return path, hashlib.sha256(path.read_bytes()).hexdigest()


def unsafe_archive(folder: Path, kind: str) -> tuple[Path, str]:
    """A code archive that tries to leave its release folder: ``traversal`` has an entry named ``../outside``, ``symlink`` a ``dags`` link out of it."""
    path = folder / f"code_{kind}.tar.gz"
    with tarfile.open(path, "w:gz") as tar:
        if kind == "traversal":
            data = b"escaped\n"
            info = tarfile.TarInfo("../outside")
            info.size = len(data)
            tar.addfile(info, io.BytesIO(data))
        else:
            info = tarfile.TarInfo("dags")
            info.type, info.linkname = tarfile.SYMTYPE, "../../outside"
            tar.addfile(info)
    return path, hashlib.sha256(path.read_bytes()).hexdigest()


class StandInS3:
    """Serves one archive per run ID from local files and records every download; ``fail`` makes every download raise."""

    def __init__(self, archives: dict[str, Path], fail: bool = False) -> None:
        self.archives, self.fail, self.downloads = archives, fail, list[str]()

    def download_file(self, bucket: str, key: str, destination: str) -> None:
        self.downloads.append(key)
        if self.fail or bucket != BUCKET:
            raise ConnectionError("stand-in download refused")
        run = key.split("/")[2]
        if key != f"lakehouse/run_stage/{run}/inputs/code.tar.gz" or run not in self.archives:
            raise FileNotFoundError(key)
        Path(destination).write_bytes(self.archives[run].read_bytes())


def host_after_prepare(root: Path) -> Path:
    """The data folder as host_prepare leaves it: an empty repo folder."""
    host = root / "host"
    (host / "repo").mkdir(parents=True)
    return host


def environment(run: str | None, sha: str | None) -> dict[str, str]:
    values = {"DATA_BUCKET_NAME": BUCKET}
    if run is not None:
        values["CODE_RUN_ID"] = run
    if sha is not None:
        values["CODE_SHA256"] = sha
    return values


def call(script: Any, host: Path, values: dict[str, str], client: StandInS3) -> tuple[int, str]:
    """Run the script once in this process with the given task environment; returns the exit code and what it printed."""
    out, err = io.StringIO(), io.StringIO()
    saved = dict(os.environ)
    os.environ.clear()
    os.environ.update(values)
    try:
        with redirect_stdout(out), redirect_stderr(err):
            code = script.run(host, FETCH_DIR, client)
    finally:
        os.environ.clear()
        os.environ.update(saved)
    return code, out.getvalue() + err.getvalue()


def marker(host: Path) -> str | None:
    """The marker of the checkout that /host/repo shows, or None."""
    path = host / "repo" / "dags" / "marker.py"
    return path.read_text() if path.is_file() else None


def archive_files(path: Path) -> dict[str, bytes]:
    """The regular files of an archive by name."""
    with tarfile.open(path) as tar:
        found = {member.name: tar.extractfile(member) for member in tar if member.isreg()}
        return {name: source.read() for name, source in found.items() if source is not None}


def checkout_files(host: Path) -> dict[str, bytes]:
    """The files that /host/repo shows, by name, or an empty result when it shows none."""
    root = host / "repo"
    return {item.relative_to(root).as_posix(): item.read_bytes() for item in sorted(root.rglob("*")) if item.is_file()} if root.is_dir() else {}


def leftovers(host: Path, *releases: str) -> list[str]:
    """The entries of repo_releases that are not a release folder of ``releases`` and not a hidden bookkeeping file."""
    folder = host / "repo_releases"
    known = tuple(releases)
    return sorted(item.name for item in folder.iterdir() if not item.name.startswith((".", *known))) if folder.is_dir() else []


@contextmanager
def task_environment(values: dict[str, str]) -> Iterator[io.StringIO]:
    """Give every thread of a case one task environment and one captured output; yields the captured text."""
    saved, out = dict(os.environ), io.StringIO()
    os.environ.clear()
    os.environ.update(values)
    try:
        with redirect_stdout(out), redirect_stderr(out):
            yield out
    finally:
        os.environ.clear()
        os.environ.update(saved)


def interrupted_after_first_member(script: Any, host: Path, values: dict[str, str], client: StandInS3) -> int:
    """Run one fetch that is interrupted right after it unpacks its first archive member; returns 130 when it was interrupted."""

    def interrupt() -> None:
        raise KeyboardInterrupt

    worker = Worker("interrupted", lambda: script.run(host, FETCH_DIR, client))
    with task_environment(values), after_first_member({"interrupted": interrupt}):
        worker.start()
        joined(worker)
    return 130 if isinstance(worker.error, KeyboardInterrupt) else int(worker.result)


def fixture() -> tuple[Any, Path, dict[str, tuple[Path, str]], Path]:
    work = Path(tempfile.mkdtemp(prefix="code_fetch_case_"))
    archives = {RUN_A: archive(work, "a"), RUN_B: archive(work, "b")}
    return load_script(), host_after_prepare(work), archives, work


def first_fetch_links_the_checkout() -> dict[str, bool]:
    script, host, archives, _ = fixture()
    path, sha = archives[RUN_A]
    code, text = call(script, host, environment(RUN_A, sha), StandInS3({RUN_A: path}))
    link = host / "repo"
    return {
        "the fetch succeeds": code == 0,
        "repo is a relative link to the release of the code digest": link.is_symlink() and os.readlink(link) == f"repo_releases/{sha}/code",
        "the checkout shows the archive content": marker(host) == "MARKER = 'a'\n",
        "the archive stays beside its release, so a rerun can recognise it": (host / "repo_releases" / sha / "code.tar.gz").is_file(),
        "no temporary link is left": not (host / "repo.next").exists() and not (host / "repo.next").is_symlink(),
        "the output names the run and the digest": RUN_A in text and sha[:12] in text,
    }


def rerun_for_the_same_code_changes_nothing() -> dict[str, bool]:
    script, host, archives, _ = fixture()
    path, sha = archives[RUN_A]
    call(script, host, environment(RUN_A, sha), StandInS3({RUN_A: path}))
    link, release = host / "repo", host / "repo_releases" / sha / "code" / "dags" / "marker.py"
    before = (os.lstat(link).st_mtime_ns, os.stat(release).st_mtime_ns)
    second = StandInS3({RUN_A: path})
    code, _ = call(script, host, environment(RUN_A, sha), second)
    return {
        "the rerun succeeds": code == 0,
        "the rerun downloads nothing": second.downloads == [],
        "the link and the release are untouched": (os.lstat(link).st_mtime_ns, os.stat(release).st_mtime_ns) == before,
        "one release exists": [item.name for item in (host / "repo_releases").iterdir()] == [sha],
    }


def new_code_swaps_the_link() -> dict[str, bool]:
    script, host, archives, _ = fixture()
    (path_a, sha_a), (path_b, sha_b) = archives[RUN_A], archives[RUN_B]
    call(script, host, environment(RUN_A, sha_a), StandInS3({RUN_A: path_a}))
    code, _ = call(script, host, environment(RUN_B, sha_b), StandInS3({RUN_B: path_b}))
    return {
        "the second fetch succeeds": code == 0,
        "repo now shows the new code": os.readlink(host / "repo") == f"repo_releases/{sha_b}/code" and marker(host) == "MARKER = 'b'\n",
        "the previous release is kept": (host / "repo_releases" / sha_a / "code" / "dags" / "marker.py").is_file(),
    }


def hash_mismatch_keeps_the_old_checkout() -> dict[str, bool]:
    script, host, archives, _ = fixture()
    (path_a, sha_a), (path_b, _) = archives[RUN_A], archives[RUN_B]
    call(script, host, environment(RUN_A, sha_a), StandInS3({RUN_A: path_a}))
    wrong = "0" * 64
    code, text = call(script, host, environment(RUN_B, wrong), StandInS3({RUN_B: path_b}))
    return {
        "a mismatched archive is refused": code != 0 and "differs from CODE_SHA256" in text,
        "repo still shows the old code": os.readlink(host / "repo") == f"repo_releases/{sha_a}/code" and marker(host) == "MARKER = 'a'\n",
        "nothing of the refused archive is unpacked": not (host / "repo_releases" / wrong / "code").exists(),
    }


def failed_download_keeps_the_old_checkout() -> dict[str, bool]:
    script, host, archives, _ = fixture()
    (path_a, sha_a), (_, sha_b) = archives[RUN_A], archives[RUN_B]
    call(script, host, environment(RUN_A, sha_a), StandInS3({RUN_A: path_a}))
    code, text = call(script, host, environment(RUN_B, sha_b), StandInS3({}, fail=True))
    return {
        "a failed download is refused": code != 0 and "cannot download" in text,
        "repo still shows the old code": os.readlink(host / "repo") == f"repo_releases/{sha_a}/code" and marker(host) == "MARKER = 'a'\n",
    }


def unsafe_archive_keeps_the_old_checkout() -> dict[str, bool]:
    checks: dict[str, bool] = {}
    for kind in ("traversal", "symlink"):
        script, host, archives, work = fixture()
        path_a, sha_a = archives[RUN_A]
        call(script, host, environment(RUN_A, sha_a), StandInS3({RUN_A: path_a}))
        path_bad, sha_bad = unsafe_archive(work, kind)
        code, text = call(script, host, environment(RUN_B, sha_bad), StandInS3({RUN_B: path_bad}))
        checks[f"a {kind} archive with the right digest is refused at unpacking"] = code != 0 and "cannot unpack" in text
        checks[f"after a {kind} archive repo still shows the old code"] = (
            os.readlink(host / "repo") == f"repo_releases/{sha_a}/code" and marker(host) == "MARKER = 'a'\n"
        )
        checks[f"a {kind} archive writes nothing outside its release folder"] = not any(item.name == "outside" for item in work.rglob("outside"))
    return checks


def missing_archive_keeps_the_old_checkout() -> dict[str, bool]:
    script, host, archives, _ = fixture()
    (path_a, sha_a), (_, sha_b) = archives[RUN_A], archives[RUN_B]
    call(script, host, environment(RUN_A, sha_a), StandInS3({RUN_A: path_a}))
    code, text = call(script, host, environment(RUN_B, sha_b), StandInS3({RUN_A: path_a}))
    return {
        "a run with no staged archive is refused": code != 0 and "cannot download" in text,
        "repo still shows the old code": os.readlink(host / "repo") == f"repo_releases/{sha_a}/code" and marker(host) == "MARKER = 'a'\n",
        "no release folder is made for the missing run": not (host / "repo_releases" / sha_b).exists(),
    }


def no_code_keeps_an_existing_checkout() -> dict[str, bool]:
    script, host, archives, _ = fixture()
    path, sha = archives[RUN_A]
    call(script, host, environment(RUN_A, sha), StandInS3({RUN_A: path}))
    client = StandInS3({})
    code, text = call(script, host, environment(None, None), client)
    return {
        "start without code succeeds when a checkout exists": code == 0,
        "it downloads nothing": client.downloads == [],
        "repo is unchanged": os.readlink(host / "repo") == f"repo_releases/{sha}/code",
        "the output says the checkout was kept": "kept" in text,
    }


def no_code_and_no_checkout_is_refused() -> dict[str, bool]:
    script, host, _, _ = fixture()
    code, text = call(script, host, environment(None, None), StandInS3({}))
    return {
        "start without code on an empty repo is refused": code != 0,
        "the refusal names the missing options": "--code-run" in text and "--code-sha256" in text,
        "the empty repo folder is left as it was": (host / "repo").is_dir() and not (host / "repo").is_symlink(),
    }


def half_given_code_is_refused() -> dict[str, bool]:
    script, host, archives, _ = fixture()
    _, sha = archives[RUN_A]
    client = StandInS3({})
    only_run, _ = call(script, host, environment(RUN_A, None), client)
    only_sha, _ = call(script, host, environment(None, sha), client)
    bad_sha, _ = call(script, host, environment(RUN_A, "../" + "a" * 61), client)
    return {
        "a run ID without a digest is refused": only_run != 0,
        "a digest without a run ID is refused": only_sha != 0,
        "a digest that is not 64 hex characters is refused": bad_sha != 0,
        "nothing was downloaded": client.downloads == [],
        "no release folder was made": not (host / "repo_releases").exists(),
    }


def repo_folder_with_content_is_refused() -> dict[str, bool]:
    script, host, archives, _ = fixture()
    path, sha = archives[RUN_A]
    (host / "repo" / "README.md").write_text("an older hand copy\n")
    code, text = call(script, host, environment(RUN_A, sha), StandInS3({RUN_A: path}))
    return {
        "a repo folder with content is refused": code != 0 and "repo" in text,
        "the folder and its file are untouched": (host / "repo").is_dir() and not (host / "repo").is_symlink() and (host / "repo" / "README.md").is_file(),
    }


def interrupted_unpacking_is_not_a_release_on_retry() -> dict[str, bool]:
    script, host, archives, _ = fixture()
    (path_a, sha_a), (path_b, sha_b) = archives[RUN_A], archives[RUN_B]
    call(script, host, environment(RUN_A, sha_a), StandInS3({RUN_A: path_a}))
    stopped = interrupted_after_first_member(script, host, environment(RUN_B, sha_b), StandInS3({RUN_B: path_b}))
    kept = checkout_files(host) == archive_files(path_a)
    code, _ = call(script, host, environment(RUN_B, sha_b), StandInS3({RUN_B: path_b}))
    return {
        "the interrupted fetch stopped": stopped == 130,
        "after the interruption repo still shows the previous checkout": kept,
        "the retry with the same digest succeeds": code == 0,
        "after the retry repo shows every file of the archive": checkout_files(host) == archive_files(path_b),
        "no temporary folder is left": leftovers(host, sha_a, sha_b) == [],
    }


def concurrent_fetches_of_one_digest_are_serialized() -> dict[str, bool]:
    script, host, archives, _ = fixture()
    path, sha = archives[RUN_A]
    paused, second_done, seen = threading.Event(), threading.Event(), dict[str, dict[str, bytes]]()

    def pause() -> None:
        paused.set()
        second_done.wait(GATE_SECONDS)

    def second() -> int:
        try:
            return int(load_script().run(host, FETCH_DIR, StandInS3({RUN_A: path})))
        finally:
            seen["files"] = checkout_files(host)
            second_done.set()

    first = Worker("first", lambda: script.run(host, FETCH_DIR, StandInS3({RUN_A: path})))
    follower = Worker("second", second)
    with task_environment(environment(RUN_A, sha)), after_first_member({"first": pause}):
        first.start()
        reached(paused, "the first fetch unpacking its first member")
        follower.start()
        finished = joined(first, follower)
    expected = archive_files(path)
    return {
        "both fetches finish within the deadline": finished,
        "the first fetch succeeds": first.result == 0,
        "the second fetch succeeds": follower.result == 0,
        "when the second fetch returns, repo shows every file of the archive": seen.get("files") == expected,
        "at the end repo shows every file of the archive": checkout_files(host) == expected,
        "no temporary folder is left": leftovers(host, sha) == [],
    }


def interrupted_fetch_hands_over_to_the_waiting_fetch() -> dict[str, bool]:
    script, host, archives, _ = fixture()
    (path_a, sha_a), (path_b, sha_b) = archives[RUN_A], archives[RUN_B]
    call(script, host, environment(RUN_A, sha_a), StandInS3({RUN_A: path_a}))
    paused, second_started, seen = threading.Event(), threading.Event(), dict[str, dict[str, bytes]]()

    def interrupt() -> None:
        paused.set()
        second_started.wait(GATE_SECONDS)
        raise KeyboardInterrupt

    def second() -> int:
        second_started.set()
        try:
            return int(load_script().run(host, FETCH_DIR, StandInS3({RUN_B: path_b})))
        finally:
            seen["files"] = checkout_files(host)

    first = Worker("first", lambda: script.run(host, FETCH_DIR, StandInS3({RUN_B: path_b})))
    follower = Worker("second", second)
    with task_environment(environment(RUN_B, sha_b)), after_first_member({"first": interrupt}):
        first.start()
        reached(paused, "the first fetch unpacking its first member")
        follower.start()
        finished = joined(first, follower)
    return {
        "both fetches finish within the deadline": finished,
        "the first fetch was interrupted": isinstance(first.error, KeyboardInterrupt),
        "the second fetch succeeds": follower.result == 0,
        "when the second fetch returns, repo shows every file of the archive": seen.get("files") == archive_files(path_b),
        "no temporary folder is left": leftovers(host, sha_a, sha_b) == [],
    }


def fetch_that_cannot_lock_in_time_fails_and_keeps_the_checkout() -> dict[str, bool]:
    script, host, archives, _ = fixture()
    (path_a, sha_a), (path_b, sha_b) = archives[RUN_A], archives[RUN_B]
    call(script, host, environment(RUN_A, sha_a), StandInS3({RUN_A: path_a}))
    paused, second_done, seen = threading.Event(), threading.Event(), dict[str, dict[str, bytes]]()

    def pause() -> None:
        paused.set()
        second_done.wait(GATE_SECONDS * 3)

    def second() -> int:
        impatient = load_script()
        impatient.LOCK_SECONDS = 0.5
        try:
            return int(impatient.run(host, FETCH_DIR, StandInS3({RUN_B: path_b})))
        finally:
            seen["files"] = checkout_files(host)
            second_done.set()

    first = Worker("first", lambda: script.run(host, FETCH_DIR, StandInS3({RUN_B: path_b})))
    follower = Worker("second", second)
    with task_environment(environment(RUN_B, sha_b)) as text, after_first_member({"first": pause}):
        first.start()
        reached(paused, "the first fetch unpacking its first member")
        follower.start()
        finished = joined(first, follower)
    return {
        "both fetches finish within the deadline": finished,
        "the second fetch fails when the lock is not free before its deadline": follower.result == 1 and "lock" in text.getvalue(),
        "the failed fetch leaves repo on the previous checkout": seen.get("files") == archive_files(path_a),
        "the first fetch still succeeds": first.result == 0 and checkout_files(host) == archive_files(path_b),
    }


def release_with_a_changed_file_is_installed_again() -> dict[str, bool]:
    script, host, archives, _ = fixture()
    path, sha = archives[RUN_A]
    call(script, host, environment(RUN_A, sha), StandInS3({RUN_A: path}))
    (host / "repo_releases" / sha / "code" / "dags" / "second.py").write_text("SECOND = 'changed'\n")
    (host / "repo_releases" / sha / "code" / "config" / "third.json").unlink()
    code, _ = call(script, host, environment(RUN_A, sha), StandInS3({RUN_A: path}))
    return {
        "the rerun succeeds": code == 0,
        "repo shows every file of the archive again": checkout_files(host) == archive_files(path),
        "no temporary folder is left": leftovers(host, sha) == [],
    }


def no_code_refuses_a_selected_release_that_is_not_complete() -> dict[str, bool]:
    script, host, archives, _ = fixture()
    path, sha = archives[RUN_A]
    call(script, host, environment(RUN_A, sha), StandInS3({RUN_A: path}))
    (host / "repo_releases" / sha / "code" / "config" / "third.json").unlink()
    code, text = call(script, host, environment(None, None), StandInS3({}))
    return {
        "start without code refuses the incomplete release": code != 0,
        "the refusal says the release is not complete": "complete" in text,
        "repo is not changed": os.readlink(host / "repo") == f"repo_releases/{sha}/code",
    }


def verify_mode(script: Any, host: Path, sha: str, mode: str) -> tuple[int, str]:
    """Run the script in one of its checking modes for the release of ``sha``."""
    return call(script, host, {**environment(RUN_A, sha), "CODE_MODE": mode}, StandInS3({}))


def verify_accepts_only_a_complete_selected_release() -> dict[str, bool]:
    script, host, archives, _ = fixture()
    (path_a, sha_a), (_, sha_b) = archives[RUN_A], archives[RUN_B]
    call(script, host, environment(RUN_A, sha_a), StandInS3({RUN_A: path_a}))
    complete, _ = verify_mode(script, host, sha_a, "verify")
    other, other_text = verify_mode(script, host, sha_b, "verify")
    (host / "repo_releases" / sha_a / "code" / "config" / "third.json").unlink()
    missing, missing_text = verify_mode(script, host, sha_a, "verify")
    return {
        "a complete selected release verifies": complete == 0,
        "a release that repo does not show is refused": other != 0 and "does not show" in other_text,
        "a release without one of its files is refused": missing != 0 and "every file" in missing_text,
        "checking changes nothing": os.readlink(host / "repo") == f"repo_releases/{sha_a}/code" and not (host / "repo_releases" / sha_b).exists(),
    }


RUN_C = "20261108T140000Z-cccccccc"


def identify(script: Any, host: Path) -> tuple[int, str]:
    """Run the identity one-shot (the command of a code task): it names the run that staged the release repo shows, and changes nothing."""
    spec = importlib.util.spec_from_file_location("code_identify_one_shot", IDENTIFY)
    if spec is None or spec.loader is None:
        raise FileNotFoundError(IDENTIFY)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    out = io.StringIO()
    with redirect_stdout(out), redirect_stderr(out):
        code = module.run(host)
    return code, out.getvalue()


def record_of(host: Path, sha: str) -> str | None:
    """The run ID record beside the staged digest, or None."""
    path = host / "repo_releases" / sha / "run_id"
    return path.read_text() if path.is_file() else None


def the_install_records_its_run_beside_the_staged_digest() -> dict[str, bool]:
    script, host, archives, _ = fixture()
    path, sha = archives[RUN_A]
    call(script, host, environment(RUN_A, sha), StandInS3({RUN_A: path}))
    code, text = identify(script, host)
    return {
        "the run ID is recorded beside the archive of the release": record_of(host, sha) == f"{RUN_A}\n",
        "the record does not disturb the verification of the release": verify_mode(script, host, sha, "verify")[0] == 0,
        "identify exits 0 and names the run and the digest": code == 0 and f"code_identify: identity run {RUN_A} code {sha}" in text,
        "no temporary record is left": not (host / "repo_releases" / sha / "run_id.partial").exists(),
    }


def a_later_install_for_the_same_digest_rewrites_the_record() -> dict[str, bool]:
    script, host, archives, _ = fixture()
    path, sha = archives[RUN_A]
    client = StandInS3({RUN_A: path, RUN_C: path})
    call(script, host, environment(RUN_A, sha), client)
    again, _ = call(script, host, environment(RUN_C, sha), client)
    code, text = identify(script, host)
    (host / "repo_releases" / sha / "run_id").unlink()
    call(script, host, environment(RUN_C, sha), client)
    return {
        "the second install for the same digest succeeds and keeps one release": again == 0
        and [item.name for item in (host / "repo_releases").iterdir()] == [sha],
        "the run that was given wins": code == 0 and f"run {RUN_C} " in text and f"run {RUN_A} " not in text,
        "a repeat for the same code writes a missing record again": record_of(host, sha) == f"{RUN_C}\n",
    }


def identify_refuses_what_has_no_record_for_the_shown_release() -> dict[str, bool]:
    script, host, archives, _ = fixture()
    (path_a, sha_a), (path_b, sha_b) = archives[RUN_A], archives[RUN_B]
    call(script, host, environment(RUN_A, sha_a), StandInS3({RUN_A: path_a}))
    call(script, host, environment(RUN_B, sha_b), StandInS3({RUN_B: path_b}))
    (host / "repo_releases" / sha_b / "run_id").unlink()
    code, text = identify(script, host)
    before = (os.readlink(host / "repo"), record_of(host, sha_a))
    return {
        "repo shows a release with no record, although another release has one": code != 0 and "no code run record" in text,
        "the refusal names the shown digest and no run": sha_b[:12] in text and RUN_A not in text and "identity" not in text,
        "nothing changed": before == (f"repo_releases/{sha_b}/code", f"{RUN_A}\n") and record_of(host, sha_b) is None,
    }


def identify_refuses_a_record_that_is_not_a_run_id() -> dict[str, bool]:
    script, host, archives, _ = fixture()
    path, sha = archives[RUN_A]
    call(script, host, environment(RUN_A, sha), StandInS3({RUN_A: path}))
    outcomes = {}
    for label, content in (("empty", ""), ("text", "latest\n"), ("two lines", f"{RUN_A}\n{RUN_B}\n"), ("padded", f" {RUN_A}\n")):
        (host / "repo_releases" / sha / "run_id").write_text(content)
        code, text = identify(script, host)
        outcomes[f"a record that is {label} is refused and no run is named"] = code != 0 and "run record" in text and "identity" not in text
    return outcomes


def identify_refuses_a_host_with_no_complete_release_shown() -> dict[str, bool]:
    script, host, archives, _ = fixture()
    path, sha = archives[RUN_A]
    empty, empty_text = identify(script, host)
    call(script, host, environment(RUN_A, sha), StandInS3({RUN_A: path}))
    (host / "repo").unlink()
    (host / "repo").mkdir()
    (host / "repo" / "dags").mkdir()
    folder, folder_text = identify(script, host)
    return {
        "an empty repo folder is refused": empty != 0 and "no code run record" in empty_text and "identity" not in empty_text,
        "a checkout folder that is no release has no record": folder != 0 and "no code run record" in folder_text and "identity" not in folder_text,
    }


def restore_returns_to_the_previous_checkout() -> dict[str, bool]:
    script, host, archives, _ = fixture()
    (path_a, sha_a), (path_b, sha_b) = archives[RUN_A], archives[RUN_B]
    call(script, host, environment(RUN_A, sha_a), StandInS3({RUN_A: path_a}))
    call(script, host, environment(RUN_B, sha_b), StandInS3({RUN_B: path_b}))
    (host / "repo_releases" / sha_b / "code" / "dags" / "second.py").unlink()
    broken, _ = call(script, host, {**environment(RUN_B, sha_b), "CODE_MODE": "restore"}, StandInS3({}))
    restored = checkout_files(host) == archive_files(path_a)
    again, _ = call(script, host, {**environment(RUN_B, sha_b), "CODE_MODE": "restore"}, StandInS3({}))
    return {
        "restore succeeds": broken == 0,
        "repo shows the previous checkout again": restored,
        "a second restore changes nothing and succeeds": again == 0 and checkout_files(host) == archive_files(path_a),
    }


def restore_with_no_verified_checkout_leaves_none_selected() -> dict[str, bool]:
    script, host, archives, _ = fixture()
    path, sha = archives[RUN_A]
    call(script, host, environment(RUN_A, sha), StandInS3({RUN_A: path}))
    (host / "repo_releases" / sha / "code" / "dags" / "second.py").unlink()
    code, text = call(script, host, {**environment(RUN_A, sha), "CODE_MODE": "restore"}, StandInS3({}))
    return {
        "restore fails when no verified checkout exists": code != 0 and "no verified checkout" in text,
        "repo no longer shows the incomplete release": not (host / "repo").is_symlink(),
        "repo is an empty folder, as after host_prepare": (host / "repo").is_dir() and not any((host / "repo").iterdir()),
    }


def restore_after_a_failed_install_keeps_the_selected_checkout() -> dict[str, bool]:
    script, host, archives, _ = fixture()
    (path_a, sha_a), (_, sha_b) = archives[RUN_A], archives[RUN_B]
    call(script, host, environment(RUN_A, sha_a), StandInS3({RUN_A: path_a}))
    failed, _ = call(script, host, environment(RUN_B, sha_b), StandInS3({}, fail=True))
    code, _ = call(script, host, {**environment(RUN_B, sha_b), "CODE_MODE": "restore"}, StandInS3({}))
    return {
        "the install failed": failed != 0,
        "restore after it succeeds": code == 0,
        "repo still shows the previous checkout": checkout_files(host) == archive_files(path_a),
    }


def the_script_fits_the_run_task_override_limit() -> dict[str, bool]:
    variables = [{"name": "CODE_RUN_ID", "value": RUN_A}, {"name": "CODE_SHA256", "value": "a" * 64}, {"name": "CODE_MODE", "value": "restore"}]
    overrides = json.dumps({"containerOverrides": [{"name": "code_fetch", "command": [SCRIPT.read_text()], "environment": variables}]})
    return {"the script as a command override with its three variables stays under the 8192 characters RunTask accepts": len(overrides) <= 8192}


def damaged_selected_release() -> tuple[Any, Path, Path, str, str]:
    """A host that shows release A with one file missing, after release B was shown first; returns the script, the host, the archive of A and both digests."""
    script, host, archives, _ = fixture()
    (path_a, sha_a), (path_b, sha_b) = archives[RUN_A], archives[RUN_B]
    call(script, host, environment(RUN_B, sha_b), StandInS3({RUN_B: path_b}))
    call(script, host, environment(RUN_A, sha_a), StandInS3({RUN_A: path_a}))
    (host / "repo_releases" / sha_a / "code" / "config" / "third.json").unlink()
    return script, host, path_a, sha_a, sha_b


def repair_with_renames(script: Any, host: Path, path: Path, sha: str, stop_after: int | None) -> list[bool]:
    """Repair the damaged selected release and record after every rename whether repo still shows the readable file ``dags/marker.py``.

    With ``stop_after`` the fetch is interrupted right after that rename.
    """
    original, states = os.replace, list[bool]()

    def replace(source: Any, target: Any) -> None:
        original(source, target)
        states.append((host / "repo" / "dags" / "marker.py").is_file())
        if stop_after == len(states):
            raise KeyboardInterrupt

    worker = Worker("repair", lambda: script.run(host, FETCH_DIR, StandInS3({RUN_A: path})))
    with task_environment(environment(RUN_A, sha)), mock.patch.object(os, "replace", replace):
        worker.start()
        joined(worker)
    return states


def repair_of_the_selected_release_keeps_repo_readable_at_every_rename() -> dict[str, bool]:
    script, host, path, sha, sha_b = damaged_selected_release()
    states = repair_with_renames(script, host, path, sha, None)
    checks = {
        "a repair renames at least twice": len(states) >= 2,
        "repo shows a readable file after every rename": bool(states) and all(states),
        "the repaired checkout shows every file of the archive": checkout_files(host) == archive_files(path),
        "the previous release stays the previous link": os.readlink(host / "repo.previous") == f"repo_releases/{sha_b}/code",
    }
    for stop in range(1, len(states) + 1):
        script, host, path, sha, _ = damaged_selected_release()
        stopped = repair_with_renames(script, host, path, sha, stop)
        retry, _ = call(script, host, environment(RUN_A, sha), StandInS3({RUN_A: path}))
        checks[f"interrupted after rename {stop}: repo shows a readable file"] = (
            len(stopped) == stop and stopped[-1] and (host / "repo" / "dags" / "marker.py").is_file()
        )
        checks[f"interrupted after rename {stop}: the retry succeeds and shows every file"] = retry == 0 and checkout_files(host) == archive_files(path)
    return checks


def services_inputs_files() -> dict[str, Any]:
    """A synthetic existing rendered services inputs file, the role and bucket files of the owner note and where the cases write."""
    account = "".join(["123"] * 4)
    work = Path(tempfile.mkdtemp(prefix="services_inputs_case_"))
    role = f"arn:aws:iam::{account}:role/example_project_host_code_fetch_task_dev"
    existing = {
        "aws_profile": "example_profile",
        "aws_region": "example-region-1",
        "cluster_name": "example-cluster",
        "environment": "dev",
        "expected_account_id": account,
        "log_group_names": {"airflow": "example-log-group"},
        "mlflow_artifact_bucket": "example-mlflow-artifacts",
        "mlflow_enabled": True,
        "mlflow_host_ip": "10.0.1.20",
        "mlflow_task_role_arn": f"arn:aws:iam::{account}:role/example_project_mlflow_task_dev",
        "model_api_rate_per_minute": 120,
        "monitoring_enabled": True,
        "project_name": "example-project",
        "code_bucket_name": "example-old-bucket",
        "code_fetch_task_role_arn": f"arn:aws:iam::{account}:role/example_project_old_host_code_fetch_task_dev",
    }
    (work / "existing.json").write_text(json.dumps(existing), encoding="utf-8")
    (work / "role.json").write_text(json.dumps({"name": "example_project_host_code_fetch_task_dev", "arn": role}), encoding="utf-8")
    (work / "bucket.txt").write_text("example-data-bucket\n", encoding="utf-8")
    return {"work": work, "existing": existing, "role": role}


def run_inputs_tool(files: dict[str, Any], **changes: str) -> tuple[int, str]:
    """Run the services inputs tool on the synthetic files; ``changes`` replace an argument by name. Returns the exit code and what it printed."""
    from scripts.infrastructure import services_code_inputs

    work = files["work"]
    arguments = {"existing": work / "existing.json", "role-file": work / "role.json", "bucket-file": work / "bucket.txt", "output": work / "services_code.json"}
    arguments.update({name.replace("_", "-"): Path(value) for name, value in changes.items()})
    out = io.StringIO()
    with redirect_stdout(out), redirect_stderr(out):
        code = services_code_inputs.main([item for name, value in arguments.items() for item in (f"--{name}", str(value))])
    return code, out.getvalue()


def services_inputs_keep_every_existing_supported_setting() -> dict[str, bool]:
    from scripts.infrastructure import render_platform_config

    files = services_inputs_files()
    code, text = run_inputs_tool(files)
    output = files["work"] / "services_code.json"
    written = json.loads(output.read_text(encoding="utf-8")) if output.is_file() else {}
    wanted = {
        name: value
        for name, value in files["existing"].items()
        if name in {"mlflow_artifact_bucket", "mlflow_enabled", "mlflow_task_role_arn", "model_api_rate_per_minute", "monitoring_enabled"}
    }
    wanted.update(code_bucket_name="example-data-bucket", code_fetch_task_role_arn=files["role"])
    digest = hashlib.sha256(output.read_bytes()).hexdigest() if output.is_file() else ""
    rendered = render_platform_config.stack_preparation(output, digest, "services") if output.is_file() else {}
    return {
        "the tool succeeds": code == 0,
        "the file holds every supported existing setting and the new role and bucket, and nothing else": written.get("variables") == wanted,
        "the file names the services root and schema version": written.get("root") == "services" and written.get("schema_version") == 1,
        "the renderer accepts the file by its digest and keeps mlflow_enabled": rendered.get("mlflow_enabled") is True
        and rendered.get("code_bucket_name") == "example-data-bucket",
        "the output names no ARN and no account": "arn:aws" not in text and "".join(["123"] * 4) not in text,
        "the output names the kept settings": "mlflow_enabled" in text,
    }


def services_inputs_refuse_what_they_cannot_carry() -> dict[str, bool]:
    from scripts.infrastructure import services_code_inputs

    files = services_inputs_files()
    missing, missing_text = run_inputs_tool(files, existing=files["work"] / "absent.json")
    (files["work"] / "role.json").write_text(json.dumps({"arn": "arn:aws:iam::not-a-role"}), encoding="utf-8")
    bad_role, _ = run_inputs_tool(files)
    uncarried = sorted(services_code_inputs.declared_variables() - services_code_inputs.supported_variables() - services_code_inputs.rendered_variables())
    checks = {
        "a missing existing file is refused": missing != 0 and "existing" in missing_text,
        "a role ARN of another form is refused and nothing is written": bad_role != 0 and not (files["work"] / "services_code.json").exists(),
    }
    if uncarried:
        carried = services_inputs_files()
        (carried["work"] / "existing.json").write_text(json.dumps({**carried["existing"], uncarried[0]: "x"}), encoding="utf-8")
        refused, text = run_inputs_tool(carried)
        checks["a declared setting that no preparation file can carry is refused by name"] = refused != 0 and uncarried[0] in text
    return checks


def the_task_runs_the_script_as_its_main_program() -> dict[str, bool]:
    text = SCRIPT.read_text()
    return {
        "the script reads the host folder and the image's fetch script": 'Path("/host")' in text and 'Path("/opt/hai")' in text,
        "it exits with the result of run": "sys.exit(run(" in text,
    }


CASES: dict[str, Callable[[], dict[str, bool]]] = {
    "first_fetch_links_the_checkout": first_fetch_links_the_checkout,
    "rerun_for_the_same_code_changes_nothing": rerun_for_the_same_code_changes_nothing,
    "new_code_swaps_the_link": new_code_swaps_the_link,
    "hash_mismatch_keeps_the_old_checkout": hash_mismatch_keeps_the_old_checkout,
    "failed_download_keeps_the_old_checkout": failed_download_keeps_the_old_checkout,
    "unsafe_archive_keeps_the_old_checkout": unsafe_archive_keeps_the_old_checkout,
    "missing_archive_keeps_the_old_checkout": missing_archive_keeps_the_old_checkout,
    "no_code_keeps_an_existing_checkout": no_code_keeps_an_existing_checkout,
    "no_code_and_no_checkout_is_refused": no_code_and_no_checkout_is_refused,
    "half_given_code_is_refused": half_given_code_is_refused,
    "repo_folder_with_content_is_refused": repo_folder_with_content_is_refused,
    "the_task_runs_the_script_as_its_main_program": the_task_runs_the_script_as_its_main_program,
    "interrupted_unpacking_is_not_a_release_on_retry": interrupted_unpacking_is_not_a_release_on_retry,
    "concurrent_fetches_of_one_digest_are_serialized": concurrent_fetches_of_one_digest_are_serialized,
    "interrupted_fetch_hands_over_to_the_waiting_fetch": interrupted_fetch_hands_over_to_the_waiting_fetch,
    "fetch_that_cannot_lock_in_time_fails_and_keeps_the_checkout": fetch_that_cannot_lock_in_time_fails_and_keeps_the_checkout,
    "release_with_a_changed_file_is_installed_again": release_with_a_changed_file_is_installed_again,
    "no_code_refuses_a_selected_release_that_is_not_complete": no_code_refuses_a_selected_release_that_is_not_complete,
    "verify_accepts_only_a_complete_selected_release": verify_accepts_only_a_complete_selected_release,
    "the_install_records_its_run_beside_the_staged_digest": the_install_records_its_run_beside_the_staged_digest,
    "a_later_install_for_the_same_digest_rewrites_the_record": a_later_install_for_the_same_digest_rewrites_the_record,
    "identify_refuses_what_has_no_record_for_the_shown_release": identify_refuses_what_has_no_record_for_the_shown_release,
    "identify_refuses_a_record_that_is_not_a_run_id": identify_refuses_a_record_that_is_not_a_run_id,
    "identify_refuses_a_host_with_no_complete_release_shown": identify_refuses_a_host_with_no_complete_release_shown,
    "restore_returns_to_the_previous_checkout": restore_returns_to_the_previous_checkout,
    "restore_with_no_verified_checkout_leaves_none_selected": restore_with_no_verified_checkout_leaves_none_selected,
    "restore_after_a_failed_install_keeps_the_selected_checkout": restore_after_a_failed_install_keeps_the_selected_checkout,
    "the_script_fits_the_run_task_override_limit": the_script_fits_the_run_task_override_limit,
    "repair_of_the_selected_release_keeps_repo_readable_at_every_rename": repair_of_the_selected_release_keeps_repo_readable_at_every_rename,
    "services_inputs_keep_every_existing_supported_setting": services_inputs_keep_every_existing_supported_setting,
    "services_inputs_refuse_what_they_cannot_carry": services_inputs_refuse_what_they_cannot_carry,
}
CASE_ERRORS = (AssertionError, LookupError, OSError, TypeError, ValueError, AttributeError, NameError, ImportError)


def main() -> int:
    """Run every case, save the outcomes and return 1 when any check failed."""
    results: dict[str, dict[str, bool]] = {}
    for name, function in CASES.items():
        try:
            results[name] = function()
        except CASE_ERRORS as error:
            results[name] = {f"the case ended with {type(error).__name__}": False}
    passed = all(all(checks.values()) for checks in results.values())
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    OUTPUT.mkdir(parents=True, exist_ok=True)
    (OUTPUT / f"report_{stamp}.json").write_text(json.dumps({"run_at_utc": stamp, "results": results, "passed": passed}, indent=2, sort_keys=True) + "\n")
    for name, checks in results.items():
        failed = [check for check, ok in checks.items() if not ok]
        sys.stdout.write(f"{'PASS' if not failed else 'FAIL'} {name}{': ' + '; '.join(failed) if failed else ''}\n")
    sys.stdout.write(f"{sum(all(checks.values()) for checks in results.values())} of {len(results)} cases passed\n")
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
