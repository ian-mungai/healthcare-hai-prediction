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
from collections.abc import Callable
from contextlib import redirect_stderr, redirect_stdout
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = REPO_ROOT / "infra" / "services" / "scripts" / "code_fetch.py"
FETCH_DIR = REPO_ROOT / "services" / "training"
OUTPUT = REPO_ROOT / "data" / "e2e" / "code_fetch"
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
    """A code archive with a dags folder whose one file holds ``marker``; returns its path and SHA-256."""
    path = folder / f"code_{marker}.tar.gz"
    with tarfile.open(path, "w:gz") as tar:
        data = f"MARKER = {marker!r}\n".encode()
        info = tarfile.TarInfo("dags/marker.py")
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
