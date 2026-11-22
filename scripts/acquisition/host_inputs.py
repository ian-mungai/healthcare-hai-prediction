"""The inputs of a host run, built from the code archive and checked before any capture request.

The archive of a commit carries the tracked bundle (``config/acquisition/baseline_run/``, with its lock) and the bootstrap seed folder
(``config/acquisition/bootstrap_seed/``, files through Git LFS). ``prepare`` checks the bundle against its lock, copies the tracked plan inputs and the
seed files that the collector plans read into the code root as real files, and checks that every expected plan input is there. It reads no bucket. Its one
network use is the download of the seed's release assets (below), which comes before any capture request. A problem stops the run and names the files, never
a digest or a value.

The seed also names files that are public assets of one GitHub release of this repository (``release_assets.json``: name, size, SHA-256 and address). The
code archive does not carry them. ``prepare`` downloads each one that the seed folder lacks through the project's reviewed transport, within its size bound,
checks size and SHA-256 and places it in the seed folder only when every asset passes. A missing asset, a size or a digest that differs stops the run before
any capture request, so a capture never reads a file that was not checked.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from pathlib import Path, PurePosixPath
from typing import Any

from scripts.acquisition import capture_mode, check_baseline_run, redownload_route, transport

BUNDLE = Path("config/acquisition/baseline_run")
SEED = Path("config/acquisition/bootstrap_seed")
PLAN_INPUTS = "plan_inputs"
MANIFEST = "manifest.json"
SEED_KIND = "bootstrap_seed_manifest"
DIGEST = re.compile(r"[0-9a-f]{64}")
POINTER_HEAD = b"version https://"
POINTER_BYTES = 1024
RELEASE_MANIFEST = "release_assets.json"
RELEASE_KIND = "bootstrap_seed_release_manifest"
# A release asset is at most 2 GiB (GitHub's own limit), and so is each download here.
ASSET_LIMIT = 2 * 2**30
ASSET_FORMATS = {".zip": "zip", ".xlsx": "xlsx", ".xls": "xls", ".pdf": "pdf", ".txt": "txt", ".csv": "csv"}
DOWNLOAD_FOLDER = ".release_download"


class InputsError(ValueError):
    """The inputs cannot start a run. The message holds fixed text and repository paths only."""


def named(problems: list[str]) -> str:
    """The problems as one sentence part. Every problem is named, because a cut list hides which files to fix."""
    return "; ".join(problems)


def plan_path(path: object) -> PurePosixPath | None:
    """A plan input path under the code root's data folder, or None."""
    if not isinstance(path, str) or not path:
        return None
    relative = PurePosixPath(path)
    return relative if not relative.is_absolute() and ".." not in relative.parts and relative.parts[0] == "data" and len(relative.parts) > 1 else None


def conflict(code_root: Path, relative: PurePosixPath, body: bytes) -> bool:
    """A file already at the path that is a link or holds other bytes."""
    target = code_root / relative
    return target.is_symlink() or (target.exists() and target.read_bytes() != body)


def tracked_files(code_root: Path) -> dict[PurePosixPath, bytes]:
    """The tracked plan inputs of the bundle, by the path the locked plans read them at."""
    folder = code_root / BUNDLE / PLAN_INPUTS
    found: dict[PurePosixPath, bytes] = {}
    for path in sorted(item for item in folder.rglob("*") if item.is_file()):
        relative = plan_path(path.relative_to(folder).as_posix())
        if relative is None:
            raise InputsError("A tracked plan input is not under the data folder.")
        found[relative] = path.read_bytes()
    return found


def is_pointer(body: bytes) -> bool:
    """A Git LFS pointer file: a short text that names a spec version and an object ID, which an LFS checkout would have replaced with the file."""
    return len(body) < POINTER_BYTES and body.startswith(POINTER_HEAD) and b"\noid sha256:" in body


def seed_problem(folder: Path, item: dict[str, Any]) -> str | None:
    """Why a seed file cannot be used, or None: missing, an unresolved pointer, no recorded digest, another size or another digest."""
    path, digest, size = item["path"], item.get("sha256"), item.get("bytes")
    found = folder / path
    if not found.is_file():
        return f"{path} is missing"
    body = found.read_bytes()
    if is_pointer(body):
        return f"{path} is an unresolved LFS pointer"
    if digest is None:
        return f"{path} has no recorded digest"
    if not isinstance(digest, str) or not DIGEST.fullmatch(digest):
        return f"{path} has an unusable recorded digest"
    if size is not None and (type(size) is not int or len(body) != size):
        return f"{path} differs from the manifest size"
    return None if hashlib.sha256(body).hexdigest() == digest else f"{path} differs from the manifest digest"


def seed_entries(code_root: Path) -> list[dict[str, Any]]:
    """The manifest entries of the seed files that a plan reads, after the manifest passes its checks against the bundle's queue."""
    try:
        manifest = json.loads((code_root / SEED / MANIFEST).read_text(encoding="utf-8"))
        queue = hashlib.sha256((code_root / BUNDLE / "queue.json").read_bytes()).hexdigest()
        kind, bound, files = manifest.get("kind"), manifest.get("queue_sha256"), manifest["files"]
    except (OSError, ValueError, KeyError, AttributeError, TypeError):
        raise InputsError("The bootstrap seed manifest is not usable.") from None
    if kind != SEED_KIND:
        raise InputsError("The bootstrap seed manifest is not a bootstrap seed manifest.")
    if bound != queue:
        raise InputsError("The bootstrap seed manifest belongs to another queue.")
    if not isinstance(files, list) or not all(isinstance(item, dict) and isinstance(item.get("path"), str) for item in files):
        raise InputsError("The bootstrap seed manifest is not usable.")
    return [item for item in files if "plan_input" in item]


def seed_files(code_root: Path) -> dict[PurePosixPath, bytes]:
    """Check every seed file that a plan reads and return its bytes by plan input path. Any problem with any file stops the run."""
    folder = code_root / SEED
    entries = seed_entries(code_root)
    problems = []
    for item in entries:
        path = PurePosixPath(item["path"])
        if not item["path"] or path.is_absolute() or ".." in path.parts:
            problems.append(f"{item['path']} is not a relative path inside the seed folder")
        elif plan_path(item["plan_input"]) is None:
            problems.append(f"{item['path']} has a plan input path that is not under the data folder")
        elif (problem := seed_problem(folder, item)) is not None:
            problems.append(problem)
    if problems:
        raise InputsError(f"The bootstrap seed is not usable: {named(problems)}.")
    return {relative: (folder / item["path"]).read_bytes() for item in entries if (relative := plan_path(item["plan_input"])) is not None}


def require_expected(code_root: Path, coming: set[PurePosixPath]) -> None:
    """Every plan input the locked plans read is a real file in the code root or about to be copied there, so no collector finds one missing after the run
    has started."""
    try:
        expected = json.loads((code_root / BUNDLE / check_baseline_run.EXPECTED_NAME).read_text(encoding="utf-8"))["expected_plan_inputs"]
    except (OSError, ValueError, KeyError, TypeError):
        raise InputsError("The list of expected plan inputs is not usable.") from None
    missing = [path for path in expected if (relative := plan_path(path)) is None or (relative not in coming and not (code_root / path).is_file())]
    if missing:
        raise InputsError(f"Plan inputs are missing: {named([f'{path} is not in the code root' for path in missing])}.")


def prepare(code_root: Path, opener: transport.ReviewedTransport | None = None, log: Callable[[dict], None] | None = None) -> Path:
    """Check the bundle against its lock and the seed against its manifest, download the seed's release assets, then copy the tracked plan inputs and the seed
    files into the code root and return the bundle folder. Nothing is copied unless every check passes, and no capture request comes before the assets
    passed."""
    bundle = code_root / BUNDLE
    problems = check_baseline_run.lock_problems(bundle)
    if problems:
        raise InputsError(f"The tracked inputs differ from their lock: {named(problems)}.")
    files = tracked_files(code_root)
    for relative, body in seed_files(code_root).items():
        if relative in files and files[relative] != body:
            raise InputsError("A plan input is both tracked and in the seed, with other bytes.")
        files[relative] = body
    require_expected(code_root, set(files))
    if any(conflict(code_root, relative, body) for relative, body in files.items()):
        raise InputsError("A plan input already in the code root differs from the tracked one.")
    fetch_release_assets(code_root, opener, log)
    for relative, body in files.items():
        target = code_root / relative
        if not target.exists():
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(body)
    return bundle


def release_entries(code_root: Path) -> list[dict[str, Any]]:
    """The release assets the seed names, after the manifest passes its checks against the bundle's queue and the seed manifest. None named means no list."""
    path = code_root / SEED / RELEASE_MANIFEST
    if not path.is_file():
        return []
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
        queue = hashlib.sha256((code_root / BUNDLE / "queue.json").read_bytes()).hexdigest()
        kind, bound, tag, assets = document.get("kind"), document.get("queue_sha256"), document["tag"], document["assets"]
        listed = {item["path"]: item.get("sha256") for item in json.loads((code_root / SEED / MANIFEST).read_text(encoding="utf-8"))["files"]}
    except (OSError, ValueError, KeyError, AttributeError, TypeError):
        raise InputsError("The seed release manifest is not usable.") from None
    if kind != RELEASE_KIND or bound != queue or not isinstance(tag, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,99}", tag):
        raise InputsError("The seed release manifest is not a seed release manifest of this queue.")
    problems: list[str] = []
    if not isinstance(assets, list):
        raise InputsError("The seed release manifest is not usable.")
    for item in assets:
        name, path_text = (item.get("name"), item.get("path")) if isinstance(item, dict) else (None, None)
        if not (isinstance(name, str) and isinstance(path_text, str)):
            raise InputsError("The seed release manifest is not usable.")
        relative = PurePosixPath(path_text)
        found = [
            "is not a plain stored file name" if re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,199}", name) is None else None,
            "has a format that has no check" if Path(name).suffix.lower() not in ASSET_FORMATS else None,
            "is not a seed path of its own name"
            if relative.is_absolute() or ".." in relative.parts or relative.parts[:1] != ("manual_units",) or relative.name != name
            else None,
            "has no size within the bound" if type(item.get("bytes")) is not int or not 0 < item["bytes"] <= ASSET_LIMIT else None,
            "has no usable digest" if not isinstance(item.get("sha256"), str) or not DIGEST.fullmatch(item["sha256"]) else None,
            "has an address other than its release download address" if item.get("url") != f"{transport.RELEASE_DOWNLOAD_PREFIX}{tag}/{name}" else None,
            "is not listed in the seed manifest with its digest" if listed.get(path_text) != item.get("sha256") else None,
        ]
        problems += [f"{name} {why}" for why in found if why is not None]
    names = [item["name"] for item in assets]
    problems += [f"{name} is listed twice" for name in sorted({name for name in names if names.count(name) > 1})]
    if problems:
        raise InputsError(f"The seed release manifest is not usable: {named(problems)}.")
    return list(assets)


def asset_problem(folder: Path, item: dict[str, Any]) -> str | None:
    """Why a seed file in place cannot stand for its release asset, or None: another size or another digest. A file that is not there has no problem."""
    found = folder / item["path"]
    if not found.exists() and not found.is_symlink():
        return None
    if found.is_symlink() or not found.is_file():
        return f"{item['name']} in the seed folder is not a plain file"
    body = found.read_bytes()
    if len(body) != item["bytes"] or hashlib.sha256(body).hexdigest() != item["sha256"]:
        return f"{item['name']} in the seed folder differs from the release manifest"
    return None


@contextmanager
def approved_hosts(code_root: Path) -> Iterator[None]:
    """The approved hosts file and its approval, checked against the queue as capture mode checks them, set for the transport while the assets download."""
    bundle = code_root / BUNDLE
    hosts, approval = bundle / "publisher_hosts_scope.json", bundle / "hosts_approval.json"
    found = redownload_route.host_problems((bundle / "queue.json").read_bytes(), hosts, approval)
    if found:
        raise InputsError(f"The approved hosts file cannot govern the release download: {named(found)}.")
    names = (transport.APPROVED_HOSTS_VARIABLE, transport.APPROVED_HOSTS_APPROVAL_VARIABLE)
    earlier = {name: os.environ.get(name) for name in names}
    os.environ[names[0]], os.environ[names[1]] = str(hosts), str(approval)
    try:
        yield
    finally:
        for name, value in earlier.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value


def download_asset(item: dict[str, Any], folder: Path, opener: transport.ReviewedTransport, deadline: float) -> tuple[Path | None, str | None]:
    """Download one asset into its own folder, within the time its size allows and before ``deadline`` (a ``time.monotonic`` time for all the downloads).

    The transport holds the request to that absolute time, headers and every read of the body, so a publisher that sends a byte at a time cannot outlast it.
    Returns the file, or no file with the problem: the transport's fixed failure code, a size or a digest.
    """
    left = deadline - time.monotonic()
    if left <= 0:
        return None, f"{item['name']} was not downloaded: the time for all the release downloads ran out"
    limits = transport.Limits(attempts=2, max_seconds=min(capture_mode.download_seconds(item["bytes"]), left), max_bytes=min(ASSET_LIMIT, item["bytes"] + 1))
    try:
        result = transport.download(item["url"], folder, item["name"], ASSET_FORMATS[Path(item["name"]).suffix.lower()], "data", limits, opener=opener)
    except transport.CaptureError as refusal:
        return None, f"{item['name']} was refused: {refusal}"
    if not result.complete:
        return None, f"{item['name']} is not served by the release ({result.failure})"
    if result.byte_count != item["bytes"]:
        return None, f"{item['name']} differs from the release manifest size"
    if result.sha256 != item["sha256"]:
        return None, f"{item['name']} differs from the release manifest digest"
    return result.path, None


def fetch_release_assets(code_root: Path, opener: transport.ReviewedTransport | None = None, log: Callable[[dict], None] | None = None) -> None:
    """Download the release assets the seed folder lacks and place them there. Every asset is checked before any is placed, and a seed file already in place
    is checked and never downloaded again."""
    assets = release_entries(code_root)
    folder = code_root / SEED
    in_place = [problem for item in assets if (problem := asset_problem(folder, item)) is not None]
    if in_place:
        raise InputsError(f"The seed holds a file that is not its release asset: {named(in_place)}.")
    pending = [item for item in assets if not (folder / item["path"]).exists()]
    if log is not None:
        log({"seed_release": {"assets": len(assets), "in_place": len(assets) - len(pending), "to_download": len(pending)}})
    if not pending:
        return
    base = opener if opener is not None else transport.ReviewedTransport(transport.PublicHTTPSHandler())
    work = folder / DOWNLOAD_FOLDER
    shutil.rmtree(work, ignore_errors=True)
    found: dict[str, Path] = {}
    problems: list[str] = []
    # One bound for all the downloads: the time each asset's size allows, added up.
    deadline = time.monotonic() + sum(capture_mode.download_seconds(item["bytes"]) for item in pending)
    try:
        with approved_hosts(code_root):
            for number, item in enumerate(pending):
                path, problem = download_asset(item, work / f"{number:03}", base, deadline)
                if path is None:
                    problems.append(problem or f"{item['name']} was not downloaded")
                else:
                    found[item["name"]] = path
        if problems:
            raise InputsError(f"The seed release assets are not usable: {named(problems)}.")
        for item in pending:
            target = folder / item["path"]
            target.parent.mkdir(parents=True, exist_ok=True)
            os.replace(found[item["name"]], target)
    finally:
        shutil.rmtree(work, ignore_errors=True)
    if log is not None:
        log({"seed_release": {"downloaded": len(pending)}})
