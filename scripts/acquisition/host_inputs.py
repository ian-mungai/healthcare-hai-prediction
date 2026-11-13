"""The inputs of a host run, built from the code archive and checked before the run sends any request.

The archive of a commit carries the tracked bundle (``config/acquisition/baseline_run/``, with its lock) and the bootstrap seed folder
(``config/acquisition/bootstrap_seed/``, files through Git LFS). ``prepare`` checks the bundle against its lock, copies the tracked plan inputs and the
seed files that the collector plans read into the code root as real files, and checks that every expected plan input is there. It reads no bucket and
makes no request. A problem stops the run and names the files, never a digest or a value.
"""

from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path, PurePosixPath
from typing import Any

from scripts.acquisition import check_baseline_run

BUNDLE = Path("config/acquisition/baseline_run")
SEED = Path("config/acquisition/bootstrap_seed")
PLAN_INPUTS = "plan_inputs"
MANIFEST = "manifest.json"
SEED_KIND = "bootstrap_seed_manifest"
SHOWN = 12
DIGEST = re.compile(r"[0-9a-f]{64}")
POINTER_HEAD = b"version https://"
POINTER_BYTES = 1024


class InputsError(ValueError):
    """The inputs cannot start a run. The message holds fixed text and repository paths only."""


def named(problems: list[str]) -> str:
    """The problems as one sentence part: the first ones, then how many more."""
    shown = "; ".join(problems[:SHOWN])
    return shown + (f"; and {len(problems) - SHOWN} more" if len(problems) > SHOWN else "")


def plan_path(path: object) -> PurePosixPath | None:
    """A plan input path under the code root's data folder, or None."""
    if not isinstance(path, str) or not path:
        return None
    relative = PurePosixPath(path)
    return relative if not relative.is_absolute() and ".." not in relative.parts and relative.parts[0] == "data" and len(relative.parts) > 1 else None


def install(code_root: Path, relative: PurePosixPath, body: bytes) -> None:
    """Write a plan input as a real file. A file already there must be the same bytes."""
    target = code_root / relative
    if target.is_symlink() or (target.exists() and target.read_bytes() != body):
        raise InputsError("A plan input already in the code root differs from the tracked one.")
    if not target.exists():
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(body)


def install_tracked(code_root: Path) -> None:
    """Copy the tracked plan inputs of the bundle to the paths the locked plans read."""
    folder = code_root / BUNDLE / PLAN_INPUTS
    for path in sorted(item for item in folder.rglob("*") if item.is_file()):
        relative = plan_path(path.relative_to(folder).as_posix())
        if relative is None:
            raise InputsError("A tracked plan input is not under the data folder.")
        install(code_root, relative, path.read_bytes())


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


def install_seed(code_root: Path) -> None:
    """Check every seed file that a plan reads and copy it to its plan input path. Nothing is copied while any file has a problem."""
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
    for item in entries:
        relative = plan_path(item["plan_input"])
        if relative is not None:
            install(code_root, relative, (folder / item["path"]).read_bytes())


def require_expected(code_root: Path) -> None:
    """Every plan input the locked plans read is a real file in the code root, so no collector finds one missing after the run has started."""
    try:
        expected = json.loads((code_root / BUNDLE / check_baseline_run.EXPECTED_NAME).read_text(encoding="utf-8"))["expected_plan_inputs"]
    except (OSError, ValueError, KeyError, TypeError):
        raise InputsError("The list of expected plan inputs is not usable.") from None
    missing = [path for path in expected if plan_path(path) is None or not (code_root / path).is_file()]
    if missing:
        raise InputsError(f"Plan inputs are missing: {named([f'{path} is not in the code root' for path in missing])}.")


def prepare(code_root: Path) -> Path:
    """Check the bundle against its lock, install the tracked plan inputs and the seed files and return the bundle folder."""
    bundle = code_root / BUNDLE
    problems = check_baseline_run.lock_problems(bundle)
    if problems:
        raise InputsError(f"The tracked inputs differ from their lock: {named(problems)}.")
    install_tracked(code_root)
    install_seed(code_root)
    require_expected(code_root)
    return bundle
