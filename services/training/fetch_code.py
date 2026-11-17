"""Download the code archive of one training run, check its SHA-256 and unpack it once (first container of the AWS training task).

The image carries this file at /opt/hai/fetch_code.py because the task has no other code before this step. It needs only the standard library and boto3.
CODE_RUN_ID, CODE_SHA256 and DATA_BUCKET_NAME come from the task environment. The archive is lakehouse/run_stage/<CODE_RUN_ID>/inputs/code.tar.gz.
The code run is the run whose code was staged. A run that derives from another (a layer of a chain or the training that a drift run triggers)
fetches the code of that run.
The archive is unpacked into ``code.partial`` and renamed to ``code`` only when ``code`` holds every member of it with its content, so an interrupted
unpacking never leaves a ``code`` folder. A rerun on the same workspace finds the archive with the right hash and a ``code`` folder that holds every member
of it, and does nothing; a ``code`` folder that lacks a member or differs from the archive is unpacked again.
One lock file in the workspace serializes the whole fetch of every process that uses it: the check, download, unpacking and rename, and the removal of the
temporary folder. A process that cannot take the lock within LOCK_SECONDS fails visibly and changes nothing.
The job looks for DVC at <code>/.tools/dvc/bin/dvc, so this step links that folder to the image's own DVC environment at /opt/dvc. The link lies outside the
declared code roots and does not change the code digest.
"""

from __future__ import annotations

import fcntl
import hashlib
import os
import re
import shutil
import sys
import tarfile
import time
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

# A code run ID: a commit time and eight digits of the commit (staged by CI), or a run ID <name>_<8 hex>[_<attempt>] (the run its code is staged under).
RUN_ID = re.compile(r"[0-9]{8}T[0-9]{6}Z-[a-f0-9]{8}|[a-z]+(?:_[a-z]+)*_[a-f0-9]{8}(?:_[0-9]{1,2}){0,2}")
SHA256 = re.compile(r"[a-f0-9]{64}")
BUCKET = re.compile(r"[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]")
WORK = Path("/workspace")
TOOLS = Path("/opt/dvc")
LOCK_SECONDS = 600


class FetchRefused(Exception):
    """The archive cannot be fetched or trusted."""


def sha256_of(path: Path) -> str:
    """Hash a file in blocks."""
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while block := source.read(1 << 20):
            digest.update(block)
    return digest.hexdigest()


def setting(name: str, pattern: re.Pattern[str]) -> str:
    """Return a task variable that has the expected form."""
    value = os.environ.get(name, "")
    if not pattern.fullmatch(value):
        raise FetchRefused(f"{name} is not set or has another form")
    return value


def link_tools(code: Path, tools: Path) -> None:
    """Link <code>/.tools/dvc to the image's DVC environment, unless the link exists or the image has none."""
    link = code / ".tools/dvc"
    if link.is_symlink() or not (tools / "bin/dvc").is_file():
        return
    link.parent.mkdir(exist_ok=True)
    link.symlink_to(tools)


def complete(archive: Path, code: Path) -> bool:
    """True when ``code`` holds every member of ``archive`` with the same content: folders, files (size and SHA-256) and links."""
    try:
        with tarfile.open(archive) as tar:
            for member in tar:
                path = code / member.name
                if member.isdir():
                    found = path.is_dir()
                elif member.isreg() and (source := tar.extractfile(member)) is not None:
                    found = path.is_file() and path.stat().st_size == member.size
                    if found:
                        with path.open("rb") as installed:
                            found = block_digest(source) == block_digest(installed)
                else:
                    found = member.issym() and path.is_symlink() and os.readlink(path) == member.linkname
                if not found:
                    return False
    except (OSError, tarfile.TarError):
        return False
    return True


def block_digest(source: Any) -> str:
    """The SHA-256 of an open binary stream, read in blocks."""
    digest = hashlib.sha256()
    while block := source.read(1 << 20):
        digest.update(block)
    return digest.hexdigest()


@contextmanager
def workspace_lock(work: Path) -> Iterator[None]:
    """Hold the workspace's lock file; raises FetchRefused when it is not free within LOCK_SECONDS."""
    end = time.monotonic() + LOCK_SECONDS
    with (work / ".fetch.lock").open("a") as handle:
        while True:
            try:
                fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                if time.monotonic() >= end:
                    raise FetchRefused(f"the workspace lock was held for {LOCK_SECONDS} seconds by another fetch; nothing was changed") from None
                time.sleep(0.1)
        yield


def fetch(client: Any, work: Path, tools: Path) -> None:
    """Make work/code.tar.gz match CODE_SHA256 and work/code hold its content, under the workspace lock."""
    run, expected, bucket = setting("CODE_RUN_ID", RUN_ID), setting("CODE_SHA256", SHA256), setting("DATA_BUCKET_NAME", BUCKET)
    archive, code, partial = work / "code.tar.gz", work / "code", work / "code.partial"
    with workspace_lock(work):
        try:
            if archive.is_file() and sha256_of(archive) == expected:
                if code.is_dir() and complete(archive, code):
                    link_tools(code, tools)
                    return
                # An earlier unpacking stopped or the folder changed: the archive, checked by its digest, is the truth.
                shutil.rmtree(code, ignore_errors=True)
            else:
                if code.exists():
                    raise FetchRefused("the workspace holds an unpacked code folder without a matching archive")
                key = f"lakehouse/run_stage/{run}/inputs/code.tar.gz"
                try:
                    client.download_file(bucket, key, str(archive))
                except Exception as error:
                    raise FetchRefused(f"cannot download code.tar.gz of {run}: {type(error).__name__}: {error}") from None
                if sha256_of(archive) != expected:
                    archive.unlink()
                    raise FetchRefused("code.tar.gz differs from CODE_SHA256")
            shutil.rmtree(partial, ignore_errors=True)
            try:
                with tarfile.open(archive) as tar:
                    tar.extractall(partial, filter="data")
            except (OSError, tarfile.TarError) as error:
                raise FetchRefused(f"cannot unpack code.tar.gz: {type(error).__name__}: {error}") from None
            if not complete(archive, partial):
                raise FetchRefused("the unpacked code does not match code.tar.gz")
            os.replace(partial, code)
            link_tools(code, tools)
        finally:
            shutil.rmtree(partial, ignore_errors=True)


def main(client: Any = None, work: Path = WORK, tools: Path = TOOLS) -> int:
    """Fetch with the S3 client of the task role, or with the client the caller passes, and report a refusal on standard error."""
    try:
        if client is None:
            import boto3

            client = boto3.client("s3")
        fetch(client, work, tools)
    except FetchRefused as refusal:
        sys.stderr.write(f"fetch refused: {refusal}\n")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
