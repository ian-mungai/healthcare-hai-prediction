"""Download the code archive of one training run, check its SHA-256 and unpack it once (first container of the AWS training task).

The image carries this file at /opt/hai/fetch_code.py because the task has no other code before this step. It needs only the standard library and boto3.
CODE_RUN_ID, CODE_SHA256 and DATA_BUCKET_NAME come from the task environment. The archive is lakehouse/run_stage/<CODE_RUN_ID>/inputs/code.tar.gz.
The code run is the run whose code was staged. A run that derives from another (a layer of a chain or the training that a drift run triggers)
fetches the code of that run.
A rerun on the same workspace finds the archive with the right hash and the unpacked folder, and does nothing.
The job looks for DVC at <code>/.tools/dvc/bin/dvc, so this step links that folder to the image's own DVC environment at /opt/dvc. The link lies outside the
declared code roots and does not change the code digest.
"""

from __future__ import annotations

import hashlib
import os
import re
import shutil
import sys
import tarfile
from pathlib import Path
from typing import Any

RUN_ID = re.compile(r"[0-9]{8}T[0-9]{6}Z-[a-f0-9]{8}")
SHA256 = re.compile(r"[a-f0-9]{64}")
BUCKET = re.compile(r"[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]")
WORK = Path("/workspace")
TOOLS = Path("/opt/dvc")


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


def fetch(client: Any, work: Path, tools: Path) -> None:
    """Make work/code.tar.gz match CODE_SHA256 and work/code hold its content."""
    run, expected, bucket = setting("CODE_RUN_ID", RUN_ID), setting("CODE_SHA256", SHA256), setting("DATA_BUCKET_NAME", BUCKET)
    archive, code = work / "code.tar.gz", work / "code"
    if archive.is_file() and sha256_of(archive) == expected and code.is_dir():
        link_tools(code, tools)
        return
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
    try:
        with tarfile.open(archive) as tar:
            tar.extractall(code, filter="data")
    except (OSError, tarfile.TarError) as error:
        shutil.rmtree(code, ignore_errors=True)
        raise FetchRefused(f"cannot unpack code.tar.gz: {type(error).__name__}: {error}") from None
    link_tools(code, tools)


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
