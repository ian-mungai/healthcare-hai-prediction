"""Code one-shot of the host: CODE_MODE install (default), verify or restore the checkout /host/repo; modes: infra/services/code_fetch.tf."""

from __future__ import annotations

import fcntl
import hashlib
import importlib
import os
import re
import shutil
import sys
import tarfile
import time
from pathlib import Path
from typing import Any

SHA256 = re.compile(r"[a-f0-9]{64}")
SELECTED = re.compile(r"repo_releases/([a-f0-9]{64})/code")
LOCK_SECONDS = 600


def same(first: Any, second: Any) -> bool:
    return hashlib.file_digest(first, "sha256").digest() == hashlib.file_digest(second, "sha256").digest()


def verified(release: Path, digest: str) -> bool:
    try:
        with (release / "code.tar.gz").open("rb") as stream:
            if hashlib.file_digest(stream, "sha256").hexdigest() != digest:
                return False
            stream.seek(0)
            with tarfile.open(fileobj=stream) as tar:
                for member in tar:
                    path = release / "code" / member.name
                    if member.isreg() and (source := tar.extractfile(member)) is not None:
                        with path.open("rb") as installed:
                            found = same(source, installed)
                    else:
                        found = path.is_dir() if member.isdir() else member.issym() and path.is_symlink() and os.readlink(path) == member.linkname
                    if not found:
                        return False
    except (OSError, tarfile.TarError):
        return False
    return True


def selected(host: Path, name: str = "repo") -> str | None:
    found = SELECTED.fullmatch(os.readlink(host / name)) if (host / name).is_symlink() else None
    return found.group(1) if found else None


def show(host: Path, name: str, target: str) -> None:
    link, staged = host / name, host / f"{name}.next"
    if link.is_dir() and not link.is_symlink():
        link.rmdir()
    staged.unlink(missing_ok=True)
    staged.symlink_to(target)
    os.replace(staged, link)


def install(host: Path, fetch_dir: Path, client: Any, run_id: str, digest: str) -> int:
    link, current = host / "repo", selected(host)
    if not run_id:
        if current is not None and not verified(host / "repo_releases" / current, current):
            sys.stderr.write(f"code_fetch refused: repo shows release {current[:12]}, which is not complete\n")
            return 1
        if (link / "dags").is_dir():
            sys.stdout.write("code_fetch: no code given; kept the checkout\n")
            return 0
        sys.stderr.write("code_fetch refused: the host has no checkout; give --code-run and --code-sha256\n")
        return 1
    if link.exists() and not link.is_symlink() and any(link.iterdir()):
        sys.stderr.write("code_fetch refused: repo is a folder with content; move it aside first\n")
        return 1
    releases = host / "repo_releases"
    releases.mkdir(parents=True, exist_ok=True)
    release, partial, replaced = releases / digest, releases / f"{digest}.partial", releases / f"{digest}.replaced"
    shutil.rmtree(replaced, ignore_errors=True)
    if not verified(release, digest):
        try:
            shutil.rmtree(partial, ignore_errors=True)
            partial.mkdir()
            sys.path.insert(0, str(fetch_dir))
            code = int(importlib.import_module("fetch_code").main(client, partial, partial / "no_tools"))
            if code == 0 and not verified(partial, digest):
                sys.stderr.write("code_fetch refused: the unpacked release does not match its archive\n")
                code = 1
            if code != 0:
                return code
            if release.exists():
                os.replace(release, replaced)
            os.replace(partial, release)
        finally:
            shutil.rmtree(partial, ignore_errors=True)
            shutil.rmtree(replaced, ignore_errors=True)
    if current == digest:
        sys.stdout.write(f"code_fetch: repo already shows {digest[:12]}\n")
        return 0
    if link.is_symlink():
        show(host, "repo.previous", os.readlink(link))
    else:
        (host / "repo.previous").unlink(missing_ok=True)
    show(host, "repo", f"repo_releases/{digest}/code")
    sys.stdout.write(f"code_fetch: repo now shows run {run_id} code {digest[:12]}\n")
    return 0


def verify(host: Path, digest: str) -> int:
    if selected(host) != digest:
        sys.stderr.write("code_fetch verify failed: repo does not show this release\n")
    elif not verified(host / "repo_releases" / digest, digest):
        sys.stderr.write("code_fetch verify failed: the release does not hold every file of its archive\n")
    else:
        sys.stdout.write(f"code_fetch verify: ok {digest[:12]}\n")
        return 0
    return 1


def restore(host: Path, digest: str) -> int:
    link, current, earlier = host / "repo", selected(host), selected(host, "repo.previous")
    if current is not None and current != digest and verified(host / "repo_releases" / current, current):
        sys.stdout.write(f"code_fetch restore: repo shows the verified release {current[:12]}\n")
        return 0
    if current is None and (link / "dags").is_dir():
        sys.stdout.write("code_fetch restore: repo is a checkout folder\n")
        return 0
    if earlier is not None and earlier != current and verified(host / "repo_releases" / earlier, earlier):
        show(host, "repo", f"repo_releases/{earlier}/code")
        sys.stdout.write(f"code_fetch restore: repo shows the previous release {earlier[:12]} again\n")
        return 0
    if link.is_symlink():
        link.unlink()
    link.mkdir(exist_ok=True)
    sys.stderr.write("code_fetch restore failed: no verified checkout; repo is an empty folder\n")
    return 1


def lock(host: Path) -> Any:
    handle, end = (host / ".code_fetch.lock").open("a"), time.monotonic() + LOCK_SECONDS
    while True:
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
            return handle
        except BlockingIOError:
            if time.monotonic() >= end:
                handle.close()
                raise TimeoutError(f"the host lock was held for {LOCK_SECONDS} seconds") from None
            time.sleep(0.1)


def run(host: Path, fetch_dir: Path, client: Any = None) -> int:
    mode, run_id, digest = os.environ.get("CODE_MODE", "install"), os.environ.get("CODE_RUN_ID", ""), os.environ.get("CODE_SHA256", "")
    if mode not in {"install", "verify", "restore"}:
        sys.stderr.write("code_fetch refused: CODE_MODE is not install, verify or restore\n")
        return 1
    if (bool(run_id) != bool(digest) and mode == "install") or (digest and not SHA256.fullmatch(digest)) or (mode == "verify" and not digest):
        sys.stderr.write("code_fetch refused: give both a run ID and a 64-character lowercase hex code digest\n")
        return 1
    try:
        handle = lock(host)
    except TimeoutError as error:
        sys.stderr.write(f"code_fetch refused: {error}\n")
        return 1
    try:
        return install(host, fetch_dir, client, run_id, digest) if mode == "install" else verify(host, digest) if mode == "verify" else restore(host, digest)
    finally:
        handle.close()


if __name__ == "__main__":
    sys.exit(run(Path("/host"), Path("/opt/hai")))
