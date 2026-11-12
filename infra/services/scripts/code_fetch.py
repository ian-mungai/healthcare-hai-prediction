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
SELECTED = re.compile(r"repo_releases/([a-f0-9]{64}(?:\.r[0-9]+)?)/code")
LOCK_SECONDS = 600


def same(first: Any, second: Any) -> bool:
    return hashlib.file_digest(first, "sha256").digest() == hashlib.file_digest(second, "sha256").digest()


def verified(release: Path) -> bool:
    """True when the release folder holds the archive with the digest that starts its name and a code folder with every member of it."""
    try:
        with (release / "code.tar.gz").open("rb") as stream:
            if hashlib.file_digest(stream, "sha256").hexdigest() != release.name[:64]:
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
    link, current, releases = host / "repo", selected(host), host / "repo_releases"
    if not run_id:
        if current is not None and not verified(releases / current):
            sys.stderr.write(f"code_fetch refused: repo shows release {current[:12]}, which is not complete\n")
            return 1
        if (link / "dags").is_dir():
            sys.stdout.write("code_fetch: no code given; kept the checkout\n")
            return 0
        sys.stderr.write("code_fetch refused: the host has no checkout; give --code-run and --code-sha256\n")
        return 1
    if link.exists() and not link.is_symlink() and any(link.iterdir()):
        sys.stderr.write("code_fetch refused: repo is a folder with content\n")
        return 1
    releases.mkdir(parents=True, exist_ok=True)
    partial = releases / f"{digest}.partial"
    shutil.rmtree(partial, ignore_errors=True)
    folders = sorted((item for item in releases.iterdir() if re.fullmatch(digest + r"(\.r[0-9]+)?", item.name)), key=lambda item: int(item.name[66:] or 0))
    good = next((item for item in reversed(folders) if verified(item)), None)
    if good is None:
        try:
            partial.mkdir()
            sys.path.insert(0, str(fetch_dir))
            code = int(importlib.import_module("fetch_code").main(client, partial, partial / "no_tools"))
            if code == 0 and not verified(partial):
                sys.stderr.write("code_fetch refused: the release does not match its archive\n")
                code = 1
            if code != 0:
                return code
            good = releases / (f"{digest}.r{int(folders[-1].name[66:] or 0) + 1}" if folders else digest)
            os.replace(partial, good)
        finally:
            shutil.rmtree(partial, ignore_errors=True)
    if current == good.name:
        sys.stdout.write(f"code_fetch: repo already shows {digest[:12]}\n")
        return 0
    if link.is_symlink():
        if (current or "")[:64] != digest:
            show(host, "repo.previous", os.readlink(link))
    else:
        (host / "repo.previous").unlink(missing_ok=True)
    show(host, "repo", f"repo_releases/{good.name}/code")
    sys.stdout.write(f"code_fetch: repo now shows run {run_id} code {digest[:12]}\n")
    return 0


def verify(host: Path, digest: str) -> int:
    now = selected(host)
    if now is None or now[:64] != digest:
        sys.stderr.write("code_fetch: repo does not show this release\n")
    elif not verified(host / "repo_releases" / now):
        sys.stderr.write("code_fetch: the release does not hold every file of its archive\n")
    else:
        sys.stdout.write(f"code_fetch: ok {digest[:12]}\n")
        return 0
    return 1


def restore(host: Path, digest: str) -> int:
    link, current, earlier = host / "repo", selected(host), selected(host, "repo.previous")
    if current is not None and current[:64] != digest and verified(host / "repo_releases" / current):
        sys.stdout.write(f"code_fetch: repo shows the verified release {current[:12]}\n")
        return 0
    if current is None and (link / "dags").is_dir():
        sys.stdout.write("code_fetch: repo is a checkout folder\n")
        return 0
    if earlier is not None and earlier != current and verified(host / "repo_releases" / earlier):
        show(host, "repo", f"repo_releases/{earlier}/code")
        sys.stdout.write(f"code_fetch: repo shows the previous release {earlier[:12]} again\n")
        return 0
    if link.is_symlink():
        link.unlink()
    link.mkdir(exist_ok=True)
    sys.stderr.write("code_fetch: no verified checkout; repo is an empty folder\n")
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
        sys.stderr.write("code_fetch refused: unknown CODE_MODE\n")
        return 1
    if (bool(run_id) != bool(digest) and mode == "install") or (digest and not SHA256.fullmatch(digest)) or (mode == "verify" and not digest):
        sys.stderr.write("code_fetch refused: CODE_RUN_ID and a 64-hex CODE_SHA256 go together\n")
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
