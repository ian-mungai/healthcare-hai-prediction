"""Install hash-pinned native quality tools locally without extracting archive paths."""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import logging
import os
import platform
import re
import stat
import tarfile
import tempfile
import urllib.error
import urllib.parse
import urllib.request
import zipfile
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
MAX_ARCHIVE = 64 * 1024 * 1024
MAX_BINARY = 192 * 1024 * 1024  # trivy 0.74.0 is about 161 MB.
LOGGER = logging.getLogger(__name__)


def sha256(content: bytes) -> str:
    """Hash an archive or executable for integrity and replay checks."""
    return hashlib.sha256(content).hexdigest()


class HTTPSRedirect(urllib.request.HTTPRedirectHandler):
    """Refuse redirects outside GitHub's HTTPS release asset hosts."""

    def redirect_request(self, req: Any, fp: Any, code: int, msg: str, headers: Any, newurl: str) -> Any:
        target = urllib.parse.urlsplit(newurl)
        if target.scheme != "https" or target.hostname not in {"github.com", "release-assets.githubusercontent.com", "objects.githubusercontent.com"}:
            raise ValueError("Unapproved release redirect")
        return super().redirect_request(req, fp, code, msg, headers, newurl)


def archive_bytes(release: dict[str, Any], directory: Path | None) -> bytes:
    """Load bounded publisher bytes and reject unexpected hashes before extraction."""
    url = urllib.parse.urlsplit(release["url"])
    if url.scheme != "https" or url.hostname != "github.com" or url.username or url.password or url.query or url.fragment:
        raise ValueError("Invalid release URL")
    if not re.fullmatch(r"/[\w.-]+/[\w.-]+/releases/download/[^/]+/[\w.-]+", url.path):
        raise ValueError("Expected a GitHub release asset")
    if not re.fullmatch(r"[a-f0-9]{64}", release["sha256"]):
        raise ValueError("Invalid archive hash")
    if directory is not None:
        with (directory / Path(url.path).name).open("rb") as source:
            content = source.read(MAX_ARCHIVE + 1)
    else:
        opener = urllib.request.build_opener(HTTPSRedirect())
        with opener.open(release["url"], timeout=60) as response:
            content = response.read(MAX_ARCHIVE + 1)
    if len(content) > MAX_ARCHIVE or sha256(content) != release["sha256"]:
        raise ValueError("Archive size or SHA-256 mismatch")
    return content


def binary_bytes(content: bytes, kind: str, name: str) -> bytes:
    """Read one named regular member; never write publisher-controlled archive paths."""
    if kind == "tar.gz":
        with tarfile.open(fileobj=io.BytesIO(content), mode="r:gz") as archive:
            members = [item for item in archive.getmembers() if item.name == name]
            if len(members) != 1 or not members[0].isfile() or members[0].size > MAX_BINARY:
                raise ValueError("Invalid executable member")
            stream = archive.extractfile(members[0])
            if stream is None:
                raise ValueError("Missing executable member")
            with stream:
                return stream.read(MAX_BINARY + 1)
    if kind == "zip":
        with zipfile.ZipFile(io.BytesIO(content)) as archive:
            entries = [item for item in archive.infolist() if item.filename == name]
            if len(entries) != 1 or entries[0].is_dir() or entries[0].file_size > MAX_BINARY:
                raise ValueError("Invalid executable member")
            mode = entries[0].external_attr >> 16
            if stat.S_IFMT(mode) not in {0, stat.S_IFREG}:
                raise ValueError("Non-regular executable member")
            return archive.read(entries[0])
    raise ValueError("Unsupported archive format")


def publish(path: Path, content: bytes, mode: int) -> None:
    """Publish immutable local bytes atomically; preserve identical files on replay."""
    if path.is_symlink():
        raise ValueError("Symlink destination is not allowed")
    if path.exists():
        if path.read_bytes() != content or stat.S_IMODE(path.stat().st_mode) != mode:
            raise ValueError("Existing installation differs; review before replacing it")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=path.parent, delete=False) as stream:
        temporary = Path(stream.name)
        stream.write(content)
        stream.flush()
        os.fsync(stream.fileno())
    try:
        temporary.chmod(mode)
        try:
            os.link(temporary, path)
        except FileExistsError:
            if path.is_symlink() or path.read_bytes() != content:
                raise ValueError("Concurrent installation differs") from None
    finally:
        temporary.unlink(missing_ok=True)


def install(manifest: Path, destination: Path, archives: Path | None) -> None:
    """Validate every asset before publishing binaries and deterministic receipts."""
    config = json.loads(manifest.read_text(encoding="utf-8"))
    if config["schema_version"] != 1 or set(config["tools"]) != {"gitleaks", "tflint", "trivy"}:
        raise ValueError("Unsupported tool manifest")
    architecture = {"x86_64": "amd64", "AMD64": "amd64"}.get(platform.machine(), platform.machine())
    selected = f"{platform.system().lower()}_{architecture}"
    pending: list[tuple[Path, bytes, int]] = []
    if any(path.is_symlink() for path in (destination, destination / "bin", destination / "receipts")):
        raise ValueError("Symlink installation directories are not allowed")
    for name, tool in sorted(config["tools"].items()):
        if not re.fullmatch(r"\d+\.\d+\.\d+", tool["version"]):
            raise ValueError("Stable tool version required")
        release = tool["releases"][selected]
        content = binary_bytes(archive_bytes(release, archives), release["format"], name)
        if not content or len(content) > MAX_BINARY:
            raise ValueError("Invalid executable size")
        receipt = {"version": tool["version"], "platform": selected, "archive_sha256": release["sha256"], "binary_sha256": sha256(content)}
        pending.extend(
            [
                (destination / "bin" / name, content, 0o700),
                (destination / "receipts" / f"{name}.json", (json.dumps(receipt, sort_keys=True) + "\n").encode(), 0o600),
            ]
        )
    for path, content, mode in pending:
        if path.is_symlink() or (path.exists() and (path.read_bytes() != content or stat.S_IMODE(path.stat().st_mode) != mode)):
            raise ValueError("Existing installation differs; no files were replaced")
    for path, content, mode in pending:
        publish(path, content, mode)


def main() -> None:
    """Install approved project-scoped tools, with sanitized failure diagnostics."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, default=ROOT / "config/quality_tools.json")
    parser.add_argument("--destination", type=Path, default=ROOT / ".tools")
    parser.add_argument("--archive-directory", type=Path)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    try:
        install(args.manifest, args.destination, args.archive_directory)
    except (OSError, ValueError, KeyError, TypeError, tarfile.TarError, zipfile.BadZipFile, urllib.error.URLError) as error:
        LOGGER.error("event=quality_install status=failed error_type=%s", type(error).__name__)
        raise SystemExit(1) from None
    LOGGER.info("event=quality_install status=verified")


if __name__ == "__main__":
    main()
