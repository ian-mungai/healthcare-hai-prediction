"""Install hash-pinned native quality tools locally without extracting archive paths."""

from __future__ import annotations

import argparse
import hashlib
import http.client
import io
import json
import logging
import math
import os
import platform
import re
import secrets
import socket
import ssl
import stat
import tarfile
import tempfile
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import zipfile
from contextlib import suppress
from dataclasses import dataclass
from functools import partial
from pathlib import Path
from typing import Any, cast

ROOT = Path(__file__).resolve().parents[2]
MAX_ARCHIVE = 64 * 1024 * 1024
MAX_BINARY = 192 * 1024 * 1024  # trivy is about 160 MB.
READ_CHUNK = 1024 * 1024
# The shortest hosted job that installs tools allows 10 minutes and the tightest measured ones leave about 4.5 minutes beyond their own work.
INSTALL_DEADLINE = 180.0
TRANSIENT_STATUSES = frozenset({429, 500, 502, 503, 504})
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


@dataclass(frozen=True)
class RetryPolicy:
    """Bound a download: attempts, one total deadline, doubling waits with jitter and a cap on each attempt's wait for the network."""

    attempts: int = 5
    deadline: float = 120.0
    base_delay: float = 2.0
    max_delay: float = 30.0
    attempt_timeout: float = 60.0

    def __post_init__(self) -> None:
        seconds = (self.deadline, self.base_delay, self.max_delay, self.attempt_timeout)
        if type(self.attempts) is not int or self.attempts < 1 or not all(math.isfinite(value) and value > 0 for value in seconds):
            raise ValueError("Invalid retry policy")


RETRY_POLICY = RetryPolicy()


@dataclass(frozen=True)
class DownloadSettings:
    """Where archives may come from and how long downloads may take; the defaults are production and only tests pass others."""

    scheme: str = "https"
    host: str = "github.com"
    policy: RetryPolicy = RETRY_POLICY
    install_seconds: float = INSTALL_DEADLINE

    def __post_init__(self) -> None:
        if not math.isfinite(self.install_seconds) or self.install_seconds <= 0:
            raise ValueError("Invalid install deadline")


DOWNLOADS = DownloadSettings()


class DownloadFailed(Exception):
    """Transient download failures outlasted the retry policy; the message names the tool, the attempts, the reason and the last status."""

    def __init__(self, tool: str, attempts: int, reason: str, last: str) -> None:
        super().__init__(f"tool={tool} attempts={attempts} reason={reason} last={last}")


def transient_status(error: BaseException) -> str | None:
    """Name a failure worth retrying (a retryable HTTP status, a timeout or a lost connection), or return None for a permanent one."""
    if isinstance(error, urllib.error.HTTPError):
        return str(error.code) if error.code in TRANSIENT_STATUSES else None
    if isinstance(error, urllib.error.URLError):
        if isinstance(error.reason, ssl.SSLCertVerificationError):
            return None
        return "timeout" if isinstance(error.reason, TimeoutError) else "connection_error"
    if isinstance(error, TimeoutError):
        return "timeout"
    if isinstance(error, (ConnectionError, http.client.IncompleteRead, http.client.BadStatusLine)):
        return "connection_error"
    return None


def verified(content: bytes, limit: int, expected_sha256: str) -> bytes:
    """Return the bytes only when they fit the size limit and match the pinned hash."""
    if len(content) > limit or sha256(content) != expected_sha256:
        raise ValueError("Archive size or SHA-256 mismatch")
    return content


class DeadlineExpired(Exception):
    """The watchdog cut an attempt's connections at its deadline; no retry can follow because the deadline is also the end of the budget."""


class AttemptGuard:
    """Shut down every connection of one attempt when its deadline passes, so no blocked read, header parse or chunk-size parse outlasts it."""

    def __init__(self, seconds: float) -> None:
        self.expired = threading.Event()
        self.lock = threading.Lock()
        self.sockets: list[socket.socket] = []
        self.timer = threading.Timer(max(seconds, 0.0), self.expire)
        self.timer.name = "download_deadline_watchdog"
        self.timer.daemon = True

    def __enter__(self) -> AttemptGuard:
        self.timer.start()
        return self

    def __exit__(self, *exception: object) -> None:
        self.timer.cancel()
        self.timer.join(timeout=5)
        if self.timer.is_alive():
            raise RuntimeError("Download watchdog did not stop")

    def expire(self) -> None:
        with self.lock:
            self.expired.set()
            sockets = list(self.sockets)
        for connection in sockets:
            self.cut(connection)

    def add(self, connection: socket.socket) -> None:
        with self.lock:
            self.sockets.append(connection)
            late = self.expired.is_set()
        if late:
            self.cut(connection)

    @staticmethod
    def cut(connection: socket.socket) -> None:
        with suppress(OSError):  # The peer or the attempt may have closed it already.
            connection.shutdown(socket.SHUT_RDWR)


class GuardedHTTPConnection(http.client.HTTPConnection):
    """Hand the connected socket to the attempt's watchdog."""

    def __init__(self, *args: Any, guard: AttemptGuard, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.guard = guard

    def connect(self) -> None:
        super().connect()
        self.guard.add(cast(socket.socket, self.sock))


class GuardedHTTPSConnection(http.client.HTTPSConnection):
    """Hand the connected, wrapped socket to the attempt's watchdog."""

    def __init__(self, *args: Any, guard: AttemptGuard, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.guard = guard

    def connect(self) -> None:
        super().connect()
        self.guard.add(cast(socket.socket, self.sock))


class GuardedHTTPHandler(urllib.request.HTTPHandler):
    """Open plain HTTP connections that the watchdog can cut."""

    def __init__(self, guard: AttemptGuard) -> None:
        super().__init__()
        self.guard = guard

    def http_open(self, req: urllib.request.Request) -> Any:
        return self.do_open(partial(GuardedHTTPConnection, guard=self.guard), req)


class GuardedHTTPSHandler(urllib.request.HTTPSHandler):
    """Open HTTPS connections that the watchdog can cut."""

    def __init__(self, guard: AttemptGuard, context: ssl.SSLContext | None = None) -> None:
        super().__init__(context=context)
        self.guard = guard
        self.tls_context = context

    def https_open(self, req: urllib.request.Request) -> Any:
        return self.do_open(partial(GuardedHTTPSConnection, guard=self.guard), req, context=self.tls_context)


def fetch_once(url: str, limit: int, expected_sha256: str, timeout: float, deadline_at: float) -> bytes:
    """Make one request and verify the whole body; a watchdog cuts the connections at the deadline, whatever the server is sending."""
    with AttemptGuard(deadline_at - time.monotonic()) as guard:
        opener = urllib.request.build_opener(HTTPSRedirect(), GuardedHTTPHandler(guard), GuardedHTTPSHandler(guard))
        try:
            with opener.open(url, timeout=timeout) as response:
                announced = response.headers.get("Content-Length", "")
                chunks: list[bytes] = []
                size = 0
                while size <= limit:
                    chunk = response.read(min(READ_CHUNK, limit + 1 - size))
                    if not chunk:
                        break
                    chunks.append(chunk)
                    size += len(chunk)
            content = b"".join(chunks)
            # A body shorter than its announced length is a lost connection, not a bad hash.
            if announced.isdecimal() and size <= limit and size < int(announced):
                raise http.client.IncompleteRead(content, int(announced) - size)
            return verified(content, limit, expected_sha256)
        except (OSError, http.client.HTTPException, ValueError):
            # A cut connection surfaces as an end of stream, a reset, a short body or a closed-socket error, so the watchdog's flag decides.
            if guard.expired.is_set():
                raise DeadlineExpired from None
            raise


def download_verified(
    url: str,
    *,
    tool: str,
    expected_sha256: str,
    limit: int,
    policy: RetryPolicy = RETRY_POLICY,
    install_deadline_at: float | None = None,
) -> bytes:
    """Download and hash-check an archive, retrying only transient failures within the policy's attempts, its deadline and what remains of the install's."""
    own_deadline_at = time.monotonic() + policy.deadline
    deadline_at = own_deadline_at if install_deadline_at is None else min(own_deadline_at, install_deadline_at)
    expired = "install_deadline" if deadline_at < own_deadline_at else "deadline"
    last = "none"
    for attempt in range(1, policy.attempts + 1):
        remaining = deadline_at - time.monotonic()
        if remaining <= 0:
            raise DownloadFailed(tool, attempt - 1, expired, last)
        try:
            return fetch_once(url, limit, expected_sha256, min(policy.attempt_timeout, remaining), deadline_at)
        except DeadlineExpired:
            raise DownloadFailed(tool, attempt, expired, "timeout") from None
        except (OSError, http.client.HTTPException) as error:
            if isinstance(error, urllib.error.HTTPError):
                error.close()
            status = transient_status(error)
            if status is None:
                raise
            last = status
        if attempt == policy.attempts:
            raise DownloadFailed(tool, attempt, "attempt_limit", last)
        wait = min(policy.max_delay, policy.base_delay * 2 ** (attempt - 1)) * secrets.SystemRandom().uniform(0.5, 1.0)
        if wait >= deadline_at - time.monotonic():
            raise DownloadFailed(tool, attempt, expired, last)
        LOGGER.warning("event=quality_download_retry tool=%s attempt=%d of %d status=%s wait_seconds=%.1f", tool, attempt, policy.attempts, last, wait)
        time.sleep(wait)
    raise DownloadFailed(tool, policy.attempts, "attempt_limit", last)


def archive_bytes(
    release: dict[str, Any], directory: Path | None, tool: str, settings: DownloadSettings = DOWNLOADS, install_deadline_at: float | None = None
) -> bytes:
    """Load bounded publisher bytes and reject unexpected hashes before extraction."""
    url = urllib.parse.urlsplit(release["url"])
    if url.scheme != settings.scheme or url.hostname != settings.host or url.username or url.password or url.query or url.fragment:
        raise ValueError("Invalid release URL")
    if not re.fullmatch(r"/[\w.-]+/[\w.-]+/releases/download/[^/]+/[\w.-]+", url.path):
        raise ValueError("Expected a GitHub release asset")
    if not re.fullmatch(r"[a-f0-9]{64}", release["sha256"]):
        raise ValueError("Invalid archive hash")
    ceiling = MAX_BINARY if release["format"] == "binary" else MAX_ARCHIVE
    limit = release.get("max_archive_bytes", ceiling)
    if type(limit) is not int or not 0 < limit <= ceiling:
        raise ValueError("Invalid archive size limit")
    if directory is not None:
        with (directory / Path(url.path).name).open("rb") as source:
            return verified(source.read(limit + 1), limit, release["sha256"])
    return download_verified(
        release["url"], tool=tool, expected_sha256=release["sha256"], limit=limit, policy=settings.policy, install_deadline_at=install_deadline_at
    )


def binary_bytes(content: bytes, kind: str, name: str) -> bytes:
    """Read one named regular member; never write publisher-controlled archive paths."""
    if kind == "binary":
        return content
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


def install(manifest: Path, destination: Path, archives: Path | None, tools: list[str] | None = None, settings: DownloadSettings = DOWNLOADS) -> None:
    """Validate every asset before publishing binaries and deterministic receipts; all downloads share one install deadline."""
    install_deadline_at = time.monotonic() + settings.install_seconds
    config = json.loads(manifest.read_text(encoding="utf-8"))
    native = {"gitleaks", "tflint", "trivy", "shellcheck", "actionlint", "hadolint", "duckdb", "osv-scanner", "zizmor", "typos", "uv"}
    names = set(config["tools"])
    other_installers = {name for name, tool in config["tools"].items() if "installer" in tool}
    if config["schema_version"] != 1 or not {"gitleaks", "tflint", "trivy"} <= names or names - native - other_installers:
        raise ValueError("Unsupported tool manifest")
    selected_tools = names & native if tools is None else set(tools)
    if not selected_tools or not selected_tools <= names & native:
        raise ValueError("Unknown native tool selection")
    architecture = {"x86_64": "amd64", "AMD64": "amd64"}.get(platform.machine(), platform.machine())
    selected = f"{platform.system().lower()}_{architecture}"
    pending: list[tuple[Path, bytes, int]] = []
    if any(path.is_symlink() for path in (destination, destination / "bin", destination / "receipts")):
        raise ValueError("Symlink installation directories are not allowed")
    for name, tool in sorted(config["tools"].items()):
        if name not in selected_tools:
            continue
        if not re.fullmatch(r"\d+\.\d+\.\d+", tool["version"]):
            raise ValueError("Stable tool version required")
        release = tool["releases"][selected]
        content = binary_bytes(archive_bytes(release, archives, name, settings, install_deadline_at), release["format"], release.get("member", name))
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
    parser.add_argument("--tool", action="append", help="install only this native tool (repeat to select more)")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    try:
        install(args.manifest, args.destination, args.archive_directory, args.tool)
    except DownloadFailed as error:
        LOGGER.error("event=quality_install status=failed error_type=DownloadFailed %s", error)
        raise SystemExit(1) from None
    except (OSError, ValueError, KeyError, TypeError, tarfile.TarError, zipfile.BadZipFile, urllib.error.URLError) as error:
        LOGGER.error("event=quality_install status=failed error_type=%s", type(error).__name__)
        raise SystemExit(1) from None
    LOGGER.info("event=quality_install status=verified")


if __name__ == "__main__":
    main()
