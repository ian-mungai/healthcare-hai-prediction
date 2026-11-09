"""Exercise offline tool installation and real secret scanning with synthetic fixtures."""

from __future__ import annotations

import argparse
import copy
import hashlib
import http.server
import importlib
import importlib.metadata
import io
import json
import logging
import os
import platform
import shutil
import sys
import tarfile
import tempfile
import threading
import time
import urllib.error
import urllib.request
import zipfile
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from types import ModuleType
from typing import Any, NamedTuple, cast

from scripts.process import TimeoutExpired, run_command

ROOT = Path(__file__).resolve().parents[2]
LOGGER = logging.getLogger(__name__)


class ProcessCheckFailed(ValueError):
    """Retain sanitized expected and observed results for failed process assertions."""

    def __init__(self, evidence: dict[str, Any]) -> None:
        super().__init__("Process assertion failed")
        self.evidence = evidence


def digest(path: Path) -> str:
    """Return the SHA-256 of one fixture or implementation file."""
    return hashlib.sha256(path.read_bytes()).hexdigest()


def check(condition: bool, message: str) -> None:
    """Fail an E2E assertion without including source data in diagnostics."""
    if not condition:
        raise ValueError(message)


def run(command: list[str], cwd: Path, *, expected: int = 0, forbidden: str | None = None) -> dict[str, Any]:
    """Run an actual local process and retain only sanitized assertion metadata."""
    # GIT_ variables are dropped too: inside a commit hook Git exports GIT_INDEX_FILE, which in a linked worktree is an
    # absolute path, so a fixture repository's git add would otherwise write into the real index.
    environment = {key: value for key, value in os.environ.items() if not key.startswith(("AWS_", "BC_", "GITLEAKS_", "GIT_"))}
    # PYTHONPATH lets module entry points (python -m scripts.quality...) resolve from any working directory, including hook runs.
    environment.update(
        AWS_EC2_METADATA_DISABLED="true", PYTHONDONTWRITEBYTECODE="1", PRE_COMMIT_HOME=str(ROOT / ".tools" / "pre_commit_cache"), PYTHONPATH=str(ROOT)
    )
    result = run_command(command[0], command[1:], cwd=cwd, env=environment, timeout=120)
    evidence = {
        "expected_exit": expected,
        "observed_exit": result.returncode,
        "passed": result.returncode == expected,
        "stdout_sha256": hashlib.sha256(result.stdout.encode()).hexdigest(),
        "stderr_sha256": hashlib.sha256(result.stderr.encode()).hexdigest(),
    }
    if forbidden is not None:
        check(forbidden not in result.stdout + result.stderr, "Process disclosed synthetic sentinel")
        evidence["sentinel_absent_from_diagnostics"] = True
    if result.returncode != expected:
        raise ProcessCheckFailed(evidence)
    return evidence


def installation_cases(workspace: Path, cases: list[dict[str, Any]]) -> None:
    """Verify the installer CLI using hash-pinned local archives, without HTTP."""
    archives = workspace / "archives"
    archives.mkdir()
    selected_platform = f"{platform.system().lower()}_{'amd64' if platform.machine() in {'x86_64', 'AMD64'} else platform.machine()}"
    tools: dict[str, Any] = {}
    for name, kind in [
        ("duckdb", "zip"),
        ("gitleaks", "tar.gz"),
        ("tflint", "zip"),
        ("trivy", "tar.gz"),
        ("osv-scanner", "tar.gz"),
        ("zizmor", "tar.gz"),
        ("typos", "zip"),
    ]:
        path = archives / f"{name}.{kind}"
        content = b"synthetic executable fixture; never executed\n"
        if kind == "zip":
            with zipfile.ZipFile(path, "w") as archive:
                archive.writestr(name, content)
        else:
            with tarfile.open(path, "w:gz") as archive:
                member = tarfile.TarInfo(name)
                member.size = len(content)
                archive.addfile(member, io.BytesIO(content))
        tools[name] = {
            "version": "1.2.3",
            "releases": {
                selected_platform: {
                    "url": f"https://github.com/example_org/example_tool/releases/download/v1.2.3/{path.name}",
                    "sha256": digest(path),
                    "format": kind,
                    "member": name,
                    "max_archive_bytes": path.stat().st_size,
                }
            },
        }
    manifest = workspace / "manifest.json"
    manifest.write_text(json.dumps({"schema_version": 1, "tools": tools}), encoding="utf-8")
    destination = workspace / "installed"
    command = [
        sys.executable,
        str(ROOT / "scripts/quality/install_tools.py"),
        "--manifest",
        str(manifest),
        "--destination",
        str(destination),
        "--archive-directory",
        str(archives),
    ]
    cases.append({"name": "install_from_verified_offline_archives", **run(command, ROOT)})
    for name in ("osv-scanner", "zizmor", "typos"):
        check((destination / "bin" / name).is_file(), f"Hosted linter {name} was not installed")
        check(json.loads((destination / "receipts" / f"{name}.json").read_text())["version"] == "1.2.3", f"Hosted linter {name} receipt differs")
    before = {str(p.relative_to(destination)): (digest(p), p.stat().st_mtime_ns) for p in destination.rglob("*") if p.is_file()}
    cases.append({"name": "replay_install", **run(command, ROOT)})
    after = {str(p.relative_to(destination)): (digest(p), p.stat().st_mtime_ns) for p in destination.rglob("*") if p.is_file()}
    check(before == after, "Repeated install changed files")
    bounded = copy.deepcopy(tools)
    bounded["duckdb"]["releases"][selected_platform]["max_archive_bytes"] -= 1
    manifest.write_text(json.dumps({"schema_version": 1, "tools": bounded}), encoding="utf-8")
    cases.append({"name": "reject_duckdb_archive_over_limit", **run(command, ROOT, expected=1)})
    corrupted = copy.deepcopy(tools)
    corrupted["gitleaks"]["releases"][selected_platform]["sha256"] = "0" * 64
    manifest.write_text(json.dumps({"schema_version": 1, "tools": corrupted}), encoding="utf-8")
    cases.append({"name": "reject_hash_mismatch", **run(command, ROOT, expected=1)})
    check(
        before == {str(p.relative_to(destination)): (digest(p), p.stat().st_mtime_ns) for p in destination.rglob("*") if p.is_file()},
        "Rejected input changed installed files",
    )
    manifest.write_text(json.dumps({"schema_version": 1, "tools": tools}), encoding="utf-8")
    (destination / "bin/gitleaks").write_bytes(b"corrupted synthetic executable")
    cases.append({"name": "reject_corrupt_existing_binary", **run(command, ROOT, expected=1)})

    raw = archives / "gitleaks.bin"
    raw_content = b"synthetic raw executable fixture; never executed\n"
    raw.write_bytes(raw_content)
    raw_tools = copy.deepcopy(tools)
    raw_release = raw_tools["gitleaks"]["releases"][selected_platform]
    raw_release.update(url=f"https://github.com/example_org/example_tool/releases/download/v1.2.3/{raw.name}", format="binary", sha256="0" * 64)
    raw_manifest = workspace / "raw_manifest.json"
    raw_destination = workspace / "raw_installed"
    raw_command = [str(raw_manifest) if arg == str(manifest) else str(raw_destination) if arg == str(destination) else arg for arg in command]
    raw_manifest.write_text(json.dumps({"schema_version": 1, "tools": raw_tools}), encoding="utf-8")
    cases.append({"name": "reject_raw_binary_hash_mismatch", **run(raw_command, ROOT, expected=1)})
    check(not raw_destination.exists(), "Rejected raw binary created an installation")
    raw_release["sha256"] = digest(raw)
    raw_manifest.write_text(json.dumps({"schema_version": 1, "tools": raw_tools}), encoding="utf-8")
    cases.append({"name": "install_verified_raw_binary", **run(raw_command, ROOT)})
    check((raw_destination / "bin/gitleaks").read_bytes() == raw_content, "Raw executable bytes differ")
    receipt = json.loads((raw_destination / "receipts/gitleaks.json").read_text())
    check(receipt["archive_sha256"] == digest(raw) and receipt["binary_sha256"] == digest(raw), "Raw binary receipt differs")

    nested = archives / "nested-gitleaks.tar.gz"
    nested_content = b"synthetic nested executable fixture; never executed\n"
    with tarfile.open(nested, "w:gz") as archive:
        for name, content in (("gitleaks", b"unselected decoy"), ("release/bin/gitleaks", nested_content)):
            member = tarfile.TarInfo(name)
            member.size = len(content)
            archive.addfile(member, io.BytesIO(content))
    nested_tools = copy.deepcopy(tools)
    nested_tools["gitleaks"]["releases"][selected_platform].update(
        url=f"https://github.com/example_org/example_tool/releases/download/v1.2.3/{nested.name}",
        sha256=digest(nested),
        member="release/bin/gitleaks",
        max_archive_bytes=nested.stat().st_size,
    )
    nested_manifest = workspace / "nested_manifest.json"
    nested_manifest.write_text(json.dumps({"schema_version": 1, "tools": nested_tools}), encoding="utf-8")
    nested_destination = workspace / "nested_installed"
    nested_command = [str(nested_manifest) if arg == str(manifest) else str(nested_destination) if arg == str(destination) else arg for arg in command]
    outcome = run(nested_command, ROOT)
    check((nested_destination / "bin/gitleaks").read_bytes() == nested_content, "Installer ignored the selected nested member")
    check(not (nested_destination / "release").exists(), "Installer extracted publisher paths")
    cases.append({"name": "install_selected_nested_tar_member", **outcome})

    selected_archives = workspace / "selected_archives"
    selected_archives.mkdir()
    shutil.copyfile(archives / "gitleaks.tar.gz", selected_archives / "gitleaks.tar.gz")
    selected_destination = workspace / "selected_installed"
    for name, content in (("bin/tflint", b"existing unselected binary"), ("receipts/tflint.json", b"existing unselected receipt")):
        path = selected_destination / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)
    untouched = {
        name: (digest(selected_destination / name), (selected_destination / name).stat().st_mtime_ns) for name in ("bin/tflint", "receipts/tflint.json")
    }
    selected_command = [
        str(selected_destination) if arg == str(destination) else str(selected_archives) if arg == str(archives) else arg for arg in command
    ] + ["--tool", "gitleaks"]
    outcome = run(selected_command, ROOT)
    check(
        untouched == {name: (digest(selected_destination / name), (selected_destination / name).stat().st_mtime_ns) for name in untouched},
        "Selecting one tool changed another tool or receipt",
    )
    check((selected_destination / "bin/gitleaks").read_bytes() == b"synthetic executable fixture; never executed\n", "Selected executable bytes differ")
    check(not (selected_destination / "bin/trivy").exists(), "Unselected tool was installed")
    cases.append({"name": "tool_selection_preserves_unselected_installation", **outcome})

    # A matching hash must not make an archive's symbolic-link executable acceptable.
    malicious = archives / "gitleaks.tar.gz"
    with tarfile.open(malicious, "w:gz") as archive:
        member = tarfile.TarInfo("gitleaks")
        member.type = tarfile.SYMTYPE
        member.linkname = "../outside_fixture"
        archive.addfile(member)
    tools["gitleaks"]["releases"][selected_platform]["sha256"] = digest(malicious)
    manifest.write_text(json.dumps({"schema_version": 1, "tools": tools}), encoding="utf-8")
    rejected_destination = workspace / "rejected_install"
    unsafe_command = [str(rejected_destination) if argument == str(destination) else argument for argument in command]
    cases.append({"name": "reject_verified_archive_symlink", **run(unsafe_command, ROOT, expected=1)})
    check(not rejected_destination.exists(), "Rejected archive created an installation")


ARCHIVE_FIXTURE = b"synthetic archive fixture; never executed\n"
WRONG_FIXTURE = b"synthetic wrong bytes; never executed\n"
RESPONSE_ECHO = "SENTINEL_RESPONSE_HEADER"
QUERY_ECHO = "SENTINEL_URL_QUERY"
RUNAWAY_REQUESTS = 40


class ScriptedHandler(http.server.BaseHTTPRequestHandler):
    """Answer each request with the next scripted step, so a case controls every attempt."""

    def log_message(self, format: str, *args: Any) -> None:
        return None

    def do_GET(self) -> None:
        server = cast(Any, self.server)
        with server.guard:
            index = len(server.requests)
            server.requests.append(self.path)
        # A client that never stops retrying meets a permanent answer, so a planted unbounded loop ends and is counted.
        kind, value = server.script[min(index, len(server.script) - 1)] if index < RUNAWAY_REQUESTS else ("status", 404)
        try:
            self.answer(kind, value)
        except (BrokenPipeError, ConnectionResetError):
            self.close_connection = True

    def answer(self, kind: str, value: Any) -> None:
        if kind == "stall":
            time.sleep(value)
            kind, value = "status", 504
        if kind == "drop":
            self.close_connection = True
            return
        if kind == "status" or kind == "redirect":
            self.send_response(value if kind == "status" else 302)
            if kind == "redirect":
                self.send_header("Location", value)
            self.send_header("X-Echo", RESPONSE_ECHO)
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        body = value if kind == "body" else value[: len(value) // 2]
        self.send_response(200)
        self.send_header("Content-Length", str(len(value)))
        self.end_headers()
        self.wfile.write(body)
        self.wfile.flush()
        self.close_connection = True


@contextmanager
def loopback_server(script: list[tuple[str, Any]]) -> Iterator[tuple[str, list[str]]]:
    """Serve scripted answers on 127.0.0.1 only and always stop the server and its thread."""
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), ScriptedHandler)
    server.daemon_threads = True
    cast(Any, server).script = script
    cast(Any, server).requests = []
    cast(Any, server).guard = threading.Lock()
    thread = threading.Thread(target=server.serve_forever, name="loopback_download_server", daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_address[1]}", cast(Any, server).requests
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=10)
        check(not thread.is_alive(), "Loopback download server did not stop")


class Attempt(NamedTuple):
    """One download's result or error, request count, duration and retry log lines."""

    outcome: bytes | BaseException
    requests: int
    seconds: float
    log: list[str]


def download_attempt(
    tools: ModuleType, script: list[tuple[str, Any]], policy: Any, *, limit: int | None = None, payload: bytes = ARCHIVE_FIXTURE, query: str = ""
) -> Attempt:
    """Download once through the installer's verified download and return the result or error, the request count, the seconds and the log."""
    messages: list[str] = []

    class Capture(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            messages.append(record.getMessage())

    capture = Capture(level=logging.DEBUG)
    tools.LOGGER.addHandler(capture)
    tools.LOGGER.setLevel(logging.DEBUG)
    started = time.monotonic()
    try:
        with loopback_server(script) as (base, requests):
            opener = urllib.request.build_opener(tools.HTTPSRedirect())
            outcome: bytes | BaseException
            try:
                outcome = tools.download_verified(
                    opener,
                    f"{base}/asset.tar.gz{query}",
                    tool="example_tool",
                    expected_sha256=hashlib.sha256(payload).hexdigest(),
                    limit=len(payload) if limit is None else limit,
                    policy=policy,
                )
            except (ValueError, OSError, tools.DownloadFailed) as error:
                outcome = error
            count = len(requests)
    finally:
        tools.LOGGER.removeHandler(capture)
    return Attempt(outcome, count, time.monotonic() - started, messages)


def transient_statuses_are_retried(tools: ModuleType, policy: Any) -> dict[str, Any]:
    """Each transient answer is retried and the bytes that follow are accepted after a hash check."""
    for status in (429, 500, 502, 503, 504):
        attempt = download_attempt(tools, [("status", status), ("status", status), ("body", ARCHIVE_FIXTURE)], policy)
        check(attempt.outcome == ARCHIVE_FIXTURE and attempt.requests == 3, f"HTTP {status} was not retried until the bytes arrived")
        retries = [line for line in attempt.log if "tool=example_tool" in line and f"status={status}" in line]
        check([("attempt=1" in line, "attempt=2" in line) for line in retries] == [(True, False), (False, True)], f"HTTP {status} retries were not logged")
    return {"statuses": [429, 500, 502, 503, 504], "requests_each": 3}


def connection_failures_are_retried(tools: ModuleType, policy: Any) -> dict[str, Any]:
    """A silent close, a truncated body and a stalled answer are transient and the next attempt succeeds."""
    quick_timeout = tools.RetryPolicy(attempts=5, deadline=30.0, base_delay=0.01, max_delay=0.05, attempt_timeout=0.3)
    for name, script, used in (
        ("dropped_connection", [("drop", None), ("body", ARCHIVE_FIXTURE)], policy),
        ("truncated_body", [("truncated", ARCHIVE_FIXTURE), ("body", ARCHIVE_FIXTURE)], policy),
        ("read_timeout", [("stall", 1.5), ("body", ARCHIVE_FIXTURE)], quick_timeout),
    ):
        attempt = download_attempt(tools, script, used)
        check(attempt.outcome == ARCHIVE_FIXTURE and attempt.requests == 2, f"{name} was not retried")
    return {"failures": ["dropped_connection", "truncated_body", "read_timeout"], "requests_each": 2}


def gives_up_at_attempt_limit(tools: ModuleType, policy: Any) -> dict[str, Any]:
    """Persistent 504 answers stop at the attempt limit with an error that names the tool, the count and the status."""
    limited = tools.RetryPolicy(attempts=3, deadline=30.0, base_delay=0.01, max_delay=0.05, attempt_timeout=5.0)
    attempt = download_attempt(tools, [("status", 504)], limited)
    check(isinstance(attempt.outcome, tools.DownloadFailed), "Exhausted retries did not raise the download error")
    check(attempt.requests == 3, f"Expected exactly 3 requests, saw {attempt.requests}")
    text = str(attempt.outcome)
    check("tool=example_tool" in text and "attempts=3" in text and "reason=attempt_limit" in text and "last=504" in text, "Give-up error is not clear")
    return {"requests": attempt.requests}


def permanent_answers_are_not_retried(tools: ModuleType, policy: Any) -> dict[str, Any]:
    """A client error, a refused redirect, an oversized body and a wrong hash each end after one request."""
    for status in (400, 403, 404):
        attempt = download_attempt(tools, [("status", status)], policy)
        check(
            isinstance(attempt.outcome, urllib.error.HTTPError) and attempt.outcome.code == status and attempt.requests == 1,
            f"HTTP {status} was retried or hidden",
        )
    attempt = download_attempt(tools, [("redirect", "http://127.0.0.1:9/elsewhere")], policy)
    check(isinstance(attempt.outcome, ValueError) and "redirect" in str(attempt.outcome) and attempt.requests == 1, "A refused redirect was retried")
    attempt = download_attempt(tools, [("body", ARCHIVE_FIXTURE)], policy, limit=len(ARCHIVE_FIXTURE) - 1)
    check(isinstance(attempt.outcome, ValueError) and "mismatch" in str(attempt.outcome) and attempt.requests == 1, "An oversized body was retried")
    attempt = download_attempt(tools, [("body", WRONG_FIXTURE), ("body", ARCHIVE_FIXTURE)], policy)
    check(isinstance(attempt.outcome, ValueError) and "mismatch" in str(attempt.outcome) and attempt.requests == 1, "A hash mismatch was retried or accepted")
    return {"requests_each": 1}


def hash_is_checked_after_retry(tools: ModuleType, policy: Any) -> dict[str, Any]:
    """Wrong bytes that follow a transient failure are rejected, so no attempt skips the hash check."""
    attempt = download_attempt(tools, [("status", 504), ("body", WRONG_FIXTURE), ("body", ARCHIVE_FIXTURE)], policy)
    check(isinstance(attempt.outcome, ValueError) and "mismatch" in str(attempt.outcome) and attempt.requests == 2, "Bytes after a retry were not hash-checked")
    attempt = download_attempt(tools, [("truncated", ARCHIVE_FIXTURE), ("body", WRONG_FIXTURE)], policy)
    check(
        isinstance(attempt.outcome, ValueError) and "mismatch" in str(attempt.outcome) and attempt.requests == 2,
        "Bytes after a truncated body were not hash-checked",
    )
    return {"requests_each": 2}


def deadline_is_honoured(tools: ModuleType, policy: Any) -> dict[str, Any]:
    """With attempts to spare, the total deadline ends the retries on time with a clear error."""
    short = tools.RetryPolicy(attempts=1000, deadline=1.0, base_delay=0.3, max_delay=0.3, attempt_timeout=5.0)
    attempt = download_attempt(tools, [("status", 504)], short)
    check(
        isinstance(attempt.outcome, tools.DownloadFailed) and "reason=deadline" in str(attempt.outcome),
        "The deadline did not end the retries with a clear error",
    )
    check(2 <= attempt.requests < 10, f"Deadline ran {attempt.requests} requests")
    check(attempt.seconds < 2.5, f"Deadline of 1.0 s took {attempt.seconds:.2f} s")
    stalled = tools.RetryPolicy(attempts=1000, deadline=1.0, base_delay=0.1, max_delay=0.1, attempt_timeout=30.0)
    stall = download_attempt(tools, [("stall", 5.0)], stalled)
    check(isinstance(stall.outcome, tools.DownloadFailed) and "reason=deadline" in str(stall.outcome), "A stalled answer outlived the deadline")
    check(stall.seconds < 2.5, f"A stalled answer held the download for {stall.seconds:.2f} s")
    return {"requests": attempt.requests, "seconds": round(attempt.seconds, 2), "stalled_seconds": round(stall.seconds, 2)}


def default_policy_is_bounded(tools: ModuleType, policy: Any) -> dict[str, Any]:
    """The policy that runs in CI has a few attempts and a deadline of minutes, never an open end."""
    defaults = tools.RETRY_POLICY
    check(
        2 <= defaults.attempts <= 10 and 60 <= defaults.deadline <= 300 and 0 < defaults.attempt_timeout <= defaults.deadline,
        "Default retry policy is unbounded",
    )
    return {"attempts": defaults.attempts, "deadline_seconds": defaults.deadline}


def retry_log_hides_url_and_headers(tools: ModuleType, policy: Any) -> dict[str, Any]:
    """The retry log line carries the tool, the attempt and the status, and never the URL query, host or response headers."""
    attempt = download_attempt(tools, [("status", 504), ("body", ARCHIVE_FIXTURE)], policy, query=f"?token={QUERY_ECHO}")
    check(attempt.outcome == ARCHIVE_FIXTURE and attempt.requests == 2, "Retry with a query did not succeed")
    logged = "\n".join(attempt.log)
    check("tool=example_tool" in logged and "attempt=1" in logged and "status=504" in logged, "Retry log lacks tool, attempt or status")
    check(not any(secret in logged for secret in (QUERY_ECHO, RESPONSE_ECHO, "127.0.0.1", "token=")), "Retry log disclosed the URL or response headers")
    return {"log_lines": len(attempt.log)}


def download_retry_cases(workspace: Path, cases: list[dict[str, Any]]) -> None:
    """Verify the installer's download retry against a loopback server; every case runs, and any failure names its case."""
    tools = importlib.import_module("scripts.quality.install_tools")
    scenarios: list[tuple[str, Callable[[ModuleType, Any], dict[str, Any]]]] = [
        ("retry_succeeds_after_transient_statuses", transient_statuses_are_retried),
        ("retry_succeeds_after_connection_failures", connection_failures_are_retried),
        ("retry_gives_up_at_attempt_limit", gives_up_at_attempt_limit),
        ("no_retry_on_permanent_failures", permanent_answers_are_not_retried),
        ("hash_checked_on_every_attempt", hash_is_checked_after_retry),
        ("retry_deadline_is_honoured", deadline_is_honoured),
        ("default_retry_policy_is_bounded", default_policy_is_bounded),
        ("retry_log_hides_url_and_headers", retry_log_hides_url_and_headers),
    ]
    failed: list[str] = []
    for name, scenario in scenarios:
        try:
            policy = tools.RetryPolicy(attempts=5, deadline=30.0, base_delay=0.01, max_delay=0.05, attempt_timeout=5.0)
            cases.append({"name": name, "passed": True, **scenario(tools, policy)})
        except (ValueError, OSError, AttributeError, TypeError) as error:
            failed.append(name)
            cases.append({"name": name, "passed": False, "error_type": type(error).__name__, "error": str(error)[:300]})
    check(not failed, f"Download retry cases failed: {', '.join(failed)}")


def secret_cases(workspace: Path, cases: list[dict[str, Any]]) -> None:
    """Exercise a real Gitleaks process and pre-commit hook against a temporary index."""
    git = shutil.which("git")
    check(git is not None, "Git is required")
    repository = workspace / "repository"
    repository.mkdir()
    run([str(git), "init", "--quiet", str(repository)], ROOT)
    source = repository / "example.txt"
    source.write_text("Generic non-secret fixture\n", encoding="utf-8")
    run([str(git), "add", "example.txt"], repository)
    scanner = [sys.executable, "-m", "scripts.quality.scan_secrets", "--repository", str(repository), "--staged"]
    cases.append({"name": "clean_staged_file", **run(scanner, ROOT)})
    sentinel = hashlib.sha256(b"public synthetic scanner fixture, not an issued credential").hexdigest()
    source.write_text(f'api_key = "{sentinel}"\n', encoding="utf-8")
    run([str(git), "add", "example.txt"], repository)
    source.write_text("Working copy is clean; index still has the synthetic sentinel\n", encoding="utf-8")
    cases.append({"name": "staged_secret_cannot_hide_behind_clean_worktree", **run(scanner, ROOT, expected=1, forbidden=sentinel)})
    # Keep the actual hook definition; only relocate its executable paths into this disposable repository.
    hook_config = (ROOT / ".pre-commit-config.yaml").read_text(encoding="utf-8")
    hook_config = hook_config.replace(".venv/bin/python -m scripts.quality.scan_secrets", f"{sys.executable} -m scripts.quality.scan_secrets")
    fixture_config = repository / ".pre-commit-config.yaml"
    fixture_config.write_text(hook_config, encoding="utf-8")
    hook = [sys.executable, "-m", "pre_commit", "run", "gitleaks", "--all-files", "--config", str(fixture_config)]
    cases.append({"name": "real_pre_commit_hook_rejects_synthetic_secret", **run(hook, repository, expected=1, forbidden=sentinel)})
    run([str(git), "add", "example.txt"], repository)
    cases.append({"name": "clean_index_replay", **run(scanner, ROOT)})
    cases.append({"name": "real_pre_commit_hook_accepts_clean_index", **run(hook, repository)})


def terraform_cases(workspace: Path, cases: list[dict[str, Any]]) -> None:
    """Confirm the real static-analysis entry point rejects insecure Terraform."""
    fixture = workspace / "terraform"
    fixture.mkdir()
    (fixture / "main.tf").write_text('resource "aws_s3_bucket" "example" {\n  bucket = "example-synthetic-bucket"\n}\n', encoding="utf-8")
    command = [
        sys.executable,
        "-m",
        "scripts.quality.static_checks",
        "--directory",
        str(fixture),
        "--output-directory",
        str(workspace / "static_evidence"),
    ]
    cases.append({"name": "reject_insecure_terraform", **run(command, ROOT, expected=1)})
    report = json.loads((workspace / "static_evidence/report.json").read_text(encoding="utf-8"))
    check(report["checkov"]["failed_checks"] > 0, "Checkov did not identify missing S3 protections")
    check(report["tflint"]["returncode"] != 0, "TFLint did not identify missing provider/version declarations")
    check(report["trivy"]["failed_checks"] > 0 and report["trivy"]["returncode"] != 0, "trivy did not identify missing S3 protections")

    # Failure modes: hidden exceptions, global suppression or suppression of an unapproved control.
    scoped = workspace / "scoped_terraform"
    scoped.mkdir()
    deferred = {"CKV2_AWS_62", "CKV_AWS_144", "CKV_AWS_145", "CKV_AWS_18"}
    comments = "\n".join(f"  #checkov:skip={identifier}:Synthetic staged-development exception; review before production." for identifier in sorted(deferred))
    trivy_deferred = {"AWS-0089", "AWS-0132"}
    trivy_comments = "# Synthetic staged-development exception; review before production.\n" + "".join(
        f"#trivy:ignore:{item}\n" for item in sorted(trivy_deferred)
    )
    (scoped / "main.tf").write_text(
        trivy_comments + 'resource "aws_s3_bucket" "scoped" {\n' + comments + '\n  bucket = "example-scoped-bucket"\n}\n'
        'resource "aws_s3_bucket" "unscoped" {\n  bucket = "example-unscoped-bucket"\n}\n',
        encoding="utf-8",
    )
    evidence = workspace / "scoped_evidence"
    scoped_command = [sys.executable, "-m", "scripts.quality.static_checks", "--directory", str(scoped), "--output-directory", str(evidence)]
    outcome = run(scoped_command, ROOT, expected=1)
    scoped_report = json.loads((evidence / "report.json").read_text(encoding="utf-8"))["checkov"]
    exceptions = scoped_report.get("exceptions", [])
    check(scoped_report.get("skipped_checks") == 4, "Exception count was not retained")
    check({item["check_id"] for item in exceptions} == deferred, "Approved exception identifiers were not retained")
    check(all(item["resource"] == "aws_s3_bucket.scoped" and item.get("reason") for item in exceptions), "Exception scope or reason was lost")
    findings = {(item["resource"], item["check_id"]) for item in scoped_report["findings"]}
    check(all(("aws_s3_bucket.unscoped", identifier) in findings for identifier in deferred), "An exception suppressed another resource")
    check(("aws_s3_bucket.scoped", "CKV_AWS_21") in findings, "Unapproved versioning control was suppressed")
    trivy_report = json.loads((evidence / "report.json").read_text(encoding="utf-8"))["trivy"]
    trivy_exceptions = trivy_report["exceptions"]
    check({item["check_id"] for item in trivy_exceptions} == trivy_deferred, "Approved trivy exception identifiers were not retained")
    check(all(item["resource"] == "aws_s3_bucket.scoped" and item["reason"] for item in trivy_exceptions), "trivy exception scope or reason was lost")
    trivy_findings = {(item["resource"], item["check_id"]) for item in trivy_report["findings"]}
    check(all(("aws_s3_bucket.unscoped", identifier) in trivy_findings for identifier in trivy_deferred), "A trivy exception suppressed another resource")
    check(not any(("aws_s3_bucket.scoped", identifier) in trivy_findings for identifier in trivy_deferred), "A scoped trivy exception was not applied")
    check(("aws_s3_bucket.scoped", "AWS-0090") in trivy_findings, "Unapproved trivy versioning control was suppressed")
    cases.append({"name": "resource_scoped_exceptions_remain_visible_and_fail_closed", **outcome})


def main() -> None:
    """Run synthetic E2E cases and publish a repeatable local artifact on pass or fail."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-directory", type=Path, required=True)
    args = parser.parse_args()
    output = args.output_directory.resolve()
    output.mkdir(parents=True, exist_ok=False)
    os.umask(0o077)
    cases: list[dict[str, Any]] = []
    report: dict[str, Any] = {
        "status": "running",
        "started_at_utc": datetime.now(UTC).isoformat(),
        "cases": cases,
        "fixtures": "Generic synthetic data only",
        "aws_calls": 0,
        "tested_boundary": "Real installer, scanner and pre-commit subprocesses; offline synthetic archives; installer download retry on loopback",
        "untested": ["Publisher or AWS ingestion", "Real GitHub release hosts; download retry is proven on loopback only"],
        "reproduce": [".venv/bin/python", "-m", "scripts.quality.run_e2e", "--output-directory", "<new_directory>"],
        "prerequisites": "Install pinned requirements and project-local quality tools before running.",
        "runtime": {"python": platform.python_version(), "platform": sys.platform},
        "dependencies": {name: importlib.metadata.version(name) for name in ("pre-commit", "checkov", "ruff", "mypy")},
    }
    try:
        with tempfile.TemporaryDirectory(prefix="quality_e2e_") as temporary:
            workspace = Path(temporary)
            installation_cases(workspace, cases)
            download_retry_cases(workspace, cases)
            secret_cases(workspace, cases)
            terraform_cases(workspace, cases)
        report["status"] = "passed"
    except (OSError, ValueError, TimeoutExpired) as error:
        if isinstance(error, ProcessCheckFailed):
            cases.append({"name": "failed_process", **error.evidence})
        report.update(
            status="failed", failure_type=type(error).__name__, failure=str(error) if isinstance(error, ValueError) else "Process or filesystem failure"
        )
    finally:
        report["finished_at_utc"] = datetime.now(UTC).isoformat()
        report["cleanup"] = "Temporary synthetic workspace removed; local evidence retained. No cloud resources created."
        report["source_sha256"] = {p.name: digest(p) for p in [*sorted(Path(__file__).parent.glob("*.py")), ROOT / "scripts/process.py"]}
        report["requirements_sha256"] = digest(ROOT / "uv.lock")
        report["configuration_sha256"] = {name: digest(ROOT / name) for name in ("config/quality_tools.json", ".pre-commit-config.yaml")}
        report["native_tool_sha256"] = {
            name: digest(ROOT / ".tools/bin" / name) for name in ("duckdb", "gitleaks", "tflint", "trivy") if (ROOT / ".tools/bin" / name).is_file()
        }
        (output / "report.json").write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        (output / "evidence_manifest.json").write_text(json.dumps({"report.json": digest(output / "report.json")}) + "\n", encoding="utf-8")
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    LOGGER.info("event=quality_e2e status=%s cases=%s", report["status"], len(cases))
    raise SystemExit(0 if report["status"] == "passed" else 1)


if __name__ == "__main__":
    main()
