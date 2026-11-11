"""A secret read leaves no value in any SDK log, even at DEBUG.

Contract: the botocore SDK logs the body of every response at DEBUG, and a ``GetSecretValue`` or ``GetParameter`` body holds the decrypted value.
Every code path that reads a secret value with the real SDK must switch that logging off for the call (``scripts.sdk_logs.quiet_sdk_logs``).

Each case makes a real SDK call against a loopback server that holds a generated marker as the secret, with DEBUG logging switched on at the root and at
``botocore`` and a handler that keeps every record. The value must come back unchanged (a positive effect) and the marker must be in no record and in no
handler output. A control case shows that the same call without the protection does log the marker, so the detector can fail.

Failure modes the cases map to: a read that succeeds (the logged body), a read the service refuses (no value in the answer), logging that was already
disabled above DEBUG (the protection must not lower it) and a failed read (the protection must restore the level). No real AWS endpoint or credential is used.
"""

from __future__ import annotations

import io
import json
import logging
import queue
import secrets
import threading
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, cast

import pytest

from scripts import sdk_logs
from scripts.acquisition import api_keys, pseudonymize, task_role
from scripts.features import feast_aws
from scripts.infrastructure import secret_store
from tests.support import check

ACCOUNT = "1" * 12
FAKE_CREDENTIALS: dict[str, Any] = {"aws_access_key_id": "stand-in", "aws_secret_access_key": "stand-in", "region_name": "us-west-2"}
ROLE = "example_project_job_publisher_task_dev"
REGION = "us-west-2"


class Loopback(ThreadingHTTPServer):
    """One server for the three calls the sites make: STS identity, a Secrets Manager read and a Parameter Store read. The value is the generated marker."""

    marker: str
    calls: list[str]
    gated: bool = False
    arrivals: list[threading.Event]
    arrived: threading.Semaphore


class Handler(BaseHTTPRequestHandler):
    def log_message(self, format: str, *args: Any) -> None:
        return

    def do_POST(self) -> None:
        server = cast(Loopback, self.server)
        body = self.rfile.read(int(self.headers.get("Content-Length", "0"))).decode()
        target = self.headers.get("X-Amz-Target", "")
        if "Action=GetCallerIdentity" in body:
            server.calls.append("GetCallerIdentity")
            payload = (
                '<GetCallerIdentityResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/"><GetCallerIdentityResult>'
                f"<Arn>arn:aws:sts::{ACCOUNT}:assumed-role/{ROLE}/session</Arn><UserId>stand-in</UserId><Account>{ACCOUNT}</Account>"
                "</GetCallerIdentityResult><ResponseMetadata><RequestId>stand-in</RequestId></ResponseMetadata></GetCallerIdentityResponse>"
            ).encode()
            content_type = "text/xml"
        else:
            request = json.loads(body or "{}")
            name = str(request.get("SecretId") or request.get("Name") or (request.get("Names") or [""])[0])
            server.calls.append(target.rpartition(".")[2])
            if server.gated:
                # The caller is inside its guard: hold the answer until the case releases this arrival, so the order of the answers is the case's.
                gate = threading.Event()
                server.arrivals.append(gate)
                server.arrived.release()
                gate.wait(30)
            if target.endswith("GetSecretValue"):
                answer: dict[str, Any] = {"Name": name, "VersionId": "v1", "SecretString": server.marker, "VersionStages": ["AWSCURRENT"]}
            elif target.endswith("GetParameters"):
                answer = {"Parameters": [{"Name": name, "Type": "SecureString", "Value": server.marker, "Version": 1}], "InvalidParameters": []}
            else:
                answer = {"Parameter": {"Name": name, "Type": "SecureString", "Value": server.marker, "Version": 1}}
            payload = json.dumps(answer).encode()
            content_type = "application/x-amz-json-1.1"
        self.send_response(200)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)


@contextmanager
def loopback(marker: str) -> Iterator[Loopback]:
    server = Loopback(("127.0.0.1", 0), Handler)
    server.marker, server.calls = marker, []
    server.arrivals, server.arrived = [], threading.Semaphore(0)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield server
    finally:
        for gate in server.arrivals:
            gate.set()
        server.shutdown()
        server.server_close()
        thread.join(timeout=10)


def endpoint_of(server: Loopback) -> str:
    return f"http://127.0.0.1:{server.server_address[1]}"


class Captured:
    """Every record and the text a stream handler wrote, from DEBUG up, on the root logger and on ``botocore``."""

    def __init__(self) -> None:
        self.records: list[logging.LogRecord] = []
        self.stream = io.StringIO()

    def text(self) -> str:
        parts = [record.getMessage() for record in self.records] + [self.stream.getvalue()]
        return "\n".join(parts)


@contextmanager
def debug_logging() -> Iterator[Captured]:
    captured = Captured()

    class Keep(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            captured.records.append(record)

    keep, stream = Keep(level=logging.DEBUG), logging.StreamHandler(captured.stream)
    stream.setLevel(logging.DEBUG)
    names = ("", "botocore", "boto3", "urllib3")
    loggers = [logging.getLogger(name) for name in names]
    saved = [(item.level, item.propagate) for item in loggers]
    for item in loggers:
        item.setLevel(logging.DEBUG)
    root = logging.getLogger()
    root.addHandler(keep)
    root.addHandler(stream)
    previous_disable = logging.root.manager.disable
    logging.disable(logging.NOTSET)
    try:
        yield captured
    finally:
        logging.disable(previous_disable)
        root.removeHandler(keep)
        root.removeHandler(stream)
        for item, (level, propagate) in zip(loggers, saved, strict=True):
            item.setLevel(level)
            item.propagate = propagate


def client(service: str, server: Loopback) -> Any:
    import boto3

    session: Any = boto3.session.Session(**FAKE_CREDENTIALS)
    return session.client(service, endpoint_url=endpoint_of(server))


def read_task_key(server: Loopback, _directory: Path) -> str:
    environment = {"AWS_REGION": REGION, "EXPECTED_ACCOUNT_ID": ACCOUNT, "TASK_ROLE_NAME": ROLE, "DATA_BUCKET_NAME": "example-bucket"}
    keys = task_role.TaskKeys("bls_api_v2", environment, lambda service, _region: client(service, server))
    return keys.read_api_key("bls_api_key")


def read_api_key_in_child(server: Loopback, directory: Path) -> str:
    """The owner's key fetch (``api_keys.fetch``) with a profile of synthetic static credentials."""
    config, credentials = directory / "config", directory / "credentials"
    config.write_text(f"[profile probe]\nregion = {REGION}\n")
    credentials.write_text("[probe]\naws_access_key_id = stand-in\naws_secret_access_key = stand-in\n")
    arn = f"arn:aws:sts::{ACCOUNT}:assumed-role/{ROLE}/session"
    with pytest.MonkeyPatch.context() as patch:
        patch.setenv("AWS_CONFIG_FILE", str(config))
        patch.setenv("AWS_SHARED_CREDENTIALS_FILE", str(credentials))
        return api_keys.fetch("bls_api_key", "probe", REGION, ACCOUNT, arn, endpoint_of(server))


def read_shared_key_store(server: Loopback, _directory: Path) -> str:
    store = secret_store.SecretStore(client("ssm", server), lambda: client("secretsmanager", server))
    return store.read("bls_api_key") or ""


def read_parameter_store(server: Loopback, _directory: Path) -> str:
    store = secret_store.SecretStore(client("ssm", server), lambda: client("secretsmanager", server))
    return store.read(secret_store.REDIS_PASSWORD) or ""


def read_feast_password(server: Loopback, _directory: Path) -> str:
    return feast_aws.SsmSecretReader(client("ssm", server)).get(feast_aws.redis_secret_name())


def read_pseudonym_key(server: Loopback, _directory: Path) -> str:
    return pseudonymize.parameter_store_reader(client("ssm", server))("v1") or ""


SITES: dict[str, Callable[[Loopback, Path], str]] = {
    "task_role.TaskKeys.read_api_key": read_task_key,
    "api_keys.fetch": read_api_key_in_child,
    "secret_store.SecretStore.read (Secrets Manager)": read_shared_key_store,
    "secret_store.SecretStore.read (Parameter Store)": read_parameter_store,
    "feast_aws.SsmSecretReader.get": read_feast_password,
    "pseudonymize.parameter_store_reader": read_pseudonym_key,
}


@pytest.mark.parametrize("site", sorted(SITES))
def test_secret_read_leaves_no_value_in_debug_logs(site: str, tmp_path: Path) -> None:
    marker = "marker" + secrets.token_hex(16)
    with loopback(marker) as server, debug_logging() as captured:
        value = SITES[site](server, tmp_path)
    check(value == marker, f"{site}: the read must still return the value")
    check(any(call in {"GetSecretValue", "GetParameter", "GetParameters"} for call in server.calls), f"{site}: the secret call must have reached the server")
    check(marker not in captured.text(), f"{site}: the SDK logged the secret value")
    check(all(marker not in repr(record.__dict__) for record in captured.records), f"{site}: a log record carries the secret value")


def test_control_unprotected_sdk_call_logs_the_value() -> None:
    """The detector works: the same real call without the protection puts the marker in a log record."""
    marker = "marker" + secrets.token_hex(16)
    with loopback(marker) as server, debug_logging() as captured:
        returned = client("secretsmanager", server).get_secret_value(SecretId="bls_api_key")["SecretString"]
    check(returned == marker and marker in captured.text(), "the control read must be seen in the DEBUG log, or the other cases prove nothing")


def test_protection_keeps_a_higher_disable_level_and_restores_after_a_failure() -> None:
    logging.disable(logging.ERROR)
    try:
        with sdk_logs.quiet_sdk_logs():
            check(logging.root.manager.disable == logging.ERROR, "the protection must not lower an existing disable level")
        with pytest.raises(RuntimeError), sdk_logs.quiet_sdk_logs():
            raise RuntimeError("read failed")
        check(logging.root.manager.disable == logging.ERROR, "a failed read must restore the previous level")
    finally:
        logging.disable(logging.NOTSET)
    with sdk_logs.quiet_sdk_logs():
        check(logging.root.manager.disable >= logging.DEBUG, "the protection must switch DEBUG off")
    check(logging.root.manager.disable == logging.NOTSET, "the protection must switch logging back on")


OVERLAPS = {
    "task key and shared store": (read_task_key, read_shared_key_store),
    "pseudonym key and parameter store": (read_pseudonym_key, read_parameter_store),
    "two task keys": (read_task_key, read_task_key),
}


@pytest.mark.parametrize("pair", sorted(OVERLAPS))
def test_overlapping_secret_reads_log_no_value_and_restore_logging(pair: str, tmp_path: Path) -> None:
    """Two real reads are inside their guards together and the first answer arrives and returns while the second is still waiting for its own.

    The second answer must not be logged, logging must stay off while the second read is active and logging must be back on after both end.
    """
    marker = "marker" + secrets.token_hex(16)
    readers = OVERLAPS[pair]
    values: dict[str, str] = {}
    done: queue.Queue[str] = queue.Queue()

    def run(label: str, reader: Callable[[Loopback, Path], str], server: Loopback) -> None:
        folder = tmp_path / label
        folder.mkdir()
        values[label] = reader(server, folder)
        done.put(label)

    with loopback(marker) as server, debug_logging() as captured:
        server.gated = True
        threads = [threading.Thread(target=run, args=(f"reader_{index}", reader, server), daemon=True) for index, reader in enumerate(readers)]
        for thread in threads:
            # One at a time, so the first reader to enter its guard is the first arrival and the first to be released.
            thread.start()
            check(server.arrived.acquire(timeout=20), "each read must reach the server while inside its guard")
        server.arrivals[0].set()
        done.get(timeout=20)
        while_second_waits = logging.root.manager.disable
        server.arrivals[1].set()
        done.get(timeout=20)
        for thread in threads:
            thread.join(timeout=20)
        after_both = logging.root.manager.disable
    check(while_second_waits >= logging.DEBUG, f"{pair}: the first reader left while the second was active and switched logging back on")
    check(sorted(values.values()) == [marker, marker], f"{pair}: both reads must return the value")
    check(marker not in captured.text(), f"{pair}: the second read's response was logged")
    check(after_both == logging.NOTSET, f"{pair}: logging stays disabled after both reads end (disable={after_both})")


def test_nested_and_threaded_guards_restore_only_when_the_last_one_leaves() -> None:
    logging.disable(logging.NOTSET)
    inside: list[int] = []
    with sdk_logs.quiet_sdk_logs():
        with sdk_logs.quiet_sdk_logs():
            inside.append(logging.root.manager.disable)
        inside.append(logging.root.manager.disable)
        worker = threading.Thread(target=lambda: inside.append(_enter_and_read_level()))
        worker.start()
        worker.join(timeout=10)
        inside.append(logging.root.manager.disable)
    check(all(level >= logging.DEBUG for level in inside), f"logging stays off until the last guard leaves: {inside}")
    check(logging.root.manager.disable == logging.NOTSET, "the last guard to leave restores the level the first one saw")
    with pytest.raises(RuntimeError), sdk_logs.quiet_sdk_logs(), sdk_logs.quiet_sdk_logs():
        raise RuntimeError("read failed")
    check(logging.root.manager.disable == logging.NOTSET, "an exception inside nested guards still restores the level")


def _enter_and_read_level() -> int:
    with sdk_logs.quiet_sdk_logs():
        return logging.root.manager.disable


def test_readers_entering_together_save_the_level_once(monkeypatch: pytest.MonkeyPatch) -> None:
    """Without one lock around the first entry, a reader that arrives just after the first one raised the threshold saves that raised level as its own.

    The switch call is slowed after it has taken effect, so a second and a third reader arrive inside that window. Every reader must find the guard held,
    and the level after all of them leave must be the level before the first one entered.
    """
    original = logging.disable

    def slow_disable(level: int = logging.CRITICAL) -> None:
        original(level)
        time.sleep(0.05)

    logging.disable(logging.NOTSET)
    monkeypatch.setattr(logging, "disable", slow_disable)
    threads = [threading.Thread(target=_hold_the_guard, daemon=True) for _ in range(3)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=20)
    monkeypatch.setattr(logging, "disable", original)
    check(logging.root.manager.disable == logging.NOTSET, f"readers that entered together left the threshold at {logging.root.manager.disable}")


def _hold_the_guard() -> None:
    with sdk_logs.quiet_sdk_logs():
        time.sleep(0.2)
