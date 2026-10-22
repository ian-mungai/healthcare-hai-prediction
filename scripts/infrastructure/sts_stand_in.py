"""A loopback stand-in for the AWS command ``aws sts assume-role``, for offline end-to-end cases; it never leaves the machine.

It receives the command line a test registry's launcher would have run and answers as the command would: the exit code, the JSON output or the
error text. It checks the profile, the region and the role ARN, refuses a role it does not know the way STS refuses a missing role and refuses a
duration the role cannot give. Each answer carries new random values. ``log`` keeps the role, the session name, the duration, the profile and the
region of each request, never a value; ``issued`` keeps the values so a case can search for them.
"""

from __future__ import annotations

import json
import secrets
import threading
import time
import urllib.parse
from datetime import UTC, datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, cast

from orchestration import pipeline_session

STALL_LIMIT_SECONDS = 60.0
ACCOUNT = "123456789012"
PROFILE = "stand-in-profile"
REGION = "stand-in-region"


def option_map(argv: list[str]) -> dict[str, str]:
    """The ``--name value`` pairs of a command line."""
    return {argv[i]: argv[i + 1] for i in range(len(argv) - 1) if argv[i].startswith("--")}


class StandInStsServer(ThreadingHTTPServer):
    """The HTTP server; it carries the stand-in whose state the handler reads and changes."""

    stand_in: StandInSts


class StandInStsHandler(BaseHTTPRequestHandler):
    """Answers the command line of ``aws sts assume-role``."""

    def log_message(self, format: str, *args: Any) -> None:
        return

    def reply(self, returncode: int, stdout: str = "", stderr: str = "") -> None:
        """Send the command's exit code and output."""
        payload = json.dumps({"returncode": returncode, "stdout": stdout, "stderr": stderr}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def do_POST(self) -> None:
        stand_in = cast(StandInStsServer, self.server).stand_in
        argv = [str(part) for part in json.loads(self.rfile.read(int(self.headers.get("Content-Length", "0"))) or b"{}").get("argv", [])]
        if stand_in.stalled:
            stand_in.released.wait(STALL_LIMIT_SECONDS)
        with stand_in.lock:
            code, out, err = stand_in.answer(argv)
        self.reply(code, out, err)


class StandInSts:
    """Knows the roles and what each may give; counts requests and refuses what ``denied`` names.

    ``roles`` maps a role name to its longest session in seconds. A role in ``denied`` is refused as access denied. While ``stalled`` is set a request
    waits until ``released`` is set. ``short_by`` takes seconds off the end time of every answer. ``malformed`` names a fault in the answer:
    ``no_credentials``, ``empty_token``, ``bad_expiry`` or ``not_an_object``.
    """

    def __init__(self, roles: dict[str, int] | None = None, account: str = ACCOUNT, profile: str = PROFILE, region: str = REGION) -> None:
        self.roles = dict(roles) if roles is not None else dict.fromkeys(pipeline_session.ROLE_NAMES.values(), pipeline_session.MAX_SESSION_SECONDS)
        self.account, self.profile, self.region = account, profile, region
        self.denied: set[str] = set()
        self.stalled = False
        self.released = threading.Event()
        self.short_by = 0
        self.malformed: str | None = None
        self.log: list[dict[str, Any]] = []
        self.issued: list[dict[str, str]] = []
        self.lock = threading.Lock()
        self.server = StandInStsServer(("127.0.0.1", 0), StandInStsHandler)
        self.server.stand_in = self
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)

    @property
    def endpoint(self) -> str:
        """The base URL clients use."""
        return f"http://127.0.0.1:{self.server.server_address[1]}"

    @staticmethod
    def refusal(code: str, text: str) -> tuple[int, str, str]:
        """The exit code and error text of a refused request."""
        return 254, "", f"\nAn error occurred ({code}) when calling the AssumeRole operation: {text}\n"

    def answer(self, argv: list[str]) -> tuple[int, str, str]:
        """The exit code, output and error text for one command line."""
        if argv[:3] != ["aws", "sts", "assume-role"]:
            return 252, "", "usage: aws [options] <command> <subcommand> [<subcommand> ...] [parameters]"
        options = option_map(argv)
        if options.get("--profile") != self.profile:
            return 255, "", f"\nThe config profile ({options.get('--profile')}) could not be found\n"
        if options.get("--region") != self.region:
            return 255, "", "\nThe region is not the stand-in's\n"
        arn, session_name, duration = options.get("--role-arn", ""), options.get("--role-session-name", ""), options.get("--duration-seconds", "")
        if not arn or not session_name or not duration.isdigit():
            return 252, "", "the following arguments are required: --role-arn, --role-session-name, --duration-seconds"
        role = arn.removeprefix(f"arn:aws:iam::{self.account}:role/") if arn.startswith(f"arn:aws:iam::{self.account}:role/") else ""
        self.log.append(
            {"role": role, "session_name": session_name, "duration": int(duration), "profile": options.get("--profile"), "region": options.get("--region")}
        )
        if role not in self.roles or role in self.denied:
            return self.refusal("AccessDenied", "the stand-in refuses this role")
        if not pipeline_session.MIN_SESSION_SECONDS <= int(duration) <= self.roles[role]:
            return self.refusal("ValidationError", "The requested DurationSeconds exceeds the MaxSessionDuration set for this role.")
        values = {
            "role": role,
            "AccessKeyId": "ASIA" + secrets.token_hex(8).upper(),
            "SecretAccessKey": secrets.token_urlsafe(30),
            "SessionToken": secrets.token_urlsafe(60),
        }
        self.issued.append(values)
        ends = datetime.fromtimestamp(time.time() + int(duration) - self.short_by, UTC).isoformat(timespec="seconds")
        credentials: dict[str, Any] = {k: values[k] for k in ("AccessKeyId", "SecretAccessKey", "SessionToken")} | {"Expiration": ends}
        if self.malformed == "no_credentials":
            return 0, json.dumps({"AssumedRoleUser": {}}), ""
        if self.malformed == "empty_token":
            credentials["SessionToken"] = ""
        if self.malformed == "bad_expiry":
            credentials["Expiration"] = "not a time"
        if self.malformed == "not_an_object":
            return 0, json.dumps([1]), ""
        return 0, json.dumps({"Credentials": credentials, "AssumedRoleUser": {"AssumedRoleId": "stand-in", "Arn": arn}}), ""

    def requests(self) -> list[dict[str, Any]]:
        """Every request that arrived, in order, without any value."""
        with self.lock:
            return [dict(entry) for entry in self.log]

    def __enter__(self) -> StandInSts:
        self.thread.start()
        return self

    def __exit__(self, *_: object) -> None:
        self.released.set()
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=10)


WIRE_PROFILE = "wire-profile"
WIRE_REGION = "us-west-2"
WIRE_KEY_ID = "wire-profile-key-id"
WIRE_KEY_VALUE = "wire-profile-key-value"


class WireStsServer(ThreadingHTTPServer):
    """The HTTP server of the wire stand-in; it carries the stand-in whose state the handler changes."""

    stand_in: WireSts


class WireStsHandler(BaseHTTPRequestHandler):
    """Answers the request the real AWS command sends for ``sts assume-role``: a form post in the query protocol, with an XML answer."""

    def log_message(self, format: str, *args: Any) -> None:
        return

    def do_POST(self) -> None:
        stand_in = cast(WireStsServer, self.server).stand_in
        form = urllib.parse.parse_qs(self.rfile.read(int(self.headers.get("Content-Length", "0"))).decode())
        values = stand_in.issue({key: items[0] for key, items in form.items()})
        document = (
            '<AssumeRoleResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/"><AssumeRoleResult><Credentials>'
            f"<AccessKeyId>{values['AccessKeyId']}</AccessKeyId><SecretAccessKey>{values['SecretAccessKey']}</SecretAccessKey>"
            f"<SessionToken>{values['SessionToken']}</SessionToken><Expiration>{values['Expiration']}</Expiration></Credentials>"
            f"<AssumedRoleUser><AssumedRoleId>wire:stand-in</AssumedRoleId><Arn>arn:aws:sts::{ACCOUNT}:assumed-role/wire/stand-in</Arn>"
            "</AssumedRoleUser></AssumeRoleResult><ResponseMetadata><RequestId>wire</RequestId></ResponseMetadata></AssumeRoleResponse>"
        ).encode()
        self.send_response(200)
        self.send_header("Content-Type", "text/xml")
        self.send_header("Content-Length", str(len(document)))
        self.end_headers()
        self.wfile.write(document)


class WireSts:
    """A loopback stand-in for the AWS endpoint itself, so the real AWS command can run its ``sts assume-role`` child offline.

    ``requests`` keeps the parameters of each request that arrived, never a value. ``issued`` keeps the values each answer carried, so a case can search
    the command's history files for them. A case points the child at ``endpoint`` and gives it a temporary HOME; nothing leaves the machine.
    """

    def __init__(self) -> None:
        self.requests: list[dict[str, str]] = []
        self.issued: list[dict[str, str]] = []
        self.lock = threading.Lock()
        self.server = WireStsServer(("127.0.0.1", 0), WireStsHandler)
        self.server.stand_in = self
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)

    @property
    def endpoint(self) -> str:
        """The base URL the AWS command is told to use."""
        return f"http://127.0.0.1:{self.server.server_address[1]}"

    def issue(self, form: dict[str, str]) -> dict[str, str]:
        """Record a request and return new random values with an end time one hour away."""
        values = {
            "AccessKeyId": "ASIA" + secrets.token_hex(8).upper(),
            "SecretAccessKey": secrets.token_urlsafe(30),
            "SessionToken": secrets.token_urlsafe(60),
            "Expiration": datetime.fromtimestamp(time.time() + 3600, UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        }
        with self.lock:
            self.requests.append({key: form.get(key, "") for key in ("Action", "RoleArn", "RoleSessionName", "DurationSeconds")})
            self.issued.append(values)
        return values

    def __enter__(self) -> WireSts:
        self.thread.start()
        return self

    def __exit__(self, *_: object) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=10)


def write_aws_files(home: Path, history: str | None, *, in_credentials: bool = False, profile: str = WIRE_PROFILE) -> None:
    """Write a synthetic AWS config and credentials file under a temporary ``home``; ``history`` is the ``cli_history`` value, or None for no setting.

    The setting goes into the credentials file when ``in_credentials`` is set, otherwise into the config file. The keys are fixed stand-in strings.
    """
    folder = home / ".aws"
    folder.mkdir(parents=True, exist_ok=True)
    line = [f"cli_history = {history}"] if history is not None else []
    config = [f"[profile {profile}]", f"region = {WIRE_REGION}", *([] if in_credentials else line)]
    credentials = [f"[{profile}]", f"aws_access_key_id = {WIRE_KEY_ID}", f"aws_secret_access_key = {WIRE_KEY_VALUE}", *(line if in_credentials else [])]
    (folder / "config").write_text("\n".join(config) + "\n")
    (folder / "credentials").write_text("\n".join(credentials) + "\n")


def history_bytes(home: Path) -> bytes:
    """Everything the AWS command saved under ``home/.aws/cli/history``, as raw bytes, for a search for values; empty when nothing was saved."""
    folder = home / ".aws" / "cli" / "history"
    return b"".join(path.read_bytes() for path in sorted(folder.rglob("*")) if path.is_file()) if folder.is_dir() else b""
