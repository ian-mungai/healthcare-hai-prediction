"""Short-lived AWS role sessions for pipeline jobs, from AWS STS through the project's AWS command wrapper.

A pipeline job whose class needs AWS gets the session of one role: the lakehouse reader for a read job, the lakehouse writer for a write job. The
session lasts at least until the job's deadline plus a margin and is asked for when the job's container is rendered, so no session is kept for a
later job or a retry. The values live in memory and reach the container as environment values; nothing is written to a file. The AWS command is not
started for a profile whose command history could save the answer. An error names the role and the error code, never an answer.
"""

from __future__ import annotations

import http.client
import json
import math
import re
import time
import urllib.parse
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from scripts.acquisition.s3_store import AwsCli, StorageError
from scripts.infrastructure import secret_store
from scripts.process import CompletedProcess, TimeoutExpired, run_command

PREFIX = "session:"
READER = "reader"
WRITER = "writer"
CLASS_ROLE = {"lakehouse_read": READER, "lakehouse_write": WRITER}
ROLE_NAMES = {READER: "healthcare_hai_prediction_lakehouse_reader", WRITER: "healthcare_hai_prediction_lakehouse_writer"}
ROLE_VARIABLES = {READER: "lakehouse_reader_role_name", WRITER: "lakehouse_writer_role_name"}
SESSION_VARIABLE = "lakehouse_session_max_seconds"
HISTORY_SETTING = "cli_history"
MAX_SESSION_SECONDS = 7200
MIN_SESSION_SECONDS = 900
MARGIN_SECONDS = 300
COVER_SECONDS = 120
REQUEST_SECONDS = 30.0
VARIABLES = "infra/iam/variables.tf"
DEPLOYMENT = "infra/deployment.auto.tfvars.json"
LOOPBACK = {"127.0.0.1", "localhost"}
Runner = Callable[..., CompletedProcess[str]]


class SessionError(secret_store.SecretStoreError):
    """A role session could not be had; the message names the role and an error code, never an answer or a value."""


@dataclass(frozen=True, repr=False)
class RoleSession:
    """One role's session: its role, its three values and the epoch second it ends."""

    role: str
    access_key_id: str
    secret_access_key: str
    session_token: str
    expires_at: int

    def __repr__(self) -> str:
        return f"RoleSession(role={self.role!r}, expires_at={self.expires_at})"

    def secret_values(self) -> frozenset[str]:
        """The three values that must never be logged, recorded or shown."""
        return frozenset((self.access_key_id, self.secret_access_key, self.session_token))

    def environment(self, region: str) -> dict[str, str]:
        """The four environment values a container receives."""
        return {
            "AWS_ACCESS_KEY_ID": self.access_key_id,
            "AWS_SECRET_ACCESS_KEY": self.secret_access_key,
            "AWS_SESSION_TOKEN": self.session_token,
            "AWS_REGION": region,
        }

    def covers(self, deadline_utc: int) -> bool:
        """True when the session outlives the job's deadline by at least the cover margin."""
        return self.expires_at >= deadline_utc + COVER_SECONDS


def placeholder(role: str) -> RoleSession:
    """Fixed stand-in values for a render that only records what it needs and creates nothing."""
    return RoleSession(role, "recorded-session-key", "recorded-session-secret", "recorded-session-token", 2**40)


def name_of(role: str) -> str:
    """The name under which a role's session travels beside the fetched secrets."""
    return PREFIX + role


def session_name(label: str) -> str:
    """A session name that traces the job in AWS logs: the label with the characters STS refuses replaced, at most 64 characters."""
    return re.sub(r"[^\w+=,.@-]", "-", f"hai-{label}")[:64]


def terraform_defaults(root: Path) -> dict[str, str]:
    """The defaults of the two role name variables and the session limit in ``infra/iam/variables.tf``, so a check can compare them with this module."""
    text = (root / VARIABLES).read_text()
    found = {}
    for variable in (*ROLE_VARIABLES.values(), SESSION_VARIABLE):
        match = re.search(r'variable "' + variable + r'" \{.*?default\s*=\s*"?([^"\n]+)"?\n', text, flags=re.DOTALL)
        if match is None:
            raise SessionError(f"{VARIABLES} has no default for {variable}")
        found[variable] = match.group(1).strip()
    return found


def deployment_settings(root: Path) -> dict[str, str]:
    """The project profile, region and account from the rendered Terraform variables."""
    try:
        values = json.loads((root / DEPLOYMENT).read_text())
        return {key: str(values[key]) for key in ("aws_profile", "aws_region", "expected_account_id")}
    except (OSError, KeyError, ValueError):
        raise SessionError(f"{DEPLOYMENT} is missing or incomplete; render the configuration first") from None


def deadline_problem(seconds: int) -> str | None:
    """What refuses a job whose timeout the roles' sessions cannot cover, or None."""
    if seconds + MARGIN_SECONDS > MAX_SESSION_SECONDS:
        return f"a job with a role session may run at most {MAX_SESSION_SECONDS - MARGIN_SECONDS} s: the lakehouse roles give at most {MAX_SESSION_SECONDS} s"
    return None


def duration_seconds(deadline_utc: int, now: float) -> int:
    """The session length that reaches the job's deadline plus the margin, at least the STS minimum; a length the roles cannot give is refused."""
    needed = max(MIN_SESSION_SECONDS, math.ceil(deadline_utc - now) + MARGIN_SECONDS)
    if needed > MAX_SESSION_SECONDS:
        raise SessionError(f"the job deadline needs a role session of {needed} s; the lakehouse roles give at most {MAX_SESSION_SECONDS} s")
    return needed


class SessionCli(AwsCli):
    """The project's AWS command wrapper for STS calls. It does not load the storage policy, which only the S3 operations read."""

    def __init__(self, configuration: dict[str, str], runner: Runner) -> None:
        self.configuration = configuration
        self.runner = runner


def history_problem(profile: str, env: Mapping[str, str] | None, seconds: float) -> str | None:
    """What stops a request when the AWS command could save its answer in its history file, or None when the profile's history is confirmed off.

    The role session answer holds the three session values, and the AWS command saves the answers of its calls when the profile sets ``cli_history``
    to ``enabled``. The same command is asked for that one setting of the profile with the child's own environment, so it reads the same files and
    the same merged profile that the history switch reads. Only ``disabled`` or no setting at all (exit code 1, no output on either stream) lets the
    request go on. Exit code 1 with a message on the error stream is a setting the command could not read, for example a section in place of a value.
    """
    asked = run_command("aws", ["configure", "get", HISTORY_SETTING, "--profile", profile, "--no-cli-pager", "--no-cli-auto-prompt"], env=env, timeout=seconds)
    value = asked.stdout.strip()
    if (asked.returncode == 1 and not value and not asked.stderr.strip()) or (asked.returncode == 0 and value == "disabled"):
        return None
    if asked.returncode == 0 and value == "enabled":
        return (
            f"the AWS command history is on for profile {profile}, and would save the role session answer with its keys on disk; "
            f"set {HISTORY_SETTING} to disabled for that profile and run again"
        )
    return f"the AWS command history setting of profile {profile} could not be confirmed off (exit code {asked.returncode}); no role session was requested"


def aws_runner(seconds: float) -> Runner:
    """The runner of the real registry: the AWS command with its output captured, stopped after at most ``seconds`` in all.

    The history check and the request share that deadline. A request starts only after the profile's command history is confirmed off.
    """

    def run(command: Sequence[str], *, env: Mapping[str, str] | None = None, timeout: float = seconds, **_: Any) -> CompletedProcess[str]:
        limit = min(float(timeout), seconds)
        started = time.monotonic()
        arguments = list(command)
        if "--profile" not in arguments[:-1]:
            raise SessionError("the AWS command history cannot be checked: the request names no profile")
        problem = history_problem(arguments[arguments.index("--profile") + 1], env, limit)
        if problem is not None:
            raise SessionError(problem)
        left = limit - (time.monotonic() - started)
        if left <= 0:
            raise TimeoutExpired(arguments, limit)
        return run_command("aws", arguments[1:], env=env, timeout=left)

    return run


def loopback_runner(endpoint: str, seconds: float) -> Runner:
    """The runner of a test registry: it sends the command line to the loopback stand-in and returns the answer as the command's exit code and output."""
    parsed = urllib.parse.urlparse(endpoint)
    if parsed.hostname not in LOOPBACK or parsed.port is None:
        raise SessionError("a stand-in endpoint must be on the loopback address")
    host, port = str(parsed.hostname), parsed.port

    def run(command: Sequence[str], **_: Any) -> CompletedProcess[str]:
        connection = http.client.HTTPConnection(host, port, timeout=seconds)
        try:
            connection.request("POST", "/", json.dumps({"argv": list(command)}).encode(), {"Content-Type": "application/json"})
            answer = json.loads(connection.getresponse().read())
            return CompletedProcess(list(command), int(answer["returncode"]), str(answer["stdout"]), str(answer["stderr"]))
        except TimeoutError:
            raise TimeoutExpired(list(command), seconds) from None
        except (OSError, ValueError, KeyError, TypeError, http.client.HTTPException):
            return CompletedProcess(list(command), 255, "", "Could not connect to the endpoint URL")
        finally:
            connection.close()

    return run


def client_for(settings: Mapping[str, str], endpoint: str | None, test_registry: bool, seconds: float) -> AwsCli:
    """The STS client: the AWS command with the project profile for the real registry, the loopback stand-in for a test registry that names one.

    A test registry without a stand-in has no session source, and a real registry never uses one.
    """
    if test_registry and endpoint is None:
        raise SessionError("a test registry has no role session source; name a stand-in endpoint in its settings")
    if not test_registry and endpoint is not None:
        raise SessionError("a real registry takes its role sessions from AWS only")
    runner = loopback_runner(endpoint, seconds) if endpoint is not None else aws_runner(seconds)
    return SessionCli({"aws_profile": settings["aws_profile"], "aws_region": settings["aws_region"]}, runner)


def explain(role: str, name: str, code: str, seconds: int, limit: float) -> str:
    """The sentence for a failed request: the role and the error code only."""
    if code == "request_unconfirmed":
        return f"the {role} role session request did not finish within {limit:g} s, or the AWS command could not run"
    if code == "AccessDenied":
        return f"the {role} role {name} could not be assumed: AccessDenied (the role is missing or the project user may not assume it)"
    if code == "ValidationError":
        return f"the {role} role {name} cannot give a session of {seconds} s: ValidationError"
    return f"the {role} role session request failed: {code}"


def read_expiry(text: str) -> int:
    """The epoch second of an STS expiry time; a time with no zone counts as UTC."""
    moment = datetime.fromisoformat(text)
    return int((moment if moment.tzinfo else moment.replace(tzinfo=UTC)).timestamp())


def assume(client: AwsCli, account: str, role: str, seconds: int, session_name: str, limit: float = REQUEST_SECONDS) -> RoleSession:
    """Ask STS for the role's session and check that the answer is complete."""
    name = ROLE_NAMES[role]
    arguments = ["--role-arn", f"arn:aws:iam::{account}:role/{name}", "--role-session-name", session_name, "--duration-seconds", str(seconds)]
    try:
        answer = client.call("sts", "assume-role", arguments)
    except StorageError as error:
        raise SessionError(explain(role, name, error.code, seconds, limit)) from None
    credentials = answer.get("Credentials")
    found = credentials if isinstance(credentials, dict) else {}
    key_id, secret, token, ends = (
        found[key] if isinstance(found.get(key), str) else "" for key in ("AccessKeyId", "SecretAccessKey", "SessionToken", "Expiration")
    )
    if not (key_id and secret and token and ends):
        raise SessionError(f"the {role} role session answer is incomplete")
    try:
        expires = read_expiry(ends)
    except ValueError:
        raise SessionError(f"the {role} role session answer has no readable end time") from None
    return RoleSession(role, key_id, secret, token, expires)


def fetch(
    role: str,
    *,
    settings: Mapping[str, str],
    endpoint: str | None,
    test_registry: bool,
    deadline_utc: int,
    session_name: str,
    limit: float = REQUEST_SECONDS,
    now: Callable[[], float] = time.time,
) -> RoleSession:
    """A new session of the role for a job that ends at ``deadline_utc``; nothing is reused from an earlier call.

    The session ends after the deadline: the answer is refused when its end time does not cover it.
    """
    seconds = duration_seconds(deadline_utc, now())
    client = client_for(settings, endpoint, test_registry, limit)
    session = assume(client, settings["expected_account_id"], role, seconds, session_name, limit)
    if not session.covers(deadline_utc):
        raise SessionError(f"the {role} role session ends before the job's deadline")
    return session
