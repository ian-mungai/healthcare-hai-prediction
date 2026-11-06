"""Read a publisher API key from AWS Secrets Manager into memory with boto3, so the AWS command-line tool never sees it.

The AWS command-line tool saves the answers of its calls when a profile turns its history on; a key read through it could land in that history file before
any code sees it. boto3 keeps no such record. The shared keys hold plain text.

The read runs in a child Python process (``python -m scripts.acquisition.api_keys``) whose environment comes from the same builder as every ``AwsCli``
call: no inherited ``AWS_*`` variable, so only the explicit profile and region select its credentials. The child first asks STS who its credentials
belong to and compares the answer with the account and ARN that the caller's identity check verified; only on a match does it read the key. It
resolves the credentials once and signs both requests with that frozen set, so a credential refresh cannot change the identity in between. It starts
its own process group (the one the launcher gives it, or one it starts with ``setsid`` when run by hand) and kills that group at an internal deadline a few
seconds before the caller's launcher timeout, so a stalled setup, credential process or request ends with the call and cannot send a request after the caller
failed.

The value leaves the child only as one JSON object on standard output. On failure the child writes a fixed error code to standard error and nothing to
standard output; the caller reads only the exit code. Errors name the secret and a fixed cause, never a value or a provider message.

A key made only of digits, ``e``, ``E``, ``+``, ``-`` and ``.`` is refused, in the child before it writes the value and again in the caller. A JSON
number can hold exactly these characters, so a publisher reply could echo such a key as a number spelled another way (``1e19`` for
``10000000000000000000``) that no text comparison finds. The owner rotates such a key to one with another character.

This module imports only acquisition modules, so the collector code fingerprints that hash the package also cover it.
"""

from __future__ import annotations

import json
import math
import os
import signal
import sys
import threading
import time
import urllib.parse
from pathlib import Path
from typing import Any

from scripts.acquisition.cli_tools import run_argv
from scripts.process import SubprocessError, TimeoutExpired

SHARED_KEYS = ("bls_api_key", "census_api_key", "hud_api_key")
READ_DEADLINE_SECONDS = 60.0
CLEANUP_MARGIN_SECONDS = 5.0
ROOT = Path(__file__).resolve().parents[2]
LOOPBACK = {"127.0.0.1", "localhost"}
CLIENT_OPTIONS: dict[str, Any] = {
    "retries": {"total_max_attempts": 2, "mode": "standard"},
    "connect_timeout": 5,
    "read_timeout": 10,
    "ignore_configured_endpoint_urls": True,
}
FAILURES: dict[str, tuple[int, str]] = {
    "usage": (2, "the reader received unusable arguments"),
    "session": (3, "the reader could not start its own process group"),
    "identity_mismatch": (4, "the SDK credentials belong to another identity than the checked one"),
    "not_found": (5, "the secret does not exist"),
    "denied": (6, "access to the secret was denied"),
    "service": (7, "the service refused the request"),
    "sdk": (8, "the SDK could not resolve credentials or reach the service"),
    "empty": (9, "the secret is missing or empty"),
    "unexpected": (10, "the reader failed unexpectedly"),
    "credentials": (11, "the SDK resolved no usable credentials"),
    "number_shaped": (
        12,
        "the secret is made only of digits, e, E, plus, minus and dot, which a JSON number can echo; rotate the key to one with another character",
    ),
}
CAUSES = dict(FAILURES.values())
NUMBER_CHARACTERS = frozenset("0123456789eE+-.")


class ApiKeyError(ValueError):
    """The key could not be read; the message names the secret and the cause, never a value."""


class Refused(Exception):
    """A child-side failure that carries only one of the fixed error codes."""

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


def loopback(endpoint: str) -> bool:
    """True for an ``http`` URL on the loopback address, the only endpoint a test stand-in may name."""
    try:
        parsed = urllib.parse.urlsplit(endpoint)
        return parsed.scheme == "http" and parsed.hostname in LOOPBACK
    except ValueError:
        return False


def answer(text: str) -> str | None:
    """The value from the child's standard output: one JSON object holding one non-empty string ``value``, else None."""
    try:
        document = json.loads(text)
    except ValueError:
        return None
    if isinstance(document, dict) and set(document) == {"value"} and isinstance(document["value"], str) and document["value"]:
        return document["value"]
    return None


def read(name: str, profile: str, region: str, account: str, arn: str, seconds: float = READ_DEADLINE_SECONDS, endpoint: str | None = None) -> str:
    """The plain-text value of one shared publisher key, read by a child process whose credentials must belong to ``account`` and ``arn``.

    ``profile``, ``region``, ``account`` and ``arn`` are the settings and the answer of the caller's identity check. The child process ends within
    ``seconds``; ``endpoint`` is a loopback stand-in for STS and Secrets Manager that only tests name.
    """
    if name not in SHARED_KEYS:
        raise ApiKeyError(f"{name} is not a shared publisher key")
    if not (isinstance(profile, str) and profile and isinstance(region, str) and region):
        raise ApiKeyError(f"reading secret {name} failed: the checked client has no usable profile or region")
    if not (isinstance(account, str) and account and isinstance(arn, str) and arn):
        raise ApiKeyError(f"reading secret {name} failed: the checked identity has no account or ARN")
    if not (isinstance(seconds, int | float) and math.isfinite(seconds) and seconds > CLEANUP_MARGIN_SECONDS):
        raise ApiKeyError(f"reading secret {name} failed: the deadline must be longer than {CLEANUP_MARGIN_SECONDS:g} s")
    if endpoint is not None and not loopback(endpoint):
        raise ApiKeyError(f"reading secret {name} failed: a stand-in endpoint must be on the loopback address")
    from scripts.acquisition.s3_store import aws_environment

    limit = repr(float(seconds) - CLEANUP_MARGIN_SECONDS)
    result = None
    started = True
    try:
        result = run_argv(
            [sys.executable, "-B", "-E", "-s", "-m", "scripts.acquisition.api_keys", name, profile, region, account, arn, limit, endpoint or ""],
            cwd=ROOT,
            env=aws_environment(),
            timeout=seconds,
        )
    except TimeoutExpired:
        pass
    except (OSError, SubprocessError):
        started = False
    if not started:
        raise ApiKeyError(f"reading secret {name} failed: the reader process could not start")
    if result is None or result.returncode == -signal.SIGKILL:
        raise ApiKeyError(f"reading secret {name} did not finish within {seconds:g} s")
    if result.returncode:
        raise ApiKeyError(f"reading secret {name} failed: {CAUSES.get(result.returncode, 'the reader process stopped unexpectedly')}")
    value = answer(result.stdout)
    if value is None:
        raise ApiKeyError(f"reading secret {name} failed: the reader's answer is not one non-empty value")
    if number_shaped(value):
        raise ApiKeyError(f"reading secret {name} failed: {FAILURES['number_shaped'][1]}")
    return value


def number_shaped(value: str) -> bool:
    """True when every character of ``value`` can appear in a JSON number."""
    return set(value) <= NUMBER_CHARACTERS


def usable(text: object) -> bool:
    """True for a non-empty string, the only form a resolved access key, secret key or session token may take."""
    return isinstance(text, str) and bool(text)


def fetch(name: str, profile: str, region: str, account: str, arn: str, endpoint: str | None) -> str:
    """In the child: resolve the profile's credentials once, then check their identity with STS and read the key with exactly that frozen set.

    Both clients come from a session that holds the frozen access key, secret key, session token and the region. It also names the profile, so the
    profile's own settings (such as its CA bundle) apply instead of the default profile's; explicit credentials take precedence over the profile's
    credential chain, so no provider chain or credential refresh can change the signing identity between the identity check and the key request.
    Failures raise ``Refused``.
    """
    import boto3
    from botocore.config import Config
    from botocore.exceptions import BotoCoreError, ClientError

    options: dict[str, Any] = {"config": Config(**CLIENT_OPTIONS)} | ({"endpoint_url": endpoint} if endpoint else {})
    try:
        resolved = boto3.Session(profile_name=profile, region_name=region).get_credentials()
        if resolved is None:
            raise Refused("credentials")
        frozen = resolved.get_frozen_credentials()
        if not (usable(frozen.access_key) and usable(frozen.secret_key) and (frozen.token is None or usable(frozen.token))):
            raise Refused("credentials")
        session = boto3.Session(
            profile_name=profile,
            aws_access_key_id=frozen.access_key,
            aws_secret_access_key=frozen.secret_key,
            aws_session_token=frozen.token,
            region_name=region,
        )
        caller = session.client("sts", **options).get_caller_identity()
        if caller.get("Account") != account or caller.get("Arn") != arn:
            raise Refused("identity_mismatch")
        value = session.client("secretsmanager", **options).get_secret_value(SecretId=name).get("SecretString")
    except ClientError as error:
        code = error.response.get("Error", {}).get("Code")
        raise Refused(
            "not_found" if code == "ResourceNotFoundException" else "denied" if code in ("AccessDeniedException", "AccessDenied") else "service"
        ) from None
    except BotoCoreError:
        raise Refused("sdk") from None
    if not isinstance(value, str) or not value:
        raise Refused("empty")
    if number_shaped(value):
        raise Refused("number_shaped")
    return value


def end_group_at(moment: float) -> None:
    """Kill this process and every process it started (its whole process group) at ``moment`` on the monotonic clock."""
    time.sleep(max(0.0, moment - time.monotonic()))
    os.killpg(0, signal.SIGKILL)


def failed(code: str) -> int:
    """Report a fixed error code on standard error and return its exit code."""
    sys.stderr.write(code + "\n")
    return FAILURES[code][0]


def child(arguments: list[str]) -> int:
    """The child process: its own process group first, then the deadline watchdog, then the identity check and the key read."""
    started = time.monotonic()
    try:
        if os.getpgrp() != os.getpid():  # the launcher already made this process a group leader; setsid refuses a leader
            os.setsid()
    except OSError:
        return failed("session")
    try:
        name, profile, region, account, arn, limit_text, endpoint = arguments
        limit = float(limit_text)
    except ValueError:
        return failed("usage")
    if name not in SHARED_KEYS or not all((profile, region, account, arn)) or not (math.isfinite(limit) and limit > 0) or (endpoint and not loopback(endpoint)):
        return failed("usage")
    threading.Thread(target=end_group_at, args=(started + limit,), name="api-key-deadline", daemon=True).start()
    try:
        value = fetch(name, profile, region, account, arn, endpoint or None)
    except Refused as refusal:
        return failed(refusal.code)
    except Exception:
        return failed("unexpected")
    sys.stdout.write(json.dumps({"value": value}))
    sys.stdout.flush()
    return 0


if __name__ == "__main__":
    raise SystemExit(child(sys.argv[1:]))
