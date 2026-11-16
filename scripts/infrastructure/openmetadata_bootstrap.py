"""Prepare a freshly started OpenMetadata server for the ingestion steps of the catalog task.

Runs in the ingestion image as ``python openmetadata_bootstrap.py``. It waits for the server within a deadline, replaces the default administrator password with
the one from the secret store (``OM_ADMIN_PASSWORD``, injected by the task definition), and writes the ingestion bot's token to an owner-only file. A rerun
changes nothing when the new password already works. On any failure it writes the abort file, which the deadline wrapper of the long-lived containers
watches, so a task with no usable server stops at once instead of running until its deadline. The routes are the ones of the OpenMetadata REST API as the
author knew them and are unproven against a real release. Output names steps and never prints a password or a token.
"""

from __future__ import annotations

import argparse
import base64
import http.client
import json
import os
import re
import string
import sys
import time
import urllib.parse
from collections.abc import Mapping
from pathlib import Path
from typing import Any

ADMIN_EMAIL = "admin@example.org"
FACTORY_LOGIN_VALUE = "admin"
REQUEST_SECONDS = 10
DETAIL_LIMIT = 300
# OpenMetadata 2.0.7 PasswordUtil.validatePassword (Passay 1.6.6): 8 to 56 characters, no white space, one upper case letter, one lower case letter,
# one digit and one special character; the ASCII specials of Passay's EnglishCharacterData.Special are string.punctuation.
POLICY_TEXT = (
    "the OpenMetadata password policy (8 to 56 characters, no white space, one upper case letter, one lower case letter, one digit and one special character)"
)


def call(method: str, url: str, body: Mapping[str, Any] | None = None, session: str = "") -> tuple[int, Any]:
    """Send one JSON request to a plain-HTTP address and return the status and parsed body. Status 0 means the server did not answer.

    A body that is not JSON (the server answers a changed password with the plain text ``Password Updated Successfully``) comes back as ``{"message": text}``.
    """
    parts = urllib.parse.urlsplit(url)
    headers = {"Content-Type": "application/json"}
    if session:
        headers["Authorization"] = f"Bearer {session}"
    connection = http.client.HTTPConnection(parts.hostname or "", parts.port, timeout=REQUEST_SECONDS)
    try:
        connection.request(method, parts.path + (f"?{parts.query}" if parts.query else ""), json.dumps(body) if body is not None else None, headers)
        reply = connection.getresponse()
        raw = reply.read()
        status = reply.status
    except (OSError, http.client.HTTPException):
        return 0, {}
    finally:
        connection.close()
    try:
        return status, json.loads(raw) if raw else {}
    except ValueError:
        return status, {"message": raw.decode("utf-8", "replace")}


def refusal_detail(status: int, reply: Any, *secrets: str) -> str:
    """Name the HTTP status and the server's message of a refused request; the given secrets are blanked and the text is cut short."""
    message = reply.get("message") if isinstance(reply, dict) else None
    text = " ".join(str(message).split())[:DETAIL_LIMIT] if message else "no message"
    for secret in secrets:
        if secret:
            text = text.replace(secret, "***")
    return f"HTTP {status}: {text}" if status else "no answer from the server"


def password_problem(value: str) -> str:
    """Return a sentence that names the policy when the server would refuse ``value`` as a new administrator password, otherwise an empty string."""
    fine = (
        8 <= len(value) <= 56
        and re.search(r"\s", value) is None
        and any(character in string.ascii_uppercase for character in value)
        and any(character in string.ascii_lowercase for character in value)
        and any(character in string.digits for character in value)
        and any(character in string.punctuation for character in value)
    )
    return "" if fine else f"the stored administrator password breaks {POLICY_TEXT}; replace the stored parameter with a password that meets it"


def login(base: str, secret: str) -> str:
    """Return the session token of the administrator, or an empty string when the login is refused."""
    encoded = base64.b64encode(secret.encode()).decode()
    status, reply = call("POST", f"{base}/v1/users/login", {"email": ADMIN_EMAIL, "password": encoded})
    return str(reply.get("accessToken", "")) if status == 200 else ""


def write_private(path: Path, text: str) -> None:
    """Write a file that only its owner can read, replacing an earlier one."""
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(descriptor, "w") as handle:
        handle.write(text)


def main(argv: list[str], env: Mapping[str, str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base", default="http://127.0.0.1:8585/api")
    parser.add_argument("--token-file", type=Path, default=Path("/run/om/ingestion_token"))
    parser.add_argument("--abort-file", type=Path, default=Path("/run/om/abort"))
    parser.add_argument("--deadline-seconds", type=int, default=1200)
    parser.add_argument("--poll-seconds", type=int, default=5)
    options = parser.parse_args(argv)
    environment = os.environ if env is None else env

    def fail(reason: str) -> int:
        sys.stderr.write(f"openmetadata_bootstrap: {reason}\n")
        write_private(options.abort_file, f"bootstrap failed: {reason}\n")
        return 1

    new_secret = environment.get("OM_ADMIN_PASSWORD", "")
    if not new_secret:
        return fail("OM_ADMIN_PASSWORD is not set")
    limit = time.monotonic() + options.deadline_seconds
    while call("GET", f"{options.base}/v1/system/version")[0] != 200:
        if time.monotonic() >= limit:
            return fail(f"the server was not healthy within {options.deadline_seconds} seconds")
        time.sleep(options.poll_seconds)
    session = login(options.base, new_secret)
    if not session:
        session = login(options.base, FACTORY_LOGIN_VALUE)
        if not session:
            return fail("neither the stored administrator password nor the default one logs in")
        problem = password_problem(new_secret)
        if problem:
            return fail(problem)
        body = {"username": "admin", "oldPassword": FACTORY_LOGIN_VALUE, "newPassword": new_secret, "confirmPassword": new_secret, "requestType": "SELF"}
        status, reply = call("PUT", f"{options.base}/v1/users/changePassword", body, session)
        if status != 200:
            return fail(f"the administrator password change was refused ({refusal_detail(status, reply, new_secret)})")
        session = login(options.base, new_secret)
        if not session:
            return fail("the new administrator password does not log in")
    status, bot = call("GET", f"{options.base}/v1/bots/name/ingestion-bot", session=session)
    user = (bot.get("botUser") or {}).get("id") if status == 200 else None
    if not user:
        return fail("the ingestion bot was not found")
    status, mechanism = call("GET", f"{options.base}/v1/users/auth-mechanism/{user}", session=session)
    token = (mechanism.get("config") or {}).get("JWTToken") if status == 200 else None
    if not token:
        return fail("the ingestion bot has no token")
    write_private(options.token_file, str(token) + "\n")
    sys.stdout.write("openmetadata_bootstrap: ready\n")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
