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
import sys
import time
import urllib.parse
from collections.abc import Mapping
from pathlib import Path
from typing import Any

ADMIN_EMAIL = "admin@open-metadata.org"
FACTORY_LOGIN_VALUE = "admin"
REQUEST_SECONDS = 10


def call(method: str, url: str, body: Mapping[str, Any] | None = None, session: str = "") -> tuple[int, Any]:
    """Send one JSON request to a plain-HTTP address and return the status and parsed body. Status 0 means the server did not answer."""
    parts = urllib.parse.urlsplit(url)
    headers = {"Content-Type": "application/json"}
    if session:
        headers["Authorization"] = f"Bearer {session}"
    connection = http.client.HTTPConnection(parts.hostname or "", parts.port, timeout=REQUEST_SECONDS)
    try:
        connection.request(method, parts.path + (f"?{parts.query}" if parts.query else ""), json.dumps(body) if body is not None else None, headers)
        reply = connection.getresponse()
        raw = reply.read()
        return reply.status, json.loads(raw) if raw else {}
    except (OSError, http.client.HTTPException, ValueError):
        return 0, {}
    finally:
        connection.close()


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
        body = {"username": "admin", "oldPassword": FACTORY_LOGIN_VALUE, "newPassword": new_secret, "confirmPassword": new_secret, "requestType": "SELF"}
        if call("PUT", f"{options.base}/v1/users/changePassword", body, session)[0] != 200:
            return fail("the administrator password change was refused")
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
