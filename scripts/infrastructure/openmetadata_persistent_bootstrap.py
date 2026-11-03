"""Prepare the persistent OpenMetadata server for its callers: set the administrator password and issue one bot token per caller into Parameter Store.

Runs in the ingestion image as the one-shot ``openmetadata_bootstrap`` host task: ``python openmetadata_persistent_bootstrap.py``.
It reads ``OM_API``, ``OM_ADMIN_PASSWORD`` (injected from Parameter Store by the task definition) and ``OM_BOT_PARAMETERS``
(a JSON object that names the token parameter of the callers lineage, ingestion and comparison).
It waits for the server within a deadline, replaces the factory administrator password, makes sure each caller has a bot with its role,
and writes each bot's token straight to that caller's SecureString parameter.
A rerun changes nothing when everything is current. A token that differs from its parameter is written again.
Exit codes: 0 done, 1 a step failed (the message names the caller or the class of the error), 2 the configuration was refused before any call.
Output names steps and callers and never prints a password or a token.
The routes and the role names are the ones of the OpenMetadata REST API as the author knew them and are unproven against release 2.0.7.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from collections.abc import Mapping
from typing import Any

try:
    from scripts.infrastructure import openmetadata_bootstrap as base
except ImportError:
    import openmetadata_bootstrap as base

CALLER_ROLES = {"lineage": "IngestionBotRole", "ingestion": "IngestionBotRole", "comparison": "DataConsumer"}
PARAMETER_PATTERN = re.compile(r"^/(?:[A-Za-z0-9_]+/){0,14}[A-Za-z0-9_]+$")


class ParameterMissing(Exception):
    """The parameter does not exist yet."""


def read_parameter(ssm: Any, name: str) -> str | None:
    """The decrypted value of a parameter, or None when it does not exist."""
    try:
        return str(ssm.get_parameter(Name=name, WithDecryption=True)["Parameter"]["Value"])
    except ParameterMissing:
        return None
    except Exception as error:
        if type(error).__name__ == "ParameterNotFound":
            return None
        raise


def bot_names(raw: str) -> dict[str, str] | None:
    """The token parameter of each caller, or None when the setting is not exactly one distinct absolute parameter path for each known caller."""
    try:
        parsed = json.loads(raw)
    except ValueError:
        return None
    if not isinstance(parsed, dict) or set(parsed) != set(CALLER_ROLES):
        return None
    values = list(parsed.values())
    if not all(isinstance(value, str) and PARAMETER_PATTERN.fullmatch(value) for value in values) or len(set(values)) != len(values):
        return None
    return {caller: str(parsed[caller]) for caller in CALLER_ROLES}


def ensure_bot(api: str, session: str, caller: str) -> str:
    """The token of the caller's bot. A missing bot, with its user and role, is created. Raises RuntimeError with a fixed reason when a step is refused."""
    name = f"{caller}-bot"
    status, bot = base.call("GET", f"{api}/v1/bots/name/{name}", session=session)
    user = (bot.get("botUser") or {}).get("id") if status == 200 else None
    if not user:
        role_status, role = base.call("GET", f"{api}/v1/roles/name/{CALLER_ROLES[caller]}", session=session)
        if role_status != 200 or not role.get("id"):
            raise RuntimeError("the role of the bot was not found")
        body = {
            "name": name,
            "email": f"{name}@open-metadata.org",
            "isBot": True,
            "roles": [role["id"]],
            "authenticationMechanism": {"authType": "JWT", "config": {"JWTTokenExpiry": "Unlimited"}},
        }
        created_status, created = base.call("PUT", f"{api}/v1/users", body, session)
        user = created.get("id") if created_status in (200, 201) else None
        if not user:
            raise RuntimeError("the bot user was refused")
        if base.call("PUT", f"{api}/v1/bots", {"name": name, "botUser": name}, session)[0] not in (200, 201):
            raise RuntimeError("the bot was refused")
    status, mechanism = base.call("GET", f"{api}/v1/users/auth-mechanism/{user}", session=session)
    token = (mechanism.get("config") or {}).get("JWTToken") if status == 200 else None
    if not token:
        raise RuntimeError("the bot has no token")
    return str(token)


def main(argv: list[str], env: Mapping[str, str] | None = None, ssm: Any = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--deadline-seconds", type=int, default=900)
    parser.add_argument("--poll-seconds", type=int, default=5)
    options = parser.parse_args(argv)
    environment = os.environ if env is None else env

    def fail(reason: str, code: int = 1) -> int:
        sys.stderr.write(f"openmetadata_persistent_bootstrap: {reason}\n")
        return code

    api = environment.get("OM_API", "")
    secret = environment.get("OM_ADMIN_PASSWORD", "")
    parameters = bot_names(environment.get("OM_BOT_PARAMETERS", ""))
    if not api or not secret:
        return fail("OM_API and OM_ADMIN_PASSWORD must be set", 2)
    if parameters is None:
        return fail("OM_BOT_PARAMETERS must name one distinct absolute parameter for each of the callers lineage, ingestion and comparison", 2)
    limit = time.monotonic() + options.deadline_seconds
    while base.call("GET", f"{api}/v1/system/version")[0] != 200:
        if time.monotonic() >= limit:
            return fail(f"the server was not healthy within {options.deadline_seconds} seconds")
        time.sleep(options.poll_seconds)
    session = base.login(api, secret)
    if not session:
        session = base.login(api, base.FACTORY_LOGIN_VALUE)
        if not session:
            return fail("neither the stored administrator password nor the default one logs in")
        body = {"username": "admin", "oldPassword": base.FACTORY_LOGIN_VALUE, "newPassword": secret, "confirmPassword": secret, "requestType": "SELF"}
        if base.call("PUT", f"{api}/v1/users/changePassword", body, session)[0] != 200:
            return fail("the administrator password change was refused")
        session = base.login(api, secret)
        if not session:
            return fail("the new administrator password does not log in")
        sys.stdout.write("openmetadata_persistent_bootstrap: administrator password replaced\n")
    if ssm is None:
        import boto3

        ssm = boto3.client("ssm", region_name=environment.get("AWS_REGION"))
    failed: list[str] = []
    for caller, name in parameters.items():
        try:
            token = ensure_bot(api, session, caller)
            if read_parameter(ssm, name) != token:
                ssm.put_parameter(Name=name, Value=token, Type="SecureString", Overwrite=True)
                sys.stdout.write(f"openmetadata_persistent_bootstrap: {caller} token written\n")
            else:
                sys.stdout.write(f"openmetadata_persistent_bootstrap: {caller} token current\n")
        except RuntimeError as error:
            failed.append(f"{caller} ({error})")
        except Exception as error:
            failed.append(f"{caller} ({type(error).__name__})")
    if failed:
        return fail(f"failed for: {'; '.join(failed)}")
    sys.stdout.write("openmetadata_persistent_bootstrap: ready\n")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
