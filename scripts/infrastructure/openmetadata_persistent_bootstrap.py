"""Prepare the persistent OpenMetadata server for its callers: set the administrator password and issue one bot token per caller into Parameter Store.

Runs in the ingestion image as the one-shot ``openmetadata_bootstrap`` host task: ``python openmetadata_persistent_bootstrap.py``.
It reads ``OM_API``, ``OM_ADMIN_PASSWORD`` (injected from Parameter Store by the task definition) and ``OM_BOT_PARAMETERS``
(a JSON object that names the token parameter of the callers lineage and ingestion).
It waits for the server within a deadline, replaces the factory administrator password, makes sure each caller has a bot with its role,
writes each bot's token straight to that caller's SecureString parameter, and turns ``autoCreateEntities`` off in the OpenLineage settings
(owner decision of Nov 7 2026: the OpenLineage API resolves only tables the Glue connector cataloged and never invents a table or a pipeline).
OpenMetadata 2.0.7 keeps those settings in its database (``openLineageSettings``, seeded on first start with ``autoCreateEntities`` true by
SettingsCache), not in a configuration file, so the bootstrap reads ``GET /v1/system/settings/openLineageSettings`` and writes
``PUT /v1/system/settings`` only when the value is not already false; the other OpenLineage settings keep the server's values.
After a restore of the database the search index can be empty while the catalog holds tables, so the bootstrap also starts one search reindex
(``POST /v1/apps/trigger/SearchIndexingApplication`` with ``recreateIndex`` true) when the table count is above 0 and the table search index has no
document or does not exist. A filled index and an empty catalog start nothing, so a rerun changes nothing.
A rerun changes nothing when everything is current. A token that differs from its parameter is written again.
It also retires the bots that earlier releases created for callers that no longer exist (``RETIRED_CALLERS``): it revokes the token and deletes the bot and
its user. It touches a bot only when its name is exactly ``<caller>-bot`` and its user is the bot user this script creates (same name, ``<name>@example.org``,
``isBot``, and ``isAdmin`` reported false); any other user of that name is left and reported. A user lookup that fails fails the run. A bot that is gone while
its user remains (a deletion that stopped half way) is finished on the next run by the same checks. An absent bot and user are a no-op. The token parameter of a
retired caller is not deleted here, because the task role may only read and write the parameters of the current callers: the owner deletes it.
Exit codes: 0 done, 1 a step failed (the message names the caller or the class of the error), 2 the configuration was refused before any call.
Output names steps and callers and never prints a password or a token.
The routes and the role names are the ones of the OpenMetadata REST API as the author knew them and are unproven against release 2.0.7.
"""

from __future__ import annotations

import argparse
import importlib
import json
import os
import re
import sys
import time
from collections.abc import Mapping
from typing import Any

try:
    base = importlib.import_module("scripts.infrastructure.openmetadata_bootstrap")
except ImportError:
    base = importlib.import_module("openmetadata_bootstrap")

CALLER_ROLES = {"lineage": "IngestionBotRole", "ingestion": "IngestionBotRole"}
RETIRED_CALLERS = ("comparison",)
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
            "email": f"{name}@example.org",
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


def not_ours(user: Mapping[str, Any], name: str) -> str | None:
    """Why a user must not be retired, or None when it is the bot user this script creates and it is not an administrator."""
    if user.get("name") != name or user.get("email") != f"{name}@example.org" or user.get("isBot") is not True:
        return "left: the user was not created by this project"
    if user.get("isAdmin") is not False:
        return "left: the user is an administrator or its administrator status is not confirmed"
    return None


def read_user(api: str, session: str, route: str) -> dict[str, Any] | None:
    """The user record of a route, or None when there is no such user. Raises RuntimeError when the read fails, so a lookup error never counts as a result."""
    status, user = base.call("GET", f"{api}/v1/users/{route}", session=session)
    if status == 404:
        return None
    if status != 200 or not isinstance(user, dict):
        raise RuntimeError("the bot user could not be read")
    return user


def retire_user(api: str, session: str, user_id: str) -> None:
    """Revoke the token of a verified bot user and delete the user. Raises RuntimeError with a fixed reason when a step is refused."""
    if base.call("PUT", f"{api}/v1/users/revokeToken", {"id": user_id}, session)[0] != 200:
        raise RuntimeError("the token was not revoked")
    if base.call("DELETE", f"{api}/v1/users/{user_id}?hardDelete=true", session=session)[0] not in (200, 404):
        raise RuntimeError("the user was not deleted")


def retire_bot(api: str, session: str, caller: str) -> str:
    """Retire the project's bot of a caller that no longer exists; return ``absent``, ``retired`` or ``left: <reason>``.

    Raises RuntimeError with a fixed reason when a read or a step fails, so a run never reports success without having checked the user.

    The bot and its user are touched only when the user is the bot user ``ensure_bot`` creates and is not an administrator. A bot that is gone while its
    user remains (a deletion that stopped half way) is finished by the same checks.
    """
    name = f"{caller}-bot"
    status, bot = base.call("GET", f"{api}/v1/bots/name/{name}", session=session)
    if status == 404:
        user = read_user(api, session, f"name/{name}")
        if user is None:
            return "absent"
        reason = not_ours(user, name)
        if reason or not user.get("id"):
            return reason or "left: the user has no identity"
        retire_user(api, session, str(user["id"]))
        return "retired"
    if status != 200 or not isinstance(bot, dict):
        raise RuntimeError("the bot could not be read")
    bot_id = bot.get("id")
    user_id = (bot.get("botUser") or {}).get("id")
    if not bot_id or not user_id:
        return "left: the bot has no user to check"
    user = read_user(api, session, str(user_id))
    if user is None:
        raise RuntimeError("the bot user could not be read")
    reason = not_ours(user, name)
    if reason:
        return reason
    if base.call("PUT", f"{api}/v1/users/revokeToken", {"id": user_id}, session)[0] != 200:
        raise RuntimeError("the token was not revoked")
    if base.call("DELETE", f"{api}/v1/bots/{bot_id}?hardDelete=true", session=session)[0] not in (200, 404):
        raise RuntimeError("the bot was not deleted")
    if base.call("DELETE", f"{api}/v1/users/{user_id}?hardDelete=true", session=session)[0] not in (200, 404):
        raise RuntimeError("the user was not deleted")
    return "retired"


def lineage_settings(api: str, session: str) -> str:
    """Turn autoCreateEntities off in the server's OpenLineage settings; return ``changed`` or ``current``. Raises RuntimeError when refused."""
    status, current = base.call("GET", f"{api}/v1/system/settings/openLineageSettings", session=session)
    value = current.get("config_value") if status == 200 and isinstance(current, dict) else None
    if not isinstance(value, dict):
        raise RuntimeError("the OpenLineage settings could not be read")
    if value.get("autoCreateEntities") is False:
        return "current"
    body = {"config_type": "openLineageSettings", "config_value": {**value, "autoCreateEntities": False}}
    if base.call("PUT", f"{api}/v1/system/settings", body, session)[0] != 200:
        raise RuntimeError("the OpenLineage settings change was refused")
    return "changed"


def search_reindex(api: str, session: str) -> str:
    """Start one search reindex when the catalog has tables and the table search index is empty. Return ``reindex started``, ``index current`` or
    ``nothing to index``. The reindex runs on the server after the call returns, so the answer does not wait for it.
    Raises RuntimeError when a count or the start is refused."""
    status, listed = base.call("GET", f"{api}/v1/tables?limit=1", session=session)
    paging = listed.get("paging") if status == 200 and isinstance(listed, dict) else None
    tables = paging.get("total") if isinstance(paging, dict) else None
    if not isinstance(tables, int):
        raise RuntimeError("the table count could not be read")
    if tables == 0:
        return "nothing to index"
    status, found = base.call("GET", f"{api}/v1/search/query?q=*&index=table_search_index&from=0&size=0", session=session)
    if status == 404:
        documents = 0
    else:
        total = (((found.get("hits") or {}).get("total") or {}).get("value")) if status == 200 and isinstance(found, dict) else None
        if not isinstance(total, int):
            raise RuntimeError("the search index could not be read")
        documents = total
    if documents > 0:
        return "index current"
    if base.call("POST", f"{api}/v1/apps/trigger/SearchIndexingApplication", {"recreateIndex": True, "entities": ["all"]}, session)[0] != 200:
        raise RuntimeError("the reindex was refused")
    return "reindex started"


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
        return fail("OM_BOT_PARAMETERS must name one distinct absolute parameter for each of the callers lineage and ingestion", 2)
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
        problem = base.password_problem(secret)
        if problem:
            return fail(problem)
        # OpenMetadata 2.0.7 UserResource: POST /v1/users/login Base64-decodes the password (base.login encodes it); PUT /v1/users/changePassword passes the
        # ChangePasswordRequest fields to BasicAuthenticator.changeUserPwdWithOldPwd as they are, so these three fields are plain text.
        body = {"username": "admin", "oldPassword": base.FACTORY_LOGIN_VALUE, "newPassword": secret, "confirmPassword": secret, "requestType": "SELF"}
        status, reply = base.call("PUT", f"{api}/v1/users/changePassword", body, session)
        if status != 200:
            return fail(f"the administrator password change was refused ({base.refusal_detail(status, reply, secret)})")
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
    for caller in (caller for caller in RETIRED_CALLERS if caller not in CALLER_ROLES):
        try:
            sys.stdout.write(f"openmetadata_persistent_bootstrap: {caller} bot {retire_bot(api, session, caller)}\n")
        except RuntimeError as error:
            failed.append(f"{caller} retirement ({error})")
        except Exception as error:
            failed.append(f"{caller} retirement ({type(error).__name__})")
    try:
        sys.stdout.write(f"openmetadata_persistent_bootstrap: OpenLineage settings {lineage_settings(api, session)}\n")
    except RuntimeError as error:
        failed.append(f"OpenLineage settings ({error})")
    try:
        sys.stdout.write(f"openmetadata_persistent_bootstrap: search {search_reindex(api, session)}\n")
    except RuntimeError as error:
        failed.append(f"search reindex ({error})")
    if failed:
        return fail(f"failed for: {'; '.join(failed)}")
    sys.stdout.write("openmetadata_persistent_bootstrap: ready\n")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
