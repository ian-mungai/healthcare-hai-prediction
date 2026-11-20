"""Exercise the bootstrap of the persistent OpenMetadata service offline, against a stateful HTTP stand-in and an in-memory Parameter Store.

The stand-in models the OpenMetadata REST routes the bootstrap calls, as the author knew them:
the routes of the finite catalog task's bootstrap plus bot creation.
It is not the real server, so these cases prove the script's own behavior: password change, one bot and one token parameter per caller, no value in the output,
reruns that change nothing, and visible failure. Nothing reaches AWS or Docker. The runner writes one report per run.
"""

from __future__ import annotations

import argparse
import base64
import contextlib
import hashlib
import io
import json
import sys
import threading
import uuid
from collections.abc import Callable, Iterator
from datetime import UTC, datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

from scripts.infrastructure import openmetadata_persistent_bootstrap as bootstrap
from scripts.infrastructure.openmetadata_bootstrap import ADMIN_EMAIL

ROOT = Path(__file__).resolve().parents[2]
ADMIN_VALUE = "Synthetic-Admin-1!"
FACTORY_VALUE = "admin"
PARAMETERS = {
    "lineage": "/example/openmetadata_bot_lineage",
    "ingestion": "/example/openmetadata_bot_ingestion",
}
OBSERVED: dict[str, Any] = {}
# The caller that earlier releases created and this release retires, with the token parameter those releases wrote for it.
RETIRED_BOT = "comparison-bot"
RETIRED_PARAMETER = "/example/retired_caller_token"


def bot_id(name: str) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_URL, "bot:" + name))


def require(condition: object, message: object) -> None:
    if not condition:
        raise AssertionError(message)


class FakeParameterStore:
    """The two Parameter Store calls the bootstrap makes, kept in memory, with an optional injected failure."""

    def __init__(self) -> None:
        self.values: dict[str, str] = {}
        self.writes: list[tuple[str, str, bool]] = []
        self.fail_put = False

    def get_parameter(self, **kwargs: Any) -> dict[str, Any]:
        require(kwargs.get("WithDecryption") is True, "a SecureString parameter was read without decryption")
        name = kwargs["Name"]
        if name not in self.values:
            raise bootstrap.ParameterMissing(name)
        return {"Parameter": {"Name": name, "Value": self.values[name]}}

    def put_parameter(self, **kwargs: Any) -> dict[str, Any]:
        if self.fail_put:
            raise PermissionError("AccessDeniedException")
        require(kwargs.get("Type") == "SecureString" and kwargs.get("Overwrite") is True, "a token parameter must be a SecureString written with overwrite")
        self.values[kwargs["Name"]] = kwargs["Value"]
        self.writes.append((kwargs["Name"], hashlib.sha256(kwargs["Value"].encode()).hexdigest(), True))
        return {"Version": len(self.writes)}


class FakeServer:
    """State and switches of the OpenMetadata stand-in."""

    def __init__(self) -> None:
        self.admin = FACTORY_VALUE
        self.healthy_after = 0
        self.version_calls = 0
        self.bots: dict[str, str] = {}
        self.users: dict[str, dict[str, Any]] = {}
        self.calls: list[tuple[str, str]] = []
        self.refuse_bot: str | None = None
        self.refuse_delete = False
        self.fail_user_read = False
        self.malformed_bot = False
        self.fail_once: str | None = None
        self.revoked: list[str] = []
        self.missing_role: str | None = None
        self.password_changes = 0
        # OpenMetadata 2.0.7 seeds openLineageSettings in its database on first start (SettingsCache) with these values; no config file sets them.
        self.openlineage: dict[str, Any] = {
            "enabled": True,
            "autoCreateEntities": True,
            "defaultPipelineService": "openlineage",
            "eventTypeFilter": ["COMPLETE"],
        }
        self.refuse_settings = False
        self.settings_writes = 0
        self.password_refusal: tuple[int, str] | None = None
        # The catalogued tables and the documents of the table search index: None makes the index answer 404 as OpenSearch does for an index it does not have.
        self.tables = 0
        self.index_docs: int | None = 0
        self.refuse_trigger = False
        self.unreadable_count = False
        self.triggers: list[dict[str, Any]] = []


@contextlib.contextmanager
def open_metadata() -> Iterator[tuple[FakeServer, str]]:
    state = FakeServer()
    lock = threading.Lock()

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, _format: str, *args: Any) -> None:
            """Keep synthetic requests out of the console."""

        def reply(self, status: int, body: dict[str, Any] | None = None) -> None:
            data = json.dumps(body or {}).encode()
            self.send_response(status)
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def reply_text(self, status: int, text: str) -> None:
            data = text.encode()
            self.send_response(status)
            self.send_header("Content-Type", "text/plain")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def body(self) -> dict[str, Any]:
            length = int(self.headers.get("Content-Length") or 0)
            return json.loads(self.rfile.read(length)) if length else {}

        def authorized(self) -> bool:
            return self.headers.get("Authorization") == "Bearer admin-session"

        def do_GET(self) -> None:
            with lock:
                state.calls.append(("GET", self.path))
                if self.path == "/api/v1/system/version":
                    state.version_calls += 1
                    return self.reply(200 if state.version_calls > state.healthy_after else 503)
                if not self.authorized():
                    return self.reply(401)
                if self.path == "/api/v1/system/settings/openLineageSettings":
                    return self.reply(200, {"config_type": "openLineageSettings", "config_value": dict(state.openlineage)})
                if self.path.startswith("/api/v1/tables?"):
                    return self.reply(500) if state.unreadable_count else self.reply(200, {"data": [], "paging": {"total": state.tables}})
                if self.path.startswith("/api/v1/search/query?"):
                    if state.index_docs is None:
                        return self.reply(404)
                    return self.reply(200, {"hits": {"total": {"value": state.index_docs, "relation": "eq"}, "hits": []}})
                if self.path.startswith("/api/v1/roles/name/"):
                    name = self.path.rsplit("/", 1)[1]
                    return self.reply(404) if name == state.missing_role else self.reply(200, {"id": str(uuid.uuid5(uuid.NAMESPACE_DNS, name)), "name": name})
                if self.path.startswith("/api/v1/bots/name/"):
                    name = self.path.rsplit("/", 1)[1]
                    return (
                        self.reply(200, {"id": bot_id(name), "name": name, "botUser": "unexpected" if state.malformed_bot else {"id": state.bots[name]}})
                        if name in state.bots
                        else self.reply(404)
                    )
                if self.path.startswith("/api/v1/users/") and "/auth-mechanism/" not in self.path:
                    if state.fail_user_read:
                        return self.reply(503)
                    target = self.path.split("?", 1)[0].rsplit("/", 1)[1]
                    if self.path.startswith("/api/v1/users/name/"):
                        found = state.users.get(target)
                    else:
                        found = next((item for item in state.users.values() if item["id"] == target), None)
                    shown = ("id", "name", "email", "isBot", "isAdmin")
                    return self.reply(200, {key: found[key] for key in shown if key in found}) if found else self.reply(404)
                if self.path.startswith("/api/v1/users/auth-mechanism/"):
                    user_id = self.path.rsplit("/", 1)[1]
                    user = next((item for item in state.users.values() if item["id"] == user_id), None)
                    return self.reply(200, {"authType": "JWT", "config": {"JWTToken": user["token"]}}) if user else self.reply(404)
            self.reply(404)

        def do_POST(self) -> None:
            with lock:
                state.calls.append(("POST", self.path))
                if self.path == "/api/v1/users/login":
                    request = self.body()
                    secret = base64.b64decode(request["password"]).decode()
                    if request["email"] == ADMIN_EMAIL and secret == state.admin:
                        return self.reply(200, {"accessToken": "admin-session"})
                    return self.reply(401)
                if self.path == "/api/v1/apps/trigger/SearchIndexingApplication":
                    if not self.authorized():
                        return self.reply(401)
                    request = self.body()
                    if state.refuse_trigger:
                        return self.reply(403)
                    require(request.get("recreateIndex") is True, "the reindex must recreate the index")
                    state.triggers.append(request)
                    state.index_docs = state.tables
                    return self.reply(200)
            self.reply(404)

        def do_PUT(self) -> None:
            with lock:
                state.calls.append(("PUT", self.path))
                if not self.authorized():
                    return self.reply(401)
                request = self.body()
                if self.path == "/api/v1/system/settings":
                    if state.refuse_settings:
                        return self.reply(403)
                    require(request.get("config_type") == "openLineageSettings", f"only the OpenLineage settings may change: {request.get('config_type')}")
                    state.openlineage = dict(request["config_value"])
                    state.settings_writes += 1
                    return self.reply(200, request)
                if self.path == "/api/v1/users/revokeToken":
                    if state.refuse_delete:
                        return self.reply(403)
                    if state.fail_once == "revoke":
                        state.fail_once = None
                        return self.reply(503)
                    require(any(item["id"] == request.get("id") for item in state.users.values()), "a token revocation must name a known user")
                    state.revoked.append(str(request["id"]))
                    return self.reply(200, {})
                if self.path == "/api/v1/users/changePassword":
                    # UserResource.changeUserPassword of 2.0.7 answers a success with the plain text below, not JSON, and a refusal with {"code", "message"}.
                    if state.password_refusal is not None:
                        return self.reply(state.password_refusal[0], {"code": state.password_refusal[0], "message": state.password_refusal[1]})
                    if request["oldPassword"] != state.admin or request["newPassword"] != request["confirmPassword"]:
                        return self.reply(400, {"code": 400, "message": "Old Password is not correct"})
                    state.admin = request["newPassword"]
                    state.password_changes += 1
                    return self.reply_text(200, "Password Updated Successfully")
                if self.path == "/api/v1/users":
                    require(request.get("isBot") is True and request["authenticationMechanism"]["authType"] == "JWT", "a bot user must be a JWT bot")
                    require(request["email"] == f"{request['name']}@example.org", "a bot email is on the reserved domain the server is configured with")
                    require(request["authenticationMechanism"]["config"]["JWTTokenExpiry"] == "Unlimited", "a bot token must not expire silently")
                    require(request.get("roles") and len(request["roles"]) == 1, "a bot user needs exactly one role")
                    existing = state.users.get(request["name"])
                    if existing is None:
                        digest = hashlib.sha256(f"{request['name']}-token-material".encode()).hexdigest()
                        existing = {
                            "id": str(uuid.uuid5(uuid.NAMESPACE_URL, request["name"])),
                            "token": f"synthetic.{digest}.jwt",
                            "name": request["name"],
                            "email": request["email"],
                            "isBot": True,
                            "isAdmin": False,
                        }
                        state.users[request["name"]] = existing
                    return self.reply(200, {"id": existing["id"], "name": request["name"]})
                if self.path == "/api/v1/bots":
                    if request["name"] == state.refuse_bot:
                        return self.reply(403)
                    state.bots[request["name"]] = state.users[request["botUser"]]["id"]
                    return self.reply(200, {"name": request["name"]})
            self.reply(404)

        def do_DELETE(self) -> None:
            with lock:
                state.calls.append(("DELETE", self.path))
                if not self.authorized():
                    return self.reply(401)
                if state.refuse_delete:
                    return self.reply(403)
                route, _, query = self.path.partition("?")
                if state.fail_once == "delete_" + route.rsplit("/", 2)[1][:-1]:
                    state.fail_once = None
                    return self.reply(503)
                require(query == "hardDelete=true", "a retired bot and user are deleted for good")
                _, _, kind, target = route.rsplit("/", 3)
                if kind == "bots":
                    name = next((item for item in state.bots if bot_id(item) == target), None)
                    if name is None:
                        return self.reply(404)
                    del state.bots[name]
                    return self.reply(200, {})
                if kind == "users":
                    name = next((key for key, item in state.users.items() if item["id"] == target), None)
                    if name is None:
                        return self.reply(404)
                    del state.users[name]
                    return self.reply(200, {})
            self.reply(404)

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.05}, name="om_standin")
    thread.start()
    try:
        yield state, f"http://127.0.0.1:{server.server_port}/api"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)
        require(not thread.is_alive(), "the OpenMetadata stand-in did not stop")


def environment(base: str, **changes: str | None) -> dict[str, str]:
    values = {"OM_API": base, "OM_ADMIN_PASSWORD": ADMIN_VALUE, "OM_BOT_PARAMETERS": json.dumps(PARAMETERS), "AWS_REGION": "us-west-2"}
    for name, value in changes.items():
        if value is None:
            values.pop(name, None)
        else:
            values[name] = value
    return values


def run(env: dict[str, str], store: FakeParameterStore, *extra: str) -> tuple[int, str, str]:
    out, err = io.StringIO(), io.StringIO()
    with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
        code = bootstrap.main(["--deadline-seconds", "3", "--poll-seconds", "0", *extra], env=env, ssm=store)
    return code, out.getvalue(), err.getvalue()


def assert_no_values(*texts: str) -> None:
    for text in texts:
        for value in (ADMIN_VALUE, FACTORY_VALUE + "-", "synthetic."):
            require(value not in text, "a password or a token reached the output")


def fresh_start() -> None:
    with open_metadata() as (state, base):
        store = FakeParameterStore()
        code, out, err = run(environment(base), store)
        require(code == 0, f"a fresh start failed: {err}")
        require(
            not [call for call in state.calls if call[0] == "DELETE" or call[1].endswith("/revokeToken")], "a bot that is absent must be a no-op, not a request"
        )
        require(state.admin == ADMIN_VALUE and state.password_changes == 1, "the factory password was not replaced exactly once")
        require(sorted(state.bots) == ["ingestion-bot", "lineage-bot"], f"wrong bots: {sorted(state.bots)}")
        require(set(store.values) == set(PARAMETERS.values()), "every caller needs its own token parameter")
        require(len(set(store.values.values())) == 2, "two callers received the same token")
        require(state.openlineage["autoCreateEntities"] is False, "the OpenLineage API must not create tables or pipelines for unknown datasets")
        require(
            {key: value for key, value in state.openlineage.items() if key != "autoCreateEntities"}
            == {"enabled": True, "defaultPipelineService": "openlineage", "eventTypeFilter": ["COMPLETE"]},
            f"the other OpenLineage settings must stay as the server had them: {state.openlineage}",
        )
        for caller, name in PARAMETERS.items():
            require(store.values[name] == state.users[f"{caller}-bot"]["token"], f"{caller} parameter does not hold its own bot token")
        assert_no_values(out, err)
        OBSERVED["fresh_start"] = {"bots": sorted(state.bots), "parameters_written": len(store.writes), "password_changes": state.password_changes}


def rerun_changes_nothing() -> None:
    with open_metadata() as (state, base):
        store = FakeParameterStore()
        require(run(environment(base), store)[0] == 0, "the first run failed")
        writes, calls = list(store.writes), len(state.calls)
        code, out, err = run(environment(base), store)
        require(code == 0, f"the rerun failed: {err}")
        require(store.writes == writes, "a rerun wrote a parameter although every value was current")
        require(state.password_changes == 1, "a rerun changed the administrator password again")
        mutating = [call for call in state.calls[calls:] if call[0] in ("PUT",)]
        require(mutating == [], f"a rerun sent a change request: {mutating}")
        assert_no_values(out, err)
        OBSERVED["rerun"] = {"extra_writes": 0, "extra_changes": 0}


def stale_parameter_is_repaired() -> None:
    with open_metadata() as (state, base):
        store = FakeParameterStore()
        run(environment(base), store)
        store.values[PARAMETERS["ingestion"]] = "stale-value"
        before = len(store.writes)
        code, _out, err = run(environment(base), store)
        require(code == 0, f"the repair run failed: {err}")
        require(
            len(store.writes) == before + 1 and store.values[PARAMETERS["ingestion"]] == state.users["ingestion-bot"]["token"],
            "the stale token was not replaced alone",
        )
        OBSERVED["stale_parameter"] = {"rewritten": ["ingestion"]}


def server_that_never_starts_fails_in_time() -> None:
    with open_metadata() as (state, base):
        state.healthy_after = 10_000
        store = FakeParameterStore()
        code, _out, err = run(environment(base), store, "--deadline-seconds", "1")
        require(code == 1 and "not healthy within" in err, f"an unhealthy server must fail visibly: {code} {err}")
        require(store.writes == [] and state.password_changes == 0, "nothing may be written while the server is down")
        OBSERVED["unhealthy_server"] = {"exit": code}


def refused_configuration_changes_nothing() -> None:
    with open_metadata() as (state, base):
        store = FakeParameterStore()
        cases = {
            "no admin password": environment(base, OM_ADMIN_PASSWORD=None),
            "parameters not json": environment(base, OM_BOT_PARAMETERS="lineage"),
            "unknown caller": environment(base, OM_BOT_PARAMETERS=json.dumps({**PARAMETERS, "extra": "/example/x"})),
            "removed caller": environment(base, OM_BOT_PARAMETERS=json.dumps({**PARAMETERS, "comparison": "/example/x"})),
            "missing caller": environment(base, OM_BOT_PARAMETERS=json.dumps({"lineage": PARAMETERS["lineage"]})),
            "shared parameter": environment(base, OM_BOT_PARAMETERS=json.dumps({**PARAMETERS, "ingestion": PARAMETERS["lineage"]})),
            "relative parameter": environment(base, OM_BOT_PARAMETERS=json.dumps({**PARAMETERS, "lineage": "lineage"})),
        }
        for name, env in cases.items():
            code, _out, err = run(env, store)
            require(code == 2, f"{name}: expected exit 2, got {code} ({err})")
        require(state.calls == [] and store.writes == [], "a refused configuration reached the server or the store")
        OBSERVED["refused_configuration"] = sorted(cases)


def refused_bot_fails_but_others_finish() -> None:
    with open_metadata() as (state, base):
        state.refuse_bot = "ingestion-bot"
        store = FakeParameterStore()
        code, _out, err = run(environment(base), store)
        require(code == 1 and "ingestion" in err, f"a refused bot must fail visibly and name the caller: {code} {err}")
        require(PARAMETERS["ingestion"] not in store.values, "a token was written for a bot that was never created")
        require(PARAMETERS["lineage"] in store.values, "one refused bot stopped the other")
        assert_no_values(err)
        OBSERVED["refused_bot"] = {"exit": code}


def missing_role_fails_visibly() -> None:
    with open_metadata() as (state, base):
        state.missing_role = "IngestionBotRole"
        store = FakeParameterStore()
        code, _out, err = run(environment(base), store)
        require(code == 1 and "lineage" in err and "role" in err, f"a missing role must fail and name the caller: {code} {err}")
        require(not store.values, "a token was written for a caller without its role")
        OBSERVED["missing_role"] = {"exit": code}


def store_failure_names_only_the_class() -> None:
    with open_metadata() as (_state, base):
        store = FakeParameterStore()
        store.fail_put = True
        code, out, err = run(environment(base), store)
        require(code == 1 and "PermissionError" in err, f"a store failure must fail with its class: {code} {err}")
        assert_no_values(out, err)
        OBSERVED["store_failure"] = {"exit": code}


def wrong_stored_password_with_changed_factory_value_fails() -> None:
    with open_metadata() as (state, base):
        state.admin = "someone-changed-it"
        store = FakeParameterStore()
        code, _out, err = run(environment(base), store)
        require(code == 1 and "neither" in err, f"an unknown administrator password must fail visibly: {code} {err}")
        require(store.writes == [] and state.password_changes == 0, "nothing may change when no login works")
        OBSERVED["unknown_admin_password"] = {"exit": code}


def openlineage_settings_refusal_fails_visibly() -> None:
    with open_metadata() as (state, base):
        state.refuse_settings = True
        store = FakeParameterStore()
        code, out, err = run(environment(base), store)
        require(code == 1 and "OpenLineage settings" in err, f"a refused settings change must fail and say so: {code} {err}")
        require(set(store.values) == set(PARAMETERS.values()), "the bot tokens are still issued when only the settings change is refused")
        require(state.openlineage["autoCreateEntities"] is True, "the stand-in kept its seeded value")
        assert_no_values(out, err)


def openlineage_settings_already_off_are_left_alone() -> None:
    with open_metadata() as (state, base):
        state.openlineage["autoCreateEntities"] = False
        code, _, err = run(environment(base), FakeParameterStore())
        require(code == 0 and state.settings_writes == 0, f"settings that are already right must not be written: {state.settings_writes} {err}")


def reindex_starts_when_tables_exist_and_the_index_is_empty() -> None:
    with open_metadata() as (state, base):
        state.tables = 12
        code, out, err = run(environment(base), FakeParameterStore())
        require(code == 0, f"a restored catalog with an empty index failed: {err}")
        require(len(state.triggers) == 1 and state.index_docs == 12, f"one reindex must run: {state.triggers}")
        require("search reindex started" in out, f"the output must say the reindex started: {out}")
        code, out, err = run(environment(base), FakeParameterStore())
        require(code == 0 and len(state.triggers) == 1 and "search index current" in out, f"a rerun must not index again: {len(state.triggers)} {out}")
        assert_no_values(out, err)


def reindex_treats_a_missing_index_as_empty() -> None:
    with open_metadata() as (state, base):
        state.tables, state.index_docs = 3, None
        code, out, err = run(environment(base), FakeParameterStore())
        require(code == 0 and len(state.triggers) == 1, f"a missing index must be rebuilt: {code} {err} {state.triggers}")


def reindex_is_not_started_without_tables() -> None:
    with open_metadata() as (state, base):
        code, out, err = run(environment(base), FakeParameterStore())
        require(code == 0 and state.triggers == [] and "nothing to index" in out, f"an empty catalog needs no reindex: {out} {err}")


def reindex_is_not_started_when_the_index_has_documents() -> None:
    with open_metadata() as (state, base):
        state.tables, state.index_docs = 12, 12
        code, out, err = run(environment(base), FakeParameterStore())
        require(code == 0 and state.triggers == [], f"a filled index is left alone: {err} {state.triggers}")


def reindex_refusal_fails_visibly_after_the_tokens_are_issued() -> None:
    with open_metadata() as (state, base):
        state.tables, state.refuse_trigger = 5, True
        store = FakeParameterStore()
        code, out, err = run(environment(base), store)
        require(code == 1 and "search reindex" in err, f"a refused reindex must fail and say so: {code} {err}")
        require(set(store.values) == set(PARAMETERS.values()), "the bot tokens are still issued when only the reindex is refused")
        assert_no_values(out, err)


def unreadable_table_count_fails_visibly() -> None:
    with open_metadata() as (state, base):
        state.unreadable_count = True
        code, out, err = run(environment(base), FakeParameterStore())
        require(code == 1 and "search reindex" in err and state.triggers == [], f"an unreadable count must fail without a reindex: {code} {err}")


def refused_password_change_shows_status_and_server_message() -> None:
    with open_metadata() as (state, base):
        state.password_refusal = (
            400,
            f"Password must be of minimum 8 characters, with one special, one Upper, one lower case character, and one Digit. {ADMIN_VALUE}",
        )
        store = FakeParameterStore()
        code, out, err = run(environment(base), store)
        require(code == 1 and "administrator password change was refused" in err, f"a refused change must fail and say so: {code} {err}")
        require("HTTP 400" in err and "Password must be of minimum 8 characters" in err, f"the status and the server's reason must be shown: {err}")
        require(state.password_changes == 0 and store.writes == [], "nothing may change after a refused password change")
        assert_no_values(out, err)
        OBSERVED["refused_password_change"] = {"exit": code, "stderr_has_status": True}


def stored_password_outside_the_server_policy_is_refused_first() -> None:
    bad = {
        "longer than 56 characters": "Aa1!" + "x" * 53,
        "shorter than 8 characters": "Aa1!xyz",
        "no special character": "Aa1" + "x" * 20,
        "no digit": "Aa!" + "x" * 20,
        "no upper case letter": "aa1!" + "x" * 20,
        "no lower case letter": "AA1!" + "X" * 20,
        "white space": "Aa1! " + "x" * 20,
    }
    for name, value in bad.items():
        with open_metadata() as (state, base):
            store = FakeParameterStore()
            code, out, err = run(environment(base, OM_ADMIN_PASSWORD=value), store)
            require(code == 1 and "OpenMetadata password policy" in err and "8 to 56" in err, f"{name}: the policy must be named: {code} {err}")
            require(not [call for call in state.calls if call[0] == "PUT"] and state.password_changes == 0, f"{name}: no change request may be sent")
            require(store.writes == [], f"{name}: nothing may be written")
            require(value not in out + err, f"{name}: the password reached the output")
    with open_metadata() as (state, base):
        state.admin = "Aa1!" + "x" * 53
        code, _out, err = run(environment(base, OM_ADMIN_PASSWORD=state.admin), FakeParameterStore())
        require(code == 0, f"a stored password that already logs in is never judged by the policy: {err}")
    with open_metadata() as (state, base):
        value = "Aa1!" + "x" * 52
        code, _out, err = run(environment(base, OM_ADMIN_PASSWORD=value), FakeParameterStore())
        require(code == 0 and state.admin == value, f"a 56-character password is within the policy: {err}")
    OBSERVED["password_policy"] = sorted(bad)


def seed_bot(
    state: FakeServer, name: str, *, email: str | None = None, admin: bool | None = False, store: FakeParameterStore | None = None, parameter: str = ""
) -> None:
    """A bot and its user as an earlier run or someone else left them; with a store and a parameter, also the token parameter an earlier run wrote."""
    user: dict[str, Any] = {
        "id": str(uuid.uuid5(uuid.NAMESPACE_URL, name)),
        "token": f"synthetic.{hashlib.sha256(name.encode()).hexdigest()}.jwt",
        "name": name,
        "email": email or f"{name}@example.org",
        "isBot": True,
    }
    if admin is not None:
        user["isAdmin"] = admin
    state.users[name] = user
    state.bots[name] = user["id"]
    if store is not None and parameter:
        store.values[parameter] = user["token"]


def retirements(state: FakeServer, since: int = 0) -> list[tuple[str, str]]:
    return [call for call in state.calls[since:] if call[0] == "DELETE" or call[1].endswith("/revokeToken")]


def retired_bot_is_removed_on_upgrade() -> None:
    require(RETIRED_BOT.removesuffix("-bot") not in bootstrap.CALLER_ROLES, "the bootstrap still creates a bot for the retired caller")
    require(
        (RETIRED_BOT.removesuffix("-bot"),) == bootstrap.RETIRED_CALLERS,
        f"the bootstrap retires more or fewer callers than the one: {bootstrap.RETIRED_CALLERS}",
    )
    with open_metadata() as (state, base):
        store = FakeParameterStore()
        code, _out, err = run(environment(base), store)
        require(code == 0, f"the current callers failed first: {err}")
        seed_bot(state, RETIRED_BOT, store=store, parameter=RETIRED_PARAMETER)
        require(sorted(state.bots) == [RETIRED_BOT, "ingestion-bot", "lineage-bot"], "the earlier release left three bots")
        code, out, err = run(environment(base), store)
        require(code == 0, f"the upgrade run failed: {code} {err}")
        require(sorted(state.bots) == ["ingestion-bot", "lineage-bot"], f"the retired bot is still there: {sorted(state.bots)}")
        require(sorted(state.users) == ["ingestion-bot", "lineage-bot"], f"the retired bot user is still there: {sorted(state.users)}")
        require(len(state.revoked) == 1 and state.revoked[0] == str(uuid.uuid5(uuid.NAMESPACE_URL, RETIRED_BOT)), "the retired token was not revoked")
        require([call[0] for call in retirements(state)] == ["PUT", "DELETE", "DELETE"], f"revoke, then delete the bot and its user: {retirements(state)}")
        require(RETIRED_PARAMETER in store.values, "the bootstrap role cannot delete a parameter; that is an owner step")
        require("retired" in out and "comparison" in out, f"the run must say what it retired: {out}")
        assert_no_values(out, err)
        seen = len(state.calls)
        code, out, err = run(environment(base), store)
        require(code == 0 and retirements(state, seen) == [], f"a rerun must retire nothing: {retirements(state, seen)}")
        OBSERVED["retired_bot"] = {"bots_after": sorted(state.bots), "requests": [call[0] for call in retirements(state)]}


def foreign_bots_are_left_alone() -> None:
    with open_metadata() as (state, base):
        store = FakeParameterStore()
        seed_bot(state, "airflow-bot", email="airflow-bot@other.example")
        seed_bot(state, RETIRED_BOT + "-extra")
        before = sorted(state.bots)
        code, out, err = run(environment(base), store)
        require(code == 0, f"foreign bots must not fail a run: {err}")
        require(retirements(state) == [] and set(before) <= set(state.bots), f"a foreign bot was touched: {retirements(state)}")
    with open_metadata() as (state, base):
        store = FakeParameterStore()
        seed_bot(state, RETIRED_BOT, email="someone@other.example")
        code, out, err = run(environment(base), store)
        require(
            code == 0 and RETIRED_BOT in state.bots and RETIRED_BOT in state.users, f"a bot with this name that the project did not create must stay: {err}"
        )
        require(retirements(state) == [], f"nothing may be revoked or deleted: {retirements(state)}")
        require("left" in out, f"the run must say it left the bot: {out}")
        OBSERVED["foreign_bots"] = {"requests": 0}


def refused_retirement_fails_visibly_but_others_finish() -> None:
    with open_metadata() as (state, base):
        store = FakeParameterStore()
        seed_bot(state, RETIRED_BOT, store=store, parameter=RETIRED_PARAMETER)
        state.refuse_delete = True
        code, out, err = run(environment(base), store)
        require(code == 1 and "comparison" in err, f"a refused retirement must fail and name the caller: {code} {err}")
        require(RETIRED_BOT in state.bots, "a refused retirement must leave the bot where it is")
        require(PARAMETERS["lineage"] in store.values and PARAMETERS["ingestion"] in store.values, "a refused retirement stopped the other callers")
        assert_no_values(out, err)
        OBSERVED["refused_retirement"] = {"exit": code}


def retirement_is_refused_for_an_administrator_or_unconfirmed_user() -> None:
    for label, admin in (("administrator", True), ("administrator status not reported", None)):
        with open_metadata() as (state, base):
            store = FakeParameterStore()
            seed_bot(state, RETIRED_BOT, admin=admin, store=store, parameter=RETIRED_PARAMETER)
            code, out, err = run(environment(base), store)
            require(code == 0, f"{label}: a refusal to retire is not a failure of the run: {err}")
            require(retirements(state) == [], f"{label}: nothing may be revoked or deleted: {retirements(state)}")
            require(RETIRED_BOT in state.bots and RETIRED_BOT in state.users, f"{label}: the bot and its user must stay")
            require("left" in out and "administrator" in out, f"{label}: the run must say why it left the bot: {out}")
            assert_no_values(out, err)
    OBSERVED["administrator_user"] = {"requests": 0}


def failed_user_read_fails_the_run_and_changes_nothing() -> None:
    with open_metadata() as (state, base):
        store = FakeParameterStore()
        seed_bot(state, RETIRED_BOT, store=store, parameter=RETIRED_PARAMETER)
        state.fail_user_read = True
        code, out, err = run(environment(base), store)
        require(code == 1 and "comparison" in err and "read" in err, f"a failed lookup must fail the run and name the caller: {code} {err}")
        require("ready" not in out, f"the run must not report ready: {out}")
        require(retirements(state) == [] and RETIRED_BOT in state.bots and RETIRED_BOT in state.users, "nothing may change after a failed lookup")
        require(PARAMETERS["lineage"] in store.values and PARAMETERS["ingestion"] in store.values, "a failed lookup stopped the other callers")
        state.fail_user_read = False
        code, out, err = run(environment(base), store)
        require(code == 0 and sorted(state.bots) == ["ingestion-bot", "lineage-bot"], f"the next run must finish the retirement: {code} {err}")
        OBSERVED["failed_user_read"] = {"exit": 1}


def every_failed_retirement_step_recovers_on_the_next_run() -> None:
    after_failure = {
        "revoke": (True, True),
        "delete_bot": (True, True),
        "delete_user": (False, True),
    }
    for step, (bot_stays, user_stays) in after_failure.items():
        with open_metadata() as (state, base):
            store = FakeParameterStore()
            seed_bot(state, RETIRED_BOT, store=store, parameter=RETIRED_PARAMETER)
            state.fail_once = step
            code, _out, err = run(environment(base), store)
            require(code == 1 and "comparison" in err, f"{step}: the failed step must fail the run: {code} {err}")
            require(
                (RETIRED_BOT in state.bots) == bot_stays and (RETIRED_BOT in state.users) == user_stays,
                f"{step}: unexpected state {sorted(state.bots)} {sorted(state.users)}",
            )
            code, out, err = run(environment(base), store)
            require(code == 0, f"{step}: the rerun failed: {err}")
            require(
                sorted(state.bots) == ["ingestion-bot", "lineage-bot"] and sorted(state.users) == ["ingestion-bot", "lineage-bot"],
                f"{step}: not retired after the rerun",
            )
            seen = len(state.calls)
            code, _out, _err = run(environment(base), store)
            require(code == 0 and retirements(state, seen) == [], f"{step}: a further rerun must send nothing")
    OBSERVED["recovery_steps"] = sorted(after_failure)


def an_unexpected_bot_answer_fails_the_run_with_its_class() -> None:
    with open_metadata() as (state, base):
        store = FakeParameterStore()
        seed_bot(state, RETIRED_BOT, store=store, parameter=RETIRED_PARAMETER)
        state.malformed_bot = True
        code, out, err = run(environment(base), store)
        require(code == 1 and "comparison" in err and "AttributeError" in err, f"an unexpected answer must fail the run with its class: {code} {err}")
        require(retirements(state) == [] and RETIRED_BOT in state.users, "nothing may change after an unexpected answer")
        require(PARAMETERS["lineage"] in store.values, "an unexpected answer stopped the other callers")


def an_orphaned_user_is_checked_like_a_bot_user_before_it_is_deleted() -> None:
    for label, kwargs in (
        ("foreign email", {"email": "someone@other.example"}),
        ("administrator", {"admin": True}),
        ("no administrator evidence", {"admin": None}),
    ):
        with open_metadata() as (state, base):
            store = FakeParameterStore()
            seed_bot(state, RETIRED_BOT, **kwargs)
            del state.bots[RETIRED_BOT]
            code, out, err = run(environment(base), store)
            require(code == 0 and "left" in out, f"{label}: an orphan that is not the project's must be left: {code} {out} {err}")
            require(retirements(state) == [] and RETIRED_BOT in state.users, f"{label}: nothing may be revoked or deleted")
    with open_metadata() as (state, base):
        store = FakeParameterStore()
        seed_bot(state, RETIRED_BOT)
        del state.bots[RETIRED_BOT]
        state.fail_user_read = True
        code, out, err = run(environment(base), store)
        require(code == 1 and "comparison" in err and "ready" not in out, f"a failed orphan lookup must fail the run: {code} {err}")
        require(RETIRED_BOT in state.users and retirements(state) == [], "nothing may change after a failed orphan lookup")


CASES: dict[str, Callable[[], None]] = {
    "retirement_is_refused_for_an_administrator_or_unconfirmed_user": retirement_is_refused_for_an_administrator_or_unconfirmed_user,
    "failed_user_read_fails_the_run_and_changes_nothing": failed_user_read_fails_the_run_and_changes_nothing,
    "every_failed_retirement_step_recovers_on_the_next_run": every_failed_retirement_step_recovers_on_the_next_run,
    "an_orphaned_user_is_checked_like_a_bot_user_before_it_is_deleted": an_orphaned_user_is_checked_like_a_bot_user_before_it_is_deleted,
    "an_unexpected_bot_answer_fails_the_run_with_its_class": an_unexpected_bot_answer_fails_the_run_with_its_class,
    "retired_bot_is_removed_on_upgrade": retired_bot_is_removed_on_upgrade,
    "foreign_bots_are_left_alone": foreign_bots_are_left_alone,
    "refused_retirement_fails_visibly_but_others_finish": refused_retirement_fails_visibly_but_others_finish,
    "fresh_start": fresh_start,
    "refused_password_change_shows_status_and_server_message": refused_password_change_shows_status_and_server_message,
    "stored_password_outside_the_server_policy_is_refused_first": stored_password_outside_the_server_policy_is_refused_first,
    "openlineage_settings_refusal_fails_visibly": openlineage_settings_refusal_fails_visibly,
    "openlineage_settings_already_off_are_left_alone": openlineage_settings_already_off_are_left_alone,
    "reindex_starts_when_tables_exist_and_the_index_is_empty": reindex_starts_when_tables_exist_and_the_index_is_empty,
    "reindex_treats_a_missing_index_as_empty": reindex_treats_a_missing_index_as_empty,
    "reindex_is_not_started_without_tables": reindex_is_not_started_without_tables,
    "reindex_is_not_started_when_the_index_has_documents": reindex_is_not_started_when_the_index_has_documents,
    "reindex_refusal_fails_visibly_after_the_tokens_are_issued": reindex_refusal_fails_visibly_after_the_tokens_are_issued,
    "unreadable_table_count_fails_visibly": unreadable_table_count_fails_visibly,
    "rerun_changes_nothing": rerun_changes_nothing,
    "stale_parameter_is_repaired": stale_parameter_is_repaired,
    "server_that_never_starts_fails_in_time": server_that_never_starts_fails_in_time,
    "refused_configuration_changes_nothing": refused_configuration_changes_nothing,
    "refused_bot_fails_but_others_finish": refused_bot_fails_but_others_finish,
    "missing_role_fails_visibly": missing_role_fails_visibly,
    "store_failure_names_only_the_class": store_failure_names_only_the_class,
    "wrong_stored_password_with_changed_factory_value_fails": wrong_stored_password_with_changed_factory_value_fails,
}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--case", action="append", choices=sorted(CASES))
    parser.add_argument("--output-root", type=Path, default=ROOT / "data/e2e/openmetadata_persistent")
    args = parser.parse_args()
    output = args.output_root / datetime.now(UTC).strftime("%Y%m%dT%H%M%S_%fZ")
    output.mkdir(parents=True)
    sources = [
        ROOT / path for path in ("scripts/infrastructure/openmetadata_persistent_bootstrap.py", "scripts/infrastructure/run_openmetadata_persistent_e2e.py")
    ]
    results = []
    for name in args.case or CASES:
        try:
            CASES[name]()
        except (AssertionError, OSError, ValueError, KeyError, TypeError, AttributeError, RuntimeError) as error:
            results.append({"case": name, "pass": False, "error": f"{type(error).__name__}: {error}"})
        else:
            results.append({"case": name, "pass": True})
    report = {
        "created_at": datetime.now(UTC).isoformat(),
        "command": [sys.executable, "-m", "scripts.infrastructure.run_openmetadata_persistent_e2e", *sys.argv[1:]],
        "environment": {"python": sys.version.split()[0], "platform": sys.platform},
        "input_fixtures": "synthetic passwords and bot names in this file",
        "boundary": (
            "loopback HTTP stand-in of the OpenMetadata routes as the author knew them and an in-memory Parameter Store; "
            "no real OpenMetadata, AWS or Docker; routes and role names unproven against 2.0.7"
        ),
        "observed": OBSERVED,
        "cases": results,
        "pass": all(row["pass"] for row in results),
        "source_sha256": {str(path.relative_to(ROOT)): hashlib.sha256(path.read_bytes()).hexdigest() for path in sources},
        "cleanup": "no services started; the stand-in server and its thread were joined",
    }
    (output / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    sys.stdout.write(json.dumps({"pass": report["pass"], "report": str(output / "report.json"), "cases": results}) + "\n")
    return 0 if report["pass"] else 1


if __name__ == "__main__":
    sys.exit(main())
