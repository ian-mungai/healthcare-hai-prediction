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

ROOT = Path(__file__).resolve().parents[2]
ADMIN_VALUE = "Synthetic-Admin-1!"
FACTORY_VALUE = "admin"
PARAMETERS = {
    "lineage": "/example/openmetadata_bot_lineage",
    "ingestion": "/example/openmetadata_bot_ingestion",
    "comparison": "/example/openmetadata_bot_comparison",
}
OBSERVED: dict[str, Any] = {}


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
        self.missing_role: str | None = None
        self.password_changes = 0


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
                if self.path.startswith("/api/v1/roles/name/"):
                    name = self.path.rsplit("/", 1)[1]
                    return self.reply(404) if name == state.missing_role else self.reply(200, {"id": str(uuid.uuid5(uuid.NAMESPACE_DNS, name)), "name": name})
                if self.path.startswith("/api/v1/bots/name/"):
                    name = self.path.rsplit("/", 1)[1]
                    return self.reply(200, {"name": name, "botUser": {"id": state.bots[name]}}) if name in state.bots else self.reply(404)
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
                    if request["email"] == "admin@open-metadata.org" and secret == state.admin:
                        return self.reply(200, {"accessToken": "admin-session"})
                    return self.reply(401)
            self.reply(404)

        def do_PUT(self) -> None:
            with lock:
                state.calls.append(("PUT", self.path))
                if not self.authorized():
                    return self.reply(401)
                request = self.body()
                if self.path == "/api/v1/users/changePassword":
                    if request["oldPassword"] != state.admin or request["newPassword"] != request["confirmPassword"]:
                        return self.reply(400)
                    state.admin = request["newPassword"]
                    state.password_changes += 1
                    return self.reply(200)
                if self.path == "/api/v1/users":
                    require(request.get("isBot") is True and request["authenticationMechanism"]["authType"] == "JWT", "a bot user must be a JWT bot")
                    require(request["authenticationMechanism"]["config"]["JWTTokenExpiry"] == "Unlimited", "a bot token must not expire silently")
                    require(request.get("roles") and len(request["roles"]) == 1, "a bot user needs exactly one role")
                    existing = state.users.get(request["name"])
                    if existing is None:
                        digest = hashlib.sha256(f"{request['name']}-token-material".encode()).hexdigest()
                        existing = {"id": str(uuid.uuid5(uuid.NAMESPACE_URL, request["name"])), "token": f"synthetic.{digest}.jwt"}
                        state.users[request["name"]] = existing
                    return self.reply(200, {"id": existing["id"], "name": request["name"]})
                if self.path == "/api/v1/bots":
                    if request["name"] == state.refuse_bot:
                        return self.reply(403)
                    state.bots[request["name"]] = state.users[request["botUser"]]["id"]
                    return self.reply(200, {"name": request["name"]})
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
        require(state.admin == ADMIN_VALUE and state.password_changes == 1, "the factory password was not replaced exactly once")
        require(sorted(state.bots) == ["comparison-bot", "ingestion-bot", "lineage-bot"], f"wrong bots: {sorted(state.bots)}")
        require(set(store.values) == set(PARAMETERS.values()), "every caller needs its own token parameter")
        require(len(set(store.values.values())) == 3, "two callers received the same token")
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
        require(PARAMETERS["lineage"] in store.values and PARAMETERS["comparison"] in store.values, "one refused bot stopped the others")
        assert_no_values(err)
        OBSERVED["refused_bot"] = {"exit": code}


def missing_role_fails_visibly() -> None:
    with open_metadata() as (state, base):
        state.missing_role = "DataConsumer"
        store = FakeParameterStore()
        code, _out, err = run(environment(base), store)
        require(code == 1 and "comparison" in err and "role" in err, f"a missing role must fail and name the caller: {code} {err}")
        require(PARAMETERS["comparison"] not in store.values, "a token was written for a caller without its role")
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


CASES: dict[str, Callable[[], None]] = {
    "fresh_start": fresh_start,
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
