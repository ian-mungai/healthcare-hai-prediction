"""Run the MLflow user one-shot against a loopback stand-in of the MLflow auth routes and keep the report under data/e2e/mlflow_user/.

Run from the repository root: ``.venv/bin/python -m scripts.infrastructure.run_mlflow_user_e2e``. The script is the one the services stack puts in the task
definition (``infra/services/scripts/mlflow_user.py``). A threaded HTTP server on 127.0.0.1 answers the routes it uses, as the author knew them for MLflow 3.17
(unproven against the live server), and a dictionary stands in for Parameter Store. No AWS service is contacted and no container starts.
"""

from __future__ import annotations

import base64
import contextlib
import importlib.util
import io
import json
import sys
import threading
import urllib.parse
from collections.abc import Callable, Iterator
from datetime import UTC, datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = REPO_ROOT / "infra" / "services" / "scripts" / "mlflow_user.py"
OUTPUT = REPO_ROOT / "data" / "e2e" / "mlflow_user"
ADMIN = "synthetic-admin-value-0001"
USER_NAME = "model_api_reader"
USER_VALUE = "synthetic-user-value-0002"
NAMES = {"admin": "/p/mlflow_admin_password", "user": "/p/model_api_mlflow_user", "credential": "/p/model_api_mlflow_credential"}


def load() -> Any:
    spec = importlib.util.spec_from_file_location("mlflow_user", SCRIPT)
    if spec is None or spec.loader is None:
        raise FileNotFoundError(SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class State:
    """The stand-in server's data and the calls it saw."""

    def __init__(self) -> None:
        self.users: dict[str, str] = {}
        self.permissions: dict[tuple[str, str, str], str] = {}
        self.models = {"hai_model_HAI_1"}
        self.experiments = {"hai_training_HAI_1": "7"}
        self.calls: list[tuple[str, str]] = []
        self.refuse_admin = False


@contextlib.contextmanager
def server(state: State) -> Iterator[str]:
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, _format: str, *args: Any) -> None:
            """Keep synthetic requests out of the console."""

        def reply(self, status: int, body: dict[str, Any] | None = None) -> None:
            data = json.dumps(body or {}).encode()
            self.send_response(status)
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def login(self) -> tuple[str, str] | None:
            header = self.headers.get("Authorization", "")
            if not header.startswith("Basic "):
                return None
            name, _, secret = base64.b64decode(header[6:]).decode().partition(":")
            return name, secret

        def handle_request(self, method: str) -> None:
            parts = urllib.parse.urlsplit(self.path)
            query = {key: values[0] for key, values in urllib.parse.parse_qs(parts.query).items()}
            length = int(self.headers.get("Content-Length") or 0)
            body = json.loads(self.rfile.read(length)) if length else {}
            state.calls.append((method, parts.path))
            who = self.login()
            is_admin = who == ("admin", ADMIN) and not state.refuse_admin
            is_user = who is not None and state.users.get(who[0]) == who[1]
            if not (is_admin or is_user):
                return self.reply(401)
            path = parts.path
            if path.endswith("/registered-models/search") or path.endswith("/experiments/search"):
                return self.reply(200)
            if not is_admin:
                return self.reply(403)
            if path == "/api/2.0/mlflow/users/get":
                return (
                    self.reply(200, {"user": {"username": query["username"]}})
                    if query["username"] in state.users
                    else self.reply(404, {"error_code": "RESOURCE_DOES_NOT_EXIST"})
                )
            if path == "/api/2.0/mlflow/users/create":
                if body["username"] in state.users:
                    return self.reply(400, {"error_code": "RESOURCE_ALREADY_EXISTS"})
                state.users[body["username"]] = body["password"]
                return self.reply(200, {"user": {"username": body["username"]}})
            if path == "/api/2.0/mlflow/registered-models/get":
                return self.reply(200) if query["name"] in state.models else self.reply(404, {"error_code": "RESOURCE_DOES_NOT_EXIST"})
            if path == "/api/2.0/mlflow/experiments/get-by-name":
                found = state.experiments.get(query["experiment_name"])
                return self.reply(200, {"experiment": {"experiment_id": found}}) if found else self.reply(404, {"error_code": "RESOURCE_DOES_NOT_EXIST"})
            if path == "/api/3.0/mlflow/users/permissions/get":
                key = (query["username"], query["resource_type"], query["resource_id"])
                return self.reply(200, {"permission": state.permissions.get(key, "NO_PERMISSIONS")})
            if path == "/api/3.0/mlflow/users/permissions/grant":
                state.permissions[(body["username"], body["resource_type"], body["resource_id"])] = body["permission"]
                return self.reply(200)
            return self.reply(404)

        def do_GET(self) -> None:
            self.handle_request("GET")

        def do_POST(self) -> None:
            self.handle_request("POST")

    httpd = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=httpd.serve_forever, kwargs={"poll_interval": 0.05}, name="mlflow_standin")
    thread.start()
    try:
        yield f"http://127.0.0.1:{httpd.server_port}"
    finally:
        httpd.shutdown()
        httpd.server_close()
        thread.join(timeout=5)


class Parameters:
    """Parameter Store: the three parameters the role may read."""

    def __init__(self, values: dict[str, str] | None = None) -> None:
        self.values = dict(values if values is not None else {NAMES["admin"]: ADMIN, NAMES["user"]: USER_NAME, NAMES["credential"]: USER_VALUE})
        self.reads: list[str] = []

    def get_parameter(self, **arguments: Any) -> dict[str, Any]:
        self.reads.append(arguments["Name"])
        if arguments["Name"] not in self.values:
            raise ParameterNotFound(arguments["Name"])
        return {"Parameter": {"Value": self.values[arguments["Name"]]}}


class ParameterNotFound(Exception):
    """The stand-in's missing parameter, named as boto3 names it."""


def environment(url: str, types: str = "HAI_1") -> dict[str, str]:
    return {
        "MLFLOW_URL": url,
        "MLFLOW_ADMIN_USERNAME": "admin",
        "ADMIN_PASSWORD_PARAMETER": NAMES["admin"],
        "USER_PARAMETER": NAMES["user"],
        "CREDENTIAL_PARAMETER": NAMES["credential"],
        "MODEL_API_HAI_TYPES": types,
    }


def run(state: State, parameters: Parameters, types: str = "HAI_1") -> tuple[int, str]:
    script = load()
    out, err = io.StringIO(), io.StringIO()
    with server(state) as url, contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
        code = script.run(environment(url, types), parameters)
    return code, out.getvalue() + err.getvalue()


def no_values(text: str) -> bool:
    return ADMIN not in text and USER_VALUE not in text


def case_creates_the_user_and_grants_read() -> dict[str, bool]:
    state = State()
    code, output = run(state, Parameters())
    return {
        "the exit code is 0": code == 0,
        "the user exists with the stored credential": state.users.get(USER_NAME) == USER_VALUE,
        "the registered model and the experiment are readable by the user": state.permissions
        == {(USER_NAME, "registered_model", "hai_model_HAI_1"): "READ", (USER_NAME, "experiment", "7"): "READ"},
        "the output reports created": "user created" in output,
        "no password reaches the output": no_values(output),
    }


def case_a_rerun_changes_nothing() -> dict[str, bool]:
    state = State()
    run(state, Parameters())
    before = (dict(state.users), dict(state.permissions))
    calls = len(state.calls)
    code, output = run(state, Parameters())
    writes = [call for call in state.calls[calls:] if call[0] == "POST"]
    return {
        "the exit code is 0": code == 0,
        "nothing was created or granted again": writes == [] and (dict(state.users), dict(state.permissions)) == before,
        "the output says the user and the grants are current": "user current" in output and "already" in output,
    }


def case_a_user_with_another_password_is_never_overwritten() -> dict[str, bool]:
    state = State()
    state.users[USER_NAME] = "another-password-value"
    code, output = run(state, Parameters())
    return {
        "the exit code is 1": code == 1,
        "the password is unchanged": state.users[USER_NAME] == "another-password-value",
        "the output names the refused login": "login" in output and "not overwritten" in output,
        "no grant was written": not state.permissions,
        "no password reaches the output": no_values(output) and "another-password-value" not in output,
    }


def case_a_refused_administrator_login_fails_before_any_write() -> dict[str, bool]:
    state = State()
    state.refuse_admin = True
    code, output = run(state, Parameters())
    return {
        "the exit code is 1": code == 1,
        "nothing is written": not state.users and not [call for call in state.calls if call[0] == "POST"],
        "the output names the administrator login": "administrator login" in output,
    }


def case_a_missing_login_parameter_skips_the_step() -> dict[str, bool]:
    state = State()
    code, output = run(state, Parameters({NAMES["admin"]: ADMIN}))
    return {
        "the exit code is 0": code == 0,
        "no call reaches MLflow": state.calls == [],
        "the output says the model API login is not stored yet": "not stored" in output,
    }


def case_a_model_not_registered_yet_is_pending() -> dict[str, bool]:
    state = State()
    state.models, state.experiments = set(), {}
    code, output = run(state, Parameters())
    return {
        "the exit code is 0": code == 0,
        "the user is still created": state.users.get(USER_NAME) == USER_VALUE,
        "no grant is written": not state.permissions,
        "the output says pending for the model and the experiment": output.count("pending") >= 2,
    }


def case_every_served_type_is_covered() -> dict[str, bool]:
    state = State()
    state.models |= {"hai_model_HAI_2"}
    state.experiments["hai_training_HAI_2"] = "9"
    code, _ = run(state, Parameters(), types="HAI_1,HAI_2")
    return {
        "the exit code is 0": code == 0,
        "both types get a model and an experiment grant": len(state.permissions) == 4,
    }


def case_a_stronger_grant_is_reported_and_kept() -> dict[str, bool]:
    state = State()
    state.users[USER_NAME] = USER_VALUE
    state.permissions[(USER_NAME, "registered_model", "hai_model_HAI_1")] = "MANAGE"
    code, output = run(state, Parameters())
    return {
        "the exit code is 0": code == 0,
        "the stronger grant is kept": state.permissions[(USER_NAME, "registered_model", "hai_model_HAI_1")] == "MANAGE",
        "the output reports it": "MANAGE" in output and "kept" in output,
    }


CASES: dict[str, Callable[[], dict[str, bool]]] = {
    name.removeprefix("case_"): function for name, function in sorted(globals().items()) if name.startswith("case_")
}
CASE_ERRORS = (AssertionError, LookupError, OSError, TypeError, ValueError, AttributeError, NameError, ImportError)


def main() -> int:
    """Run every case, save the outcomes and return 1 when any check failed."""
    results: dict[str, dict[str, bool]] = {}
    for name, function in CASES.items():
        try:
            results[name] = function()
        except CASE_ERRORS as error:
            results[name] = {f"the case ended with {type(error).__name__}: {error}": False}
    passed = all(all(checks.values()) for checks in results.values())
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    OUTPUT.mkdir(parents=True, exist_ok=True)
    (OUTPUT / f"report_{stamp}.json").write_text(json.dumps({"run_at_utc": stamp, "results": results, "passed": passed}, indent=2, sort_keys=True) + "\n")
    for name, checks in results.items():
        failed = [check for check, ok in checks.items() if not ok]
        sys.stdout.write(f"{'PASS' if not failed else 'FAIL'} {name}{': ' + '; '.join(failed) if failed else ''}\n")
    sys.stdout.write(f"{sum(all(checks.values()) for checks in results.values())} of {len(results)} cases passed\n")
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
