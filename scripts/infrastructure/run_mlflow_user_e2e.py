"""Run the MLflow user one-shot against a loopback stand-in of the MLflow auth routes and keep the report under data/e2e/mlflow_user/.

Run from the repository root: ``.venv/bin/python -m scripts.infrastructure.run_mlflow_user_e2e``. The script is the one the services stack puts in the task
definition (``infra/services/scripts/mlflow_user.py``), which also holds the one check of the model API login and its READ grants that the serving secrets tool
calls. The shared MLflow auth stand-in (``scripts/infrastructure/mlflow_auth_stand_in.py``, the MLflow 3.17.2 routes) answers on 127.0.0.1
and a dictionary stands in for Parameter Store. No AWS service is contacted and no container starts.
"""

from __future__ import annotations

import contextlib
import importlib.util
import io
import json
import sys
import time
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from scripts.infrastructure.mlflow_auth_stand_in import MlflowAuthStandIn

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


def stand_in(**changes: Any) -> MlflowAuthStandIn:
    """The shared MLflow auth stand-in with one registered model and one experiment, as a server whose default for a user is no permission."""
    server = MlflowAuthStandIn(ADMIN, models={"hai_model_HAI_1"}, experiments={"hai_training_HAI_1": ("7", "active")})
    server.default_permission = "NO_PERMISSIONS"
    for name, value in changes.items():
        setattr(server, name, value)
    return server


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


def run(server: MlflowAuthStandIn, parameters: Parameters, types: str = "HAI_1", run_seconds: float | None = None) -> tuple[int, str]:
    script = load()
    out, err = io.StringIO(), io.StringIO()
    arguments = () if run_seconds is None else (run_seconds,)
    with contextlib.nullcontext() if server.thread.is_alive() else server, contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
        code = script.run(environment(server.url, types), parameters, *arguments)
    return code, out.getvalue() + err.getvalue()


def no_values(text: str) -> bool:
    return ADMIN not in text and USER_VALUE not in text


READ_MODEL, READ_EXPERIMENT = (USER_NAME, "registered_model", "hai_model_HAI_1"), (USER_NAME, "experiment", "7")


def posts(server: MlflowAuthStandIn, since: int = 0) -> list[tuple[str, str]]:
    return [call for call in server.log[since:] if call[0] == "POST"]


def case_creates_the_user_and_grants_read() -> dict[str, bool]:
    server = stand_in()
    code, output = run(server, Parameters())
    return {
        "the exit code is 0": code == 0,
        "the user exists with the stored credential": server.users.get(USER_NAME) == USER_VALUE,
        "the registered model and the experiment are readable by the user": server.grants == {READ_MODEL: "READ", READ_EXPERIMENT: "READ"},
        "the output reports created and ready": "user created" in output and "mlflow_user: ready" in output,
        "no password reaches the output": no_values(output),
    }


def case_a_rerun_changes_nothing() -> dict[str, bool]:
    server = stand_in()
    with server:
        run(server, Parameters())
        before = (dict(server.users), dict(server.grants))
        calls = len(server.log)
        code, output = run(server, Parameters())
    return {
        "the exit code is 0": code == 0,
        "nothing was created or granted again": posts(server, calls) == [] and (dict(server.users), dict(server.grants)) == before,
        "the output says the user and the grants are current": "user current" in output and "already" in output,
    }


def case_a_user_with_another_password_is_never_overwritten() -> dict[str, bool]:
    server = stand_in()
    server.users[USER_NAME] = "another-password-value"
    code, output = run(server, Parameters())
    return {
        "the exit code is 1": code == 1,
        "the password is unchanged": server.users[USER_NAME] == "another-password-value",
        "the output names the refused login": "login" in output and "not overwritten" in output,
        "no grant was written and nothing is reported ready": not server.grants and "ready" not in output,
        "no password reaches the output": no_values(output) and "another-password-value" not in output,
    }


def case_a_refused_administrator_login_fails_before_any_write() -> dict[str, bool]:
    server = stand_in()
    code, output = run(server, Parameters({**Parameters().values, NAMES["admin"]: "not-" + ADMIN}))
    return {
        "the exit code is 1": code == 1,
        "nothing is written": set(server.users) == {"admin"} and not posts(server),
        "the output names the administrator login": "administrator login" in output,
        "nothing is reported ready": "ready" not in output,
    }


def case_a_missing_login_parameter_skips_the_step() -> dict[str, bool]:
    server = stand_in()
    script = load()
    code, output = run(server, Parameters({NAMES["admin"]: ADMIN}))
    return {
        "the exit code is the distinct skipped code 3, not 0": code == 3 == script.SKIPPED,
        "no call reaches MLflow": server.log == [],
        "the output says the model API login is not stored yet, that nothing was checked and no account is reported ready": "not stored" in output
        and "skipped" in output
        and "ready" not in output,
    }


def case_a_model_not_registered_yet_is_pending() -> dict[str, bool]:
    server = stand_in()
    server.models, server.experiments = set(), {}
    code, output = run(server, Parameters())
    return {
        "the exit code is 0": code == 0,
        "the user is still created": server.users.get(USER_NAME) == USER_VALUE,
        "no grant is written": not server.grants,
        "the output says pending for the model and the experiment": output.count("pending") >= 2,
    }


def case_every_served_type_is_covered() -> dict[str, bool]:
    server = stand_in()
    server.models |= {"hai_model_HAI_2"}
    server.experiments["hai_training_HAI_2"] = ("9", "active")
    code, _ = run(server, Parameters(), types="HAI_1,HAI_2")
    return {
        "the exit code is 0": code == 0,
        "both types get a model and an experiment grant": len(server.grants) == 4,
    }


def case_a_stronger_grant_is_reported_and_kept() -> dict[str, bool]:
    server = stand_in()
    server.users[USER_NAME] = USER_VALUE
    server.grants[READ_MODEL] = "MANAGE"
    code, output = run(server, Parameters())
    return {
        "the exit code is 0": code == 0,
        "the stronger grant is kept": server.grants[READ_MODEL] == "MANAGE",
        "the output reports it": "MANAGE" in output and "kept" in output,
    }


def case_a_deny_grant_is_refused_and_never_reported_ready() -> dict[str, bool]:
    results: dict[str, bool] = {}
    for label, kind in (("model", READ_MODEL), ("experiment", READ_EXPERIMENT)):
        server = stand_in()
        server.users[USER_NAME] = USER_VALUE
        server.grants[kind] = "DENY"
        code, output = run(server, Parameters())
        results[f"a DENY on the {label}: exit 1, DENY named, grant unchanged, never ready"] = (
            code == 1 and "DENY" in output and server.grants[kind] == "DENY" and "mlflow_user: ready" not in output
        )
    return results


def case_an_explicit_no_permission_row_is_refused() -> dict[str, bool]:
    server = stand_in()
    server.users[USER_NAME] = USER_VALUE
    server.grants[READ_MODEL] = "NO_PERMISSIONS"
    code, output = run(server, Parameters())
    return {
        "the exit code is 1": code == 1,
        "the row is unchanged": server.grants[READ_MODEL] == "NO_PERMISSIONS",
        "nothing is reported ready": "mlflow_user: ready" not in output,
    }


def case_a_server_default_of_read_needs_no_write() -> dict[str, bool]:
    server = stand_in(default_permission="READ")
    server.users[USER_NAME] = USER_VALUE
    code, output = run(server, Parameters())
    return {
        "the exit code is 0 and the account is ready": code == 0 and "mlflow_user: ready" in output,
        "no grant row is written": posts(server) == [] and not server.grants,
    }


def case_a_slow_answer_body_stops_at_the_run_deadline() -> dict[str, bool]:
    server = stand_in(slow_body_seconds=0.03)
    began = time.monotonic()
    code, output = run(server, Parameters(), run_seconds=0.6)
    elapsed = time.monotonic() - began
    return {
        "the exit code is 1": code == 1,
        "the run ends within a second of its deadline": elapsed < 2.5,
        "the output names the deadline": "deadline" in output,
        "nothing is reported ready": "mlflow_user: ready" not in output,
    }


def case_a_deny_on_an_experiment_in_the_trash_is_refused() -> dict[str, bool]:
    server = stand_in()
    server.experiments = {"hai_training_HAI_1": ("7", "deleted")}
    server.users[USER_NAME] = USER_VALUE
    server.grants[READ_EXPERIMENT] = "DENY"
    code, output = run(server, Parameters())
    permission_lookups = [address for method, address in server.log if "users/permissions/get" in address and "resource_id=7" in address]
    return {
        "the exit code is 1": code == 1,
        "the DENY on the deleted experiment is named": "DENY" in output and "hai_training_HAI_1" in output,
        "the permission of the deleted experiment was looked up": len(permission_lookups) == 1,
        "the DENY is unchanged and the experiment is not restored": server.grants[READ_EXPERIMENT] == "DENY"
        and server.experiments["hai_training_HAI_1"][1] == "deleted",
        "nothing is reported ready": "mlflow_user: ready" not in output,
    }


def case_a_stalled_closing_response_stops_at_the_run_deadline() -> dict[str, bool]:
    results: dict[str, bool] = {}
    for label, http11 in (("HTTP/1.0", False), ("HTTP/1.1", True)):
        server = stand_in(stall_path="/api/3.0/mlflow/users/permissions/get", stall_http11=http11)
        server.users[USER_NAME] = USER_VALUE
        began = time.monotonic()
        code, output = run(server, Parameters(), run_seconds=0.4)
        elapsed = time.monotonic() - began
        results[f"{label} closing response that stalls after one byte: exit 1, deadline named, never ready, ended within 0.2 s of the deadline"] = (
            code == 1 and "deadline" in output and "mlflow_user: ready" not in output and elapsed < 0.6
        )
    return results


def case_an_experiment_in_the_trash_is_pending() -> dict[str, bool]:
    server = stand_in()
    server.experiments = {"hai_training_HAI_1": ("7", "deleted")}
    code, output = run(server, Parameters())
    return {
        "the exit code is 0": code == 0,
        "the model is granted and the deleted experiment is not": server.grants == {READ_MODEL: "READ"},
        "the output says the experiment is pending": "experiment hai_training_HAI_1 pending" in output,
        "the deleted experiment's permission was looked up and not changed": not server.grants.get(READ_EXPERIMENT),
    }


def case_an_unsupported_type_is_refused_before_any_request() -> dict[str, bool]:
    server = stand_in()
    script = load()
    with server:
        refusal = ""
        try:
            script.ensure_account(server.url, ("admin", ADMIN), USER_NAME, USER_VALUE, ("HAI_9",), time.monotonic() + 30)
        except script.Refused as error:
            refusal = str(error)
        calls = list(server.log)
    return {"the type is refused by name": "HAI_9" in refusal, "no request reached MLflow": calls == []}


def case_an_expired_deadline_sends_no_request() -> dict[str, bool]:
    server = stand_in()
    script = load()
    with server:
        refusal = ""
        try:
            script.ensure_account(server.url, ("admin", ADMIN), USER_NAME, USER_VALUE, ("HAI_1",), time.monotonic() - 1)
        except script.Refused as error:
            refusal = str(error)
        calls = list(server.log)
    return {"the call is refused as past its deadline": "deadline" in refusal, "no request reached MLflow": calls == []}


CASES: dict[str, Callable[[], dict[str, bool]]] = {
    "a_deny_grant_is_refused_and_never_reported_ready": case_a_deny_grant_is_refused_and_never_reported_ready,
    "a_deny_on_an_experiment_in_the_trash_is_refused": case_a_deny_on_an_experiment_in_the_trash_is_refused,
    "a_missing_login_parameter_skips_the_step": case_a_missing_login_parameter_skips_the_step,
    "a_model_not_registered_yet_is_pending": case_a_model_not_registered_yet_is_pending,
    "a_refused_administrator_login_fails_before_any_write": case_a_refused_administrator_login_fails_before_any_write,
    "a_rerun_changes_nothing": case_a_rerun_changes_nothing,
    "a_server_default_of_read_needs_no_write": case_a_server_default_of_read_needs_no_write,
    "a_slow_answer_body_stops_at_the_run_deadline": case_a_slow_answer_body_stops_at_the_run_deadline,
    "a_stalled_closing_response_stops_at_the_run_deadline": case_a_stalled_closing_response_stops_at_the_run_deadline,
    "a_stronger_grant_is_reported_and_kept": case_a_stronger_grant_is_reported_and_kept,
    "a_user_with_another_password_is_never_overwritten": case_a_user_with_another_password_is_never_overwritten,
    "an_experiment_in_the_trash_is_pending": case_an_experiment_in_the_trash_is_pending,
    "an_expired_deadline_sends_no_request": case_an_expired_deadline_sends_no_request,
    "an_explicit_no_permission_row_is_refused": case_an_explicit_no_permission_row_is_refused,
    "an_unsupported_type_is_refused_before_any_request": case_an_unsupported_type_is_refused_before_any_request,
    "creates_the_user_and_grants_read": case_creates_the_user_and_grants_read,
    "every_served_type_is_covered": case_every_served_type_is_covered,
}
if len(CASES) != sum(name.startswith("case_") for name in globals()):
    raise SystemExit("a case_ function is missing from CASES")
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
